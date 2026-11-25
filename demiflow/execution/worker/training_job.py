"""Training job implementation."""
from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path
import sys
import time

from demiflow.execution.contracts.recovery_contract import document
from demiflow.execution.contracts.training import FitResult, ProjectObject, TrainingSnapshotRef
from demiflow.execution.storage.model_store import local_path, open_regular_file, read_metadata
from demiflow.execution.storage.state_artifacts import atomic_bytes


def _stage_file_object(value, *, model_dir: Path, check_cancelled, label: str) -> ProjectObject:
    if isinstance(value, Path):
        candidate = value
    elif type(value) is str and value and '\x00' not in value:
        candidate = local_path(value) if '://' in value else Path(value)
    else:
        raise ValueError(f"{label}: expected a file path (str or Path)")
    if '..' in candidate.parts:
        raise ValueError(f"{label}: path cannot contain parent traversal ('..')")
    path = candidate if candidate.is_absolute() else (model_dir / candidate)
    if not path.is_relative_to(model_dir):
        raise ValueError(f"{label}: model file must stay inside context.model_dir ({model_dir})")
    if path == model_dir:
        raise ValueError(f"{label}: model file cannot be the model_dir directory itself")
    try:
        with open_regular_file(path) as stream:
            st = os.fstat(stream.fileno())
            digest = hashlib.sha256()
            size = 0
            while True:
                check_cancelled()
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
            if size != st.st_size:
                raise ValueError(f"{label}: model file changed while hashing")
    except OSError as exc:
        raise ValueError(f"{label}: model file is not an accessible direct regular file under model_dir ({exc})") from exc
    return ProjectObject(str(path), digest.hexdigest(), size)


def _stage_file_map(entries, *, model_dir: Path, check_cancelled, step: int, group: str, allow_empty: bool):
    if not isinstance(entries, Mapping):
        raise ValueError(f"Step {step}: '{group}' must be a mapping from logical names to file paths")
    if not allow_empty and not entries:
        raise ValueError(f"Step {step}: '{group}' must be non-empty")
    if len(entries) > 16384:
        raise ValueError(f"Step {step}: '{group}' exceeds file count budget")
    staged = {}
    for name, raw_path in entries.items():
        if type(name) is not str or not name:
            raise ValueError(f"Step {step}: '{group}' keys must be non-empty strings")
        staged[name] = _stage_file_object(
            raw_path, model_dir=model_dir, check_cancelled=check_cancelled,
            label=f"Step {step} {group}[{name!r}]",
        )
    return staged


def _build_snapshot_from_files(step, files, resume_files, *, request, model_dir: Path, check_cancelled) -> TrainingSnapshotRef:
    if type(step) is not int or step < 1:
        raise ValueError(f"Checkpoint step must be a positive integer, got {step!r}")
    staged_files = _stage_file_map(
        files, model_dir=model_dir, check_cancelled=check_cancelled,
        step=step, group='files', allow_empty=False,
    )
    staged_resume = {}
    if resume_files is not None:
        staged_resume = _stage_file_map(
            resume_files, model_dir=model_dir, check_cancelled=check_cancelled,
            step=step, group='resume_files', allow_empty=True,
        )
    return TrainingSnapshotRef(
        step=step,
        model_format=request.semantics['model_format'],
        files=staged_files,
        resume_files=staged_resume,
    )


def _build_snapshot_from_spec(step, spec, *, request, model_dir: Path, check_cancelled) -> TrainingSnapshotRef:
    if type(step) is not int or step < 1:
        raise ValueError(f"Checkpoint step must be a positive integer, got {step!r}")
    if not isinstance(spec, Mapping) or not spec:
        raise ValueError(f"Step {step}: checkpoint entry must be a non-empty mapping")
    has_mapping_value = any(isinstance(v, Mapping) for v in spec.values())
    has_none_resume = 'resume_files' in spec and spec['resume_files'] is None
    if has_mapping_value or has_none_resume:
        extra = sorted(str(k) for k in spec.keys() if k not in ('files', 'resume_files'))
        if extra:
            raise ValueError(
                f"Step {step}: cannot mix extended checkpoint fields with plain file fields {extra}"
            )
        if 'files' not in spec or not isinstance(spec['files'], Mapping):
            raise ValueError(f"Step {step}: extended checkpoint requires 'files' mapping")
        resume_files = spec.get('resume_files')
        if resume_files is not None and not isinstance(resume_files, Mapping):
            raise ValueError(f"Step {step}: field 'resume_files' must be a mapping or None")
        return _build_snapshot_from_files(
            step, spec['files'], resume_files,
            request=request, model_dir=model_dir, check_cancelled=check_cancelled,
        )
    for key, val in spec.items():
        if not isinstance(val, (str, Path)):
            raise ValueError(
                f"Step {step}: field {key!r} must be a file path (str or Path), got {type(val).__name__}"
            )
    return _build_snapshot_from_files(
        step, spec, None,
        request=request, model_dir=model_dir, check_cancelled=check_cancelled,
    )


def _snapshot_from_reconcile(payload, *, request, model_dir: Path, check_cancelled) -> TrainingSnapshotRef:
    if not isinstance(payload, Mapping):
        raise ValueError("Trainer snapshot inspection returned an invalid reference")
    extra = sorted(str(k) for k in payload.keys() if k not in ('step', 'files', 'resume_files'))
    if extra:
        raise ValueError(f"Trainer snapshot inspection returned unexpected fields {extra}")
    if 'step' not in payload or 'files' not in payload:
        raise ValueError("Trainer snapshot inspection requires 'step' and 'files'")
    step = payload['step']
    if type(step) is not int or step not in request.semantics['save_steps']:
        raise ValueError(f"Trainer snapshot inspection returned undeclared step {step!r}")
    return _build_snapshot_from_files(
        step, payload['files'], payload.get('resume_files'),
        request=request, model_dir=model_dir, check_cancelled=check_cancelled,
    )


def _build_result_from_return(fitted, *, request, ctx: ManagedFitContext) -> FitResult:
    if fitted is None:
        raw_checkpoints, metrics = {}, {}
    elif isinstance(fitted, Mapping):
        if not fitted:
            raw_checkpoints, metrics = {}, {}
        elif all(type(k) is int for k in fitted):
            raw_checkpoints, metrics = fitted, {}
        elif all(type(k) is str for k in fitted):
            extra = sorted(k for k in fitted if k not in ('checkpoints', 'metrics'))
            if extra:
                raise ValueError(
                    "Trainer return mapping must use integer step keys or 'checkpoints'/'metrics'; "
                    f"unexpected keys: {extra}"
                )
            raw_checkpoints = fitted.get('checkpoints')
            if raw_checkpoints is None:
                raw_checkpoints = {}
            elif not isinstance(raw_checkpoints, Mapping) or any(type(k) is not int for k in raw_checkpoints):
                raise ValueError("Trainer return 'checkpoints' must map integer steps to checkpoint files")
            raw_metrics = fitted.get('metrics')
            if raw_metrics is None:
                metrics = {}
            elif not isinstance(raw_metrics, Mapping):
                raise ValueError("Trainer return 'metrics' must be a mapping")
            else:
                metrics = raw_metrics
        else:
            raise ValueError(
                "Trainer return mapping cannot mix integer step keys with wrapper keys; "
                "use {step: ...} or {'checkpoints': {step: ...}, 'metrics': {...}}"
            )
    else:
        raise ValueError("Trainer returned an invalid result")

    merged = dict(ctx._reported_snapshots)
    for step, spec in raw_checkpoints.items():
        snap = _build_snapshot_from_spec(
            step, spec, request=request, model_dir=ctx.model_dir, check_cancelled=ctx.check_cancelled,
        )
        previous = merged.get(step)
        if previous is not None and previous != snap:
            raise ValueError(f"Step {step}: returned checkpoint contradicts previously reported snapshot")
        merged[step] = snap

    if not merged:
        raise ValueError("Trainer completed without delivering any checkpoint")
    ordered = tuple(merged[step] for step in sorted(merged))
    final_step = ordered[-1].step
    expected_final = request.semantics['budget'].get('optimizer_updates')
    if expected_final is not None and final_step != expected_final:
        raise ValueError(
            f"Trainer delivered final step {final_step}, expected optimizer_updates={expected_final}"
        )
    # Only validate this launch's shape, paths, conflicts, and final step here.
    # Full save_steps completeness is verified by the driver after merging historical steps.
    return FitResult(
        instance_id=request.instance_id,
        fit_key=request.fit_key,
        snapshots=ordered,
        final_step=final_step,
        metrics=metrics,
    )


def execute_training_job(launch, request, code, ctx):
    if 'inspect_source' in launch:
        origin = launch['inspect_source']
        original_dir = ctx.work_dir.parent / ctx.request_key
        original_raw = read_metadata(original_dir / 'launch.json', 1024 * 1024)
        if (origin['key'] != ctx.request_key or hashlib.sha256(original_raw).hexdigest() != origin['launch_digest']
                or json.loads(original_raw)['request'] != request.to_dict()):
            raise ValueError("Native snapshot inspection differs from the authorized launch")
        reconcile = getattr(code.value.target, 'reconcile', None)
        snapshot = reconcile(request, model_dir=ctx.model_dir, work_dir=original_dir) if reconcile else None
        if snapshot is not None and type(snapshot) is not TrainingSnapshotRef:
            snapshot = _snapshot_from_reconcile(
                snapshot, request=request, model_dir=ctx.model_dir, check_cancelled=ctx.check_cancelled,
            )
        result = {'snapshot': snapshot.to_dict() if snapshot else None}
    else:
        fitted = code.value.instantiate()(request, ctx)
        if type(fitted) is FitResult:
            if fitted.instance_id != request.instance_id:
                raise ValueError("Trainer returned an invalid result")
        else:
            fitted = _build_result_from_return(fitted, request=request, ctx=ctx)
        result = fitted.to_dict()
    ctx.check_cancelled()
    return result


class ManagedFitContext:
    def __init__(self, launch, request, binding):
        self.request_key = launch['request_key']
        self.binding = binding
        self.work_dir = Path(launch['work_dir'])
        self.model_dir = local_path(binding.model_store_uri) / 'staging' / request.instance_id / self.request_key
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self._request = request
        self._deadline = time.monotonic() + max(0., launch['deadline_unix'] - time.time())
        self._stopping = False
        self._reported_snapshots = {}

    def stop(self, *_):
        self._stopping = True

    def check_cancelled(self):
        if self._stopping or (self.work_dir / 'cancel').exists():
            raise InterruptedError("Training was cancelled")
        if time.monotonic() >= self._deadline:
            raise TimeoutError("Training exceeded its original deadline")

    def report_progress(self, metrics):
        self.check_cancelled()
        atomic_bytes(self.work_dir / 'progress.json', document(dict(metrics), 65536))

    def report_snapshot(self, snapshot=None, *, step=None, files=None, resume_files=None):
        self.check_cancelled()
        if type(snapshot) is TrainingSnapshotRef:
            if step is not None or files is not None or resume_files is not None:
                raise ValueError("Cannot mix TrainingSnapshotRef with step/files/resume_files")
            ref = snapshot
        else:
            if snapshot is not None and step is not None:
                raise ValueError("Specify step either positionally or by keyword, not both")
            resolved_step = snapshot if snapshot is not None else step
            if type(resolved_step) is not int or files is None:
                raise ValueError("Undeclared training snapshot")
            if resolved_step not in self._request.semantics['save_steps']:
                raise ValueError("Undeclared training snapshot")
            ref = _build_snapshot_from_files(
                resolved_step, files, resume_files,
                request=self._request, model_dir=self.model_dir, check_cancelled=self.check_cancelled,
            )
        if ref.step not in self._request.semantics['save_steps']:
            raise ValueError("Undeclared training snapshot")
        raw = document(ref.to_dict())
        name = self.work_dir / 'reported' / (str(ref.step) + '.json')
        if name.exists() and read_metadata(name, max(len(raw), 1024 * 1024)) != raw:
            raise ValueError("A reported step is immutable within a launch")
        atomic_bytes(name, raw)
        self._reported_snapshots[ref.step] = ref

    def run_group(self, entry, args, deadline):
        self.check_cancelled()
        if entry not in self.binding.provider_options.get('allowed_modules', ()):
            raise ValueError("Training launcher is outside the deployment's allowed modules")
        if type(args) not in (tuple, list) or len(args) > 128 or any(type(arg) is not str for arg in args):
            raise ValueError("Invalid training launcher arguments")
        timeout = min(self._deadline, deadline)
        # The sealing loader mounted the verified project import closure in sys.path.
        # Propagate that order to torchrun and its ranks in the same environment.
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p))
        from demiflow.execution.hosts.training_bootstrap import run_training_group
        return run_training_group(entry, args,
            work_dir=self.work_dir, env=env, timeout=timeout, check_cancelled=self.check_cancelled)
