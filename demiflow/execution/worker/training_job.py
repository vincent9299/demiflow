"""Training job implementation."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from demiflow.execution.contracts.recovery_contract import document
from demiflow.execution.storage.state_artifacts import atomic_bytes
from demiflow.execution.storage.model_store import local_path

import hashlib
import json
from demiflow.execution.contracts.training import FitResult, TrainingSnapshotRef
from demiflow.execution.storage.model_store import read_metadata


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
            raise ValueError("Trainer snapshot inspection returned an invalid reference")
        result = {'snapshot': snapshot.to_dict() if snapshot else None}
    else:
        fitted = code.value.instantiate()(request, ctx)
        if type(fitted) is not FitResult or fitted.instance_id != request.instance_id:
            raise ValueError("Trainer returned an invalid result")
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

    def report_snapshot(self, snapshot):
        self.check_cancelled()
        if type(snapshot) is not TrainingSnapshotRef or snapshot.step not in self._request.semantics['save_steps']:
            raise ValueError("Undeclared training snapshot")
        raw = document(snapshot.to_dict())
        name = self.work_dir / 'reported' / (str(snapshot.step) + '.json')
        if name.exists() and read_metadata(name, len(raw)) != raw:
            raise ValueError("A reported step is immutable within a launch")
        atomic_bytes(name, raw)

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
