"""Execute the sealed trainer under one resource-unit grant."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from ...resources.process_group import spawn_command, supervise_unit
from ...resources.process_observation import inherited_unit
from ..contracts.dataflow import Bounds
from ..contracts.dataflow_codec import decode_wire_document
from ..contracts.training import FitBinding, FitRequest, FitResult, TrainingSnapshotRef
from ..recovery_contract import document
from ..seal.plan import load_sealed_dataflow, verify_dataflow_program
from ..state_artifacts import atomic_bytes
from ..storage.objects import RunArtifactStore
from .store import local_path


def read_metadata(path, limit):
    if path.resolve(strict=True) != path:
        raise ValueError("Job metadata must be a direct file")
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("Job metadata exceeds its admitted byte limit")
    return raw


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
        process = spawn_command([sys.executable, '-B', '-m', entry, *args], cwd=self.work_dir,
                         env=env, shutdown_timeout_s=5.)
        try:
            while process.poll() is None:
                self.check_cancelled()
                if time.monotonic() >= timeout:
                    raise TimeoutError("Managed training command exceeded its deadline")
                time.sleep(.05)
            if process.returncode:
                raise subprocess.CalledProcessError(process.returncode, entry)
        finally:
            if process.poll() is None:
                process.terminate()
                # The guardian owns the unit FD until all ranks have exited.
                process.wait()


def main(location, expected, phase=None):
    if not inherited_unit():
        raise ValueError("Managed training bootstrap needs a real process unit")
    raw = read_metadata(Path(location), 1024 * 1024)
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("Training launch differs from the resource allocation")
    launch = json.loads(raw)
    if phase is None:
        return supervise_unit([sys.executable, '-B', '-m', __spec__.name, location, expected, 'worker'],
                              deadline_unix=launch['deadline_unix'], max_rss_bytes=launch['max_rss_bytes'],
                              shutdown_timeout_s=launch['cancel_grace_s'])
    if phase != 'worker':
        raise ValueError('Unknown managed training phase')
    bounds = Bounds(**launch['bounds'])
    sealed = decode_wire_document(launch['sealed'], type_name='SealedExecution', bounds=bounds)
    request = FitRequest.from_dict(launch['request'])
    binding = FitBinding.from_dict(launch['binding'])
    if Path(location).parent != Path(launch['work_dir']):
        raise ValueError("Training work directory differs from the original launch")
    ctx = ManagedFitContext(launch, request, binding)
    signal.signal(signal.SIGTERM, ctx.stop)
    signal.signal(signal.SIGINT, ctx.stop)
    store = RunArtifactStore(root=Path(launch['store_root']), store_id=sealed.logical_ref.store_id,
                             bounds=bounds, journal=None, readonly=True)
    try:
        graph, _, pins = load_sealed_dataflow(sealed, action_id=launch['action'], store=store, bounds=bounds)
        job, = (j for j in graph.jobs if j.occurrence_id == launch['occurrence'])
        # Check membership without importing business code. Publication of the
        # matching activation proof is reserved to the fenced controller.
        ready = document({'request_key': ctx.request_key, 'launch_digest': expected})
        atomic_bytes(ctx.work_dir / 'ready.json', ready)
        while not (ctx.work_dir / 'activate.json').exists():
            ctx.check_cancelled()
            time.sleep(.05)
        if read_metadata(ctx.work_dir / 'activate.json', 4096) != ready:
            raise ValueError("Invalid managed job activation")
        code = verify_dataflow_program(job.program_ref, expected_pin=pins['programs'][job.occurrence_id],
                                       store=store, bounds=bounds, cache=ctx.work_dir / 'code')
        if code.digest != request.semantics['trainer_digest']:
            raise ValueError("Training request differs from the authorized program pin")
        ctx.check_cancelled()
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
        atomic_bytes(ctx.work_dir / 'result.json', document(result))
    except Exception as exc:
        atomic_bytes(ctx.work_dir / 'error.json', document({'type': type(exc).__name__,
                                                            'reason': str(exc)[:2048]}))
        raise
    finally:
        store.close()


if __name__ == '__main__':
    raise SystemExit(main(*sys.argv[1:]))
