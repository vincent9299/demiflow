"""Execute the sealed trainer under one resource-unit grant."""
from demiflow.execution.worker.training_job import ManagedFitContext
from demiflow.execution.storage.model_store import read_metadata
import hashlib
import json

from pathlib import Path
import signal
import subprocess
import sys
import time

from demiflow.resources.process_group import spawn_command, supervise_unit
from demiflow.resources.process_observation import inherited_unit
from demiflow.execution.contracts.dataflow import Bounds
from demiflow.execution.contracts.dataflow_codec import decode_wire_document
from demiflow.execution.contracts.training import FitBinding, FitRequest
from demiflow.execution.contracts.recovery_contract import document
from demiflow.execution.seal.plan import load_sealed_dataflow, verify_dataflow_program
from demiflow.execution.storage.state_artifacts import atomic_bytes
from demiflow.execution.storage.objects import RunArtifactStore


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
        from demiflow.execution.worker.training_job import execute_training_job
        result = execute_training_job(launch, request, code, ctx)
        atomic_bytes(ctx.work_dir / 'result.json', document(result))
    except Exception as exc:
        atomic_bytes(ctx.work_dir / 'error.json', document({'type': type(exc).__name__,
                                                            'reason': str(exc)[:2048]}))
        raise
    finally:
        store.close()


def run_training_group(entry, args, *, work_dir, env, timeout, check_cancelled):
    process = spawn_command([sys.executable, '-B', '-m', entry, *args], cwd=work_dir,
                     env=env, shutdown_timeout_s=5.)
    try:
        while process.poll() is None:
            check_cancelled()
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


if __name__ == '__main__':
    raise SystemExit(main(*sys.argv[1:]))
