"""Exercise the same program archive and verification that current workers consume."""
from contextlib import contextmanager
from functools import partial
import json

from demiflow.execution.contracts.dataflow import ProgramDeclaration
from demiflow.execution.recovery_contract import identity
from demiflow.execution.seal.plan import _stage_one_program, verify_dataflow_program
from demiflow.execution.storage.objects import RunArtifactStore
from demiflow.execution.storage.staging import DriverArtifactPort
from demiflow.planning.dataflow_policy import resolve_runtime_policy


@contextmanager
def program_store(action):
    run = action.context
    policy, bounds = resolve_runtime_policy({}, run.limits, provider='local')
    store = RunArtifactStore(root=run.directory / '.dataflow-store', store_id=policy.store_id,
                             journal=DriverArtifactPort(action, bounds=bounds), bounds=bounds)
    try:
        yield store, bounds
    finally:
        store.close()


def stage_callable(spec, *, store, bounds):
    declaration = ProgramDeclaration(identity('worker-pin-fixture'), spec, None, None, {})
    ref = _stage_one_program(declaration, store=store, bounds=bounds)
    manifest = json.loads(b''.join(store.read(ref, max_bytes=bounds.max_control_bytes)))
    return ref, manifest


def worker_loader(fixture, spec):
    ref, manifest = stage_callable(spec, store=fixture.store, bounds=fixture.bounds)
    return partial(verify_dataflow_program, ref, expected_pin=manifest['code_identity'],
                   store=fixture.store, bounds=fixture.bounds)
