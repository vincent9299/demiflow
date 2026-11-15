"""Real same-process worker hosting, resource context and cleanup evidence."""
import os
from dataclasses import replace

import pytest

from demiflow import data
from pathlib import Path
import demiflow
@pytest.fixture(autouse=True)
def _bind_test_process_to_worktree():
    # Business modules may legitimately be imported from their sealed archive.
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])
from demiflow.execution.run_context import RunContext
from demiflow.planning.dataflow_policy import resolve_runtime_policy


def _pid(row):
    import os
    import threading
    from demiflow.resources.capacity import inherited_envelope
    grant = inherited_envelope()
    return {'n': row['n'], 'pid': os.getpid(), 'tid': threading.get_native_id(),
            'allocation': grant['allocation_id'], 'memory': grant['memory_bytes']}


def test_policy_mode_is_explicit_local_and_historical_default_omitted():
    from test_runtime_v3_c import _physical
    from demiflow.execution.contracts.dataflow_codec import encode_wire, decode_wire, compute_physical_digest
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    physical = _physical()
    wire = encode_wire(physical, type_name='PhysicalDataflow', bounds=physical.bounds)
    assert b'worker_mode' not in wire
    assert decode_wire(wire, type_name='PhysicalDataflow', bounds=physical.bounds).policy.worker_mode is None
    process, _ = resolve_runtime_policy({'worker_mode': 'process'}, {}, provider='local')
    assert process.worker_mode is None
    thread, _ = resolve_runtime_policy({'worker_mode': 'thread'}, {}, provider='local')
    assert thread.worker_mode == 'thread'
    assert compute_physical_digest(replace(physical, policy=replace(physical.policy, worker_mode='thread'))) != compute_physical_digest(physical)
    with pytest.raises(DataflowError, match='local'):
        resolve_runtime_policy({'worker_mode': 'thread'}, {}, provider='ray_core')


@pytest.mark.parametrize('mode', ['thread', 'process'])
def test_real_dataset_pid_and_original_allocation_envelope(tmp_path, mode):
    import threading
    from demiflow.resources.capacity import inherited_envelope
    before = inherited_envelope()
    with RunContext.start(tmp_path/'run'), data.local_execution(worker_mode=mode, workers=1):
        rows = data.from_items([{'n': n} for n in range(5)]).map(_pid).take_all()
    assert [r['n'] for r in rows] == list(range(5))
    assert all((r['pid'] == os.getpid()) == (mode == 'thread') for r in rows)
    assert all(r['tid'] != threading.get_native_id() and r['memory'] > 0 for r in rows)
    assert inherited_envelope() == before
    assert not any(t.name in {'demiflow-thread-host', 'dataflow-business'} for t in threading.enumerate())


async def _async_pid(row):
    import asyncio
    return await asyncio.to_thread(_pid, row)


def test_real_async_and_default_pool_inherit_same_grant(tmp_path):
    with RunContext.start(tmp_path/'run'), data.local_execution(worker_mode='thread', workers=3):
        rows = data.from_items([{'n': n} for n in range(4)]).map_async(_async_pid).take_all()
    assert [r['n'] for r in rows] == list(range(4))
    assert {r['pid'] for r in rows} == {os.getpid()}
    assert len({r['allocation'] for r in rows}) == 1


def test_thread_exit_evidence_waits_for_framework_work(tmp_path, monkeypatch):
    import threading
    import time
    from demiflow.execution.dataflow_thread_host import _ThreadUnit
    from demiflow.execution.worker import dataflow_host
    from demiflow.resources.capacity import inherited_envelope, track_owned_thread
    entered, release = threading.Event(), threading.Event()
    class Shell:
        def __init__(self, *args):
            self.worker = None
        def work(self):
            assert inherited_envelope()['allocation_id'] == 'allocation'
            self.worker = threading.Thread(target=lambda: (entered.set(), release.wait()), daemon=True)
            self.worker.start()
            track_owned_thread(self.worker)
        def close(self):
            pass  # Deliberately insufficient: provider still needs actual exit.
    monkeypatch.setattr(dataflow_host, 'HostedDataflowWorker', Shell)
    unit = _ThreadUnit({'boot_id': 'boot', 'dataflow_v3': {'bounds': {'max_pending_events': 2}}},
        spec_hash='0'*64, envelope={'allocation_id': 'allocation', 'spec_hash': '0'*64,
            'cpu_millis': 1000, 'memory_bytes': 1024, 'resource_directory': str(tmp_path)})
    try:
        unit.submit('work').result(timeout=1)
        assert entered.wait(1)
        assert not unit.shutdown(timeout_ms=10)
        assert unit.host.worker.is_alive()
    finally:
        release.set()
    assert unit.shutdown(timeout_ms=1000)


def test_hook_failure_never_substitutes_for_physical_exit(tmp_path, monkeypatch):
    import threading
    from demiflow.execution.dataflow_thread_host import _ThreadUnit
    from demiflow.execution.worker import dataflow_host
    from demiflow.resources.capacity import track_owned_thread
    release = threading.Event()
    class Shell:
        def __init__(self, *args):
            self.worker = threading.Thread(target=release.wait, daemon=True)
            self.worker.start()
            track_owned_thread(self.worker)
        def close(self): raise ValueError('cleanup failed')
    monkeypatch.setattr(dataflow_host, 'HostedDataflowWorker', Shell)
    unit = _ThreadUnit({'boot_id':'boot', 'dataflow_v3':{'bounds':{'max_pending_events':2}}},
        spec_hash='0'*64, envelope={'allocation_id':'allocation', 'spec_hash':'0'*64,
            'cpu_millis':1000, 'memory_bytes':1024, 'resource_directory':str(tmp_path)})
    try:
        assert not unit.shutdown(timeout_ms=20)
        assert unit.host.worker.is_alive()
        assert isinstance(unit.cleanup_error, ValueError)
    finally:
        release.set()
    assert unit.shutdown(timeout_ms=1000)
    assert not unit.thread.is_alive() and not unit.host.worker.is_alive()
    assert isinstance(unit.cleanup_error, ValueError)  # Never reported as a successful hook.


def test_live_streaming_scope_is_attempt_only_and_releases_registry(tmp_path, monkeypatch):
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution import dataflow_thread_programs as live
    from demiflow.execution.dataflow_seal import _read_json, PROGRAM_MEDIA
    seen, shared = [], []
    prepare = DataflowExecution.prepare
    def capture(execution):
        result = prepare(execution)
        nodes = [n for n in execution._graph.nodes if n.kernel.configuration.get('thread_attempt')]
        assert len(nodes) == 1
        assert nodes[0].kernel.state == 'nonrecoverable'
        assert execution._physical.recovery == 'attempt_only'
        manifest = _read_json(execution._store, nodes[0].program_ref,
                             max_bytes=execution._bounds.max_control_bytes, media_type=PROGRAM_MEDIA)
        assert 'thread_attempt' in manifest and 'payload_ref' not in manifest
        assert set(manifest['thread_attempt']) == {'schema', 'capability', 'program_key'}
        binding = live.owner(execution._action)
        seen.extend(token for token, item in live._VALUES.items() if item[0] == binding)
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', capture)
    def transform(row):
        shared.append(row['n'])
        return row
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=2):
        assert data.from_items([{'n': 1}]).map(transform, execution='thread').take_all() == [{'n': 1}]
    assert shared == [1]
    assert seen and all(token not in live._VALUES for token in seen)


def test_explicit_process_does_not_silently_adopt_live_thread_program(tmp_path):
    import threading
    lock = threading.Lock()
    def transform(row):
        with lock:
            return row
    with pytest.raises(Exception, match='live handle'):
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=2, worker_mode='process'):
            data.from_items([{'n': 1}]).map(transform, execution='thread').take_all()


def test_plain_task_nonpickleable_program_does_not_become_shared(tmp_path):
    import threading
    lock = threading.Lock()
    def transform(row):
        with lock:
            return row
    with pytest.raises(Exception, match='task/process scope'):
        with RunContext.start(tmp_path/'run'), data.local_execution(worker_mode='thread', workers=2):
            data.from_items([{'n': 1}]).map(transform).take_all()


def test_registry_rejects_other_attempt_and_missing_entry():
    from demiflow.execution import dataflow_thread_programs as live
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    binding = {'run_id': 'run', 'action_id': 'action', 'attempt': 1, 'fence': 'fence'}
    value = object()
    marker = live.register(value, binding=binding, occurrence_id='occurrence', max_entries=16)
    try:
        assert live.lookup(marker, binding=binding, occurrence_id='occurrence') is value
        with pytest.raises(DataflowError, match='absent or fenced'):
            live.lookup(marker, binding={**binding, 'attempt': 2}, occurrence_id='occurrence')
    finally:
        live.release(binding)
    with pytest.raises(DataflowError, match='absent or fenced'):
        live.lookup(marker, binding=binding, occurrence_id='occurrence')


def _recovery_rows():
    return data.from_items([{'n': n} for n in range(6)]).map(_pid)


def test_recovery_uses_original_sealed_thread_mode(tmp_path, monkeypatch):
    from demiflow.execution.dataflow import DataflowExecution
    class Interrupted(Exception): pass
    seen = []
    def stop(execution, image):
        seen.append(execution._physical)
        raise Interrupted()
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=2, worker_mode='thread'):
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_retire_phase', stop)
                _recovery_rows().take_all()
    assert seen[0].policy.worker_mode == 'thread'
    # Caller default must not replace the original sealed host strategy.
    with RunContext.resume(ref), data.local_execution(workers=2):
        rows = _recovery_rows().take_all()
    assert [r['n'] for r in rows] == list(range(6))
    assert {r['pid'] for r in rows} == {os.getpid()}


def test_original_capacity_remains_charged_until_real_thread_exit(tmp_path, monkeypatch):
    import threading
    from demiflow.execution.worker import dataflow_host
    from demiflow.execution.dataflow_thread_host import DataflowThreadConnector
    from demiflow.resources.thread import ThreadResourceProvider
    from demiflow.resources.capacity import LocalCapacityPool, track_owned_thread
    from demiflow.resources.contracts import spec_hash
    release = threading.Event()
    class Shell:
        def __init__(self, *args):
            self.child = threading.Thread(target=release.wait, daemon=True)
            self.child.start()
            track_owned_thread(self.child)
        def close(self): pass
    monkeypatch.setattr(dataflow_host, 'HostedDataflowWorker', Shell)
    pool = LocalCapacityPool(tmp_path/'pool', capacity={'cpu_millis':1000, 'memory_bytes':1024})
    connector = DataflowThreadConnector()
    connector.stage('allocation', {'boot_id':'boot', 'dataflow_v3':{'bounds':{'max_pending_events':2}}})
    provider = ThreadResourceProvider(pool, tmp_path/'units', control=connector)
    request = {'profile':{'requests':{'cpu_millis':1000, 'memory_bytes':1024}}, 'provision_deadline_ms':1000}
    provider.ensure_present('allocation', {'scope_id':'scope', 'request':request, 'spec_hash':spec_hash(request)})
    connector.units['allocation'].ready.result(timeout=1)
    exact = {'scope_id':'scope', 'allocation_id':'allocation'}
    try:
        assert provider.ensure_absent(exact, 10)['cleanup'] == 'UNCONFIRMED'
        assert pool.snapshot()['charged'] == {'cpu_millis':1000, 'memory_bytes':1024}
    finally:
        release.set()
    assert provider.ensure_absent(exact, 1000)['cleanup'] == 'CONFIRMED'
    assert pool.snapshot()['charged'] == {'cpu_millis':0, 'memory_bytes':0}
    assert not connector.units and not connector._staged


def test_live_program_still_checks_real_code_pin(tmp_path):
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.contracts.dataflow import ProgramDeclaration
    from demiflow.execution.dataflow_store import RunArtifactStore
    from demiflow.execution.dataflow_authority import DriverArtifactPort
    from demiflow.execution.dataflow_seal import (_stage_one_program, verify_dataflow_program,
        _read_json, PROGRAM_MEDIA)
    from demiflow.execution.dataflow_thread_programs import owner, release
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    def original(row): return row
    def changed(row): return {'changed': True}
    declaration = ProgramDeclaration(occurrence_id='1'*64, callable_spec=CallableSpec.create(original),
        source_spec=None, action_program=None, code_identity={})
    with RunContext.start(tmp_path/'run') as run, run.action({'kind':'take_all'}) as action:
        store = RunArtifactStore(root=run.directory/'.dataflow-store', store_id='local',
            journal=DriverArtifactPort(action, bounds=bounds), bounds=bounds)
        binding = owner(action)
        try:
            ref = _stage_one_program(declaration, store=store, bounds=bounds, live_binding=binding, force_live=True)
            manifest = _read_json(store, ref, max_bytes=bounds.max_control_bytes, media_type=PROGRAM_MEDIA)
            loaded = verify_dataflow_program(ref, expected_pin=manifest['code_identity'],
                store=store, bounds=bounds, live_binding=binding)
            assert loaded.value.target is original
            old_code, original.__code__ = original.__code__, changed.__code__
            try:
                with pytest.raises(DataflowError, match='code differs'):
                    verify_dataflow_program(ref, expected_pin=manifest['code_identity'],
                        store=store, bounds=bounds, live_binding=binding)
            finally:
                original.__code__ = old_code
        finally:
            release(binding)
        action.complete({'kind': 'thread-program-proof', 'code_pin_verified': True})


def test_failure_group_transport_is_finite_and_preserves_known_causes():
    import json
    from demiflow.execution.dataflow_failures import failure_payload, restore_failure
    error = ExceptionGroup('actors', [ValueError('first'), RuntimeError('second')])
    restored = restore_failure(failure_payload(error))
    assert isinstance(restored, ExceptionGroup)
    assert [type(e) for e in restored.exceptions] == [ValueError, RuntimeError]
    from demiflow.execution.contracts.dataflow import _json_map
    assert isinstance(restore_failure(_json_map(failure_payload(error), 'failure')), ExceptionGroup)
    large = ValueError('x'*2000)
    for _ in range(6):
        large = ExceptionGroup('wide', [large]*16)
    payload = failure_payload(large)
    assert len(json.dumps(payload)) < 100000
    assert isinstance(restore_failure(payload), ExceptionGroup)


def _finite_iterator():
    for n in range(3):
        yield {'n': n}


@pytest.mark.parametrize('terminal', ['take', 'count', 'write_lance'])
def test_explicit_inline_iterator_stream_fits_two_original_workers(tmp_path, terminal):
    import pyarrow as pa
    import lance
    from demiflow.data.api import DataAPI
    from demiflow.execution.executors.local import LocalDatasetExecutor
    executor = LocalDatasetExecutor(workers=2)
    api = DataAPI(executor)
    try:
        with RunContext.start(tmp_path/'run'):
            plan = api.from_iter(_finite_iterator).map(lambda row: row, execution='inline')
            if terminal == 'take':
                assert plan.take(2) == [{'n':0}, {'n':1}]
            elif terminal == 'count':
                assert plan.count() == 3
            else:
                target = tmp_path/'rows.lance'
                plan.write_lance(str(target), schema=pa.schema([('n', pa.int64())]))
                assert lance.dataset(str(target)).to_table().to_pylist() == [{'n':n} for n in range(3)]
            facts = plan.execution_metadata().diagnostics
            assert facts['peak_active_workers'] <= 2
    finally:
        executor.close()


async def _raise_group(row):
    raise ExceptionGroup('business group', [ValueError('invalid input'), RuntimeError('provider failed')])


def test_business_failure_group_crosses_real_worker_event(tmp_path):
    with pytest.raises(ExceptionGroup) as captured:
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=2, worker_mode='thread'):
            data.from_items([{'n':1}]).map_async(_raise_group).take_all()
    assert [type(e) for e in captured.value.exceptions] == [ValueError, RuntimeError]


def test_live_thread_callable_instance_keeps_per_action_copy_contract(tmp_path):
    class Counter:
        def __init__(self): self.count = 0
        def __call__(self, row):
            self.count += 1
            return {**row, 'count': self.count}
    actor = Counter()
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=2, worker_mode='thread'):
        plan = data.from_items([{'n': 1}]).map(actor, execution='thread')
        assert plan.take_all() == [{'n': 1, 'count': 1}]
        assert plan.take_all() == [{'n': 1, 'count': 1}]
    assert actor.count == 0


def test_live_manifest_and_plan_identity_exclude_attempt_fence_and_token(tmp_path):
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.contracts.dataflow import ProgramDeclaration
    from demiflow.execution.dataflow_store import RunArtifactStore
    from demiflow.execution.dataflow_authority import DriverArtifactPort
    from demiflow.execution.dataflow_seal import _stage_one_program, _read_json, PROGRAM_MEDIA
    from demiflow.execution import dataflow_thread_programs as live
    from demiflow.execution.contracts.dataflow_codec import compute_graph_digest, compute_physical_digest
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from test_runtime_v3_c import _graph, _physical
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    def fn(row): return row
    declaration = ProgramDeclaration('1'*64, CallableSpec.create(fn), None, None, {})
    with RunContext.start(tmp_path/'run') as run, run.action({'kind':'thread-program-proof'}) as action:
        store = RunArtifactStore(root=run.directory/'.dataflow-store', store_id='local',
            journal=DriverArtifactPort(action, bounds=bounds), bounds=bounds)
        first = live.owner(action)
        second = {**first, 'attempt': first['attempt'] + 1, 'fence': 'different-fence'}
        try:
            one = _stage_one_program(declaration, store=store, bounds=bounds, live_binding=first, force_live=True)
            manifest = _read_json(store, one, max_bytes=bounds.max_control_bytes, media_type=PROGRAM_MEDIA)
            with pytest.raises(DataflowError, match='absent or fenced'):
                live.lookup(manifest['thread_attempt'], binding=second, occurrence_id='1'*64)
            two = _stage_one_program(declaration, store=store, bounds=bounds, live_binding=second, force_live=True)
            assert one == two
            tokens = [token for token, item in live._VALUES.items() if item[0] in (first, second)]
            assert len(tokens) == len(set(tokens)) == 2
            graph = _graph()
            def dag(ref):
                node = replace(graph.nodes[0], program_ref=ref)
                return compute_graph_digest(replace(graph, nodes=(node,) + graph.nodes[1:]))
            physical = _physical()
            def plan(ref):
                stage = replace(physical.stages[0], kernel_refs=(ref,))
                return compute_physical_digest(replace(physical, stages=(stage,) + physical.stages[1:]))
            assert dag(one) == dag(two) and plan(one) == plan(two)
        finally:
            live.release(first)
            live.release(second)
        action.complete({'kind':'thread-program-proof', 'stable_identity':True})
