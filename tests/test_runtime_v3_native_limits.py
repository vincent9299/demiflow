"""Public native budgets constrain the unified path and survive recovery."""
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_codec import encode_wire, decode_wire, compute_physical_digest
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.dataflow_native_limits import (
    NativeBudgetGuard, admission_timeout_ms, intersect_native_limits,
)
from demiflow.execution.run_context import RunContext


def test_policy_and_assignment_preserve_old_wire_and_bind_new_ceilings():
    from test_runtime_v3_c import make, _physical
    physical = _physical()
    raw = encode_wire(physical, type_name='PhysicalDataflow', bounds=physical.bounds)
    assert 'native_limits' not in json.loads(raw)['payload']['policy']
    assert encode_wire(decode_wire(raw, type_name='PhysicalDataflow', bounds=physical.bounds),
                       type_name='PhysicalDataflow', bounds=physical.bounds) == raw
    constrained = replace(physical, policy=replace(physical.policy,
        native_limits={'memory_bytes': 100000, 'timeout_s': .2}))
    assert compute_physical_digest(physical) != compute_physical_digest(constrained)
    decoded = decode_wire(encode_wire(constrained, type_name='PhysicalDataflow', bounds=physical.bounds),
                          type_name='PhysicalDataflow', bounds=physical.bounds)
    assert decoded.policy.native_limits['timeout_s'] == .2
    assignment = make('WorkerAssignment')
    old = encode_wire(assignment, type_name='WorkerAssignment', bounds=physical.bounds)
    assert 'native_limits' not in json.loads(old)['payload']
    assert encode_wire(decode_wire(old, type_name='WorkerAssignment', bounds=physical.bounds),
                       type_name='WorkerAssignment', bounds=physical.bounds) == old
    with pytest.raises(DataflowError):
        replace(physical.policy, native_limits={'memory_bytes': True})


def test_native_guard_samples_actual_process_and_preserves_headroom_direction():
    with pytest.raises(DataflowError, match='RSS'):
        NativeBudgetGuard({'max_rss_bytes': 1}).check()
    assert intersect_native_limits({'batch_rows': 2, 'cgroup_headroom_bytes': 128},
                                   {'batch_rows': 8, 'cgroup_headroom_bytes': 64}) == {
                                       'batch_rows': 2, 'cgroup_headroom_bytes': 128}
    assert admission_timeout_ms(SimpleNamespace(native_limits={'admission_timeout_s': .125},
                                                control_timeout_ms=30000)) == 125


def test_public_native_limits_constrain_real_lance_input_join_and_sink(tmp_path, monkeypatch):
    import lance
    import pyarrow as pa
    from demiflow.execution.lance_sink import LanceSink
    from demiflow.execution.resource_state import RunResourceState
    source = tmp_path/'input.lance'
    lance.write_dataset(pa.table({'n': list(range(4))}), str(source))
    batches, physicals, charges = [], [], []
    original, prepare, charged = LanceSink.prepare, DataflowExecution.prepare, RunResourceState._charged
    def native_prepare(sink, blocks, context):
        def observed():
            for block in blocks:
                batches.append(block.num_rows)
                yield block
        return original(sink, observed(), context)
    def physical(execution):
        sealed = prepare(execution)
        physicals.append(execution._physical)
        return sealed
    def charge(state, db, scope):
        result = charged(state, db, scope)
        charges.append(dict(result))
        assert result['memory_bytes'] <= 8 * 1024**2
        return result
    monkeypatch.setattr(LanceSink, 'prepare', native_prepare)
    monkeypatch.setattr(DataflowExecution, 'prepare', physical)
    monkeypatch.setattr(RunResourceState, '_charged', charge)
    limits = dict(memory_bytes=8*1024**2, max_row_bytes=64*1024,
                  max_scratch_bytes=64*1024**2, batch_rows=1, timeout_s=45,
                  admission_timeout_s=10, threads=1, partitions=1)
    output = tmp_path/'result.lance'
    with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=2, native_resources=limits):
        data.read_lance(str(source)).join(data.from_items([{'n': i, 'v': i+1} for i in range(4)]),
                                         on='n').write_lance(str(output), mode='create')
        assert all(c['status'] == 'acked' for c in run.journal.execution_commits('action-1'))
    assert sorted(lance.dataset(str(output)).to_table().to_pylist(), key=lambda r:r['n']) == [
        {'n': i, 'v': i+1} for i in range(4)]
    assert batches == [1, 1, 1, 1]
    assert charges and max(c['memory_bytes'] for c in charges) > 0
    physical = physicals[0]
    assert physical.bounds.max_resident_bytes == limits['memory_bytes']
    assert physical.bounds.max_row_bytes == limits['max_row_bytes']
    assert physical.bounds.max_spill_bytes == limits['max_scratch_bytes']
    assert physical.bounds.timeout_ms == 45000
    assert dict(physical.policy.native_limits) == limits


def test_public_native_row_cap_rejects_before_lance_publication(tmp_path):
    output = tmp_path/'reject.lance'
    with pytest.raises((DataflowError, ValueError), match='[Rr]ow|row|payload'):
        with RunContext.start(tmp_path/'run'), data.local_execution(native_resources={'max_row_bytes': 512}):
            data.from_items([{'n': 'x'*2048}]).write_lance(str(output), mode='create')
    assert not (output/'_versions').exists()


def test_public_native_rss_cap_rejects_before_any_worker_launch(tmp_path, monkeypatch):
    launches = []
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', lambda *args: launches.append(args))
    with pytest.raises(DataflowError, match='RSS'):
        with RunContext.start(tmp_path/'run'), data.local_execution(native_resources={'max_rss_bytes': 1}):
            data.from_items([{'n': 1}]).take_all()
    assert not launches


def _mapped():
    return data.from_items([{'n': i} for i in range(8)]).map(lambda r: {'n': r['n']+1})


def test_resume_retains_native_ceilings_and_original_plan_when_option_omitted(tmp_path, monkeypatch):
    class Interrupted(Exception):
        pass
    sealed = []
    # This fault targets a real durable phase boundary. Explicitly select the
    # existing unfused policy so optional pure-pipeline co-location cannot
    # remove the injection point and turn the test into a successful full run.
    from demiflow.execution import dataflow_api
    resolve = dataflow_api.resolve_action_runtime
    def unfused(request):
        policy, bounds = resolve(request)
        return replace(policy, fusion=False), bounds
    monkeypatch.setattr(dataflow_api, 'resolve_action_runtime', unfused)
    def stop(execution, image):
        assert not image.cut.final
        assert any(edge.mode == 'durable_incremental' for edge in execution._physical.exchanges)
        assert any(c['commit_id'] == image.commit_id and c['status'] == 'acked'
                   for c in execution._action.context.journal.execution_commits('action-1'))
        sealed.append(execution._physical)
        raise Interrupted()
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=2,
                native_resources={'memory_bytes': 4*1024**2, 'batch_rows': 1}):
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_retire_phase', stop)
                _mapped().take_all()
    original = DataflowExecution._prepare_launch
    observed = []
    def launch(execution, lease, stage):
        assert execution._physical.plan_id == sealed[0].plan_id
        assert execution._bounds.max_resident_bytes == 4*1024**2
        assert execution._native_limits['batch_rows'] == 1
        assert execution.services.policy.native_limits['memory_bytes'] == 4*1024**2
        observed.append(execution._bounds)
        return original(execution, lease, stage)
    with RunContext.resume(ref), data.local_execution(workers=2):
        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, '_prepare_launch', launch)
            assert _mapped().take_all() == [{'n': i+1} for i in range(8)]
    assert observed


def test_public_native_admission_deadline_bounds_real_scheduler_retry(tmp_path, monkeypatch):
    import time
    from demiflow.execution.dataflow_scheduler import DataflowScheduler
    retries = []
    def pending(scheduler, work, **kwargs):
        retries.append(time.monotonic())
        raise DataflowError('CAPACITY_PENDING', 'fixture capacity unavailable', retryable=True)
    monkeypatch.setattr(DataflowScheduler, 'place', pending)
    with pytest.raises(DataflowError, match='admission deadline'):
        with RunContext.start(tmp_path/'run'), data.local_execution(
                native_resources={'admission_timeout_s': .03}):
            data.from_items([{'n':1}]).take_all()
    assert len(retries) > 1
    assert retries[-1] - retries[0] < .5


def test_public_native_execution_deadline_does_not_publish_result(tmp_path, monkeypatch):
    import time
    original = DataflowExecution.start
    def start(execution):
        time.sleep(.03)
        return original(execution)
    monkeypatch.setattr(DataflowExecution, 'start', start)
    with pytest.raises(DataflowError, match='admitted execution timeout'):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(
                native_resources={'timeout_s': .02}):
            try:
                data.from_items([{'n':1}]).take_all()
            finally:
                assert not run.journal.execution_commits('action-1')
