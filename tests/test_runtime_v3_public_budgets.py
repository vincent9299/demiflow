"""Public resource settings must constrain real unified workers before launch."""
import json
from dataclasses import replace

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_codec import (
    encode_wire, decode_wire, compute_physical_digest,
)
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext
from demiflow.planning.dataflow_policy import resolve_runtime_policy


def test_optional_worker_limit_keeps_historical_wire_and_identity():
    from test_runtime_v3_c import _physical
    from demiflow.execution.contracts.dataflow_codec import _identity_project, _canonical_sha
    physical = _physical()
    old_policy = {
        'provider': physical.policy.provider, 'durability': physical.policy.durability,
        'default_partitions': physical.policy.default_partitions,
        'max_replicas': physical.policy.max_replicas, 'fusion': physical.policy.fusion,
        'checkpoint_rows': physical.policy.checkpoint_rows,
        'checkpoint_interval_ms': physical.policy.checkpoint_interval_ms,
        'fairness_quantum_bytes': physical.policy.fairness_quantum_bytes,
        'control_timeout_ms': physical.policy.control_timeout_ms,
        'store_id': physical.policy.store_id, 'required_recovery': physical.policy.required_recovery,
    }
    raw = encode_wire(physical, type_name='PhysicalDataflow', bounds=physical.bounds)
    frame = json.loads(raw)
    assert frame['payload']['policy'] == old_policy
    decoded = decode_wire(raw, type_name='PhysicalDataflow', bounds=physical.bounds)
    assert decoded.policy.max_active_workers is None
    assert encode_wire(decoded, type_name='PhysicalDataflow', bounds=physical.bounds) == raw
    historical_projection = _identity_project(physical)
    historical_projection.pop('plan_id')
    historical_projection['policy'] = old_policy
    assert compute_physical_digest(decoded) == _canonical_sha(['dataflow-physical-v3', historical_projection])
    bounded = replace(physical, policy=replace(physical.policy, max_active_workers=1))
    assert compute_physical_digest(bounded) != compute_physical_digest(physical)
    encoded = encode_wire(bounded, type_name='PhysicalDataflow', bounds=physical.bounds)
    assert decode_wire(encoded, type_name='PhysicalDataflow', bounds=physical.bounds).policy.max_active_workers == 1


def test_memory_option_tightens_defaults_without_erasing_explicit_limits():
    from demiflow.execution.runtime_limits import execution_limits
    run = execution_limits({'memory_bytes': 512 * 1024})
    policy, limits = resolve_runtime_policy({'memory_bytes': 64 * 1024, 'max_active_workers': 1},
                                           run, provider='local')
    assert policy.max_active_workers == 1
    assert limits.max_resident_bytes == 64 * 1024
    assert limits.max_row_bytes == limits.max_batch_bytes == limits.max_state_bytes == 16 * 1024
    _, larger = resolve_runtime_policy({'memory_bytes': 1024 * 1024}, run, provider='local')
    assert larger.max_resident_bytes == 512 * 1024
    _, explicit = resolve_runtime_policy({'memory_bytes': 64 * 1024},
        execution_limits({'max_row_bytes': 48 * 1024, 'max_batch_bytes': 48 * 1024}), provider='local',
        explicit_limits=('max_row_bytes', 'max_batch_bytes'))
    assert explicit.max_row_bytes == explicit.max_batch_bytes == 48 * 1024


@pytest.mark.parametrize('workers', [1, 2])
def test_public_worker_budget_uses_actual_phases_and_native_allocation(tmp_path, monkeypatch, workers):
    from demiflow.execution.resource_state import RunResourceState
    physicals, charges = [], []
    original_prepare = DataflowExecution.prepare
    original_charge = RunResourceState._charged
    def prepare(execution):
        result = original_prepare(execution)
        physicals.append(execution._physical)
        return result
    def charged(state, db, scope):
        result = original_charge(state, db, scope)
        charges.append(dict(result))
        assert result['workers'] <= workers
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', prepare)
    monkeypatch.setattr(RunResourceState, '_charged', charged)
    with RunContext.start(tmp_path / 'run'), data.local_execution(workers=workers, memory_bytes=128 * 1024):
        rows = data.from_items([{'n': n} for n in range(6)]).map(lambda row: {'n': row['n'] + 1}).take_all()
    assert rows == [{'n': n + 1} for n in range(6)]
    physical = physicals[0]
    assert physical.policy.max_active_workers == workers
    assert physical.bounds.max_resident_bytes == 128 * 1024
    assert all(sum(len(s.partitions) for s in physical.stages if s.stage_id in group) <= workers
               for group in physical.residency.simultaneous_groups)
    assert any(edge.mode == 'durable_incremental' for edge in physical.exchanges)
    assert max(c['workers'] for c in charges) <= workers
    assert max(c['memory_bytes'] for c in charges) <= 128 * 1024


def test_source_splits_keep_identity_under_replica_ceiling():
    from test_runtime_v3_c import make, bounds, sha
    from demiflow.execution.contracts.dataflow import DataflowGraph
    from demiflow.planning.dataflow_place import partition_stages, _refine_source_partitions
    source_id = sha('source')
    node = replace(make('GraphNode'), occurrence_id=source_id, kind='source',
                   kernel=replace(make('KernelSpec'), state='none', effect='pure', inputs=()))
    graph = DataflowGraph(schema='demiflow.dataflow_graph.v3', action_id='action', dag_id='0' * 64,
                          nodes=(node,), edges=(), terminals=(), sources=(source_id,))
    policy = replace(make('RuntimePolicy'), default_partitions=8, max_replicas=8, max_active_workers=2)
    stages = partition_stages(graph, policy=policy, bounds=bounds())
    splits = tuple(replace(make('SourceSplit'), source_id=source_id, split_id=sha(f'split-{i}')) for i in range(8))
    refined = _refine_source_partitions(graph, stages, splits, policy, bounds())
    assert len(refined[0].partitions) == refined[0].replica_target == 2
    assert [s.split_id for s in splits] == [sha(f'split-{i}') for i in range(8)]


def _three_stage_dataset():
    return data.from_items([{'n': n} for n in range(8)]).map(lambda row: {'n': row['n'] + 1})


def test_concurrent_profile_must_fit_before_any_upstream_launch(tmp_path, monkeypatch):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    launches = []
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', lambda *args: launches.append(args))
    with pytest.raises(DataflowError, match='resident budget'):
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=1, memory_bytes=128 * 1024):
            data.from_items([{'n': 1}]).map_async(lambda row: row, concurrency=4).take_all()
    assert launches == []


def test_real_source_split_coverage_with_fewer_active_workers(tmp_path, monkeypatch):
    from demiflow.execution import dataflow_api
    original = dataflow_api.resolve_runtime_policy
    seen = []
    def policy(*args, **kwargs):
        policy, bounds = original(*args, **kwargs)
        return replace(policy, default_partitions=8, max_replicas=8), bounds
    prepare = DataflowExecution.prepare
    def capture(execution):
        result = prepare(execution)
        seen.append(execution._physical)
        return result
    monkeypatch.setattr(dataflow_api, 'resolve_runtime_policy', policy)
    monkeypatch.setattr(DataflowExecution, 'prepare', capture)
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=2):
        rows = _three_stage_dataset().take_all()
    assert sorted(row['n'] for row in rows) == list(range(1, 9))
    assert len(seen[0].splits) == len({s.split_id for s in seen[0].splits}) == 8
    source_ids = {s.source_id for s in seen[0].splits}
    source_stages = [s for s in seen[0].stages if set(s.occurrences) & source_ids]
    assert len(source_stages[0].partitions) == 2


def test_resume_tightens_live_worker_envelope_without_rewriting_sealed_plan(tmp_path, monkeypatch):
    class Interrupted(Exception):
        pass
    original = DataflowExecution._retire_phase
    seen = []
    def stop(execution, image):
        seen.append(execution._physical)
        raise Interrupted()
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=2):
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_retire_phase', stop)
                _three_stage_dataset().take_all()
    frozen = seen[0]
    assert frozen.policy.max_active_workers == 2
    original_place = DataflowExecution._place_hosts
    def place(execution, pending):
        assert execution._physical.plan_id == frozen.plan_id
        assert execution._physical.policy.max_active_workers == 2
        assert execution._active_worker_limit == 1
        assert len(pending) <= 1 or not execution._started
        return original_place(execution, pending)
    with RunContext.resume(ref), data.local_execution(workers=1):
        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, '_place_hosts', place)
            assert _three_stage_dataset().take_all() == [{'n': n} for n in range(1, 9)]
