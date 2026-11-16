"""Compatibility declarations stay on one real, budgeted dataflow action."""
from contextlib import closing
from types import SimpleNamespace

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.dataset_native import execute, _cast_sink_batch
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext

MiB = 2**20


def placement(*, cpu=2000, memory=32*MiB, rows=2, bytes=MiB, output=None):
    budget = {'inflight_rows': rows, 'inflight_bytes': bytes}
    if output is not None:
        budget['output_bytes'] = output
    return SimpleNamespace(profile={'limits': {'cpu_millis': cpu, 'memory_bytes': memory}},
                           stage_ids=('native',), stage_limits={'native': budget})


def source(tmp_path, values=range(8)):
    uri = str(tmp_path/'source.lance')
    table = pa.table({'id': pa.array(list(values), type=pa.int64()), 'extra': ['x']*len(values)})
    lance.write_dataset(table, uri)
    return uri


def consume(session, uri, **kwargs):
    ds = data.read_lance(uri, version=1).select_columns(['id'])
    return execute(session._executor, ds._source, ds._plan, arrow=True, **kwargs)


@pytest.mark.parametrize('cpu', [1000, 2000])
def test_real_cast_batches_scope_and_smaller_run_intersect(tmp_path, monkeypatch, cpu):
    from demiflow.execution.resource_state import RunResourceState
    physicals, charges = [], []
    original, charged = DataflowExecution.prepare, RunResourceState._charged
    def prepare(execution):
        result = original(execution)
        physicals.append((execution._graph, execution._physical))
        return result
    def charge(state, db, scope):
        result = charged(state, db, scope)
        charges.append(dict(result))
        assert result['cpu_millis'] <= cpu
        assert result['memory_bytes'] <= 16*MiB
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', prepare)
    monkeypatch.setattr(RunResourceState, '_charged', charge)
    uri = source(tmp_path)
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False)], metadata={b'app': b'native-bound'})
    with RunContext.start(tmp_path/'run', limits={'memory_bytes': 16*MiB, 'batch_rows': 1,
            'max_row_bytes': 8192, 'max_batch_bytes': 16384}) as run, \
            data.local_execution(workers=3, worker_mode='thread') as session:
        original_options = session._executor._local_kernel
        batches = list(consume(session, uri, execution=placement(cpu=cpu), sink_schema=schema))
        assert session._executor._local_kernel is original_options
        assert run.position == 1
        record = run.journal.execution_action('action-1')
        assert record['result'] is not None
        assert run.journal.execution_action('action-2') is None
        assert all(c['status'] == 'acked' for c in run.journal.execution_commits('action-1'))
    assert len(batches) == 8 and all(isinstance(batch, pa.RecordBatch) and len(batch) == 1 for batch in batches)
    assert pa.Table.from_batches(batches).equals(pa.Table.from_pylist([{'id':i} for i in range(8)], schema=schema), check_metadata=True)
    graph, physical = physicals[0]
    assert any(node.kernel.kind == 'MapBatchesOp' for node in graph.nodes)
    assert physical.bounds.max_resident_bytes == 16*MiB
    assert physical.bounds.max_row_bytes == 8192 and physical.bounds.max_batch_bytes == 16384
    assert physical.policy.native_limits['batch_rows'] == 1
    assert all(edge.credit_rows <= 2 and edge.credit_bytes <= MiB for edge in physical.exchanges)
    assert charges and 1000 <= max(c['cpu_millis'] for c in charges) <= cpu
    profiles = {stage.stage_id: stage for stage in physical.stages}
    assert all(sum(profiles[key].profile.requests['cpu_millis'] * profiles[key].replica_target
                   for key in group) <= cpu for group in physical.residency.simultaneous_groups)


@pytest.mark.parametrize('invalid', ['cpu', 'schema'])
def test_invalid_declaration_rejected_without_worker_or_commit(tmp_path, monkeypatch, invalid):
    launches = []
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', lambda *args: launches.append(args))
    uri = source(tmp_path)
    with RunContext.start(tmp_path/'run') as run, data.local_execution(worker_mode='thread') as session:
        kwargs = {'execution': placement(cpu=999)} if invalid == 'cpu' else {'sink_schema': {'id': 'int32'}}
        with pytest.raises(DataflowError, match='whole CPU|pyarrow.Schema'):
            list(consume(session, uri, **kwargs))
        assert run.position == 0
        assert not run.journal.execution_commits('action-1')
    assert not launches


def test_row_budget_failure_preserves_failure_and_does_not_publish(tmp_path):
    uri = source(tmp_path)
    with pytest.raises(DataflowError, match='budget|bytes|metadata|bounds'):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(worker_mode='thread') as session:
            try:
                list(consume(session, uri, execution=placement(bytes=32)))
            finally:
                assert not run.journal.execution_commits('action-1')


@pytest.mark.parametrize('fault', ['overflow', 'columns'])
def test_safe_cast_failure_has_no_terminal_publication(tmp_path, fault):
    uri = source(tmp_path, [128])
    schema = pa.schema([('id' if fault == 'overflow' else 'different', pa.int8())])
    with pytest.raises((pa.ArrowInvalid, RecoveryContractError, DataflowError), match='range|fields|cast'):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(worker_mode='thread') as session:
            try:
                list(consume(session, uri, execution=placement(), sink_schema=schema))
            finally:
                commits = run.journal.execution_commits('action-1')
                # Parking may commit an empty source-progress cut before the
                # cast. It must never publish a row or report final success.
                assert all(c['receipts']['iter_batches']['data']['rows'] == 0
                           for c in commits if c['status'] == 'acked')
                assert all(not c['prepared']['sinks']['iter_batches']['manifest']['final']
                           for c in commits if c['status'] == 'acked')
                assert run.journal.execution_action('action-1')['result'] is None


def test_completed_schema_replays_same_action_and_changed_schema_is_refused(tmp_path, monkeypatch):
    class Interrupted(Exception):
        pass
    uri = source(tmp_path)
    schema = pa.schema([('id', pa.int32())])
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(worker_mode='thread') as session:
            ref = run.ref
            expected = list(consume(session, uri, execution=placement(), sink_schema=schema))
            commits = run.journal.execution_commits('action-1')
            assert commits[-1]['status'] == 'acked'
            raise Interrupted()
    launches = []
    original = DataflowExecution._prepare_launch
    def launch(*args, **kwargs):
        launches.append(args)
        return original(*args, **kwargs)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', launch)
    with pytest.raises((RecoveryContractError, DataflowError), match='differ|identity|changed'):
        with RunContext.resume(ref), data.local_execution(worker_mode='thread') as session:
            list(consume(session, uri, execution=placement(), sink_schema=pa.schema([('id', pa.int16())])))
    assert not launches
    with RunContext.resume(ref) as run, data.local_execution(worker_mode='thread') as session:
        actual = list(consume(session, uri, execution=placement(), sink_schema=schema))
        assert run.position == 1
        assert run.journal.execution_commits('action-1') == commits
    assert not launches
    assert pa.Table.from_batches(actual).equals(pa.Table.from_batches(expected), check_metadata=True)


def test_close_confirms_delivered_prefix_and_releases_owned_scope(tmp_path):
    uri = source(tmp_path, range(32))
    with RunContext.start(tmp_path/'run') as run, data.local_execution(worker_mode='thread') as session:
        with closing(consume(session, uri, execution=placement(), sink_schema=pa.schema([('id', pa.int32())]))) as batches:
            first = next(batches)
            assert first.num_rows == 2 and first.schema.field('id').type == pa.int32()
        assert run.journal.execution_action('action-1')['result']['delivery'] == 'closed'
        commits = run.journal.execution_commits('action-1')
        assert commits and all(c['status'] == 'acked' for c in commits)
        assert session._executor._last_metadata.diagnostics['state'] in {'closed', 'succeeded'}


def test_cast_admits_input_and_new_output_together_before_allocation(monkeypatch):
    from demiflow.resources.capacity import allocation_envelope
    from demiflow.execution.arrow_batch_sink import _cast_allocation
    from demiflow.execution.dataflow_schema import encode
    from demiflow.execution.dataflow_api import _PREFLIGHT_BOUNDS
    table = pa.table({'id': pa.array(range(64), type=pa.int64())})
    schema = pa.schema([('id', pa.float64())])
    encoded = encode(schema, bounds=_PREFLIGHT_BOUNDS)
    total = table.get_total_buffer_size() + _cast_allocation(table, schema)
    assert table.get_total_buffer_size() < total-1
    with allocation_envelope({'allocation_id': 'test-native', 'spec_hash': '0'*64,
            'resource_directory': '/tmp/demiflow-native-unit-envelope',
            'cpu_millis': 1000, 'memory_bytes': total-1}):
        with pytest.raises(DataflowError, match='input and cast output'):
            _cast_sink_batch(table, schema_bytes=encoded, max_bytes=total-1)
    with allocation_envelope({'allocation_id': 'test-native', 'spec_hash': '0'*64,
            'resource_directory': '/tmp/demiflow-native-unit-envelope',
            'cpu_millis': 1000, 'memory_bytes': total}):
        actual = _cast_sink_batch(table, schema_bytes=encoded, max_bytes=total)
    assert actual.equals(table.cast(schema))


def test_real_legacy_caller_reuses_one_action_and_both_seals(tmp_path, monkeypatch):
    from demiflow.execution.checked import action
    from demiflow.execution.capabilities import ActionDecision
    from demiflow.execution.native_plan import bind_native_execution, execute_native_plan
    from demiflow.execution.run_context import current_action
    class Interrupted(Exception):
        pass
    uri = source(tmp_path)
    schema = pa.schema([('id', pa.int32())], metadata={b'legacy': b'bound'})
    decision = ActionDecision('iter_batches', 'datafusion', 'native_relational', 'final_commit')
    profile = {'requests': {'cpu_millis': 2000, 'memory_bytes': 32*MiB}}
    def execute_caller(session, target_schema=schema, changed_limits=False):
        from dataclasses import replace
        ds = data.read_lance(uri, version=1).select_columns(['id'])
        with action('iter_batches', ds._source, ds._plan, arguments={'batch_format': 'pyarrow'}):
            active = current_action()
            layout, owner = bind_native_execution(active, ds._source, ds._plan,
                                                  decision=decision, profile=profile)
            if changed_limits:
                altered = replace(owner, profile={'requests': {'cpu_millis': 1000, 'memory_bytes': 32*MiB}})
                layout = replace(layout, executions=(altered,))
            return list(execute_native_plan(session._executor, ds._source, ds._plan,
                layout=layout, execution_id=owner.execution_id, arrow=True, sink_schema=target_schema))
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'legacy-run', limits={'memory_bytes':32*MiB,
                'batch_rows':2, 'max_row_bytes':8192, 'max_batch_bytes':16384}) as run, \
                data.local_execution(workers=2, worker_mode='thread') as session:
            ref = run.ref
            expected = execute_caller(session)
            old_seal = run.journal.source_binding('action-1-sealed-plan')
            new_seal = run.journal.source_binding('action-1-dataflow-sealed')
            commits = run.journal.execution_commits('action-1')
            assert old_seal and new_seal and commits[-1]['status'] == 'acked'
            assert run.position == 1
            assert run.journal.execution_action('action-2') is None
            raise Interrupted()
    launches = []
    original = DataflowExecution._prepare_launch
    def launch(*args, **kwargs):
        launches.append(args)
        return original(*args, **kwargs)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', launch)
    for changed in ({'target_schema': pa.schema([('id', pa.int16())])}, {'changed_limits': True}):
        with pytest.raises(DataflowError, match='limits or schema differ'):
            with RunContext.resume(ref) as run, data.local_execution(workers=2, worker_mode='thread') as session:
                execute_caller(session, **changed)
                assert False, 'changed adapter must fail before result replay'
        assert not launches
    with RunContext.resume(ref) as run, data.local_execution(workers=2, worker_mode='thread') as session:
        actual = execute_caller(session)
        assert run.position == 1
        assert run.journal.source_binding('action-1-sealed-plan') == old_seal
        assert run.journal.source_binding('action-1-dataflow-sealed') == new_seal
        assert run.journal.execution_commits('action-1') == commits
    assert not launches
    assert all(isinstance(batch, pa.RecordBatch) and len(batch) <= 2 for batch in actual)
    assert sum(map(len, actual)) == 8
    assert pa.Table.from_batches(actual).equals(pa.Table.from_batches(expected), check_metadata=True)
    assert all(batch.schema.equals(schema, check_metadata=True) for batch in actual)


def test_unrelated_active_action_cannot_borrow_native_execution(tmp_path, monkeypatch):
    from demiflow.execution.checked import action
    uri = source(tmp_path)
    launches = []
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', lambda *args: launches.append(args))
    with pytest.raises(DataflowError, match='source/plan differ'):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(worker_mode='thread') as session:
            ds = data.read_lance(uri, version=1)
            try:
                with action('iter_batches', ds._source, ds._plan):
                    list(consume(session, uri, execution=placement()))
            finally:
                assert not launches
                assert not run.journal.execution_commits('action-1')


@pytest.mark.parametrize('limit', [0, 1, 3])
def test_native_take_uses_original_terminal_and_preserves_borrowed_action(tmp_path, monkeypatch, limit):
    from demiflow.execution.checked import action
    seen = []
    prepare = DataflowExecution.prepare
    def record(execution):
        result = prepare(execution)
        seen.append((execution._request.kind, dict(execution._request.arguments)))
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', record)
    uri = source(tmp_path)
    with RunContext.start(tmp_path/'run') as run, data.local_execution(worker_mode='thread') as session:
        ds = data.read_lance(uri, version=1).select_columns(['id'])
        with action('take', ds._source, ds._plan, arguments={'limit': limit}) as outer:
            actual = list(execute(session._executor, ds._source, ds._plan, execution=placement(), limit=limit))
            assert outer.execution.record['result'] is None
            assert run.position == 1
        assert actual == [{'id':i} for i in range(limit)]
        assert run.journal.execution_action('action-2') is None
    assert seen == ([] if limit == 0 else [('take', {
        'limit': limit, 'native_delivery_limit': limit,
        'native_execution_limits': {'cpu_millis':2000, 'memory_bytes':32*MiB,
                                   'inflight_rows':2, 'inflight_bytes':MiB}})])
