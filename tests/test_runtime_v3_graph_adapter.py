"""Legacy graph declarations must execute their actual branches in one v3 action."""
import json
from pathlib import Path

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext


def _observe(row, *, directory, name, increment):
    import os
    from pathlib import Path
    value = {**row, 'x': row['x'] + increment}
    with (Path(directory) / (name + '.jsonl')).open('a') as output:
        output.write(__import__('json').dumps({**value, 'pid': os.getpid()}) + '\n')
    return value


def _plan(directory, name, increment):
    declaration = CallableSpec.create(_observe, fn_kwargs={
        'directory': str(directory), 'name': name, 'increment': increment})
    return LogicalPlan((MapOp(declaration, stream_options=MapStreamOptions(concurrency=1)),))


def _spec(directory):
    return {'source': ItemsSource(tuple({'x': x} for x in range(3))),
            'source_id': 'adapter-shared-source',
            'ancestor': {'plan': _plan(directory, 'ancestor', 10),
                         'logical_node_id': 'ancestor', 'occurrence_id': 'ancestor-0'},
            'targets': [{'plan': _plan(directory, name, increment),
                         'logical_node_id': name, 'occurrence_id': name + '-0', 'reader_id': name}
                        for name, increment in [('left', 100), ('right', 200)]],
            'routing': 'broadcast'}


def test_graph_legacy_location_cannot_override_executor_domain(graph_environment, monkeypatch):
    from demiflow.execution import graph
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    executor = LocalDatasetExecutor(resource_root=graph_environment)
    called = []
    monkeypatch.setattr(graph, '_run_graph', lambda spec, caller, **kwargs: called.append(caller))
    with pytest.raises(DataflowError, match='host placement'):
        graph.run_local_graph({}, host_id='other-host', _executor=executor)
    with pytest.raises(DataflowError, match='resource_root differs'):
        graph.run_local_graph({}, resource_root=graph_environment / 'other-domain', _executor=executor)
    assert not called
    graph.run_local_graph({}, resource_root=graph_environment, _executor=executor)
    assert called == [executor]


class _Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, row):
        self.count += 1
        return {**row, 'count': self.count, 'typed': (b'\x00', -0.0)}

    def snapshot_state(self):
        return {'count': self.count}

    def restore_state(self, state):
        self.count = state['count']


def _slow(row):
    import time
    time.sleep(.1)
    return row


def _expand_pair(row):
    return [row, dict(row)]


@pytest.fixture
def graph_environment(tmp_path, monkeypatch):
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])
    for key, name in [('DEMIFLOW_RESOURCE_ROOT', 'pool'),
                      ('DEMIFLOW_RESOURCE_HOME', 'resource-home'),
                      ('DEMIFLOW_FORGE_DOMAIN', 'forge-domain')]:
        monkeypatch.setenv(key, str(tmp_path / name))
    return tmp_path


def test_real_graph_runs_each_branch_and_shared_ancestor_once(graph_environment, monkeypatch):
    from demiflow.execution import dataflow
    directory = graph_environment
    original = dataflow.execute_action
    executions = []
    def observe(request, **kwargs):
        handle = original(request, **kwargs)
        executions.append(handle)
        return handle
    monkeypatch.setattr(dataflow, 'execute_action', observe)
    with RunContext.start(directory / 'run', limits={
            'memory_bytes': 128 * 1024**2, 'max_row_bytes': 4096,
            'max_batch_bytes': 16384, 'max_state_bytes': 65536,
            'timeout_s': 60}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'real-graph'}}) as action:
            try:
                result = LocalDatasetExecutor(workers=3, unified_graph=True).materialize_graph(_spec(directory))
                assert isinstance(result, dict), 'graph adapter returned the ancestor terminal instead of the graph result'
                assert result['egress_rows'] == 3
                assert result['target_rows'] == {'left': 3, 'right': 3}
                assert len(executions) == 1, 'all targets belong to one v3 execution'
                assert 0 < executions[0]._execution._peak_active_workers <= 3
                for name, values in [('ancestor', [10, 11, 12]), ('left', [110, 111, 112]),
                                     ('right', [210, 211, 212])]:
                    observed = [json.loads(line)['x'] for line in (directory / (name + '.jsonl')).read_text().splitlines()]
                    assert sorted(observed) == values
                facts = run.journal.execution_commits(action.action_id)
                assert facts and all(fact['status'] == 'acked' for fact in facts)
                with run.journal._connection() as database:
                    assert database.execute('SELECT count(*) FROM runtime_actions').fetchone()[0] == 1
                action.complete({'kind': 'graph-materialized', 'egress': result['egress_rows']})
            finally:
                # Also cleans up the broken baseline, whose adapter forgets close.
                for handle in executions:
                    handle.close()


def test_real_graph_preserves_caller_and_run_bounds(graph_environment, monkeypatch):
    from demiflow.execution import dataflow
    directory = graph_environment
    original = dataflow.execute_action
    executions, admitted = [], []
    def observe(request, **kwargs):
        admitted.append((kwargs['policy'], kwargs['bounds']))
        handle = original(request, **kwargs)
        executions.append(handle)
        return handle
    monkeypatch.setattr(dataflow, 'execute_action', observe)
    with RunContext.start(directory / 'run', limits={
            'memory_bytes': 64 * 1024**2, 'max_row_bytes': 4096,
            'max_batch_bytes': 16384, 'max_state_bytes': 65536,
            'timeout_s': 30}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'graph-bounds'}}) as action:
            try:
                LocalDatasetExecutor(workers=3, unified_graph=True).materialize_graph(_spec(directory))
                assert len(admitted) == 1
                policy, bounds = admitted[0]
                assert policy.max_active_workers == 3
                assert bounds.max_resident_bytes <= 64 * 1024**2
                assert bounds.max_row_bytes <= 4096
                assert bounds.max_batch_bytes <= 16384
                assert bounds.timeout_ms <= 30_000
                action.complete({'kind': 'graph-materialized'})
            finally:
                for handle in executions:
                    handle.close()


@pytest.mark.parametrize('routing', ['broadcast', 'hash', 'rebalance', 'target'])
def test_actual_graph_routes_reach_distinct_programs(graph_environment, monkeypatch, routing):
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.dataflow_terminals import materialized_handle
    from demiflow.execution.sealed_plans import load_sealed_plan
    directory = graph_environment
    spec = _spec(directory)
    spec['source'] = ItemsSource(tuple({'x': i, 'group': 'a' if i % 2 == 0 else 'b'} for i in range(6)))
    spec.update(routing=routing, key_fields=['group'], target_field='group',
                target_classes={'a': ['left'], 'b': ['right']})
    outputs, summaries = [], []
    original = DataflowExecution.finish
    def observe(execution):
        result = original(execution)
        stored = materialized_handle(result, execution=execution)
        try:
            outputs.extend(stored.rows())
        finally:
            stored.release()
        assert execution._peak_active_workers <= 3
        summaries.append(result)
        return result
    monkeypatch.setattr(DataflowExecution, 'finish', observe)
    with RunContext.start(directory / 'run', limits={
            'memory_bytes': 128 * 1024**2, 'max_row_bytes': 4096,
            'max_batch_bytes': 16384, 'max_state_bytes': 2 * 1024**2, 'timeout_s': 90}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'graph-route', 'route': routing}}) as action:
            result = LocalDatasetExecutor(workers=3, unified_graph=True).materialize_graph(spec)
            assert result['egress_rows'] == 6
            action.complete({'kind': 'graph-materialized'})
        sealed = load_sealed_plan(run.directory, action.action_id, version='v3')
        router = next(node.occurrence_id for node in sealed['graph'].nodes if 'graph_route' in node.kernel.configuration)
        incoming = next(edge for edge in sealed['graph'].edges if edge.consumer == router)
        exchange = next(edge for edge in sealed['physical'].exchanges if edge.edge_id == incoming.edge_id)
        assert exchange.routing == routing and exchange.consumer_partitions == ('left', 'right')
        assert sealed['physical'].policy.max_active_workers == 3
    grouped = {reader: [item['row'] for item in outputs if item['reader_id'] == reader] for reader in ('left', 'right')}
    assert result['target_rows'] == {reader: len(rows) for reader, rows in grouped.items()}
    originals = {reader: [row['x'] - (110 if reader == 'left' else 210) for row in rows] for reader, rows in grouped.items()}
    if routing == 'broadcast':
        assert all(sorted(values) == list(range(6)) for values in originals.values())
    else:
        assert sorted(originals['left'] + originals['right']) == list(range(6))
        if routing in ('rebalance', 'target'):
            assert sorted(originals['left']) == [0, 2, 4]
            assert sorted(originals['right']) == [1, 3, 5]
        else:
            destinations = {}
            for reader, rows in grouped.items():
                for row in rows:
                    assert destinations.setdefault(row['group'], reader) == reader
    assert len((directory / 'ancestor.jsonl').read_text().splitlines()) == 6
    assert len(summaries) == 1


@pytest.mark.parametrize('routing', ['broadcast', 'hash', 'rebalance', 'target'])
def test_actual_graph_recovers_native_routes_and_target_identities(runtime_v3_fixture, monkeypatch, routing):
    from demiflow.data.api import _current_executor
    from demiflow.execution.run_context import current_run
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.dataflow_terminals import materialized_handle
    from runtime_v3_route_recovery_scenarios import _recover
    fx = runtime_v3_fixture
    directory = fx.root / 'graph-observations'
    directory.mkdir()
    summaries, published = [], []
    original = DataflowExecution.finish
    def observe(execution):
        terminal = original(execution)
        assert execution._peak_active_workers <= 3
        stored = materialized_handle(terminal, execution=execution)
        try:
            published[:] = stored.rows()
        finally:
            stored.release()
        return terminal
    monkeypatch.setattr(DataflowExecution, 'finish', observe)
    class GraphAction:
        def take_all(self):
            spec = _spec(directory)
            spec['source'] = ItemsSource(tuple({'x': i, 'group': 'a' if i % 2 == 0 else 'b'} for i in range(8)))
            spec.update(routing=routing, key_fields=['group'], target_field='group',
                        target_classes={'a': ['left'], 'b': ['right']})
            with current_run().action({'kind': 'materialize', 'source': {'fixture': 'graph-native-recovery', 'route': routing}}) as action:
                summary = _current_executor.get().materialize_graph(spec)
                summaries.append(summary)
                action.complete({'kind': 'graph-materialized', 'rows': sum(summary['target_rows'].values())})
            return list(published)
    prepared = {}
    prepare = DataflowExecution.prepare
    def prepare_observed(execution):
        value = prepare(execution)
        router = next(node.occurrence_id for node in execution._graph.nodes if 'graph_route' in node.kernel.configuration)
        edge = next(edge for edge in execution._graph.edges if edge.consumer == router)
        prepared['execution'] = execution
        prepared['channel'] = next(item.channel_id for item in execution._physical.exchanges if item.edge_id == edge.edge_id)
        return value
    monkeypatch.setattr(DataflowExecution, 'prepare', prepare_observed)
    def eligible(facts):
        execution = prepared['execution']
        active = any(key in execution._current_worker_observations and key not in execution._done
                     for key in execution._endpoints)
        return active and any(entry['state']['channel_id'] == prepared['channel']
                              and entry['state']['routing_ordinal'] > 0 for entry in facts['channels'])
    result = _recover(fx, 'graph-adapter-' + routing, GraphAction, workers=3, eligible=eligible, parked=True)
    assert len(summaries) == 1 and summaries[0]['egress_rows'] == 8
    rows = result['rows']
    by_reader = {reader: [item['row']['x'] - (110 if reader == 'left' else 210)
                         for item in rows if item['reader_id'] == reader] for reader in ('left', 'right')}
    if routing == 'broadcast':
        assert all(sorted(values) == list(range(8)) for values in by_reader.values())
    else:
        assert sorted(by_reader['left'] + by_reader['right']) == list(range(8))
        if routing in ('rebalance', 'target'):
            assert sorted(by_reader['left']) == [0, 2, 4, 6]
            assert sorted(by_reader['right']) == [1, 3, 5, 7]
    assert summaries[0]['target_rows'] == {reader: len(values) for reader, values in by_reader.items()}
    assert result['physical']['dag_id'] == summaries[0]['dag_id']
    assert result['physical']['plan_id'] == summaries[0]['plan_id']
    assert result['verified_suffix']
    assert result['duplicate_delivery'][0]['source'] == 'native durable entries'
    owner_attempt = {entry['payload']['worker_process_id']: entry['attempt']
                     for entry in result['events'] if entry['kind'] == 'ready'}
    seen = [json.loads(line) for line in (directory / 'ancestor.jsonl').read_text().splitlines()]
    from collections import Counter
    # Reprocessing an uncommitted suffix across attempts is legal. Sharing
    # must not execute the ancestor once per target within a single attempt.
    assert all(count == 1 for count in Counter((owner_attempt[row['pid']], row['x']) for row in seen).values())
    fx._save_evidence('graph-adapter-' + routing, result)


@pytest.mark.parametrize('mode,empty', [('memory_incremental', False), ('finite_sealed', False), ('finite_sealed', True)])
def test_reused_target_actor_is_independent_and_final_replay_starts_no_worker(graph_environment, monkeypatch, mode, empty):
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.dataflow_terminals import materialized_handle
    directory = graph_environment
    results, executions = [], []
    original = DataflowExecution.finish
    def observe(execution):
        terminal = original(execution)
        executions.append(execution)
        stored = materialized_handle(terminal, execution=execution)
        try:
            results.append(list(stored.rows()))
        finally:
            stored.release()
        return terminal
    monkeypatch.setattr(DataflowExecution, 'finish', observe)
    def specification():
        spec = _spec(directory)
        spec['exchange_mode'] = mode
        if empty:
            spec['source'] = ItemsSource(())
        shared_plan = LogicalPlan((MapOp(CallableSpec.create(_Counter),
            stream_options=MapStreamOptions(concurrency=1)),))
        for target in spec['targets']:
            target['plan'] = shared_plan
        return spec
    declaration = {'kind': 'materialize', 'source': {'fixture': 'independent-graph-actors', 'mode': mode}}
    class StopAfterResult(Exception):
        pass
    with pytest.raises(StopAfterResult):
        with RunContext.start(directory / 'run', limits={'memory_bytes': 128 * 1024**2,
                'max_row_bytes': 4096, 'max_batch_bytes': 16384, 'max_state_bytes': 2 * 1024**2,
                'timeout_s': 120}) as run:
            reference = run.ref
            with run.action(declaration) as action:
                summary = LocalDatasetExecutor(workers=3).materialize_graph(specification())
                action.complete({'kind': 'graph-materialized'})
            commits = run.journal.execution_commits(action.action_id)
            assert commits[-1]['status'] == 'acked'
            raise StopAfterResult()
    count = 0 if empty else 3
    assert summary['egress_rows'] == count
    assert summary['target_rows'] == {'left': count, 'right': count}
    for reader in ('left', 'right'):
        values = [value['row'] for value in results[0] if value['reader_id'] == reader]
        assert sorted(value['count'] for value in values) == list(range(1, count + 1))
        import math
        assert all(type(value['typed']) is tuple and value['typed'][0] == b'\x00'
                   and math.copysign(1, value['typed'][1]) == -1 for value in values)
    with RunContext.resume(reference) as resumed:
        with resumed.action(declaration) as action:
            replayed = LocalDatasetExecutor(workers=3).materialize_graph(specification())
            action.complete({'kind': 'graph-materialized'})
        assert resumed.journal.execution_commits(action.action_id) == commits
    assert results[1] == results[0]
    assert replayed['target_rows'] == summary['target_rows']
    assert replayed['dag_id'] == summary['dag_id'] and replayed['plan_id'] == summary['plan_id']
    assert replayed['workers'] == 0 and executions[-1]._endpoints == {}
    assert all(value is None for value in replayed['final_arrival'].values())
    assert all(execution._peak_active_workers <= 3 for execution in executions)
    if not empty:
        assert len((directory / 'ancestor.jsonl').read_text().splitlines()) == 3


@pytest.mark.parametrize('expand', [False, True])
def test_mixed_residency_waits_for_memory_peer_but_rejects_durable_expansion(graph_environment, monkeypatch, expand):
    from demiflow.data.plan import FlatMapOp
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.sealed_plans import load_sealed_plan
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    commits = []
    commit = DataflowCheckpointCoordinator.commit
    def observed(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        commits.append(image)
        return image
    monkeypatch.setattr(DataflowCheckpointCoordinator, 'commit', observed)
    directory = graph_environment
    spec = _spec(directory)
    if expand:
        spec['prefetch_slots'] = 1
    else:
        # The source's one-row memory peer genuinely holds credit while this
        # slow ancestor works. The absent graph egress retains its original
        # larger durable grant, so this is legal backpressure, not expansion.
        spec['ancestor']['plan'] = LogicalPlan((MapOp(CallableSpec.create(_slow),
            stream_options=MapStreamOptions(concurrency=1)),))
    spec['targets'][0]['plan'] = LogicalPlan((FlatMapOp(CallableSpec.create(_expand_pair)),)) if expand else LogicalPlan((
        MapOp(CallableSpec.create(_slow), stream_options=MapStreamOptions(concurrency=1)),))
    def execute():
        with RunContext.start(directory/'run', limits={'memory_bytes':128*1024**2,
                'max_row_bytes':4096,'max_batch_bytes':16384,'max_state_bytes':2*1024**2,
                'timeout_s':60,'checkpoint_interval_ms':60000}) as run:
            with run.action({'kind':'materialize','source':{'fixture':'mixed-graph-residency'}}) as action:
                result=LocalDatasetExecutor(workers=3).materialize_graph(spec)
                action.complete({'kind':'graph-materialized'})
            return result, load_sealed_plan(run.directory,action.action_id,version='v3')
    if expand:
        with pytest.raises(DataflowError, match='one operator invocation exceeds the parked durable credit window'):
            execute()
    else:
        result, bundle = execute()
        assert result['target_rows']=={'left':3,'right':3}
        assert {edge.mode for edge in bundle['physical'].exchanges} >= {'memory_incremental','durable_incremental'}
        assert bundle['physical'].policy.max_active_workers==3
        assert commits[0].source_vector[0].next_sequence==3
        assert commits[0].source_vector[0].end_of_input
