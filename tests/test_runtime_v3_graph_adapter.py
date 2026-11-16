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
    value = {'x': row['x'] + increment}
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
                result = LocalDatasetExecutor(workers=16, unified_graph=True).materialize_graph(_spec(directory))
                assert isinstance(result, dict), 'graph adapter returned the ancestor terminal instead of the graph result'
                assert result['egress_rows'] == 3
                assert result['target_rows'] == {'left': 3, 'right': 3}
                assert len(executions) == 1, 'all targets belong to one v3 execution'
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
                LocalDatasetExecutor(workers=16, unified_graph=True).materialize_graph(_spec(directory))
                assert len(admitted) == 1
                policy, bounds = admitted[0]
                assert policy.max_active_workers == 16
                assert bounds.max_resident_bytes <= 64 * 1024**2
                assert bounds.max_row_bytes <= 4096
                assert bounds.max_batch_bytes <= 16384
                assert bounds.timeout_ms <= 30_000
                action.complete({'kind': 'graph-materialized'})
            finally:
                for handle in executions:
                    handle.close()
