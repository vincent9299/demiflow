"""L018: real Ray graph credit, original sealed replay and bounded execution.

The old AST-only checks and permanent skip are retired. These tests need the
Ray environment; when selected there every assertion runs against real workers
or that real run's original sealed facts. They share one immutable fixture run.
"""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.sealed_plans import load_sealed_plan
from demiflow.planning.dataflow_graph_adapter import graph_credit, validate_graph_replay
from test_runtime_v3_graph_adapter import _spec


@pytest.fixture(scope='module')
def ray_credit_case():
    ray = pytest.importorskip('ray', reason='Real Ray environment is required; not a successful Ray gate')
    import tempfile
    from unittest.mock import patch
    from demiflow.data.api import _current_executor
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.dataflow_terminals import materialized_handle
    from demiflow.execution.dataflow_api import resolve_action_runtime
    from demiflow.execution.graph import _graph_action_request
    from runtime_v3_residency_scenarios import _provider
    from runtime_v3_support import RuntimeV3Fixture, make_scope, DEFAULT_BOUNDS, _RayDataBlocker
    owned = not ray.is_initialized()
    if owned:
        ray.init(address='local', num_cpus=3, num_gpus=0, include_dashboard=False,
            object_store_memory=100*1024**2,
            _temp_dir=tempfile.mkdtemp(prefix='demiflow-l018-ray-', dir='/tmp'))
    fx = RuntimeV3Fixture(make_scope('demiflow-l018-'), provider='ray_core', bounds=DEFAULT_BOUNDS)
    results, executions = [], []
    finish = DataflowExecution.finish
    def observed(execution):
        terminal = finish(execution)
        assert execution._peak_active_workers <= 3
        executions.append(execution)
        handle = materialized_handle(terminal, execution=execution)
        try:
            results.append(list(handle.rows()))
        finally:
            handle.release()
        return terminal
    spec = _spec(fx.root)
    spec['prefetch_slots'] = 1
    declaration = {'kind':'materialize','source':{'fixture':'ray-graph-original-credit'}}
    class InterruptedAfterResult(Exception):
        pass
    try:
        with _provider(fx, workers=3, fusion=False), _RayDataBlocker() as blocker, \
                patch.object(DataflowExecution, 'finish', observed):
            with pytest.raises(InterruptedAfterResult):
                with RunContext.start(fx.journal_dir/'run', limits={'memory_bytes':128*1024**2,
                        'max_row_bytes':4096,'max_batch_bytes':16384,'max_state_bytes':2*1024**2,
                        'timeout_s':120}) as run:
                    reference = run.ref
                    with run.action(declaration) as action:
                        request = _graph_action_request(spec, _current_executor.get())
                        _, bounds = resolve_action_runtime(request)
                        summary = _current_executor.get().materialize_graph(spec)
                        action.complete({'kind':'graph-materialized'})
                    commits = run.journal.execution_commits(action.action_id)
                    checkpoint_rows = run.limits['checkpoint_rows']
                    bundle = load_sealed_plan(run.directory, action.action_id, version='v3')
                    raise InterruptedAfterResult()
            router = next(node.occurrence_id for node in bundle['graph'].nodes if 'graph_route' in node.kernel.configuration)
            selected = {edge.edge_id for edge in bundle['graph'].edges if router in (edge.producer, edge.consumer)}
            channels = tuple(edge for edge in bundle['physical'].exchanges if edge.edge_id in selected)
            assert len(channels)==3 and not blocker.attempts
            yield SimpleNamespace(fx=fx, spec=spec, declaration=declaration, reference=reference,
                summary=summary, bundle=bundle, commits=commits, bounds=bounds,
                channels=channels, results=results, executions=executions, blocker=blocker,
                checkpoint_rows=checkpoint_rows)
            assert not blocker.attempts
            fx._save_evidence('L018-real-ray-graph', {'dag_id':summary['dag_id'],
                'plan_id':summary['plan_id'],'target_rows':summary['target_rows'],
                'credit':[(edge.credit_rows,edge.credit_bytes) for edge in channels],
                'commits':commits,'rows':results,'workers':[value._observed_worker_count for value in executions],
                'peak_active_workers':[value._peak_active_workers for value in executions],
                'ray_data_import_attempts':blocker.attempts})
    finally:
        try:
            fx.close()
        finally:
            if owned:
                ray.shutdown()


def test_ray_prefetch_wiring_identical_to_local(ray_credit_case):
    case=ray_credit_case
    expected=graph_credit(case.spec,bounds=case.bounds)
    assert expected=={'rows':1,'bytes':16384}
    assert {(edge.credit_rows,edge.credit_bytes) for edge in case.channels}=={(expected['rows'],expected['bytes'])}


def test_ray_recovery_guard_identical_to_local(ray_credit_case):
    from demiflow.data.api import _current_executor
    from demiflow.execution.graph import _graph_action_request
    case=ray_credit_case
    with pytest.raises(DataflowError,match='sealed graph credit') as error:
        with RunContext.resume(case.reference) as run:
            with run.action(case.declaration):
                request=_graph_action_request(case.spec,_current_executor.get())
                validate_graph_replay(request,bounds=replace(case.bounds,max_batch_bytes=8192))
    assert error.value.code=='BUDGET_EXCEEDED'
    assert run.journal.execution_commits('action-1')==case.commits
    assert load_sealed_plan(run.directory,'action-1',version='v3')['physical']==case.bundle['physical']
    assert len(case.executions)==1


def test_ray_wiring_consumes_both_resolved_fields(ray_credit_case):
    case=ray_credit_case
    assert all(edge.credit_rows==1 and edge.credit_bytes==16384 for edge in case.channels)
    assert case.bundle['physical'].policy.provider=='ray_core'
    assert case.bundle['physical'].policy.max_active_workers==3
    assert case.bundle['physical'].policy.checkpoint_rows==case.checkpoint_rows>1
    assert case.commits and all(commit['status']=='acked' for commit in case.commits)
    assert case.summary['target_rows']=={'left':3,'right':3}


def test_ray_end_to_end_execution_unverified(ray_credit_case):
    """Historical nodeid retained; this now executes and verifies actual replay."""
    from demiflow.data.api import _current_executor
    case=ray_credit_case
    with RunContext.resume(case.reference) as run:
        with run.action(case.declaration) as action:
            replayed=_current_executor.get().materialize_graph(case.spec)
            action.complete({'kind':'graph-materialized'})
        assert run.journal.execution_commits('action-1')==case.commits
    assert replayed['workers']==0 and case.executions[-1]._endpoints=={}
    assert replayed['target_rows']==case.summary['target_rows']
    assert replayed['dag_id']==case.summary['dag_id'] and replayed['plan_id']==case.summary['plan_id']
    assert case.results[1]==case.results[0] and len(case.results[0])==6
    assert len((case.fx.root/'ancestor.jsonl').read_text().splitlines())==3
    assert not case.blocker.attempts
