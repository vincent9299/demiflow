"""L017: actual v3 graph credit admission, sealing and historical protection.

The retired source-extraction/sample-resolver tests are replaced by the original
resolver, real Local workers and original sealed/commit facts. No mirror callee.
"""
from dataclasses import replace

import pytest

from demiflow.execution.run_context import RunContext
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.contracts.errors import ReservationError
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.planning.dataflow_graph_adapter import graph_credit
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from test_runtime_v3_graph_adapter import graph_environment, _spec


def _bounds(row=4096, batch=16384):
    bounds = resolve_runtime_policy({}, {'memory_bytes':64*1024**2, 'max_row_bytes':row,
        'max_batch_bytes':batch, 'max_state_bytes':65536}, provider='local')[1]
    return replace(bounds, max_row_bytes=row, max_batch_bytes=batch)


@pytest.mark.parametrize('row,batch,expected', [(1000,4096,4096),(10000,4096,10256)])
def test_bound_path_derives_inflight_bytes_and_explicit_credit(row,batch,expected):
    declaration = {'prefetch_slots':4, 'credit_budget':{'rows':4,'bytes':expected}}
    assert graph_credit(declaration, bounds=_bounds(row,batch)) == declaration['credit_budget']
    with pytest.raises(ReservationError, match='inflight byte grant'):
        graph_credit({**declaration,'credit_budget':{'rows':4,'bytes':expected+1}}, bounds=_bounds(row,batch))


def test_bound_path_without_explicit_credit_marks_default():
    assert graph_credit({'prefetch_slots':4}, bounds=_bounds()) == {'rows':4,'bytes':16384}


def test_default_credit_has_no_explicit_slot_grant():
    # No explicit slot grant is invented for the old unbound declaration. The
    # actual channel still intersects its default with the native byte ceiling.
    assert graph_credit({}, bounds=_bounds()) == {'rows':64,'bytes':65536}


@pytest.mark.parametrize('credit',[{'rows':64,'bytes':8192},{'rows':1,'bytes':16385}])
def test_explicit_credit_rejection_propagates_before_source_read(graph_environment,credit):
    directory=graph_environment
    spec=_spec(directory);spec.update(prefetch_slots=1,credit_budget=credit)
    with pytest.raises(ReservationError) as error:
        with RunContext.start(directory/'run',limits={'memory_bytes':64*1024**2,'max_row_bytes':4096,
                'max_batch_bytes':16384,'max_state_bytes':65536,'timeout_s':30}) as run:
            with run.action({'kind':'materialize','source':{'fixture':'credit-rejection'}}) as action:
                LocalDatasetExecutor(workers=3).materialize_graph(spec)
    assert error.value.code=='BUDGET_EXCEEDED'
    assert not (directory/'ancestor.jsonl').exists()
    assert not run.journal.execution_commits(action.action_id)
    assert run.journal.source_binding(action.action_id+'-dataflow-sealed') is None


def test_default_credit_tightens_to_admitted_grant():
    assert graph_credit({'prefetch_slots':1},bounds=_bounds())=={'rows':1,'bytes':16384}


def test_native_channels_use_both_resolved_credit_dimensions(graph_environment):
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    directory=graph_environment;spec=_spec(directory);spec['prefetch_slots']=1
    with RunContext.start(directory/'run',limits={'memory_bytes':64*1024**2,'max_row_bytes':4096,
            'max_batch_bytes':16384,'max_state_bytes':65536,'timeout_s':60}) as run:
        with run.action({'kind':'materialize','source':{'fixture':'one-credit'}}) as action:
            result=LocalDatasetExecutor(workers=3).materialize_graph(spec)
            assert result['target_rows']=={'left':3,'right':3}
            action.complete({'kind':'graph-materialized'})
        bundle=load_sealed_plan(run.directory,action.action_id,version='v3')
        assert bundle['physical'].policy.checkpoint_rows==run.limits['checkpoint_rows']>1
        router=next(node.occurrence_id for node in bundle['graph'].nodes if 'graph_route' in node.kernel.configuration)
        edges={edge.edge_id for edge in bundle['graph'].edges if router in (edge.producer,edge.consumer)}
        channels=[exchange for exchange in bundle['physical'].exchanges if exchange.edge_id in edges]
        assert len(channels)==3
        assert all(channel.credit_rows==1 and channel.credit_bytes==16384 for channel in channels)
        assert all(fact['status']=='acked' for fact in run.journal.execution_commits(action.action_id))


def test_recovery_byte_grant_guard_is_present(graph_environment):
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    directory=graph_environment;spec=_spec(directory);spec['prefetch_slots']=4
    class Interrupted(Exception):pass
    limits={'memory_bytes':64*1024**2,'max_row_bytes':4096,'max_batch_bytes':16384,
            'max_state_bytes':65536,'timeout_s':60}
    with pytest.raises(Interrupted):
        with RunContext.start(directory/'run',limits=limits) as run:
            ref=run.ref
            with run.action({'kind':'materialize','source':{'fixture':'credit-replay'}}) as action:
                LocalDatasetExecutor(workers=3).materialize_graph(spec)
                action.complete({'kind':'graph-materialized'})
            old=load_sealed_plan(run.directory,action.action_id,version='v3')['physical']
            facts=run.journal.execution_commits(action.action_id)
            raise Interrupted()
    from demiflow.execution.executors.graph import graph_action_request
    from demiflow.execution.seal.plan import validate_graph_replay
    with pytest.raises(DataflowError, match='sealed graph credit') as error:
        with RunContext.resume(ref) as run:
            with run.action({'kind':'materialize','source':{'fixture':'credit-replay'}}):
                request = graph_action_request(spec, LocalDatasetExecutor(workers=3))
                validate_graph_replay(request.action, request.source.declaration,
                    graph_credit(request.source.declaration, bounds=_bounds(batch=8192)))
    assert error.value.code=='BUDGET_EXCEEDED'
    assert run.journal.execution_commits('action-1')==facts
    assert load_sealed_plan(run.directory,'action-1',version='v3')['physical']==old
