"""Small worker admission keeps original state, recovery and physical grants."""
import pytest
from demiflow import data


def test_native_suffix_keeps_original_deduplication_state(tmp_path, monkeypatch):
    import demiflow.planning.dataflow_place as place
    original=place._select_budgeted_residency
    seen=[]
    def inspect(graph,snapshots,stages,exchanges,**kwargs):
        dedup=next(n for n in graph.nodes if n.kernel.kind=='DeduplicateOp')
        assert dedup.kernel.effect=='journaled' and dedup.kernel.state=='nonrecoverable'
        stage=next(s for s in stages if dedup.occurrence_id in s.occurrences)
        assert len(stage.occurrences)>1
        assert stage.state_bytes >= kwargs['bounds'].max_state_bytes
        seen.append(True)
        return original(graph,snapshots,stages,exchanges,**kwargs)
    monkeypatch.setattr(place,'_select_budgeted_residency',inspect)
    def fail(row):
        raise RuntimeError('downstream failed')
    with pytest.raises(RuntimeError,match='downstream failed'):
        data.from_items([{'id':'a','sha':'1'}]).deduplicate('sha',path=tmp_path/'keys',key='id').map(fail).run_stream()
    assert seen


def test_single_worker_does_not_colocate_async_with_arbitrary_blocking_source(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    def source():
        (tmp_path/'opened').touch()
        yield {'x':1}
    async def call(row):
        return row
    with pytest.raises(DataflowError) as failure:
        with data.local_execution(workers=1,partitions=1):
            data.from_iter(source).map_async(call).take_all()
    assert failure.value.code=='RESOURCE_INFEASIBLE'
    assert not (tmp_path/'opened').exists()


from test_dataflow_shared_actor import owned_ray


def test_one_worker_real_ray_shared_actor_recovery(tmp_path, monkeypatch, owned_ray):
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from demiflow.data.api import DataAPI
    from test_dataflow_shared_actor import test_shared_actor_real_ray_cut_recovery
    executor=RayDatasetExecutor(workers=1)
    try:
        test_shared_actor_real_ray_cut_recovery(tmp_path,monkeypatch,DataAPI(executor))
    finally:
        executor.close()
