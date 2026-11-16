"""Automatic pure linear co-location keeps asynchronous progress independent."""
from dataclasses import replace
from pathlib import Path
import asyncio
import time

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.planning.dataflow_fusion import fuse_single_worker_pipeline
from demiflow.resources.contracts import ResourceProfile
from test_runtime_v3_default_suffix_fusion import compiled, HOSTS


def simple():
    return data.range(7).map(lambda r:{'v':r['id']*3+1}).filter(lambda r:r['v']%2==0).map(lambda r:r)


@pytest.mark.parametrize('provider',['local','ray_core'])
def test_worker_ceiling_permits_one_original_pure_pipeline_owner(provider):
    graph,stages,policy,bounds=compiled(dataset=simple())
    policy=replace(policy,provider=provider)
    hosts=tuple(replace(host,provider=provider) for host in HOSTS)
    result=fuse_single_worker_pipeline(graph,stages,policy=policy,bounds=bounds,hosts=hosts)
    assert len(result)==1 and policy.max_active_workers==4
    stage=result[0]
    assert stage.occurrences==tuple(n.occurrence_id for n in graph.nodes)
    assert stage.profile.requests['cpu_millis']==max(s.profile.requests['cpu_millis'] for s in stages)
    assert stage.profile.requests['memory_bytes']==sum(max(s.resident_bytes,s.profile.requests['memory_bytes']) for s in stages)
    assert stage.kernel_refs==tuple(dict.fromkeys(ref for s in stages for ref in s.kernel_refs))


@pytest.mark.parametrize('change',['state','effect','streaming','timer','gpu','isolation','partition','disabled','host_cpu','host_memory'])
def test_automatic_linear_fusion_preserves_all_original_admission_boundaries(change):
    graph,stages,policy,bounds=compiled(dataset=simple())
    hosts=HOSTS
    if change in ('state','effect','streaming','timer'):
        nodes=list(graph.nodes);node=nodes[1];kernel=node.kernel
        if change=='state':kernel=replace(kernel,state='auto_json')
        if change=='effect':kernel=replace(kernel,effect='journaled')
        if change in ('streaming','timer'):
            kernel=replace(kernel,configuration={**dict(kernel.configuration),
                ('streaming_map' if change=='streaming' else 'flush_interval'):True if change=='streaming' else .01})
        nodes[1]=replace(node,kernel=kernel);graph=replace(graph,nodes=tuple(nodes))
    if change in ('gpu','isolation','partition'):
        values=list(stages);stage=values[1]
        if change=='gpu':stage=replace(stage,profile=ResourceProfile(requests={**dict(stage.profile.requests),'accelerators':[{'type':'gpu','count':1}]}))
        if change=='isolation':stage=replace(stage,profile=ResourceProfile(requests=dict(stage.profile.requests),requirements={'isolation':'process'}))
        if change=='partition':stage=replace(stage,partitions=(*stage.partitions,'extra-partition'),replica_target=2,replica_max=2)
        values[1]=stage;stages=tuple(values)
    if change=='disabled':policy=replace(policy,fusion=False)
    if change in ('host_cpu','host_memory'):
        required=sum(max(s.resident_bytes,s.profile.requests['memory_bytes']) for s in stages)
        hosts=tuple(replace(HOSTS[0],host_id=str(i),logical_resources={
            'cpu_millis':500 if change=='host_cpu' else 16000,
            'memory_bytes':required-1 if change=='host_memory' else 2**30}) for i in range(8))
    assert fuse_single_worker_pipeline(graph,stages,policy=policy,bounds=bounds,hosts=hosts)==stages


def test_shared_source_and_multi_source_are_never_folded_into_the_pure_chain():
    shared=data.range(2)
    for ds in (shared.map(lambda r:r).union(shared.map(lambda r:r)),
               data.range(2).union(data.range(3))):
        graph,stages,policy,bounds=compiled(dataset=ds)
        assert fuse_single_worker_pipeline(graph,stages,policy=policy,bounds=bounds,hosts=HOSTS)==stages


@pytest.mark.parametrize('mode',['process','thread'])
@pytest.mark.parametrize('kind',['async','timer'])
def test_blocking_source_cannot_starve_async_or_timer_before_first_public_result(tmp_path,monkeypatch,mode,kind):
    from test_runtime_v3_stream_suffix import _capture
    plans,allocations=_capture(monkeypatch)
    gate,eoi=tmp_path/'consumer-read-first',tmp_path/'source-eoi'
    def source():
        yield {'v':1}
        deadline=time.monotonic()+12
        while not gate.exists():
            if time.monotonic()>=deadline:raise RuntimeError('downstream first result starved behind blocked source')
            time.sleep(.005)
        yield {'v':2}
        eoi.touch()
    async def echo(row):
        await asyncio.sleep(.02)
        return row
    ds=data.from_iter(source)
    if kind=='async':ds=ds.map_async(echo,concurrency=1)
    else:ds=ds.batch_map(lambda batch:batch,max_batch=8,flush_interval=.04,concurrency=1)
    with RunContext.start(tmp_path/'run',limits={'timeout_s':25}),data.local_execution(workers=4,worker_mode=mode):
        cursor=ds.iter_rows()
        try:
            assert next(cursor)=={'v':1}
            assert not eoi.exists()
            gate.touch()
            assert list(cursor)==[{'v':2}]
        finally:
            gate.touch();cursor.close()
    graph,physical=plans[0]
    source_stage=next(s for s in physical.stages if graph.sources[0] in s.occurrences)
    assert source_stage.occurrences==graph.sources
    assert len(physical.stages)==len(allocations)>=2
    assert eoi.exists()


def test_resume_uses_original_unfused_seal_instead_of_new_automatic_layout(tmp_path,monkeypatch):
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    from demiflow.planning import dataflow_fusion
    from test_runtime_v3_stream_suffix import _capture
    plans,_=_capture(monkeypatch)
    fuse=dataflow_fusion.fuse_single_worker_pipeline
    commit=DataflowCheckpointCoordinator.commit
    class AfterOriginalAck(Exception):pass
    def previous_layout(graph,stages,*,policy,bounds,hosts=None):
        # The previous automatic rule only fused explicitly requested workers=1.
        # Keep the actual unchanged policy, compiler, execution and commit path.
        return fuse(graph,stages,policy=policy,bounds=bounds)
    def interrupt(coordinator,*args,**kwargs):
        image=commit(coordinator,*args,**kwargs)
        if not image.cut.final:raise AfterOriginalAck()
        return image
    limits={'checkpoint_rows':2,'checkpoint_interval_ms':60000,'timeout_s':30}
    with pytest.raises(AfterOriginalAck),RunContext.start(tmp_path/'run',limits=limits) as run,data.local_execution(workers=4):
        ref=run.ref
        with monkeypatch.context() as patch:
            patch.setattr(dataflow_fusion,'fuse_single_worker_pipeline',previous_layout)
            patch.setattr(DataflowCheckpointCoordinator,'commit',interrupt)
            simple().take_all()
    assert len(plans[0][1].stages)==2
    with RunContext.resume(ref) as resumed,data.local_execution(workers=4):
        before=resumed.journal.execution_commits('action-1')
        assert simple().take_all()==[{'v':4},{'v':10},{'v':16}]
        after=resumed.journal.execution_commits('action-1')
        assert after[:len(before)]==before and all(x['status']=='acked' for x in after)
    assert plans[-1][1]==plans[0][1]


@pytest.mark.parametrize('workers',[4,8,None])
def test_sufficient_worker_ceiling_keeps_short_cpu_pipeline_independently_placeable(workers):
    graph,stages,policy,bounds=compiled(dataset=data.range(7).map(lambda row:row))
    assert len(stages)==3
    policy=replace(policy,max_active_workers=workers)
    assert fuse_single_worker_pipeline(graph,stages,policy=policy,bounds=bounds,hosts=HOSTS)==stages
