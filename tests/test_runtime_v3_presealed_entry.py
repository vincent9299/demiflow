"""First-attempt sealed submission uses the same authenticated engine path."""
from contextlib import contextmanager
from pathlib import Path

import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError


def _plain(row):return {**row,'out':row['i']*2}


@contextmanager
def _prepared(tmp_path,*,thread=False,wrong_program_pin=False):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.executors.dataflow_api import make_action_request
    from demiflow.execution.action_driver.services import make_driver_services
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    shared=[]
    def live(row):
        shared.append(row['i'])
        return {**row,'out':row['i']*2}
    mode='thread' if thread else 'process'
    class PreparedOnly(Exception):pass
    try:
        with RunContext.start(tmp_path/'run') as run,data.local_execution(workers=3,worker_mode=mode):
            ds=data.from_items([{'i':0},{'i':1}]).map(live,execution='thread') if thread else data.from_items([{'i':0},{'i':1}]).map(_plain)
            request=make_action_request(ds._executor,ds._source,ds._plan,kind='take_all',arguments={'limit':None})
            policy,bounds=resolve_runtime_policy({'worker_mode':mode},run.limits,provider='local')
            services=make_driver_services(request.action,policy=policy,bounds=bounds)
            second=DataflowExecution(request,services=services)
            from demiflow.planning.dataflow_compile import lower_action
            from demiflow.execution.seal.plan import stage_dataflow_programs,resolve_execution_constants,seal_dataflow
            from demiflow.execution.sources.snapshots import snapshot_source, plan_source_splits
            from demiflow.execution.seal.runtime_bundle import stage_runtime_bundle
            from demiflow.planning.dataflow_place import compile_physical_dataflow
            try:
                compiled=lower_action(request,bounds=bounds)
                graph=stage_dataflow_programs(compiled,action=request.action,store=services.store,bounds=bounds,worker_mode=mode)
                if wrong_program_pin:
                    # Fault input is immutable and checksum-correct. Its pin is
                    # inconsistent with the actual serialized program, so only
                    # the original loader's code recomputation can reject it.
                    import json
                    from dataclasses import replace
                    from demiflow.execution.contracts.recovery_contract import document
                    node=next(n for n in graph.nodes if n.kind=='transform')
                    manifest=json.loads(b''.join(services.store.read(node.program_ref,max_bytes=bounds.max_control_bytes)))
                    manifest['code_identity']={**manifest['code_identity'],'wrong_program_pin':True}
                    raw=document(manifest,bounds.max_control_bytes)
                    bad=services.store.put(iter([raw]),max_bytes=len(raw),media_type=node.program_ref.media_type)
                    graph=replace(graph,nodes=tuple(replace(n,program_ref=bad) if n is node else n for n in graph.nodes))
                snapshots=[];splits=[]
                for declaration in compiled.declarations:
                    if declaration.source_spec is None:continue
                    snapshot=snapshot_source(declaration.source_spec,action=request.action,store=services.store,
                        bounds=bounds,source_id=declaration.occurrence_id)
                    snapshots.append(snapshot)
                    splits.extend(plan_source_splits(snapshot,declaration.source_spec,target_partitions=policy.default_partitions,bounds=bounds))
                constants=resolve_execution_constants(graph,action=request.action,store=services.store,policy=policy)
                physical=compile_physical_dataflow(graph,tuple(snapshots),tuple(splits),policy=policy,
                    hosts=services.hosts,bounds=bounds,constants_ref=constants)
                sealed=seal_dataflow(request.action,graph,physical,store=services.store)
                stage_runtime_bundle(sealed,store=services.store,bounds=bounds)
                yield run,ds,{'graph':graph,'physical':physical,'sealed':sealed,'services':services},second,shared
            finally:second.close()

            if run.journal.incomplete_execution():raise PreparedOnly()
    except PreparedOnly:pass


@pytest.fixture(autouse=True)
def isolated_tree(tmp_path,monkeypatch):
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',str(tmp_path/'capacity'))


@pytest.mark.parametrize('thread',[False,True])
def test_first_sealed_plan_executes_without_a_second_compile_or_final_ack(tmp_path,monkeypatch,thread):
    from demiflow.execution.action_driver import dataflow as driver
    from demiflow.execution.outputs.terminals import open_terminal, collect_rows
    observed=[];lower=driver.lower_action
    def watch(*args,**kwargs):observed.append(1);return lower(*args,**kwargs)
    with _prepared(tmp_path,thread=thread) as (run,ds,first,second,shared):
        assert not run.journal.execution_commits('action-1')
        if thread:assert any(n.kernel.configuration.get('thread_attempt') for n in first['graph'].nodes)
        monkeypatch.setattr(driver,'lower_action',watch)
        assert second.prepare()==first['sealed']
        assert second._physical.plan_id==first['physical'].plan_id and observed==[]
        second.start()
        rows=collect_rows(open_terminal(second._request,execution=second),limit=None,bounds=second._bounds)
        assert rows==[{'i':0,'out':0},{'i':1,'out':2}]
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'
        assert shared==([0,1] if thread else [])
        second._action.complete({'kind':'take_all','delivery':'completed'})


def test_first_sealed_plan_bad_code_pin_carrier_is_rejected_before_execution(tmp_path):
    with _prepared(tmp_path) as (run,ds,first,second,_):
        path=first['services'].store._object_path(first['sealed'].code_pin_ref)
        old=path.read_bytes();path.write_bytes(old[:-1]+bytes([old[-1]^1]))
        with pytest.raises(DataflowError,match='artifact bytes do not match its reference'):
            second.prepare()
        assert not second._started and not run.journal.execution_commits('action-1')
        path.write_bytes(old)


def test_first_sealed_plan_for_another_action_is_rejected(tmp_path):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.executors.dataflow_api import make_action_request
    from demiflow.execution.action_driver.services import make_driver_services
    with _prepared(tmp_path) as (run,ds,first,unused,_):
        request=make_action_request(ds._executor,ds._source,ds._plan,kind='take_all',arguments={'limit':None})
        assert request.action.action_id!='action-1'
        facts=run.journal.source_binding('action-1-dataflow-sealed')
        run.journal.bind_source(run.fence,request.action.action_id+'-dataflow-sealed',facts)
        second=DataflowExecution(request,services=make_driver_services(request.action,policy=first['physical'].policy,bounds=first['physical'].bounds))
        try:
            with pytest.raises(DataflowError,match='action|Action'):second.prepare()
            assert not second._started and not run.journal.execution_commits(request.action.action_id)
        finally:second.close()


def test_prepared_files_without_original_cut_still_cannot_resume(tmp_path):
    class PreparedOnly(Exception):pass
    with pytest.raises(PreparedOnly):
        with _prepared(tmp_path) as (run,ds,first,second,_):
            reference=run.ref
            assert not run.journal.execution_commits('action-1')
            raise PreparedOnly()
    with pytest.raises(DataflowError,match='No committed cut with journal proof'):
        with RunContext.resume(reference),data.local_execution(worker_mode='process'):
            data.from_items([{'i':0},{'i':1}]).map(_plain).take_all()


def test_first_sealed_checksum_correct_wrong_program_pin_fails_recomputation(tmp_path):
    with _prepared(tmp_path,wrong_program_pin=True) as (run,ds,first,second,_):
        with pytest.raises(DataflowError,match='Loaded program code differs from the sealed pin'):
            second.prepare()
        assert not second._started and not run.journal.execution_commits('action-1')
