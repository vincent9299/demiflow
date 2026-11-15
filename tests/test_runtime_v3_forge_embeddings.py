"""Original model values → checked native embeddings → original run receipts."""
import importlib.util
from pathlib import Path
import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.data.flow_contract import arrow_fields
from demiflow.execution.checked import CheckedExecution
from demiflow.execution.run_context import RunContext
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules,LIMITS
from demiflow.workbench.runs.manager import DEFAULT_LIMITS
from runtime_v3_recovery_scenarios import _http_fixture,_native_calls


@pytest.fixture(scope='module')
def catalog():
    return inspect_modules(['demiflow.data','demiflow.embeddings'],Path(data.__file__).resolve().parents[1],LIMITS)


def fixture(root,catalog,url,*,extra='',model=None,explicit_schema=False):
    (root/'flows').mkdir();(root/'operators').mkdir()
    source=str(root/'input.lance');target=str(root/'output.lance')
    table=pa.Table.from_pylist([{'id':1,'text':'first'},{'id':2,'text':None}])
    lance.write_dataset(table,source)
    model=model or 'EmbeddingModel("fixture","v1",3,endpoint)'
    schema=''
    if explicit_schema:
        schema='    schema=data.schema({"fields":[{"name":"id","type":"int64"},{"name":"text","type":"string"},{"name":"embedding","type":{"list":{"name":"item","type":"float32"}}},{"name":"error","type":"string"}]})\n'
    body='from demiflow import data\nfrom demiflow.embeddings import EmbeddingModel\ndef run(*,source,target,endpoint):\n'+schema+f'    model={model}\n    rows=data.read_lance(source,version=1).map_embeddings(model=model,inputs={{"text":"text"}},error_output="error",batch_size=2,max_requests=2,options={{"max_request_bytes":1048576,"max_response_bytes":1048576,"timeout_s":5}},{extra})\n    return rows.write_lance(target,mode="create",return_receipt=True'+(',schema=schema' if explicit_schema else '')+')\n'
    p=root/'flows'/'embedding_case.py';p.write_text(body)
    args=dict(source=source,target=target,endpoint=url+'/v1')
    report=FlowChecker(root,catalog,{'input':{'uri':source,'version':1,'fields':arrow_fields(table.schema)}},[target]).inspect('flows/embedding_case.py:run',args)
    return p,args,report


@pytest.mark.parametrize('explicit_schema',[False,True])
def test_checked_original_embedding_model_real_http_receipt(tmp_path,catalog,explicit_schema):
    with _http_fixture(tmp_path) as (requests,url):
        path,args,report=fixture(tmp_path,catalog,url,explicit_schema=explicit_schema)
        assert report['status']=='checked',report['diagnostics']
        node=next(n for n in report['nodes'] if n['kind']=='embeddings')
        assert node['analysis']['native']['contract']=='demiflow.embeddings.v2'
        assert node['analysis']['request_budget']=={'scope':'execution','maximum':2}
        assert node['fields']['embedding']['type']['length']==3
        spec=importlib.util.spec_from_file_location('flows.embedding_case',path)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        obs=[]
        checked=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
            source_root=tmp_path,limits=DEFAULT_LIMITS,record=lambda *a:None,
            observe=lambda *a,**kw:obs.append((a,kw)),check_stop=lambda:None,report_digest=report['digest'])
        with RunContext.start(tmp_path/'run') as run,data.local_execution(workers=4),checked.bind_context():
            receipt=module.run(**args)
            assert run.journal.execution_commits(report['actions'][0])[-1]['status']=='acked'
            native=_native_calls(run.journal)
            assert native['confirmed']==1 and native['requests_consumed']==1 and not native['errors']
        rows=sorted(lance.dataset(receipt.uri).to_table().to_pylist(),key=lambda r:r['id'])
        assert [r['id'] for r in rows]==[1,2]
        if explicit_schema:assert lance.dataset(receipt.uri).schema.field('embedding').type.value_type==pa.float32()
        assert rows[0]['embedding']==[1.,0.,0.] and rows[0]['error'] is None
        assert rows[1]['embedding'] is None and 'nonempty string' in rows[1]['error']
        assert len(requests)==1 and requests[0]['body']['input']==['first']
        assert obs


@pytest.mark.parametrize('extra,model',[
    ('service={"fake":True}',None),('request_gate={"fake":True}',None),
    ('request_policy={"invented":True}',None),
    ('','EmbeddingModel("fixture","v1",0,endpoint)'),
    ('','{"embedding_model":{"name":"forged"}}'),
])
def test_embedding_decl_invalid_never_calls_provider(tmp_path,catalog,extra,model):
    with _http_fixture(tmp_path) as (requests,url):
        _,_,report=fixture(tmp_path,catalog,url,extra=extra,model=model)
        assert report['status']!='checked'
        assert not requests and not (tmp_path/'output.lance').exists()


def test_native_vector_validation_uses_actual_list_contract():
    from demiflow.data.runtime_contract import check_value,RuntimeContractError
    expected={'kind':'list','item':{'kind':'float','nullable':False},'length':3,'nullable':False}
    value=[-0.,1.,2.]
    check_value(value,expected,'vector')
    import math
    assert math.copysign(1,value[0])<0
    for bad in [(0.,1.,2.),[1.,2.],[1.,2.,True]]:
        with pytest.raises(RuntimeContractError):check_value(bad,expected,'vector')


def test_native_factory_keeps_actual_checked_vector_contract(tmp_path,catalog,monkeypatch):
    from demiflow.embeddings.runtime import EmbeddingActor
    from demiflow.data.runtime_contract import RuntimeContractError
    original=EmbeddingActor._encode_prepared
    async def bad_vector(actor,*args,**kwargs):
        rows=await original(actor,*args,**kwargs)
        for row in rows:
            if row[actor.output] is not None:row[actor.output]=row[actor.output][:-1]
        return rows
    monkeypatch.setattr(EmbeddingActor,'_encode_prepared',bad_vector)
    with _http_fixture(tmp_path) as (requests,url):
        path,args,report=fixture(tmp_path,catalog,url)
        assert report['status']=='checked',report['diagnostics']
        spec=importlib.util.spec_from_file_location('flows.embedding_case',path)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        checked=CheckedExecution(**{k:report[k] for k in ('nodes','edges','actions','operators')},
            source_root=tmp_path,limits=DEFAULT_LIMITS,record=lambda *a:None,check_stop=lambda:None)
        with pytest.raises(RuntimeContractError,match='sequence length'):
            # A real thread worker observes this deliberate implementation fault;
            # transport, original native request/response and sink remain real.
            with RunContext.start(tmp_path/'run'),data.local_execution(workers=4,worker_mode='thread'),checked.bind_context():
                module.run(**args)
        assert len(requests)==1 and not Path(args['target']).exists()


def test_runtime_type_contract_does_not_change_original_native_semantic_identity():
    from dataclasses import replace
    from demiflow.embeddings import EmbeddingModel
    from demiflow.execution.recovery_identity import operation_identity
    from demiflow.planning.dataflow_compile import _operation_semantic_facts
    op=data.from_items([{'text':'a'}]).map_embeddings(model=EmbeddingModel('fixture','v1',3,'http://127.0.0.1:1/v1'),
        inputs={'text':'text'},max_requests=1)._plan.operations[-1]
    changed=replace(op,callable=replace(op.callable,runtime_contract={'label':'checked'}))
    assert operation_identity(op)==operation_identity(changed)
    assert _operation_semantic_facts(op)==_operation_semantic_facts(changed)


def test_catalog_does_not_invoke_a_custom_default_factory():
    import dataclasses
    from demiflow.forge.platform import _signature
    calls=[]
    def custom():
        calls.append('bad');raise AssertionError('factory must stay opaque')
    @dataclasses.dataclass
    class Passive:
        options:dict=dataclasses.field(default_factory=custom)
    Passive.__demiflow_flow__={'role':'value'}
    info=_signature(Passive,'class',32768)
    assert info['parameters'][0]['default_value']['status']=='opaque'
    assert calls==[]
