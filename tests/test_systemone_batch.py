import asyncio
from contextlib import asynccontextmanager
import json
import threading

import httpx
import pytest

from demiflow.operator_llm.client import AsyncOperatorLLMClient
from demiflow.operator_llm.model import OperatorLLMRequest, PromptModel, TextPart
from demiflow.operator_llm.systemone_batch import BatchTransport
from demiflow.services.model_worker import make_server
from demiflow.services.quality import read_evidence
from demiflow.operator_llm.journal import UncertainPromptCall


def request(index, *, choice=None):
    return OperatorLLMRequest('score','v1','fixture',(TextPart(str(index)),),
        systemone=choice or {'type':'noul','instructions':'Score'})


class Adapter:
    def __init__(self): self.batches=[]; self.singles=[]
    def single(self,body):
        self.singles.append(body)
        return {'answers':{'score':{'type':'noul','noul':float(body['state'])/1000}}}
    def batch(self,bodies):
        self.batches.append(bodies)
        return [{'answers':{'score':{'type':'noul','noul':float(body['state'])/1000}}} for body in bodies]


@pytest.fixture
def server(tmp_path):
    adapter=Adapter()
    # Synthetic CUDA identity is deliberately scoped to this fake-server test;
    # no report is written to a real deployment or used to certify a model.
    identity={'hardware':{'kind':'cuda','devices':['fixture']},'model_contract':{'revision':'r1'}}
    proof=tmp_path/'fixture.json'
    proof.write_text(json.dumps({'schema_version':'demiflow_batch_quality_v2','identity':identity,
        'passed':True,'sample_count':200,'max_size':8,'partner_rounds':3,'label_mismatches':0,
        'probability_failures':0,'cases_sha256':'fixture','parallel':8,'probability_atol':1e-6,
        'tested_batch_sizes':list(range(1,9)),'max_probability_delta':0,
        'input_coverage':{'modalities':['text'],'question_types':['noul'],'choice_sizes':[],'max_questions':1,
                          'shapes':['["text",[["noul",0]]]']},
        'metrics':{mode:{'rows':600 if mode.startswith('batch_') else 200,
                         'label_mismatches':0,'probability_failures':0,'max_probability_delta':0,
                         'batch_size':int(mode[6:]) if mode.startswith('batch_') else 1,
                         'parallel_requests':1 if mode=='single' else 8}
                   for mode in ['single','parallel_single']+['batch_'+str(n) for n in range(1,9)]}}))
    srv=make_server(adapter,'fixture',0,8,identity=identity,batch_evidence=proof,batch_max_size=8)
    thread=threading.Thread(target=srv.serve_forever,daemon=True);thread.start()
    yield f'http://127.0.0.1:{srv.server_port}/v1',adapter
    srv.shutdown();srv.server_close();thread.join()


@pytest.mark.asyncio
async def test_batch_keeps_row_journal_identity_and_takes_one_slot_per_http(server,tmp_path):
    base,adapter=server
    model=PromptModel('fixture','systemone',base_url=base)
    options={'model_revision':'r1','trust_env':False,'journal_dir':str(tmp_path/'journal'),
             'systemone_batch':{'max_size':8,'wait_ms':30}}
    client=AsyncOperatorLLMClient(model,options)
    slots=0
    @asynccontextmanager
    async def context():
        nonlocal slots
        slots+=1
        yield base
    client.transport_context=context
    try:
        results=await asyncio.gather(*(client.execute(request(i)) for i in range(16)))
        assert len(results)==16 and len(adapter.batches)<16
        assert slots==len(adapter.batches)
        assert sum(map(len,adapter.batches))==16 and not adapter.singles
        plain=AsyncOperatorLLMClient(model,{k:v for k,v in options.items() if k!='systemone_batch'})
        try:
            assert client.record(request(3)).request_key==plain.record(request(3)).request_key
            for i in range(16):
                _, cached=await plain.prepare_lookup(request(i))
                assert cached is not None
        finally: await plain.aclose()
    finally: await client.aclose()


@pytest.mark.asyncio
async def test_batch_requires_quality_capability_and_matching_revision(server):
    base,adapter=server
    client=AsyncOperatorLLMClient(PromptModel('fixture','systemone',base_url=base),
        {'model_revision':'different','systemone_batch':{'max_size':2},'trust_env':False})
    try:
        with pytest.raises(ValueError,match='validated'): await client.execute(request(1))
        assert not adapter.batches
    finally: await client.aclose()


@pytest.mark.asyncio
async def test_bounded_queue_parallelism_cancellation_and_close():
    active=peak=0; release=asyncio.Event(); entered=asyncio.Event(); sends=[]
    async def send(items):
        nonlocal active,peak
        sends.append(items); active+=1; peak=max(peak,active); entered.set()
        await release.wait(); active-=1
        return items
    transport=BatchTransport({'max_size':2,'wait_ms':0,'parallel':2,'max_queue_rows':1,
        'max_queue_bytes':2048,'max_batch_bytes':2048},send)
    tasks=[asyncio.create_task(transport.submit(b'{}')) for _ in range(6)]
    await entered.wait(); await asyncio.sleep(.02)
    assert peak==2 and len(transport.queue)<=1 and transport.bytes<=2048
    tasks[-1].cancel()
    release.set()
    result=await asyncio.gather(*tasks,return_exceptions=True)
    await transport.aclose()
    assert sum(isinstance(x,asyncio.CancelledError) for x in result)==1
    with pytest.raises(RuntimeError,match='closed'): await transport.submit(b'{}')


@pytest.mark.asyncio
async def test_one_cancelled_inflight_row_does_not_cancel_its_partner():
    entered=asyncio.Event(); release=asyncio.Event()
    async def send(items):
        entered.set(); await release.wait(); return items
    transport=BatchTransport({'max_size':2,'wait_ms':100},send)
    one=asyncio.create_task(transport.submit(b'1')); two=asyncio.create_task(transport.submit(b'2'))
    await entered.wait(); one.cancel(); release.set()
    assert await two==b'2'
    with pytest.raises(asyncio.CancelledError): await one
    await transport.aclose()


@pytest.mark.asyncio
async def test_exact_byte_boundary_rejects_before_queueing_instead_of_spinning():
    async def send(items): return items
    transport=BatchTransport({'max_size':1,'max_batch_bytes':2048,'max_queue_bytes':2048},send)
    try:
        with pytest.raises(ValueError,match='Single request'):
            await transport.submit(b'x'*1024)
        assert await asyncio.wait_for(transport.submit(b'x'*1023),1)==b'x'*1023
    finally: await transport.aclose()


def test_choice_order_is_part_of_identity_and_response_is_checked():
    from demiflow.operator_llm.systemone import response_result
    from demiflow.operator_llm.errors import PromptResponseContractError
    client=AsyncOperatorLLMClient(PromptModel('fixture','systemone',base_url='http://localhost:1/v1'))
    q={'type':'choice','instructions':'Choose','criteria':{'a':None,'b':None}}
    one=request(1,choice=q); two=request(1,choice={**q,'criteria':{'b':None,'a':None}})
    assert client.record(one).request_key!=client.record(two).request_key
    good={'answers':{'score':{'type':'choice','choice':'a','probabilities':{'a':.8,'b':.2}}}}
    assert response_result(one,good)['result']['choice']=='a'
    for bad in (None,[],{'type':'choice','choice':'c','probabilities':{'a':.8,'b':.2}},
                {'type':'choice','choice':'a','probabilities':{'a':True,'b':0}}):
        with pytest.raises(PromptResponseContractError): response_result(one,{'answers':{'score':bad}})


def test_fake_or_stale_evidence_cannot_certify_a_model(tmp_path):
    proof=tmp_path/'proof.json'; proof.write_text('{}')
    with pytest.raises(ValueError,match='evidence'): read_evidence(proof,{'hardware':{'kind':'test'}},2)


def test_evidence_rejects_missing_batch_sizes_and_contradictory_metrics(server,tmp_path):
    proof=tmp_path/'fixture.json'; report=json.loads(proof.read_text()); identity=report['identity']
    from copy import deepcopy
    for mutate in (
        lambda r:r.update(schema_version='demiflow_batch_quality_v1'),
        lambda r:r.update(tested_batch_sizes=[1,8]),
        lambda r:r['metrics']['batch_3'].update(probability_failures=1),
        lambda r:r['metrics']['batch_3'].update(parallel_requests=1),
        lambda r:r['metrics']['batch_3'].update(max_probability_delta=.1),
        lambda r:r['input_coverage'].update(question_types=['choice']),
    ):
        changed=deepcopy(report); mutate(changed); proof.write_text(json.dumps(changed))
        with pytest.raises(ValueError): read_evidence(proof,identity,8,parallel=8)


@pytest.mark.asyncio
async def test_batch_wrong_model_and_unverified_input_are_row_local_errors(server):
    base,adapter=server
    good={'state':'1','questions':{'score':{'type':'noul','instructions':'Score'}}}
    unverified={**good,'questions':{'score':{'type':'choice','instructions':'Choose','criteria':{'a':None,'b':None}}}}
    async with httpx.AsyncClient(trust_env=False) as client:
        response=await client.post(base+'/systemone_batch',json={'model':'fixture','requests':[
            good,{**good,'model':'wrong'},unverified,good]})
    rows=response.json()['responses']
    assert response.status_code==200 and len(rows)==4
    assert rows[0]==rows[3] and rows[1]['error']['status']==rows[2]['error']['status']==422
    assert adapter.batches==[[good,good]]


@pytest.mark.asyncio
async def test_batch_row_http_error_retries_only_that_row_and_accounts_each_attempt(tmp_path,monkeypatch):
    from test_prompt_systemone import PACK
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime, InProcessOperatorLLMCoordinator
    from demiflow.operator_llm import runtime as runtime_module
    from demiflow.operator_llm.call_ref import journal_totals
    monkeypatch.setenv('TEST_SYSTEMONE_URL','http://fixture/v1')
    requests=[]
    def handle(req):
        if req.method=='GET':
            return httpx.Response(200,json={'data':[{'id':'mock-decision','capabilities':{
                'model_revision':'r1','batch':{'supported':True,'quality_verified':True,
                    'max_size':2,'evidence_sha256':'fixture'}}}]})
        payload=json.loads(req.content); requests.append((req.url.path,payload))
        good={'answers':{'match':{'type':'noul','noul':.8}}}
        if req.url.path.endswith('_batch'):
            return httpx.Response(200,json={'model':'mock-decision','evidence_sha256':'fixture',
                'responses':[{'error':{'status':429,'detail':'busy'}} if row['state'].strip()=='0' else good
                             for row in payload['requests']]})
        return httpx.Response(200,json=good)
    def create(model,options):
        client=AsyncOperatorLLMClient(model,options)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        client.configure_capacity=lambda _:None
        return client
    monkeypatch.setattr(runtime_module,'create_async_operator_llm_client',create)
    path=tmp_path/'calls.sqlite'
    options={'model_revision':'r1','systemone_batch':{'max_size':2,'wait_ms':50},
        'sqlite_journal':{'path':str(path),'max_requests':3},
        'http_error_retry':{'statuses':[429],'max_retries':1,'backoff_s':[0]}}
    runtime=AsyncOperatorLLMRuntime(parse_prompt_pack(PACK),InProcessOperatorLLMCoordinator(),options)
    try:
        answers=await asyncio.gather(*(runtime.call('match',{'payload':i}) for i in range(2)))
        assert all(a['result']['noul']==.8 for a in answers)
    finally: await runtime.aclose()
    assert [path for path,_ in requests]==['/v1/systemone_batch','/v1/systemone']
    assert requests[-1][1]['state'].strip()=='0'
    assert journal_totals(path)['requests']==3


@pytest.mark.asyncio
async def test_malformed_batch_response_is_uncertain_and_never_automatically_resent(tmp_path):
    from demiflow.operator_llm.errors import PromptStreamError
    posts=[]
    def handle(req):
        if req.method=='GET':
            return httpx.Response(200,json={'data':[{'id':'fixture','capabilities':{
                'model_revision':'r1','batch':{'supported':True,'quality_verified':True,
                    'max_size':2,'evidence_sha256':'fixture'}}}]})
        posts.append(req)
        return httpx.Response(200,json={'model':'fixture','evidence_sha256':'fixture','responses':[]})
    client=AsyncOperatorLLMClient(PromptModel('fixture','systemone',base_url='http://fixture/v1'),
        {'model_revision':'r1','systemone_batch':{'max_size':2,'wait_ms':50},
         'sqlite_journal':{'path':str(tmp_path/'calls.sqlite'),'max_requests':2}})
    client.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
    # Synchronize after journal/encoding work; disk latency must not decide
    # whether this error-isolation test actually forms a two-row batch.
    transport=client._batch_transport=BatchTransport(client.options['systemone_batch'],client._send_batch)
    original=transport.submit; arrived=0; ready=asyncio.Event()
    async def together(payload):
        nonlocal arrived
        arrived+=1
        if arrived==2: ready.set()
        await asyncio.wait_for(ready.wait(),5)
        return await original(payload)
    transport.submit=together
    try:
        result=await asyncio.gather(*(client.execute(request(i)) for i in range(2)),return_exceptions=True)
        assert all(isinstance(error,PromptStreamError) for error in result)
        assert result[0] is not result[1]
        assert result[0].call!=result[1].call
        assert len(posts)==1
        with pytest.raises(UncertainPromptCall,match='without complete response'): await client.execute(request(0))
        assert len(posts)==1
    finally: await client.aclose()


def test_quality_rejects_consistently_omitted_answers():
    from demiflow.services.quality import validate_answers
    case={'questions':{'a':{'type':'noul'},'b':{'type':'choice','criteria':{'x':None,'y':None}}}}
    with pytest.raises(ValueError,match='question keys'):
        validate_answers(case,{'answers':{'a':{'type':'noul','noul':.8}}})
    with pytest.raises(ValueError,match='choice keys'):
        validate_answers(case,{'answers':{'a':{'type':'noul'},'b':{'type':'choice','probabilities':{'x':1}}}})


def test_quality_coverage_does_not_multiply_unverified_input_combinations():
    from demiflow.services.quality import input_coverage,covered_request
    text={'questions':{'q':{'type':'choice','criteria':{'a':None,'b':None}}}}
    image={'images':['fixture'],'questions':{'q':{'type':'noul'}}}
    coverage=input_coverage([text,image])
    assert covered_request(text,coverage) and covered_request(image,coverage)
    assert not covered_request({**text,'images':['fixture']},coverage)
    assert not covered_request({'questions':{**text['questions'],'second':{'type':'noul'}}},coverage)


@pytest.mark.asyncio
async def test_quality_rejects_duplicate_sample_padding():
    from demiflow.services.quality import benchmark
    case={'state':'duplicate','questions':{'score':{'type':'noul','instructions':'Score'}}}
    with pytest.raises(ValueError,match='distinct'):
        await benchmark('http://unused/v1','fixture',[case]*200)


@pytest.mark.asyncio
@pytest.mark.parametrize('pipeline',[False,True])
async def test_unverified_jev_cannot_continuously_batch_concurrent_singles(pipeline):
    import time
    from demiflow.services.model_adapters import JEVAdapter
    class Native(JEVAdapter):
        def __init__(self): self.active=self.peak=0
        def single(self,body):
            self.active+=1; self.peak=max(self.peak,self.active)
            time.sleep(.03); self.active-=1
            return {'answers':{'q':{'type':'noul','noul':.8}}}
    adapter=Native()
    identity={'model_contract':{'options':{'max_num_seqs':1}}} if pipeline else None
    srv=make_server(adapter,'fixture',0,4,identity=identity,backend_single_sequence=pipeline)
    thread=threading.Thread(target=srv.serve_forever,daemon=True); thread.start()
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            base=f'http://127.0.0.1:{srv.server_port}/v1'
            responses=await asyncio.gather(*(client.post(base+'/systemone',json={'model':'fixture'}) for _ in range(4)))
            caps=(await client.get(base+'/models')).json()['data'][0]['capabilities']
        assert all(r.status_code==200 for r in responses)
        assert adapter.peak>1 if pipeline else adapter.peak==1
        assert caps['parallel']==1 and not caps['continuous_batching'] and not caps['batch']['supported']
        assert caps['request_pipelining'] is pipeline
    finally:
        srv.shutdown();srv.server_close();thread.join()


def test_single_sequence_pipeline_cannot_bypass_native_batch_gate():
    from demiflow.services.model_adapters import JEVAdapter
    adapter=object.__new__(JEVAdapter)
    identity={'model_contract':{'options':{'max_num_seqs':1}}}
    for options in ({'identity':None},{'identity':identity,'validation_batch_size':2},
                    {'identity':identity,'batch_max_size':2},
                    {'identity':identity,'batch_evidence':'/unused.json'}):
        with pytest.raises(ValueError,match='Single-sequence'):
            make_server(adapter,'fixture',0,4,backend_single_sequence=True,**options)


@pytest.mark.asyncio
async def test_cpu_acceptance_harness_produces_metrics_but_cannot_certify_real_hardware():
    from demiflow.services.quality import benchmark
    from demiflow.services.model_worker import make_server
    adapter=Adapter(); srv=make_server(adapter,'fixture',0,8,validation_batch_size=4)
    thread=threading.Thread(target=srv.serve_forever,daemon=True); thread.start()
    cases=[{'state':str(i),'questions':{'score':{'type':'noul','instructions':'Score'}}} for i in range(200)]
    try:
        result=await benchmark(f'http://127.0.0.1:{srv.server_port}/v1','fixture',cases,max_size=4,parallel=2)
        assert result['passed'] and result['sample_count']==200
        assert result['identity']['hardware']['kind']=='test'
        assert set(result['metrics'])=={'single','parallel_single','batch_1','batch_2','batch_3','batch_4'}
        assert result['metrics']['single']['peak_gpu_memory_mib'] is None
    finally:
        srv.shutdown();srv.server_close();thread.join()
