"""Model-family protocol tests with fake encoders/backends, never model imports."""
from types import SimpleNamespace
import json

import httpx
import pytest

from demiflow.services.model_adapters import PplxAdapter, JEVAdapter, AmosAdapter
from demiflow.services.contracts import served_model_names


def body(state='a', choices=None):
    question={'type':'choice','instructions':'Choose','criteria':dict.fromkeys(choices)} if choices else {
        'type':'noul','instructions':'Is it a?'}
    return {'state':state,'questions':{'q':question}}


def test_pplx_native_list_preserves_order_usage_and_isolates_bad_inputs():
    calls=[]
    class Encoder:
        def plan(self,request,limit):
            if request['state']=='bad': raise ValueError('bad input')
            return [SimpleNamespace(length=2)]
    class Decider:
        encoder=Encoder(); max_length=100
        def decide(self,value):
            calls.append(value)
            return [{'answers':{'q':request['state']},'usage':{'input_tokens':2}} for request in value]
    adapter=PplxAdapter(Decider())
    results=adapter.batch([body('first'),body('bad'),body('last'),
                          {**body('bad-image'),'images':['data:image/png;base64,Zm9v'] }])
    assert len(calls)==1 and isinstance(calls[0],list)
    assert results[0]['answers']['q']=='first' and results[2]['answers']['q']=='last'
    assert results[1]['error']['status']==422 and results[2]['usage']['input_tokens']==2
    assert results[3]['error']['status']==422


def test_pplx_splits_token_budget_without_rejecting_valid_batch_partners():
    calls=[]
    class Decider:
        max_length=262144
        encoder=SimpleNamespace(plan=lambda request,limit:[SimpleNamespace(length=150000)])
        def decide(self,value):
            calls.append(value)
            return [{'answers':{'q':request['state']}} for request in value]
    result=PplxAdapter(Decider()).batch([body('first'),body('second'),body('third')])
    assert [len(x) for x in calls]==[1,1,1]
    assert [r['answers']['q'] for r in result]==['first','second','third']


def test_standalone_worker_does_not_shadow_stdlib_http():
    import subprocess,sys
    from pathlib import Path
    from demiflow.services import model_worker
    result=subprocess.run([sys.executable,str(Path(model_worker.__file__)),'--help'],
                          capture_output=True,text=True,timeout=10)
    assert result.returncode==0,result.stderr


def test_jev_batches_only_compatible_option_sets_and_reorders_backend_choices():
    calls=[]
    class Formatter:
        def options(self,q): return list(q.get('criteria',{'no':None,'yes':None})),None
        def render(self,tokenizer,state,question,codes): return state
        def to_answer(self,q,probability):
            keys=self.options(q)[0]
            return {'type':q['type'],'order':keys,'probabilities':probability}
    tokenizer=SimpleNamespace(encode=lambda text,**kw:list(text))
    def backend(request):
        payload=json.loads(request.content); calls.append(payload)
        prompts=payload['prompt']; prompts=[prompts] if isinstance(prompts,str) else prompts
        return httpx.Response(200,json={'choices':[{'index':index,'logprobs':{'top_logprobs':[
            {f'token_id:{token}':-float(index+offset) for offset,token in enumerate(payload['allowed_token_ids'])}]}}
            for index in reversed(range(len(prompts)))]})
    with httpx.Client(transport=httpx.MockTransport(backend)) as client:
        adapter=JEVAdapter(Formatter(),tokenizer,{'readout_token_ids':[1,2,3],'codes':['A','B','C']},
                           'http://backend/v1','fixture',client)
        single=adapter.single(body('baseline'))
        assert isinstance(calls[0]['prompt'],str)
        results=adapter.batch([body('first',['a','b']),body('triple',['c','d','e']),
                               body('second',['b','a']),{'state':'bad','questions':{}}])
    assert [call['prompt'] for call in calls[1:]]==[['first','second'],['triple']]
    assert results[0]['answers']['q']['order']==['a','b']
    assert results[2]['answers']['q']['order']==['b','a']
    assert results[3]['error']['status']==422
    assert sum(single['answers']['q']['probabilities'])==pytest.approx(1)


def test_amos_delegates_without_changing_the_question_or_reimplementing_calibration():
    seen=[]
    def engine(value): seen.append(value); return {'answers':{'q':{'type':'noul','noul':.625}}}
    adapter=AmosAdapter(engine); value=body()
    assert adapter.single({'model':'fixture',**value})['answers']['q']['noul']==.625
    assert seen==[value] and not hasattr(adapter,'batch')


def test_jev_splits_aggregate_token_budget_without_partner_failures():
    calls=[]
    formatter=SimpleNamespace(to_answer=lambda question,probability:{'type':'noul','noul':probability[1]})
    adapter=JEVAdapter(formatter,None,{},'http://unused','fixture',None)
    adapter._prepare=lambda request:[('q'+str(i),{'type':'noul'},request['state'],2,30000) for i in range(4)]
    def read(prompts,count):
        calls.append(prompts); return [[.2,.8] for _ in prompts],None
    adapter._read=read
    rows=adapter.batch([body(str(i)) for i in range(4)])
    assert [len(x) for x in calls]==[8,8]
    assert all(len(row['answers'])==4 for row in rows)


def test_d3_delegates_native_protocol_without_cross_request_batch():
    from demiflow.services.model_adapters import D3Adapter
    seen=[]
    adapter=D3Adapter(SimpleNamespace(answer=lambda request:seen.append(request) or {'answers':{}}))
    value={**body(),'videos':[]}
    adapter.single(value)
    assert seen==[value] and not hasattr(adapter,'batch')


def test_native_systemone_and_openai_discovery_are_both_supported():
    assert served_model_names({'models':[{'name':'amos'}]})==['amos']
    assert served_model_names({'data':[{'id':'vllm'}]})==['vllm']
    for value in ({'data':'bad'},{'models':[{'id':'wrong'}]},None):
        with pytest.raises(ValueError): served_model_names(value)


@pytest.mark.asyncio
async def test_model_discovery_refuses_oversized_documents():
    from demiflow.services.contracts import bounded_metadata_get
    async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request:httpx.Response(200,content=b'x'*65537))) as client:
        with pytest.raises(ValueError,match='64 KiB'):
            await bounded_metadata_get(client,'http://fixture/v1/models')


def jev_with_usage(client, **kwargs):
    formatter=SimpleNamespace(options=lambda q: (list(q.get('criteria',{'yes':None,'no':None})),None),
        render=lambda tokenizer,state,question,codes: state,
        to_answer=lambda q,p: {'type':'noul','noul':p[0]})
    return JEVAdapter(formatter,SimpleNamespace(encode=lambda text,**kw:list(text)),
        {'readout_token_ids':[1,2],'codes':['A','B']},'http://backend/v1','fixture',client,**kwargs)


def jev_response(request,usage=True):
    value=json.loads(request.content);prompts=value['prompt'];prompts=[prompts] if isinstance(prompts,str) else prompts
    payload={'choices':[{'index':i,'logprobs':{'top_logprobs':[{'token_id:1':-1.,'token_id:2':-2.}]}}
                        for i in range(len(prompts))]}
    if usage is True:
        payload['usage']={'prompt_tokens':sum(map(len,prompts)),'completion_tokens':len(prompts)}
    elif usage is not False: payload['usage']=usage
    return httpx.Response(200,json=payload)


def test_jev_preserves_actual_provider_usage_per_request_and_question():
    request=body('alpha');request['questions']['another']=dict(request['questions']['q'])
    with httpx.Client(transport=httpx.MockTransport(jev_response)) as client:
        adapter=jev_with_usage(client)
        single=adapter.single(request)
        assert single['usage']=={'input_tokens':10,'output_tokens':2}
        batch=adapter.batch([request,body('beta')])
        assert batch[0]==single and batch[1]['usage']=={'input_tokens':4,'output_tokens':1}
    with httpx.Client(transport=httpx.MockTransport(lambda r:jev_response(r,False))) as client:
        adapter=jev_with_usage(client)
        assert 'usage' not in adapter.single(request)
        assert all('usage' not in r for r in adapter.batch([request,body('beta')]))


@pytest.mark.parametrize('usage',[{'prompt_tokens':True,'completion_tokens':1},
                                  {'prompt_tokens':1,'completion_tokens':-1},{}])
def test_jev_rejects_invalid_provider_usage(usage):
    with httpx.Client(transport=httpx.MockTransport(lambda r:jev_response(r,usage))) as client:
        with pytest.raises(RuntimeError,match='usage'):jev_with_usage(client).single(body())


def test_jev_does_not_guess_per_row_usage_when_batch_totals_differ():
    with httpx.Client(transport=httpx.MockTransport(lambda r:jev_response(r,
             {'prompt_tokens':100,'completion_tokens':2}))) as client:
        with pytest.raises(RuntimeError,match='attribute'):
            jev_with_usage(client).batch([body('a'),body('b')])


def test_jev_pipeline_overlaps_encoding_but_never_sends_two_backend_requests():
    import threading
    from concurrent.futures import ThreadPoolExecutor
    started=threading.Event();encoded=threading.Event();release=threading.Event();lock=threading.Lock()
    active=peak=0
    def backend(request):
        nonlocal active,peak
        with lock: active+=1;peak=max(peak,active)
        try:
            if json.loads(request.content)['prompt']=='first':
                started.set(); assert release.wait(5)
            return jev_response(request)
        finally:
            with lock:active-=1
    with httpx.Client(transport=httpx.MockTransport(backend)) as client:
        adapter=jev_with_usage(client,serialize_backend=True)
        def render(tokenizer,state,question,codes):
            if state=='second':encoded.set()
            return state
        adapter.formatter.render=render
        with ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(adapter.single,body('first'));assert started.wait(5)
            second=pool.submit(adapter.single,body('second'))
            try:
                assert encoded.wait(5) and not second.done() and peak==1
            finally:release.set()
            assert first.result(5)['usage']['input_tokens']==5
            assert second.result(5)['usage']['input_tokens']==6
    assert peak==1
