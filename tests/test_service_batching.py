import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from demiflow.services.batching import BatchingAdapter, batching_config
from demiflow.services.pplx_fast import PplxFastAdapter, fast_config
from demiflow.services.quality import compare, quality_policy


def request(state):
    return {'state':state,'questions':{'q':{'type':'noul','instructions':'Check'}}}


def test_queue_batches_independent_requests_and_drains_short_tail():
    calls=[]
    backend=SimpleNamespace(batch=lambda rows: calls.append(rows) or [{'answers':r} for r in rows])
    adapter=BatchingAdapter(backend,{'max_size':4,'wait_ms':50})
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            output=list(pool.map(adapter.single,[request(str(i)) for i in range(4)]))
        assert [r['answers']['state'] for r in output]==list(map(str,range(4)))
        assert len(calls)==1 and len(calls[0])==4
        assert adapter.single(request('tail'))['answers']['state']=='tail'
        assert len(calls[-1])==1
    finally: adapter.close()
    assert not adapter.worker.is_alive()
    with pytest.raises(RuntimeError,match='closed'):adapter.single(request('late'))


def test_pending_budgets_include_inflight_and_failure_releases_capacity():
    entered=threading.Event(); release=threading.Event()
    def batch(rows):
        entered.set(); assert release.wait(3)
        raise RuntimeError('GPU failed')
    adapter=BatchingAdapter(SimpleNamespace(batch=batch),{'max_size':2,'max_pending_rows':2,'wait_ms':0})
    pool=ThreadPoolExecutor(max_workers=2)
    try:
        first=pool.submit(adapter.single,request('a'));assert entered.wait(3)
        second=pool.submit(adapter.single,request('b'))
        for _ in range(100):
            if adapter.execution_status()['pending_rows']==2:break
            time.sleep(.005)
        with pytest.raises(RuntimeError,match='capacity'):adapter.single(request('c'))
        release.set()
        for task in [first,second]:
            with pytest.raises(RuntimeError,match='GPU failed'):task.result(3)
    finally:
        release.set();pool.shutdown();adapter.close()
    assert adapter.pending_rows==adapter.pending_bytes==0


def test_bytes_split_batch_and_large_request_rejected_before_enqueue():
    calls=[]
    backend=SimpleNamespace(batch=lambda rows:calls.append(rows) or [{'answers':r} for r in rows])
    size=len(json.dumps(request('a'),ensure_ascii=False).encode())
    adapter=BatchingAdapter(backend,{'max_size':4,'wait_ms':30,'max_request_bytes':size,
        'max_batch_bytes':size*2,'max_pending_bytes':size*4})
    try:
        with pytest.raises(ValueError,match='max_request_bytes'):adapter.single(request('a'*1000))
        with ThreadPoolExecutor(max_workers=4) as pool:
            values=list(pool.map(adapter.single,[request('a')]*4))
        assert len(values)==4 and max(map(len,calls))==2
    finally:adapter.close()


def test_close_drains_admitted_requests_before_backend_close():
    events=[]
    backend=SimpleNamespace(batch=lambda rows:events.append('batch') or [{'answers':r} for r in rows],
                            close=lambda:events.append('close'))
    adapter=BatchingAdapter(backend,{'wait_ms':1000})
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(adapter.single,request('a'))
        for _ in range(100):
            if adapter.pending_rows:break
            time.sleep(.005)
        adapter.close();assert future.result(2)['answers']['state']=='a'
    assert events==['batch','close']


def test_fast_uses_one_thread_and_preserves_partner_errors_and_long_input(monkeypatch):
    threads=[];calls=[]
    monkeypatch.setattr(PplxFastAdapter,'_initialize',lambda self:threads.append(threading.get_ident()))
    class Encoder:
        def plan(self,req,limit):
            if req['state']=='bad':raise ValueError('invalid native input')
            return [SimpleNamespace(length=int(req['state']))]
    def decide(rows):
        threads.append(threading.get_ident());calls.append(rows)
        return [{'answers':r} for r in rows]
    adapter=PplxFastAdapter(SimpleNamespace(encoder=Encoder(),max_length=20,decide=decide),
        {'token_budget':10,'buckets':[5,10],'max_input_tokens':20})
    try:
        values=adapter.batch([request('6'),request('bad'),request('6'),request('15'),request('21')])
        assert [len(c) for c in calls]==[1,1,1]
        assert values[1]['error']['status']==values[4]['error']['status']==422
        assert values[3]['answers']['state']=='15'
        with ThreadPoolExecutor(max_workers=3) as pool:
            assert len(list(pool.map(adapter.single,[request('4')]*3)))==3
        assert len(set(threads))==1 and threads[0]!=threading.get_ident()
        assert adapter.execution_status()['long_requests']==1
    finally:adapter.close()


@pytest.mark.parametrize('value',[{'max_size':True},{'max_size':65},{'wait_ms':-1},
    {'max_pending_bytes':1},{'max_batch_bytes':1},{'unknown':16}])
def test_bad_batching_config(value):
    with pytest.raises(ValueError):batching_config(value)


@pytest.mark.parametrize('value',[{'compile':'mystery'},{'compile':None},
    {'buckets':[1024,1024]},{'buckets':[2048,1024]},{'token_budget':1},
    {'max_input_tokens':True},{'fp8':True}])
def test_bad_fast_config(value):
    with pytest.raises(ValueError):fast_config(value)


def choice(a,b,label):
    return {'answers':{'q':{'type':'choice','choice':label,'probabilities':{'a':a,'b':b}}}}


def test_policy_accepts_only_near_ties_and_still_checks_all_probabilities():
    a,b=choice(.505,.495,'a'),choice(.495,.505,'b')
    assert compare(a,b,.02)[0]==1
    assert compare(a,b,.02,label_margin=.02)==(0,0,pytest.approx(.01))
    assert compare(choice(.8,.2,'a'),choice(.79,.21,'b'),.02,label_margin=.02)[0]==1
    assert compare(a,b,.001,label_margin=.02)[1]>0
    with pytest.raises(ValueError):quality_policy({'probability_atol':.01,'label_margin':.02})


def test_configuration_and_recipe_gate_batching_without_evidence(tmp_path):
    from demiflow.services.deployment import deployment_config
    from demiflow.services.profiles import build_service
    cfg=deployment_config(dict(profile='pplx',weights=str(tmp_path),revision='fixture',gpus=[0],
        options={'pplx_fast':{},'batching':{'max_size':3}},request_concurrency=8))
    manifest={'python':'/fixture/python','ports':[12345],'id':'fixture','model_contract':{},
              'runtime':{},'adapter_version':'fixture'}
    with pytest.raises(ValueError,match='evidence'):build_service('fixture',cfg,manifest)
    command=build_service('fixture',cfg,manifest,validation_batch_size=3).command
    assert json.loads(command[command.index('--batching')+1])['max_size']==3
    assert '--pplx-fast' in command
    with pytest.raises(ValueError,match='combine'):
        deployment_config({**cfg,'performance':'optimized'})


def test_v3_evidence_binds_explicit_policy_and_raw_change_counts(tmp_path):
    from demiflow.services.quality import read_evidence
    # The fixture preserves the existing v2 strict contract. A new policy is a
    # new evidence version; it cannot silently turn old failed evidence valid.
    identity={'hardware':{'kind':'cuda','devices':['fixture']}}
    path=tmp_path/'evidence.json'
    report=dict(identity=identity,passed=True,sample_count=200,max_size=2,partner_rounds=3,
                label_mismatches=0,probability_failures=0,cases_sha256='fixture',parallel=2,
                tested_batch_sizes=[1,2],max_probability_delta=.01,
                input_coverage=dict(modalities=['text'],question_types=['noul'],choice_sizes=[],
                                    max_questions=1,shapes=['["text",[["noul",0]]]']),metrics={})
    for mode in ['single','parallel_single','batch_1','batch_2']:
        report['metrics'][mode]=dict(rows=600 if mode.startswith('batch_') else 200,
            label_mismatches=0,probability_failures=0,max_probability_delta=.01,
            batch_size=int(mode[6:]) if mode.startswith('batch_') else 1,
            parallel_requests=1 if mode=='single' else 2)
    report.update(schema_version='demiflow_batch_quality_v3',
                  policy={'probability_atol':.03,'label_margin':.02}, probability_atol=.03)
    for metric in report['metrics'].values():metric['label_changes']=2
    path.write_text(json.dumps(report))
    assert read_evidence(path,identity,2)
    report['policy']['label_margin']=.04
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError,match='margin'):read_evidence(path,identity,2)


def test_http_single_protocol_uses_configured_queue_and_body_limit():
    import httpx
    from demiflow.services.model_worker import make_server
    calls=[]
    backend=SimpleNamespace(batch=lambda rows:calls.append(rows) or [
        {'answers':{'q':{'type':'noul','noul':.5}}} for _ in rows])
    server=make_server(backend,'fixture',0,4,validation_batch_size=4,
                      batching={'max_size':4,'wait_ms':50,'max_request_bytes':256})
    thread=threading.Thread(target=server.serve_forever);thread.start()
    try:
        with httpx.Client(base_url=f'http://127.0.0.1:{server.server_port}',trust_env=False) as client:
            def one(i):return client.post('/v1/systemone',json={'model':'fixture',**request(str(i))})
            with ThreadPoolExecutor(max_workers=4) as pool:
                responses=list(pool.map(one,range(4)))
            assert all(r.status_code==200 for r in responses)
            assert len(calls)==1 and len(calls[0])==4
            assert client.post('/v1/systemone',json={'model':'fixture',**request('x'*500)}).status_code==413
            caps=client.get('/v1/models').json()['data'][0]['capabilities']
            assert caps['continuous_batching'] and not caps['batch']['quality_verified']
            assert caps['execution']['batch_sizes']=={'4':1}
    finally:
        server.shutdown();server.server_close();thread.join();server.adapter.close()


def test_standalone_service_declaration_needs_no_business_prompt(tmp_path):
    from demiflow.services.cli import declarations
    value={'schema_version':'demiflow_model_service_v1','model':{'name':'fixture','transport':'systemone',
        'deployment':{'profile':'pplx','weights':str(tmp_path),'revision':'r1','gpus':[0]}}}
    path=tmp_path/'service.json';path.write_text(json.dumps(value))
    assert declarations(path)[0][0]=='fixture'
    assert sorted(p.name for p in tmp_path.iterdir())==['service.json']
    value['business_threshold']=.8;path.write_text(json.dumps(value))
    with pytest.raises(ValueError,match='only'):declarations(path)
