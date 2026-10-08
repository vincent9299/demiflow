"""Generic contract tests; no external website or provider calls."""
import asyncio
import gzip
from pathlib import Path
import httpx
import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.collect.documents import store_document,read_document,DocumentError
from demiflow.collect.web import WebClient
from demiflow.execution.request_limits import RequestGate,ServiceStopped
from demiflow.execution.stream_lance import StreamLanceWriter


class Bytes(httpx.AsyncByteStream):
    def __init__(self,value):self.value=value
    async def __aiter__(self):
        for i in range(0,len(self.value),13):yield self.value[i:i+13]


def response(body=b'',code=200,**headers):
    return httpx.Response(code,headers=headers,stream=Bytes(body))


def test_document_structure_and_immutable_integrity(tmp_path):
    body=b'<html><title>Source</title><nav>navigation</nav><article><h1>Scope</h1><p>A condition.</p><table><tr><th>Stage</th><th>Value</th></tr><tr><td>Adult</td><td>X</td></tr></table><figure><figcaption>Caption</figcaption></figure></article><footer>footer</footer></html>'
    args=dict(url='https://example.org',final_url='https://example.org',content_type='text/html',retrieved_at='fixed')
    ref=store_document(tmp_path,body,**args)
    doc=read_document(ref)
    assert doc['source']['title']=='Source'
    assert {'heading','paragraph','table','caption'}<={b['kind'] for b in doc['blocks']}
    assert 'navigation' not in str(doc['blocks']) and 'footer' not in str(doc['blocks'])
    assert doc['blocks'][2]['text'].startswith('Stage | Value\nAdult | X')
    assert store_document(tmp_path,body,**args)==ref
    from urllib.parse import urlsplit
    Path(urlsplit(ref['uri']).path).write_bytes(b'corrupt')
    with pytest.raises(DocumentError,match='sha256'):read_document(ref)


def test_stream_writer_flush_replay_and_duplicate_keys(tmp_path):
    schema=pa.schema([('id',pa.string()),('value',pa.int64())])
    writer=StreamLanceWriter(tmp_path/'rows.lance',schema,key='id');writer.initialize()
    data.from_items([{'id':str(i),'value':i} for i in range(5)]).batch_map(
        writer.__call__,max_batch=2,flush_interval=.01,concurrency=1,queue_depth=2).run_stream()
    old=writer.reference();assert lance.dataset(**old).count_rows()==5
    assert writer.version==4  # empty snapshot + 2 + 2 + tail 1
    replay=StreamLanceWriter(tmp_path/'rows.lance',schema,key='id');replay.initialize()
    asyncio.run(replay([{'id':'0','value':9}]))
    assert lance.dataset(**old).count_rows()==5
    assert lance.dataset(**replay.reference()).count_rows()==1
    with pytest.raises(ValueError,match='unique'):asyncio.run(replay([{'id':'0','value':9}]))


def test_gate_limits_and_service_stop():
    async def run():
        gate=RequestGate(2,failures=2)
        async def one():
            async with gate.enter():await asyncio.sleep(.001)
        await asyncio.gather(*(one() for _ in range(12)))
        assert gate.peak==2 and gate.active==0
        gate.result(transient=True);gate.result(success=True);gate.result(transient=True)
        with pytest.raises(ServiceStopped):gate.result(transient=True)
        with pytest.raises(ServiceStopped):
            async with gate.enter():pass
    asyncio.run(run())


def test_isolated_parse_deadline_is_a_handled_technical_failure(tmp_path,monkeypatch):
    import time
    from demiflow.execution.isolation import run_isolated
    with pytest.raises(TimeoutError,match='exceeded'):
        run_isolated(time.sleep, 10, timeout_s=.2)
    def stalled(*args,**kwargs):
        return run_isolated(time.sleep, 10, timeout_s=.2)
    monkeypatch.setattr('demiflow.collect.web.run_isolated',stalled)
    async def run():
        client=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',search_url='https://example.org/search',host_interval_s=0)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(lambda req:response(b'body',**{'content-type':'text/plain'})))
        result=await client.fetch('https://example.org/page')
        assert result['status']=='parse_error' and result['raw_ref'] and result['document_ref'] is None
        assert client.snapshot_metrics()['outcomes']=={'download:ok':1,'fetch:parse_error':1}
        await client.aclose()
    asyncio.run(run())


def test_explicit_fetch_proxy_does_not_proxy_search(tmp_path,monkeypatch):
    original=httpx.AsyncClient;settings=[]
    def client(**kwargs):
        settings.append(dict(kwargs))
        proxy=kwargs.pop('proxy',None)
        def handle(req):
            if req.url.path=='/search':
                assert proxy is None
                return response(b'{"results":[]}',**{'content-type':'application/json'})
            assert proxy=='http://proxy.example:3128'
            return response(b'Actual document',**{'content-type':'text/plain'})
        return original(**kwargs,transport=httpx.MockTransport(handle))
    monkeypatch.setattr(httpx,'AsyncClient',client)
    async def run():
        web=WebClient(cache_path=tmp_path/'ledger',object_directory=tmp_path/'objects',
            search_url='http://localhost/search',fetch_proxy_url='http://proxy.example:3128',host_interval_s=0)
        assert (await web.search('q'))['status']=='no_results'
        assert (await web.fetch('https://example.org/body'))['status']=='ok'
        await web.aclose()
        assert web.client.is_closed and web.proxy_client.is_closed
    asyncio.run(run())
    assert len(settings)==2 and all(s['trust_env'] is False for s in settings)


def test_explicit_role_counting_scales_with_system_content():
    from demiflow.operator_llm.tokens import TextTokenCounter
    profile={'encoding':'o200k_base','tokens_per_message':3,'reply_tokens':6,'models':['fixture'],
             'context_tokens':40000,'max_output_tokens':8000,'verification':'synthetic repeated system accounting',
             'verified':False,'role_content_copies':{'system':2}}
    counter=TextTokenCounter(profile)
    messages=[{'role':'system','content':'Return JSON.'},{'role':'user','content':'银杏'}]
    base=counter.messages(messages)
    messages[0]['content']+=' A new property.'
    difference=counter.text(messages[0]['content'])-counter.text('Return JSON.')
    assert counter.messages(messages)-base==2*difference


def test_http_byte_caps_retry_and_cache(tmp_path):
    async def run():
        attempts=[]
        def handler(req):
            attempts.append(str(req.url))
            if req.url.path=='/search':return response(b'{"results":[]}',**{'content-type':'application/json'})
            if req.url.path=='/large':return response(gzip.compress(b'x'*10000),**{'content-encoding':'gzip','content-type':'text/plain'})
            if req.url.path=='/429':return response(code=429,**{'retry-after':'90'})
            if req.url.path=='/retry':return response(code=503)
            return response(b'Actual body.',**{'content-type':'text/plain'})
        client=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',search_url='https://example.org/search',
            max_bytes=1000,retry_delay_s=0,host_interval_s=0)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        one,two=await asyncio.gather(client.search('q'),client.search('q'))
        assert one['status']=='no_results' and one==two and len(attempts)==1
        assert (await client.search('q'))==one and len(attempts)==1
        large=await client.fetch('https://example.org/large')
        assert large['status']=='content_error' and 'decoded' in large['reason']
        limited=await client.fetch('https://example.org/429')
        assert limited['status']=='rate_limited' and len(limited['attempts'])==1
        bad=await client.fetch('https://example.org/retry')
        assert bad['status']=='http_error' and len(bad['attempts'])==2
        good=await client.fetch('https://example.org/body')
        assert good['status']=='ok' and read_document(good['document_ref'])['blocks'][0]['text']=='Actual body.'
        await client.aclose()
    asyncio.run(run())


def test_parser_failure_preserves_raw_snapshot_and_does_not_retry(tmp_path):
    async def run():
        calls=[]
        def handler(req):
            calls.append(str(req.url));return response(b'%PDF-1.7',**{'content-type':'application/pdf'})
        client=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',search_url='https://example.org/search',host_interval_s=0)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        r=await client.fetch('https://example.org/file')
        assert r['status']=='parse_error' and r['document_ref'] is None
        from demiflow.objects import ObjectRef
        assert ObjectRef(**r['raw_ref']).read()==b'%PDF-1.7'
        assert await client.fetch('https://example.org/file')==r and len(calls)==1
        await client.aclose()
    asyncio.run(run())


def test_cross_host_redirects_release_host_permits_and_limit_hops(tmp_path):
    async def run():
        def handler(req):
            if req.url.path=='/loop':return response(code=302,location=str(req.url))
            if req.url.host=='a.example':return response(code=302,location='https://b.example/done')
            return response(b'ok',**{'content-type':'text/plain'})
        client=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',search_url='https://s.example/search',host_interval_s=0,host_concurrency=1,timeout_s=1)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        out=await client.fetch('https://a.example/start')
        assert out['status']=='ok' and read_document(out['document_ref'])['source']['final_url']=='https://b.example/done'
        before=client.metrics['http_hops'];bad=await client.fetch('https://a.example/loop')
        assert bad['reason']=='redirect_limit' and client.metrics['http_hops']-before==4
        await client.aclose()
    asyncio.run(run())


def test_unknown_stream_append_is_reconciled_before_delivery(tmp_path,monkeypatch):
    import demiflow.execution.stream_lance as module
    schema=pa.schema([('id',pa.string()),('v',pa.int64())])
    writer=StreamLanceWriter(tmp_path/'out.lance',schema,key='id');writer.initialize()
    native=module.write_lance
    def commit_then_fail(spec,batches):
        native(spec,batches)
        raise OSError('receipt lost after successful commit')
    monkeypatch.setattr(module,'write_lance',commit_then_fail)
    delivered=asyncio.run(writer([{'id':'a','v':1}]))
    assert delivered==[{'id':'a','v':1}]
    assert lance.dataset(**writer.reference()).count_rows()==1


def test_request_gate_host_pacing_is_not_hidden_concurrency():
    async def run():
        gate=RequestGate(2,interval_s=.025);starts=[]
        async def one():
            async with gate.enter():starts.append(asyncio.get_running_loop().time());await asyncio.sleep(.03)
        await asyncio.gather(*(one() for _ in range(4)))
        assert gate.peak<=2 and all(b-a>=.024 for a,b in zip(starts,starts[1:]))
    asyncio.run(run())


def test_empty_results_with_engine_failures_are_not_normal_no_results(tmp_path):
    async def run():
        client=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',search_url='https://s.example/search',host_interval_s=0)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r:response(
            b'{"results":[],"unresponsive_engines":[["engine","timeout"]]}',**{'content-type':'application/json'})))
        result=await client.search('q')
        assert result['status']=='search_incomplete' and 'timeout' in result['reason']
        await client.aclose()
    asyncio.run(run())


def test_http_200_engine_failures_stop_new_searches_and_preserve_receipt(tmp_path):
    async def run():
        calls=[]
        def handler(req):
            calls.append(req.url.params['q'])
            if req.url.params['q']=='healthy':
                return response(b'{"results":[]}',**{'content-type':'application/json'})
            return response(b'{"results":[],"unresponsive_engines":[["engine","too many requests"]]}',
                            **{'content-type':'application/json'})
        client=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',
                         search_url='https://s.example/search',failure_limit=2,retries=1)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            assert (await client.search('first-failure'))['status']=='search_incomplete'
            assert (await client.search('healthy'))['status']=='no_results'
            assert client.search_gate.consecutive==0
            assert (await client.search('second-failure'))['status']=='search_incomplete'
            last=await client.search('third-failure')
            assert last['status']=='search_incomplete' and 'too many requests' in last['reason']
            assert client.snapshot_metrics()['admission']['search']['stopped']=='service_consecutive_failure_limit'
            assert await client.search('third-failure')==last  # saved receipt, not an interrupted placeholder
            with pytest.raises(ServiceStopped):await client.search('must-not-send')
            assert calls==['first-failure','healthy','second-failure','third-failure']
        finally:await client.aclose()
    asyncio.run(run())


def test_search_pacing_is_shared_by_queries_and_cache_does_not_consume_slots(tmp_path):
    async def run():
        starts=[]
        def handler(req):
            starts.append(asyncio.get_running_loop().time())
            return response(b'{"results":[]}',**{'content-type':'application/json'})
        client=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',
                         search_url='https://s.example/search',search_concurrency=3,search_interval_s=.05)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            results=await asyncio.gather(*(client.search(str(i)) for i in range(4)))
            assert all(r['status']=='no_results' for r in results)
            assert len(starts)==4 and all(b-a>=.045 for a,b in zip(starts,starts[1:]))
            await client.search('0')
            assert len(starts)==4 and client.search_gate.admitted==4
        finally:await client.aclose()
    asyncio.run(run())
