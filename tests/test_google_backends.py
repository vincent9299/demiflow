"""Google parsing and real HTTP/Chromium boundaries, using a local fixture."""
import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs

import pytest

from demiflow import data
from demiflow.collect import SearchConfig
from demiflow.collect.native_search import NativeSearchSession
from demiflow.collect.native_search.google_search import parse_page
from demiflow.collect.session import WebSession


@pytest.mark.parametrize('content,url,code,status', [
    ('<a href="https://example.org/a"><h3>Real title</h3></a>', 'https://www.google.com/search', 200, 'ok'),
    ('<a href="/httpservice/retry/enablejs">Enable JavaScript</a>', 'https://www.google.com/search', 200, 'render_required'),
    ('<form id="captcha-form"></form>', 'https://www.google.com/search', 200, 'captcha'),
    ('<html>blocked</html>', 'https://www.google.com/sorry/index', 200, 'captcha'),
    ('<html>hello</html>', 'https://consent.google.com/m', 200, 'consent_required'),
    ('<html>blocked</html>', 'https://www.google.com/search', 403, 'access_denied'),
    ('<html>rate</html>', 'https://www.google.com/search', 429, 'rate_limited'),
    ('<div id="topstuff">Your search did not match any documents.</div>', 'https://www.google.com/search', 200, 'no_results'),
    ('<html><body>Unknown layout</body></html>', 'https://www.google.com/search', 200, 'parse_error'),
])
def test_google_page_classification(content, url, code, status):
    assert parse_page(content,url,code,10)[0] == status


def test_result_url_and_snippet_are_same_card():
    status, _, rows = parse_page('''<div><a href="/url?q=https%3A%2F%2Fexample.org%2Ffirst"><h3>First</h3></a>
      <div class="VwiC3b">First only.</div></div>
      <div><a href="https://example.org/second"><h3>Second</h3></a><div class="VwiC3b">Second only.</div></div>''',
      'https://www.google.com/search',200,10)
    assert status == 'ok'
    assert [(r['url'],r['content']) for r in rows] == [
        ('https://example.org/first','First only.'),('https://example.org/second','Second only.')]
    assert parse_page('<a href="https://support.google.com/websearch/answer/86640"><h3>Google Help</h3></a>',
        'https://www.google.com/search',200,10)[0]=='ok'


async def test_render_required_does_not_rotate_or_penalize_proxy_routes(tmp_path,monkeypatch):
    from demiflow.collect.search_routes import SearchRoutePool
    calls=[]
    async def call(self,context,message):
        calls.append(message['query'])
        return {'status':'render_required','reason':'fixture','results':[],
                'http':[{'host':'search.example','http_status':200,'bytes':30,'elapsed_s':.01}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    pool=SearchRoutePool(cache_path=tmp_path/'routes.sqlite',
        config=SearchConfig(engines=['google'],language='en'),
        routes=[{'name':n,'proxy':'http://'+n+'.example:3128','interval_s':0} for n in ('a','b')],
        max_route_attempts=2,failure_limit=1,cooldown_s=60)
    try:
        first=await pool.search('needs JS')
        assert len(calls)==1 and first['status']=='search_failed'
        assert all(r['failures']==0 and r['until']==0 for r in pool.routes)
        assert await pool.search('needs JS')==first and len(calls)==1
        assert not pool.query_gate.stopped
    finally:await pool.aclose()


@pytest.mark.parametrize('value', [{'max_requests':0}, {'max_requests':257}, {'ready_timeout_s':float('nan')}, {'unknown':1}])
def test_browser_budget_validation(value):
    with pytest.raises(ValueError): SearchConfig(browser=value)


def test_backend_declaration_and_roundtrip():
    for backend in ('http','browser'):
        c = SearchConfig(engines=({'name':'google','backend':backend},))
        assert SearchConfig.from_mapping(c.snapshot()).snapshot() == c.snapshot()
        assert c.source_configs()[0]['backend'] == backend
    with pytest.raises(ValueError): SearchConfig(engines=({'name':'bing','backend':'browser'},)).source_configs()


def test_browser_proxy_tunnels_are_separate_from_http_admission(tmp_path):
    cfg=SearchConfig(engines=({'name':'google','backend':'browser'},),workers=2,request_concurrency=1)
    session=NativeSearchSession(cache_path=tmp_path/'unopened.sqlite',config=cfg)
    assert session.proxy_pool.options['max_connections']==16
    assert session.config.request_concurrency==1
    with pytest.raises(ValueError):SearchConfig(browser={'proxy_connections_per_worker':33})


@pytest.fixture
def site():
    calls=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_GET(self):
            p=urlsplit(self.path); q=parse_qs(p.query).get('q',[''])[0]
            calls.append((p.path,q))
            code=200; kind='text/html; charset=utf-8'; headers={}
            if p.path == '/data':
                body=b'{"title":"Rendered evidence"}';kind='application/json'
            elif q == 'redirect':
                code=302;headers['Location']='/search?q=plain';body=b''
            elif q == 'loop':
                code=302;headers['Location']='/search?q=loop';body=b''
            elif q == 'captcha': body=b'<form id="captcha-form"></form>'
            elif q == 'empty': body=b'<div id="topstuff">Your search did not match any documents.</div>'
            elif q == 'oversize': body=b'x'*100000
            elif q == 'hang': time.sleep(10);body=b'<html>late</html>'
            elif q.startswith('js'):
                body=b'''<html><body><noscript><a href="/httpservice/retry/enablejs">Enable JS</a></noscript>
                  <script>fetch('/data').then(r=>r.json()).then(x=>{
                  let a=document.createElement('a');a.href='https://example.org/evidence';
                  a.innerHTML='<h3>'+x.title+'</h3>';document.body.appendChild(a)});</script></body></html>'''
            elif q == 'many':
                body=b'''<html><script>for(let i=0;i<100;i++) fetch('/data?i='+i)</script></html>'''
            else: body=b'<html><a href="https://example.org/plain"><h3>Plain evidence</h3></a></html>'
            try:
                self.send_response(code)
                for k,v in headers.items():self.send_header(k,v)
                self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
            except (BrokenPipeError,ConnectionResetError):pass
    http=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    t=threading.Thread(target=http.serve_forever,daemon=True);t.start()
    yield f'http://127.0.0.1:{http.server_port}/search',calls
    http.shutdown();http.server_close();t.join()


def config(site, backend, **kwargs):
    return SearchConfig(engines=({'name':'google','backend':backend,'base_url':site[0], 'enable_http':True},),
        language='en',workers=1,host_interval_s=0,timeout_s=20,**kwargs)


async def test_http_backend_classifies_js_and_retains_failed_receipt(site,tmp_path):
    s=NativeSearchSession(cache_path=tmp_path/'http.sqlite',config=config(site,'http',query_failure_limit=1,retries=3))
    try:
        value=await s.search('js')
        assert value['engine_receipts'][0]['status']=='render_required',value
        assert value['engine_receipts'][0]['backend']=='http'
        assert len(site[1])==1
        assert await s.search('js')==value
        assert (await s.search('plain'))['candidates'][0]['title']=='Plain evidence'
        assert len(site[1])==2
    finally:await s.aclose()


async def test_browser_real_js_cache_context_reuse_and_shutdown(site,tmp_path):
    s=NativeSearchSession(cache_path=tmp_path/'browser.sqlite',config=config(site,'browser'))
    try:
        value=await s.search('js')
        assert value['status']=='ok',value
        assert value['candidates'][0]['title']=='Rendered evidence'
        receipt=value['engine_receipts'][0]
        assert receipt['backend']=='browser'
        assert len(receipt['attempts'][0]['http'])==2
        assert await s.search('js')==value
        second=await s.search('js2')
        assert second['status']=='ok',second
        assert json.loads(second['engine_receipts'][0]['browser_metrics_json'])['context_query']==2
        assert s.snapshot_metrics()['http_peak']==1
        import psutil
        children=psutil.Process(s.workers[0].process.pid).children(recursive=True)
        from pathlib import Path
        directory=Path(s.workers[0].directory.name)
        assert list(directory.glob('playwright_chromiumdev_profile-*'))
    finally:await s.aclose()
    assert all(not p.is_running() or p.status()=='zombie' for p in children)
    assert not directory.exists()


@pytest.mark.parametrize('query,options,status',[
    ('redirect',{},'ok'),('loop',{'max_redirects':1},'redirect_limit'),
    ('oversize',{'browser':{'max_total_bytes':1024}},'response_too_large'),
    ('many',{'browser':{'max_requests':3}},'request_budget'),
    ('plain',{'browser':{'max_rss_bytes':1}},'resource_limit'),
    ('empty',{},'no_results'),
])
async def test_browser_resource_boundaries(site,tmp_path,query,options,status):
    s=NativeSearchSession(cache_path=tmp_path/'bounds.sqlite',config=config(site,'browser',**options))
    try:
        value=await s.search(query)
        assert value['engine_receipts'][0]['status']==status,value
        assert s.http_gate.active==0
        if query=='many':assert len(site[1])<=3
    finally:await s.aclose()


async def test_browser_cancellation_keeps_receipt_and_releases_owned_processes(site,tmp_path):
    import psutil
    s=NativeSearchSession(cache_path=tmp_path/'cancel.sqlite',config=config(site,'browser'))
    try:
        assert (await s.search('plain'))['status']=='ok'
        children=psutil.Process(s.workers[0].process.pid).children(recursive=True)
        task=asyncio.create_task(s.search('hang'))
        async with asyncio.timeout(5):
            while not any(q=='hang' for _,q in site[1]):await asyncio.sleep(.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert s.http_gate.active==0
        saved=await s.search('hang')
        assert saved['engine_receipts'][0]['status']=='interrupted'
        assert sum(q=='hang' for _,q in site[1])==1
        assert all(not p.is_running() or p.status()=='zombie' for p in children)
    finally:await s.aclose()


def test_dataset_http_backend_receipt_schema(site,tmp_path):
    from demiflow.collect.contracts import SEARCH_RESULT
    import pyarrow as pa
    web=WebSession(cache_path=tmp_path/'dataset.sqlite',object_directory=tmp_path/'objects',search=config(site,'http'))
    rows=[]
    data.from_items([{'requests':[{'request_id':'one','query':'plain'}]}]).search_web(
        requests='requests',output='results',session=web).map(lambda r:rows.append(r) or r).run_stream()
    value=rows[0]['results'][0]
    table=pa.Table.from_pylist([{'result':value}],schema=pa.schema([('result',SEARCH_RESULT)]))
    assert table.to_pylist()[0]['result']['engine_receipts'][0]['backend']=='http'


async def test_browser_authenticated_proxy_and_no_server_credential_leak(tmp_path, monkeypatch):
    """Real Chromium: a 407 challenge must work with our CDP interception."""
    import base64
    from demiflow.collect import Secret
    expected=b'Basic '+base64.b64encode(b'fixture:proxy_password')
    seen=[];tasks=set();leaked=[]
    async def proxy(reader,writer):
        task=asyncio.current_task();tasks.add(task)
        try:
            head=await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'),5)
            headers=dict(line.split(b': ',1) for line in head.split(b'\r\n')[1:] if b': ' in line)
            authorized=headers.get(b'Proxy-Authorization')==expected
            seen.append(authorized)
            if not authorized:
                writer.write(b'HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm="fixture"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            elif b'q=server-auth' in head.split(b'\r\n')[0]:
                # A target server asking for Basic auth must never get the proxy password.
                leaked.append(b'Authorization' in headers)
                writer.write(b'HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm="target"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            else:
                body=b'<html><a href="https://example.org/evidence"><h3>Proxy evidence</h3></a></html>'
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(body)).encode()+b'\r\nConnection: close\r\n\r\n'+body)
            await writer.drain()
        finally:
            writer.close();await writer.wait_closed();tasks.discard(task)
    server=await asyncio.start_server(proxy,'127.0.0.1',0)
    monkeypatch.setenv('BROWSER_FIXTURE_PROXY',f'http://fixture:proxy_password@127.0.0.1:{server.sockets[0].getsockname()[1]}')
    s=NativeSearchSession(cache_path=tmp_path/'proxy.sqlite',config=config(('http://fixture.example/search',[]),'browser',proxy=Secret('BROWSER_FIXTURE_PROXY')))
    try:
        value=await s.search('plain')
        assert value['status']=='ok',value
        assert value['candidates'][0]['title']=='Proxy evidence'
        denied=await s.search('server-auth')
        assert denied['engine_receipts'][0]['status']=='authentication_error',denied
        assert True in seen and False in seen
        assert leaked and not any(leaked)
    finally:
        await s.aclose();server.close();await server.wait_closed()
        await asyncio.gather(*tasks,return_exceptions=True)


async def test_browser_parent_pipe_closure_releases_children(site,tmp_path):
    import psutil
    s=NativeSearchSession(cache_path=tmp_path/'parent.sqlite',config=config(site,'browser'))
    try:
        assert (await s.search('plain'))['status']=='ok'
        worker=s.workers[0]
        children=psutil.Process(worker.process.pid).children(recursive=True)
        worker.process.stdin.close()
        await asyncio.wait_for(worker.process.wait(),10)
        assert all(not p.is_running() or p.status()=='zombie' for p in children)
    finally:await s.aclose()


async def test_browser_connect_proxy_tls_failure_is_prompt(tmp_path,monkeypatch):
    """Real Chromium CONNECT authentication; a broken TLS target stays bounded."""
    import base64
    from demiflow.collect import Secret
    seen=[];tasks=set()
    async def proxy(reader,writer):
        task=asyncio.current_task();tasks.add(task)
        try:
            head=await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'),5)
            authorized=b'Proxy-Authorization: Basic '+base64.b64encode(b'fixture:password') in head
            seen.append(authorized)
            if not authorized:
                writer.write(b'HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm="fixture"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            else:
                writer.write(b'HTTP/1.1 200 Connection Established\r\n\r\n')
            await writer.drain()
            if authorized:await asyncio.wait_for(reader.read(4096),5)
        finally:
            writer.close();await writer.wait_closed();tasks.discard(task)
    server=await asyncio.start_server(proxy,'127.0.0.1',0)
    monkeypatch.setenv('BROWSER_CONNECT_PROXY',f'http://fixture:password@127.0.0.1:{server.sockets[0].getsockname()[1]}')
    s=NativeSearchSession(cache_path=tmp_path/'connect.sqlite',config=config(('https://fixture.example/search',[]),'browser',proxy=Secret('BROWSER_CONNECT_PROXY')))
    try:
        started=time.monotonic();value=await s.search('plain');e=value['engine_receipts'][0]
        # TLS deliberately closes: report that promptly, not an admission timeout.
        assert e['status']=='network_error',value
        assert time.monotonic()-started<10
        assert True in seen and False in seen
        assert s.snapshot_metrics()['http_requests']==1
    finally:
        await s.aclose();server.close();await server.wait_closed()
        await asyncio.gather(*tasks,return_exceptions=True)


async def test_cdp_connect_auth_event_replay_does_not_deadlock(monkeypatch):
    """Replay the two Fetch IDs/one Network ID seen in the real Google trace."""
    from types import SimpleNamespace
    from demiflow.collect.native_search.browser import BrowserTransport
    sent=[];continued={key:asyncio.Event() for key in ('first','restart')}
    class CDP:
        def __init__(self):self.handlers={}
        def on(self,name,fn):self.handlers[name]=fn
        async def send(self,name,params=None):
            if name=='Page.getFrameTree':return {'frameTree':{'frame':{'id':'main'}}}
            if name=='Fetch.continueRequest':continued[params['requestId']].set()
    cdp=CDP()
    class Page:
        url='https://fixture.example/search'
        def on(self,*args):pass
        async def goto(self,*args,**kwargs):
            base={'networkId':'network-one','frameId':'main','resourceType':'Document',
                  'request':{'url':self.url,'method':'GET'}}
            cdp.handlers['Fetch.requestPaused']({**base,'requestId':'first'})
            await asyncio.wait_for(continued['first'].wait(),1)
            await cdp.handlers['Fetch.authRequired']({'requestId':'first',
                'authChallenge':{'source':'Proxy','origin':'http://127.0.0.1:1234'}})
            cdp.handlers['Fetch.requestPaused']({**base,'requestId':'restart'})
            await asyncio.wait_for(continued['restart'].wait(),1)
            cdp.handlers['Network.loadingFinished']({'requestId':'network-one'})
            return SimpleNamespace(status=200)
        async def wait_for_function(self,*args,**kwargs):pass
        async def evaluate(self,*args):return '<html>rendered</html>'
        async def close(self):pass
    class Context:
        async def new_page(self):return Page()
        async def new_cdp_session(self,page):return cdp
    async def start(self,*args):
        self.context=Context();self.browser=SimpleNamespace(version='fixture')
    monkeypatch.setattr(BrowserTransport,'start',start)
    b=BrowserTransport({'timeout_s':3,'max_bytes':4096,'max_redirects':5},sent.append,lambda:{'event':'http_grant'})
    try:
        result=await b._fetch('https://fixture.example/search','http://fixture:password@127.0.0.1:1234',
            'en',['fixture.example'],'true')
        assert result['metrics']['auth_restarts']==1
        assert result['metrics']['requests']==1
        assert [e['event'] for e in sent]==['http_open','http_close']
    finally:b.loop.close()
