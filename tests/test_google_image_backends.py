import json
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlencode,urlsplit,parse_qs

import pytest
from demiflow.collect import SearchConfig
from demiflow.collect.native_search import NativeSearchSession
from demiflow.collect.native_search.google_images import parse_image_page
from demiflow.collect.native_search.google_search import request_url


def cards():
    return ''.join('<a href="/imgres?'+urlencode({'imgurl':'https://images.example/'+name+'.jpg',
        'imgrefurl':'https://source.example/page'})+'"><img src="/thumb" alt="实际标题 '+name+'"></a>'
        for name in ['one','two','one'])


def test_image_bindings_keep_distinct_originals_and_never_promote_thumbnails():
    value=parse_image_page(cards(), 'https://www.google.com/search',200,10)
    assert value[0]=='ok' and len(value[2])==2
    assert [r['img_src'] for r in value[2]]==['https://images.example/one.jpg','https://images.example/two.jpg']
    assert {r['url'] for r in value[2]}=={'https://source.example/page'}
    assert parse_image_page('<img src="https://images.example/thumbnail">','https://www.google.com/search',200,10)[0]=='parse_error'
    assert len(parse_image_page(cards(),'https://www.google.com/search',200,1)[2])==1
    assert parse_image_page('<form id="captcha-form"></form>'+cards(),'https://www.google.com/search',200,10)[0]=='captcha'
    for src in ['https://encrypted-tbn0.gstatic.com/images?q=tbn:abc','https://images.example/same']:
        markup='<a href="/imgres?'+urlencode({'imgurl':src,'imgrefurl':'https://source.example/page','tbnurl':src})+'">thumbnail</a>'
        assert parse_image_page(markup,'https://www.google.com/search',200,10)[0]=='parse_error'


def test_image_backend_uses_image_endpoint_and_image_parser():
    for backend in ('http','browser'):
        source=SearchConfig(engines=({'name':'google images','backend':backend},)).source_configs()[0]
        query=parse_qs(urlsplit(request_url('银杏',{'pageno':2,'language':'zh-CN','safesearch':2,'time_range':'month'},source)).query)
        assert query['udm']==['2'] and query['q']==['银杏'] and query['start']==['10']
        assert query['safe']==['active'] and query['tbs']==['qdr:m']


@pytest.fixture
def image_site():
    calls=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            calls.append(self.path)
            if self.path.startswith('/data'):
                body=json.dumps({'html':cards()}).encode();kind='application/json'
            else:
                query=parse_qs(urlsplit(self.path).query).get('q',[''])[0]
                body=('<form id="captcha-form"></form>' if query=='captcha' else
                    '<noscript><a href="/httpservice/retry/enablejs">Enable JS</a></noscript>'
                    '<script>fetch("/data").then(r=>r.json()).then(x=>document.body.insertAdjacentHTML("beforeend",x.html))</script>').encode()
                kind='text/html; charset=utf-8'
            self.send_response(200);self.send_header('Content-Type',kind)
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);worker=Thread(target=server.serve_forever,daemon=True);worker.start()
    yield f'http://127.0.0.1:{server.server_port}/search',calls
    server.shutdown();server.server_close();worker.join()


@pytest.mark.parametrize('backend',['http','browser'])
async def test_real_image_js_binding_replay_and_cleanup(image_site,tmp_path,backend):
    cfg=SearchConfig(engines=({'name':'google images','backend':backend,'base_url':image_site[0],'enable_http':True},),
        language='zh-CN',workers=1,request_concurrency=1,host_interval_s=0,timeout_s=20,retries=0)
    session=NativeSearchSession(cache_path=tmp_path/'images.sqlite',config=cfg)
    try:
        result=await session.search('银杏')
        receipt=result['engine_receipts'][0]
        assert receipt['backend']==backend and receipt['parser_version']=='google-images-dom-1'
        assert receipt['status']==('ok' if backend=='browser' else 'render_required'),result
        if backend=='browser':
            assert len(result['candidates'])==2
            assert result['candidates'][0]['img_src'].startswith('https://images.example/')
            assert result['candidates'][0]['url']=='https://source.example/page'
            assert not any('/thumb' in p for p in image_site[1])
        count=len(image_site[1]);assert await session.search('银杏')==result
        assert len(image_site[1])==count
        assert (await session.search('captcha'))['engine_receipts'][0]['status']=='captcha'
    finally:await session.aclose()
    assert session.snapshot_metrics()['worker_count']==0
