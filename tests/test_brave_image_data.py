import json
import pytest
from demiflow.collect.native_search.brave_data import extract_data


def test_svelte_scalar_functions_and_old_literals_keep_originals():
    value={'url':'https://source.example/page','title':'Tree','source':'example',
           'properties':{'url':'https://images.example/original.png'},
           'thumbnail':{'src':'https://thumb.example/small.png'}}
    for body in [json.dumps(value), '(function(a,b){return {url:"https://source.example/page",title:"Tree",source:"example",properties:{url:a},thumbnail:{src:b}}}("https://images.example/original.png","https://thumb.example/small.png"))']:
        doc='<script>reportError()</script><script>kit.start(app,{data:[{}, {data:{body:{response:{results:['+body+']}}}}]});</script>'
        assert extract_data(doc)['data'][1]['data']['body']['response']['results']==[value]


@pytest.mark.parametrize('body',[
    'fetch("https://example.com")',
    '(function(a){return a}(fetch("https://example.com")))',
    '(function(a){return a}({x:1}))',
    '(function(a){return unknown}(1))',
    '(function(a){return a}(1,2))',
    '{x:1,x:2}',
    'x.y',
    '['*70+'0'+']'*70,
])
def test_unsupported_expressions_and_limits_fail_closed(body):
    with pytest.raises(ValueError):extract_data('<script>kit.start(app,{data:['+body+']});</script>')


def test_missing_or_oversized_data_does_not_mean_empty_success():
    with pytest.raises(ValueError):extract_data('<html>blocked</html>')
    with pytest.raises(ValueError):extract_data(' '* (8*1024*1024+1))


async def test_native_brave_images_keeps_original_and_reports_unsupported_page(tmp_path):
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    from threading import Thread
    from demiflow.collect import SearchConfig
    from demiflow.collect.native_search import NativeSearchSession
    requests=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            requests.append(self.path)
            body=b'<script>report()</script><script>kit.start(app,{data:[{}, {data:{body:{response:{results:[(function(a,b){return {url:"https://source.example/page",title:"Tree",source:"example",properties:{url:a},thumbnail:{src:b}}}("https://images.example/original.png","https://thumb.example/small.png"))]}}}}]});</script>'
            self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=Thread(target=server.serve_forever,daemon=True);thread.start()
    config=SearchConfig(engines=({'name':'brave.images','base_url':f'http://127.0.0.1:{server.server_port}/','enable_http':True,'network':{'enable_http':True,'enable_http2':False,'enable_http3':False},'enable_http3':False},),language='en-US',workers=1,request_concurrency=1,retries=0,timeout_s=10,source_interval_s=0)
    session=NativeSearchSession(cache_path=tmp_path/'search.sqlite',config=config)
    try:
        result=await session.search('tree')
        assert result['status']=='ok',result
        assert result['candidates'][0]['img_src']=='https://images.example/original.png'
        assert result['candidates'][0]['thumbnail_src']=='https://thumb.example/small.png'
        second=await session.search('tree',pageno=2)
        assert second['engine_receipts'][0]['status']=='unsupported_parameters'
        assert len(requests)==1
    finally:
        await session.aclose();server.shutdown();server.server_close();thread.join()
