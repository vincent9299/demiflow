"""Real HTTP/cookie lifecycle with explicit synthetic registered-byte adapter.

No codec/OCI/B1 claim: fixed bytes let this test inspect transport fencing.
"""
import hashlib
import json
from urllib.error import HTTPError
from urllib.request import Request,urlopen
from urllib.parse import urlencode

from demiflow.workbench.analysis import media
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.output_transport import OutputRange
from test_forge_analysis_http import helper,registered
from test_forge_public_api import service
from test_forge_workspace import isolated,demo
from test_forge_markdown_reading import browser


def wire(app,path,*,body=None,cookie=None,token=False,method=None,headers=None):
    fields=dict(headers or {})
    if cookie:fields['Cookie']=cookie
    if token:fields['X-Forge-Token']=app.token
    if body is not None:fields['Content-Type']='application/json'
    request=Request(app.origin+path,data=json.dumps(body).encode() if body is not None else None,
        headers=fields,method=method)
    try:response=urlopen(request,timeout=15)
    except HTTPError as error:response=error
    with response:return response.status,response.headers,response.read(2*1024*1024)


def test_actual_cookie_ranges_are_current_scoped_and_late_cleanup_cannot_revoke_new_player(tmp_path,isolated,helper):
    registry,project,prefix=registered(tmp_path,isolated)
    with service(None,registry=registry,analysis_config='explicit-fake-media-byte-adapter') as app:
        adapter=app.analysis;adapter.principal='owner';adapter.allowed=True;adapter.deny_after_read=False
        data=b'x'*(2*1024*1024+11);sha=hashlib.sha256(data).hexdigest();reads=[];cancelled=[]
        def descriptor(reference):
            return dict(artifact_id=reference['expected_output_id'],output_index=reference['output_index'],mime='video/webm',
                sha256=sha,bytes=len(data),width=320,height=240,fps=25,duration_s=1,codec='vp8')
        adapter.media_current=lambda key,ref,desc:adapter.allowed and desc==descriptor(ref)
        adapter.media_inspect=lambda key,ref,**kwargs:descriptor(ref)
        adapter.cancel_output=lambda key:cancelled.append(key)
        def read_output(key,operation,payload,**kwargs):
            reads.append((key,operation,payload,kwargs));start,end,status=media.byte_range(payload['range'],len(data))
            value=OutputRange(dict(id=payload['expected_output_id'],generation=1,digest=payload['expected_binding'],sha256=sha,bytes=len(data)),
                dict(status=status,mime='video/webm',start=start,end=end,total=len(data),descriptor=descriptor(payload)),data[start:end+1])
            if adapter.deny_after_read:adapter.allowed=False
            return value
        adapter.read_output=read_output
        ref=dict(execution_id='execution',expected_output_id='o'+'1'*32,expected_binding='a'*64,
            output_index=0,mime='video/webm',expected_sha256=sha)
        route=prefix+'analysis-media'
        prepare=dict(action='prepare',reference=ref)
        assert wire(app,route,body=prepare)[0]==403
        assert wire(app,route+'?binding='+c.digest(ref),headers={'Range':'bytes=0-9'})[0]==422
        code,headers,raw=wire(app,route,body=prepare,token=True)
        assert code==200,raw
        public=json.loads(raw);cookie=headers['Set-Cookie'].split(';')[0]
        url=public['url'];assert url==route+'?binding='+c.digest(ref) and public['expires_in']==300
        assert 'HttpOnly' in headers['Set-Cookie'] and 'SameSite=Strict' in headers['Set-Cookie']
        assert 'Path='+route in headers['Set-Cookie'] and not any(key in public for key in ('token','key','cookie'))
        assert cookie.split('=',1)[1] not in raw.decode()
        code,headers,raw=wire(app,url,cookie=cookie,headers={'Range':'bytes=0-99'})
        assert code==206 and raw==data[:100] and headers['Content-Range']==f'bytes 0-99/{len(data)}'
        assert reads[-1][3]['cancellation_tag'] and headers['X-Forge-Binding']==ref['expected_binding']
        assert wire(app,url,cookie=cookie,method='HEAD')[1]['Content-Length']==str(len(data))
        before=len(reads)
        assert wire(app,url,cookie=cookie,headers={'Range':'bytes=0-1048576'})[0]==416
        assert len(reads)==before
        assert wire(app,route+'?token=never-accepted',cookie=cookie,headers={'Range':'bytes=0-9'})[0]==400
        assert wire(app,url,cookie=cookie,headers={'Range':'bytes=0-9','Origin':'http://localhost:1'})[0]==422
        assert wire(app,prefix+'analysis-documents?kind=notebook',cookie=cookie)[0]==403
        # Replacement and cleanup share the constant route, but original
        # public reference comparisons prevent late old cleanup/renewal.
        ref2={**ref,'expected_output_id':'o'+'2'*32,'expected_binding':'b'*64}
        code,headers,raw=wire(app,route,body=dict(action='prepare',reference=ref2),token=True,cookie=cookie)
        assert code==200,raw
        cookie2=headers['Set-Cookie'].split(';')[0];url2=json.loads(raw)['url']
        assert wire(app,url,cookie=cookie2,headers={'Range':'bytes=0-9'})[0]==409
        assert wire(app,url,cookie=cookie2,method='HEAD')[0]==409
        code,headers,raw=wire(app,route,body=dict(action='revoke',reference=ref),token=True,cookie=cookie2)
        assert code==200 and json.loads(raw)==dict(revoked=False) and 'Set-Cookie' not in headers
        assert wire(app,route,body=dict(action='renew',reference=ref),token=True,cookie=cookie2)[0]==409
        assert wire(app,route,body=dict(action='renew',reference=ref2),token=True,cookie=cookie2)[0]==200
        assert wire(app,url2,cookie=cookie2,headers={'Range':'bytes=100-'})[0]==206
        assert len(reads[-1][2]['range'])<96
        adapter.deny_after_read=True
        assert wire(app,url2,cookie=cookie2,headers={'Range':'bytes=0-9'})[0]==422
        # The synthetic worker returned bytes, but the second current check
        # removed the ticket and none was returned to the HTTP consumer.
        assert wire(app,url2,cookie=cookie2,headers={'Range':'bytes=0-9'})[0]==422
        assert wire(app,route,body=dict(action='revoke',reference=ref2),token=True,cookie=cookie2)[0]==200
        adapter.allowed=True;adapter.deny_after_read=False
        code,headers,raw=wire(app,route,body=prepare,token=True)
        cookie3=headers['Set-Cookie'].split(';')[0];assert code==200
        # Console token rotation invalidates already-issued playback cookies.
        app.token='changed-console-token'
        assert wire(app,url,cookie=cookie3,headers={'Range':'bytes=0-9'})[0]==422


def test_inspection_route_is_token_authenticated_typed_and_read_only(tmp_path,isolated,helper):
    registry,project,prefix=registered(tmp_path,isolated)
    with service(None,registry=registry,analysis_config='explicit-fake-media-inspect') as app:
        app.analysis.media_inspect=lambda key,ref,**kwargs:dict(artifact_id=ref['expected_output_id'],output_index=ref['output_index'])
        params=dict(execution_id='one',expected_output_id='o'+'1'*32,expected_binding='a'*64,
            output_index=2,mime='image/png',expected_sha256='b'*64)
        path=prefix+'analysis-output-inspect?'+urlencode(params)
        assert wire(app,path)[0]==403
        code,headers,raw=wire(app,path,token=True)
        assert code==200 and json.loads(raw)['descriptor']['output_index']==2
        params['output_index']='true'
        assert wire(app,prefix+'analysis-output-inspect?'+urlencode(params),token=True)[0]==400


def test_two_real_browser_tabs_cannot_read_new_object_through_old_player_url(browser,tmp_path,isolated,helper):
    registry,project,prefix=registered(tmp_path,isolated)
    with service(None,registry=registry,analysis_config='explicit-two-tab-byte-adapter') as app:
        adapter=app.analysis;adapter.principal='owner';adapter.allowed=True
        objects={'o'+'1'*32:b'A'*2048,'o'+'2'*32:b'B'*2048}
        def descriptor(reference):
            raw=objects[reference['expected_output_id']]
            return dict(artifact_id=reference['expected_output_id'],output_index=0,mime='video/webm',sha256=hashlib.sha256(raw).hexdigest(),
                bytes=len(raw),width=320,height=240,duration_s=1,codec='vp8',fps=25)
        adapter.media_inspect=lambda key,ref,**kwargs:descriptor(ref)
        adapter.media_current=lambda key,ref,desc:adapter.allowed and descriptor(ref)==desc
        adapter.cancel_output=lambda tag:None
        def output(key,operation,ref,**kwargs):
            raw=objects[ref['expected_output_id']];start,end,status=media.byte_range(ref['range'],len(raw))
            return OutputRange(dict(id=ref['expected_output_id'],generation=1,digest=ref['expected_binding']),
                dict(status=status,mime='video/webm',start=start,end=end,total=len(raw),descriptor=descriptor(ref)),raw[start:end+1])
        adapter.read_output=output
        def ref(index):
            identifier='o'+str(index)*32
            return dict(execution_id='execution'+str(index),expected_output_id=identifier,expected_binding=str(index)*64,
                output_index=0,mime='video/webm',expected_sha256=hashlib.sha256(objects[identifier]).hexdigest())
        context=browser.new_context()
        try:
            context.route(app.origin+'/',lambda route:route.fulfill(content_type='text/html',body='<video preload="none"></video>'))
            first=context.new_page();second=context.new_page()
            for page in (first,second):page.goto(app.origin+'/')
            prepare='''async({route,reference,token})=>{const response=await fetch(route,{method:'POST',headers:{'X-Forge-Token':token,'Content-Type':'application/json'},body:JSON.stringify({action:'prepare',reference})});return {status:response.status,...await response.json()};}'''
            a=first.evaluate(prepare,dict(route=prefix+'analysis-media',reference=ref(1),token=app.token))
            assert a['status']==200
            first.evaluate('url=>{const video=document.querySelector("video");video.src=url;video.pause();}',a['url'])
            read='''async({url,method='GET'})=>{const response=await fetch(url,{method,headers:method==='GET'?{Range:'bytes=0-9'}:{}});return {status:response.status,body:method==='HEAD'?'':await response.text()};}'''
            assert first.evaluate(read,dict(url=a['url']))==dict(status=206,body='A'*10)
            b=second.evaluate(prepare,dict(route=prefix+'analysis-media',reference=ref(2),token=app.token))
            assert b['status']==200 and a['url']!=b['url']
            assert first.evaluate(read,dict(url=a['url']))['status']==409
            assert first.evaluate(read,dict(url=a['url'],method='HEAD'))['status']==409
            assert second.evaluate(read,dict(url=b['url']))==dict(status=206,body='B'*10)
            assert first.evaluate('document.querySelector("video").paused')
            assert 'DemiFlowMedia' not in first.evaluate('document.cookie')
            # Old tab cleanup has the exact old public reference and cannot
            # clear the new tab's HttpOnly cookie.
            revoked=first.evaluate('''async({route,reference,token})=>{const r=await fetch(route,{method:'POST',headers:{'X-Forge-Token':token,'Content-Type':'application/json'},body:JSON.stringify({action:'revoke',reference})});return await r.json();}''',dict(route=prefix+'analysis-media',reference=ref(1),token=app.token))
            assert revoked==dict(revoked=False)
            assert second.evaluate(read,dict(url=b['url']))['status']==206
            adapter.allowed=False
            assert second.evaluate(read,dict(url=b['url']))['status']==422
        finally:context.close()
