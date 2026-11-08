"""Real Chromium, shared reading worker and authenticated private media.

No business service, platform grant, model, production data or B1 executor.
The fixture's HttpOnly cookie demonstrates native range transport, not an
existing production cookie capability (Forge API uses X-Forge-Token today).
"""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
from http.cookies import SimpleCookie
import secrets
import json
import os
from pathlib import Path
import threading
import time

from playwright.sync_api import expect
import pytest

from test_forge_markdown_reading import browser
from test_forge_outputs_media import PROJECT, OBJECT, image_bytes, wav_bytes, witness_and_raw
from demiflow.workbench.analysis import media
from demiflow.workbench.analysis import outputs
from demiflow.workbench.analysis import output_reader
from demiflow.workbench.analysis import contracts as c

WEB=Path(__file__).resolve().parents[1]/'demiflow/workbench/web'
PREFIX='/api/workspaces/'+PROJECT+'/analysis-media'


@contextmanager
def fixture():
    assets={name:(WEB/name).read_bytes() for name in ['analysis-render.js','analysis-output.js','analysis-output-media.js','analysis-output.css','reading.js','reading.css','markdown-worker.js','markdown-it.min.js']}
    png,wav=image_bytes(),wav_bytes(.15)
    refs={'image':media.probe_image(png,artifact_id=OBJECT,mime='image/png'), 'audio':media.probe_wav(wav,artifact_id=OBJECT)}
    state={'assets':{'image':png,'audio':wav},'refs':refs,'requests':[],'allowed':True,'expires':float('inf'),'bytes_sent':0,'tickets':{},'require_binding':False}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            if self.headers.get('X-Forge-Token')!='fixture-control-token' or int(self.headers.get('Content-Length','0'))>1024:
                self.send_error(403);return
            body=json.loads(self.rfile.read(int(self.headers.get('Content-Length','0'))))
            kind=body.get('kind')
            if self.path==PREFIX+'-revoke':
                state['tickets']={key:value for key,value in state['tickets'].items() if not(value['kind']==kind and ('reference' not in body or value['reference']==body['reference']))}
                data=b'{}';self.send_response(200)
            elif self.path in {PREFIX+'-prepare',PREFIX+'-renew'} and state['allowed'] and kind in state['refs'] and len(state['tickets'])<16:
                if 'reference' in body and body['reference']!=state['refs'][kind]:self.send_error(409);return
                cookie=SimpleCookie(self.headers.get('Cookie',''));current=cookie.get('fixture_media');old=state['tickets'].get(current.value) if current else None
                if self.path.endswith('-renew') and (not old or old['reference']!=body.get('reference')):self.send_error(403);return
                binding=c.digest(state['refs'][kind]);opaque=secrets.token_urlsafe(24);state['tickets'][opaque]={'kind':kind,'expires':time.monotonic()+300,'binding':binding,'reference':dict(state['refs'][kind])}
                data=json.dumps({'descriptor':state['refs'][kind],'url':PREFIX+'?kind='+kind+'&binding='+binding,'binding':binding}).encode();self.send_response(200)
                self.send_header('Set-Cookie','fixture_media='+opaque+'; HttpOnly; SameSite=Strict; Path='+PREFIX)
            else:self.send_error(403);return
            self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.send_header('Cache-Control','private, no-store');self.end_headers();self.wfile.write(data)
        def do_HEAD(self):self.do_GET()
        def do_GET(self):
            if len(state['requests'])>300:raise RuntimeError('private fixture request budget')
            state['requests'].append({'path':self.path,'range':self.headers.get('Range'),'cookie':bool(self.headers.get('Cookie')),'method':self.command})
            path=self.path.split('?')[0]
            if path=='/':
                body=b'''<!doctype html><meta charset="utf-8"><link rel="stylesheet" href="/reading.css"><link rel="stylesheet" href="/analysis-output.css"><style>body{margin:20px;font-family:sans-serif}main{max-width:900px}button{padding:5px;margin:3px}</style><script src="/markdown-it.min.js"></script><script src="/reading.js"></script><script src="/analysis-output-media.js"></script><script src="/analysis-render.js"></script><script src="/analysis-output.js"></script><main id="mount"></main>'''
                self.send_response(200);self.send_header('Content-Type','text/html');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
            if path[1:] in assets:
                body=assets[path[1:]];self.send_response(200);self.send_header('Content-Type','text/css' if path.endswith('.css') else 'text/javascript');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
            if path==PREFIX:
                from urllib.parse import parse_qs,urlsplit
                query=parse_qs(urlsplit(self.path).query);kind=query.get('kind',[''])[0]
                cookie=SimpleCookie(self.headers.get('Cookie',''));ticket=cookie.get('fixture_media')
                admitted=state['tickets'].get(ticket.value) if ticket else None
                if not state['allowed'] or time.monotonic()>=state['expires'] or not admitted or admitted['kind']!=kind or time.monotonic()>=admitted['expires']:
                    self.send_error(403);return
                if state['require_binding'] or 'binding' in query:
                    if set(query)!={'kind','binding'} or query.get('binding')!=[admitted['binding']] or admitted['binding']!=c.digest(state['refs'][kind]):self.send_error(403);return
                data=state['assets'].get(kind)
                if data is None:self.send_error(404);return
                try:start,end,status=media.byte_range(self.headers.get('Range'),len(data))
                except ValueError:self.send_error(416);return
                body=data[start:end+1];state['bytes_sent']+=len(body) if self.command!='HEAD' else 0;self.send_response(status);self.send_header('Content-Type',state['refs'][kind]['mime']);self.send_header('Accept-Ranges','bytes');self.send_header('Content-Range',f'bytes {start}-{end}/{len(data)}');self.send_header('Content-Length',str(len(body)));self.send_header('Cache-Control','private, no-store');self.end_headers();self.wfile.write(body) if self.command!='HEAD' else None;return
            self.send_error(404)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:yield 'http://127.0.0.1:'+str(server.server_port),state
    finally:server.shutdown();server.server_close();thread.join(timeout=3)


@contextmanager
def page_for(browser):
    with fixture() as (url,state):
        context=browser.new_context(viewport=dict(width=1100,height=900));page=context.new_page();page.set_default_timeout(15000);errors=[];page.on('pageerror',lambda error:errors.append(str(error)));state['browser_requests']=[];page.on('request',lambda request:state['browser_requests'].append(request.url))
        try:
            page.goto(url)
            page.evaluate('''() => {
              window.readers=new Set();window.fields=[];
              window.mountText=(parent,text)=>{const reader=ForgeReading.createTextReader({text});readers.add(reader);parent.append(reader.element);return reader;};
              window.disposeReaders=parent=>{for(const reader of readers)if(parent.contains(reader.element)){reader.dispose();readers.delete(reader);}};
              window.draw=options=>{const field=ForgeAnalysisRender.field({parent:document.getElementById('mount'),mountText,disposeReaders,...options});fields.push(field);return field.ready;};
              window.clear=()=>{ForgeAnalysisRender.disposeWithin(document.getElementById('mount'));disposeReaders(document.getElementById('mount'));document.getElementById('mount').replaceChildren();};
            }''')
            yield page,state,errors
        finally:context.close()


def capture(page,tmp_path,name):
    folder=Path(os.environ.get('FORGE_OUTPUT_EVIDENCE',tmp_path));folder.mkdir(parents=True,exist_ok=True)
    page.screenshot(path=str(folder/name),full_page=True)


def test_fields_preserve_types_reuse_markdown_and_isolate_html(browser,tmp_path):
    with page_for(browser) as (page,state,errors):
        page.evaluate(r'''async () => {
          await draw({format:'json',value:{null:null,false:false,zero:0,text:'<img src=x onerror=evil()>'}});
          await draw({format:'md',value:'# Markdown\n\n<script>globalThis.pwned=true</script> ![remote](https://bad.invalid/a.png)'});
          await draw({format:'code',value:'print("read only")'});
          await draw({format:'tag',value:['needs-review','数据标签']});
          await draw({format:'dialogue',value:[{role:'user',content:[{format:'text',value:'你好'}]},{role:'assistant',content:'回复'}]});
          await draw({format:'text',value:null});await draw({format:'text',value:false});await draw({format:'image',value:'https://bad.invalid/a.png'});
        }''')
        html=outputs.html_preview('<h3>Safe HTML</h3><script>parent.pwned=true</script><form action="https://bad.invalid"><input></form><img src="https://bad.invalid/x"><p onclick="bad()">结果</p>')
        page.evaluate('rendered=>draw({format:"html",value:rendered.source,preview:async()=>rendered})',html)
        expect(page.locator('iframe')).to_have_attribute('sandbox','')
        expect(page.frame_locator('iframe').get_by_text('Safe HTML')).to_be_visible()
        assert page.locator('main').get_by_text('字段类型与 text 不匹配',exact=False).count()==1
        assert page.evaluate('globalThis.pwned') is None
        assert not any('bad.invalid' in url for url in state['browser_requests'])
        assert page.locator('.analysis-message').count()==2
        for width in (1100,390):
            page.set_viewport_size(dict(width=width,height=900))
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        capture(page,tmp_path,'typed-fields-html-mobile.png')
        page.evaluate('clear()')
        assert page.evaluate('ForgeAnalysisRender.stats().fields')==0
        assert not errors,errors


def test_html_late_response_and_detach_cleanup_cannot_repopulate_old_case(browser):
    with page_for(browser) as (page,state,errors):
        page.evaluate('''() => {
          window.old=ForgeAnalysisRender.field({parent:document.getElementById('mount'),format:'html',value:'old',preview:()=>new Promise(resolve=>window.finishHTML=resolve)});
        }''')
        page.wait_for_function('typeof finishHTML === "function"')
        page.evaluate('''() => {old.dispose();old.element.remove();finishHTML({format:'html',sandbox:'',active_content:false,preview:'<p>late old case</p>'});}''')
        page.wait_for_timeout(50)
        assert page.locator('iframe').count()==0
        page.evaluate('draw({format:"text",value:"detached"})')
        page.evaluate('document.getElementById("mount").replaceChildren()')
        page.wait_for_function('ForgeAnalysisRender.stats().fields===0')
        assert not errors,errors


def transport(page,state):
    page.evaluate('''({refs,prefix})=>{
      window.transport=ForgeAnalysisMedia.createTransport({
        inspect:async request=>{const response=await fetch(prefix+'-prepare',{method:'POST',headers:{'Content-Type':'application/json','X-Forge-Token':'fixture-control-token'},body:JSON.stringify({kind:request.kind}),signal:request.signal});if(!response.ok)throw Error('媒体准备不可用');return response.json();},
        readRange:request=>fetch(prefix+'?kind='+request.kind,{headers:{Range:'bytes='+request.start+'-'+request.end},signal:request.signal}),
        playback:async request=>({url:prefix+'?kind='+request.kind,authorizeURL:url=>url.pathname===prefix,dispose:()=>{fetch(prefix+'-revoke',{method:'POST',headers:{'Content-Type':'application/json','X-Forge-Token':'fixture-control-token'},body:JSON.stringify({kind:request.kind})}).catch(()=>{});}})
      });
    }''',dict(refs=state['refs'],prefix=PREFIX))


def test_real_image_audio_range_playback_and_dispose_releases_every_object_url(browser,tmp_path):
    with page_for(browser) as (page,state,errors):
        transport(page,state)
        page.evaluate('''refs=>{
          window.urls={created:0,revoked:0};const create=URL.createObjectURL.bind(URL),revoke=URL.revokeObjectURL.bind(URL);
          URL.createObjectURL=blob=>{urls.created++;return create(blob);};URL.revokeObjectURL=url=>{urls.revoked++;return revoke(url);};
          return Promise.all([draw({format:'image',value:refs.image,resolveMedia:transport}),draw({format:'audio',value:refs.audio,resolveMedia:transport})]);
        }''',state['refs'])
        page.get_by_role('button',name='预览图片',exact=True).click();page.wait_for_function('document.querySelector("img.analysis-media")?.naturalWidth===8')
        page.get_by_role('button',name='加载音频',exact=True).click()
        expect(page.locator('audio')).to_have_attribute('preload','none')
        page.evaluate('document.querySelector("audio").play()');page.wait_for_function('document.querySelector("audio").readyState>=2')
        capture(page,tmp_path,'image-audio-real-decoders.png')
        assert any(item['range'] for item in state['requests'] if item['path'].startswith(PREFIX))
        assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')>0
        page.evaluate('clear()');page.wait_for_function('ForgeAnalysisMedia.stats().encodedBytes===0')
        assert page.evaluate('urls.created===urls.revoked')
        assert page.locator('audio,img.analysis-media').count()==0
        assert not errors,errors


def test_media_bad_digest_codec_missing_and_revoked_access_are_explicit(browser):
    with page_for(browser) as (page,state,errors):
        transport(page,state)
        bad=dict(state['refs']['image']);bad['sha256']='f'*64
        page.evaluate('ref=>draw({format:"image",value:ref,resolveMedia:transport})',bad)
        page.get_by_role('button',name='预览图片',exact=True).click();expect(page.get_by_role('status')).to_contain_text('摘要')
        assert page.locator('img.analysis-media').count()==0
        page.evaluate('clear()');state['allowed']=False
        page.evaluate('ref=>draw({format:"audio",value:ref,resolveMedia:transport})',state['refs']['audio'])
        page.get_by_role('button',name='加载音频',exact=True).click();expect(page.get_by_role('status')).to_contain_text('准备')
        assert page.locator('audio').count()==0 and page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        assert not errors,errors


def test_notebook_mime_mount_and_execution_identity(browser,tmp_path):
    items=[dict(output_type='display_data',data={'application/json':{'zero':0,'false':False,'null':None},'text/html':'<p>Notebook HTML</p>','application/javascript':'alert(1)'},metadata={})]
    witness,raw=witness_and_raw(items);output=output_reader.project_document(raw,witness)
    with page_for(browser) as (page,state,errors):
        page.evaluate('({output,project})=>{window.output=ForgeAnalysisOutput.mount({parent:document.getElementById("mount"),output,project,mountText,disposeReaders});}',dict(output=output,project=PROJECT))
        expect(page.locator('.analysis-output-provenance')).to_contain_text('generation')
        page.get_by_label('输出 0 MIME 格式').select_option(label='text/html')
        expect(page.frame_locator('iframe').get_by_text('Notebook HTML')).to_be_visible()
        page.get_by_label('输出 0 MIME 格式').select_option(label='application/javascript · 不可用/原值')
        expect(page.get_by_text('unsupported_MIME_no_widgets_or_scripts',exact=False)).to_be_visible()
        assert page.locator('iframe').count()==0
        capture(page,tmp_path,'notebook-mime-provenance.png')
        page.evaluate('output.dispose()');assert page.evaluate('ForgeAnalysisRender.stats().fields')==0
        assert not errors,errors


def test_private_webm_native_range_decoder_and_single_player_lifecycle(browser,tmp_path):
    with page_for(browser) as (page,state,errors):
        data=bytes(page.evaluate('''async()=>{
          const canvas=document.createElement('canvas');canvas.width=32;canvas.height=24;const context=canvas.getContext('2d');
          const stream=canvas.captureStream(10),chunks=[];
          const recorder=new MediaRecorder(stream,{mimeType:'video/webm;codecs=vp8',videoBitsPerSecond:32000});
          const done=new Promise(resolve=>{recorder.ondataavailable=event=>chunks.push(event.data);recorder.onstop=resolve;});
          recorder.start(50);let count=0;const timer=setInterval(()=>{context.fillStyle=count++%2?'blue':'red';context.fillRect(0,0,32,24);},40);
          await new Promise(resolve=>setTimeout(resolve,350));clearInterval(timer);recorder.stop();await done;stream.getTracks().forEach(track=>track.stop());
          return [...new Uint8Array(await new Blob(chunks).arrayBuffer())];
        }'''))
        assert 0<len(data)<1024*1024
        state['assets']['video']=data
        state['refs']['video']=dict(artifact_id=OBJECT,sha256=hashlib.sha256(data).hexdigest(),mime='video/webm',bytes=len(data),width=32,height=24,fps=10,codec='vp8',duration_s=.35)
        transport(page,state)
        page.evaluate('''refs=>Promise.all([draw({format:'audio',value:refs.audio,resolveMedia:transport}),draw({format:'video',value:refs.video,resolveMedia:transport})])''',state['refs'])
        page.get_by_role('button',name='加载音频',exact=True).click();page.wait_for_function('document.querySelector("audio") !== null');page.evaluate('document.querySelector("audio").play()')
        page.get_by_role('button',name='加载视频',exact=True).click();page.wait_for_function('document.querySelector("video") !== null')
        expect(page.locator('video')).to_have_attribute('preload','none')
        page.evaluate('document.querySelector("video").play()');page.wait_for_function('document.querySelector("video")?.videoWidth===32')
        page.wait_for_function('document.querySelector("audio")===null')
        assert page.evaluate('ForgeAnalysisMedia.stats().activePlayers')==1
        assert any(item['range'] and item['cookie'] for item in state['requests'] if 'kind=video' in item['path'])
        assert all('token=' not in item['path'] for item in state['requests'])
        capture(page,tmp_path,'private-webm-range-decoder.png')
        page.evaluate('clear()');assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        assert page.evaluate('ForgeAnalysisMedia.stats().activePlayers')==0
        assert not errors,errors


def test_async_media_dispose_and_missing_codec_do_not_keep_budget(browser):
    with page_for(browser) as (page,state,errors):
        page.evaluate('''ref=>draw({format:'audio',value:ref,resolveMedia:()=>new Promise(resolve=>window.resolveLate=resolve)})''',state['refs']['audio'])
        page.get_by_role('button',name='加载音频',exact=True).click();page.wait_for_function('typeof resolveLate === "function"')
        page.evaluate('''ref=>{clear();window.adapterDisposals=0;resolveLate({descriptor:ref,blob:new Blob(['bad'],{type:'audio/wav'}),dispose:async()=>{adapterDisposals++;throw Error("injected disconnected revoke");}});}''',state['refs']['audio'])
        page.wait_for_function('adapterDisposals===1')
        assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0 and page.locator('audio').count()==0
        wrong=dict(state['refs']['audio']);wrong['codec']='not-a-codec';wrong['mime']='audio/webm'
        page.evaluate('''ref=>draw({format:'audio',value:ref,resolveMedia:async()=>({descriptor:ref,blob:new Blob(['bad'],{type:ref.mime})})})''',wrong)
        page.get_by_role('button',name='加载音频',exact=True).click();expect(page.get_by_role('status')).to_contain_text('codec')
        assert page.locator('audio').count()==0 and page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        assert not errors,errors


def test_execution_mount_close_reopen_and_late_page_identity(browser):
    witness,raw=witness_and_raw();output=output_reader.project_document(raw,witness)
    with page_for(browser) as (page,state,errors):
        page.evaluate('''({output,project})=>{
          window.calls=0;window.slot=ForgeAnalysisOutput.mountExecution({parent:document.getElementById('mount'),project,
            execution:{id:output.provenance.execution_id,generation:output.provenance.kernel_generation,output_id:output.artifact.id},mountText,disposeReaders,
            api:async(name,params,body,project,options)=>{calls++;if(options.signal.aborted)throw Error('aborted');return output;}});
        }''',dict(output=output,project=PROJECT))
        page.locator('.analysis-output-slot>summary').click();expect(page.get_by_text('1',exact=True)).to_be_visible()
        page.locator('.analysis-output-slot>summary').click();expect(page.locator('.analysis-output')).to_have_count(0)
        page.locator('.analysis-output-slot>summary').click();expect(page.get_by_text('1',exact=True)).to_be_visible()
        assert page.evaluate('calls')==2
        page.evaluate('slot.dispose()');assert page.evaluate('ForgeAnalysisRender.stats().fields')==0
        assert not errors,errors


def test_narrow_integration_patch_mounts_output_inside_original_notebook_and_releases_on_mode_switch(browser,tmp_path):
    # Lane cannot edit analysis.js; exercise its generated narrow patch preview.
    # Once merged, the same test uses the real integrated file automatically.
    script=WEB/'analysis.js'
    if 'mountExecution' not in script.read_text():
        script=WEB.parents[3]/'evidence/integration-preview/demiflow/workbench/web/analysis.js'
        if not script.is_file():pytest.skip('Notebook integration patch not applied; no lane preview available')
    witness,raw=witness_and_raw();output=output_reader.project_document(raw,witness)
    with page_for(browser) as (page,state,errors):
        page.add_script_tag(path=str(WEB/'analysis-state.js'));page.add_script_tag(path=str(script))
        page.evaluate('''({project,output})=>{
          window.apiCalls=[];const store=new ForgeAnalysisState.Store();store.saveReference(project,'book',{session:{id:'session',generation:'generation'},bindings:{}});
          window.editor=ForgeAnalysis.mount({parent:document.getElementById('mount'),project,connected:true,permissions:{},mountText,disposeReaders,
            api:async(name,query={},body)=>{
              apiCalls.push({name,body:!!body});if(body)throw Error('must not execute/save while viewing output');
              if(name==='analysis-documents')return {items:[{id:'book',revision:1}]};
              if(name==='analysis-notebook')return {id:'book',revision:1,document:{title:'Real Notebook mount',cells:[{id:'cell',language:'python',source:'print(1)'}]},next_offset:null};
              if(name==='analysis-session')return {id:'session',generation:'generation',state:'idle'};
              if(name==='analysis-executions')return {items:[{id:'execution',cell_id:'cell',generation:'generation',source_sha256:output.provenance.source_sha256,notebook_revision:1,state:'finished',output_id:output.artifact.id}],next_cursor:null};
              if(name==='analysis-output')return output;
              throw Error('unexpected '+name);
            }});
        }''',dict(project=PROJECT,output=output))
        page.get_by_label('选择 Notebook',exact=True).select_option('book')
        page.locator('.analysis-cell .analysis-output-slot>summary').click()
        expect(page.locator('.analysis-cell .analysis-output')).to_be_visible()
        expect(page.locator('.analysis-cell .analysis-output-provenance')).to_contain_text('generation')
        capture(page,tmp_path,'original-notebook-output-mount.png')
        page.evaluate('editor.setMode(false)');assert page.locator('.analysis-output-slot').count()==0
        assert page.evaluate('ForgeAnalysisRender.stats().fields')==0
        page.evaluate('editor.setMode(true)');page.locator('.analysis-cell .analysis-output-slot>summary').click()
        expect(page.locator('.analysis-cell .analysis-output')).to_be_visible()
        assert page.evaluate('apiCalls.every(call=>!call.body)')
        page.evaluate('editor.dispose()');assert page.evaluate('ForgeAnalysisRender.stats().fields')==0
        assert not errors,errors


def test_native_video_above_client_blob_budget_uses_scoped_cookie_ranges_and_revokes_on_switch(browser,tmp_path):
    with page_for(browser) as (page,state,errors):
        small=bytes(page.evaluate('''async()=>{
          const canvas=document.createElement('canvas');canvas.width=32;canvas.height=24;const ctx=canvas.getContext('2d');
          const stream=canvas.captureStream(10),chunks=[],recorder=new MediaRecorder(stream,{mimeType:'video/webm;codecs=vp8'});
          const done=new Promise(resolve=>{recorder.ondataavailable=event=>chunks.push(event.data);recorder.onstop=resolve;});
          recorder.start(50);ctx.fillStyle='green';ctx.fillRect(0,0,32,24);await new Promise(resolve=>setTimeout(resolve,250));recorder.stop();await done;stream.getTracks().forEach(track=>track.stop());
          return [...new Uint8Array(await new Blob(chunks).arrayBuffer())];
        }'''))
        # A legal EBML Void pads the private WebM beyond the 32MiB blob budget.
        # This is an encoded-byte/range fixture, not a 40MiB decoded frame.
        padding=40*1024*1024
        data=small+b'\xec'+((1<<28)|padding).to_bytes(4,'big')+b'\0'*padding
        assert 32*1024*1024<len(data)<=64*1024*1024
        state['assets']['video']=data;state['refs']['video']=dict(artifact_id=OBJECT,sha256=hashlib.sha256(data).hexdigest(),mime='video/webm',bytes=len(data),width=32,height=24,fps=10,codec='vp8',duration_s=.25)
        transport(page,state)
        page.evaluate('ref=>draw({format:"video",value:ref,resolveMedia:transport})',state['refs']['video'])
        page.get_by_role('button',name='加载视频',exact=True).click();expect(page.locator('video')).to_be_visible()
        cookies=page.context.cookies([page.url.rstrip('/')+PREFIX])
        assert len(cookies)==1 and cookies[0]['httpOnly'] and cookies[0]['sameSite']=='Strict' and cookies[0]['path']==PREFIX
        assert page.evaluate('document.cookie')==''
        page.evaluate('document.querySelector("video").play()');page.wait_for_function('document.querySelector("video")?.videoWidth===32&&document.querySelector("video").readyState>=2')
        assert page.evaluate('document.querySelector("video").src.startsWith("blob:")') is False
        assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')<=1024*1024
        transferred=state['bytes_sent'];assert 0<transferred<32*1024*1024
        native=[item for item in state['requests'] if item['path'].startswith(PREFIX+'?')]
        assert native and all(item['range'] and item['cookie'] for item in native)
        assert all('token' not in item['path'].lower() and 'ticket' not in item['path'].lower() for item in state['requests'])
        state['expires']=0
        assert page.evaluate('prefix=>fetch(prefix+"?kind=video",{headers:{Range:"bytes=0-15"}}).then(response=>response.status)',PREFIX)==403
        state['expires']=float('inf');state['allowed']=False
        assert page.evaluate('prefix=>fetch(prefix+"?kind=video",{headers:{Range:"bytes=0-15"}}).then(response=>response.status)',PREFIX)==403
        assert page.evaluate('path=>fetch(path,{headers:{Range:"bytes=0-15"}}).then(response=>response.status)',PREFIX.replace(PROJECT,'w'+'9'*32)+'?kind=video') in (403,404)
        capture(page,tmp_path,'private-40MiB-video-range-cookie.png')
        with page.expect_response(lambda response:response.url.endswith('-revoke')) as revoked:page.evaluate('clear()')
        assert revoked.value.status==200 and state['tickets']=={}
        assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0 and page.locator('video').count()==0
        folder=Path(os.environ.get('FORGE_OUTPUT_EVIDENCE',tmp_path))
        (folder/'private-large-video-range-proof.json').write_text(json.dumps({'scope':'private_fixture_not_production_transport_or_ffprobe_proof','encoded_bytes':len(data),'bytes_sent_before_dispose':transferred,'client_reserved_encoded_bytes_max':1024*1024,'native_cookie_path':PREFIX,'http_only':True,'same_site':'Strict','expired_status':403,'revoked_status':403,'cross_project_rejected':True,'ticket_revoked_on_switch':True},indent=2))
        assert not errors,errors


@pytest.mark.parametrize('kind,load_label,tag',[('audio','加载音频','audio'),('image','预览图片','img.analysis-media')])
def test_review_media_manual_close_reopens_same_field_with_fresh_authority(browser,tmp_path,kind,load_label,tag):
    with page_for(browser) as (page,state,errors):
        transport(page,state)
        page.evaluate('''({ref,kind})=>{
          window.loads=0;window.released=[];window.urls={created:0,revoked:0};
          const create=URL.createObjectURL.bind(URL),revoke=URL.revokeObjectURL.bind(URL);
          URL.createObjectURL=blob=>{urls.created++;return create(blob);};URL.revokeObjectURL=url=>{urls.revoked++;return revoke(url);};
          return draw({format:kind,value:ref,resolveMedia:async request=>{const id=++loads,result=await transport(request);return {...result,dispose:()=>released.push(id)};}});
        }''',dict(ref=state['refs'][kind],kind=kind))
        for turn in range(3):
            expect(page.get_by_role('button',name=load_label,exact=True)).to_be_enabled()
            page.get_by_role('button',name=load_label,exact=True).click();expect(page.locator(tag)).to_have_count(1)
            if kind=='audio':
                page.evaluate('document.querySelector("audio").play()');page.wait_for_function('document.querySelector("audio").readyState>=2')
            else:page.wait_for_function('document.querySelector("img.analysis-media")?.naturalWidth===8')
            page.get_by_role('button',name='关闭预览',exact=True).click()
            expect(page.locator(tag)).to_have_count(0)
            expect(page.get_by_role('button',name=load_label,exact=True)).to_be_enabled()
            assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
            assert page.evaluate('ForgeAnalysisMedia.stats().activePlayers')==0
        assert page.evaluate('loads')==3 and page.evaluate('released')==[1,2,3]
        assert page.evaluate('urls.created')==3 and page.evaluate('urls.revoked')==3
        assert len([item for item in state['requests'] if item['path'].startswith(PREFIX+'?')])==3
        capture(page,tmp_path,'review-'+kind+'-manual-close-reopened.png')
        folder=Path(os.environ.get('FORGE_OUTPUT_EVIDENCE',tmp_path))
        (folder/('review-'+kind+'-manual-close-proof.json')).write_text(json.dumps({'finding':'P3-MEDIA-REOPEN-1','kind':kind,'scope':'real private authorized bytes/Chromium decode, not production/B1','reopens':3,'source_reads':3,'releases':page.evaluate('released'),'urls':page.evaluate('urls'),'stats':page.evaluate('ForgeAnalysisMedia.stats()')},indent=2))
        page.evaluate('fields.at(-1).dispose()');assert page.locator('main button').count()==0
        assert not errors,errors


def test_review_real_audio_player_switch_leaves_both_fields_reopenable(browser,tmp_path):
    with page_for(browser) as (page,state,errors):
        transport(page,state)
        page.evaluate('''ref=>{
          window.loads=0;window.released=[];
          window.resolveAudio=async request=>{const id=++loads,result=await transport(request);return {...result,dispose:()=>released.push(id)};};
          return Promise.all([draw({format:'audio',value:ref,resolveMedia:resolveAudio}),draw({format:'audio',value:ref,resolveMedia:resolveAudio})]);
        }''',state['refs']['audio'])
        first=page.locator('.analysis-field-value').nth(0);second=page.locator('.analysis-field-value').nth(1)
        first.get_by_role('button',name='加载音频',exact=True).click();expect(first.locator('audio')).to_have_count(1)
        page.evaluate('document.querySelectorAll(".analysis-field-value")[0].querySelector("audio").play()');page.wait_for_function('ForgeAnalysisMedia.stats().activePlayers===1')
        second.get_by_role('button',name='加载音频',exact=True).click();expect(second.locator('audio')).to_have_count(1)
        page.evaluate('document.querySelectorAll(".analysis-field-value")[1].querySelector("audio").play()')
        expect(first.locator('audio')).to_have_count(0);expect(first.get_by_role('button',name='加载音频',exact=True)).to_be_enabled()
        assert page.evaluate('ForgeAnalysisMedia.stats().activePlayers')==1
        first.get_by_role('button',name='加载音频',exact=True).click();expect(first.locator('audio')).to_have_count(1)
        page.evaluate('document.querySelectorAll(".analysis-field-value")[0].querySelector("audio").play()')
        expect(second.locator('audio')).to_have_count(0);expect(second.get_by_role('button',name='加载音频',exact=True)).to_be_enabled()
        assert page.locator('audio').count()==1 and page.evaluate('loads')==3 and page.evaluate('released')==[1,2]
        capture(page,tmp_path,'review-two-real-audio-players-reopened.png')
        first.get_by_role('button',name='关闭预览',exact=True).click()
        expect(first.get_by_role('button',name='加载音频',exact=True)).to_be_enabled()
        assert page.evaluate('released')==[1,2,3] and page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        folder=Path(os.environ.get('FORGE_OUTPUT_EVIDENCE',tmp_path))
        (folder/'review-real-player-switch-proof.json').write_text(json.dumps({'finding':'P3-MEDIA-REOPEN-1','scope':'two actual private WAV play() calls and re-open; no synthetic play event, not production/B1','source_reads':page.evaluate('loads'),'releases':page.evaluate('released'),'both_fields_enabled':first.get_by_role('button',name='加载音频',exact=True).is_enabled() and second.get_by_role('button',name='加载音频',exact=True).is_enabled(),'stats':page.evaluate('ForgeAnalysisMedia.stats()')},indent=2))
        page.evaluate('clear()');assert not errors,errors


def test_review_reopen_rechecks_revocation_and_binding_before_reading_bytes(browser):
    with page_for(browser) as (page,state,errors):
        transport(page,state)
        page.evaluate('ref=>draw({format:"audio",value:ref,resolveMedia:transport})',state['refs']['audio'])
        page.get_by_role('button',name='加载音频',exact=True).click();expect(page.locator('audio')).to_have_count(1)
        page.get_by_role('button',name='关闭预览',exact=True).click();state['allowed']=False
        sent=state['bytes_sent'];page.get_by_role('button',name='加载音频',exact=True).click()
        expect(page.get_by_role('status')).to_contain_text('准备不可用');expect(page.get_by_role('button',name='加载音频',exact=True)).to_be_enabled()
        assert page.locator('audio').count()==0 and state['bytes_sent']==sent
        state['allowed']=True
        original=state['refs']['audio'];state['refs']['audio']={**original,'sha256':'f'*64}
        page.get_by_role('button',name='加载音频',exact=True).click();expect(page.get_by_role('status')).to_contain_text('固定引用或摘要')
        assert page.locator('audio').count()==0 and state['bytes_sent']==sent
        state['refs']['audio']=original
        page.get_by_role('button',name='加载音频',exact=True).click();expect(page.locator('audio')).to_have_count(1)
        page.evaluate('clear()');assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        assert not errors,errors


def test_review_cancelled_late_load_cannot_clear_reopened_preview_or_release_its_scope(browser):
    with page_for(browser) as (page,state,errors):
        transport(page,state)
        page.evaluate('''ref=>{
          window.loads=0;window.released=[];window.signals=[];window.finalAbort=new AbortController();
          window.direct=ForgeAnalysisMedia.create({host:document.getElementById('mount'),kind:'audio',reference:ref,signal:finalAbort.signal,
            resolveMedia:async request=>{const id=++loads;signals.push(request.signal);const result={...await transport(request),dispose:()=>released.push(id)};
              if(id===1)return new Promise(resolve=>{window.late=()=>resolve(result);});return result;}});
        }''',state['refs']['audio'])
        page.get_by_role('button',name='加载音频',exact=True).click();page.wait_for_function('typeof late==="function"')
        page.evaluate('direct.close();direct.close()')
        assert page.evaluate('signals[0].aborted') and page.evaluate('ForgeAnalysisMedia.stats().pendingLoads')==1
        page.get_by_role('button',name='加载音频',exact=True).click();expect(page.locator('audio')).to_have_count(1)
        src=page.locator('audio').get_attribute('src');page.evaluate('late()')
        page.wait_for_function('released.length===1&&ForgeAnalysisMedia.stats().pendingLoads===0')
        assert page.locator('audio').get_attribute('src')==src and page.evaluate('released')==[1]
        assert page.evaluate('ForgeAnalysisMedia.stats().allocations')==1
        page.evaluate('direct.close();direct.close()');expect(page.get_by_role('button',name='加载音频',exact=True)).to_be_enabled()
        assert page.evaluate('released')==[1,2] and page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        page.evaluate('finalAbort.abort();direct.dispose();document.querySelector("main button").click()')
        expect(page.get_by_role('button',name='加载音频',exact=True)).to_be_disabled();assert page.evaluate('loads')==2
        assert not errors,errors


def test_review_cancelled_pending_loads_keep_a_bounded_charge_until_settled(browser):
    with page_for(browser) as (page,state,errors):
        # These held adapter replies deliberately ignore cancellation. The
        # production adapter must abort IO; this probes the client backstop.
        page.evaluate('''ref=>{
          window.waiters=[];window.releases=0;
          window.direct=ForgeAnalysisMedia.create({host:document.getElementById('mount'),kind:'audio',reference:ref,
            resolveMedia:request=>new Promise(resolve=>waiters.push(()=>resolve({descriptor:ref,dispose:()=>releases++}))) });
        }''',state['refs']['audio'])
        for _ in range(4):
            page.get_by_role('button',name='加载音频',exact=True).click();page.evaluate('direct.close();direct.close()')
        assert page.evaluate('waiters.length')==4 and page.evaluate('ForgeAnalysisMedia.stats().pendingLoads')==4
        page.get_by_role('button',name='加载音频',exact=True).click();expect(page.get_by_role('status')).to_contain_text('4 项预算')
        assert page.evaluate('waiters.length')==4
        page.evaluate('waiters.forEach(resolve=>resolve())');page.wait_for_function('ForgeAnalysisMedia.stats().pendingLoads===0')
        assert page.evaluate('releases')==4 and page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        expect(page.get_by_role('button',name='加载音频',exact=True)).to_be_enabled()
        page.evaluate('direct.dispose()');assert not errors,errors


@pytest.fixture
def registered_output(request):
    """Real presentations.register + SQLite, with only proposed origin field added.

    The source/deployment authority is a private fixture. No native table/B1
    claim. The backend module and its one-line wire delta are hashed evidence.
    A merged checkout can supply FORGE_PRESENTATIONS_MODULE directly.
    """
    import importlib.util
    import copy
    from types import SimpleNamespace
    from demiflow.workbench.analysis import contracts as c
    from demiflow.workbench.analysis.records import Access
    from test_forge_outputs_media import registered_database
    location=Path(os.environ.get('FORGE_PRESENTATIONS_MODULE',WEB.parents[3]/'evidence/origin-wiring-r3/presentations-server-preview.py'))
    if not location.is_file():
        location=WEB.parent/'analysis/presentations.py'
        if not location.is_file():pytest.skip('Actual presentations registry module/preview unavailable')
    name='demiflow.workbench.analysis._output_browser_registry'
    spec=importlib.util.spec_from_file_location(name,location);backend=importlib.util.module_from_spec(spec);spec.loader.exec_module(backend)
    numeric=getattr(request,'param',None)=='numeric'
    source=c.source_snapshot(dict(kind='fixed_lance',binding='b'*64,schema=[dict(name='number' if numeric else 'text',type='float64' if numeric else 'utf8',nullable=True)],rows=1,case_key=dict(kind='row_id',field=None),retention='external'))
    descriptor=dict(version=1,source_snapshot=source['digest'],case_key=source['case_key'],fields=[dict(id='f',field='number' if numeric else 'text',label='Material',role='input',format='json' if numeric else 'text')],grouping=[],filters=dict(op='range',field='number',value=dict(lower=1e-6,upper=1.0)) if numeric else None,sort=[],layout='card',questions=[dict(id='quality',title='质量',type='rating',required=False,minimum=1,maximum=5) if numeric else dict(id='quality',title='质量',type='text',required=False)])
    witness,raw=witness_and_raw([dict(output_type='display_data',data={outputs.CASE_MIME:descriptor,'text/plain':'Case output'},metadata={})])
    records,digest=registered_database(raw,witness);db=records.database.connection
    ddl=WEB.parents[3]/'evidence/origin-wiring-r3/presentation-table.sql'
    # A merged checkout already has the real current platform table. The
    # legacy standalone overlay is only needed when that schema is absent.
    registered=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='analysis_output_presentations'").fetchone()
    if registered is None and ddl.is_file():db.executescript(ddl.read_text())
    elif registered is None:
        db.execute('CREATE TABLE analysis_output_presentations (project TEXT NOT NULL,origin_id TEXT NOT NULL,execution_id TEXT NOT NULL,output_id TEXT NOT NULL,output_binding TEXT NOT NULL,output_index INTEGER NOT NULL,descriptor_digest TEXT NOT NULL,source_snapshot TEXT NOT NULL,descriptor BLOB NOT NULL,binding BLOB NOT NULL,PRIMARY KEY(project,origin_id),UNIQUE(project,output_id,output_binding,output_index,descriptor_digest))')
    db.execute('INSERT INTO source_snapshots VALUES (?,?,?)',(PROJECT,source['digest'],c.encoded(source)))
    db.execute('UPDATE analysis_sessions SET sources=? WHERE id=?',(c.encoded({'data':source['digest']}),'session'));db.commit()
    @contextmanager
    def transaction(*,payload_bytes):
        assert payload_bytes<=128*1024
        db.execute('BEGIN')
        try:yield db
        except BaseException:db.rollback();raise
        else:db.commit()
    records.database.transaction=transaction
    access=Access(PROJECT,'owner',lambda *args:True)
    witness=output_reader.registered_witness(records,access,'execution',expected_output_id=OBJECT,expected_binding=digest)
    page=output_reader.project_document(raw,witness)
    registered=backend.register(records,access,witness=witness,projection=page)
    rep=next(item for item in registered['items'][0]['representations'] if item['format']=='case')
    assert rep['definition_witness']['registered'] is True and rep['origin']==rep['definition_witness']['origin']
    stored=db.execute('SELECT descriptor,binding FROM analysis_output_presentations WHERE project=? AND origin_id=?',(PROJECT,rep['origin']['reference'])).fetchone()
    assert c.decode(bytes(stored['descriptor']))==descriptor
    backend.require(db,access,rep['origin']['reference'],source,{c.digest(question) for question in descriptor['questions']})
    try:yield SimpleNamespace(records=records,db=db,backend=backend,access=access,witness=witness,source=source,descriptor=descriptor,page=page,registered=registered,rep=rep)
    finally:db.close()


def mount_case_output(page,output):
    page.evaluate('''({output,project})=>{
      window.offered=output;window.adoptions=[];
      window.output=ForgeAnalysisOutput.mount({parent:document.getElementById('mount'),output,project,mountText,disposeReaders,
        onCase:(descriptor,evidence)=>{adoptions.push({descriptor:structuredClone(descriptor),evidence:structuredClone(evidence)});descriptor.questions[0].title='consumer edit';evidence.origin&&(evidence.origin.reference='consumer edit');}});
    }''',dict(output=output,project=PROJECT))


def test_origin_real_registry_projection_hands_exact_origin_and_fixed_descriptor(browser,tmp_path,registered_output):
    fixture=registered_output
    with page_for(browser) as (page,state,errors):
        mount_case_output(page,fixture.registered)
        # Later caller/consumer mutations cannot rewrite the fixed projection.
        page.evaluate('offered.items[0].representations.find(rep=>rep.format==="case").value.questions[0].title="forged caller mutation"')
        button=page.get_by_role('button',name='在 Case 工作台查看此描述',exact=True)
        button.click();button.click()
        adopted=page.evaluate('adoptions')
        assert len(adopted)==2
        for item in adopted:
            assert item['descriptor']==fixture.descriptor
            assert item['evidence']['origin']==fixture.rep['origin']
            assert item['evidence']['annotation_origin_registered'] is True
            assert item['evidence']['definition_witness']==fixture.rep['definition_witness']
        assert page.get_by_text('此输出尚未登记评审来源，可查看描述。').count()==0
        capture(page,tmp_path,'registered-notebook-origin-handoff.png')
        folder=Path(os.environ.get('FORGE_OUTPUT_EVIDENCE',tmp_path))
        (folder/'registered-origin-handoff-proof.json').write_text(json.dumps({'scope':'actual presentations.register + private SQLite + Chromium consumer handoff, not full Case save/B1','registered_origin':fixture.rep['origin'],'registered_binding':fixture.rep['definition_witness']['binding'],'fixed_descriptor':fixture.descriptor,'adoptions':adopted},ensure_ascii=False,indent=2))
        page.evaluate('output.dispose()');assert not errors,errors


@pytest.mark.parametrize('kind',['unregistered','truthy','missing_origin','different_origin','wrong_source','wrong_index'])
def test_origin_unregistered_or_inconsistent_projection_keeps_view_without_synthesized_authority(browser,registered_output,kind):
    import copy
    value=copy.deepcopy(registered_output.page if kind=='unregistered' else registered_output.registered)
    rep=next(rep for rep in value['items'][0]['representations'] if rep['format']=='case')
    if kind=='unregistered':rep['origin']=registered_output.rep['origin']
    if kind=='truthy':rep['definition_witness']['registered']='true'
    if kind=='missing_origin':rep.pop('origin')
    if kind=='different_origin':rep['origin']={'kind':'notebook_output','reference':'no'+'f'*64}
    if kind=='wrong_source':rep['definition_witness']['binding']['source_snapshot']='f'*64
    if kind=='wrong_index':rep['definition_witness']['binding']['output_index']=1
    with page_for(browser) as (page,state,errors):
        mount_case_output(page,value)
        page.get_by_role('button',name='在 Case 工作台查看此描述',exact=True).click()
        adoption=page.evaluate('adoptions[0]')
        assert adoption['descriptor']==registered_output.descriptor
        assert adoption['evidence']['origin'] is None and adoption['evidence']['definition_witness'] is None
        assert adoption['evidence']['annotation_origin_registered'] is False
        expect(page.get_by_text('此输出尚未登记评审来源，可查看描述。')).to_be_visible()
        page.evaluate('output.dispose()');assert not errors,errors


@pytest.mark.parametrize('kind',['descriptor','source','offered_origin','output_binding'])
def test_origin_real_registry_rejects_forged_descriptor_or_witness_without_committing(registered_output,kind):
    import copy
    from demiflow.forge.schema import ForgeError
    value=copy.deepcopy(registered_output.page)
    rep=next(rep for rep in value['items'][0]['representations'] if rep['format']=='case')
    if kind=='descriptor':rep['value']['questions'][0]['title']='伪造定义'
    if kind=='source':rep['value']['source_snapshot']='f'*64
    if kind=='offered_origin':rep['definition_witness']['origin']['reference']='no'+'f'*64
    if kind=='output_binding':value['artifact']['digest']='f'*64
    with pytest.raises(ForgeError):registered_output.backend.register(registered_output.records,registered_output.access,witness=registered_output.witness,projection=value)
    assert registered_output.db.execute('SELECT count(*) FROM analysis_output_presentations').fetchone()[0]==1


def test_origin_late_unregistered_response_and_removed_mime_button_cannot_replace_current_origin(browser,registered_output):
    fixture=registered_output
    with page_for(browser) as (page,state,errors):
        page.evaluate('''({oldPage,currentPage,project})=>{
          window.adoptions=[];window.calls=0;
          window.slot=ForgeAnalysisOutput.mountExecution({parent:document.getElementById('mount'),project,mountText,disposeReaders,
            execution:{id:currentPage.provenance.execution_id,generation:currentPage.provenance.kernel_generation,output_id:currentPage.artifact.id},
            onCase:(descriptor,evidence)=>adoptions.push({descriptor,evidence}),
            api:async()=>{if(++calls===1)return new Promise(resolve=>window.finishOld=()=>resolve(oldPage));return currentPage;}});
        }''',dict(oldPage=fixture.page,currentPage=fixture.registered,project=PROJECT))
        toggle=page.locator('.analysis-output-slot>summary')
        toggle.click();page.wait_for_function('typeof finishOld==="function"')
        toggle.click();page.wait_for_function('!document.querySelector(".analysis-output-slot").open')
        toggle.click();button=page.get_by_role('button',name='在 Case 工作台查看此描述',exact=True);button.click()
        page.evaluate('finishOld()');page.wait_for_timeout(50)
        button.click();assert page.evaluate('adoptions.length')==2
        assert all(item['evidence']['origin']==fixture.rep['origin'] and item['evidence']['annotation_origin_registered'] for item in page.evaluate('adoptions'))
        page.evaluate('window.removedCaseButton=[...document.querySelectorAll("button")].find(button=>button.textContent==="在 Case 工作台查看此描述")')
        page.get_by_label('输出 0 MIME 格式').select_option(label='text/plain')
        page.evaluate('removedCaseButton.click()');assert page.evaluate('adoptions.length')==2
        page.evaluate('slot.dispose();removedCaseButton.click()');assert page.evaluate('adoptions.length')==2
        assert not errors,errors


@pytest.mark.parametrize('registered_output',['numeric'],indirect=True)
def test_origin_numeric_filter_preserves_server_digest_without_python_js_rehash(browser,registered_output):
    fixture=registered_output
    with page_for(browser) as (page,state,errors):
        mount_case_output(page,fixture.registered)
        page.get_by_role('button',name='在 Case 工作台查看此描述',exact=True).click()
        adoption=page.evaluate('adoptions[0]')
        assert adoption['descriptor']['filters']['value']==dict(lower=1e-6,upper=1.0)
        assert adoption['evidence']['annotation_origin_registered'] and adoption['evidence']['origin']==fixture.rep['origin']
        assert adoption['evidence']['definition_witness']['binding']['descriptor_sha256']==fixture.rep['definition_witness']['binding']['descriptor_sha256']
        assert '0.000001' in page.evaluate('JSON.stringify(adoptions[0].descriptor)')
        assert '1e-06' in json.dumps(fixture.descriptor)
        page.evaluate('output.dispose()');assert not errors,errors


def private_webm(page,color):
    return bytes(page.evaluate('''async color=>{
      const canvas=document.createElement('canvas');canvas.width=32;canvas.height=24;const context=canvas.getContext('2d');
      const stream=canvas.captureStream(10),chunks=[],recorder=new MediaRecorder(stream,{mimeType:'video/webm;codecs=vp8'});
      const done=new Promise(resolve=>{recorder.ondataavailable=event=>chunks.push(event.data);recorder.onstop=resolve;});
      recorder.start(50);context.fillStyle=color;context.fillRect(0,0,32,24);await new Promise(resolve=>setTimeout(resolve,250));recorder.stop();await done;stream.getTracks().forEach(track=>track.stop());
      return [...new Uint8Array(await new Blob(chunks).arrayBuffer())];
    }''',color))


def video_ref(data,identifier=OBJECT):
    return dict(artifact_id=identifier,sha256=hashlib.sha256(data).hexdigest(),mime='video/webm',bytes=len(data),width=32,height=24,fps=10,codec='vp8',duration_s=.25)


def binding_transport(page):
    # This is the closed URL/cookie contract fixture, not production tickets.
    # Server-side fixed identity is tested separately from close/unload hooks.
    page.evaluate('''prefix=>{
      window.prepares=[];window.releases=[];window.holdA=false;window.badURL=false;
      window.boundTransport=ForgeAnalysisMedia.createTransport({inspect:async request=>({descriptor:request.reference}),readRange:()=>{throw Error('video must use native ranges');},
        playback:async request=>{
          if(holdA&&request.reference.artifact_id.endsWith('1'.repeat(32)))await new Promise(resolve=>{window.finishPrepareA=resolve;});
          request.beforePrepare?.();
          prepares.push({artifact_id:request.reference.artifact_id,videos:document.querySelectorAll('video').length,stats:ForgeAnalysisMedia.stats()});
          const body=JSON.stringify({kind:'video',reference:request.reference});
          const response=await fetch(prefix+'-prepare',{method:'POST',headers:{'Content-Type':'application/json','X-Forge-Token':'fixture-control-token'},body,signal:request.signal});
          if(!response.ok)throw Error('media prepare '+response.status);const value=await response.json(),target=new URL(value.url,location.href);
          if(target.origin!==location.origin||target.pathname!==prefix||!/^\\?kind=video&binding=[a-f0-9]{64}$/.test(target.search)||Object.keys(request.descriptor).some(key=>value.descriptor[key]!==request.descriptor[key]))throw Error('fixed media prepare identity differs');
          const expected=target.pathname+target.search;
          return {url:badURL?expected+'&unexpected=1':expected,
            authorizeURL:(url,load)=>load?.reference.artifact_id===request.reference.artifact_id&&url.origin===location.origin&&url.pathname+url.search===expected&&!url.hash&&!url.username&&!url.password,
            dispose:()=>{releases.push(request.reference.artifact_id);return fetch(prefix+'-revoke',{method:'POST',headers:{'Content-Type':'application/json','X-Forge-Token':'fixture-control-token'},body}).catch(()=>{});}};
        }});
    }''',PREFIX)


@pytest.mark.parametrize('mode',['preload','paused'])
def test_media_prepare_closes_old_video_before_new_cookie_including_unplayed_or_paused(browser,tmp_path,mode):
    with page_for(browser) as (page,state,errors):
        data=private_webm(page,'navy');first=video_ref(data);second=video_ref(data,'o'+'2'*32)
        state['require_binding']=True;state['assets']['video']=data;state['refs']['video']=first
        binding_transport(page)
        page.evaluate('refs=>Promise.all(refs.map(value=>draw({format:"video",value,resolveMedia:boundTransport})))',[first,second])
        first_host=page.locator('.analysis-field-value').nth(0);second_host=page.locator('.analysis-field-value').nth(1)
        first_host.get_by_role('button',name='加载视频',exact=True).click();expect(first_host.locator('video')).to_have_count(1)
        page.evaluate('window.oldVideo=document.querySelector("video")')
        if mode=='paused':
            page.evaluate('''async()=>{await oldVideo.play();oldVideo.pause();}''');page.wait_for_function('oldVideo.readyState>=2')
        else:assert page.evaluate('ForgeAnalysisMedia.stats().activePlayers')==0
        state['refs']['video']=second
        second_host.get_by_role('button',name='加载视频',exact=True).click();expect(second_host.locator('video')).to_have_count(1)
        expect(first_host.locator('video')).to_have_count(0);expect(first_host.get_by_role('button',name='加载视频',exact=True)).to_be_enabled()
        assert page.evaluate('oldVideo.paused&&!oldVideo.isConnected&&oldVideo.getAttribute("src")===null')
        assert page.evaluate('prepares[1].videos')==0 and page.evaluate('prepares[1].stats.activePlayers')==0
        assert page.evaluate('releases')==[first['artifact_id']]
        capture(page,tmp_path,'prepare-before-'+mode+'-video-released.png')
        page.evaluate('clear()');assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        assert not errors,errors


def test_media_prepare_closes_pending_owner_and_late_hook_cannot_prepare_over_new_video(browser):
    with page_for(browser) as (page,state,errors):
        data=private_webm(page,'green');first=video_ref(data);second=video_ref(data,'o'+'2'*32)
        state['require_binding']=True;state['assets']['video']=data;state['refs']['video']=second
        binding_transport(page)
        page.evaluate('''refs=>{holdA=true;return Promise.all(refs.map(value=>draw({format:'video',value,resolveMedia:boundTransport})));}''',[first,second])
        first_host=page.locator('.analysis-field-value').nth(0);second_host=page.locator('.analysis-field-value').nth(1)
        first_host.get_by_role('button',name='加载视频',exact=True).click();page.wait_for_function('typeof finishPrepareA==="function"')
        second_host.get_by_role('button',name='加载视频',exact=True).click();expect(second_host.locator('video')).to_have_count(1)
        url=second_host.locator('video').get_attribute('src')
        expect(first_host.get_by_role('button',name='加载视频',exact=True)).to_be_enabled()
        page.evaluate('finishPrepareA()');page.wait_for_function('ForgeAnalysisMedia.stats().pendingLoads===0')
        assert page.evaluate('prepares.map(item=>item.artifact_id)')==[second['artifact_id']]
        assert second_host.locator('video').get_attribute('src')==url and page.evaluate('ForgeAnalysisMedia.stats().allocations')==1
        state['refs']['video']=first
        page.evaluate('holdA=false');first_host.get_by_role('button',name='加载视频',exact=True).click();expect(first_host.locator('video')).to_have_count(1)
        expect(second_host.get_by_role('button',name='加载视频',exact=True)).to_be_enabled()
        assert page.evaluate('prepares.map(item=>item.artifact_id)')==[second['artifact_id'],first['artifact_id']]
        page.evaluate('clear()');assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        assert not errors,errors


def test_media_fixed_url_validator_rejects_extra_query_without_weakening_identity(browser):
    with page_for(browser) as (page,state,errors):
        data=private_webm(page,'red');ref=video_ref(data)
        state['require_binding']=True;state['assets']['video']=data;state['refs']['video']=ref
        binding_transport(page)
        page.evaluate('''ref=>{badURL=true;return draw({format:'video',value:ref,resolveMedia:boundTransport});}''',ref)
        with page.expect_response(lambda response:response.url.endswith('-revoke')) as cleanup:page.get_by_role('button',name='加载视频',exact=True).click()
        expect(page.get_by_role('status')).to_contain_text('固定路由核对');assert cleanup.value.status==200
        assert page.locator('video').count()==0 and not any('unexpected=' in request['path'] for request in state['requests'])
        assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
        page.evaluate('badURL=false');page.get_by_role('button',name='加载视频',exact=True).click();expect(page.locator('video')).to_have_count(1)
        page.evaluate('clear()');assert not errors,errors


def test_media_two_pages_fixed_url_cookie_get_head_and_late_old_controls_keep_reference(browser,tmp_path):
    with page_for(browser) as (page,state,errors):
        data_a=private_webm(page,'navy');data_b=private_webm(page,'red');ref_a=video_ref(data_a);ref_b=video_ref(data_b,'o'+'2'*32)
        assert ref_a['sha256']!=ref_b['sha256']
        state['require_binding']=True;state['assets']['video']=data_a;state['refs']['video']=ref_a
        binding_transport(page);page.evaluate('ref=>draw({format:"video",value:ref,resolveMedia:boundTransport})',ref_a)
        page.get_by_role('button',name='加载视频',exact=True).click();expect(page.locator('video')).to_have_count(1)
        url_a=page.locator('video').get_attribute('src')
        read='''async({url,method='GET'})=>{const response=await fetch(url,{method,headers:{Range:'bytes=0-'},cache:'no-store'});return {status:response.status,length:response.headers.get('Content-Length'),sha:method==='HEAD'||!response.ok?null:[...new Uint8Array(await crypto.subtle.digest('SHA-256',await response.arrayBuffer()))].map(value=>value.toString(16).padStart(2,'0')).join('')};}'''
        assert page.evaluate(read,{'url':url_a})['sha']==ref_a['sha256']
        other=page.context.new_page();other_errors=[];other.on('pageerror',lambda error:other_errors.append(str(error)))
        try:
            other.goto(page.url);binding_transport(other)
            state['assets']['video']=data_b;state['refs']['video']=ref_b
            other.evaluate('''ref=>{window.media=ForgeAnalysisRender.field({parent:document.getElementById('mount'),format:'video',value:ref,resolveMedia:boundTransport});return media.ready;}''',ref_b)
            other.get_by_role('button',name='加载视频',exact=True).click();expect(other.locator('video')).to_have_count(1)
            url_b=other.locator('video').get_attribute('src');assert url_a!=url_b
            # A same-page hook cannot close another tab. A native error may
            # independently close A after identity rejection; its old URL
            # must still reject B's cookie even outside the player DOM.
            for method in ('GET','HEAD'):
                assert page.evaluate(read,{'url':url_a,'method':method})['status']==403
                assert other.evaluate(read,{'url':url_b,'method':method})['status']==206
            controls='''async({prefix,action,reference})=>fetch(prefix+'-'+action,{method:'POST',headers:{'Content-Type':'application/json','X-Forge-Token':'fixture-control-token'},body:JSON.stringify({kind:'video',reference})}).then(response=>response.status)'''
            assert page.evaluate(controls,{'prefix':PREFIX,'action':'renew','reference':ref_a}) in (403,409)
            assert page.evaluate(controls,{'prefix':PREFIX,'action':'revoke','reference':ref_a})==200
            assert other.evaluate(read,{'url':url_b})['sha']==ref_b['sha256']
            state['allowed']=False
            for method in ('GET','HEAD'):assert other.evaluate(read,{'url':url_b,'method':method})['status']==403
            capture(page,tmp_path,'fixed-identity-old-tab-rejected.png');capture(other,tmp_path,'fixed-identity-new-tab-kept.png')
            folder=Path(os.environ.get('FORGE_OUTPUT_EVIDENCE',tmp_path))
            (folder/'two-page-fixed-identity-contract-proof.json').write_text(json.dumps({'scope':'real Chromium shared cookie context with private closed-URL ticket/byte contract fixture; NOT production media_http acceptance','url_a':url_a,'url_b':url_b,'old_a_get':403,'old_a_head':403,'new_b_get_head_before_revoke':206,'old_renew_rejected':True,'old_revoke_does_not_revoke_b':True,'b_bytes_sha256':ref_b['sha256'],'current_read_revoked_get_head':403},indent=2))
            with other.expect_response(lambda response:response.url.endswith('-revoke')):other.evaluate('media.dispose()')
            page.evaluate('clear()')
            assert page.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0 and other.evaluate('ForgeAnalysisMedia.stats().encodedBytes')==0
            assert not errors and not other_errors,(errors,other_errors)
        finally:other.close()
