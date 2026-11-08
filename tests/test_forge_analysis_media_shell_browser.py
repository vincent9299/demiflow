"""Actual authenticated full project shell/media HTTP and persistent Case basis.

The private helper adapter forwards to real guarded SQLite, Web native worker,
Outbox, pending ingress, bounded PNG child and canonical range reader. Runtime,
watchdog and deferred scheduling are explicit doubles; no production/B1.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import threading

from playwright.sync_api import expect

from demiflow.workbench.projects import registry as console
from demiflow.forge.platform import content_digest
from demiflow.workbench.analysis import web_service
from demiflow.workbench.analysis.output_transport import OutputRange
from demiflow.forge.schema import ForgeError
from test_forge_analysis_media_source_export import declared,fixed
from urllib.error import HTTPError
from urllib.request import Request,urlopen
from test_forge_markdown_reading import browser
from test_forge_public_api import service


def wire(app,path,*,body=None,cookie=None,token=False,headers=None):
    fields=dict(headers or {})
    if cookie:fields['Cookie']=cookie
    if token:fields['X-Forge-Token']=app.token
    if body is not None:fields['Content-Type']='application/json'
    request=Request(app.origin+path,data=json.dumps(body).encode() if body is not None else None,headers=fields)
    try:response=urlopen(request,timeout=65)
    except HTTPError as error:response=error
    with response:return response.status,response.headers,response.read(2*1024*1024)


@contextmanager
def private_helper(config):
    root=Path(__file__).resolve().parents[1]
    process=subprocess.Popen([sys.executable,'-B','-u',str(Path(__file__).with_name('_forge_analysis_media_capture_driver.py')),'rpc',str(config)],
        cwd=root,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(root)),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    def read():
        with selectors.DefaultSelector() as poll:
            poll.register(process.stdout,selectors.EVENT_READ)
            assert poll.select(75),'private helper response timed out'
        line=process.stdout.readline();assert line,process.stderr.read();return json.loads(line)
    def call(value):process.stdin.write(json.dumps(value)+'\n');process.stdin.flush();return read()
    try:
        assert read()==dict(ready=True);yield call
    finally:
        process.stdin.close()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        error=process.stderr.read();process.stdout.close();process.stderr.close();assert process.returncode==0,error


def test_actual_shell_field_capture_preview_annotation_history_and_narrow_layout(browser,tmp_path,monkeypatch):
    root,raw=declared(tmp_path);locator,source=fixed(root)
    registry=tmp_path/'registry.json';project=console.add(registry,root)['workspace_id']
    console.migrate(registry,expected=content_digest(console.load_registry(registry)))
    outbox=tmp_path/'outbox';outbox.mkdir();outbox.chmod(0o2770)
    config=tmp_path/'fixture.json';config.write_text(json.dumps(dict(workspace=str(root),outbox=str(outbox),state=str(tmp_path/'state'),project=project,locator=locator,source=source)))
    calls=[];lock=threading.Lock()
    with private_helper(config) as invoke:
        class Helper(web_service.WebService):
            principal='fixture-reader'
            features=dict(case_media=True,annotation_media_basis=True)
            def __init__(self,server,config):self.server=server
            def ceiling(self):return dict(analysis_write=True,case_annotate=True,analysis_execute=False)
            def assert_ready(self):return True
            def close(self):pass
            def cancel_output(self,key):pass  # explicit synchronous fixture, not stop proof
            def call(self,key,name,payload):
                inventory=self.server.inventory();actual,_=console.resolve_workspace(inventory,key);assert actual==root
                permissions={'read':True,**self.server.effective_permissions(inventory['projects'][key])}
                with lock:
                    calls.append((name,payload))
                    response=invoke(dict(name=name,payload=payload,permissions=permissions))
                if 'error' in response:raise ForgeError(response['error'],code=response['code'])
                return response['result']
            def read_output(self,key,name,payload,**kwargs):
                value=self.call(key,name,payload)
                if value.get('format')=='fixture-verified-range':return OutputRange(value['artifact'],value['metadata'],bytes.fromhex(value['data']))
                return value
        monkeypatch.setattr(web_service,'WebService',Helper)
        with service(None,registry=registry,analysis_config='explicit-private-actual-media-chain-Runtime-double') as app:
            def grant(**changes):
                state=console.load_registry(registry)
                return console.set_capabilities(registry,project,expected=content_digest(state),
                    capabilities=dict(console.capabilities(state['projects'][project]),**changes),analysis_service=app.analysis)
            grant(analysis_write=True,case_annotate=True)
            context=browser.new_context(viewport=dict(width=1440,height=1100));page=context.new_page();page.set_default_timeout(65000);errors=[]
            page.on('pageerror',lambda error:errors.append(str(error)))
            sample_reads=[]
            page.on('request',lambda request:sample_reads.append(request.url) if request.url.split('?')[0].endswith('/sample') else None)
            try:
                page.goto(app.origin+'/#token='+app.token)
                page.get_by_role('button',name='数据',exact=True).click()
                expect(page.get_by_role('heading',name='Case 0',exact=True)).to_be_visible(timeout=65000)
                expect(page.locator('.analysis-workspace')).to_have_class('analysis-workspace analysis-case-source-shared')
                expect(page.locator('.data-source-context')).to_contain_text('固定版本 v1')
                header_source=page.locator('.data-source-context > span').get_attribute('title')
                assert len(header_source)==64
                expect(page.locator('.case-ui-context')).to_contain_text(header_source[:12])
                samples=page.get_by_role('button',name='数据样例与字段分布',exact=True)
                expect(samples).to_be_enabled()
                assert sample_reads==[], 'The hidden catalog sample must not delay Case source registration'
                samples.click()
                expect(page.locator('.data-samples .bound-table tbody tr').first).to_be_visible(timeout=65000)
                assert len(sample_reads)==1
                samples.click();samples.click()
                expect(page.locator('.data-samples .bound-table tbody tr').first).to_be_visible()
                assert len(sample_reads)==1, 'Reopening the same fixed sample must not reread it'
                assert page.locator('.data-source-context > span').get_attribute('title')==header_source
                samples.click()
                page.get_by_role('button',name='字段与评测配置',exact=True).click()
                page.get_by_label('预览格式 1',exact=True).select_option('image')
                page.get_by_role('button',name='字段与评测配置',exact=True).click()
                expect(page.get_by_role('button',name='固定媒体供评测',exact=True)).to_be_visible(timeout=65000)
                assert not any(name=='media-capture' for name,_ in calls)
                page.get_by_role('button',name='固定媒体供评测',exact=True).click()
                expect(page.get_by_role('button',name='预览图片',exact=True)).to_be_visible(timeout=65000)
                page.get_by_role('button',name='预览图片',exact=True).click()
                expect(page.locator('img.analysis-media')).to_be_visible(timeout=65000)
                assert page.locator('img.analysis-media').evaluate('(element)=>element.naturalWidth===8&&element.naturalHeight===6')
                page.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
                page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
                expect(page.locator('.analysis-case-review')).to_contain_text('当前答案 v1',timeout=65000)
                saved=[payload for name,payload in calls if name=='annotations-save'][-1]
                ref=saved['entries'][0]['media_basis'][0]
                assert ref['field']=='image_path' and ref['case_key']=='0' and ref['artifact_generation']==1
                # Both the catalog and this saved view point at real immutable
                # source snapshots. The second has a different fixed projection.
                prefix='/api/workspaces/'+project+'/'
                view_source=source
                if view_source['digest']==header_source:
                    status,_,raw_source=wire(app,prefix+'analysis-source-register',token=True,body=dict(locator={**locator,'columns':['image_path','image_sha']}))
                    assert status==200,raw_source
                    view_source=json.loads(raw_source)
                assert view_source['digest']!=header_source
                descriptor=dict(version=1,source_snapshot=view_source['digest'],case_key=view_source['case_key'],
                    fields=[dict(id='image_only',field='image_path',label='路径',role='input',format='text')],
                    grouping=[],filters=None,sort=[],layout='card',questions=[])
                status,_,raw_view=wire(app,prefix+'analysis-view-save',token=True,body=dict(request_id='header-fixed-view',view_id=None,expected_revision=0,descriptor=descriptor))
                assert status==200,raw_view
                different_view=json.loads(raw_view)
                newer=json.loads(json.dumps(descriptor));newer['fields'][0]['label']='更新后的路径标题'
                status,_,new_view=wire(app,prefix+'analysis-view-save',token=True,body=dict(request_id='header-fixed-view-v2',view_id=different_view['id'],expected_revision=1,descriptor=newer))
                assert status==200 and json.loads(new_view)['revision']==2,new_view
                page.get_by_role('button',name='保存视图',exact=True).click()
                expect(page.locator('.case-ui-context')).to_contain_text('已存视图 v1',timeout=65000)
                expect(page.locator('.case-ui-notice')).to_contain_text('视图已保存为 v1',timeout=65000)
                # The save refreshes both review heads and media lookup. Wait
                # for that actual UI operation before a third fixture request;
                # the real two-slot admission limit must remain unchanged.
                expect(page.get_by_role('button',name='预览图片',exact=True)).to_be_visible(timeout=65000)
                page.screenshot(path=str(tmp_path/'media-shell-desktop.png'),full_page=True)
                prefix='/api/workspaces/'+project+'/'
                code,headers,body=wire(app,prefix+'analysis-media',body=dict(action='prepare',reference=ref),token=True)
                assert code==200,body
                ticket=json.loads(body);cookie=headers['Set-Cookie'].split(';')[0]
                code,headers,body=wire(app,ticket['url'],cookie=cookie,headers=dict(Range='bytes=0-9'))
                assert code==206 and body==raw[:10]
                assert headers['X-Forge-Binding']==ref['expected_binding']
                grant(analysis_write=False)
                code,headers,body=wire(app,prefix+'analysis-media',body=dict(action='renew',reference=ref),token=True,cookie=cookie)
                assert code==200,body
                # Read-only fixed playback and text evaluation need no new
                # persistent capture grant. New capture still fails at HTTP.
                assert wire(app,prefix+'analysis-media-capture',token=True,body=dict(request_id='denied-capture',digest=ref['source_snapshot'],case_key='0',field='image_path'))[0]==422
                page.get_by_text('标注历史与恢复',exact=True).click();page.get_by_role('button',name='读取历史',exact=True).click()
                expect(page.locator('.analysis-case-review')).to_contain_text(ref['expected_sha256'],timeout=65000)
                page.set_viewport_size(dict(width=420,height=900))
                collapse=page.get_by_role('button',name='收起数据目录',exact=True)
                if collapse.count():collapse.click()
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
                page.screenshot(path=str(tmp_path/'media-shell-narrow.png'),full_page=True)
                page.get_by_role('button',name='Notebook',exact=True).click()
                expect(page.locator('img.analysis-media')).to_have_count(0)
                assert page.evaluate('ForgeAnalysisMedia.stats().allocations===0&&ForgeAnalysisMedia.stats().encodedBytes===0')
                assert len([name for name,_ in calls if name=='media-capture'])==1
                busy_views=[]
                def busy_before_view_read(route):
                    if different_view['id'] in route.request.url and len(busy_views)<2:
                        busy_views.append(True)
                        route.fulfill(status=503,content_type='application/json',body=json.dumps(dict(code='busy',error='Analysis request capacity is busy; no operation was dispatched')))
                    else:route.continue_()
                page.route('**/analysis-view?*',busy_before_view_read)
                page.get_by_role('button',name='Case 分析',exact=True).click()
                page.get_by_label('选择 Case 视图',exact=True).select_option(different_view['id'])
                expect(page.locator('.data-source-context > span')).to_have_attribute('title',view_source['digest'],timeout=65000)
                expect(page.locator('.case-ui-context')).to_contain_text(view_source['digest'][:12])
                expect(page.get_by_role('heading',name='Case 0',exact=True)).to_be_visible()
                expect(page.locator('img.analysis-media')).to_have_count(0)
                assert len(busy_views)==2
                assert len([name for name,_ in calls if name=='annotations-save'])==1
                page.screenshot(path=str(tmp_path/'media-shell-adopted-source-header.png'),full_page=True)
                # One generic fixed-view link reuses the original adoption
                # hook. Opening it neither reads an unrelated default table
                # sample nor runs code, and adoption fetches exactly v1.
                before_samples=len(sample_reads)
                reference=dict(v=2,workspace=project,kind='tables',view_id=different_view['id'],revision=1)
                link=page.evaluate('value=>ForgeView.encodeView(value)',reference)
                page.goto(app.origin+'/'+link);page.reload()
                expect(page.locator('.case-ui-presentation')).to_contain_text(different_view['id'],timeout=65000)
                expect(page.locator('.case-ui-presentation')).to_contain_text('v1')
                assert len(sample_reads)==before_samples
                page.get_by_role('button',name='采用已保存视图',exact=True).click()
                expect(page.locator('.data-source-context > span')).to_have_attribute('title',view_source['digest'],timeout=65000)
                expect(page.get_by_role('heading',name='Case 0',exact=True)).to_be_visible(timeout=65000)
                expect(page.get_by_role('heading',name='输入 · 路径',exact=True)).to_be_visible()
                expect(page.get_by_role('heading',name='输入 · 更新后的路径标题',exact=True)).to_have_count(0)
                assert len([name for name,_ in calls if name=='annotations-save'])==1
                assert not any(name in {'session-start','cell-submit'} for name,_ in calls)
                page.screenshot(path=str(tmp_path/'media-shell-fixed-view-link.png'),full_page=True)
                assert not errors,errors
            except BaseException:
                page.screenshot(path=str(tmp_path/'media-shell-failure.png'),full_page=True);raise
            finally:context.close()
