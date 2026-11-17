"""Case UI through real authenticated project HTTP, metadata/SQLite and Lance.

The analysis-helper/authority transport is an explicit test-only adapter into a
private metadata subprocess. Native table reads use the real bounded workspace
worker. No B1, OCI/Jupyter kernel, business pipeline or paid model is started.
Controlled delayed/lost browser responses are identified in each test.
"""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading

import lance
import pyarrow as pa
import pytest
from playwright.sync_api import expect

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import web_service
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from test_forge_analysis_records import Controller
from test_forge_markdown_reading import browser
from test_forge_public_api import service

ASSETS=Path(__file__).resolve().parents[1]/'demiflow/workbench/web'
CHILD=Path(__file__).with_name('_forge_analysis_case_ui_controller.py')
expect.set_options(timeout=18000)

HTML='''<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="stylesheet" href="/style.css"><link rel="stylesheet" href="/analysis.css"><link rel="stylesheet" href="/reading.css"><link rel="stylesheet" href="/analysis-case.css"><main id="mount"></main>'''


@contextmanager
def metadata_process(directory):
    import os
    process=subprocess.Popen([sys.executable,'-B','-u',str(CHILD),str(directory)],
        cwd=ASSETS.parents[2],env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(ASSETS.parents[2])),
        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    client=Controller(process)
    try:
        assert client.read()==dict(ready=True);yield client
    finally:
        process.stdin.close()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        error=process.stderr.read().decode()
        process.stdout.close();process.stderr.close()
        assert process.returncode==0,error


def source_files(root):
    return {str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob('*') if path.is_file()}


@pytest.fixture
def owned_case(browser,tmp_path,monkeypatch):
    root=tmp_path/'owned-project';workspace.init_workspace(root,name='Case UI acceptance')
    (root/'data').mkdir();(root/'tables').mkdir()
    control=workspace.read_control(root/'workspace.json');previous=content_digest(control)
    control['locations']['local']=dict(root='data',writable=True)
    workspace.write_control(root/'workspace.json',control,expected=previous)
    pa.set_cpu_count(1);pa.set_io_thread_count(1)
    for alias,count in [('cases',24),('other',3),('over_budget',1001)]:
        table=pa.table(dict(input=[f'问题 {i}' for i in range(count)],output=[f'**回答 {i}**' for i in range(count)],
            bucket=[f'组{i%3}' for i in range(count)],score=pa.array(range(count),type=pa.float64()),flag=[i%2==0 for i in range(count)]))
        lance.write_dataset(table,str(root/'data'/(alias+'.lance')))
        workspace.attach_table(root,alias,relative_uri=alias+'.lance',version=1,dataset_id=alias,schema_name='case_ui',schema_version='1')
    locators={}
    for alias in ('cases','other','over_budget'):
        info=workspace.describe_table(root,alias)
        locators[alias]=dict(label=alias+' · fixed v1',locator=dict(kind='workspace_table',alias=alias,binding=info['binding']['digest'],columns=['input','output','bucket','score','flag']))
    registry=tmp_path/'registry.json';project=console.add(registry,root)['workspace_id']
    console.migrate(registry,expected=content_digest(console.load_registry(registry)))
    calls=[];lock=threading.Lock()
    with metadata_process(tmp_path/'platform-records') as client:
        class TestHelper(web_service.WebService):
            def __init__(self,server,config):self.server=server
            def ceiling(self):return dict(analysis_execute=False,analysis_write=True,case_annotate=True)
            def execution_profile(self):return None # Metadata-only double has no admitted runtime.
            def assert_ready(self):return True
            def stop_generation(self,*args,**kwargs):return dict(accepted=True,cleanup_proven=True)
            def close(self):pass
            def read_output(self,project,operation,payload,**kwargs):return self.call(project,operation,payload)
            def call(self,key,name,payload):
                inventory=self.server.inventory();actual,_=console.resolve_workspace(inventory,key)
                grants={'read':True,**self.server.effective_permissions(inventory['projects'][key])}
                with lock:
                    calls.append((key,name,payload))
                    response=client.call('fixture',root=str(actual),project=key,permissions=grants,name=name,payload=payload)
                if 'code' in response:raise ForgeError(response['error'],code=response['code'])
                return response['result']
        monkeypatch.setattr(web_service,'WebService',TestHelper)
        with service(None,registry=registry,analysis_config='explicit-test-only-Case-metadata-adapter') as app:
            def grant(**updates):
                value=console.load_registry(registry);permissions={**console.capabilities(value['projects'][project]),**updates}
                return console.set_capabilities(registry,project,expected=content_digest(value),capabilities=permissions,analysis_service=app.analysis)
            grant(analysis_write=True,case_annotate=True)
            before=source_files(root)
            context=browser.new_context(viewport=dict(width=1440,height=1100));page=context.new_page();page.set_default_timeout(12000);errors=[]
            page.on('pageerror',lambda error:errors.append(str(error)))
            page.route(app.origin+'/',lambda route:route.fulfill(content_type='text/html',body=HTML))
            # Only the new static asset awaits integration routing. API calls
            # below go through the actual authenticated WorkspaceServer.
            page.route('**/analysis-case.css',lambda route:route.fulfill(content_type='text/css',body=(ASSETS/'analysis-case.css').read_text()))
            def boot(key=project,source='cases'):
                page.goto(app.origin+'/')
                for name in ('markdown-it.min.js','reading.js','analysis-state.js','analysis-render.js','analysis-case.js'):page.add_script_tag(path=str(ASSETS/name))
                page.evaluate('''({project,token,source})=>{
                  window.currentProject=project;window.fixtureSource=source;
                  window.caseAPI=async(name,query={},body,key=project)=>{
                    const response=await fetch('/api/workspaces/'+key+'/'+name+(Object.keys(query).length?'?'+new URLSearchParams(query):''),{method:body?'POST':'GET',headers:{'X-Forge-Token':token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});
                    const value=await response.json();if(!response.ok){const error=Error(value.error);error.code=value.code;throw error;}
                    if(window.dropNext===name){window.dropNext=null;throw Error('Injected lost browser delivery after the real committed response');}
                    if(window.holdNext===name){window.holdNext=null;return await new Promise(resolve=>{(window.heldResponses??=[]).push(()=>resolve(value));});}return value;
                  };
                  window.caseReaders=new Set();window.caseContexts=[];window.caseMutateContext=false;
                  window.mountCase=(key=project)=>{window.caseView=ForgeCase.mount({parent:document.querySelector('#mount'),api:caseAPI,project:key,connected:true,
                    permissions:{analysis_write:true,case_annotate:true},onContext:value=>{caseContexts.push(ForgeAnalysisState.clone(value));if(caseMutateContext&&value.source){value.source.digest='host-mutation';value.source.schema=[];}},mountText:(host,text)=>{const reader=ForgeReading.createTextReader({text,defaultMode:'preview',identity:{object:'owned-case-field',generation:1}});caseReaders.add(reader);host.append(reader.element);return reader;},disposeReaders:root=>{for(const reader of caseReaders)if(root.contains(reader.element)){reader.dispose();caseReaders.delete(reader);}}});};
                  mountCase();caseView.activate(true);caseView.setSource(source);
                }''',dict(project=key,token=app.token,source=locators[source]))
            try:
                boot();expect(page.get_by_role('heading',name='Case 0',exact=True)).to_be_visible()
                expect(page.get_by_label('标注 结果是否符合预期',exact=True)).to_be_visible()
                yield dict(page=page,app=app,project=project,root=root,calls=calls,errors=errors,locators=locators,boot=boot,grant=grant,
                    register_output=lambda descriptor,register=True:app.analysis.call(project,'fixture-output-register',dict(descriptor=descriptor,register=register)),evidence=tmp_path)
            except BaseException:
                page.screenshot(path=str(tmp_path/'case-ui-failure.png'),full_page=True);raise
            finally:
                page.unroute_all(behavior='ignoreErrors');context.close();assert source_files(root)==before,'Case UI modified its source project'
            assert not errors,errors


def query_call(fixture):
    return [payload for _,name,payload in fixture['calls'] if name=='case-page'][-1]


def test_real_full_query_nested_types_view_reload_and_narrow_layout(owned_case,tmp_path):
    f=owned_case;p=f['page']
    # Current Case layout keeps filters in the visible rail. Grouping lives in
    # the settings panel and stable ordering has its own collapsed section.
    p.get_by_role('button',name='添加筛选条件',exact=True).click()
    p.get_by_label('筛选字段',exact=True).select_option('score');p.get_by_label('筛选关系',exact=True).select_option('range')
    p.get_by_label('筛选下限',exact=True).fill('0');p.get_by_label('筛选上限',exact=True).fill('0.000001')
    p.get_by_role('button',name='应用筛选与分组',exact=True).click();expect(p.locator('.analysis-case-coverage')).to_contain_text('范围内匹配 1 条')
    assert query_call(f)['query']['filters']['value']['upper']==1e-6
    p.get_by_label('筛选下限',exact=True).fill('5');p.get_by_label('筛选上限',exact=True).fill('23')
    p.get_by_role('button',name='添加筛选条件',exact=True).click()
    p.get_by_label('筛选字段 2',exact=True).select_option('flag');p.get_by_label('筛选值 2',exact=True).select_option('true')
    p.get_by_role('button',name='配置分组与筛选',exact=True).click()
    p.get_by_role('button',name='添加分组字段',exact=True).click();p.get_by_label('分组字段',exact=True).select_option('bucket')
    p.get_by_role('button',name='字段与评测配置',exact=True).click()
    p.get_by_text('排序与取样范围',exact=True).click()
    p.get_by_role('button',name='添加排序字段',exact=True).click()
    p.get_by_text('排序与取样范围',exact=True).click()
    p.get_by_label('排序字段',exact=True).select_option('score');p.get_by_label('排序方向',exact=True).select_option('desc')
    p.get_by_role('button',name='应用筛选与分组',exact=True).click()
    # Filtering keeps the current Case/draft (also locked by the representative
    # demo test). Explicitly navigate to the first result of this sorted query.
    expect(p.locator('.analysis-case-coverage')).to_contain_text('范围内匹配 9 条')
    expect(p.locator('.analysis-case-list button').first).to_have_attribute('aria-label','Case 22')
    expect(p.locator('.analysis-case-content')).to_contain_text('当前 Case 不在本次选样页')
    p.get_by_role('button',name='Case 22',exact=True).click()
    expect(p.get_by_role('heading',name='Case 22',exact=True)).to_be_visible();expect(p.locator('.analysis-case-coverage')).to_contain_text('范围内匹配 9 条')
    payload=query_call(f);assert payload['query']['scope']=='full' and payload['query']['filters']['op']=='and'
    p.get_by_role('button',name='字段与评测配置',exact=True).click()
    p.get_by_role('button',name='内容字段与格式',exact=True).click()
    p.get_by_label('字段标题 2',exact=True).fill('模型回答');p.get_by_label('预览格式 2',exact=True).select_option('md');p.get_by_label('Case 布局',exact=True).select_option('comparison')
    p.get_by_role('button',name='保存视图',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('视图已保存为 v1')
    f['boot']();expect(p.get_by_role('heading',name='Case 22',exact=True)).to_be_visible();expect(p.locator('.analysis-case-status')).to_contain_text('已存视图 v1')
    expect(p.get_by_role('heading',name='输出 · 模型回答',exact=True)).to_be_visible()
    assert query_call(f)['view']['revision']==1
    for width in (1440,390,320):
        p.set_viewport_size(dict(width=width,height=1100));assert p.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        p.screenshot(path=str(tmp_path/f'case-ui-{width}.png'),full_page=True)


def test_save_next_keeps_original_cursor_sequence_and_conflicts_stay_put(owned_case):
    f=owned_case;p=f['page'];initial=query_call(f);source=initial['digest'];definition=initial['view']['descriptor']['questions'][0]
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
    f['app'].analysis.call(f['project'],'annotations-save',dict(request_id='other-browser-answer',target=dict(source=source,case_key='0',variant=None,field=None),entries=[dict(definition=definition,expected_revision=0,action='set',value='no')],origin=dict(kind='unsaved_view',reference='explicit-other-browser')))
    p.get_by_role('button',name='保存并下一条',exact=True).click();expect(p.locator('.analysis-case-review')).to_contain_text('保存冲突')
    expect(p.get_by_role('heading',name='Case 0',exact=True)).to_be_visible();expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')
    expect(p.get_by_role('button',name='核对 Case 原请求',exact=True)).to_have_count(0)
    p.get_by_role('button',name='保留草稿并采用服务器版本',exact=True).click();p.get_by_role('button',name='保存并下一条',exact=True).click()
    expect(p.get_by_role('heading',name='Case 1',exact=True)).to_be_visible()
    p.get_by_role('button',name='Case 19',exact=True).click();expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('')
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes');p.get_by_role('button',name='保存并下一条',exact=True).click()
    expect(p.get_by_role('heading',name='Case 20',exact=True)).to_be_visible()
    cursor=query_call(f)['cursor'];assert cursor['offset']==20 and cursor['annotation_watermark']==0
    assert len([payload for _,name,payload in f['calls'] if name=='case-page'])==2
    f['boot']();expect(p.get_by_role('heading',name='Case 20',exact=True)).to_be_visible()
    assert query_call(f)['cursor']['offset']==20 and query_call(f)['cursor']['annotation_watermark']==0
    history=f['app'].analysis.call(f['project'],'annotations',dict(target=dict(source=source,case_key='0',variant=None,field=None),definition=c.digest(definition)))
    assert [item['value'] for item in history['items']]==['yes','no']


def test_view_cas_and_authority_revocation_preserve_drafts(owned_case):
    f=owned_case;p=f['page'];p.get_by_role('button',name='保存视图',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('视图已保存为 v1')
    saved=f['app'].analysis.call(f['project'],'documents',dict(kind='case_view',limit=8))['items'][0]
    remote=f['app'].analysis.call(f['project'],'view',dict(view_id=saved['id']))['document'];remote['fields'][1]['label']='远端回答'
    f['app'].analysis.call(f['project'],'view-save',dict(request_id='other-browser-view',view_id=saved['id'],expected_revision=1,descriptor=remote))
    p.get_by_role('button',name='字段与评测配置',exact=True).click();p.get_by_label('字段标题 2',exact=True).fill('本地回答')
    p.get_by_role('button',name='保存视图',exact=True).click();expect(p.locator('.case-ui-view-conflict')).to_contain_text('当前配置草稿已保留')
    p.get_by_role('button',name='读取服务器视图版本',exact=True).click();expect(p.locator('.case-ui-view-conflict')).to_contain_text('服务器当前 v2')
    p.get_by_role('button',name='保留配置并采用服务器版本',exact=True).click();p.get_by_role('button',name='保存视图',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('视图已保存为 v3')
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes');f['grant'](case_annotate=False)
    p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('Required analysis capability is disabled: case_annotate')
    expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')
    p.evaluate('caseView.updatePermissions({analysis_write:true,case_annotate:false})');expect(p.get_by_role('button',name='保存本 Case 标注',exact=True)).to_be_disabled()
    expect(p.get_by_role('button',name='核对 Case 原请求',exact=True)).to_have_count(0)


def test_full_budget_refusal_requires_explicit_sample_and_preserves_other_source(owned_case):
    f=owned_case;p=f['page'];p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
    p.evaluate('source=>caseView.setSource(source)',f['locators']['over_budget']);expect(p.locator('.analysis-case')).to_contain_text('Full Case query exceeds')
    expect(p.get_by_role('heading',name='Case 0',exact=True)).to_have_count(0)
    p.get_by_label('明确选择样本模式',exact=True).check();p.get_by_role('button',name='应用筛选与分组',exact=True).click()
    expect(p.locator('.analysis-case-coverage')).to_contain_text('显式样本 · 1000 / 1001')
    assert query_call(f)['query']['scope']=='sample'
    p.evaluate('source=>caseView.setSource(source)',f['locators']['cases']);expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')
    assert query_call(f)['query']['scope']=='full'


def test_late_source_and_project_responses_do_not_replace_current_case(owned_case):
    f=owned_case;p=f['page'];p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
    # Delay only the real API response delivery, after its native query completes.
    p.evaluate("holdNext='analysis-case-page'");p.get_by_role('button',name='读取 Case',exact=True).click();p.wait_for_function('window.heldResponses?.length===1')
    p.evaluate('source=>caseView.setSource(source)',f['locators']['other']);expect(p.locator('.analysis-case>.analysis-bar>strong')).to_contain_text('other')
    expect(p.get_by_role('heading',name='Case 0',exact=True)).to_be_visible();expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('')
    p.evaluate('heldResponses.shift()()');expect(p.locator('.analysis-case>.analysis-bar>strong')).to_contain_text('other')
    p.evaluate('source=>caseView.setSource(source)',f['locators']['cases']);expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')
    p.evaluate("holdNext='analysis-annotation-heads'");p.get_by_role('button',name='Case 1',exact=True).click();p.wait_for_function('window.heldResponses?.length===1')
    p.evaluate('''source=>{caseView.dispose();document.querySelector('#mount').replaceChildren();mountCase('w'+'f'.repeat(32));caseView.activate(true);caseView.setSource(source);heldResponses.shift()();}''',f['locators']['cases'])
    expect(p.locator('.analysis-case')).to_contain_text('Workspace is not registered');expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_count(0)
    f['boot']();p.get_by_role('button',name='Case 0',exact=True).click();expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')


def test_trusted_context_tracks_real_source_view_and_ignores_late_disposed_reads(owned_case,tmp_path):
    f=owned_case;p=f['page'];initial=query_call(f);digest=initial['digest'];descriptor=initial['view']['descriptor']
    first=p.evaluate('caseContexts')
    assert [item['phase'] for item in first]==['empty','loading','ready']
    assert first[-1]['source']['digest']==digest and first[-1]['source']['rows']==24 and len(first[-1]['source']['schema'])==5
    assert first[-1]['label']=='cases · fixed v1' and first[-1]['view'] is None
    p.get_by_role('button',name='保存视图',exact=True).click();expect(p.locator('.analysis-case-status')).to_contain_text('已存视图 v1')
    p.wait_for_function('caseContexts.at(-1).view?.revision===1')
    saved=p.evaluate('caseContexts.at(-1).view');assert saved['id']
    before=p.evaluate('caseContexts.length')
    p.evaluate("caseMutateContext=true;holdNext='analysis-source-register'")
    p.evaluate('source=>caseView.setSource(source)',f['locators']['other']);p.wait_for_function('window.heldResponses?.length===1')
    assert p.evaluate('caseContexts.at(-1).phase')=='loading' and p.evaluate('caseContexts.at(-1).source') is None
    p.evaluate('source=>caseView.setSource(source)',f['locators']['cases']);p.evaluate('heldResponses.shift()()')
    p.wait_for_function('digest=>caseContexts.at(-1).phase==="ready"&&caseContexts.at(-1).source.digest===digest',arg=digest)
    later=p.evaluate('caseContexts.slice('+str(before)+')')
    assert all(item['source']['digest']==digest for item in later if item['phase']=='ready')
    expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_be_visible()
    assert query_call(f)['digest']==digest
    other=f['app'].analysis.call(f['project'],'source-register',dict(locator=f['locators']['other']['locator']))
    incoming=json.loads(json.dumps(descriptor));incoming['source_snapshot']=other['digest'];incoming['case_key']=other['case_key']
    adopt_origin(p,registered_origin(f,incoming))
    current=p.evaluate('caseContexts.at(-1)')
    assert current['phase']=='ready' and current['source']['digest']==other['digest'] and current['source']['rows']==3
    assert current['view'] is None and current['label']=='Notebook 显示标签不是 origin'
    source_path=f['root']/'data/other.lance';parked=tmp_path/'unavailable-context-source.lance';source_path.rename(parked)
    try:
        p.get_by_role('button',name='读取 Case',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('固定来源不可用')
        current=p.evaluate('caseContexts.at(-1)');assert current['phase']=='unavailable' and current['source']['digest']==other['digest']
    finally:parked.rename(source_path)
    p.evaluate('caseView.setSource(null)');assert p.evaluate('caseContexts.at(-1)')==dict(phase='empty',source=None,label=None,view=None)
    p.evaluate('source=>caseView.setSource(source)',f['locators']['cases']);expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_be_visible()
    p.evaluate("holdNext='analysis-annotation-heads'");p.get_by_role('button',name='Case 1',exact=True).click();p.wait_for_function('window.heldResponses?.length===1')
    before=p.evaluate('caseContexts.length');p.evaluate('caseView.dispose();heldResponses.shift()()')
    assert p.evaluate('caseContexts.length')==before
    assert p.evaluate('caseView.setSource(fixtureSource)') is False
    assert p.evaluate('caseContexts.length')==before
    assert not any(name in {'session-start','cell-submit','execute'} for _,name,_ in f['calls'])


def test_notebook_presentation_uses_fixed_descriptor_and_retains_view_edits(owned_case):
    f=owned_case;p=f['page'];original=query_call(f)['view']['descriptor'];incoming=json.loads(json.dumps(original));incoming['fields'][1]['label']='Notebook 输出回答'
    p.get_by_role('button',name='字段与评测配置',exact=True).click();p.get_by_label('字段标题 2',exact=True).fill('本地未保存回答')
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
    p.evaluate('descriptor=>caseView.setPresentation({descriptor,label:"固定 Notebook 输出"})',incoming)
    expect(p.get_by_role('heading',name='输出 · 本地未保存回答',exact=True)).to_be_visible()
    registrations=len([name for _,name,_ in f['calls'] if name=='source-register'])
    p.get_by_role('button',name='采用 Notebook 输出视图',exact=True).click();expect(p.get_by_role('heading',name='输出 · Notebook 输出回答',exact=True)).to_be_visible()
    expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')
    assert len([name for _,name,_ in f['calls'] if name=='source-register'])==registrations
    p.get_by_role('button',name='恢复先前视图草稿',exact=True).click();expect(p.get_by_role('heading',name='输出 · 本地未保存回答',exact=True)).to_be_visible()
    assert not any(any(word in name for word in ('execute','session','cell','pin')) for _,name,_ in f['calls'])


def test_real_lost_response_reconciles_original_receipt_without_second_write(owned_case):
    f=owned_case;p=f['page'];initial=query_call(f)
    # The real authenticated save commits first. The explicit JS test hook
    # discards its parsed delivery, with no second API dispatch.
    p.evaluate("dropNext='analysis-annotations-save'")
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes');p.get_by_role('button',name='保存并下一条',exact=True).click()
    expect(p.get_by_role('button',name='核对 Case 原请求',exact=True)).to_be_enabled();expect(p.get_by_role('heading',name='Case 0',exact=True)).to_be_visible()
    sent=[payload for _,name,payload in f['calls'] if name=='annotations-save'];assert len(sent)==1
    f['boot']();expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')
    p.get_by_role('button',name='核对 Case 原请求',exact=True).click();expect(p.get_by_role('button',name='核对 Case 原请求',exact=True)).to_have_count(0)
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1');expect(p.get_by_role('heading',name='Case 0',exact=True)).to_be_visible()
    assert len([payload for _,name,payload in f['calls'] if name=='annotations-save'])==1
    history=f['app'].analysis.call(f['project'],'annotations',dict(target=sent[0]['target'],definition=c.digest(initial['view']['descriptor']['questions'][0])))
    assert len(history['items'])==1 and history['items'][0]['value']=='yes'


def test_expired_fixed_source_keeps_server_history_readable(owned_case,tmp_path):
    f=owned_case;p=f['page'];p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
    p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
    p.get_by_role('button',name='保存视图',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('视图已保存为 v1')
    path=f['root']/'data/cases.lance';parked=tmp_path/'temporarily-unavailable-cases.lance';path.rename(parked)
    try:
        p.get_by_role('button',name='读取 Case',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('固定来源不可用')
        expect(p.get_by_role('button',name='保存本 Case 标注',exact=True)).to_be_disabled()
        p.get_by_text('标注历史与恢复',exact=True).click();p.get_by_role('button',name='读取历史',exact=True).click()
        expect(p.locator('.analysis-case-review')).to_contain_text('v1 · answered');expect(p.locator('.analysis-case-review')).to_contain_text('"yes"')
        expect(p.locator('.analysis-case-content')).to_contain_text('未替换为最新表')
    finally:parked.rename(path)


def test_renderer_handles_release_on_format_case_mode_and_dispose(owned_case):
    f=owned_case;p=f['page']
    # Explicit instrumentation wraps the real baseline renderer. It asserts
    # consumer ownership only, not media decode or B1 runtime capability.
    p.evaluate('''()=>{
      window.rendererStats={created:0,disposed:0,active:0};const original=ForgeAnalysisRender.field;
      window.ForgeAnalysisRender={...ForgeAnalysisRender,field:options=>{
        const result=original(options);let alive=true;rendererStats.created++;rendererStats.active++;
        return {ready:Promise.resolve(result),dispose(){if(alive){alive=false;rendererStats.disposed++;rendererStats.active--;}}};
      }};
    }''')
    p.get_by_role('button',name='Case 1',exact=True).click();expect(p.get_by_role('heading',name='Case 1',exact=True)).to_be_visible()
    p.wait_for_function('rendererStats.active===4')
    p.get_by_role('button',name='字段与评测配置',exact=True).click();p.get_by_label('预览格式 2',exact=True).select_option('json')
    p.wait_for_function('rendererStats.disposed>=4&&rendererStats.active===4')
    p.evaluate('caseView.activate(false)');assert p.evaluate('rendererStats.active')==0
    p.evaluate('caseView.activate(true)');p.wait_for_function('rendererStats.active===4')
    p.evaluate('caseView.dispose()');assert p.evaluate('rendererStats.active')==0


def test_question_types_and_invalid_option_drafts_survive_remount(browser,tmp_path):
    # This is the original explicit synthetic-source/API adapter plus real
    # SQLite, used for client type/draft behavior; it is not a native source test.
    from test_forge_analysis_case_browser import case_page
    with case_page(browser,tmp_path) as (p,client,source,calls,errors,_):
        p.get_by_role('button',name='字段与评测配置',exact=True).click()
        p.get_by_label('评测类型 1',exact=True).select_option('multi')
        p.get_by_role('button',name='添加评测项',exact=True).click();p.get_by_label('评测题目 2',exact=True).fill('评分');p.get_by_label('评测类型 2',exact=True).select_option('rating')
        p.get_by_role('button',name='添加评测项',exact=True).click();p.get_by_label('评测题目 3',exact=True).fill('备注')
        p.get_by_role('button',name='应用评测定义',exact=True).click()
        p.get_by_label('标注 结果是否符合预期 是',exact=True).check();p.get_by_label('标注 评分',exact=True).fill('9');p.get_by_label('标注 备注',exact=True).fill('保留精确说明')
        p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('评分超出定义范围')
        assert not [payload for name,payload in calls if name=='annotations-save']
        p.get_by_label('标注 评分',exact=True).fill('4');p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
        saved=[payload for name,payload in calls if name=='annotations-save'][-1]
        assert [entry['value'] for entry in saved['entries']]==[['yes'],4,'保留精确说明']
        assert client.call('list_documents',kind='case_view')['result']['items']==[]
        invalid='\n'.join(f'选项 {i}' for i in range(33));p.get_by_label('评测选项 1',exact=True).fill(invalid)
        expect(p.locator('.analysis-case')).to_contain_text('原输入已保留')
        p.evaluate('''()=>{if(!caseView.dispose())throw Error('bounded draft lost');document.querySelector('#mount').replaceChildren();mountCase();caseView.activate(true);caseView.setSource({label:'same fixed source',locator:{kind:'workspace_table',alias:'fixture',binding:'a'.repeat(64),columns:['input','output','dialogue','tags']}});}''')
        p.get_by_role('button',name='字段与评测配置',exact=True).click();expect(p.get_by_label('评测选项 1',exact=True)).to_have_value(invalid)
        p.get_by_label('评测选项 1',exact=True).fill('是\n否');p.get_by_role('button',name='应用评测定义',exact=True).click();p.get_by_role('button',name='读取 Case',exact=True).click()
        expect(p.get_by_label('标注 结果是否符合预期 是',exact=True)).to_be_checked();expect(p.get_by_label('标注 评分',exact=True)).to_have_value('4');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('保留精确说明')
        assert not errors,errors


def test_full_project_shell_compact_tools_and_markdown_preview_source(owned_case,tmp_path):
    f=owned_case;p=f['page']
    # The actual project app/index, with only the proposed CSS index link and
    # static route staged in this private fixture. This remains the baseline
    # shared renderer; final rich_output integration is a separate edition.
    p.unroute(f['app'].origin+'/')
    html=(ASSETS/'index.html').read_text().replace('<link rel="stylesheet" href="/analysis.css">','<link rel="stylesheet" href="/analysis.css"><link rel="stylesheet" href="/analysis-case.css">')
    p.route(f['app'].origin+'/',lambda route:route.fulfill(content_type='text/html',body=html))
    p.evaluate("token=>sessionStorage.setItem('forge-token',token)",f['app'].token);p.reload(wait_until='domcontentloaded')
    data=p.locator('.function-nav').get_by_role('button',name='数据',exact=True);expect(data).to_be_enabled();data.click()
    case=p.locator('.analysis-case');expect(case.get_by_role('heading',name='Case 0',exact=True)).to_be_visible()
    expect(case.get_by_role('button',name='导出 Case 草稿',exact=True)).to_be_hidden()
    case.get_by_role('button',name='字段与评测配置',exact=True).click();case.get_by_label('预览格式 2',exact=True).select_option('md')
    case.get_by_role('button',name='字段与评测配置',exact=True).click()
    output=case.locator('.analysis-case-field[data-role=output]');expect(output.locator('.reading-content strong')).to_have_text('回答 0')
    output.get_by_role('button',name='原文',exact=True).click();expect(output.locator('.reading-content')).to_contain_text('**回答 0**');p.screenshot(path=str(tmp_path/'project-shell-case-markdown-source.png'),full_page=True)
    output.get_by_role('button',name='预览',exact=True).click();expect(output.locator('.reading-content strong')).to_have_text('回答 0')
    expect(case.locator('.case-ui-query-controls')).not_to_have_attribute('open','')
    for width in (1440,390,320):
        p.set_viewport_size(dict(width=width,height=1000));assert p.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        p.screenshot(path=str(tmp_path/f'project-shell-case-{width}.png'),full_page=True)
    assert not any(any(word in name for word in ('session-start','cell-submit','execute')) for _,name,_ in f['calls'])


def test_advertised_old_definition_discovery_after_clean_browser_draft(browser,tmp_path):
    # Consumer-only test for the additive I1.1 interface: discovery delivery is
    # an explicit fixture containing a definition/head fetched from real SQLite.
    # The integrator owns the actual new enumeration backend and its HTTP tests.
    from test_forge_analysis_case_browser import case_page,PROJECT
    with case_page(browser,tmp_path) as (p,client,source,calls,errors,_):
        p.get_by_role('button',name='保存视图',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('视图已保存为 v1')
        saved=client.call('list_documents',kind='case_view')['result']['items'][0]
        p.get_by_role('button',name='字段与评测配置',exact=True).click();p.get_by_label('评测题目 1',exact=True).fill('曾用的另一题')
        p.get_by_role('button',name='应用评测定义',exact=True).click();p.get_by_label('标注 曾用的另一题',exact=True).select_option('no')
        p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
        sent=[payload for name,payload in calls if name=='annotations-save'][-1];definition=sent['entries'][0]['definition'];digest=c.digest(definition)
        history=client.call('annotations',target=sent['target'],definition=digest)['result']
        witness=dict(target=sent['target'],items=[dict(definition_digest=digest,definition=history['definition'],head=history['items'][0])],annotation_watermark=history['annotation_watermark'],next_before=None)
        p.evaluate('''({project,source,witness})=>{
          caseView.dispose();new ForgeAnalysisState.Store().removeCaseDraft(project,source);
          document.querySelector('#mount').replaceChildren();window.discoveryRequests=[];
          caseView=ForgeCase.mount({parent:document.querySelector('#mount'),project,connected:true,permissions:{analysis_write:true,case_annotate:true},features:{annotation_definitions:true},
            api:async(name,query,body,...rest)=>{if(name==='analysis-annotation-definitions'){discoveryRequests.push(body);return witness;}return caseAPI(name,query,body,...rest);},
            mountText:(host,value)=>{const pre=document.createElement('pre');pre.textContent=value;host.append(pre);},disposeReaders:()=>{}});
          caseView.activate(true);caseView.setSource({label:'fixed synthetic source',locator:{kind:'workspace_table',alias:'fixture',binding:'a'.repeat(64),columns:['input','output','dialogue','tags']}});
        }''',dict(project=PROJECT,source=source['digest'],witness=witness))
        expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_be_visible()
        p.get_by_label('选择 Case 视图',exact=True).select_option(saved['id']);expect(p.locator('.analysis-case-status')).to_contain_text('已存视图 v1')
        p.get_by_text('旧定义与答案',exact=True).click();p.get_by_role('button',name='读取已保存旧定义',exact=True).click()
        old=p.locator('.case-ui-old-answer');expect(old).to_contain_text('曾用的另一题');expect(old).to_contain_text('已存答案 v1 · "no"')
        assert p.evaluate('discoveryRequests[0]')==dict(target=sent['target'],limit=8)
        old.get_by_role('button',name='采用此题目定义',exact=True).click();expect(p.get_by_label('标注 曾用的另一题',exact=True)).to_have_value('no')
        assert client.call('view',view_id=saved['id'])['result']['document']['questions'][0]['title']=='结果是否符合预期'
        assert not errors,errors


def registered_origin(fixture,descriptor,*,register=True):
    """Real registry/read/record code, with seeded sealed execution metadata.

    An explicitly selected read-only integration backend is needed until this
    additive backend has been merged into the test tree. No kernel/B1 is run.
    """
    import os
    backend=Path(os.environ.get('FORGE_CASE_ORIGIN_BACKEND_ROOT',ASSETS.parents[2]))
    if not (backend/'demiflow/workbench/analysis/presentations.py').is_file():
        pytest.skip('The real presentation registry backend is required; no registration mock is substituted')
    value=fixture['register_output'](descriptor,register)
    evidence=fixture['evidence'];number=len(list(evidence.glob('origin-registration-*.json')))
    (evidence/f'origin-registration-{number}.json').write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    assert value['fixture']['registered'] is register
    return {key:value[key] for key in ('descriptor','origin','annotation_origin_registered','provenance','output_index')}


def adopt_origin(page,presentation):
    page.evaluate('value=>caseView.setPresentation({...value,label:"Notebook 显示标签不是 origin"})',presentation)
    page.get_by_role('button',name='采用 Notebook 输出视图',exact=True).click()
    expect(page.get_by_role('button',name='采用 Notebook 输出视图',exact=True)).to_have_count(0)
    question=presentation['descriptor']['questions'][0]['title']
    expect(page.get_by_label('标注 '+question,exact=True)).to_be_visible()


def latest_annotation_save(fixture):
    return [payload for _,name,payload in fixture['calls'] if name=='annotations-save'][-1]


def test_registered_notebook_origin_adopt_save_remount_history_and_saved_view(owned_case):
    f=owned_case;p=f['page'];descriptor=query_call(f)['view']['descriptor'];presentation=registered_origin(f,descriptor)
    adopt_origin(p,presentation);expect(p.locator('.case-ui-definition-origin')).to_contain_text('Notebook 固定输出登记引用')
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1');assert latest_annotation_save(f)['origin']==presentation['origin']
    f['boot']();expect(p.locator('.case-ui-definition-origin')).to_contain_text('Notebook 固定输出登记引用')
    retained=p.evaluate('({project,digest})=>new ForgeAnalysisState.Store().caseDraft(project,digest).presentation_origin',dict(project=f['project'],digest=descriptor['source_snapshot']))
    assert retained['origin']==presentation['origin'] and retained['descriptor']==descriptor
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('no');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v2');assert latest_annotation_save(f)['origin']==presentation['origin']
    p.get_by_text('标注历史与恢复',exact=True).click();p.get_by_role('button',name='读取历史',exact=True).click();expect(p.locator('.analysis-case-review')).to_contain_text(presentation['origin']['reference'])
    p.get_by_role('button',name='保存视图',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('视图已保存为 v1')
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v3');saved=latest_annotation_save(f)
    assert saved['origin']['kind']=='saved_view' and saved['origin']['reference'].endswith('@1')
    history=f['app'].analysis.call(f['project'],'annotations',dict(target=saved['target'],definition=c.digest(descriptor['questions'][0])))
    assert [entry['origin'] for entry in history['items']]==[saved['origin'],presentation['origin'],presentation['origin']]


def test_registered_origin_changed_question_source_and_retained_draft_do_not_leak(owned_case):
    f=owned_case;p=f['page'];descriptor=query_call(f)['view']['descriptor'];presentation=registered_origin(f,descriptor)
    p.get_by_role('button',name='字段与评测配置',exact=True).click();p.get_by_label('字段标题 2',exact=True).fill('采用前保留的回答配置')
    adopt_origin(p,presentation)
    p.get_by_label('评测题目 1',exact=True).fill('已改变的问题');p.get_by_role('button',name='应用评测定义',exact=True).click()
    p.get_by_label('标注 已改变的问题',exact=True).select_option('yes');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1');assert latest_annotation_save(f)['origin']['kind']=='unsaved_view'
    p.evaluate('value=>caseView.setSource(value)',f['locators']['other']);expect(p.locator('.analysis-case>.analysis-bar>strong')).to_contain_text('other')
    expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_be_visible();p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1');assert latest_annotation_save(f)['origin']['kind']=='unsaved_view'
    p.evaluate('value=>caseView.setSource(value)',f['locators']['cases']);expect(p.get_by_label('标注 已改变的问题',exact=True)).to_be_visible()
    p.get_by_role('button',name='恢复先前视图草稿',exact=True).click();expect(p.get_by_role('heading',name='输出 · 采用前保留的回答配置',exact=True)).to_be_visible()
    expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_be_visible();p.get_by_label('标注 结果是否符合预期',exact=True).select_option('no');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1');assert latest_annotation_save(f)['origin']['kind']=='unsaved_view'
    assert p.evaluate('({project,digest})=>new ForgeAnalysisState.Store().caseDraft(project,digest).presentation_origin',dict(project=f['project'],digest=descriptor['source_snapshot'])) is None


def test_unregistered_output_is_unsaved_and_forged_registered_origin_is_rejected(owned_case):
    f=owned_case;p=f['page'];descriptor=query_call(f)['view']['descriptor'];presentation=registered_origin(f,descriptor,register=False)
    adopt_origin(p,presentation);expect(p.locator('.case-ui-definition-origin')).to_contain_text('未保存视图定义')
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1');assert latest_annotation_save(f)['origin']['kind']=='unsaved_view'
    forged={**presentation,'annotation_origin_registered':True}
    adopt_origin(p,forged);p.get_by_label('标注 结果是否符合预期',exact=True).select_option('no');p.get_by_role('button',name='保存并下一条',exact=True).click()
    expect(p.locator('.analysis-case')).to_contain_text('Notebook output definition origin is not registered')
    expect(p.get_by_role('heading',name='Case 0',exact=True)).to_be_visible();expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('no')
    assert latest_annotation_save(f)['origin']==forged['origin']
    history=f['app'].analysis.call(f['project'],'annotations',dict(target=latest_annotation_save(f)['target'],definition=c.digest(descriptor['questions'][0])))
    assert len(history['items'])==1 and history['items'][0]['origin']['kind']=='unsaved_view'
    invalid={**presentation,'annotation_origin_registered':True,'origin':dict(kind='notebook_output',reference='Notebook-label')}
    p.evaluate('value=>caseView.setPresentation(value)',invalid);p.get_by_role('button',name='采用 Notebook 输出视图',exact=True).click();expect(p.locator('.analysis-case')).to_contain_text('不能使用标签或自行构造的来源')


def test_late_notebook_presentation_origin_cannot_replace_new_adoption_or_source(owned_case):
    f=owned_case;p=f['page'];descriptor=query_call(f)['view']['descriptor'];first=registered_origin(f,descriptor)
    changed=json.loads(json.dumps(descriptor));changed['questions'][0]['title']='第二个输出的问题';second=registered_origin(f,changed)
    p.evaluate('value=>{holdNext="analysis-source";caseView.setPresentation(value);}',first);p.get_by_role('button',name='采用 Notebook 输出视图',exact=True).click();p.wait_for_function('window.heldResponses?.length===1')
    p.evaluate('value=>{caseView.setPresentation(value);heldResponses.shift()();}',second)
    p.get_by_role('button',name='采用 Notebook 输出视图',exact=True).click();expect(p.get_by_label('标注 第二个输出的问题',exact=True)).to_be_visible()
    p.get_by_label('标注 第二个输出的问题',exact=True).select_option('yes');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1');assert latest_annotation_save(f)['origin']==second['origin']
    p.evaluate('value=>{holdNext="analysis-source";caseView.setPresentation(value);}',first);p.get_by_role('button',name='采用 Notebook 输出视图',exact=True).click();p.wait_for_function('window.heldResponses?.length===1')
    p.evaluate('value=>caseView.setSource(value)',f['locators']['other']);expect(p.locator('.analysis-case>.analysis-bar>strong')).to_contain_text('other')
    expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_be_visible();p.evaluate('heldResponses.shift()()')
    p.get_by_label('标注 结果是否符合预期',exact=True).select_option('no');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1');assert latest_annotation_save(f)['origin']['kind']=='unsaved_view'
    p.evaluate('value=>caseView.setSource(value)',f['locators']['cases']);expect(p.get_by_label('标注 第二个输出的问题',exact=True)).to_be_visible()
    adopt_origin(p,first);p.get_by_role('button',name='恢复先前视图草稿',exact=True).click()
    expect(p.get_by_label('标注 第二个输出的问题',exact=True)).to_be_visible()
    p.get_by_label('标注 第二个输出的问题',exact=True).select_option('no');p.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v2');assert latest_annotation_save(f)['origin']==second['origin']


def test_legacy_display_only_origin_is_not_upgraded_and_answer_draft_survives(browser,tmp_path):
    # Compatibility for the previously delivered display-only cache shape.
    # Synthetic fixed-source adapter with real SQLite; no registration mock.
    from test_forge_analysis_case_browser import case_page,PROJECT
    with case_page(browser,tmp_path) as (p,client,source,calls,errors,_):
        p.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
        p.evaluate('''({project,digest})=>{
          caseView.dispose();const store=new ForgeAnalysisState.Store(),draft=store.caseDraft(project,digest);
          draft.presentation_origin={kind:'notebook_output',reference:'旧 Notebook 显示标签'};store.saveCaseDraft(project,digest,draft);
          document.querySelector('#mount').replaceChildren();mountCase();caseView.activate(true);caseView.setSource({label:'same synthetic source',locator:{kind:'workspace_table',alias:'fixture',binding:'a'.repeat(64),columns:['input','output','dialogue','tags']}});
        }''',dict(project=PROJECT,digest=source['digest']))
        expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes');expect(p.locator('.case-ui-definition-origin')).to_contain_text('未保存视图定义')
        p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(p.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
        assert [body for name,body in calls if name=='annotations-save'][-1]['origin']['kind']=='unsaved_view'
        assert not errors,errors
