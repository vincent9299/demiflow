"""Representative Case demo flow over real HTTP/SQLite/Lance, no model/runtime.

Case assets may be overlaid on a frozen shell or served by the integrated shell. The
private authority/helper transport is explicit; table reads and Records are real.
Three independent output *fields* are verified, not invented multi-field variant
identities. The latter shared descriptor/proof gap is documented separately.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import threading

import lance
import pyarrow as pa
from playwright.sync_api import expect

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace
from demiflow.workbench.analysis import web_service
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from test_forge_analysis_case_ui_browser import metadata_process,source_files
from test_forge_analysis_media_shell_browser import wire
from test_forge_public_api import service
from test_forge_markdown_reading import browser

OWN=Path(__file__).resolve().parents[1]/'demiflow/workbench/web'
expect.set_options(timeout=65000)


@contextmanager
def representative_case(browser,tmp_path,monkeypatch):
    root=tmp_path/'project';workspace.init_workspace(root,name='D1.8 structure · bounded Case fixture')
    (root/'data').mkdir();(root/'tables').mkdir()
    control=workspace.read_control(root/'workspace.json');previous=content_digest(control)
    control['locations']['local']=dict(root='data',writable=False)
    workspace.write_control(root/'workspace.json',control,expected=previous)
    rows=[dict(case_title=title,category=category,prompt=prompt,instructions='请检查颜色、结构与要求是否一致。此内容为测试材料。',reference='### 参考说明\n\n'+reference,
        answer_base='**Base · 示例回答**\n\n'+notes[0],answer_1200=None if notes[1] is None else '**Checkpoint 1200 · 示例回答**\n\n'+notes[1],
        answer_2400='**Checkpoint 2400 · 示例回答**\n\n'+notes[2],score=score)
        for title,category,prompt,reference,notes,score in [
            ('红色陶瓷杯','陶瓷杯','一只红色陶瓷杯，带圆形把手，放在浅色木桌上。','杯体应为红色，圆形把手。',['颜色偏蓝，与输入要求不同。','颜色正确；杯身比例偏宽。','颜色、把手与材质较接近要求。'],92.),
            ('蓝色波点杯','陶瓷杯','一只带白色波点的蓝色马克杯，柔和自然光。','蓝色杯体，白色波点。',['缺少波点。','有波点，排列不均。','杯体与波点较完整。'],90.),
            ('黄色雨伞','雨具','一把展开的黄色雨伞，弯柄，浅灰色背景。','展开黄色伞面与弯柄。',['伞面偏橙色。',None,'颜色与伞柄较完整。'],89.)]]
    pa.set_cpu_count(1);pa.set_io_thread_count(1)
    lance.write_dataset(pa.Table.from_pylist(rows),str(root/'data/cases.lance'))
    workspace.attach_table(root,'cases',relative_uri='cases.lance',version=1,dataset_id='demo-cases',schema_name='demo',schema_version='1')
    info=workspace.describe_table(root,'cases');columns=list(rows[0])
    locator=dict(kind='workspace_table',alias='cases',binding=info['binding']['digest'],columns=columns)
    registry=tmp_path/'registry.json';project=console.add(registry,root)['workspace_id'];console.migrate(registry,expected=content_digest(console.load_registry(registry)))
    calls=[];lock=threading.Lock()
    with metadata_process(tmp_path/'records') as client:
        class Helper(web_service.WebService):
            features={}
            def __init__(self,server,config):self.server=server
            def ceiling(self):return dict(analysis_execute=False,analysis_write=True,case_annotate=True)
            def execution_profile(self):return None # Metadata-only double has no admitted runtime.
            def assert_ready(self):return True
            def close(self):pass
            def cancel_output(self,key):pass
            def read_output(self,project,operation,payload,**kwargs):return self.call(project,operation,payload)
            def call(self,key,name,payload):
                inventory=self.server.inventory();actual,_=console.resolve_workspace(inventory,key);assert actual==root
                permissions={'read':True,**self.server.effective_permissions(inventory['projects'][key])}
                with lock:
                    calls.append((name,payload));value=client.call('fixture',root=str(root),project=key,permissions=permissions,name=name,payload=payload)
                if 'code' in value:raise ForgeError(value['error'],code=value['code'])
                return value['result']
        monkeypatch.setattr(web_service,'WebService',Helper)
        with service(None,registry=registry,analysis_config='explicit-demo-structure-private-authority-real-Records-Lance') as app:
            old=console.load_registry(registry);console.set_capabilities(registry,project,expected=content_digest(old),capabilities=dict(console.capabilities(old['projects'][project]),analysis_write=True,case_annotate=True),analysis_service=app.analysis)
            code,_,raw=wire(app,'/api/workspaces/'+project+'/analysis-source-register',body=dict(locator=locator),token=True);assert code==200,raw
            source=json.loads(raw);before=source_files(root)
            fields=[dict(id='f'+str(i),field=field,label=label,role=role,format=fmt) for i,(field,label,role,fmt) in enumerate([
                ('case_title','Case 名称','context','text'),('prompt','任务说明','input','text'),('instructions','复核要求','input','md'),('reference','参考材料','reference','md'),
                ('answer_base','Base 回答','output','md'),('answer_1200','Checkpoint 1200 回答','output','md'),('answer_2400','Checkpoint 2400 回答','output','md'),('category','类别','context','text'),('score','已有示例分','evaluation','json')])]
            questions=[dict(id='tags',title='标签',type='multi',required=False,options=[dict(id='favorite',label='收藏'),dict(id='review',label='待复核')]),
                dict(id='verdict',title='人工结论',type='choice',required=False,options=[dict(id='pass',label='通过'),dict(id='review',label='待复核'),dict(id='issue',label='存在问题')]),
                dict(id='quality',title='人工评分',type='rating',required=False,minimum=1,maximum=5),dict(id='note',title='备注',type='text',required=False)]
            descriptor=dict(version=1,source_snapshot=source['digest'],case_key=source['case_key'],fields=fields,grouping=['category'],filters=dict(op='range',field='score',value=dict(lower=0,upper=100)),sort=[],layout='comparison',questions=questions)
            draft=dict(id='v'+'2'*32,view_id=None,revision=0,base=None,view_touched=False,current=None,offset=0,answers=[],descriptor=descriptor)
            context=browser.new_context(viewport=dict(width=1440,height=1100));page=context.new_page();page.set_default_timeout(65000);errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
            page.add_init_script('''(()=>{const NativeWorker=Worker,stats=window.caseReadingStats={created:0,terminated:0,live:0,maxLive:0};window.Worker=function(url,options){const worker=new NativeWorker(url,options);if(String(url).endsWith('/markdown-worker.js')){stats.created++;stats.live++;stats.maxLive=Math.max(stats.maxLive,stats.live);const terminate=worker.terminate.bind(worker);let alive=true;worker.terminate=()=>{if(alive){alive=false;stats.terminated++;stats.live--;}terminate();};}return worker;};Worker.prototype=NativeWorker.prototype;})();''')
            def serve_asset(path):
                def handle(route):return route.fulfill(content_type='text/css' if path.suffix=='.css' else 'text/javascript',body=path.read_text())
                return handle
            for name in ['analysis-case.js','analysis-case.css','analysis-case-media.js']:page.route('**/'+name,serve_asset(OWN/name))
            if os.environ.get('FORGE_CASE_READING_PROPOSAL'):page.route('**/reading.js',serve_asset(Path(os.environ['FORGE_CASE_READING_PROPOSAL'])))
            try:
                page.goto(app.origin+'/#token='+app.token)
                page.evaluate('(value)=>new ForgeAnalysisState.Store().saveCaseDraft(value.project,value.source,value.draft)',dict(project=project,source=source['digest'],draft=draft))
                page.get_by_role('button',name='数据',exact=True).click()
                expect(page.get_by_role('heading',name='Case 0',exact=True)).to_be_visible()
                expect(page.get_by_label('标注 人工结论',exact=True)).to_be_visible()
                yield dict(page=page,app=app,source=source,project=project,calls=calls,errors=errors,descriptor=descriptor,root=root,draft_id=draft['id'])
                assert not errors,errors
            except BaseException:
                page.screenshot(path=str(tmp_path/'demo-structure-failure.png'),full_page=True);raise
            finally:
                page.unroute_all(behavior='ignoreErrors');context.close();assert source_files(root)==before,'UI wrote its fixed source'


def test_demo_roles_layouts_visible_selection_field_answers_and_configuration(browser,tmp_path,monkeypatch):
    with representative_case(browser,tmp_path,monkeypatch) as f:
        p=f['page'];case=p.locator('.analysis-case');observations={}
        expect(case.locator('.case-ui-shared-materials [data-role=input]')).to_have_count(2)
        expect(case.locator('.case-ui-shared-materials [data-role=reference]')).to_have_count(1)
        expect(case.locator('.case-ui-output-card')).to_have_count(3)
        expect(case.locator('.case-ui-list-group')).to_have_count(2)
        expect(case.locator('.reading-content strong')).to_have_count(3)
        expect(case.locator('.reading-content h3')).to_have_count(1)
        assert not case.get_by_text('等待的预览已被更新的选择替代',exact=False).count()
        expect(p.get_by_label('筛选字段',exact=True)).to_be_visible()
        expect(p.get_by_label('分组取值 category',exact=True)).to_be_visible()
        for width in (1440,1024,760,390):
            p.set_viewport_size(dict(width=width,height=1100))
            assert p.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
            expect(p.get_by_role('button',name='Case 0',exact=True)).to_be_visible()
            expect(p.get_by_role('button',name='Case 1',exact=True)).to_be_visible()
            expect(p.get_by_role('button',name='应用筛选与分组',exact=True)).to_be_visible()
            expect(p.get_by_label('筛选字段',exact=True)).to_be_visible()
            observations[str(width)]=p.evaluate('''()=>Object.fromEntries(['.analysis-case-rail','.analysis-case-content','.analysis-case-review'].map(selector=>{const r=document.querySelector(selector).getBoundingClientRect();return [selector,{width:r.width,x:r.x,top:r.top+scrollY}]}))''')
            p.evaluate('scrollTo({top:0,behavior:"instant"})');p.screenshot(path=str(tmp_path/('demo-comparison-'+str(width)+'.png')),full_page=True)
        p.set_viewport_size(dict(width=1440,height=1100))
        p.get_by_label('分组取值 category',exact=True).select_option(label='雨具');expect(case.locator('.analysis-case-list button')).to_have_count(1)
        p.get_by_label('分组取值 category',exact=True).select_option('');expect(case.locator('.analysis-case-list button')).to_have_count(3)
        p.get_by_role('button',name='单条',exact=True).click();p.get_by_label('显示输出字段',exact=True).select_option('answer_1200')
        expect(case.locator('.case-ui-output-card')).to_have_count(1)
        expect(case.locator('.case-ui-output-card')).to_contain_text('Checkpoint 1200')
        p.get_by_role('button',name='画廊',exact=True).click()
        expect(case.locator('.case-ui-output-card')).to_have_count(3)
        expect(case.locator('.case-ui-output-card[data-case-key="2"]')).to_contain_text('null')
        expect(case.locator('.case-ui-output-grid .reading-content strong')).to_have_count(2)
        p.screenshot(path=str(tmp_path/'demo-gallery.png'),full_page=True)
        p.get_by_label('复核 Case 2 输出字段 Checkpoint 1200 回答',exact=True).click()
        expect(p.get_by_label('标注对象',exact=True)).to_have_value('answer_1200')
        p.get_by_label('标注 人工结论',exact=True).select_option('review');p.get_by_label('标注 备注',exact=True).fill('此输出字段的观察')
        p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(case.locator('.analysis-case-review')).to_contain_text('当前答案 v1')
        sent=[payload for name,payload in f['calls'] if name=='annotations-save'][-1]
        assert sent['target']['field']=='answer_1200' and sent['target']['case_key']=='2' and sent['target']['variant'] is None
        assert sent['origin']['reference']==f['draft_id']
        cached=p.evaluate('(value)=>new ForgeAnalysisState.Store().caseDraft(value.project,value.digest)',dict(project=f['project'],digest=f['source']['digest']))
        assert cached['id'].startswith('case-fields-v2:')
        legacy=browser.new_context(storage_state=p.context.storage_state());old_page=legacy.new_page();old_page.set_default_timeout(65000)
        writes_before=len([name for name,_ in f['calls'] if name=='annotations-save'])
        try:
            legacy_source=os.environ.get('FORGE_CASE_LEGACY_JS')
            if legacy_source:
                legacy_bytes=Path(legacy_source).read_bytes();assert len(legacy_bytes)<=131072
                expected=os.environ.get('FORGE_CASE_LEGACY_JS_SHA256')
                assert expected and hashlib.sha256(legacy_bytes).hexdigest()==expected
                old_page.route('**/analysis-case.js',lambda route:route.fulfill(status=200,content_type='text/javascript',body=legacy_bytes))
            old_page.goto(f['app'].origin+'/#token='+f['app'].token);old_page.get_by_role('button',name='数据',exact=True).click()
            if legacy_source:
                expect(old_page.locator('.analysis-case')).to_contain_text('无效的记录标识')
                expect(old_page.get_by_label('标注 备注',exact=True)).to_have_count(0)
            else:
                # Normal current-source collection remounts this same client;
                # the explicit frozen-source run above proves old refusal.
                expect(old_page.get_by_role('heading',name='Case 2',exact=True)).to_be_visible()
                expect(old_page.get_by_label('标注 备注',exact=True)).to_have_value('此输出字段的观察')
            assert len([name for name,_ in f['calls'] if name=='annotations-save'])==writes_before
        finally:legacy.close()
        p.get_by_label('标注对象',exact=True).select_option('');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('')
        p.get_by_label('标注 备注',exact=True).fill('整个 Case 的草稿')
        p.get_by_label('标注对象',exact=True).select_option('answer_1200');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('此输出字段的观察')
        note_box=case.locator('.analysis-answer').filter(has=p.get_by_role('heading',name='备注',exact=True));note_box.get_by_text('标注历史与恢复',exact=True).click();note_box.get_by_role('button',name='读取历史',exact=True).click()
        expect(note_box.locator('.case-ui-history-entry')).to_contain_text('此输出字段的观察')
        assert [payload for name,payload in f['calls'] if name=='annotations'][-1]['target']['field']=='answer_1200'
        p.reload(wait_until='domcontentloaded');p.get_by_role('button',name='数据',exact=True).click()
        expect(p.get_by_label('标注对象',exact=True)).to_have_value('answer_1200')
        expect(p.get_by_label('标注 备注',exact=True)).to_have_value('此输出字段的观察')
        p.get_by_label('标注对象',exact=True).select_option('');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('整个 Case 的草稿')
        p.get_by_role('button',name='Case 0',exact=True).click();p.get_by_label('标注 备注',exact=True).fill('Case 0 已复核')
        p.get_by_role('button',name='保存并下一条',exact=True).click();expect(p.get_by_role('heading',name='Case 1',exact=True)).to_be_visible()
        assert [payload for name,payload in f['calls'] if name=='annotations-save'][-1]['target']['case_key']=='0'
        p.get_by_role('button',name='配置评测',exact=True).click();p.get_by_label('评测题目 2',exact=True).fill('复核结论')
        p.get_by_role('button',name='应用评测定义',exact=True).click();expect(p.get_by_label('标注 复核结论',exact=True)).to_have_value('')
        p.get_by_role('button',name='内容字段与格式',exact=True).click();p.get_by_label('字段角色 3',exact=True).select_option('reference')
        p.get_by_label('预览格式 3',exact=True).select_option('code');expect(p.get_by_label('代码语言 3',exact=True)).to_be_visible()
        p.get_by_label('代码语言 3',exact=True).select_option('text')
        p.get_by_role('button',name='字段与评测配置',exact=True).click()
        expect(case.locator('.case-ui-shared-materials [data-role=input]')).to_have_count(1)
        expect(case.locator('.case-ui-shared-materials [data-role=reference]')).to_have_count(2)
        p.get_by_role('button',name='Case 2',exact=True).click()
        p.get_by_label('筛选字段',exact=True).select_option('category');p.get_by_label('筛选值',exact=True).fill('陶瓷杯')
        p.get_by_role('button',name='应用筛选与分组',exact=True).click()
        expect(case.locator('.analysis-case-list button')).to_have_count(2)
        expect(p.get_by_role('heading',name='Case 2',exact=True)).to_be_visible()
        expect(case.locator('.analysis-case-content')).to_contain_text('当前 Case 不在本次选样页')
        expect(p.get_by_label('标注 备注',exact=True)).to_have_value('整个 Case 的草稿')
        p.get_by_role('button',name='清除筛选条件',exact=True).click();expect(case.locator('.analysis-case-list button')).to_have_count(3)
        p.get_by_role('button',name='撤销当前答案修改',exact=True).click();expect(p.get_by_label('标注 备注',exact=True)).to_have_value('')
        p.get_by_label('标注对象',exact=True).select_option('answer_1200');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('此输出字段的观察')
        p.get_by_label('标注对象',exact=True).select_option('')
        p.get_by_role('button',name='保存视图',exact=True).click();expect(case.locator('.analysis-case-status')).to_contain_text('已存视图 v1')
        p.screenshot(path=str(tmp_path/'demo-configured-fields.png'),full_page=True)
        p.get_by_role('button',name='Notebook',exact=True).click();expect(case).to_be_hidden()
        p.wait_for_function('caseReadingStats.live===0');assert p.evaluate('caseReadingStats.maxLive<=1')
        observations['markdown_workers']=p.evaluate('caseReadingStats')
        assert not any(name in {'session-start','cell-submit','execute'} for name,_ in f['calls'])
        (tmp_path/'demo-operations.json').write_text(json.dumps(dict(responsive=observations,source=f['source']['digest'],rows=3,output_fields=3,questions=4,field_target_and_case_drafts_isolated=True,save_next=True,question_definition_changed=True,role_format_language_configured=True,filtered_out_case_preserved=True,source_unchanged=True,full_variant_groups_verified=False),ensure_ascii=False,indent=2)+'\n')
