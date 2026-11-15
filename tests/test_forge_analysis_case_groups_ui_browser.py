"""D8 Case consumer over exact private HTTP/SQLite/Lance/fixed PNG inputs.

The public helper/reader/controller are a read-only snapshot. Runtime/watchdog
and deferred Task scheduling in its media composition driver are explicit doubles.
Notebook registration uses sealed execution metadata, not a kernel or B1.
"""
from contextlib import contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import selectors
import re
import subprocess
import sys
import threading

import lance
import pyarrow as pa
import pytest
from playwright.sync_api import expect

Image=pytest.importorskip('PIL.Image')
ImageDraw=pytest.importorskip('PIL.ImageDraw')

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace
from demiflow.workbench.analysis import web_sources
from demiflow.workbench.analysis import web_service
from demiflow.workbench.analysis.output_transport import OutputRange
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from test_forge_analysis_media_shell_browser import wire
from test_forge_markdown_reading import browser
from test_forge_public_api import service

OWN=Path(__file__).resolve().parents[1]
BACKEND=Path(os.environ.get('FORGE_CASE_D8_BACKEND_ROOT',str(OWN)))
expect.set_options(timeout=65000)


def inventory(root):
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}


def cup(color,kind):
    image=Image.new('RGB',(384,288),'#f5f1e8');draw=ImageDraw.Draw(image)
    draw.rectangle((0,212,384,288),fill='#d8c8af');draw.ellipse((66,196,315,244),fill='#b5a994')
    draw.ellipse((245,83,324,190),outline=color,width=20)
    draw.rounded_rectangle((87,69,270,220),radius=27,fill=color)
    draw.ellipse((87,55,270,101),fill='#fff5e4',outline=color,width=12)
    if kind==1:
        for x,y in [(120,120),(165,120),(210,120),(143,167),(192,167)]:draw.ellipse((x-7,y-7,x+7,y+7),fill='white')
    output=io.BytesIO();image.save(output,format='PNG');return output.getvalue()


@contextmanager
def rpc(tmp_path,config,*,lock):
    original=BACKEND/'tests/_forge_analysis_media_capture_driver.py';text=original.read_text()
    needle="result=controller.metadata.route(authority,name,payload)";assert text.count(needle)==1
    text=text.replace(needle,"result=register_output_fixture(records,authority,payload) if name=='fixture-output-register' else controller.metadata.route(authority,name,payload)")
    text="import sys\nsys.path.insert(0,"+repr(str(OWN/'tests'))+")\nfrom _forge_analysis_case_ui_controller import register_output_fixture\n"+text
    driver=tmp_path/'case-private-media-driver.py';driver.write_text(text)
    (tmp_path/'driver-input.json').write_text(json.dumps(dict(base=str(original),base_sha256=hashlib.sha256(original.read_bytes()).hexdigest(),overlay_sha256=hashlib.sha256(text.encode()).hexdigest(),scope='Test-only sealed Notebook registration branch; original media/native/SQLite/PNG driver and explicit Runtime doubles'),indent=2)+'\n')
    process=subprocess.Popen([sys.executable,'-B','-u',str(driver),'rpc',str(config)],cwd=BACKEND,env=dict(os.environ,PYTHONPATH=str(BACKEND),PYTHONDONTWRITEBYTECODE='1',FORGE_CASE_ORIGIN_BACKEND_ROOT=str(BACKEND)),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    def receive():
        with selectors.DefaultSelector() as poll:
            poll.register(process.stdout,selectors.EVENT_READ);assert poll.select(75),'private fixture timed out'
        line=process.stdout.readline();assert line,process.stderr.read();return json.loads(line)
    def call(value):process.stdin.write(json.dumps(value)+'\n');process.stdin.flush();return receive()
    try:assert receive()==dict(ready=True);yield call
    finally:
        acquired=lock.acquire(timeout=60)
        if acquired:
            try:process.stdin.close()
            finally:lock.release()
        else:process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        error=process.stderr.read();process.stdout.close();process.stderr.close();assert process.returncode==0,error


@contextmanager
def scene(browser,tmp_path,monkeypatch,*,count=3,reserved=False):
    root=tmp_path/'project';workspace.init_workspace(root,name='D8 · 输出方案复核');(root/'data').mkdir();(root/'tables').mkdir()
    control=workspace.read_control(root/'workspace.json');old=content_digest(control);control['locations']['local']=dict(root='data',writable=False);workspace.write_control(root/'workspace.json',control,expected=old)
    rows=[]
    for i in range(count):
        title=['红色陶瓷杯','蓝色波点杯','青绿色杯'][i%3]
        row=dict(case_title=title,category='杯具',prompt='检查'+title+'的颜色、把手与材质。',reference='### 共同参考\n\n目标与参考图保持一致。',spare_answer='补充复核材料 '+str(i))
        for name,color in [('reference','#ba3b3b'),('base','#3c6fa4'),('next','#b9413d')]:
            data=cup(color,i%2);filename=name+str(i)+'.png';(root/'data'/filename).write_bytes(data);row[name+'_image']=filename;row[name+'_sha']=hashlib.sha256(data).hexdigest()
        row.update(base_answer='**Base 回答**\n\n颜色偏蓝，杯形完整。',base_score=62.+i,next_answer=None if i==2 else '**Checkpoint 回答**\n\n颜色接近目标，把手较完整。',next_score=91.+i);
        if reserved:row['group:base']='保留命名空间字段'
        rows.append(row)
    pa.set_cpu_count(1);pa.set_io_thread_count(1)
    for alias,data in [('cases',rows),('other',rows[:1])]:
        lance.write_dataset(pa.Table.from_pylist(data),str(root/'data'/(alias+'.lance')))
        attached=workspace.attach_table(root,alias,relative_uri=alias+'.lance',version=1,dataset_id=alias,schema_name='group-demo',schema_version='1')
        workspace.set_table_view(root,alias,expected=attached['digest'],display=dict(fields=[dict(path=[name+'_image'],kind='image',location_id='local',sha256_path=[name+'_sha'],max_bytes=1024*1024,mime='image/png') for name in ['reference','base','next']]+[dict(path=[name],kind='json' if name.endswith('_score') else 'text') for name in ['case_title','category','prompt','reference','base_answer','base_score','next_answer','next_score','spare_answer']+(['group:base'] if reserved else [])]),provenance={})
    fields=[dict(id=name,field=name,label=label,role=role,format=fmt) for name,label,role,fmt in [
        ('case_title','Case 名称','context','text'),('category','类别','context','text'),('prompt','任务说明','input','text'),('reference','参考说明','reference','md'),('reference_image','参考图片','reference','image'),
        ('base_image','Base 图片','output','image'),('base_answer','Base 回答','output','md'),('base_score','Base 模型分','evaluation','json'),
        ('next_image','Checkpoint 图片','output','image'),('next_answer','Checkpoint 回答','output','md'),('next_score','Checkpoint 模型分','evaluation','json'),('spare_answer','补充回答','output','text')]]
    if reserved:fields.append(dict(id='reserved',field='group:base',label='保留名字段',role='output',format='text'))
    info=workspace.describe_table(root,'cases');locator=dict(kind='workspace_table',alias='cases',binding=info['binding']['digest'],columns=list(rows[0]));source=web_sources.resolve(root,locator)['source']
    registry=tmp_path/'registry.json';project=console.add(registry,root)['workspace_id'];console.migrate(registry,expected=content_digest(console.load_registry(registry)))
    outbox=tmp_path/'outbox';outbox.mkdir();outbox.chmod(0o2770)
    config=tmp_path/'fixture.json';config.write_text(json.dumps(dict(workspace=str(root),outbox=str(outbox),state=str(tmp_path/'state'),project=project,locator=locator,source=source)))
    questions=[dict(id='tags',title='标签',type='multi',required=False,options=[dict(id='favorite',label='收藏'),dict(id='review',label='待复核')]),dict(id='verdict',title='结论',type='choice',required=False,options=[dict(id='yes',label='通过'),dict(id='review',label='待复核')]),dict(id='score',title='人工评分',type='rating',required=False,minimum=1,maximum=5),dict(id='note',title='备注',type='text',required=False)]
    descriptor=dict(version=2,source_snapshot=source['digest'],case_key=source['case_key'],fields=fields,output_groups=[dict(id='base',label='Base',fields=['base_image','base_answer','base_score']),dict(id='next',label='Checkpoint',fields=['next_image','next_answer','next_score'])],grouping=['category'],filters=None,sort=[],layout='comparison',questions=questions)
    draft=dict(id='v'+'9'*32,view_id=None,revision=0,base=None,view_touched=False,current=None,offset=0,answers=[],review_group='base',descriptor=descriptor)
    calls=[];responses=[];lock=threading.Lock();before=inventory(root)
    with rpc(tmp_path,config,lock=lock) as invoke:
        class Helper(web_service.WebService):
            principal='fixture-reader';features=dict(case_media=True,annotation_media_basis=True,case_output_groups=True,case_review_filter=True)
            def __init__(self,server,config):self.server=server
            def ceiling(self):return dict(analysis_write=True,case_annotate=True,analysis_execute=False)
            def execution_profile(self):return None # Metadata-only double has no admitted runtime.
            def assert_ready(self):return True
            def close(self):pass
            def cancel_output(self,key):pass
            def call(self,key,name,payload):
                actual,_=console.resolve_workspace(self.server.inventory(),key);assert actual==root
                permissions={'read':True,**self.server.effective_permissions(self.server.inventory()['projects'][key])}
                with lock:calls.append((name,payload));value=invoke(dict(name=name,payload=payload,permissions=permissions));responses.append((name,value))
                if 'error' in value:raise ForgeError(value['error'],code=value['code'])
                return value['result']
            def read_output(self,key,name,payload,**kwargs):
                value=self.call(key,name,payload)
                if value.get('format')=='fixture-verified-range':return OutputRange(value['artifact'],value['metadata'],bytes.fromhex(value['data']))
                return value
        monkeypatch.setattr(web_service,'WebService',Helper)
        with service(None,registry=registry,analysis_config='D8 private real Records/native media; Runtime doubles') as app:
            def grant(**changes):
                value=console.load_registry(registry);return console.set_capabilities(registry,project,expected=content_digest(value),capabilities=dict(console.capabilities(value['projects'][project]),**changes),analysis_service=app.analysis)
            grant(analysis_write=True,case_annotate=True)
            context=browser.new_context(viewport=dict(width=1440,height=1100));page=context.new_page();page.set_default_timeout(65000);page.add_init_script("(()=>{let exported;Object.defineProperty(window,'ForgeCase',{configurable:true,get(){return exported;},set(value){exported={...value,mount(options){const handle=value.mount(options);window.fixtureCase=handle;return handle;}};}});})();");errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
            def asset(path):
                def serve(route):route.fulfill(content_type='text/css' if path.suffix=='.css' else 'text/javascript',body=path.read_text())
                return serve
            for name in ['analysis-case.js','analysis-case.css','analysis-case-media.js']:page.route('**/'+name,asset(OWN/'demiflow/workbench/web'/name))
            try:
                page.goto(app.origin+'/#token='+app.token);page.get_by_role('button',name='数据',exact=True).click();expect(page.get_by_role('heading',name='Case 0',exact=True)).to_be_visible()
                source=[value['result'] for name,value in responses if name=='source-register' and 'result' in value][-1]
                descriptor['source_snapshot']=source['digest'];descriptor['case_key']=source['case_key']
                page.get_by_role('button',name='管道',exact=True).click();expect(page.locator('.analysis-case')).to_have_count(0)
                page.evaluate('(v)=>new ForgeAnalysisState.Store().saveCaseDraft(v.project,v.source,v.draft)',dict(project=project,source=source['digest'],draft=draft))
                page.get_by_role('button',name='数据',exact=True).click();expect(page.get_by_role('heading',name='Case 0',exact=True)).to_be_visible()
                assert [payload for name,payload in calls if name=='case-page'][-1]['view']['descriptor']['version']==2, 'fixture draft was not adopted for the actual source'
                yield dict(page=page,app=app,source=source,descriptor=descriptor,project=project,calls=calls,responses=responses,errors=errors,grant=grant,root=root,asset=asset)
                assert not errors,errors
            except BaseException:
                (tmp_path/'failure-operations.json').write_text(json.dumps(dict(cache=page.evaluate('(v)=>new ForgeAnalysisState.Store().caseDraft(v.project,v.source)',dict(project=project,source=source['digest'])),calls=calls[-128:],group_responses=[value for name,value in responses if name=='case-group-bindings'][-16:],images=page.locator('img.analysis-media').evaluate_all('(items)=>items.map(image=>({field:image.closest(".analysis-case-field")?.dataset.field,case_key:image.closest(".analysis-case-field")?.dataset.caseKey,loading:image.loading,complete:image.complete,width:image.naturalWidth,height:image.naturalHeight,top:image.getBoundingClientRect().top}))'),errors=errors),ensure_ascii=False,indent=2)+'\n')
                page.screenshot(path=str(tmp_path/'failure.png'),full_page=True);raise
            finally:page.unroute_all(behavior='ignoreErrors');context.close();assert inventory(root)==before,'Case changed its fixed source project'


def save(page,text):
    page.get_by_label('标注 备注',exact=True).fill(text);page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
    note=page.locator('.analysis-answer').filter(has=page.get_by_role('heading',name='备注',exact=True));expect(note.locator(':scope > p').filter(has_text='当前答案')).to_have_text(re.compile(r'^当前答案 v[1-9][0-9]*$'));expect(page.get_by_label('标注 备注',exact=True)).to_have_value(text)


def apply_filter(page):
    target=page.get_by_label('标注筛选对象',exact=True).locator('option:checked').inner_text();match=page.get_by_label('标注状态条件',exact=True);description=match.locator('option:checked').inner_text()
    if match.input_value()=='option':description+=' · '+page.get_by_label('筛选评测题目',exact=True).locator('option:checked').inner_text()+' = '+page.get_by_label('筛选评测选项',exact=True).locator('option:checked').inner_text()
    page.get_by_role('button',name='应用标注状态筛选',exact=True).click();expect(page.locator('.case-ui-review-filter-active')).to_contain_text(target+' · '+description);expect(page.get_by_role('button',name='应用标注状态筛选',exact=True)).to_be_enabled()


def latest(f,name):return [payload for operation,payload in f['calls'] if operation==name][-1]


def registered(page):
    page.get_by_role('button',name='保存配置并登记方案',exact=True).click();expect(page.get_by_label('标注 备注',exact=True)).to_be_visible()


def group_binding(f,group='base'):
    return next(item for name,value in reversed(f['responses']) if name=='case-group-bindings' and 'result' in value for item in value['result']['groups'] if item['id']==group and item['registered'])


def test_group_semantics_cas_original_receipt_late_source_and_notebook_origin(browser,tmp_path,monkeypatch):
    import copy
    with scene(browser,tmp_path,monkeypatch) as f:
        p=f['page'];case=p.locator('.analysis-case');registered(p);save(p,'原 Base 方案');old=group_binding(f);old_variant=old['variant']
        p.get_by_role('button',name='字段与评测配置',exact=True).click();p.get_by_label('方案标题 1',exact=True).fill('Base（改名）');before_reads=len([name for name,_ in f['calls'] if name=='case-page']);case.locator('.case-ui-output-group[data-group-id=base] > button').click();expect(case.locator('.analysis-case-review')).to_contain_text('请应用配置');expect(p.get_by_label('标注 备注',exact=True)).to_have_count(0);assert len([name for name,_ in f['calls'] if name=='case-page'])==before_reads;case.locator('.case-ui-group-config').get_by_role('button',name='应用材料与方案配置',exact=True).click()
        expect(case.locator('.case-ui-group-proof')).to_contain_text('Base（改名）');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('原 Base 方案');assert group_binding(f)['variant']==old_variant
        p.get_by_label('方案 1 材料 spare_answer',exact=True).check();expect(p.get_by_label('方案 2 材料 spare_answer',exact=True)).to_be_disabled();p.get_by_label('方案 1 材料 spare_answer',exact=True).uncheck();expect(p.get_by_label('方案 2 材料 spare_answer',exact=True)).to_be_enabled();p.get_by_label('方案 1 材料 spare_answer',exact=True).check();case.locator('.case-ui-group-config').get_by_role('button',name='应用材料与方案配置',exact=True).click()
        expect(p.get_by_role('button',name='保存配置并登记方案',exact=True)).to_be_visible();expect(p.get_by_label('标注 备注',exact=True)).to_have_count(0)
        p.get_by_role('button',name='字段与评测配置',exact=True).click();registered(p);new_variant=group_binding(f)['variant'];assert new_variant!=old_variant
        expect(p.get_by_label('标注 备注',exact=True)).to_have_value('')
        p.get_by_label('标注对象',exact=True).select_option('old:'+old_variant);expect(p.get_by_label('标注 备注',exact=True)).to_have_value('原 Base 方案')
        remote=copy.deepcopy(latest(f,'annotations-save'));remote['request_id']='other-principal-window-same-owner';remote['entries'][0]['expected_revision']=1;remote['entries'][0]['value']='远端原方案 v2'
        f['app'].analysis.call(f['project'],'annotations-save',remote)
        p.get_by_label('标注 备注',exact=True).fill('保留原方案本地修改');p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(case.locator('.analysis-case-review')).to_contain_text('保存冲突');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('保留原方案本地修改')
        p.get_by_role('button',name='保留草稿并采用服务器版本',exact=True).click();save(p,'保留原方案本地修改');assert latest(f,'annotations-save')['target']['variant']==old_variant
        p.evaluate('''()=>{const native=window.fetch;let dropped=false;window.fetch=async(...args)=>{const response=await native(...args);if(!dropped&&String(args[0]).endsWith('/analysis-annotations-save')){dropped=true;throw Error('Fixture lost delivery after real commit');}return response;};}''')
        p.get_by_label('标注 备注',exact=True).fill('原请求已提交');p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(p.get_by_role('button',name='核对 Case 原请求',exact=True)).to_be_visible();lost=latest(f,'annotations-save');assert lost['target']['variant']==old_variant
        p.get_by_label('标注对象',exact=True).select_option('group:base');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('');p.get_by_label('标注 备注',exact=True).fill('新方案独立草稿')
        before=len([name for name,_ in f['calls'] if name=='annotations-save']);p.get_by_role('button',name='核对 Case 原请求',exact=True).click();expect(p.get_by_role('button',name='核对 Case 原请求',exact=True)).to_have_count(0);expect(p.get_by_label('标注 备注',exact=True)).to_have_value('新方案独立草稿');assert len([name for name,_ in f['calls'] if name=='annotations-save'])==before
        p.get_by_label('标注对象',exact=True).select_option('old:'+old_variant);expect(p.get_by_label('标注 备注',exact=True)).to_have_value('原请求已提交')
        p.get_by_label('标注 备注',exact=True).fill('撤权后保留');f['grant'](case_annotate=False);p.get_by_role('button',name='保存本 Case 标注',exact=True).click();expect(case.locator('.case-ui-notice')).to_contain_text('case_annotate');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('撤权后保留');f['grant'](case_annotate=True)
        p.evaluate('''()=>{const native=window.fetch;window.heldGroupHeads=[];window.holdGroupHeads=true;window.fetch=async(...args)=>{const response=await native(...args);if(holdGroupHeads&&String(args[0]).endsWith('/analysis-annotation-heads')){holdGroupHeads=false;return new Promise(resolve=>heldGroupHeads.push(()=>resolve(response)));}return response;};}''')
        p.get_by_role('button',name='刷新当前标注',exact=True).click();p.wait_for_function('()=>heldGroupHeads.length===1');p.get_by_label('当前数据表',exact=True).select_option('other');p.evaluate('heldGroupHeads.shift()()');expect(p.get_by_label('标注 结果是否符合预期',exact=True)).to_be_visible();expect(p.get_by_label('标注 备注',exact=True)).to_have_count(0);assert latest(f,'annotation-heads')['target']['variant'] is None
        p.get_by_label('当前数据表',exact=True).select_option('cases');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('撤权后保留')
        incoming=copy.deepcopy(f['descriptor']);incoming['output_groups'][0]['label']='Notebook Base';proof=f['app'].analysis.call(f['project'],'fixture-output-register',dict(descriptor=incoming,register=True));assert proof['annotation_origin_registered'] is True
        p.evaluate('value=>fixtureCase.setPresentation(value)',proof);p.get_by_role('button',name='采用 Notebook 输出视图',exact=True).click();expect(case.locator('.case-ui-group-proof')).to_contain_text('Base 图片');save(p,'Notebook 登记来源');sent=latest(f,'annotations-save');assert sent['target']['variant']==old_variant and sent['origin']==proof['origin']
        p.get_by_label('标注对象',exact=True).select_option('old:'+new_variant);expect(p.get_by_label('标注 备注',exact=True)).to_have_value('新方案独立草稿')
        note=case.locator('.analysis-answer').filter(has=p.get_by_role('heading',name='备注',exact=True));p.get_by_label('标注对象',exact=True).select_option('group:base');note.get_by_text('标注历史与恢复',exact=True).click();note.get_by_role('button',name='读取历史',exact=True).click();expect(note.locator('.case-ui-history-entry').first).to_contain_text('Notebook 登记来源')
        p.get_by_role('button',name='字段与评测配置',exact=True).click();p.get_by_label('字段角色 1',exact=True).select_option('reference');p.get_by_label('预览格式 1',exact=True).select_option('code');p.get_by_label('代码语言 1',exact=True).select_option('text');case.locator('.case-ui-group-config').get_by_role('button',name='应用材料与方案配置',exact=True).click();expect(case.locator('.case-ui-shared-materials [data-role=reference]')).to_have_count(3)
        p.get_by_role('button',name='保存视图',exact=True).click();expect(case.locator('.case-ui-notice')).to_contain_text('视图已保存');p.reload(wait_until='domcontentloaded');p.get_by_role('button',name='数据',exact=True).click();expect(p.get_by_label('标注 备注',exact=True)).to_have_value('Notebook 登记来源');p.get_by_role('button',name='字段与评测配置',exact=True).click();expect(p.get_by_label('字段角色 1',exact=True)).to_have_value('reference');expect(p.get_by_label('预览格式 1',exact=True)).to_have_value('code');expect(p.get_by_label('代码语言 1',exact=True)).to_have_value('text')
        (tmp_path/'semantic-operations.json').write_text(json.dumps(dict(old_variant=old_variant,new_variant=new_variant,label_same_identity=True,members_new_identity=True,cas=True,original_receipt=True,source_switch=True,late_heads_ignored=True,notebook_origin=proof['origin'],role_format_language_persist=True,source_unchanged=True),indent=2)+'\n')


def test_server_review_filter_full_source_frozen_cursor_explicit_scope_and_definitions(browser,tmp_path,monkeypatch):
    from demiflow.workbench.analysis import contracts as c
    with scene(browser,tmp_path,monkeypatch,count=24) as f:
        p=f['page'];case=p.locator('.analysis-case');registered(p);binding=group_binding(f);variant=binding['variant'];note=f['descriptor']['questions'][-1]
        for number in range(23):
            f['app'].analysis.call(f['project'],'annotations-save',dict(request_id='seed-note-'+str(number),target={**binding['target'],'case_key':str(number)},entries=[dict(definition=note,expected_revision=0,action='set',value='已保存 '+str(number))],origin=dict(kind='unsaved_view',reference='explicit-private-fixture')))
        p.get_by_label('标注筛选对象',exact=True).select_option('group:base');p.get_by_label('标注状态条件',exact=True).select_option('answered_any');apply_filter(p);expect(case.locator('.analysis-case-list button')).to_have_count(20);expect(case.locator('.analysis-case-coverage')).to_contain_text('范围内匹配 23 条');cut=latest(f,'case-page')['review'];first=[value['result'] for name,value in f['responses'] if name=='case-page' and 'result' in value][-1];watermark=first['annotation_watermark']
        expect(p.get_by_label('标注 备注',exact=True)).to_have_value('已保存 0');p.get_by_label('标注 标签 待复核',exact=True).check();p.get_by_label('标注 结论',exact=True).select_option('review');p.get_by_label('标注 人工评分',exact=True).fill('4');save(p,'完整四题')
        f['app'].analysis.call(f['project'],'annotations-save',dict(request_id='clear-after-cut',target={**binding['target'],'case_key':'20'},entries=[dict(definition=note,expected_revision=1,action='clear')],origin=dict(kind='unsaved_view',reference='explicit-private-fixture')))
        p.get_by_role('button',name='下一页',exact=True).click();expect(case.locator('.analysis-case-list button')).to_have_count(3);assert set(case.locator('.analysis-case-list button').evaluate_all('(items)=>items.map(item=>item.dataset.caseKey)'))=={'20','21','22'};last=latest(f,'case-page');assert last['cursor']['annotation_watermark']==watermark and last['review']==cut
        p.get_by_role('button',name='读取 Case',exact=True).click();expect(case.locator('.analysis-case-coverage')).to_contain_text('范围内匹配 22 条')
        p.get_by_label('标注状态条件',exact=True).select_option('unanswered');apply_filter(p);expect(case.locator('.analysis-case-list button')).to_have_count(2);assert set(case.locator('.analysis-case-list button').evaluate_all('(items)=>items.map(item=>item.dataset.caseKey)'))=={'20','23'}
        p.get_by_role('button',name='Case 20',exact=True).click();p.get_by_label('标注对象',exact=True).select_option('base_answer');save(p,'只有字段答案');saved_cut=[value['result']['annotation_watermark'] for name,value in f['responses'] if name=='annotation-heads' and 'result' in value][-1];p.get_by_role('button',name='读取 Case',exact=True).click();expect(case.locator('.case-ui-review-filter-active')).to_contain_text('快照 '+str(saved_cut));expect(case.locator('.analysis-case-list button')).to_have_count(2);assert latest(f,'case-page')['review']['variant']==variant and latest(f,'case-page')['review']['field'] is None
        p.get_by_label('标注状态条件',exact=True).select_option('answered_all');apply_filter(p);expect(case.locator('.analysis-case-list button')).to_have_count(1);assert case.locator('.analysis-case-list button').get_attribute('data-case-key')=='0'
        p.get_by_label('标注状态条件',exact=True).select_option('option');p.get_by_label('筛选评测题目',exact=True).select_option('verdict');p.get_by_label('筛选评测选项',exact=True).select_option('review');apply_filter(p);expect(case.locator('.analysis-case-list button')).to_have_count(1);assert latest(f,'case-page')['review']['option']==dict(definition=c.digest(f['descriptor']['questions'][1]),value='review')
        p.get_by_label('筛选评测题目',exact=True).select_option('tags');p.get_by_label('筛选评测选项',exact=True).select_option('review');apply_filter(p);expect(case.locator('.analysis-case-list button')).to_have_count(1)
        p.get_by_label('标注筛选对象',exact=True).select_option('case');p.get_by_label('标注状态条件',exact=True).select_option('answered_any');apply_filter(p);expect(case.locator('.analysis-case-list button')).to_have_count(0)
        p.get_by_label('标注筛选对象',exact=True).select_option('group:base');p.get_by_label('标注状态条件',exact=True).select_option('answered_any');apply_filter(p);expect(case.locator('.analysis-case-list button')).to_have_count(20)
        p.get_by_role('button',name='配置评测',exact=True).click();p.get_by_role('button',name='添加评测项',exact=True).click();p.get_by_role('button',name='应用评测定义',exact=True).click();expect(p.get_by_label('标注 新评测项',exact=True)).to_be_visible();before=len([name for name,_ in f['calls'] if name=='case-page']);p.get_by_role('button',name='读取 Case',exact=True).click();expect(case.locator('.case-ui-notice')).to_contain_text('仍绑定原题目定义');assert len([name for name,_ in f['calls'] if name=='case-page'])==before;case.locator('.analysis-question-config').last.get_by_role('button',name='移除评测项',exact=True).click()
        p.get_by_label('评测题目 4',exact=True).fill('备注（新定义）');p.get_by_role('button',name='应用评测定义',exact=True).click();before=len([name for name,_ in f['calls'] if name=='case-page']);p.get_by_role('button',name='读取 Case',exact=True).click();expect(case.locator('.case-ui-notice')).to_contain_text('仍绑定原题目定义');assert len([name for name,_ in f['calls'] if name=='case-page'])==before
        p.get_by_role('button',name='清除标注状态筛选',exact=True).click();expect(case.locator('.case-ui-review-filter-active')).to_have_count(0);expect(case.locator('.analysis-case-list button')).to_have_count(20);assert 'review' not in latest(f,'case-page')
        (tmp_path/'filter-operations.json').write_text(json.dumps(dict(rows=24,seeded_saved_cases=23,original_cut=watermark,cursor_keeps_cut=True,fresh_cut_changes_match=True,group_variant=variant,whole_field_group_independent=True,choice_and_multi_options=True,added_and_changed_definition_require_explicit_filter=True,source_unchanged=True),indent=2)+'\n')


def test_group_codec_old_client_refuses_and_legacy_case_field_drafts_keep_meaning(browser,tmp_path,monkeypatch):
    import copy
    from demiflow.workbench.analysis import contracts as c
    with scene(browser,tmp_path,monkeypatch,reserved=True) as f:
        p=f['page'];case=p.locator('.analysis-case');p.get_by_role('button',name='字段与评测配置',exact=True).click();case.locator('.case-ui-group-config-item').first.get_by_role('button',name='移除输出方案',exact=True).click();case.locator('.case-ui-group-config').get_by_role('button',name='应用材料与方案配置',exact=True).click();expect(case.locator('.analysis-case-review')).to_contain_text('此方案已从当前配置移除');expect(p.get_by_label('标注 备注',exact=True)).to_have_count(0);expect(p.get_by_role('button',name='保存配置并登记方案',exact=True)).to_have_count(0);assert not any(name=='annotations-save' for name,_ in f['calls']);p.get_by_label('标注对象',exact=True).select_option('group:next');p.get_by_role('button',name='字段与评测配置',exact=True).click();registered(p);save(p,'方案原记录');before=len([name for name,_ in f['calls'] if name=='annotations-save'])
        p.evaluate('''()=>{const native=window.fetch;let altered=false;window.fetch=async(...args)=>{const response=await native(...args);if(!altered&&String(args[0]).endsWith('/analysis-case-group-bindings')){altered=true;const value=await response.json();value.groups[0].target.case_key='1';return new Response(JSON.stringify(value),{status:response.status,headers:response.headers});}return response;};}''')
        p.get_by_role('button',name='刷新当前标注',exact=True).click();expect(p.locator('.analysis-case-review')).to_contain_text('方案绑定暂不可用');expect(p.get_by_label('标注 备注',exact=True)).to_have_count(0);assert len([name for name,_ in f['calls'] if name=='annotations-save'])==before
        state=p.context.storage_state();cache=p.evaluate('(v)=>new ForgeAnalysisState.Store().caseDraft(v.project,v.source)',dict(project=f['project'],source=f['source']['digest']));assert cache['id'].startswith('case-groups-v3:')
        legacy=browser.new_context(storage_state=state);old=legacy.new_page();old.set_default_timeout(65000)
        try:
            old.goto(f['app'].origin+'/#token='+f['app'].token);old.get_by_role('button',name='数据',exact=True).click();expect(old.locator('.analysis-case')).to_contain_text('无效的记录标识');expect(old.get_by_label('标注 备注',exact=True)).to_have_count(0)
            assert old.evaluate('(v)=>new ForgeAnalysisState.Store().caseDraft(v.project,v.source)',dict(project=f['project'],source=f['source']['digest']))==cache
        finally:legacy.close()
        field_context=browser.new_context(storage_state=state);current=field_context.new_page();current.set_default_timeout(65000)
        for name in ['analysis-case.js','analysis-case.css','analysis-case-media.js']:current.route('**/'+name,f['asset'](OWN/'demiflow/workbench/web'/name))
        descriptor=copy.deepcopy(f['descriptor']);descriptor['version']=1;del descriptor['output_groups'];question=descriptor['questions'][-1]
        draft=dict(id='case-fields-v2:v'+'2'*32,view_id=None,revision=0,base=None,view_touched=False,current='0',offset=0,review_field='base_answer',descriptor=descriptor,answers=[dict(case_key='0',target_field=field,digest=c.digest(question),definition=question,value=value,expected_revision=0,dirty=True) for field,value in [(None,'旧整体草稿'),('base_answer','旧字段草稿'),('group:base','保留名字段草稿')]])
        try:
            current.goto(f['app'].origin+'/#token='+f['app'].token);current.get_by_role('button',name='管道',exact=True).click();expect(current.locator('.analysis-case')).to_have_count(0)
            current.evaluate('(v)=>new ForgeAnalysisState.Store().saveCaseDraft(v.project,v.source,v.draft)',dict(project=f['project'],source=f['source']['digest'],draft=draft));current.get_by_role('button',name='数据',exact=True).click()
            expect(current.get_by_label('标注对象',exact=True)).to_have_value('base_answer');expect(current.get_by_label('标注 备注',exact=True)).to_have_value('旧字段草稿');current.get_by_label('标注对象',exact=True).select_option('field:group:base');expect(current.get_by_label('标注 备注',exact=True)).to_have_value('保留名字段草稿');assert len([name for name,_ in f['calls'] if name=='annotations-save'])==before;save(current,'保留名字段保存');assert latest(f,'annotations-save')['target']['field']=='group:base' and latest(f,'annotations-save')['target']['variant'] is None;current.get_by_label('标注对象',exact=True).select_option('');expect(current.get_by_label('标注 备注',exact=True)).to_have_value('旧整体草稿')
            saved=current.evaluate('(v)=>new ForgeAnalysisState.Store().caseDraft(v.project,v.source)',dict(project=f['project'],source=f['source']['digest']));assert saved['id']==draft['id'];assert all(item.get('target_variant') is None for item in saved['answers'])
        finally:current.unroute_all(behavior='ignoreErrors');field_context.close()
        assert len([name for name,_ in f['calls'] if name=='annotations-save'])==before+1
        (tmp_path/'codec-operations.json').write_text(json.dumps(dict(removed_unregistered_group_requires_explicit_subject=True,tampered_binding_rejected=True,old_client_refuses_group_codec=True,legacy_case_field_drafts_preserved=True,reserved_physical_field='group:base',reserved_field_variant=None,source_unchanged=True),indent=2)+'\n')


def test_two_real_schemes_three_scopes_media_history_reload_and_responsive(browser,tmp_path,monkeypatch):
    with scene(browser,tmp_path,monkeypatch) as f:
        p=f['page'];case=p.locator('.analysis-case')
        expect(case.locator('.case-ui-output-group')).to_have_count(2)
        expect(case.locator('.case-ui-output-group[data-group-id=base] .analysis-case-field')).to_have_count(3)
        expect(case.locator('.case-ui-output-card[data-field=spare_answer]')).to_have_count(1)
        expect(p.get_by_role('button',name='保存配置并登记方案',exact=True)).to_be_visible();expect(p.get_by_label('标注 备注',exact=True)).to_have_count(0)
        assert not any(name=='annotations-save' for name,_ in f['calls'])
        p.get_by_role('button',name='保存配置并登记方案',exact=True).click();expect(p.get_by_label('标注 备注',exact=True)).to_be_visible()
        for field in ['reference_image','base_image','next_image']:
            host=case.locator('.analysis-case-field[data-field='+field+']');expect(host.get_by_role('button',name='固定媒体供评测',exact=True)).to_be_visible();host.get_by_role('button',name='固定媒体供评测',exact=True).click();expect(host.get_by_role('button',name='预览图片',exact=True)).to_be_visible();host.get_by_role('button',name='预览图片',exact=True).click()
        expect(case.locator('img.analysis-media')).to_have_count(3)
        p.wait_for_function('()=>Array.from(document.querySelectorAll(".analysis-case img.analysis-media")).every(image=>image.naturalWidth===384&&image.naturalHeight===288)')
        assert case.locator('img.analysis-media').evaluate_all('(items)=>items.every(item=>item.naturalWidth===384&&item.naturalHeight===288)')
        reference=case.locator('.analysis-case-field[data-field=reference_image]');details=reference.locator('.case-media-original');expect(reference.get_by_role('button',name='重新核对媒体',exact=True)).not_to_be_visible();details.locator('summary').click();expect(reference.get_by_role('button',name='重新核对媒体',exact=True)).to_be_visible();expect(details).to_contain_text('reference0.png');details.locator(':scope > summary').click();expect(case.locator('img.analysis-media')).to_have_count(3)
        save(p,'Base 方案记录');group=latest(f,'annotations-save');variant=group['target']['variant'];assert variant.startswith('cg') and group['target']['field'] is None
        assert {ref['field'] for ref in group['entries'][0]['media_basis']}=={'reference_image','base_image'}
        lookups_before=len([name for name,_ in f['calls'] if name=='case-media']);assert lookups_before>0
        p.get_by_label('标注对象',exact=True).select_option('');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('');save(p,'整个 Case 记录');assert latest(f,'annotations-save')['target']['variant'] is None and latest(f,'annotations-save')['target']['field'] is None
        p.get_by_label('标注对象',exact=True).select_option('base_answer');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('');save(p,'单字段记录');assert latest(f,'annotations-save')['target']['field']=='base_answer'
        p.get_by_label('标注对象',exact=True).select_option('group:base');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('Base 方案记录');assert len([name for name,_ in f['calls'] if name=='case-media'])==lookups_before
        p.reload(wait_until='domcontentloaded');p.get_by_role('button',name='数据',exact=True).click();expect(p.get_by_label('标注对象',exact=True)).to_have_value('group:base');expect(p.get_by_label('标注 备注',exact=True)).to_have_value('Base 方案记录')
        note=case.locator('.analysis-answer').filter(has=p.get_by_role('heading',name='备注',exact=True));note.get_by_text('标注历史与恢复',exact=True).click();note.get_by_role('button',name='读取历史',exact=True).click();expect(note.locator('.case-ui-history-entry')).to_contain_text('Base 方案记录');assert latest(f,'annotations')['target']['variant']==variant
        f['grant'](analysis_write=False);note.get_by_role('button',name='清除已保存答案',exact=True).click();expect(p.get_by_label('标注 备注',exact=True)).to_have_value('');assert 'media_basis' not in latest(f,'annotations-save')['entries'][0]
        note.get_by_text('标注历史与恢复',exact=True).click();note.get_by_role('button',name='读取历史',exact=True).click();note.get_by_role('button',name='恢复到 v1',exact=True).click();expect(p.get_by_label('标注 备注',exact=True)).to_have_value('Base 方案记录')
        receipt=[value['result'] for name,value in f['responses'] if name=='annotations-save' and 'result' in value][-1];assert receipt['target']['variant']==variant and receipt['heads'][0]['media_basis']==group['entries'][0]['media_basis'];f['grant'](analysis_write=True)
        for field in ['reference_image','base_image','next_image']:
            case.locator('.analysis-case-field[data-field='+field+']').get_by_role('button',name='预览图片',exact=True).click()
        expect(case.locator('img.analysis-media')).to_have_count(3);p.wait_for_function('()=>Array.from(document.querySelectorAll(\".analysis-case img.analysis-media\")).every(image=>image.naturalWidth===384&&image.naturalHeight===288)')
        for width in [1440,1024,390]:
            p.set_viewport_size(dict(width=width,height=1100));assert p.evaluate('document.documentElement.scrollWidth<=innerWidth+1');expect(p.get_by_role('button',name='应用筛选与分组',exact=True)).to_be_visible();expect(case.locator('.analysis-case-list button')).to_have_count(3);p.evaluate('()=>scrollTo(0,0)');p.evaluate('()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))');p.screenshot(path=str(tmp_path/('groups-'+str(width)+'.png')),full_page=True)
        cache=p.evaluate('(v)=>new ForgeAnalysisState.Store().caseDraft(v.project,v.digest)',dict(project=f['project'],digest=f['source']['digest']));assert cache['id'].startswith('case-groups-v3:');assert len({(item.get('target_variant'),item.get('target_field')) for item in cache['answers']})==3
        p.get_by_role('button',name='保存并下一条',exact=True).click();expect(p.get_by_role('heading',name='Case 1',exact=True)).to_be_visible();expect(p.get_by_label('标注 备注',exact=True)).to_have_value('')
        p.get_by_role('button',name='画廊',exact=True).click();p.get_by_label('显示输出方案或字段',exact=True).select_option('group:next');expect(case.locator('.case-ui-output-group')).to_have_count(3);expect(case.locator('.case-ui-output-group[data-case-key=\"2\"]')).to_contain_text('null');expect(case.locator('.case-media-field')).to_have_count(4);expect(case.get_by_role('button',name='固定媒体供评测',exact=True)).to_have_count(3)
        for host in case.locator('.case-media-field').all():
            capture=host.get_by_role('button',name='固定媒体供评测',exact=True)
            if capture.is_visible():capture.click()
            expect(host.get_by_role('button',name='预览图片',exact=True)).to_be_visible();host.get_by_role('button',name='预览图片',exact=True).click()
        expect(case.locator('img.analysis-media')).to_have_count(4)
        for picture in case.locator('img.analysis-media').all():
            picture.scroll_into_view_if_needed();p.wait_for_function('(image)=>image.naturalWidth===384&&image.naturalHeight===288',arg=picture.element_handle())
        expect(case.locator('.case-ui-reader-complete')).to_have_count(3);p.evaluate('()=>scrollTo(0,0)');p.evaluate('()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))');p.screenshot(path=str(tmp_path/'groups-gallery.png'),full_page=True)
        p.get_by_role('button',name='Notebook',exact=True).click();expect(case).to_be_hidden();assert p.evaluate('ForgeAnalysisMedia.stats().allocations===0')
        assert not any(name in {'session-start','cell-submit','execute'} for name,_ in f['calls'])
        (tmp_path/'operations.json').write_text(json.dumps(dict(source=f['source']['digest'],variant=variant,real_schemes=2,scopes=3,media_basis_fields=[ref['field'] for ref in group['entries'][0]['media_basis']],raw_reference_and_refresh_accessible=True,gallery_decoded_images=4,gallery_completed_markdown_readers=3,scope_switch_extra_media_lookups=0,media_allocations_on_leave=0,responsive_widths=[1440,1024,390],source_unchanged=True,backend=str(BACKEND),runtime='explicit doubles',B1=False),indent=2)+'\n')
