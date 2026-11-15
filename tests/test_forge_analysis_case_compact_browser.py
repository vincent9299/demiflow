"""Same-project I13 before/after in the actual authenticated project shell.

Case JS/CSS/media and the narrow optional renderer-label proposal are static
browser overlays only. All other code is the coordinator's immutable snapshot.
Actual HTTP/guarded SQLite/Lance/Outbox/pending/PNG child/ready/range are reused;
Runtime/watchdog/deferred scheduling remain the I13 explicit fixture doubles.
"""
import hashlib
import json
import os
from pathlib import Path
import threading
import uuid
import pytest

from playwright.sync_api import expect
from demiflow.workbench.projects import registry as console
from demiflow.forge.platform import content_digest
from demiflow.workbench.analysis import web_service
from demiflow.workbench.analysis.output_transport import OutputRange
from demiflow.forge.schema import ForgeError
from test_forge_analysis_media_source_export import declared, fixed
from test_forge_analysis_media_shell_browser import private_helper, wire
from test_forge_markdown_reading import browser
from test_forge_public_api import service

OWN=Path(__file__).resolve().parents[1]/'demiflow/workbench/web'
_base=os.environ.get('FORGE_CASE_UI_BASE_ROOT')
_proposed=os.environ.get('FORGE_CASE_UI_RENDERER_PROPOSAL')
# This historical before/after comparison is opt-in. Ordinary merged-source
# collection must not require another chat's frozen snapshot or static overlay;
# test_forge_analysis_media_shell_browser covers the actual current shell.
pytestmark=pytest.mark.skipif(not (_base and _proposed),reason='Paired historical UI comparison needs explicit frozen baseline and renderer proposal inputs')
BASE=Path(_base) if _base else None
PROPOSED=Path(_proposed) if _proposed else None


def test_same_project_shell_compact_context_and_real_media_answer_cas_history(browser,tmp_path,monkeypatch):
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(BASE)
    root,raw=declared(tmp_path);locator,source=fixed(root)
    registry=tmp_path/'registry.json';project=console.add(registry,root)['workspace_id']
    console.migrate(registry,expected=content_digest(console.load_registry(registry)))
    outbox=tmp_path/'outbox';outbox.mkdir();outbox.chmod(0o2770)
    config=tmp_path/'fixture.json';config.write_text(json.dumps(dict(workspace=str(root),outbox=str(outbox),state=str(tmp_path/'state'),project=project,locator=locator,source=source)))
    calls=[];lock=threading.Lock();observations={};errors=[]
    with private_helper(config) as invoke:
        class Helper(web_service.WebService):
            principal='fixture-reader';features=dict(case_media=True,annotation_media_basis=True)
            def __init__(self,server,config):self.server=server
            def ceiling(self):return dict(analysis_write=True,case_annotate=True,analysis_execute=False)
            def execution_profile(self):return None # Metadata-only double has no admitted runtime.
            def assert_ready(self):return True
            def close(self):pass
            def cancel_output(self,key):pass
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
        with service(None,registry=registry,analysis_config='I13-actual-media-chain-explicit-Runtime-double') as app:
            registry_value=console.load_registry(registry)
            console.set_capabilities(registry,project,expected=content_digest(registry_value),capabilities=dict(console.capabilities(registry_value['projects'][project]),analysis_write=True,case_annotate=True),analysis_service=app.analysis)
            storage=None;saved=None;ref=None
            for edition in ['before','after']:
                context=browser.new_context(viewport=dict(width=1440,height=1100),**(dict(storage_state=storage) if storage else {}))
                page=context.new_page();page.set_default_timeout(65000);page.on('pageerror',lambda error:errors.append(str(error)))
                if edition=='after':
                    def serve_asset(path):
                        def handle(route):return route.fulfill(content_type='text/css' if path.suffix=='.css' else 'text/javascript',body=path.read_text())
                        return handle
                    for name in ['analysis-case.js','analysis-case.css','analysis-case-media.js']:
                        page.route('**/'+name,serve_asset(OWN/name))
                    page.route('**/analysis-render.js',lambda route:route.fulfill(content_type='text/javascript',body=PROPOSED.read_text()))
                try:
                    page.goto(app.origin+'/#token='+app.token)
                    page.get_by_role('button',name='数据',exact=True).click()
                    case=page.locator('.analysis-case');expect(case.get_by_role('heading',name='Case 0',exact=True)).to_be_visible(timeout=65000)
                    if edition=='before':
                        page.get_by_role('button',name='字段与评测配置',exact=True).click()
                        page.get_by_label('预览格式 1',exact=True).select_option('image')
                        page.get_by_role('button',name='字段与评测配置',exact=True).click()
                        expect(page.get_by_role('button',name='固定媒体供评测',exact=True)).to_be_visible(timeout=65000)
                        assert not any(name=='media-capture' for name,_ in calls)
                        page.get_by_role('button',name='固定媒体供评测',exact=True).click()
                    expect(page.get_by_role('button',name='预览图片',exact=True)).to_be_visible(timeout=65000)
                    page.get_by_role('button',name='预览图片',exact=True).click()
                    expect(page.locator('img.analysis-media')).to_be_visible(timeout=65000)
                    page.wait_for_function('document.querySelector("img.analysis-media").naturalWidth===8&&document.querySelector("img.analysis-media").naturalHeight===6')
                    if edition=='before':
                        page.get_by_label('标注 结果是否符合预期',exact=True).select_option('yes')
                        page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
                        expect(case.locator('.analysis-case-review')).to_contain_text('当前答案 v1',timeout=65000)
                        saved=[payload for name,payload in calls if name=='annotations-save'][-1];ref=saved['entries'][0]['media_basis'][0]
                        # Freeze both screenshots at the same post-save query
                        # watermark, instead of comparing old selection 0 with
                        # the remounted page's fresh annotation watermark 1.
                        page.get_by_role('button',name='读取 Case',exact=True).click()
                        expect(page.get_by_role('button',name='预览图片',exact=True)).to_be_visible(timeout=65000)
                        page.get_by_role('button',name='预览图片',exact=True).click()
                        expect(page.locator('img.analysis-media')).to_be_visible(timeout=65000)
                        page.wait_for_function('document.querySelector("img.analysis-media").naturalWidth===8')
                    else:
                        expect(page.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('yes')
                        expect(case.locator('.analysis-case-review')).to_contain_text('当前答案 v1',timeout=65000)
                    # Same immutable source/Case/view draft/answer/basis and same
                    # final no-op action before both editions' screenshots.
                    page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
                    expect(case).to_contain_text('当前 Case 没有新的标注修改')
                    media=case.locator('.case-media-field')
                    descriptor_summary=media.get_by_text('原值' if edition=='before' else '媒体描述信息',exact=True)
                    descriptor_summary.click();description=json.loads(descriptor_summary.locator('..').locator('pre').inner_text())
                    assert description['artifact_id']==ref['artifact_id'] and description['sha256']==ref['expected_sha256']
                    assert description['bytes']==len(raw) and description['width']==8 and description['height']==6
                    descriptor_summary.click()
                    business=media.locator('.case-media-original');business.locator('summary').click()
                    assert business.locator('pre').inner_text()=='picture.png'
                    assert description.get('image_path') is None
                    business.locator('summary').click()
                    collapse=page.get_by_role('button',name='收起数据目录',exact=True)
                    if collapse.count():collapse.click()
                    observations[edition]=dict(project=project,source=source['digest'],reference=ref,descriptor=description,original_field='picture.png',sizes={},static_inputs={str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in ([BASE/'demiflow/workbench/web'/name for name in ['analysis-case.js','analysis-case.css','analysis-case-media.js','analysis-render.js']] if edition=='before' else [*[OWN/name for name in ['analysis-case.js','analysis-case.css','analysis-case-media.js']],PROPOSED])})
                    for label,width,height in [('desktop',1440,1100),('narrow',420,900)]:
                        page.set_viewport_size(dict(width=width,height=height))
                        if label=='narrow':
                            page.get_by_text('标注历史与恢复',exact=True).click()
                            page.get_by_role('button',name='读取历史',exact=True).click()
                            expect(case.locator('.case-ui-history-entry')).to_contain_text(ref['expected_sha256'],timeout=65000)
                        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
                        observations[edition]['sizes'][label]=page.evaluate('''()=>{const y=selector=>document.querySelector(selector).getBoundingClientRect().y+scrollY;return {grid:y('.analysis-case-grid'),material:y('.analysis-case-field'),question:y('.analysis-answer'),scope:document.querySelector('.analysis-case-rail').getBoundingClientRect().height,status:document.querySelector('.analysis-case-status').textContent,coverage:document.querySelector('.analysis-case-coverage').textContent};}''')
                        (tmp_path/'partial-before-after.json').write_text(json.dumps(observations,ensure_ascii=False,indent=2)+'\n')
                        page.evaluate('scrollTo({top:0,behavior:"instant"})')
                        page.screenshot(path=str(tmp_path/(edition+'-'+label+'.png')),full_page=True)
                    if edition=='before':
                        storage=context.storage_state();continue
                    assert observations['after']['sizes']['desktop']['coverage']==observations['before']['sizes']['desktop']['coverage']
                    expect(page.get_by_role('button',name='Case 0',exact=True)).to_be_visible()
                    expect(page.get_by_role('button',name='应用筛选与分组',exact=True)).to_be_visible()
                    page.get_by_role('button',name='字段与评测配置',exact=True).click()
                    expect(page.get_by_label('预览格式 1',exact=True)).to_have_value('image')
                    page.get_by_role('button',name='字段与评测配置',exact=True).click()
                    expect(case.locator('.analysis-case-settings')).to_be_hidden()
                    page.get_by_text('历史与草稿',exact=True).click()
                    expect(page.get_by_role('button',name='导出 Case 草稿',exact=True)).to_be_visible()
                    page.get_by_text('历史与草稿',exact=True).click()
                    page.get_by_text('字段分布与覆盖范围',exact=True).click()
                    expect(page.get_by_role('button',name='读取字段统计',exact=True)).to_be_visible()
                    page.get_by_text('字段分布与覆盖范围',exact=True).click()
                    # A real concurrent same-principal answer changes the CAS
                    # head. The local answer and its media basis remain dirty.
                    page.get_by_label('标注 结果是否符合预期',exact=True).select_option('no')
                    concurrent=json.loads(json.dumps(saved));concurrent['request_id']='c'+uuid.uuid4().hex;concurrent['entries'][0]['expected_revision']=1
                    status,_,body=wire(app,'/api/workspaces/'+project+'/analysis-annotations-save',body=concurrent,token=True)
                    assert status==200,body
                    page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
                    expect(case.locator('.analysis-answer')).to_contain_text('保存冲突',timeout=65000)
                    expect(page.get_by_label('标注 结果是否符合预期',exact=True)).to_have_value('no')
                    expect(case.locator('.analysis-case-status')).to_contain_text('1 份答案草稿')
                    expect(case.locator('.analysis-case-status')).to_contain_text(source['digest'][:12])
                    page.screenshot(path=str(tmp_path/'after-narrow-conflict.png'),full_page=True)
                    page.get_by_role('button',name='保留草稿并采用服务器版本',exact=True).click()
                    page.get_by_role('button',name='保存本 Case 标注',exact=True).click()
                    expect(case.locator('.analysis-case-review')).to_contain_text('当前答案 v3',timeout=65000)
                    last=[payload for name,payload in calls if name=='annotations-save'][-1]
                    assert last['entries'][0]['value']=='no' and last['entries'][0]['media_basis']==[ref]
                    page.get_by_role('button',name='Notebook',exact=True).click()
                    expect(page.locator('img.analysis-media')).to_have_count(0)
                    assert page.evaluate('ForgeAnalysisMedia.stats().allocations===0&&ForgeAnalysisMedia.stats().encodedBytes===0')
                except BaseException:
                    page.screenshot(path=str(tmp_path/(edition+'-failure.png')),full_page=True);raise
                finally:
                    context.close()
            assert len([name for name,_ in calls if name=='media-capture'])==1
            assert len([name for name,_ in calls if name=='annotations-save'])==4
            assert not errors,errors
            observations.update(capture_requests=1,annotation_attempts=4,concurrent_cas=True,dirty_basis_preserved=True,descriptor_business_value_distinct=True,leave_released=True,fixture_scope='Actual HTTP/SQLite/Lance/Outbox/pending/PNG child/ready/range; I13 Runtime/watchdog/Task scheduling doubles; static UI/proposed renderer overlays only')
            (tmp_path/'before-after.json').write_text(json.dumps(observations,ensure_ascii=False,indent=2)+'\n')
