"""Real-browser workbench checks against native owned workspaces and runs."""
from _forge_project_fixtures import declare_existing_flow
from pathlib import Path
import json
import os
import tempfile
from contextlib import contextmanager
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from playwright.sync_api import expect

from demiflow.forge.runs import manager as runs

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.projects import registry as console
from test_forge_project_discovery import original,business_bytes
from test_forge_web_redesign import project_info
from test_forge_workspace import demo,isolated
from test_forge_public_api import service
from test_forge_markdown_reading import browser

EVIDENCE=Path(os.environ['FORGE_WORKBENCH_EVIDENCE']) if os.environ.get('FORGE_WORKBENCH_EVIDENCE') else Path(
    tempfile.mkdtemp(prefix='demiflow-workbench-evidence-', dir='/tmp'))


@pytest.fixture(autouse=True)
def private_domain(tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'runtime-domain'))


@contextmanager
def page_for(browser,app):
    context=browser.new_context(viewport={'width':1440,'height':1050})
    page=context.new_page();page.set_default_timeout(65000)
    expect.set_options(timeout=65000)
    errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
    responses=[]
    page.on('response',lambda response:responses.append(dict(url=response.url.split('#')[0],status=response.status)) if len(responses)<512 and '/api/' in response.url else None)
    try:
        page.goto(app.origin+'/#token='+app.token)
        page.locator('.function-nav').get_by_role('button',name='管线',exact=True).wait_for()
        yield page,errors
    except BaseException:
        screenshot(page,'failure-current-browser')
        (EVIDENCE/'browser-failure-responses.json').write_text(json.dumps(dict(responses=responses,errors=errors),ensure_ascii=False,indent=2))
        raise
    finally:context.close()


def pipeline(page):
    page.locator('.function-nav').get_by_role('button',name='管线',exact=True).click()
    page.locator('.wb-node').first.wait_for()


def screenshot(page,name):
    path=EVIDENCE/'screenshots'/(name+'.png');path.parent.mkdir(parents=True,exist_ok=True)
    page.screenshot(path=str(path),full_page=True)


def source_response_for(node_id):
    def matches(response):
        url=urlsplit(response.url)
        return url.path.endswith('/graph-source') and parse_qs(url.query).get('node_id')==[node_id]
    return matches


def assert_selected_source(page,response,node_id,*,basis_kind,basis_digest=None):
    """Check public author identity, independent of the renderer's display key."""
    assert response.status==200,response.text()
    query=parse_qs(urlsplit(response.url).query)
    binding=json.loads(query['graph_binding'][0])
    value=response.json()
    assert query['node_id']==[node_id] and value['node_id']==node_id
    assert value['graph_binding']==binding and binding['basis_kind']==basis_kind
    if basis_digest is not None:
        assert binding['basis_digest']==basis_digest
    assert value['read_only'] is True and value['availability']=='available'
    expect(page.locator('.wb-node.selected')).to_have_count(1)
    expect(page.locator('.wb-source-body')).to_contain_text(value['path']+':'+str(value['start_line']))
    return value


def test_four_tabs_real_graph_parameters_code_and_layout(isolated,browser):
    with service(isolated,enable_config_write=True,workspace_config_write=True) as app,page_for(browser,app) as (page,errors):
        assert page.locator('.function-nav button').all_text_contents()==['开发','管道','数据','Readme']
        pipeline(page)
        expect(page.locator('.wb-basis')).to_contain_text('源码观察图')
        page.get_by_role('button',name='检查',exact=True).click()
        expect(page.locator('.wb-basis')).to_contain_text('检查报告图 · checked')
        assert page.locator('.wb-node').count()==7
        page.get_by_role('button',name='读图与配置',exact=True).click()
        page.locator('.wb-node').filter(has_text='normalize_question').click()
        expect(page.locator('.wb-source-body')).to_contain_text('def normalize_question')
        expect(page.locator('.wb-source-body').get_by_role('button',name='复制代码',exact=True)).to_be_visible()
        expect(page.locator('.wb-source-header').last).to_contain_text('normalize_question')
        page.locator('.wb-node').filter(has_text='above_threshold').click()
        page.get_by_label('配置 /arguments/minimum',exact=True).fill('0.7')
        expect(page.get_by_role('button',name='运行',exact=True)).to_be_disabled()
        page.get_by_role('button',name='检查问题',exact=True).click()
        expect(page.locator('.wb-check-issues')).to_contain_text('未保存修改')
        expect(page.get_by_role('button',name='重新检查当前配置',exact=True)).to_be_disabled()
        page.get_by_role('button',name='返回配置与图',exact=True).click()
        page.locator('.wb-node').filter(has_text='above_threshold').click()
        expect(page.get_by_label('配置 /arguments/minimum',exact=True)).to_have_value('0.7')
        page.get_by_role('button',name='保存配置',exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('配置已保存')
        assert json.loads((isolated/'flows/select_a.json').read_text())['arguments']['minimum']==0.7
        page.get_by_role('button',name='检查',exact=True).click()
        expect(page.locator('.wb-basis')).to_contain_text('检查报告图 · checked')
        expect(page.locator('.wb-checks')).to_be_visible()
        expect(page.locator('.wb-check-issues')).to_contain_text('当前配置的接线检查通过')
        expect(page.get_by_role('button',name='重新检查当前配置',exact=True)).to_be_enabled()
        # UI-D1.9 B05 deliberately retains the same graph and editable inspector
        # beside check issues, so a diagnostic can locate its original node.
        expect(page.locator('.wb-body')).to_be_visible()
        for width in (1920,1440,1200,1024,390,320):
            page.set_viewport_size({'width':width,'height':1050})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1'),width
            screenshot(page,'pipeline-layout-'+str(width))
        page.set_viewport_size({'width':1440,'height':1050})
        page.get_by_role('button',name='整个项目',exact=True).click()
        expect(page.locator('.wb-check-issues').get_by_role('heading',name='检查结果',exact=True)).to_be_visible()
        page.get_by_role('button',name='当前流程',exact=True).click()
        expect(page.locator('.wb-check-issues')).to_contain_text('当前配置的接线检查通过')
        for label in ('数据','Readme','开发'):
            page.locator('.function-nav').get_by_role('button',name=label,exact=True).click()
            expect(page.locator('.function-nav').get_by_role('button',name=label,exact=True)).to_have_attribute('aria-current','page')
        assert not errors,errors


def test_graph_inspection_busy_preserves_report_until_explicit_retry(isolated,browser,tmp_path):
    import hashlib
    with service(isolated) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.evaluate('() => Object.defineProperty(document,"hidden",{configurable:true,get:()=>true})')
        refresh=page.get_by_role('button',name='刷新当前视图',exact=True);refresh.click()
        expect(refresh).not_to_have_attribute('data-pending','true',timeout=65000)
        with page.expect_response('**/inspect') as initial:
            page.get_by_role('button',name='检查',exact=True).click()
        assert initial.value.status==200 and initial.value.json()['status']=='checked'
        expect(page.locator('.wb-basis')).to_contain_text('检查报告图 · checked')
        basis=page.locator('.wb-basis').inner_text();url=page.url
        def reports():
            files=sorted((isolated/'.forge/inspections').glob('*.json'))
            assert len(files)<=32 and sum(f.stat().st_size for f in files)<=2*1024**2
            return {f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in files}
        before=reports();writes=[]
        page.on('request',lambda r:writes.append(r.url) if r.method=='POST' and r.url.endswith('/inspect') else None)
        assert app.workers.acquire(timeout=65)
        try:
            with page.expect_response('**/inspect') as rejected:
                page.get_by_role('button',name='检查',exact=True).click()
            reply=rejected.value.json()
            assert rejected.value.status==503 and reply['code']=='busy' and reply['operation_started'] is False
            expect(page.locator('.wb-notice')).to_contain_text('检查未受理，尚未开始')
            expect(page.locator('.wb-notice')).not_to_contain_text('写入结果可能尚未确认')
            assert reports()==before and len(writes)==1
            assert page.url==url and page.locator('.wb-basis').inner_text()==basis
        finally:app.workers.release()
        with page.expect_response('**/inspect') as retried:
            page.get_by_role('button',name='检查',exact=True).click()
        assert retried.value.status==200 and retried.value.json()['status']=='checked'
        expect(page.locator('.wb-basis')).to_contain_text('检查报告图 · checked')
        assert len(writes)==2 and not errors,errors
        out=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path)));out.mkdir(parents=True,exist_ok=True)
        (out/'graph-inspection-admission.json').write_text(json.dumps(dict(rejection_http=503,
            operation_started=False,report_unchanged_on_refusal=True,basis_and_url_unchanged=True,
            writes_after_refusal=1,writes_after_explicit_retry=2,retry_http=200,retry_status='checked'),ensure_ascii=False,indent=2)+'\n')


def test_lost_config_save_response_adopts_content_without_claiming_attribution(isolated,browser):
    writes=[]
    with service(isolated,enable_config_write=True,workspace_config_write=True) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_role('button',name='整图配置',exact=True).click()
        page.get_by_label('配置 /arguments/minimum',exact=True).fill('0.7')
        def lost(route):
            writes.append(route.request.post_data_json)
            response=route.fetch(timeout=65000)
            assert response.ok,response.text()
            route.abort('failed')
        page.route('**/parameters-update',lost)
        page.get_by_role('button',name='保存配置',exact=True).click()
        expect(page.locator('.wb-file-pending')).to_contain_text('配置保存待核对')
        assert json.loads((isolated/'flows/select_a.json').read_text())['arguments']['minimum']==0.7
        page.get_by_role('button',name='读回当前配置',exact=True).click()
        expect(page.locator('.wb-file-pending')).to_contain_text('写入归属未确认')
        expect(page.locator('.wb-file-pending')).to_contain_text('不能由读回证明持久性')
        page.get_by_role('button',name='采用当前内容',exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('已采用当前内容')
        assert len(writes)==1 and 'request_id' not in writes[0]
        expect(page.locator('.wb-file-pending')).to_have_count(0,timeout=65000)
        page.reload();pipeline(page)
        expect(page.locator('.wb-file-pending')).to_have_count(0)
        assert len(writes)==1 and not errors,errors


def test_working_draft_cannot_replace_sealed_run_configuration(isolated,browser):
    prepared=runs.prepare(isolated,'flows/questions.py:select',params='flows/select_a.json')
    original=prepared['report']['binding']['parameters']['arguments']['minimum']
    with service(isolated,enable_config_write=True,workspace_config_write=True) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_role('button',name='整图配置',exact=True).click()
        page.get_by_label('配置 /arguments/minimum',exact=True).fill('0.123')
        page.get_by_role('button',name='运行记录',exact=True).click()
        page.locator('.wb-run-record').filter(has_text=prepared['run_id'][-10:]).click()
        # Preparation seals the report and parameters, but has not executed an
        # action. Its report must not be presented as an execution DAG.
        expect(page.get_by_role('button',name='当时检查图',exact=True)).to_be_visible()
        expect(page.locator('.wb-node')).to_have_count(0)
        expect(page.locator('.wb-graph-notices')).to_contain_text('可切换“当时检查图”')
        page.get_by_role('button',name='当时检查图',exact=True).click()
        page.locator('.wb-node').filter(has_text='above_threshold').click()
        expect(page.get_by_label('配置 /arguments/minimum',exact=True)).to_have_value(str(original))
        expect(page.get_by_label('配置 /arguments/minimum',exact=True)).to_be_disabled()
        page.get_by_role('button',name='返回当前配置',exact=True).click()
        page.get_by_role('button',name='整图配置',exact=True).click()
        expect(page.get_by_label('配置 /arguments/minimum',exact=True)).to_have_value('0.123')
        assert json.loads((isolated/'flows/select_a.json').read_text())['arguments']['minimum']==original
        assert not errors,errors


def test_large_source_graph_has_bounded_index_and_local_navigation(isolated,browser):
    from demiflow.workbench.graph import model
    (isolated/'flows/unexpanded.py').write_text('def run():\n'+''.join('    unknown()\n' for _ in range(260)))
    (isolated/'flows/unexpanded.json').write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1',arguments={})))
    declare_existing_flow(isolated,'flows/unexpanded.py:run',params='flows/unexpanded.json')
    with service(isolated) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_label('流程',exact=True).select_option('flows/unexpanded.py:run')
        expect(page.locator('.wb-node')).to_have_count(128)
        page.get_by_text('定位节点 / 查看局部',exact=True).click()
        for offset in (64,128,192):
            expected=model.get(isolated,basis_kind='source',entry='flows/unexpanded.py:run',params='flows/unexpanded.json',node_offset=offset)
            page.get_by_role('button',name='下一组节点',exact=True).click()
            expect(page.get_by_label('定位图节点',exact=True).locator('option').first).to_have_attribute('value',expected['node_index']['nodes'][0]['id'])
        target_node=expected['node_index']['nodes'][30];target=target_node['id']
        with page.expect_response(source_response_for(target)) as source:
            page.get_by_label('定位图节点',exact=True).select_option(target)
        expect(page.locator('.wb-node')).to_have_count(1)
        value=assert_selected_source(page,source.value,target,basis_kind='source',basis_digest=expected['binding']['basis_digest'])
        assert (value['path'],value['start_line'])==(target_node['source'],target_node['line'])
        expect(page.locator('.wb-source-body')).to_contain_text('unknown()')
        expect(page.locator('.wb-basis')).to_contain_text('尚未检查')
        page.get_by_role('button',name='返回全图范围',exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(128)
        assert not errors,errors




def test_hidden_report_node_deep_link_and_large_offline_export_are_bounded(isolated,tmp_path,browser):
    from demiflow.workbench.api import export_report
    (isolated/'flows/many.py').write_text("from demiflow import data\ndef run(*,source,target):\n    d=data.read_lance(source['uri'],version=source['version'])\n"+'    d=d.limit(1)\n'*130+"    return d.write_lance(target['uri'],mode='create',return_receipt=True)\n")
    (isolated/'flows/many.json').write_text(json.dumps(dict(format='demiflow_forge_flow_parameters_v1',arguments={
        'source':{'kind':'table','alias':'candidates'},'target':{'kind':'output','location_id':'local','relative_uri':'many.lance'}})))
    declare_existing_flow(isolated,'flows/many.py:run',params='flows/many.json')
    report=project_ops.inspect_flow(isolated,'flows/many.py:run',params='flows/many.json')
    assert report['status']=='checked',report['diagnostics']
    assert len(report['nodes'])>128
    hidden=report['nodes'][-1]['id'];key=ws.workspace_id(isolated,ws.load_workspace(isolated)[1])
    with service(isolated) as app,page_for(browser,app) as (page,errors):
        target=dict(v=2,workspace=key,kind='inspection',report_digest=report['digest'],node_id=hidden)
        # Exercise opening a deep link, rather than same-document hash navigation.
        page.goto('about:blank')
        with page.expect_response(source_response_for(hidden)) as source:
            page.goto(app.origin+'/#'+urlencode(dict(view=json.dumps(target))))
        value=assert_selected_source(page,source.value,hidden,basis_kind='report',basis_digest=report['digest'])
        assert (value['path'],value['start_line'])==(report['nodes'][-1]['source'],report['nodes'][-1]['line'])
        assert page.locator('.wb-node').count()<=256
        assert page.evaluate('ForgeView.decodeView(location.hash)')==target
        with page.expect_response(source_response_for(hidden)) as reloaded_source:
            page.reload()
        assert_selected_source(page,reloaded_source.value,hidden,basis_kind='report',basis_digest=report['digest'])
        assert page.evaluate('ForgeView.decodeView(location.hash)')==target
        assert page.locator('.wb-node').count()<=256
        output=tmp_path/'bounded-report.html';export_report(isolated,report['digest'],output)
        page.goto(output.as_uri())
        page.get_by_role('heading',name='接线检查报告',exact=True).wait_for()
        assert page.locator('.wb-node').count()==128
        expect(page.locator('main')).to_contain_text('显示已裁剪')
        assert not errors,errors
