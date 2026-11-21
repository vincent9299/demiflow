"""An in-flight context read cannot replace edited onboarding/permission fields."""
from _forge_project_fixtures import declare_existing_flow, grant_config_write
import json

from playwright.sync_api import expect

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from test_forge_runtime_core_flow import unregistered,root,base,browser
from test_forge_public_api import service
from test_forge_workbench_browser import page_for,pipeline


def registered_project(tmp_path,root):
    declare_existing_flow(root,'flows/questions.py:select',params='flows/select_a.json')
    registry=tmp_path/'registry.json'
    key=console.add(registry,root,allow_runs=True)['workspace_id']
    grant_config_write(registry, key)
    return registry,key


def edit_while_context_is_held(page):
    held=[]
    page.route('**/context',lambda route:held.append(route))
    with page.expect_request('**/context'):
        page.get_by_role('button',name='刷新当前视图',exact=True).click()
    form=page.locator('.wb-settings')
    form.get_by_label('登记流程入口',exact=True).fill('flows/waiting.py:run')
    form.get_by_label('登记参数文件',exact=True).fill('flows/waiting.json')
    form.get_by_label('保留结果为输入',exact=True).uncheck()
    form.get_by_label('保存图上配置',exact=True).check()
    return held,form


def finish_context(page,held):
    assert len(held)==1
    held[0].continue_()
    page.unroute('**/context')
    expect(page.get_by_role('button',name='刷新当前视图',exact=True)).to_be_enabled(timeout=65000)


def assert_drafts(form):
    expect(form.get_by_label('登记流程入口',exact=True)).to_have_value('flows/waiting.py:run')
    expect(form.get_by_label('登记参数文件',exact=True)).to_have_value('flows/waiting.json')
    expect(form.get_by_label('保留结果为输入',exact=True)).not_to_be_checked()
    expect(form.get_by_label('保存图上配置',exact=True)).to_be_checked()


def test_delayed_context_preserves_drafts_and_registers_the_edited_target(unregistered,tmp_path,browser,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    registry,key=registered_project(tmp_path,unregistered)
    with service(None,registry=registry,enable_runs=True,enable_config_write=True) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_role('button',name='接入与权限',exact=True).click()
        held,form=edit_while_context_is_held(page)
        finish_context(page,held)
        assert_drafts(form)
        with page.expect_request('**/flow-register') as registration:
            form.get_by_role('button',name='登记原生流程',exact=True).click()
        request=registration.value
        assert '/workspaces/'+key+'/' in request.url
        assert request.post_data_json==dict(entry='flows/waiting.py:run',params='flows/waiting.json')
        expect(form).not_to_be_visible(timeout=65000)
        control=ws.load_workspace(unregistered)[1]
        assert control['entries']['flows/waiting.py:run']==['flows/waiting.json']
        # Registration must not implicitly save either permission draft.
        item=console.load_registry(registry)['projects'][key]
        assert console.permissions(item)['attach'] is True
        assert console.capabilities(item)['config_write'] is False
        assert not (unregistered/'.forge/runs').exists()
        assert not errors,errors


def test_context_revocation_keeps_fields_but_invalidates_permission_cas(unregistered,tmp_path,browser,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    registry,key=registered_project(tmp_path,unregistered)
    with service(None,registry=registry,enable_runs=True,enable_config_write=True) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_role('button',name='接入与权限',exact=True).click()
        held,form=edit_while_context_is_held(page)
        old=console.load_registry(registry)
        console.set_permissions(registry,key,expected=content_digest(old),run_submit=False,run_control=False,attach=False)
        console.set_capabilities(registry,key,expected=content_digest(console.load_registry(registry)),capabilities={k:False for k in console.CAPABILITIES})
        finish_context(page,held)
        assert_drafts(form)
        expect(form.locator('.wb-settings-stale')).to_be_visible()
        writes=[]
        page.on('request',lambda r:writes.append(r.url) if r.method=='POST' else None)
        for label in ('登记原生流程','保存运行权限','保存配置写入权限'):
            button=form.get_by_role('button',name=label,exact=True)
            expect(button).to_be_disabled()
            button.evaluate('(node)=>node.click()')
        assert writes==[]
        assert 'flows/waiting.py:run' not in ws.load_workspace(unregistered)[1]['entries']
        item=console.load_registry(registry)['projects'][key]
        assert not any(console.permissions(item).values())
        assert console.capabilities(item)['config_write'] is False
        assert not errors,errors


def test_registration_waits_for_initial_graph_and_keeps_edited_target(unregistered,tmp_path,browser,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    registry,key=registered_project(tmp_path,unregistered)
    with service(None,registry=registry,enable_runs=True,enable_config_write=True) as app,page_for(browser,app) as (page,errors):
        page.evaluate('''() => {
          const native=fetch.bind(window);window.__heldGraph=false;
          window.fetch=async(...args)=>{
            const response=await native(...args),url=new URL(String(args[0]),location.href);
            if(url.pathname.endsWith('/graph')&&!window.__heldGraph){
              window.__heldGraph=true;return new Promise(resolve=>{window.__releaseGraph=()=>resolve(response);});
            }return response;
          };
        }''')
        page.get_by_role('button',name='管道',exact=True).click()
        page.wait_for_function('() => typeof window.__releaseGraph==="function"')
        page.get_by_role('button',name='接入与权限',exact=True).click()
        form=page.locator('.wb-settings')
        form.get_by_label('登记流程入口',exact=True).fill('flows/waiting.py:run')
        form.get_by_label('登记参数文件',exact=True).fill('flows/waiting.json')
        button=form.get_by_role('button',name='登记原生流程',exact=True)
        writes=[];page.on('request',lambda r:writes.append(r.url) if r.method=='POST' else None)
        expect(button).to_be_disabled();button.evaluate('(node)=>node.click()')
        assert writes==[]
        page.evaluate('() => window.__releaseGraph()')
        expect(button).to_be_enabled(timeout=65000)
        with page.expect_response('**/flow-register') as registered:button.click()
        assert registered.value.ok,registered.value.text()
        expect(form).not_to_be_visible(timeout=65000)
        expect(page.get_by_label('流程',exact=True)).to_have_value('flows/waiting.py:run',timeout=65000)
        assert ws.load_workspace(unregistered)[1]['entries']['flows/waiting.py:run']==['flows/waiting.json']
        assert not errors,errors


def test_busy_registration_is_unapplied_and_retries_only_on_explicit_click(unregistered,tmp_path,browser,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    registry,key=registered_project(tmp_path,unregistered)
    with service(None,registry=registry,enable_runs=True,enable_config_write=True) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_role('button',name='接入与权限',exact=True).click()
        form=page.locator('.wb-settings')
        form.get_by_label('登记流程入口',exact=True).fill('flows/waiting.py:run')
        form.get_by_label('登记参数文件',exact=True).fill('flows/waiting.json')
        submissions=[];page.on('request',lambda r:submissions.append(r.post_data_json) if r.method=='POST' and r.url.endswith('/flow-register') else None)
        assert app.workers.acquire(timeout=30)
        try:
            with page.expect_response('**/flow-register') as busy:
                form.get_by_role('button',name='登记原生流程',exact=True).click()
            assert busy.value.status==503 and busy.value.json()['code']=='busy'
            expect(page.locator('.wb-notice')).to_contain_text('登记尚未开始')
            expect(form.get_by_label('登记流程入口',exact=True)).to_have_value('flows/waiting.py:run')
            expect(form.get_by_label('登记参数文件',exact=True)).to_have_value('flows/waiting.json')
            assert len(submissions)==1
            assert 'flows/waiting.py:run' not in ws.load_workspace(unregistered)[1]['entries']
        finally:app.workers.release()
        with page.expect_response('**/flow-register') as registered:
            form.get_by_role('button',name='登记原生流程',exact=True).click()
        assert registered.value.ok,registered.value.text()
        expect(form).not_to_be_visible(timeout=65000)
        assert len(submissions)==2 and submissions[0]==submissions[1]
        assert ws.load_workspace(unregistered)[1]['entries']['flows/waiting.py:run']==['flows/waiting.json']
        assert not errors,errors
