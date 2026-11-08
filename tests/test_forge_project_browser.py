"""Real browser acceptance for empty console, create/register and inspection."""
import json
import os
from pathlib import Path

import pytest

from demiflow.workbench.projects import registry as console
from test_forge_public_api import service
from test_forge_web_redesign import project_info
from test_forge_project_checks import project
from test_forge_workspace import demo, isolated


def test_create_register_and_check_in_browser(tmp_path):
    playwright=pytest.importorskip('playwright.sync_api')
    executable=Path(os.environ.get('FORGE_CHROMIUM',''))
    if not executable.is_file():
        pytest.skip('FORGE_CHROMIUM must point to an installed browser')
    registry=tmp_path/'console.json'
    registry.write_text(json.dumps(dict(format=console.FORMAT,projects={})))
    existing=tmp_path/'original-pipeline'; existing.mkdir()
    (existing/'README.md').write_text('原有 pipeline：注册和检查不能执行或改写这里的代码。')
    (existing/'pipeline.py').write_text('raise RuntimeError("DO NOT EXECUTE")\n')
    history=existing/'arbitrary-history';history.mkdir()
    for number in range(1025):
        (history/f'{number}.json').write_text('{}')
    before=(existing/'pipeline.py').read_bytes()
    artifact=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path/'screenshots')))
    artifact.mkdir(parents=True,exist_ok=True)
    with service(None,registry=registry) as app, playwright.sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path=str(executable),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(60000)
        errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        page.goto(app.origin+'/#token='+app.token)
        page.get_by_role('heading',name='开始使用 DemiFlow').wait_for()
        page.get_by_role('button',name='新建项目',exact=True).click()
        dialog=page.get_by_role('dialog')
        dialog.get_by_label('项目目录').fill(str(tmp_path/'new-project'))
        dialog.get_by_label('项目名称').fill('新建项目验收')
        page.screenshot(path=str(artifact/'create-project.png'))
        dialog.get_by_role('button',name='新建项目',exact=True).click()
        page.locator('#workspace-name').filter(has_text='新建项目验收').wait_for()
        assert set(p.name for p in (tmp_path/'new-project').iterdir()) == {'.forge','workspace.json'}
        page.get_by_role('button',name='注册已有项目',exact=True).click()
        dialog=page.get_by_role('dialog');dialog.get_by_label('项目目录').fill(str(existing))
        dialog.get_by_role('button',name='注册已有项目',exact=True).click()
        page.locator('#workspace-name').filter(has_text='original-pipeline').wait_for()
        page.locator('#sync-status').filter(has_text='自动同步已开启').wait_for()
        page.get_by_role('button',name='管道',exact=True).click()
        project_info(page)
        page.locator('.notice').filter(has_text='arbitrary-history').wait_for()
        page.get_by_text('pipeline.py',exact=True).click()
        page.locator('pre').filter(has_text='DO NOT EXECUTE').wait_for()
        page.screenshot(path=str(artifact/'registered-project.png'))
        project_info(page)
        page.get_by_role('button',name='检查当前项目',exact=True).click()
        page.get_by_text('统一规范版本：2.4.0',exact=True).wait_for()
        page.locator('.error-box').filter(has_text='FL-01').wait_for()
        page.screenshot(path=str(artifact/'project-check.png'),full_page=True)
        assert (existing/'pipeline.py').read_bytes() == before
        assert len(list(history.iterdir())) == 1025
        assert not errors,errors
        assert app.token not in page.url
        browser.close()


def test_existing_declared_project_check_and_violation(project, tmp_path):
    playwright=pytest.importorskip('playwright.sync_api')
    executable=Path(os.environ.get('FORGE_CHROMIUM',''))
    if not executable.is_file():
        pytest.skip('FORGE_CHROMIUM must point to an installed browser')
    registry=tmp_path/'console.json'; console.add(registry,project)
    artifact=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path/'screenshots')))
    artifact.mkdir(parents=True,exist_ok=True)
    with service(None,registry=registry) as app, playwright.sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path=str(executable),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(60000)
        errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        page.goto(app.origin+'/#token='+app.token)
        page.locator('#workspace-name').filter(has_text='question-experiments').wait_for()
        project_info(page)
        admission=[]
        def checked_click(expected):
            check=project/'.forge/project-check.json'
            def snapshot():
                if not check.exists():return None
                assert check.stat().st_size<=2*1024**2
                return check.read_bytes()
            for attempt in range(3):
                before=snapshot()
                with page.expect_response('**/project-check',timeout=65000) as response:
                    page.get_by_role('button',name='检查当前项目',exact=True).click()
                reply=response.value;value=reply.json()
                admission.append(dict(expected=expected,http_status=reply.status,
                    status=value.get('status'),static_status=value.get('static_status'),code=value.get('code'),
                    operation_started=value.get('operation_started')))
                (artifact/'project-check-admission.json').write_text(json.dumps(admission,ensure_ascii=False,indent=2)+'\n')
                if reply.status==503:
                    assert value['code']=='busy' and value.get('operation_started') is False,value
                    assert snapshot()==before
                    notice=page.locator('.error-box').filter(has_text='检查未受理').last
                    notice.wait_for()
                    assert '写入结果可能尚未确认' not in notice.inner_text()
                    assert attempt<2,'Check remained busy after bounded explicit retries'
                    refresh=page.get_by_role('button',name='刷新当前视图',exact=True)
                    refresh.click()
                    playwright.expect(refresh).not_to_have_attribute('data-pending','true',timeout=65000)
                    continue
                assert reply.status==200,value
                assert value['static_status']==expected,value
                return
        checked_click('checked')
        page.get_by_text('整体：尚未完成；静态检查：静态检查通过',exact=True).wait_for()
        page.screenshot(path=str(artifact/'static-check-passed.png'))
        source=project/'processing.py';source.write_text('import subprocess\n'+source.read_text())
        before=source.read_bytes()
        checked_click('invalid')
        page.get_by_text('整体：不符合规范；静态检查：不符合规范',exact=True).wait_for()
        page.screenshot(path=str(artifact/'violation-check.png'))
        assert source.read_bytes()==before
        assert not errors, errors
        browser.close()


@pytest.mark.parametrize('pre_dispatch',[True,False])
def test_project_check_busy_is_unapplied_until_explicit_retry(project,tmp_path,pre_dispatch):
    from contextlib import contextmanager
    from demiflow.workbench.projects import workspace as ws
    playwright=pytest.importorskip('playwright.sync_api')
    executable=Path(os.environ.get('FORGE_CHROMIUM',''))
    if not executable.is_file():pytest.skip('FORGE_CHROMIUM must point to an installed browser')
    registry=tmp_path/'console.json';console.add(registry,project)
    artifact=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path/'screenshots')))
    artifact.mkdir(parents=True,exist_ok=True)
    with service(None,registry=registry) as app,playwright.sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path=str(executable),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(65000)
        writes=[];errors=[];trace=[]
        page.on('request',lambda r:writes.append(r.url) if r.method=='POST' and r.url.endswith('/project-check') else None)
        page.on('pageerror',lambda error:errors.append(str(error)))
        page.goto(app.origin+'/#token='+app.token)
        page.locator('#workspace-name').filter(has_text='question-experiments').wait_for()
        project_info(page)
        page.evaluate('() => Object.defineProperty(document,"hidden",{configurable:true,get:()=>true})')
        refresh=page.get_by_role('button',name='刷新当前视图',exact=True);refresh.click()
        playwright.expect(refresh).not_to_have_attribute('data-pending','true',timeout=65000)
        with page.expect_response('**/project-check') as initial:
            page.get_by_role('button',name='检查当前项目',exact=True).click()
        assert initial.value.status==200 and initial.value.json()['static_status']=='checked'
        page.get_by_text('整体：尚未完成；静态检查：静态检查通过',exact=True).wait_for()
        source=project/'processing.py';source.write_text('import subprocess\n'+source.read_text())
        check=project/'.forge/project-check.json';assert check.stat().st_size<=2*1024**2
        before=check.read_bytes()
        @contextmanager
        def occupied():
            if pre_dispatch:
                assert app.workers.acquire(timeout=65)
                try:yield
                finally:app.workers.release()
            else:
                with ws.control_lock(project):yield
        with occupied():
            with page.expect_response('**/project-check') as refused:
                page.get_by_role('button',name='检查当前项目',exact=True).click()
            body=refused.value.json()
            assert refused.value.status==503 and body['code']=='busy'
            if pre_dispatch:
                assert body.get('operation_started') is False
                notice=page.locator('.error-box').filter(has_text='检查未受理').last;notice.wait_for()
                assert '尚未开始' in notice.inner_text() and '原依据保留' in notice.inner_text()
                assert '写入结果可能尚未确认' not in notice.inner_text()
            else:
                assert 'operation_started' not in body
                notice=page.locator('.error-box').filter(has_text=body['error']).last;notice.wait_for()
                assert '写入结果可能尚未确认' in notice.inner_text()
                assert '检查未受理' not in notice.inner_text() and '尚未开始' not in notice.inner_text()
            page.get_by_text('整体：尚未完成；静态检查：静态检查通过',exact=True).wait_for()
            assert check.read_bytes()==before and len(writes)==2
            trace.append(dict(http_status=503,code='busy',operation_started=body.get('operation_started'),
                shown=notice.inner_text(),check_file_unchanged=True,writes=len(writes)))
        with page.expect_response('**/project-check') as retry:
            page.get_by_role('button',name='检查当前项目',exact=True).click()
        assert retry.value.status==200 and retry.value.json()['static_status']=='invalid'
        page.get_by_text('整体：不符合规范；静态检查：不符合规范',exact=True).wait_for()
        assert len(writes)==3 and not errors,errors
        trace.append(dict(http_status=200,static_status='invalid',explicit_retry=True,writes=len(writes)))
        (artifact/('project-check-busy-'+('before' if pre_dispatch else 'after')+'-dispatch.json')).write_text(json.dumps(trace,ensure_ascii=False,indent=2)+'\n')
        browser.close()


def test_empty_console_observes_registration_from_cli(tmp_path):
    from click.testing import CliRunner
    from demiflow.cli import main
    playwright=pytest.importorskip('playwright.sync_api')
    executable=Path(os.environ.get('FORGE_CHROMIUM',''))
    if not executable.is_file():
        pytest.skip('FORGE_CHROMIUM must point to an installed browser')
    registry=tmp_path/'console.json'
    registry.write_text(json.dumps(dict(format=console.FORMAT,projects={})))
    existing=tmp_path/'cli-pipeline';existing.mkdir();(existing/'main.py').write_text('# original')
    with service(None,registry=registry) as app, playwright.sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path=str(executable),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page();page.set_default_timeout(30000)
        page.goto(app.origin+'/#token='+app.token)
        page.get_by_role('heading',name='开始使用 DemiFlow').wait_for()
        result=CliRunner().invoke(main,['workbench','console','--registry',str(registry),'add',str(existing)])
        assert result.exit_code==0,result.output
        # No reload or browser-side registration: the same console observes it.
        page.locator('#workspace-name').filter(has_text='cli-pipeline').wait_for()
        page.get_by_role('button',name='管道',exact=True).click()
        project_info(page)
        page.get_by_text('main.py',exact=True).wait_for()
        # Platform/environment diagnostics need not have a business filename.
        page.route('**/project-status',lambda route:route.fulfill(json={
            'status':'stale','static_status':'stale',
            'diagnostics':[{'rule':'FL-10','detail':'Platform changed during inspection'}]}))
        with page.expect_response('**/project-status'):
            page.get_by_role('button',name='刷新当前视图',exact=True).click()
        project_info(page)
        diagnostic=page.locator('.error-box').filter(has_text='Platform changed during inspection')
        diagnostic.wait_for()
        assert diagnostic.inner_text() == 'FL-10 · Platform changed during inspection'
        browser.close()


