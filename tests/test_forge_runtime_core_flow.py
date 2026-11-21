"""Generic job onboarding and real execution through the authenticated UI.

The fixture authors native files/data, but does not register entries, grant
permissions, prepare or start a run on behalf of the browser.
"""
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from playwright.sync_api import expect

from demiflow.workbench.projects import registry as console
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from _forge_project_fixtures import grant_config_write
from test_forge_runs import base, root, wait
from test_forge_public_api import request, service
from test_forge_markdown_reading import browser
from test_forge_web_redesign import opened, navigate, artifact


@pytest.fixture(autouse=True)
def private_runtime_domain(tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'runtime-domain'))


@pytest.fixture
def unregistered(root):
    control = ws.read_control(root / 'workspace.json')
    expected = content_digest(control)
    control['entries'] = {}
    ws.write_control(root / 'workspace.json', control, expected=expected)
    (root / 'operators/waiting.py').write_text('''def slow(text: str | None) -> str | None:
    total = 0
    for i in range(2000000000):
        total = total + i
    return text
''')
    (root / 'flows/waiting.py').write_text('''from demiflow import data
from operators.waiting import slow
def run(*, source, target, other):
    d = data.read_lance(source['uri'], version=source['version'])
    first = d.write_lance(target['uri'], mode='create', return_receipt=True)
    second = d.map(slow, inputs={'text': 'question'}, output='waiting').write_lance(other['uri'], mode='create', return_receipt=True)
    return first, second
''')
    (root / 'flows/waiting.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1', 'arguments': {
            'source': {'kind': 'table', 'alias': 'candidates'},
            'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'first.lance'},
            'other': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'slow.lance'}}}))
    return root


def test_http_registration_permissions_cas_and_no_execution(unregistered, tmp_path):
    registry = tmp_path / 'registry.json'
    key = console.add(registry, unregistered)['workspace_id']
    prefix = f'/api/workspaces/{key}/'
    body = {'entry': 'flows/questions.py:select', 'params': 'flows/select_a.json'}
    original = registry.read_bytes()
    expected = content_digest(console.load_registry(registry))
    grant = dict(workspace_id=key, expected=expected, run_submit=True, run_control=True, attach=False)
    for mode in ({}, {'control_only': True}):
        with service(None, registry=registry, **mode) as app:
            assert request(app, '/api/project-permissions', grant)[0] == 403
            assert request(app, prefix+'flow-register', body)[0] == 403
            assert registry.read_bytes() == original
    with service(None, registry=registry, enable_runs=True, enable_config_write=True) as app:
        assert request(app, prefix+'flow-register', body)[0] == 403
        assert request(app, '/api/project-permissions', grant, headers={'X-Forge-Token': 'bad'})[0] == 403
        assert request(app, '/api/project-permissions', {**grant, 'run_submit': 1})[0] == 400
        assert request(app, '/api/project-permissions', {**grant, 'expected': '0'*64})[0] == 409
        assert request(app, '/api/project-permissions', grant)[0] == 200
        assert request(app, '/api/project-permissions', grant)[0] == 409
        assert request(app, prefix+'flow-register', body)[0] == 403
        grant_config_write(registry, key)
        before = (unregistered/'workspace.json').read_bytes()
        for invalid in ({**body, 'entry': '../outside.py:run'}, {**body, 'params': '../bad.json'},
                        {**body, 'entry': 'flows/questions.py:missing'}, {**body, 'extra': True}):
            assert request(app, prefix+'flow-register', invalid)[0] in (400, 422)
            assert (unregistered/'workspace.json').read_bytes() == before
        # AST registration must never import or execute the user module.
        (unregistered/'flows/never.py').write_text('raise RuntimeError("must not import")\ndef run():\n    pass\n')
        assert request(app, prefix+'flow-register', {**body, 'entry': 'flows/never.py:run'})[0] == 422
        assert (unregistered/'workspace.json').read_bytes() == before
        status, registered = request(app, prefix+'flow-register', body)
        assert status == 200 and registered['status'] == 'registered', registered
        assert request(app, prefix+'flow-register', body)[1]['status'] == 'unchanged'
        (unregistered/'flows/total.py').write_text('from demiflow import data\ndef run():\n    return data.range(2).count()\n')
        status, registered = request(app, prefix+'flow-register', {'entry': 'flows/total.py:run'})
        assert status == 200 and registered['params'] == 'parameters/total.json', registered
        assert registered['params'] in ws.load_workspace_control(unregistered)[1]['entries']['flows/total.py:run']
        assert request(app, prefix+'context')[1]['run_permissions'] == dict(run_submit=True, run_control=True, attach=False)
        assert not list((unregistered/'.forge/runs').glob('*'))


def open_menu_action(page, name):
    menu = page.locator('.wb-more')
    if menu.get_attribute('open') is None:
        menu.locator('summary').first.click()
    menu.get_by_role('button', name=name, exact=True).click()


def submit_run(page):
    page.locator('.wb-actions').get_by_role('button', name='运行', exact=True).click()
    page.get_by_role('button', name='确认运行', exact=True).click()


def choose_flow(page, entry):
    page.get_by_role('button', name=entry, exact=True).click()
    expect(page.get_by_role('button', name=entry, exact=True)).to_have_attribute('aria-current', 'page')


def not_admitted(response):
    if response.status != 503:
        return False
    error = response.json()
    return error.get('code') == 'busy' and error.get('operation_started') is False


def register_from_page(page, entry, params, *, evidence_dir, project_root=None):
    settings=page.locator('.wb-settings')
    if not settings.is_visible():
        open_menu_action(page, '接入与权限')
    settings.get_by_label('登记流程入口',exact=True).fill(entry)
    settings.get_by_label('登记参数文件',exact=True).fill(params)
    def registered_graph(response):
        url = urlsplit(response.url)
        query = parse_qs(url.query)
        return (url.path.endswith('/graph') and query.get('basis_kind') == ['source']
                and query.get('entry') == [entry] and query.get('params') == [params])
    control = project_root / 'workspace.json' if project_root else None
    before = control.read_bytes() if control else None
    register = settings.get_by_role('button', name='登记原生流程', exact=True)
    with page.expect_response(registered_graph) as graph:
        with page.expect_response('**/flow-register') as registered:
            register.click()
        if not_admitted(registered.value):
            expect(page.locator('.wb-notice')).to_contain_text('登记尚未开始')
            expect(settings.get_by_label('登记流程入口',exact=True)).to_have_value(entry)
            expect(settings.get_by_label('登记参数文件',exact=True)).to_have_value(params)
            if control:
                assert control.read_bytes() == before
            # An explicit second click is allowed only for a confirmed pre-dispatch refusal.
            with page.expect_response('**/flow-register') as registered:
                register.click()
        assert registered.value.ok,registered.value.text()
    assert graph.value.ok,graph.value.text()
    expect(page.locator('.wb-graph')).not_to_have_attribute('aria-busy','true')
    try:
        expect(settings).not_to_be_visible(timeout=65000)
    except AssertionError:
        evidence=dict(registration_status=registered.value.status,registration=registered.value.json(),
                      notice=page.locator('.wb-notice').inner_text(),body=page.locator('main').inner_text()[:6400])
        evidence_dir.mkdir(parents=True,exist_ok=True)
        (evidence_dir/'registration-failure-R2.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+'\n')
        raise
    expect(page.get_by_role('button', name=entry, exact=True)).to_have_attribute('aria-current', 'page')
    page.wait_for_function('(params) => ForgeView.decodeView(location.hash)?.params === params', arg=params)


def finished(page,status='已完成'):
    expect(page.locator('.wb-run-bar')).to_be_visible(timeout=65000)
    expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text(status,timeout=65000)


def current_run(page):
    return page.evaluate('ForgeView.decodeView(location.hash).run_id')


def assert_single_run(root, run_id):
    # Driver resource accounting is a sibling directory, not another run.
    # Check both the public history and the independently named run directories.
    listed = runs.list_runs(root)
    assert listed['total'] == 1
    assert [row['run_id'] for row in listed['runs']] == [run_id]
    native = [path.name for path in (root/'.forge/runs').iterdir()
              if re.fullmatch(r'r[0-9a-f]{24}', path.name)]
    assert native == [run_id]


def test_browser_onboard_submit_result_then_monitor_stop(unregistered, tmp_path, browser):
    registry = tmp_path / 'registry.json'
    registry.write_text(json.dumps(dict(format=console.FORMAT_V3, projects={})))
    author_files = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                    for folder in ('flows', 'operators') for p in (unregistered/folder).iterdir() if p.is_file()}
    active = None
    with service(None, registry=registry, enable_runs=True, enable_config_write=True) as app, opened(browser, app) as (page, requests):
        page.set_default_timeout(65000)
        page.get_by_role('button', name='注册已有项目', exact=True).click()
        dialog = page.get_by_role('dialog')
        dialog.get_by_label('项目目录', exact=True).fill(str(unregistered))
        dialog.get_by_role('button', name='注册已有项目', exact=True).click()
        navigate(page, '管道')
        settings=page.locator('.wb-settings')
        if not settings.is_visible():
            open_menu_action(page, '接入与权限')
        expect(settings.get_by_role('button', name='登记原生流程', exact=True)).to_be_disabled()
        for name in ('接入与提交', '停止与核对', '保留结果为输入'):
            settings.get_by_label(name, exact=True).check()
        settings.get_by_role('button', name='保存运行权限', exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('运行权限已更新')
        settings.get_by_label('保存图上配置', exact=True).check()
        settings.get_by_role('button', name='保存配置写入权限', exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('配置写入权限已更新')
        register_from_page(page, 'flows/questions.py:select', 'flows/select_a.json', evidence_dir=tmp_path/'evidence')
        for width in (1440, 390, 320):
            page.set_viewport_size({'width': width, 'height': 1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
            artifact(page, f'core-submit-{width}')
        submit_run(page)
        finished(page)
        first = current_run(page)
        assert runs.show(unregistered, first)['outputs'][0]['result']['ref']['row_count'] == 2
        page.get_by_role('button', name='输出结果',exact=True).click()
        page.get_by_label('结果别名 target', exact=True).fill('selected')
        page.get_by_role('button', name='保留为输入表', exact=True).click()
        page.get_by_text('已保留固定版本引用：selected', exact=True).wait_for()
        page.get_by_role('button', name='查看固定结果', exact=True).click()
        page.get_by_text('c01', exact=True).first.wait_for()
        # Returning to the pipeline starts a real preview. Even settings opened
        # mid-preview must defer registration until that read settles.
        held_preview = []
        page.route('**/plan-preview', lambda route: held_preview.append(route), times=1)
        with page.expect_request('**/plan-preview'):
            navigate(page, '管道')
        if not settings.is_visible():
            open_menu_action(page, '接入与权限')
        registration = settings.get_by_role('button',name='登记原生流程',exact=True)
        expect(registration).to_be_disabled()
        assert sum(method=='POST' and urlsplit(url).path.endswith('/flow-register') for method,url in requests)==1
        previews = []
        def record_preview(response):
            if urlsplit(response.url).path.endswith('/plan-preview'):
                previews.append(response)
        page.on('response', record_preview)
        try:
            held_preview[0].continue_()
            expect(registration).to_be_enabled(timeout=65000)
        finally:
            page.remove_listener('response', record_preview)
        # The UI can retry a read when the server explicitly declined to start it.
        assert previews and previews[-1].ok, [r.text() for r in previews]
        assert all(not_admitted(response) for response in previews[:-1])
        register_from_page(page, 'flows/waiting.py:run', 'flows/waiting.json', evidence_dir=tmp_path/'evidence')
        open_menu_action(page, '整图配置')
        page.get_by_text('本次运行设置',exact=True).click()
        page.get_by_label('本次运行秒数上限', exact=True).fill('90')
        try:
            submit_run(page)
            page.wait_for_function(
                "prior => {const v=ForgeView.decodeView(location.hash);return v.run_id && v.run_id!==prior;}",
                arg=first)
            active = current_run(page)
            expect(page.locator('.wb-run-record.selected')).to_have_attribute('data-run', active)
            page.get_by_text('部分输出已提交', exact=False).first.wait_for()
            page.get_by_text('性能趋势 · 整次运行', exact=True).click()
            page.get_by_role('img', name='整次运行的 CPU 核数趋势').wait_for()
            artifact(page, 'core-running-320')
            page.get_by_role('button', name='停止', exact=True).click()
            page.get_by_role('button', name='确认停止', exact=True).click()
            page.get_by_text('所属执行已停止', exact=False).first.wait_for()
            result = runs.show(unregistered, active)
            assert result['status'] == 'cancelled' and result['state']['execution_stopped']
            assert sum(bool(o['result']) for o in result['outputs']) == 1
            artifact(page, 'core-stopped-320')
            page.reload()
            page.get_by_text('所属执行已停止', exact=False).first.wait_for()
            assert page.evaluate('ForgeView.decodeView(location.hash).run_id') == active
        finally:
            if active and runs.show(unregistered, active)['status'] in runs.ACTIVE:
                runs.cancel(unregistered, active)
                wait(unregistered, active, timeout=20)
        assert sum(method == 'POST' and url.endswith('/run-prepare') for method, url in requests) == 2
    for path, digest in author_files.items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest


def test_lost_prepare_and_start_responses_keep_one_run(unregistered, browser, tmp_path):
    with service(unregistered, enable_runs=True, enable_config_write=True, workspace_config_write=True) as app, opened(browser, app) as (page, _):
        page.set_default_timeout(65000)
        navigate(page, '管道')
        register_from_page(page, 'flows/questions.py:select', 'flows/select_a.json', evidence_dir=tmp_path/'evidence')
        identities = []
        page.on('request', lambda r: identities.append(r.post_data_json['request_id']) if r.method == 'POST' and r.url.endswith('/run-prepare') else None)

        def drop_prepare(route):
            response = route.fetch(timeout=65000)
            assert response.ok, response.text()
            route.abort('failed')
            page.unroute('**/run-prepare', drop_prepare)

        def drop_start(route):
            response = route.fetch(timeout=65000)
            assert response.ok, response.text()
            route.abort('failed')
            page.unroute('**/run-start', drop_start)

        page.route('**/run-prepare', drop_prepare)
        page.route('**/run-start', drop_start)
        submit_run(page)
        page.locator('.wb-notice.error-box').wait_for()
        page.reload()
        page.get_by_role('button', name='以原请求重试准备', exact=True).click()
        page.get_by_role('button', name='开始这次运行',exact=True).click()
        page.get_by_role('button', name='刷新状态',exact=True).click()
        finished(page)
        assert len(identities) == 2 and identities[0] == identities[1]
        assert_single_run(unregistered, current_run(page))
        run_id = page.evaluate('ForgeView.decodeView(location.hash).run_id')
        assert runs.show(unregistered, run_id)['status'] == 'succeeded'


def test_failed_prepare_can_be_read_closed_and_corrected(unregistered, browser, tmp_path):
    with service(unregistered, enable_runs=True, enable_config_write=True, workspace_config_write=True) as app, opened(browser, app) as (page, _):
        page.set_default_timeout(65000)
        navigate(page, '管道')
        register_from_page(page, 'flows/questions.py:select', 'flows/select_a.json', evidence_dir=tmp_path/'evidence', project_root=unregistered)
        config = unregistered/'flows/select_a.json'
        original = config.read_bytes()
        config.write_bytes((unregistered/'flows/waiting.json').read_bytes())
        submit_run(page)
        page.locator('.wb-notice.error-box').wait_for()
        saved = page.evaluate("JSON.parse(sessionStorage.getItem('demiflow-workbench-run-intents-v1'))")
        assert len(saved) == 1
        receipt = json.loads((unregistered/'.forge/requests'/(saved[0]['request_id']+'.json')).read_text())
        assert receipt['status'] == 'failed'
        page.get_by_role('button', name='核对原请求', exact=True).click()
        page.get_by_role('button', name='结束此失败请求', exact=True).click()
        assert page.evaluate("JSON.parse(sessionStorage.getItem('demiflow-workbench-run-intents-v1'))") == []
        assert not list((unregistered/'.forge/runs').glob('*'))
        config.write_bytes(original)
        register_from_page(page, 'flows/questions.py:select', 'flows/select_a.json', evidence_dir=tmp_path/'evidence', project_root=unregistered)
        assert page.evaluate('ForgeView.decodeView(location.hash).params') == 'flows/select_a.json'
        submit_run(page)
        finished(page)
        # The failed receipt is preserved, and only the corrected request executes.
        assert json.loads((unregistered/'.forge/requests'/(saved[0]['request_id']+'.json')).read_text()) == receipt
        assert_single_run(unregistered, current_run(page))


def test_reload_during_start(unregistered, browser, tmp_path):
    with service(unregistered, enable_runs=True, enable_config_write=True, workspace_config_write=True) as app, opened(browser, app) as (page, _):
        page.set_default_timeout(65000)
        navigate(page, '管道')
        register_from_page(page, 'flows/questions.py:select', 'flows/select_a.json', evidence_dir=tmp_path/'evidence')
        held=[]
        page.route('**/run-start', lambda route: held.append(route))
        with page.expect_request('**/run-start'):
            submit_run(page)
        for _ in range(20):
            if held: break
            page.wait_for_timeout(50)
        assert held
        response=held[0].fetch(timeout=65000)
        assert response.ok, response.text()
        run_id=response.json()['run_id']
        page.reload()
        held[0].abort('failed')
        page.locator('#sync-status').filter(has_text='最近同步').wait_for()
        observed=page.evaluate('ForgeView.decodeView(location.hash)')
        assert observed.get('run_id') == run_id, observed
        finished(page)
        assert_single_run(unregistered, current_run(page))
