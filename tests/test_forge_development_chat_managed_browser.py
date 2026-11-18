"""Browser projection contract; API states are supplied without a model call."""
import json
import os
from pathlib import Path
import threading

import pytest

from test_forge_development_chat_http import app


def test_gate_state_controls_completion_and_stop_after_provider_finishes(tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    browser_path = Path(os.environ.get('FORGE_CHROMIUM', '/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome'))
    if not browser_path.is_file():
        pytest.skip('Installed Chromium unavailable')
    with app(tmp_path) as (server, _, _), playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch(executable_path=str(browser_path), headless=True,
                                         args=['--no-sandbox', '--disable-dev-shm-usage'])
        page = browser.new_page(viewport={'width': 390, 'height': 850})
        page.set_default_timeout(15000)
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        projection = dict(task_id='fixture-task', state='verifying', busy=True,
            gates=dict(check='passed', verify='pending', review='unavailable'),
            source_fingerprint='candidate', accepted_fingerprint=None,
            assurance='gate_only', run_policy='observe', reason='')
        current_identity = {}
        stops, sends = [], []

        def current_session(route):
            response = route.fetch()
            body = response.json()
            if body.get('turn') and body['turn']['cleanup'] == 'complete':
                body['turn']['development'] = projection.copy()
                current_identity.update(session_id=body['id'], turn_id=body['turn']['id'])
            route.fulfill(response=response, body=json.dumps(body))

        def cancel(route):
            stops.append(route.request.post_data_json)
            projection.update(state='cancelled', busy=False)
            route.fulfill(status=200, content_type='application/json', body=json.dumps({'state':'stopping'}))

        page.route('**/dev-session?*', current_session)
        page.route('**/dev-cancel', cancel)
        page.on('request', lambda request: sends.append(request.post_data_json)
                if request.method == 'POST' and request.url.endswith('/dev-turn-start') else None)
        page.goto(server.origin + '/#token=' + server.token)
        page.get_by_role('button', name='开发', exact=True).click()
        page.get_by_label('开发要求').fill('A small normal task')
        page.get_by_role('button', name='发送', exact=True).click()
        status = page.locator('.development-status')
        playwright.expect(status).to_have_text('正在验证')
        playwright.expect(page.get_by_role('button', name='停止', exact=True)).to_be_enabled()
        assert page.get_by_text('本轮已完成', exact=True).count() == 0

        # The provider is already complete, but controller-owned review/repair
        # must retain Stop and prevent a new turn, including keyboard submit.
        for state, label in [('reviewing','正在独立审阅'), ('repair_pending','发现问题，准备自动修复'),
                             ('repairing','正在自动修复')]:
            projection.update(state=state, busy=True)
            playwright.expect(status).to_have_text(label)
            page.get_by_label('开发要求').fill('Do not dispatch while ' + state)
            send = page.get_by_role('button', name='发送', exact=True, include_hidden=True)
            playwright.expect(send).to_be_disabled()
            playwright.expect(send).not_to_be_visible()
            playwright.expect(page.get_by_role('button', name='停止', exact=True)).to_be_enabled()
            page.get_by_label('开发要求').press('Enter')
            assert len(sends) == 1
            page.get_by_role('button', name='停止', exact=True).click()
            playwright.expect(status).to_have_text('开发检查已停止')
            assert stops[-1]['session_id'] == current_identity['session_id']
            assert stops[-1]['turn_id'] == current_identity['turn_id']
            assert stops[-1]['request_id']
            playwright.expect(page.get_by_role('button', name='停止', exact=True)).not_to_be_visible()
            assert '本次开发完成' not in status.inner_text()

        for state, label in [('failed','开发检查未通过'), ('unknown','开发检查结果待核对'),
                             ('cancelled','开发检查已停止')]:
            projection.update(state=state, busy=False)
            playwright.expect(status).to_have_text(label)
            assert '本次开发完成' not in status.inner_text()

        projection.update(state='review_unavailable', busy=False,
                          gates=dict(check='passed', verify='passed', review='unavailable'))
        playwright.expect(status).to_have_text('检查与验证通过 · 待独立审阅')
        page.get_by_label('开发要求').fill('Continue fixing')
        playwright.expect(page.get_by_role('button', name='发送', exact=True)).to_be_enabled()
        playwright.expect(page.get_by_role('button', name='停止', exact=True)).not_to_be_visible()

        # An accepted label with stale or incomplete evidence cannot produce a
        # successful UI result even if a future server accidentally emits it.
        projection.update(state='accepted', accepted_fingerprint='old',
                          gates=dict(check='passed', verify='passed', review='passed'))
        playwright.expect(status).to_have_text('开发检查结果待核对')
        projection['accepted_fingerprint'] = 'candidate'
        playwright.expect(status).to_have_text('本次开发完成 · 可信本地')
        for gate in ('check','verify','review'):
            projection['gates'] = {**dict(check='passed', verify='passed', review='passed'), gate:'failed'}
            playwright.expect(status).to_have_text('开发检查结果待核对')
            projection['gates'] = dict(check='passed', verify='passed', review='passed')
            playwright.expect(status).to_have_text('本次开发完成 · 可信本地')
        projection.update(source_fingerprint=None, accepted_fingerprint=None,
                          gates=dict(check='passed', verify='passed', review='passed'))
        playwright.expect(status).to_have_text('开发检查结果待核对')
        projection.update(source_fingerprint='candidate', accepted_fingerprint='candidate')
        playwright.expect(status).to_have_text('本次开发完成 · 可信本地')
        projection['assurance'] = 'enforced'
        playwright.expect(status).to_have_text('本次开发完成 · 受限执行')
        page.locator('.development-info>summary').click()
        playwright.expect(page.locator('.development-info-body')).to_contain_text('开发进程受外部执行边界约束。')
        assert '当前为可信本地模式，未防篡改。' not in page.locator('.development-info-body').inner_text()
        projection['assurance'] = 'gate_only'
        projection['state'] = 'stale'
        playwright.expect(status).to_have_text('内容已改变，需要重新检查')
        playwright.expect(page.locator('.development-info-body')).to_contain_text('未防篡改')
        assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
        assert not errors, errors
        browser.close()


def test_managed_edit_reaches_gate_in_browser_without_manual_commands(tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    browser_path = Path(os.environ.get('FORGE_CHROMIUM', '/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome'))
    if not browser_path.is_file():
        pytest.skip('Installed Chromium unavailable')
    from test_forge_development import DevelopmentTests
    from test_forge_development_chat import configuration
    from demiflow.workbench.api import WorkspaceServer
    from demiflow.workbench.projects import registry
    fixture = DevelopmentTests()
    fixture.setUp()
    server = thread = None
    try:
        _, path, value = configuration(tmp_path)
        root = fixture.root
        registry_path = tmp_path / 'registry.json'
        workspace = registry.add(registry_path, root)['workspace_id']
        value['projects'] = {workspace: dict(root=str(root), kind='demiforge_registered',
            phase='implementation', pipeline='orders', writes=['orders'], watch=['orders'],
            sources=['AGENTS.md'], non_goals=['No production'], authorization='UI protocol fixture', allow_tests=True)}
        path.write_text(json.dumps(value))
        server = WorkspaceServer(registry=registry_path, development_config=path)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch(executable_path=str(browser_path), headless=True,
                                             args=['--no-sandbox', '--disable-dev-shm-usage'])
            page = browser.new_page(viewport={'width': 390, 'height': 850})
            page.set_default_timeout(20000)
            page.goto(server.origin + '/#token=' + server.token)
            page.get_by_role('button', name='开发', exact=True).click()
            page.get_by_label('开发要求').fill('managed-edit original intent')
            page.get_by_role('button', name='发送', exact=True).click()
            status = page.locator('.development-status')
            playwright.expect(status).to_have_text('检查与验证通过 · 待独立审阅')
            checks = page.locator('.development-message').filter(has_text='开发检查记录')
            playwright.expect(checks).to_have_count(1)
            playwright.expect(checks).to_contain_text('检查与验证通过 · 待独立审阅')
            assert page.get_by_text('本轮已完成', exact=True).count() == 0
            assert page.get_by_text('本次开发完成 · 可信本地', exact=True).count() == 0
            session = server.development.store.list(workspace)['sessions'][0]['id']
            record = server.development.root / 'forge-development' / workspace / session / 'task.json'
            saved = json.loads(record.read_text())
            assert saved['check']['passed'] and saved['verification']['passed']
            assert saved['review']['status'] == 'unavailable' and saved['review']['passed'] is False
            assert saved['managed']['acceptance'] is None
            assert not (root / '.demiforge/tasks' / session).exists()
            with (root / 'orders/operators/map.py').open('a') as stream:
                stream.write('# external edit after checks\n')
            playwright.expect(status).to_have_text('内容已改变，需要重新检查')
            # Historical gate events retain their original wording. Inspecting
            # an old message never replaces the current candidate's stale state.
            page.get_by_role('button', name='定位消息：managed-edit original intent', exact=True).click()
            playwright.expect(page.locator('.development-title')).to_have_text('历史消息 · 只读')
            playwright.expect(page.get_by_label('开发要求')).to_be_disabled()
            playwright.expect(page.get_by_role('button', name='发送', exact=True)).to_be_disabled()
            playwright.expect(status).to_have_text('内容已改变，需要重新检查')
            playwright.expect(checks).to_contain_text('检查与验证通过 · 待独立审阅')
            page.get_by_role('button', name='回到最新记录', exact=True).click()
            playwright.expect(page.locator('.development-title')).to_have_text('项目对话')
            playwright.expect(status).to_have_text('内容已改变，需要重新检查')
            playwright.expect(checks).to_contain_text('检查与验证通过 · 待独立审阅')
            assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
            browser.close()
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=5)
        fixture.doCleanups()
