"""Project conversation navigation with local protocol fixtures, never a model."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
import threading

import pytest

from demiflow.workbench.api import WorkspaceServer
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.projects import registry
from test_forge_development_chat import configuration


CHROME = '/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome'


@contextmanager
def conversation_app(tmp_path):
    root, config_path, config = configuration(tmp_path)
    registry_path = tmp_path / 'registry.json'
    workspace = registry.add(registry_path, root)['workspace_id']
    config['projects'][workspace] = config['projects'].pop('wfixture')
    # Distinct CLI conversations must have distinct provider IDs; a resumed
    # process adopts the requested existing ID instead of inventing another.
    fixture = Path(__file__).with_name('devchat_fake_provider.py').read_text()
    fixture = fixture.replace("thread = 'thread-fixture'", "import uuid\nthread = 'thread-' + uuid.uuid4().hex")
    fixture = fixture.replace("project_root = Path(params['cwd'])", "if method == 'thread/resume': thread = params['threadId']\n        project_root = Path(params['cwd'])")
    fixture_path = tmp_path / 'unique_thread_fixture.py'
    fixture_path.write_text(fixture)
    executable = Path(config['providers']['codex']['executable'])
    executable.write_text('#!' + sys.executable + '\nimport runpy\nrunpy.run_path(' + repr(str(fixture_path)) + ',run_name="__main__")\n')
    config_path.write_text(json.dumps(config))
    server = WorkspaceServer(registry=registry_path, development_config=config_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, workspace, root
    finally:
        server.shutdown(); server.server_close(); thread.join()


@contextmanager
def browser_page(server, initial_drafts=None):
    playwright = pytest.importorskip('playwright.sync_api')
    if not Path(CHROME).is_file(): pytest.skip('Installed Chromium unavailable')
    with playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch(executable_path=CHROME, headless=True,
                                          args=['--no-sandbox', '--disable-dev-shm-usage'])
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.set_default_timeout(15000)
        if initial_drafts is not None:
            page.add_init_script("sessionStorage.setItem('forge-development-drafts'," + json.dumps(json.dumps(initial_drafts)) + ");")
        errors = []; page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto(server.origin + '/#token=' + server.token)
        page.get_by_role('button', name='开发', exact=True).click()
        page.wait_for_function("()=>ForgeView.decodeView(location.hash)?.kind?.startsWith('development')")
        try:
            yield page, playwright.expect
            assert not errors, errors
        finally:
            browser.close()


def current_session(page):
    return page.evaluate('ForgeView.decodeView(location.hash).session_id')


def pick(page, sid):
    if not page.locator('.development-session-sidebar').is_visible():
        page.get_by_role('button', name='会话列表', exact=True).click()
    page.locator('.development-session[data-session-id="' + sid + '"]').click()
    page.wait_for_function('(sid)=>ForgeView.decodeView(location.hash).session_id===sid', arg=sid)


def send(page, text):
    page.get_by_label('开发要求').fill(text)
    page.get_by_role('button', name='发送', exact=True).click()
    page.locator('.development-status').filter(has_text='本轮已完成').wait_for()
    return current_session(page)


def test_new_conversations_have_independent_cli_contexts_drafts_and_writer(tmp_path):
    with conversation_app(tmp_path) as (server, workspace, root), browser_page(server) as (page, expect):
        creates = []
        page.on('request', lambda request: creates.append(request.post_data_json)
                if request.url.endswith('/dev-session-create') else None)
        expect(page.get_by_role('button', name='新会话')).to_be_enabled()
        assert page.locator('.development-head,.development-tabs').count() == 0
        page.get_by_role('button', name='新会话').click()
        page.wait_for_function("()=>ForgeView.decodeView(location.hash).kind==='development_new'")
        expect(page.get_by_label('开发要求')).to_have_value('')
        assert server.development.store.list(workspace)['sessions'] == []
        first = send(page, 'First independent conversation')
        first_provider = server.development.store.session(first)['provider_session']
        assert first_provider
        page.get_by_label('开发要求').fill('First draft stays here')
        page.get_by_role('button', name='新会话').click()
        page.wait_for_function("()=>ForgeView.decodeView(location.hash).kind==='development_new'")
        expect(page.get_by_label('开发要求')).to_have_value('')
        assert len(server.development.store.list(workspace)['sessions']) == 1
        second = send(page, 'Second independent conversation')
        second_provider = server.development.store.session(second)['provider_session']
        assert first != second and first_provider != second_provider
        assert len(creates) == 2 and all(value['new_conversation'] is True for value in creates)
        expect(page.locator('.dfc-user')).to_have_count(1)
        expect(page.locator('.dfc-user')).to_contain_text('Second independent conversation')
        page.get_by_label('开发要求').fill('Second draft stays here')
        shots = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(tmp_path / 'screenshots')))
        shots.mkdir(parents=True, exist_ok=True)
        for width in (1440, 390):
            page.set_viewport_size({'width': width, 'height': 1000})
            page.evaluate('()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            page.screenshot(path=str(shots / ('project-conversations-' + str(width) + '.png')), full_page=True)
        page.set_viewport_size({'width': 1440, 'height': 1000})
        pick(page, first)
        expect(page.get_by_label('开发要求')).to_have_value('First draft stays here')
        expect(page.locator('.dfc-user')).to_contain_text('First independent conversation')
        assert 'Second independent' not in page.locator('.development-flow').inner_text()
        page.reload()
        expect(page.get_by_label('开发要求')).to_have_value('First draft stays here')
        expect(page.get_by_label('开发要求')).to_be_enabled()
        assert current_session(page) == first
        page.go_back()
        page.wait_for_function('(sid)=>ForgeView.decodeView(location.hash).session_id===sid', arg=second)
        expect(page.get_by_label('开发要求')).to_have_value('Second draft stays here')
        assert current_session(page) == second
        pick(page, first)
        page.get_by_label('开发要求').fill('hang')
        page.get_by_role('button', name='发送', exact=True).click()
        page.locator('.development-status').filter(has_text='正在开发').wait_for()
        first_turn = server.development.store.show(workspace, first)['turn']['id']
        assert server.development.store.session(first)['provider_session'] == first_provider
        pick(page, second)
        expect(page.get_by_label('开发要求')).to_have_value('Second draft stays here')
        expect(page.get_by_label('开发要求')).to_be_enabled()
        expect(page.get_by_role('button', name='发送', exact=True)).to_be_disabled()
        expect(page.locator('.development-writer-notice')).to_contain_text('另一个会话正在使用此项目')
        assert page.get_by_role('button', name='停止', exact=True).count() == 0
        assert server.development.store.turn(first_turn)['cleanup'] != 'complete'
        page.get_by_label('开发要求').fill('Second draft edited while first runs')
        page.get_by_role('button', name='查看正在运行的会话', exact=True).click()
        page.wait_for_function('(sid)=>ForgeView.decodeView(location.hash).session_id===sid', arg=first)
        page.get_by_role('button', name='停止', exact=True).click()
        page.locator('.development-status').filter(has_text='已停止').wait_for()
        assert server.development.store.turn(first_turn)['cleanup'] == 'complete'
        pick(page, second)
        expect(page.get_by_label('开发要求')).to_have_value('Second draft edited while first runs')
        expect(page.get_by_role('button', name='发送', exact=True)).to_be_enabled()
        first_events = server.development.store.events(workspace, first)['events']
        second_events = server.development.store.events(workspace, second)['events']
        assert [event['data']['text'] for event in first_events if event['type'] == 'message.user'] == ['First independent conversation', 'hang']
        assert [event['data']['text'] for event in second_events if event['type'] == 'message.user'] == ['Second independent conversation']


def test_legacy_history_stays_readonly_and_session_list_is_bounded(tmp_path):
    with conversation_app(tmp_path) as (server, workspace, root):
        service = server.development
        primary = service.create_project(workspace, root, 'primary', 'codex')['id']
        legacy = service.create(workspace, root, 'legacy', 'codex')['id']
        turn, _ = service.store.submit(workspace, legacy, 'legacy-message', 0,
                                       'An earlier read-only history', root_identity(root), service.policy_hash)
        service.store.event(turn['id'], 'message.completed', dict(item_id='old-answer', text='Saved historical answer'))
        service.store.finish_provider(turn['id'], state='completed', cleanup='complete', result={})
        before = service.store.events(workspace, legacy)['events']
        with browser_page(server) as (page, expect):
            pick(page, legacy)
            expect(page.get_by_label('开发要求')).to_be_disabled()
            expect(page.get_by_role('button', name='发送', exact=True)).to_be_disabled()
            page.get_by_text('历史会话 · 只读，不能继续发送', exact=True).wait_for()
            expect(page.locator('.dfc-answer')).to_contain_text('Saved historical answer')
            page.reload()
            expect(page.get_by_label('开发要求')).to_be_disabled()
            assert current_session(page) == legacy
            assert service.store.events(workspace, legacy)['events'] == before
            pick(page, primary)
            expect(page.get_by_label('开发要求')).to_be_enabled()
            for index in range(23): service.create_project(workspace, root, 'more-' + str(index), 'codex', new_conversation=True)
            page.reload()
            expect(page.locator('.development-session')).to_have_count(20)
            page.get_by_role('button', name='更早的会话', exact=True).click()
            expect(page.locator('.development-session')).to_have_count(5)
            page.get_by_role('button', name='较新的会话', exact=True).click()
            expect(page.locator('.development-session')).to_have_count(20)
            page.set_viewport_size({'width': 320, 'height': 850})
            page.evaluate('()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
            expect(page.locator('.development-session-sidebar')).to_be_hidden()
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            assert page.locator('.development-conversation').bounding_box()['width'] >= 300
            page.get_by_role('button', name='会话列表', exact=True).click()
            expect(page.locator('.development-session-sidebar')).to_be_visible()
            expect(page.get_by_role('button', name='收起会话列表', exact=True)).to_have_count(0)
            tools=page.locator('.development-session-sidebar .side-navigation-heading')
            expect(tools.get_by_role('button',name='会话列表',exact=True)).to_be_visible()
            tools.get_by_role('button',name='搜索会话',exact=True).click()
            page.get_by_role('searchbox',name='搜索会话').fill('no matching conversation')
            expect(page.locator('.development-session:visible')).to_have_count(0)
            page.keyboard.press('Escape')
            expect(page.locator('.development-session:visible')).to_have_count(20)
            tools.get_by_role('button', name='会话列表', exact=True).click()
            expect(page.locator('.development-session-sidebar')).to_be_hidden()
            expect(page.get_by_role('button',name='会话列表',exact=True)).to_be_focused()
            expect(page.get_by_label('开发要求')).to_be_visible()
            shots = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(tmp_path / 'screenshots')))
            shots.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(shots / 'project-conversations-320.png'), full_page=True)


def test_single_conversation_capability_does_not_fake_new_conversations(tmp_path):
    with conversation_app(tmp_path) as (server, workspace, root):
        primary = server.development.create_project(workspace, root, 'primary', 'codex')['id']
        capabilities = server.development.capabilities
        server.development.capabilities = lambda *args: {**capabilities(*args), 'multiple_conversations': False}
        with browser_page(server) as (page, expect):
            expect(page.get_by_role('button', name='新会话')).to_be_disabled()
            assert current_session(page) == primary
            target = page.evaluate('(workspace)=>ForgeView.encodeView({v:2,workspace,kind:"development_new"})', workspace)
            page.goto(server.origin + '/' + target)
            expect(page.get_by_label('开发要求')).to_be_disabled()
            page.get_by_text('当前开发服务不支持新建独立会话，请从左侧打开已有会话。', exact=True).wait_for()
            assert len(server.development.store.list(workspace)['sessions']) == 1


@pytest.mark.parametrize('has_primary', [False, True])
def test_default_unstarted_legacy_draft_survives_navigation_upgrade(tmp_path, has_primary):
    with conversation_app(tmp_path) as (server, workspace, root):
        primary = server.development.create_project(workspace, root, 'primary', 'codex')['id'] if has_primary else None
        with browser_page(server, [[workspace + ':project', 'Unsent older draft']]) as (page, expect):
            expect(page.get_by_label('开发要求')).to_have_value('Unsent older draft')
            assert len(server.development.store.list(workspace)['sessions']) == int(has_primary)
            saved = dict(page.evaluate("JSON.parse(sessionStorage.getItem('forge-development-drafts'))"))
            assert saved[workspace + ':' + (primary or 'new')] == 'Unsent older draft'
            assert workspace + ':project' not in saved


def test_existing_new_draft_does_not_erase_different_legacy_draft(tmp_path):
    with conversation_app(tmp_path) as (server, workspace, root), browser_page(server, [[workspace + ':project', 'Older unsent draft'], [workspace + ':new', 'Newer unsent draft']]) as (page, expect):
        expect(page.get_by_label('开发要求')).to_have_value('Newer unsent draft')
        saved = dict(page.evaluate("JSON.parse(sessionStorage.getItem('forge-development-drafts'))"))
        assert saved[workspace + ':new'] == 'Newer unsent draft'
        assert saved[workspace + ':project'] == 'Older unsent draft'
