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
def conversation_app(tmp_path, *, enable_config_write=False):
    root, config_path, config = configuration(tmp_path)
    registry_path = tmp_path / 'registry.json'
    workspace = registry.add(registry_path, root)['workspace_id']
    if enable_config_write:
        cur = registry.load_registry(registry_path)
        migrated = registry.migrate(registry_path, expected=registry.content_digest(cur))
        registry.set_capabilities(
            registry_path, workspace, expected=migrated['digest'],
            capabilities={k: (k == 'config_write') for k in registry.CAPABILITIES},
        )
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
    server = WorkspaceServer(registry=registry_path, development_config=config_path,
                             enable_config_write=enable_config_write)
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
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        page.locator('.pw-sidebar').get_by_role('button', name='会话', exact=True).click()
        page.wait_for_selector('.pw-assistant-host:not([hidden])')
        try:
            yield page, playwright.expect
            assert not errors, errors
        finally:
            browser.close()


def current_session(page):
    return page.evaluate('ForgeView.decodeView(location.hash).session_id')


def pick(page, sid):
    if not page.locator('.pw-sidebar').is_visible():
        page.get_by_role('button', name='展开管线目录', exact=True).click()
    page.locator('.development-session[data-session-id="' + sid + '"]').click()
    page.wait_for_function('(sid)=>ForgeView.decodeView(location.hash).session_id===sid', arg=sid)


def send(page, text):
    page.get_by_label('开发要求').fill(text)
    page.get_by_role('button', name='发送', exact=True).click()
    page.locator('.development-status[data-turn-state="completed"][data-turn-cleanup="complete"]').wait_for(state='attached')
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
        settled = page.locator('.development-session[data-session-id="' + first + '"]')
        expect(settled).to_have_text('First independent conversation')
        expect(settled.locator('.development-session-state')).to_have_count(0)
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
            layout_marker = page.evaluate('()=>{const el=document.querySelector(".development-layout");el.dataset.persistCheck="kept";return true;}')
            assert layout_marker
            pick(page, legacy)
            # In-place session switch keeps the mounted layout intact and all sessions continuable.
            assert page.evaluate('document.querySelector(".development-layout")?.dataset.persistCheck') == 'kept'
            expect(page.get_by_label('开发要求')).to_be_enabled()
            expect(page.locator('.development-session-state')).to_have_count(0)
            expect(page.locator('.dfc-answer')).to_contain_text('Saved historical answer')
            page.reload()
            expect(page.get_by_label('开发要求')).to_be_enabled()
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


def test_turn_rail_history_and_project_files_share_the_conversation(tmp_path):
    with conversation_app(tmp_path) as (server, workspace, root):
        (root / 'nested').mkdir()
        (root / 'nested' / 'notes.md').write_text('Project-only document')
        (root / 'nested' / 'step.py').write_text('print(1)\n' + ''.join(f'x_{i} = {i}\n' for i in range(120)) + 'print("END_MARKER")\n')
        store = server.development.store
        sid = server.development.create_project(workspace, root, 'rail-fixture', 'codex')['id']
        for i in range(25):
            turn, _ = store.submit(workspace, sid, 'message-'+str(i), i, '第 '+str(i+1)+' 个问题',
                                   root_identity(root), server.development.policy_hash)
            if i == 24:
                store.event(turn['id'], 'message.completed', dict(item_id='reason', channel='process',
                            text=('展开的思考段落。\n\n' * 90)))
            store.event(turn['id'], 'message.completed', dict(item_id='answer', text=('这是一段长回复。\n\n'*18)))
            store.finish_provider(turn['id'], state='completed', cleanup='complete', result={})
        with browser_page(server) as (page, expect):
            shot_dir = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', tmp_path))
            shot_dir.mkdir(parents=True, exist_ok=True)
            current = page.get_by_role('button', name='跳到第 25 轮', exact=True)
            expect(current).to_have_attribute('aria-current', 'true')
            assert current.bounding_box()['x'] > 1390
            first = page.get_by_role('button', name='加载第 1 轮', exact=True)
            expect(first).to_be_visible()
            first.hover()
            expect(page.get_by_role('tooltip')).to_contain_text('第 1 个问题')
            page.screenshot(path=str(shot_dir/'turn-rail.png'))
            first.click()
            expect(page.get_by_role('button', name='跳到第 1 轮', exact=True)).to_have_attribute('aria-current','true')
            expect(page.locator('.dfc-user').first).to_contain_text('第 1 个问题')
            expect(page.get_by_label('开发要求')).to_be_disabled()
            page.get_by_role('button',name='跳到第 3 轮',exact=True).click()
            expect(page.get_by_role('button',name='跳到第 3 轮',exact=True)).to_have_attribute('aria-current','true')
            page.get_by_role('button',name='回到最新记录').click()
            expect(current).to_have_attribute('aria-current','true')
            earlier = page.get_by_role('button', name='加载更早的记录')
            earlier.click()
            expect(page.get_by_label('开发要求')).to_be_disabled()
            page.get_by_role('button',name='回到最新记录').click()
            expect(current).to_have_attribute('aria-current','true')
            page.locator('.dfc-think>summary').last.click()
            process = page.locator('.dfc-step-process>.dfc-disclosure-body').last
            expect(process).to_contain_text('展开的思考段落')
            expect(process.locator('.dfc-markdown p').first).to_be_visible()
            metrics = process.evaluate('n=>[n.scrollHeight,n.clientHeight,n.innerText.length]')
            assert metrics[0] > 800 and metrics[0] <= metrics[1]+1, metrics
            timeline = page.locator('.development-timeline')
            process.evaluate("n=>n.scrollIntoView({block:'start'})")
            viewport = timeline.bounding_box()
            page.mouse.move(viewport['x']+viewport['width']/2, viewport['y']+viewport['height']/2)
            before = timeline.evaluate('n=>n.scrollTop')
            fixed = page.get_by_label('开发要求').bounding_box()
            page.mouse.wheel(0,360)
            page.wait_for_function("top=>document.querySelector('.development-timeline').scrollTop>top+120",arg=before)
            assert page.get_by_label('开发要求').bounding_box()['y']==fixed['y']
            assert page.evaluate('window.scrollY')==0
            page.get_by_label('开发要求').fill('草稿')
            sidebar = page.locator('.development-session-sidebar')
            sidebar.get_by_role('button',name='文件').click()
            sidebar.get_by_role('button',name='nested',exact=True).click()
            sidebar.get_by_role('button',name='step.py').click()
            pane = page.get_by_role('complementary',name='代码面板')
            expect(pane).to_be_visible()
            expect(pane.locator('.cm-editor')).to_be_visible()
            expect(pane).to_contain_text('print(1)')
            # CodeMirror gutter and code content must sit side-by-side under strict CSP (not stacked vertically!).
            gutter_box = pane.locator('.cm-gutters').bounding_box()
            content_box = pane.locator('.cm-content').bounding_box()
            assert content_box['x'] >= gutter_box['x'] + gutter_box['width'] - 2, (gutter_box, content_box)
            assert abs(content_box['y'] - gutter_box['y']) < 20, (gutter_box, content_box)
            # CodeMirror scroller must scroll through full source (not clip at viewport height).
            scroll_metrics = pane.locator('.cm-scroller').evaluate('n => ({ch: n.clientHeight, sh: n.scrollHeight, ov: getComputedStyle(n).overflowY})')
            assert scroll_metrics['ov'] in ('auto', 'scroll') and scroll_metrics['sh'] > scroll_metrics['ch'], scroll_metrics
            pane.locator('.cm-scroller').evaluate('n => { n.scrollTop = n.scrollHeight; }')
            expect(pane).to_contain_text('END_MARKER')
            assert page.get_by_role('dialog').count() == 0
            # Collapsible 项目目录 toggle in left Files pane.
            tree_toggle = sidebar.locator('.dev-tree-section .dev-section-toggle')
            expect(tree_toggle).to_have_attribute('aria-expanded', 'true')
            tree_toggle.click()
            expect(tree_toggle).to_have_attribute('aria-expanded', 'false')
            expect(sidebar.locator('.dev-tree-body')).to_be_hidden()
            tree_toggle.click()
            expect(tree_toggle).to_have_attribute('aria-expanded', 'true')
            pane.get_by_role('button',name='引用到对话',exact=True).click()
            expect(page.get_by_label('开发要求')).to_have_value('草稿\n引用文件：nested/step.py')
            page.get_by_label('开发要求').fill('草稿')
            sidebar.get_by_role('button',name='notes.md').click()
            expect(pane).to_contain_text('Project-only document')
            assert page.locator('.development-timeline').bounding_box()['x'] < pane.bounding_box()['x']
            assert page.get_by_label('开发要求').bounding_box()['x'] < pane.bounding_box()['x']
            page.screenshot(path=str(shot_dir/'project-files.png'))
            sidebar.get_by_role('button',name='刷新项目目录').click()
            expect(sidebar.get_by_role('button',name='nested')).to_have_attribute('aria-expanded','false')
            page.get_by_role('button',name='关闭代码面板').click()
            for width in (390,320):
                page.set_viewport_size({'width':width,'height':760})
                picker=page.get_by_role('combobox',name='跳到会话轮次')
                expect(picker).to_be_visible()
                picker.select_option(index=0)
                expect(page.locator('.dfc-user').first).to_contain_text('第 1 个问题')
                page.get_by_role('button',name='回到最新记录').click()
                page.get_by_role('button',name='打开代码面板').click()
                expect(pane).to_be_visible()
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                page.get_by_role('button',name='关闭代码面板').click()
                expect(page.get_by_label('开发要求')).to_have_value('草稿')
            assert page.locator('.development-more,.development-trajectory').count() == 0
            assert page.locator('.development-conversation [role=tablist]').count() == 0
            # Published main now groups operators in the shared outline;
            # neither it nor the conversation adds another tab navigation.
            assert page.locator('[role=tablist]').count() == 0


def test_append_messages_while_busy_survives_reload_and_drains_in_order(tmp_path):
    import time
    with conversation_app(tmp_path) as (server, workspace, root):
        with browser_page(server) as (page, expect):
            field = page.get_by_label('开发要求')
            field.fill('input for queue test')
            field.press('Enter')
            expect(page.locator('.development-status')).to_contain_text('等待回答')
            sid = current_session(page)
            start = server.development.show(workspace, sid)
            stop = page.get_by_role('button', name='停止', exact=True)
            expect(stop).to_be_visible()
            for index, message in enumerate(('先核对输入', '临时补充', '再汇总输出'), 1):
                field.fill(message)
                expect(page.get_by_role('button', name='追加消息')).to_be_enabled()
                if index == 2:
                    page.get_by_role('button', name='追加消息').click()
                else:
                    field.press('Enter')
                expect(page.locator('.dc-queue-row')).to_have_count(index)
            page.reload()
            rows = page.locator('.dc-queue-row')
            expect(rows).to_have_count(3)
            rows.filter(has_text='临时补充').get_by_role('button', name='撤回').click()
            expect(rows).to_have_count(2)
            expect(stop).to_be_visible()
            assert server.development.store.show(workspace,sid)['revision'] == 1
        # The browser has closed. Only the server owns accepted messages.
        prompt = server.development.store.pending(start['turn']['id'])[0]
        server.development.input(workspace, root, sid, start['turn']['id'], prompt['id'],prompt['digest'],{'q1':'yes'})
        until = time.monotonic()+15
        while time.monotonic()<until:
            final = server.development.show(workspace,sid)
            if final['revision']==3 and final['turn']['cleanup']=='complete':
                break
            time.sleep(.1)
        assert final['revision']==3 and final['turn']['state']=='completed' and not final['queued_messages'], final
        page = server.development.store.events(workspace,sid,limit=50)
        messages = [e['data']['text'] for e in page['events'] if e['type']=='message.user']
        assert messages == ['input for queue test','先核对输入','再汇总输出']


def test_command_control_enter_steer_existing_turn_and_promote_queue(tmp_path):
    with conversation_app(tmp_path) as (server, workspace, root), browser_page(server) as (page, expect):
        field = page.get_by_label('开发要求')
        field.fill('hang for steering')
        field.press('Enter')
        expect(page.locator('.development-status')).to_contain_text('正在开发')
        sid = current_session(page)
        tid = server.development.show(workspace,sid)['turn']['id']
        for combo, message in [('Meta+Enter','先看输入表'),('Control+Enter','再核对版本')]:
            field.fill(message)
            field.press(combo)
            expect(page.locator('.dfc-user').filter(has_text=message)).to_contain_text('本轮已接收')
        field.fill('继续汇总')
        field.press('Enter')
        expect(page.locator('.dc-queue-row')).to_have_count(1)
        expect(field).to_be_focused()
        page.keyboard.press('Meta+Enter')
        expect(page.locator('.development-status')).to_have_attribute('data-turn-state', 'completed')
        expect(page.locator('.development-status')).to_have_attribute('data-turn-cleanup', 'complete')
        expect(page.locator('.development-status')).to_have_text('')
        expect(page.locator('.dc-queue-row')).to_have_count(0)
        expect(page.locator('.dfc-answer')).to_contain_text('继续汇总')
        page.reload()
        expect(page.locator('.dfc-user')).to_have_count(4)
        assert server.development.show(workspace,sid)['revision']==1
        events = server.development.store.events(workspace,sid)['events']
        assert {event['turn_id'] for event in events}=={tid}
        assert [event['data']['text'] for event in events if event['type']=='message.steer']==['先看输入表','再核对版本','继续汇总']


def test_explorer_order_empty_collapse_and_json_formatting(tmp_path):
    import subprocess
    with conversation_app(tmp_path, enable_config_write=True) as (server, workspace, root):
        (root / 'configs').mkdir(exist_ok=True)
        (root / 'configs' / 'compact.json').write_text('{"alpha":1,"nested":{"beta":true}}', encoding='utf-8')
        (root / 'workspace.json').write_text('{"name":"demo","budgets":{"cancel_grace_s":30,"max_batch_bytes":1024}}', encoding='utf-8')
        git = lambda *args: subprocess.run(['git', '-C', str(root), *args], check=True, capture_output=True)
        git('init')
        git('config', 'user.email', 'test@example.com')
        git('config', 'user.name', 'Test')
        git('add', '.')
        git('commit', '-m', 'init')
        # Modify an editable file so 待审核变更 initially has 1 pending change.
        (root / 'configs' / 'compact.json').write_text('{"alpha":2,"nested":{"beta":false}}', encoding='utf-8')

        with browser_page(server) as (page, expect):
            sidebar = page.locator('.development-session-sidebar')
            sidebar.get_by_role('button', name='文件').click()
            tree_sec = sidebar.locator('.dev-tree-section')
            changed_sec = sidebar.locator('.dev-changed-section')
            changed_toggle = changed_sec.locator('.dev-section-toggle')
            expect(tree_sec).to_be_visible()
            expect(changed_sec).to_be_visible()
            expect(changed_toggle).to_have_attribute('aria-expanded', 'true')
            # 项目目录 must appear above 待审核变更.
            tree_box = tree_sec.bounding_box()
            changed_box = changed_sec.bounding_box()
            assert tree_box['y'] < changed_box['y'], (tree_box, changed_box)

            # Approving all pending changes leaves count=0 and auto-collapses 待审核变更.
            sidebar.get_by_role('button', name='全部通过', exact=True).click()
            expect(changed_sec.locator('.dev-section-count').first).to_have_text('0')
            expect(changed_toggle).to_have_attribute('aria-expanded', 'false')
            expect(changed_sec.locator('.dev-changed-list')).to_be_hidden()

            # Read-only compact workspace.json auto-formats into multiple lines with 查看原文 toggle.
            sidebar.get_by_role('button', name='workspace.json').click()
            pane = page.get_by_role('complementary', name='代码面板')
            expect(pane.locator('.cm-editor')).to_be_visible()
            fmt_btn = pane.get_by_role('button', name='查看原文', exact=True)
            expect(fmt_btn).to_be_visible()
            expect(fmt_btn).to_have_attribute('aria-pressed', 'true')
            assert pane.locator('.cm-line').count() > 2
            fmt_btn.click()
            expect(pane.get_by_role('button', name='格式化', exact=True)).to_be_visible()
            assert pane.locator('.cm-line').count() == 1

            # Editable compact configs/compact.json formats in-place into unsaved draft.
            sidebar.get_by_role('button', name='configs', exact=True).click()
            sidebar.get_by_role('button', name='compact.json').click()
            pane.get_by_role('button', name='完整源码', exact=True).click()
            expect(pane.locator('.cm-editor')).to_be_visible()
            assert pane.locator('.cm-line').count() == 1
            pane.get_by_role('button', name='格式化', exact=True).click()
            assert pane.locator('.cm-line').count() > 2
            expect(pane.locator('.dev-code-status')).to_have_text('未保存')
            pane.get_by_role('button', name='还原', exact=True).click()
            assert pane.locator('.cm-line').count() == 1
