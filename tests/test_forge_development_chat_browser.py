import os
from pathlib import Path

import pytest
from test_forge_development_chat_http import app, legacy_history


def detailed_mode(page):
    page.get_by_label('对话显示模式', exact=True).click()
    page.get_by_role('button', name='Detailed mode', exact=True).click()


def test_browser_conversation_navigation_refresh_and_mobile(tmp_path):
    playwright=pytest.importorskip('playwright.sync_api')
    browser_path=Path('/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome')
    if not browser_path.is_file():pytest.skip('Installed Chromium unavailable')
    shots=Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(tmp_path / 'screenshots')));shots.mkdir(parents=True,exist_ok=True)
    with app(tmp_path) as (server,workspace,root), playwright.sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path=str(browser_path),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        context=browser.new_context(viewport={'width':1440,'height':1000});page=context.new_page();page.set_default_timeout(15000)
        errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        page.goto(server.origin+'/#token='+server.token)
        page.get_by_role('button',name='开发',exact=True).wait_for()
        assert page.locator('[data-page]').first.get_attribute('data-page')=='development'
        page.locator('[data-page=overview][aria-current=page]').wait_for()
        page.get_by_role('button',name='开发',exact=True).click()
        page.get_by_label('开发要求').fill('A small normal task')
        page.get_by_role('button',name='发送',exact=True).click()
        try: page.get_by_text('本轮已完成',exact=True).wait_for()
        except Exception:
            print('PAGE_ERRORS',errors);print('PAGE_TEXT',page.locator('body').inner_text()[:5000]);raise
        page.locator('.dfc-message').filter(has_text='fixture answer').wait_for()
        assert page.locator('.development-history').count()==0
        assert page.get_by_role('button',name='新对话',exact=True).count()==0
        playwright.expect(page.get_by_role('navigation',name='历史消息定位')).to_be_visible()
        page.get_by_role('button',name='定位消息：A small normal task',exact=True).wait_for()
        assert page.locator('.dfc-message script').count()==0
        page.screenshot(path=str(shots/'development-desktop.png'),full_page=True)
        sid=server.development.store.list(workspace)['sessions'][0]['id']
        assert server.development.store.show(workspace,sid)['revision']==1
        page.reload();page.get_by_text('本轮已完成',exact=True).wait_for()
        assert server.development.store.show(workspace,sid)['revision']==1
        page.get_by_label('开发要求').fill('draft kept')
        page.get_by_role('button',name='Readme',exact=True).click()
        assert page.evaluate('ForgeDevelopment.activeCount()')==0
        page.get_by_role('button',name='开发',exact=True).click();page.get_by_label('开发要求').wait_for()
        page.wait_for_url('**/*development_session*')
        page.locator('.dfc-message').filter(has_text='fixture answer').wait_for()
        assert page.get_by_label('开发要求').input_value()=='draft kept'
        for width in (390,320):
            page.set_viewport_size({'width':width,'height':850});page.screenshot(path=str(shots/f'development-{width}.png'),full_page=True)
            assert page.evaluate('document.documentElement.scrollWidth<=window.innerWidth'),width
            assert page.locator('.development-history').count()==0
            page.get_by_role('tab',name='Trajectory',exact=True).click()
            playwright.expect(page.get_by_role('navigation',name='版本时间线')).to_be_visible()
            page.get_by_role('tab',name='Chat',exact=True).click()
        second=page.context.new_page();second.goto(server.origin+'/#token='+server.token)
        second.get_by_role('button',name='开发',exact=True).click()
        second.locator('.dfc-message').filter(has_text='fixture answer').wait_for()
        assert second.evaluate('ForgeView.decodeView(location.hash).session_id')==sid
        assert len(server.development.store.list(workspace)['sessions'])==1
        second.get_by_label('更多对话操作',exact=True).click()
        second.get_by_role('button',name='归档历史',exact=True).click()
        second.get_by_text('归档处理完成',exact=True).wait_for()
        playwright.expect(second.locator('.dfc-user')).to_have_count(0)
        second.get_by_role('tab',name='Trajectory',exact=True).click()
        node=second.locator('.development-version-node[data-kind=archive]');node.wait_for()
        node.click();second.get_by_role('dialog').get_by_text('历史已归档',exact=True).wait_for()
        second.get_by_role('button',name='查看对应消息',exact=True).click()
        playwright.expect(second.get_by_label('开发要求')).to_be_disabled()
        second.get_by_role('button',name='回到最新记录',exact=True).click()
        playwright.expect(second.get_by_label('开发要求')).to_be_enabled()
        second.screenshot(path=str(shots/'project-archive-desktop.png'),full_page=True)
        second.reload();second.get_by_text('归档处理完成',exact=True).wait_for()
        playwright.expect(second.locator('.dfc-user')).to_have_count(0)
        second.get_by_role('button',name='定位消息：A small normal task',exact=True).click()
        second.locator('.dfc-user').filter(has_text='A small normal task').wait_for()
        assert server.development.store.show(workspace,sid)['revision']==2
        assert not errors,errors
        browser.close()


def test_acp_permission_diff_markdown_and_dialog_lifecycle(tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    browser_path = Path('/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome')
    if not browser_path.is_file(): pytest.skip('Installed Chromium unavailable')
    with app(tmp_path, acp=True) as (server, workspace, root), playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch(executable_path=str(browser_path), headless=True,
                                          args=['--no-sandbox', '--disable-dev-shm-usage'])
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.set_default_timeout(15000)
        errors = []; page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto(server.origin + '/#token=' + server.token)
        page.get_by_role('button', name='开发', exact=True).click()
        page.get_by_label('开发要求').fill('interactive edit fixture')
        page.get_by_role('button', name='发送', exact=True).click()
        permission = page.locator('.development-input')
        permission.get_by_text('操作许可', exact=True).wait_for()
        assert 'code.py' in permission.inner_text()
        assert '# fixture addition' in permission.inner_text()
        # Even the current turn's saved-message view is history-only: no
        # approval/input controls may mutate the background active session.
        page.get_by_role('button',name='定位消息：interactive edit fixture',exact=True).click()
        playwright.expect(permission).to_have_count(0)
        page.wait_for_timeout(1700)  # Covers an active poll while reading history.
        playwright.expect(permission).to_have_count(0)
        page.get_by_role('button',name='回到最新记录',exact=True).click()
        permission.get_by_role('button', name='允许一次').click()
        page.get_by_text('本轮已完成', exact=True).wait_for()
        page.get_by_role('heading', name='Fixture result', exact=True).wait_for()
        changed = page.locator('.dfc-system').filter(has_text='Changed files')
        changed.wait_for()
        page.get_by_role('tab',name='Trajectory',exact=True).click()
        page.locator('.development-version-node[data-kind=change]').wait_for()
        page.get_by_role('tab',name='Chat',exact=True).click()
        assert not changed.evaluate('(node)=>node.open')
        changed.locator('summary').click()
        assert '+# fixture addition' in changed.inner_text()
        assert (root / 'code.py').read_text() == '# initial dirty bytes\n# fixture addition\n'
        changed.get_by_role('button', name='View original').click()
        page.get_by_role('dialog').wait_for()
        playwright.expect(page.get_by_role('dialog')).to_contain_text('fixture addition')
        page.get_by_role('button', name='关闭', exact=True).click()
        page.locator('.development-info>summary').click()
        page.get_by_role('button', name='查看开发依据').click()
        page.get_by_role('dialog').wait_for()
        # Navigation can occur through a history event while a dialog is open.
        page.evaluate("() => {const v=ForgeView.decodeView(location.hash);history.pushState(null,'',ForgeView.encodeView({v:2,workspace:v.workspace,kind:'overview'}));dispatchEvent(new PopStateEvent('popstate'));}")
        page.wait_for_function('() => ForgeDevelopment.activeCount()===0')
        assert page.get_by_role('dialog').count() == 0
        assert not errors, errors
        browser.close()


def test_first_message_ime_idempotent_creation_and_stop(tmp_path):
    """A lost create response must not duplicate the session or send a turn twice."""
    import json
    from playwright.sync_api import sync_playwright, expect
    shots=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path/'screenshots')));shots.mkdir(parents=True,exist_ok=True)
    with app(tmp_path) as (server, workspace, root), sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(20000)
        creates=[];errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
        def lose_first_reply(route):
            creates.append(route.request.post_data_json)
            response=route.fetch()
            if len(creates)==1:route.fulfill(status=503,content_type='application/json',body=json.dumps({'error':'fixture lost create reply'}))
            else:route.fulfill(response=response)
        page.route('**/dev-session-create',lose_first_reply)
        page.goto(server.origin+'/#token='+server.token)
        page.get_by_role('button',name='开发',exact=True).click()
        model=page.get_by_role('button',name='开发代理',exact=True)
        model.click();menu=page.get_by_role('menu',name='选择模型')
        expect(menu.get_by_role('menuitemradio')).to_have_attribute('aria-checked','true')
        menu.get_by_role('menuitemradio').press('Escape');expect(menu).to_be_hidden();expect(model).to_be_focused()
        model.press('ArrowDown');menu.get_by_role('menuitemradio').press('Enter')
        expect(page.get_by_label('开发要求')).to_be_focused()
        page.locator('.dc-card').click(position={'x':120,'y':4})
        expect(page.get_by_label('开发要求')).to_be_focused()
        field=page.get_by_label('开发要求');field.fill('First message')
        for width in (1440,390,320):
            page.set_viewport_size({'width':width,'height':1000})
            page.screenshot(path=str(shots/f'development-composer-{width}.png'),full_page=True)
            model.click();expect(menu).to_be_visible()
            box=menu.bounding_box();assert box['x']>=0 and box['x']+box['width']<=width
            page.screenshot(path=str(shots/f'development-model-{width}.png'),full_page=True)
            menu.get_by_role('menuitemradio').press('Escape')
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        page.set_viewport_size({'width':1440,'height':1000})
        model.click();page.get_by_role('heading',name='今天想做些什么？',exact=True).click();expect(menu).to_be_hidden()
        field.dispatch_event('keydown',{'key':'Enter','code':'Enter','isComposing':True,'bubbles':True})
        field.evaluate("node=>{node.dispatchEvent(new CompositionEvent('compositionstart',{bubbles:true}));node.dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}));node.dispatchEvent(new CompositionEvent('compositionend',{bubbles:true}));node.dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}));}")
        assert not server.development.store.list(workspace)['sessions']
        field.press('Shift+Enter');field.press('s');expect(field).to_have_value('First message\ns')
        field.press('Enter')
        page.locator('.error-box').filter(has_text='fixture lost create reply').wait_for()
        expect(field).to_have_value('First message\ns')
        sessions=server.development.store.list(workspace)['sessions'];assert len(sessions)==1
        assert server.development.store.show(workspace,sessions[0]['id'])['revision']==0
        page.get_by_role('button',name='发送',exact=True).click()
        page.wait_for_url('**/*development_session*')
        page.get_by_text('本轮已完成',exact=True).wait_for()
        sessions=server.development.store.list(workspace)['sessions'];assert len(sessions)==1
        assert len(creates)==2 and creates[0]==creates[1]
        assert server.development.store.show(workspace,sessions[0]['id'])['revision']==1
        field.fill('hang');field.press('Enter')
        page.get_by_text('正在开发',exact=True).wait_for()
        expect(page.get_by_role('button',name='停止',exact=True)).to_be_enabled()
        page.screenshot(path=str(shots/'development-composer-running.png'),full_page=True)
        assert page.get_by_role('button',name='发送',exact=True).count()==0
        field.press('Escape');page.get_by_text('已停止',exact=True).wait_for()
        assert server.development.store.show(workspace,sessions[0]['id'])['turn']['cleanup']=='complete'
        assert not errors,errors
        browser.close()


def test_unconfigured_chat_is_compact_and_read_only(tmp_path):
    from playwright.sync_api import sync_playwright, expect
    shots=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path/'screenshots')));shots.mkdir(parents=True,exist_ok=True)
    with app(tmp_path) as (server, workspace, root), sync_playwright() as runtime:
        server.development.close();server.development=None
        browser=runtime.chromium.launch(executable_path='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(15000)
        writes=[];page.on('request',lambda request:writes.append(request.method) if request.method not in ('GET','HEAD') else None)
        waiting=[];page.route('**/dev-capabilities',lambda route:waiting.append(route))
        page.goto(server.origin+'/#token='+server.token)
        page.get_by_role('button',name='开发',exact=True).click()
        page.get_by_label('更多对话操作',exact=True).click()
        page.get_by_role('button',name='归档历史',exact=True).wait_for()
        expect(page.get_by_role('button',name='归档历史',exact=True)).to_be_disabled()
        page.get_by_label('更多对话操作',exact=True).click()
        assert len(waiting)==1
        waiting[0].fulfill(response=waiting[0].fetch())
        page.get_by_text('尚未连接开发代理',exact=True).wait_for()
        expect(page.get_by_label('开发代理')).to_be_hidden()
        expect(page.get_by_label('开发要求')).to_be_disabled()
        expect(page.get_by_role('button',name='发送',exact=True)).to_be_disabled()
        assert page.get_by_role('button',name='核对连接状态',exact=True).count()==0
        for width in (1440,390,320):
            page.set_viewport_size({'width':width,'height':850})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            box=page.locator('.development-composer').bounding_box();assert box['y']+box['height']<=850
            page.screenshot(path=str(shots/f'unconfigured-{width}.png'),full_page=True)
        page.locator('.development-info>summary').click()
        page.get_by_text('当前服务尚未启用项目开发。已有运行许可不会自动授予开发权限。',exact=True).wait_for()
        assert not writes
        browser.close()


def test_acp_stream_visible_before_completion_and_reconnect(tmp_path):
    """Observe real ACP deltas while the fixture is paused, then reload without replay duplication."""
    from playwright.sync_api import sync_playwright, expect
    with app(tmp_path,acp=True) as (server,workspace,root), sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(15000)
        errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        page.goto(server.origin+'/#token='+server.token)
        page.get_by_role('button',name='开发',exact=True).click()
        page.get_by_label('开发要求').fill('stream fixture')
        page.get_by_role('button',name='发送',exact=True).click()
        first=page.locator('.dfc-answer')
        expect(first).to_contain_text('First streamed paragraph.')
        sid=server.development.store.list(workspace)['sessions'][0]['id']
        assert server.development.store.show(workspace,sid)['turn']['cleanup']!='complete'
        expect(page.get_by_role('button',name='停止',exact=True)).to_be_enabled()
        expect(page.locator('.dfc-streaming')).to_have_count(1)
        page.reload();expect(first).to_contain_text('First streamed paragraph.')
        assert first.inner_text().count('First streamed paragraph.')==1
        for width in (1440,320):
            page.set_viewport_size({'width':width,'height':1000})
            # Match a painted layout after the responsive header and its
            # ResizeObserver have committed, keeping the exact viewport bound.
            page.evaluate('()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
            box=page.locator('.development-composer').bounding_box()
            assert box['y']+box['height']<=1000
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        timeline=page.locator('.development-timeline')
        assert timeline.evaluate('(e)=>e.scrollHeight>e.clientHeight')
        timeline.evaluate('(e)=>e.scrollTop=0')
        page.wait_for_timeout(100)

        failures=[]
        def disconnect(route):
            failures.append(1)
            if len(failures)==1:route.fulfill(status=503,content_type='application/json',body='{"error":"fixture stream disconnect"}')
            else:route.continue_()
        page.route('**/dev-events?*',disconnect)
        page.get_by_text('连接中断，正在重新连接…',exact=True).wait_for()
        (root/'release-stream').write_text('release')
        page.get_by_text('本轮已完成',exact=True).wait_for()
        expect(first).to_contain_text('Second streamed paragraph.')
        assert first.inner_text().count('First streamed paragraph.')==1
        assert timeline.evaluate('(e)=>e.scrollTop')<100
        page.get_by_role('button',name='有新内容，滚动到底部').click()
        page.wait_for_function("()=>{const e=document.querySelector('.development-timeline');return e.scrollHeight-e.scrollTop-e.clientHeight<60;}")

        assert first.inner_text().count('Second streamed paragraph.')==1
        expect(page.locator('.dfc-streaming')).to_have_count(0)
        expect(page.locator('.error-box')).to_have_count(0)
        assert len(failures)>=2
        page.get_by_role('button',name='Readme',exact=True).click()
        page.get_by_role('button',name='开发',exact=True).click()
        expect(first).to_contain_text('Second streamed paragraph.')
        assert first.inner_text().count('First streamed paragraph.')==1
        assert server.development.store.show(workspace,sid)['revision']==1
        assert not errors,errors
        browser.close()


@pytest.mark.parametrize('remote_compat',[False,True])
def test_legacy_same_message_ids_stay_in_turn_and_tool_order(tmp_path,remote_compat):
    """Old saved ACP IDs can repeat both around tools and after a CLI restart."""
    from playwright.sync_api import sync_playwright, expect
    with app(tmp_path,acp=True) as (server,workspace,root), sync_playwright() as runtime:
        sid,rows=legacy_history(server,workspace,root)
        before=server.development.store.events(workspace,sid)['events']
        if remote_compat:
            original=server.development.capabilities
            def remote_capabilities(*args):return dict(original(*args),backend='existing_http')
            server.development.capabilities=remote_capabilities
        browser=runtime.chromium.launch(executable_path='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(15000)
        errors=[];requests=[]
        page.on('pageerror',lambda e:errors.append(str(e)))
        page.on('request',lambda request:requests.append(request.url))
        page.goto(server.origin+'/#token='+server.token)
        page.get_by_role('button',name='开发',exact=True).click()
        expected=['第一轮进展','第一轮工具','第一轮结果','第二轮进展','第二轮工具','第二轮结果']
        def ordered():
            expect(page.locator('.dfc-answer')).to_have_count(4)
            expect(page.locator('.dfc-tool')).to_have_count(2)
            for index,text in enumerate(expected):
                expect(page.locator('.dfc-answer,.dfc-tool').nth(index)).to_contain_text(text)
            nodes=page.locator('.dfc-answer,.dfc-tool').evaluate_all('(nodes)=>nodes.map(node=>node.textContent)')
            assert len(nodes)==len(expected)
            assert all(text in node for text,node in zip(expected,nodes)),nodes
        page.get_by_text('本轮已完成',exact=True).wait_for();ordered()
        page.reload();page.get_by_text('本轮已完成',exact=True).wait_for();ordered()
        detailed_mode(page)
        final=page.locator('.dfc-answer').filter(has_text='第二轮结果')
        final.get_by_role('button',name='View original').click()
        dialog=page.get_by_role('dialog');expect(dialog).to_contain_text('第二轮结果')
        assert all(text not in dialog.inner_text() for text in ('第一轮','第二轮进展','第二轮工具'))
        expect(dialog.get_by_role('button',name='下一页')).to_be_hidden()
        if remote_compat:assert not any('/dev-output?' in value for value in requests)
        else:assert any('/dev-output?' in value and 'turn_id=' in value and 'through_sequence=' in value for value in requests)
        assert server.development.store.events(workspace,sid)['events']==before
        assert not errors,errors
        browser.close()


def test_public_acp_process_visible_while_running_and_after_reload(tmp_path):
    """Observe public provider text, tools and answer in actual streamed order."""
    from playwright.sync_api import sync_playwright, expect
    with app(tmp_path,acp=True) as (server,workspace,root), sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(15000)
        errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(server.origin+'/#token='+server.token)
        page.get_by_role('button',name='开发',exact=True).click()
        page.get_by_label('开发要求').fill('public process fixture')
        page.get_by_role('button',name='发送',exact=True).click()
        process=page.locator('.dfc-think')
        expect(process).to_contain_text('公开过程：先检查现有数据。')
        assert not process.evaluate('(node)=>node.open')
        expect(process.locator(':scope>div')).to_be_hidden()
        process.locator(':scope>summary').click()
        assert process.evaluate('(node)=>node.open')
        expect(page.locator('.dfc-answer')).to_contain_text('正在查找数据来源。')
        sid=server.development.store.list(workspace)['sessions'][0]['id']
        assert server.development.store.show(workspace,sid)['turn']['cleanup']!='complete'
        page.reload();expect(process).to_contain_text('公开过程：先检查现有数据。')
        assert not process.evaluate('(node)=>node.open')
        process.locator(':scope>summary').click()
        assert process.inner_text().count('公开过程：先检查现有数据。')==1
        (root/'release-process').write_text('release')
        page.get_by_text('本轮已完成',exact=True).wait_for()
        expect(page.locator('.dfc-answer')).to_have_count(2)
        nodes=page.locator('.dfc-think,.dfc-answer,.dfc-tool').evaluate_all('(nodes)=>nodes.map(node=>node.textContent)')
        expected=['公开过程：先检查现有数据。','正在查找数据来源。','读取输入信息','已找到输入，下一步可以开发过滤流程。']
        for index,text in enumerate(expected):
            expect(page.locator('.dfc-think,.dfc-answer,.dfc-tool').nth(index)).to_contain_text(text)
        nodes=page.locator('.dfc-think,.dfc-answer,.dfc-tool').evaluate_all('(nodes)=>nodes.map(node=>node.textContent)')
        assert len(nodes)==len(expected)
        assert all(text in node for text,node in zip(expected,nodes)),nodes
        expect(page.locator('.dfc-streaming')).to_have_count(0)
        page.reload();expect(page.locator('.dfc-answer')).to_have_count(2)
        assert not process.evaluate('(node)=>node.open')
        process.locator(':scope>summary').click()
        assert process.inner_text().count('公开过程：先检查现有数据。')==1
        assert not errors,errors
        browser.close()


def test_large_public_summary_and_final_keep_message_role_without_duplication(tmp_path):
    """Exercise the worker's bounded completion-body chunks, including legacy shape."""
    from playwright.sync_api import sync_playwright, expect
    with app(tmp_path) as (server,workspace,root), sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(15000)
        errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(server.origin+'/#token='+server.token)
        page.get_by_role('button',name='开发',exact=True).click()
        page.get_by_label('开发要求').fill('public summary fixture')
        page.get_by_role('button',name='发送',exact=True).click()
        page.get_by_text('本轮已完成',exact=True).wait_for()
        def check():
            process=page.locator('.dfc-think');answer=page.locator('.dfc-answer')
            expect(process).to_have_count(1);expect(answer).to_have_count(1)
            assert not process.evaluate('(node)=>node.open')
            expect(process.locator(':scope>div')).to_be_hidden()
            assert process.bounding_box()['height']<56
            process.locator(':scope>summary').click()
            expect(process).to_contain_text('Summary end.');expect(answer).to_contain_text('Final answer end.')
            assert process.inner_text().count('Public summary start.')==1
            assert answer.inner_text().count('Long final answer start.')==1
            # The started item and its output update share one native tool row.
            expect(page.locator('.dfc-tool')).to_have_count(1)
            actual_tool=page.locator('.dfc-tool').last
            actual_tool.locator(':scope>summary').click()
            expect(actual_tool.locator('.dfc-row-content')).to_contain_text('fixture inspection')
            expect(actual_tool.locator('.dfc-row-content')).to_contain_text('Actual tool output.')
            assert 'PRIVATE REASONING' not in page.locator('body').text_content()
        check();page.reload();page.get_by_text('本轮已完成',exact=True).wait_for();check()
        detailed_mode(page)
        page.locator('.dfc-answer').get_by_role('button',name='View original').click()
        dialog=page.get_by_role('dialog');expect(dialog).to_contain_text('Final answer end.')
        assert dialog.inner_text().count('Long final answer start.')==1
        sid=server.development.store.list(workspace)['sessions'][0]['id']
        assert 'PRIVATE REASONING' not in str(server.development.store.events(workspace,sid))
        assert not errors,errors
        browser.close()


def test_process_disclosure_streams_compactly_and_keeps_reader_choice(tmp_path):
    """The real HTTP event feed updates disclosure text without toggling its state."""
    from playwright.sync_api import sync_playwright, expect
    from demiflow.workbench.conversations.common import root_identity
    with app(tmp_path,acp=True) as (server,workspace,root), sync_playwright() as runtime:
        service=server.development;store=service.store
        sid=service.create_project(workspace,root,'compact-stream','deepseek')['id']
        turn,_=store.submit(workspace,sid,'stream',0,'开发一个过滤流程',root_identity(root),service.policy_hash)
        store.update(turn['id'],state='running',dispatch='acknowledged',delivery='acknowledged')
        process_text='**先查看数据结构**\n\n'+'检查现有字段和样例内容。\n'*40
        store.event(turn['id'],'message.delta',dict(item_id='process',channel='process',text=process_text))
        browser=runtime.chromium.launch(executable_path='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(15000)
        errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(server.origin+'/#token='+server.token)
        page.get_by_role('button',name='开发',exact=True).click()
        process=page.locator('.dfc-think');preview=process.locator('.dfc-summary')
        expect(preview).to_have_text('检查现有字段和样例内容。')
        assert not process.evaluate('(node)=>node.open')
        expect(process.locator(':scope>div')).to_be_hidden()
        assert process.bounding_box()['height']<56
        process.locator(':scope>summary').click()
        expect(process.locator(':scope>div')).to_be_visible()
        next_process='\n\n**已找到输入结构**\n下一步创建算子。';process_text+=next_process
        store.event(turn['id'],'message.delta',dict(item_id='process',channel='process',text=next_process))
        expect(process.locator(':scope>div')).to_contain_text('下一步创建算子。')
        assert process.evaluate('(node)=>node.open')
        process.locator(':scope>summary').click()
        expect(preview).to_have_text('已找到输入结构')
        long_line='正在核对一个很长的字段说明，'*40
        next_process='\n\n**'+long_line+'**\n仍在检查。';process_text+=next_process
        store.event(turn['id'],'message.delta',dict(item_id='process',channel='process',text=next_process))
        expect(preview).to_contain_text('正在核对一个很长的字段说明')
        assert not process.evaluate('(node)=>node.open')
        assert len(preview.inner_text())<=257
        page.set_viewport_size({'width':320,'height':850})
        assert preview.evaluate('(node)=>node.scrollWidth>node.clientWidth')
        assert process.bounding_box()['height']<56
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        shots=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path/'screenshots')));shots.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(shots/'development-process-compact.png'),full_page=True)
        process.locator(':scope>summary').click()
        store.event(turn['id'],'message.completed',dict(item_id='process',channel='process',text=process_text,interrupted=True,status='incomplete'))
        expect(process.locator(':scope>summary')).to_contain_text('Incomplete')
        store.event(turn['id'],'message.delta',dict(item_id='answer',text='开始开发过滤流程。'))
        answer=page.locator('.dfc-answer');expect(answer).to_contain_text('开始开发过滤流程。')
        store.event(turn['id'],'message.delta',dict(item_id='answer',text='正在补齐算子。'))
        expect(answer).to_contain_text('正在补齐算子。')
        assert process.evaluate('(node)=>node.open')
        assert store.turn(turn['id'])['cleanup']!='complete'
        # Native block-end and canonical commit may both complete the same item.
        for _ in range(2):store.event(turn['id'],'message.completed',dict(item_id='answer',text='开始开发过滤流程。正在补齐算子。'))
        store.finish_provider(turn['id'],state='completed',cleanup='complete',result={})
        page.get_by_text('本轮已完成',exact=True).wait_for()
        assert process.evaluate('(node)=>node.open')
        process.get_by_role('button',name='View original').click()
        dialog=page.get_by_role('dialog');expect(dialog).to_contain_text('已找到输入结构')
        expect(dialog).to_contain_text('仍在检查。')
        assert dialog.inner_text().count('先查看数据结构')==1
        page.get_by_role('button',name='关闭',exact=True).click()
        detailed_mode(page)
        answer.get_by_role('button',name='View original').click()
        expect(dialog).to_contain_text('开始开发过滤流程。正在补齐算子。')
        assert dialog.inner_text().count('开始开发过滤流程。')==1
        page.get_by_role('button',name='关闭',exact=True).click()
        page.reload();expect(answer).to_contain_text('正在补齐算子。')
        expect(process.locator(':scope>div')).to_be_hidden()
        assert not process.evaluate('(node)=>node.open')
        expect(preview).to_have_text('先查看数据结构')
        expect(process.locator(':scope>summary')).to_contain_text('Incomplete')
        expect(page.locator('.dfc-streaming')).to_have_count(0)
        assert not errors,errors
        browser.close()


from test_forge_chat_markdown_browser import chat_page


def test_disposed_first_send_cannot_reopen_old_project(chat_page):
    """A successful late response clears its sent draft, without grabbing the route."""
    page=chat_page
    page.evaluate('''async () => {
      window.routeOpens=[];window.startCalls=0;window.oldMain=document.createElement('main');
      oldMain.id='late-send-fixture';document.body.append(oldMain);
      const capabilities={enabled:true,start:true,providers:[{id:'fixture',model:'Fixture',available:true}]};
      const api=async(name)=>{
        if(name==='dev-capabilities')return capabilities;
        if(name==='dev-project-turns')return {turns:[],next_offset:null};
        if(name==='dev-conversation')return {session:null};
        if(name==='dev-session-create')return {id:'old-session',provider:'fixture',revision:0};
        if(name==='dev-turn-start'){startCalls++;return new Promise(resolve=>window.releaseOldSend=resolve);}
        throw new Error('Unexpected route '+name);
      };
      await ForgeDevelopment.mount({main:oldMain,api,workspaceId:'old-project',view:{},
        disposeReaders(){},openSession:sid=>routeOpens.push(sid),error(){},register:value=>window.oldHandle=value});
    }''')
    page.locator('#late-send-fixture textarea').fill('A request already accepted by the server')
    page.locator('#late-send-fixture .dc-send').click()
    page.wait_for_function('() => !!window.releaseOldSend')
    page.evaluate("() => {oldHandle.dispose();oldMain.remove();releaseOldSend({id:'old-turn'});}")
    page.wait_for_function("() => !JSON.parse(sessionStorage.getItem('forge-development-drafts')).some(([key])=>key==='old-project:project')")
    assert page.evaluate('routeOpens')==[]
    assert page.evaluate('startCalls')==1
    assert page.evaluate('ForgeDevelopment.activeCount()')==0
