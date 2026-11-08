import os
from pathlib import Path

import pytest
from test_forge_development_chat_http import app


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
        page.locator('.development-message').filter(has_text='fixture answer').wait_for()
        assert page.locator('.development-history').count()==0
        assert page.get_by_role('button',name='新对话',exact=True).count()==0
        playwright.expect(page.get_by_role('navigation',name='历史消息定位')).to_be_visible()
        page.get_by_role('button',name='定位消息：A small normal task',exact=True).wait_for()
        assert page.locator('.development-message script').count()==0
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
        page.locator('.development-message').filter(has_text='fixture answer').wait_for()
        assert page.get_by_label('开发要求').input_value()=='draft kept'
        for width in (390,320):
            page.set_viewport_size({'width':width,'height':850});page.screenshot(path=str(shots/f'development-{width}.png'),full_page=True)
            assert page.evaluate('document.documentElement.scrollWidth<=window.innerWidth'),width
            assert page.locator('.development-history').count()==0
            assert page.get_by_role('navigation',name='版本时间线').count()==1
        second=page.context.new_page();second.goto(server.origin+'/#token='+server.token)
        second.get_by_role('button',name='开发',exact=True).click()
        second.locator('.development-message').filter(has_text='fixture answer').wait_for()
        assert second.evaluate('ForgeView.decodeView(location.hash).session_id')==sid
        assert len(server.development.store.list(workspace)['sessions'])==1
        second.get_by_role('button',name='归档历史',exact=True).click()
        second.get_by_text('归档处理完成',exact=True).wait_for()
        playwright.expect(second.locator('.development-user')).to_have_count(0)
        node=second.locator('.development-version-node[data-kind=archive]');node.wait_for()
        node.click();second.get_by_role('dialog').get_by_text('历史已归档',exact=True).wait_for()
        second.get_by_role('button',name='查看对应消息',exact=True).click()
        playwright.expect(second.get_by_label('开发要求')).to_be_disabled()
        second.get_by_role('button',name='回到最新记录',exact=True).click()
        playwright.expect(second.get_by_label('开发要求')).to_be_enabled()
        second.screenshot(path=str(shots/'project-archive-desktop.png'),full_page=True)
        second.reload();second.get_by_text('归档处理完成',exact=True).wait_for()
        playwright.expect(second.locator('.development-user')).to_have_count(0)
        second.get_by_role('button',name='定位消息：A small normal task',exact=True).click()
        second.locator('.development-user').filter(has_text='A small normal task').wait_for()
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
        permission.get_by_role('button', name='允许一次').click()
        page.get_by_text('本轮已完成', exact=True).wait_for()
        page.get_by_role('heading', name='Fixture result', exact=True).wait_for()
        changed = page.locator('.development-message').filter(has_text='观察到的变化')
        changed.wait_for()
        page.locator('.development-version-node[data-kind=change]').wait_for()
        assert not changed.evaluate('(node)=>node.open')
        changed.locator('summary').click()
        assert '+# fixture addition' in changed.inner_text()
        assert (root / 'code.py').read_text() == '# initial dirty bytes\n# fixture addition\n'
        changed.get_by_role('button', name='分页查看原文').click()
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
        field=page.get_by_label('开发要求');field.fill('First message')
        field.dispatch_event('keydown',{'key':'Enter','code':'Enter','isComposing':True,'bubbles':True})
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
        page.get_by_role('button',name='归档历史',exact=True).wait_for()
        expect(page.get_by_role('button',name='归档历史',exact=True)).to_be_disabled()
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
        first=page.locator('.development-assistant')
        expect(first).to_contain_text('First streamed paragraph.')
        sid=server.development.store.list(workspace)['sessions'][0]['id']
        assert server.development.store.show(workspace,sid)['turn']['cleanup']!='complete'
        expect(page.get_by_role('button',name='停止',exact=True)).to_be_enabled()
        expect(page.locator('.development-streaming')).to_have_count(1)
        page.reload();expect(first).to_contain_text('First streamed paragraph.')
        assert first.inner_text().count('First streamed paragraph.')==1
        for width in (1440,320):
            page.set_viewport_size({'width':width,'height':1000})
            box=page.locator('.development-composer').bounding_box();assert box['y']+box['height']<=1000
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
        expect(page.locator('.development-streaming')).to_have_count(0)
        expect(page.locator('.error-box')).to_have_count(0)
        assert len(failures)>=2
        page.get_by_role('button',name='Readme',exact=True).click()
        page.get_by_role('button',name='开发',exact=True).click()
        expect(first).to_contain_text('Second streamed paragraph.')
        assert first.inner_text().count('First streamed paragraph.')==1
        assert server.development.store.show(workspace,sid)['revision']==1
        assert not errors,errors
        browser.close()
