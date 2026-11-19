from pathlib import Path
import pytest
from test_forge_development_chat_http import app
from demiflow.workbench.projects import registry as console


def test_global_editable_settings_inheritance_draft_and_access_control(tmp_path):
    playwright=pytest.importorskip('playwright.sync_api')
    binary=Path('/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome')
    if not binary.is_file():pytest.skip('Chromium unavailable')
    with app(tmp_path) as (server,w,root),playwright.sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path=str(binary),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        page=browser.new_page(viewport={'width':1440,'height':1000});page.set_default_timeout(15000)
        errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(server.origin+'/#token='+server.token)
        page.get_by_role('button',name='开发',exact=True).click()
        draft=page.get_by_label('开发要求');draft.fill('未发送的真实草稿')
        page.get_by_role('button',name='设置',exact=True).click()
        dialog=page.get_by_role('dialog',name='设置',exact=True)
        dialog.get_by_label('模型',exact=True).fill('global-fixture')
        dialog.get_by_label('服务地址').fill('https://example.invalid/v1')
        dialog.get_by_label('API Key').fill('browser-fixture-secret')
        dialog.get_by_role('button',name='保存并应用').click()
        playwright.expect(dialog.get_by_role('status')).to_contain_text('已保存，所有项目')
        playwright.expect(dialog.get_by_label('API Key')).to_have_value('')
        assert 'browser-fixture-secret' not in page.locator('body').inner_text()
        page.screenshot(path=str(tmp_path/'global-ai-settings.png'),full_page=True)
        dialog.get_by_role('button',name='关闭',exact=True).click()
        playwright.expect(draft).to_have_value('未发送的真实草稿')
        playwright.expect(page.locator('.dc-model-name')).to_have_text('global-fixture')
        # The shield continues to show access, and opens its own permission dialog.
        page.locator('.dc-access').click()
        permission=page.get_by_role('dialog',name='当前开发权限',exact=True)
        playwright.expect(permission).to_contain_text('可信本地执行')
        assert page.get_by_role('dialog',name='设置',exact=True).count()==0
        permission.get_by_role('button',name='关闭').click()
        other=tmp_path/'another';other.mkdir();(other/'main.py').write_text('x=1')
        added=console.add(server.registry,other)
        assert server.development.capabilities(added['workspace_id'],other)['enabled']
        page.reload();page.get_by_role('button',name='设置',exact=True).click()
        dialog=page.get_by_role('dialog',name='设置',exact=True)
        playwright.expect(dialog.get_by_label('模型',exact=True)).to_have_value('global-fixture')
        assert dialog.get_by_label('API Key').input_value()==''
        page.set_viewport_size({'width':390,'height':850})
        assert dialog.evaluate('e=>e.scrollWidth<=e.clientWidth')
        assert not errors
        browser.close()
