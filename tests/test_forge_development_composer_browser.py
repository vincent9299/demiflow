"""Native-style composer behavior independent of the conversation controller."""
from pathlib import Path

import pytest


WEB=Path(__file__).parents[1]/'demiflow/workbench/web'
CHROME=Path('/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome')
CAPABILITIES=dict(enabled=True,start=True,mode='restricted',providers=[
    dict(id='deepseek',model='deepseek-v4-flash',effort='high',available=True),
    dict(id='codex',model='gpt-5.5',available=True),
    dict(id='offline',model='unavailable-model',available=False)])
STATS=dict(format='demiflow_development_stats_v1',coverage=dict(state='complete',metrics_state='complete'),
    turns=7,turns_basis='conversation_requests',steps=19,decode_tokens_per_second=45.8,
    tokens=dict(total=5000),cache_hit_ratio=.5,context=dict(used=25000,size=100000),unavailable=[])


@pytest.fixture
def page():
    playwright=pytest.importorskip('playwright.sync_api')
    if not CHROME.is_file():pytest.skip('Installed Chromium unavailable')
    with playwright.sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path=str(CHROME),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        context=browser.new_context(viewport=dict(width=1200,height=800));page=context.new_page()
        errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        page.set_content('<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><main id="host"></main><button id="outside">Outside</button></body></html>')
        # Include the legacy sheet to catch accidental retention of the old grey
        # card or global textarea/button styling in the integrated application.
        page.add_style_tag(path=str(WEB/'development.css'))
        page.add_style_tag(path=str(WEB/'development-composer.css'))
        page.add_style_tag(content='body{margin:0;background:white;font-family:Arial,sans-serif}#host{box-sizing:border-box;width:100%;max-width:852px;margin:450px auto 0;padding:0 16px}#outside{margin:16px}')
        page.add_script_tag(path=str(WEB/'development-composer.js'))
        page.evaluate('''caps=>{
          window.events={providers:[],references:0,access:0,inputs:0,submits:0,stops:0};
          window.composer=ForgeDevelopmentComposer.create({
            onProviderChange:id=>events.providers.push(id),
            onReference:()=>{events.references++;},onInspectAccess:()=>{events.access++;}
          });document.querySelector('#host').append(composer.form);
          composer.input.addEventListener('input',()=>events.inputs++);
          composer.form.onsubmit=e=>{e.preventDefault();events.submits++;};
          composer.stop.onclick=()=>events.stops++;
          composer.update({capabilities:caps});composer.resize();
        }''',CAPABILITIES)
        yield page
        assert not errors,errors
        browser.close()


def test_native_white_card_complete_controls_and_only_real_stats(page,tmp_path):
    from playwright.sync_api import expect
    card=page.locator('.dc-card')
    facts=card.evaluate('(node)=>({bg:getComputedStyle(node).backgroundColor,radius:getComputedStyle(node).borderRadius,border:getComputedStyle(node).borderTopWidth,height:node.getBoundingClientRect().height})')
    assert facts['bg']=='rgb(255, 255, 255)' and facts['radius']=='24px' and facts['border']=='1px'
    assert 95<=facts['height']<=104
    expect(page.locator('.dc-model-name')).to_have_text('DeepSeek-V4-Flash')
    expect(page.locator('.dc-effort')).to_have_text('High')
    expect(page.locator('.dc-access-label')).to_have_text('Managed access')
    assert page.get_by_text('Full access',exact=True).count()==0
    expect(page.locator('.dc-dock')).to_be_hidden()
    page.evaluate('stats=>composer.update({stats})',STATS)
    expect(page.locator('.dc-dock')).to_contain_text('7 turns 19 steps · 45.8 tok/s')
    expect(page.locator('.dc-dock')).to_contain_text('5K tok · Cache hit 50%')
    expect(page.get_by_label('上下文已用 25%',exact=True)).to_be_visible()
    assert '工作台会话轮数' in page.locator('.dc-stat').first.get_attribute('title')
    page.locator('.dc-access').click();assert page.evaluate('events.access')==1
    page.screenshot(path=str(tmp_path/'composer-native-desktop.png'),full_page=True)


def test_model_and_add_menu_keyboard_focus_and_optional_reference(page):
    from playwright.sync_api import expect
    model=page.get_by_role('button',name='开发代理',exact=True)
    model.press('ArrowDown');menu=page.get_by_role('menu',name='选择模型')
    expect(menu.get_by_role('menuitemradio',name='DeepSeek-V4-Flash',exact=True)).to_be_focused()
    expect(menu.get_by_role('menuitemradio',name='DeepSeek-V4-Flash',exact=True)).to_have_attribute('aria-checked','true')
    menu.get_by_role('menuitemradio',name='DeepSeek-V4-Flash',exact=True).press('ArrowDown')
    expect(menu.get_by_role('menuitemradio',name='gpt-5.5',exact=True)).to_be_focused()
    menu.get_by_role('menuitemradio',name='gpt-5.5',exact=True).press('Enter')
    expect(page.get_by_label('开发要求')).to_be_focused()
    assert page.evaluate('events.providers')==['codex']
    expect(page.locator('.dc-model-name')).to_have_text('gpt-5.5')
    expect(page.locator('.dc-effort')).to_be_hidden()
    model.click();menu.get_by_role('menuitemradio',name='gpt-5.5',exact=True).press('Escape')
    expect(menu).to_be_hidden();expect(model).to_be_focused()
    add=page.get_by_role('button',name='添加内容',exact=True)
    add.press('ArrowDown');plus=page.get_by_role('menu',name='添加内容')
    expect(plus.get_by_role('menuitem',name='引用文件/目录')).to_be_focused()
    plus.get_by_role('menuitem',name='引用文件/目录').press('Enter')
    assert page.evaluate('events.references')==1 and page.get_by_label('开发要求').input_value()==''
    add.click();page.locator('#outside').click();expect(plus).to_be_hidden()
    page.locator('.dc-card').click(position=dict(x=120,y=2));expect(page.get_by_label('开发要求')).to_be_focused()


def test_utf8_attachment_adds_named_text_to_draft_and_fires_existing_input_hook(page):
    from playwright.sync_api import expect
    field=page.get_by_label('开发要求');field.fill('保留原草稿')
    previous=page.evaluate('events.inputs')
    page.locator('.dc-text-file').set_input_files(dict(name='notes.txt',mimeType='text/plain',buffer='真实文本\n第二行'.encode()))
    expect(page.locator('.dc-notice')).to_contain_text('已将 notes.txt 的文本加入草稿')
    assert field.input_value()=='保留原草稿\n\n--- 文本附件：notes.txt ---\n真实文本\n第二行\n--- 文本附件结束 ---'
    assert page.evaluate('events.inputs')==previous+1
    expect(field).to_be_focused();expect(page.get_by_role('button',name='发送',exact=True)).to_be_enabled()
    assert page.evaluate('events.submits')==0


@pytest.mark.parametrize('filename,mime,data,error',[
    ('photo.png','image/png',b'fake-image','暂不支持图片'),
    ('invalid.txt','text/plain',b'\xff','有效的 UTF-8'),
    ('binary.txt','text/plain',b'a\x00b','二进制内容'),
    ('large.txt','text/plain',b'x'*(24*1024+1),'不能超过 24 KiB'),
])
def test_unsupported_attachment_does_not_fake_upload_or_change_draft(page,filename,mime,data,error):
    from playwright.sync_api import expect
    field=page.get_by_label('开发要求');field.fill('existing draft')
    page.locator('.dc-text-file').set_input_files(dict(name=filename,mimeType=mime,buffer=data))
    expect(page.get_by_role('alert')).to_contain_text(error)
    assert field.input_value()=='existing draft' and page.evaluate('events.submits')==0


def test_attachment_total_budget_and_filename_remain_plain_text(page):
    from playwright.sync_api import expect
    field=page.get_by_label('开发要求');field.fill('x'*(15*1024))
    page.locator('.dc-text-file').set_input_files(dict(name='small.txt',mimeType='text/plain',buffer=b'y'*(18*1024)))
    expect(page.get_by_role('alert')).to_contain_text('超过 32 KiB')
    assert field.input_value()=='x'*(15*1024)
    field.fill('safe')
    page.locator('.dc-text-file').set_input_files(dict(name='<img src=x onerror=alert(1)>.txt',mimeType='text/plain',buffer=b'<script>bad()</script>'))
    expect(page.locator('.dc-notice')).to_contain_text('已将')
    assert '<script>bad()</script>' in field.input_value()
    assert page.locator('.dc-notice img,.dc-notice script').count()==0


def test_busy_readonly_history_and_parent_keyboard_ownership(page):
    from playwright.sync_api import expect
    field=page.get_by_label('开发要求');field.fill('hello')
    # The host alone owns IME/Enter/Escape. This component must not secretly
    # submit on keydown, then submit again when the host installs its handler.
    field.dispatch_event('keydown',dict(key='Enter',isComposing=True,bubbles=True))
    assert page.evaluate('events.submits')==0
    page.evaluate('''()=>{composer.input.onkeydown=e=>{if(e.key==='Enter'&&!e.isComposing){e.preventDefault();composer.form.requestSubmit();}};}''')
    field.press('Enter');assert page.evaluate('events.submits')==1
    page.evaluate("composer.update({session:{provider:'deepseek',turn:{cleanup:'pending',state:'running'}}})")
    expect(page.get_by_role('button',name='停止',exact=True)).to_be_enabled()
    expect(page.get_by_role('button',name='发送',exact=True)).to_be_hidden()
    expect(page.get_by_role('button',name='开发代理',exact=True)).to_be_disabled()
    expect(field).to_be_enabled()
    page.get_by_role('button',name='停止',exact=True).click();assert page.evaluate('events.stops')==1
    page.evaluate("composer.update({historyOnly:true})")
    expect(page.locator('.dc-stop')).to_be_hidden();expect(page.locator('.dc-stop')).to_be_disabled()
    expect(page.get_by_role('button',name='发送',exact=True)).to_be_disabled()
    expect(page.get_by_role('button',name='开发代理',exact=True)).to_be_disabled()
    page.evaluate("composer.update({session:null,historyOnly:true})")
    expect(field).to_be_disabled();expect(page.locator('.dc-access-label')).to_have_text('Read only')
    page.evaluate("composer.update({historyOnly:false,sending:true})")
    expect(field).to_be_disabled();expect(page.get_by_role('button',name='添加内容',exact=True)).to_be_disabled()


def test_stats_unknown_partial_and_observed_zero_have_distinct_display(page):
    from playwright.sync_api import expect
    page.evaluate("composer.update({stats:{coverage:{state:'complete'},turns:3,steps:null,tokens:{total:null},cache_hit_ratio:null,context:{used:null,size:null},decode_tokens_per_second:null}})")
    expect(page.locator('.dc-dock')).to_have_text('3 turns')
    page.evaluate("composer.update({stats:{coverage:{state:'partial'},turns:null}})")
    expect(page.locator('.dc-dock')).to_have_text('正在读取会话统计…')
    page.evaluate("composer.update({stats:{coverage:{state:'complete',metrics_state:'partial'},turns:0,turns_basis:'conversation_requests',tokens:{total:0},cache_hit_ratio:0,context:{used:0,size:1000}}})")
    expect(page.locator('.dc-dock')).to_contain_text('0 turns')
    expect(page.locator('.dc-dock')).to_contain_text('0 tok · Cache hit 0%')
    expect(page.get_by_label('上下文已用 0%',exact=True)).to_be_visible()
    assert '仅包含有统计记录的轮次' in page.locator('.dc-dock').get_attribute('title')
    for title in page.locator('.dc-stat,.dc-context').evaluate_all('(nodes)=>nodes.map(node=>node.title)'):
        assert '仅包含有统计记录的轮次' in title
    page.evaluate("composer.update({stats:{session_id:'stale-session',coverage:{state:'complete'},turns:99}})")
    expect(page.locator('.dc-dock')).to_be_hidden()


@pytest.mark.parametrize('width',[390,320])
def test_narrow_composer_menus_and_footer_stay_inside_viewport(page,tmp_path,width):
    from playwright.sync_api import expect
    page.set_viewport_size(dict(width=width,height=850))
    page.evaluate('stats=>composer.update({stats})',STATS)
    page.get_by_label('开发要求').fill('移动设备草稿')
    for label,menu_label in [('开发代理','选择模型'),('添加内容','添加内容')]:
        trigger=page.get_by_role('button',name=label,exact=True);trigger.click()
        menu=page.get_by_role('menu',name=menu_label);expect(menu).to_be_visible()
        box=menu.bounding_box();assert box['x']>=0 and box['x']+box['width']<=width and box['y']>=0
        menu.locator('button:not(:disabled)').first.press('Escape')
    assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
    send=page.get_by_role('button',name='发送',exact=True).bounding_box()
    assert send['x']>=0 and send['x']+send['width']<=width
    page.screenshot(path=str(tmp_path/f'composer-native-{width}.png'),full_page=True)


def test_dispose_removes_menus_and_does_not_keep_document_actions_alive(page):
    from playwright.sync_api import expect
    page.get_by_role('button',name='添加内容',exact=True).click()
    page.evaluate('composer.dispose()')
    expect(page.get_by_role('menu',name='添加内容')).to_be_hidden()
    page.get_by_role('button',name='添加内容',exact=True).click()
    expect(page.get_by_role('menu',name='添加内容')).to_be_hidden()
    assert page.evaluate('events.references')==0


def test_connection_distinguishes_shared_configuration_from_project_permission(page):
    from playwright.sync_api import expect
    cases=[
        (dict(enabled=False,start=False,providers=[]),'尚未配置开发代理'),
        (dict(enabled=False,start=False,providers=[dict(id='deepseek',model='test',available=False)]),'本项目未启用开发'),
        (dict(enabled=False,start=False,service_configured=True,project_authorized=True,providers=[]),'开发代理暂不可用'),
    ]
    for caps,label in cases:
        page.evaluate('caps=>composer.update({capabilities:caps})',caps)
        assert page.evaluate('caps=>ForgeDevelopmentComposer.connectionStatus(caps).label',caps)==label
        expect(page.locator('.dc-access-label')).to_have_text('Project access')
        expect(page.locator('.dc-send')).to_be_disabled()
        page.locator('.dc-access').click()
    assert page.evaluate('events.access')==3
