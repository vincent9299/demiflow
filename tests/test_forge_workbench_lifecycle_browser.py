"""Startup reads and replaced readers must respect the current visible view."""
import pytest
from playwright.sync_api import expect

from test_forge_project_checks import project,demo,isolated
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workbench_browser import page_for
from test_forge_web_redesign import project_info


@pytest.mark.parametrize('late_error',[False,True])
def test_late_startup_metadata_cannot_replace_user_pipeline_selection(isolated,browser,monkeypatch,tmp_path,late_error):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    with service(isolated) as app:
        context=browser.new_context(viewport=dict(width=1440,height=1000))
        page=context.new_page();page.set_default_timeout(65000)
        errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        page.add_init_script('''{
          const native=fetch.bind(window);window.__heldInitialChange=false;
          window.fetch=async(...args)=>{
            const response=await native(...args),url=new URL(String(args[0]),location.href);
            if(url.pathname.endsWith('/changes')&&!window.__heldInitialChange){
              window.__heldInitialChange=true;
              return new Promise((resolve,reject)=>{window.__releaseInitialChange=fail=>fail?reject(new Error('owned old startup failure')):resolve(response);});
            }
            return response;
          };
        }''')
        try:
            page.goto(app.origin+'/#token='+app.token)
            page.wait_for_function('() => typeof window.__releaseInitialChange==="function"')
            page.get_by_role('button',name='管线',exact=True).click()
            page.locator('.wb-node').first.wait_for()
            page.wait_for_function('() => ForgeView.decodeView(location.hash)?.kind==="flows"')
            before=page.evaluate('ForgeView.decodeView(location.hash)')
            page.evaluate('fail=>window.__releaseInitialChange(fail)',late_error)
            page.wait_for_timeout(150)
            expect(page.locator('.workbench')).to_be_visible()
            expect(page.get_by_role('button',name='管线',exact=True)).to_have_attribute('aria-current','page')
            assert page.evaluate('ForgeView.decodeView(location.hash)')==before
            expect(page.locator('main')).not_to_contain_text('owned old startup failure')
            assert not errors,errors
        finally:context.close()


def test_project_detail_refresh_disposes_previous_text_readers(project,browser,monkeypatch,tmp_path):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    with service(project) as app,page_for(browser,app) as (page,errors):
        page.evaluate('''() => {
          const native=ForgeReading;window.__metadataLive=0;window.__metadataDisposed=0;
          window.ForgeReading=Object.freeze({...native,createTextReader:options=>{
            const reader=native.createTextReader(options);
            const tracked=options.text.includes('"coverage"')&&options.text.includes('"evidence"');
            if(!tracked)return reader;
            let disposed=false;window.__metadataLive++;
            return {...reader,dispose:()=>{
              if(!disposed){disposed=true;window.__metadataLive--;window.__metadataDisposed++;}
              reader.dispose();
            }};
          }});
        }''')
        project_info(page)
        for index in range(3):
            page.get_by_text('识别范围与内容摘要',exact=True).click()
            page.wait_for_function('() => window.__metadataLive===1')
            page.get_by_role('button',name='刷新当前视图',exact=True).click()
            expect(page.get_by_role('button',name='刷新当前视图',exact=True)).to_be_enabled(timeout=65000)
            page.wait_for_function('() => window.__metadataLive===0')
            assert page.evaluate('window.__metadataDisposed')==index+1
        assert not errors,errors


def test_unified_three_tab_nav_and_three_column_pipeline_workspace(project,browser,monkeypatch,tmp_path):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    with service(project) as app,page_for(browser,app) as (page,errors):
        nav = page.locator('.function-nav')
        expect(nav.get_by_role('button')).to_have_text(['管线', '数据', '文档'])
        nav.get_by_role('button', name='管线', exact=True).click()
        expect(page.locator('.pw-workspace')).to_be_visible()
        # Left sidebar exposes 管线 | 会话 | 文件 on one single row.
        sidebar = page.locator('.pw-sidebar')
        tab_ys = []
        for label in ('管线', '会话', '文件'):
            btn = sidebar.get_by_role('button', name=label, exact=True)
            expect(btn).to_be_visible()
            tab_ys.append(round(btn.bounding_box()['y']))
        assert max(tab_ys) - min(tab_ys) <= 2, f'sidebar tabs wrapped: {tab_ys}'
        expect(page.locator('.wb-node').first).to_be_visible(timeout=30000)
        # Center DAG canvas fills the full right-hand width by default.
        center_box = page.locator('.pw-center').bounding_box()
        graph_box = page.locator('.wb-graph').bounding_box()
        assert graph_box['width'] >= center_box['width'] * 0.85, (graph_box, center_box)
        # Clicking a DAG node opens the right supplementary panel on 节点详情 (no modal dialog).
        page.locator('.wb-node').first.click()
        expect(page.locator('.pw-drawer')).to_be_visible()
        expect(page.locator('.pw-drawer-pane[data-pane="detail"]')).to_be_visible()
        expect(page.get_by_role('dialog')).to_have_count(0)
        # Clicking 关闭详情 cleanly closes the right supplementary panel without bouncing back.
        page.locator('.pw-drawer-pane[data-pane="detail"]').get_by_role('button', name='关闭详情', exact=True).first.click()
        expect(page.locator('.pw-drawer')).to_be_hidden()
        # Clicking 会话 switches center stage to wide assistant and hides canvas.
        sidebar.get_by_role('button', name='会话', exact=True).click()
        expect(page.locator('.pw-assistant-host')).to_be_visible()
        expect(page.locator('.pw-workbench-host')).to_be_hidden()
        # Switching back to 管线 restores the wide canvas.
        sidebar.get_by_role('button', name='管线', exact=True).click()
        expect(page.locator('.pw-workbench-host')).to_be_visible()
        expect(page.locator('.pw-assistant-host')).to_be_hidden()
        assert not errors, errors
