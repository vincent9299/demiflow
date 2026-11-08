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
            page.get_by_role('button',name='管道',exact=True).click()
            page.locator('.wb-node').first.wait_for()
            page.wait_for_function('() => ForgeView.decodeView(location.hash)?.kind==="flows"')
            before=page.evaluate('ForgeView.decodeView(location.hash)')
            page.evaluate('fail=>window.__releaseInitialChange(fail)',late_error)
            page.wait_for_timeout(150)
            expect(page.locator('.workbench')).to_be_visible()
            expect(page.get_by_role('button',name='管道',exact=True)).to_have_attribute('aria-current','page')
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
