import os, sys, json, threading, importlib.util
from pathlib import Path
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
ROOT=Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_integration_review_20261114')
OUT=Path('/tmp/demiflow-ui-alignment-20261114')
os.environ['DEMIFLOW_FORGE_DOMAIN']=str(OUT/'domain')
os.environ['PYTHONDONTWRITEBYTECODE']='1'
sys.path[:0]=[str(ROOT),str(ROOT/'tests')]
import demiflow,pytest
assert Path(demiflow.__file__).resolve().is_relative_to(ROOT)
from playwright.sync_api import sync_playwright,expect
from test_forge_public_api import service
from test_forge_analysis_case_demo_browser import representative_case
class Quiet(SimpleHTTPRequestHandler):
    def log_message(self,*args):pass
evidence=[]
def shot(page,name):
    page.wait_for_timeout(350)
    page.screenshot(path=str(OUT/(name+'.png')),full_page=True)
    text=page.locator('body').inner_text()
    (OUT/(name+'.txt')).write_text(text)
    evidence.append(dict(name=name,viewport=page.viewport_size,overflow=page.evaluate('document.documentElement.scrollWidth>innerWidth+1'),buttons=page.get_by_role('button').all_text_contents()))
with sync_playwright() as pw:
    browser=pw.chromium.launch(executable_path='/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
    server=ThreadingHTTPServer(('127.0.0.1',0),partial(Quiet,directory=str(ROOT/'docs/assets/workflow-workbench-20261105')))
    t=threading.Thread(target=server.serve_forever,daemon=True);t.start()
    try:
        p=browser.new_page(viewport=dict(width=1440,height=1000))
        p.goto('http://127.0.0.1:'+str(server.server_port))
        shot(p,'demo-pipeline')
        p.get_by_role('button',name='数据',exact=True).click()
        shot(p,'demo-data')
        print('DEMO DATA',p.get_by_role('button').all_text_contents(),flush=True)
        for name in ['Notebook','Case 分析']:
            b=p.get_by_role('button',name=name,exact=True)
            if b.count():b.first.click();shot(p,'demo-'+('notebook' if name=='Notebook' else 'case'))
        p.set_viewport_size(dict(width=390,height=844));shot(p,'demo-mobile')
        p.close()
    finally:server.shutdown();t.join();server.server_close()
    spec=importlib.util.spec_from_file_location('private_demo',ROOT/'examples/forge_workspace/make_demo.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    workspace=OUT/'workspace'
    if not workspace.exists():m.create(workspace)
    with service(workspace,enable_runs=True,enable_config_write=True,workspace_config_write=True) as app:
        p=browser.new_page(viewport=dict(width=1440,height=1000));p.set_default_timeout(65000)
        p.goto(app.origin+'/#token='+app.token)
        p.locator('.function-nav').get_by_role('button',name='管道',exact=True).click()
        p.locator('.wb-node').first.wait_for();shot(p,'current-source')
        p.get_by_role('button',name='检查',exact=True).click()
        expect(p.locator('.wb-basis')).to_contain_text('checked',timeout=90000)
        p.get_by_role('button',name='读图与配置',exact=True).click()
        p.locator('.wb-node').filter(has_text='normalize_question').click();shot(p,'current-pipeline')
        p.get_by_role('button',name='检查问题',exact=True).click();shot(p,'current-checks')
        p.locator('.function-nav').get_by_role('button',name='数据',exact=True).click();shot(p,'current-data-disconnected')
        p.close()
    with pytest.MonkeyPatch.context() as mp:
        case_root=OUT/'case';case_root.mkdir()
        with representative_case(browser,case_root,mp) as f:
            p=f['page'];p.set_viewport_size(dict(width=1440,height=1000));shot(p,'current-case')
            p.get_by_role('button',name='Notebook',exact=True).click();shot(p,'current-notebook')
            p.get_by_role('button',name='Case 分析',exact=True).click()
            p.set_viewport_size(dict(width=390,height=844));shot(p,'current-mobile')
    browser.close()
(OUT/'capture.json').write_text(json.dumps(dict(source=str(ROOT),head='267b9144c6bf7447c6822aa716f6d5c0eda16990',note='Demo uses mocks; current pipeline real local API, no run; current Case real Records/Lance with test-only metadata helper, analysis_execute=False; not runtime acceptance.',views=evidence),ensure_ascii=False,indent=2))
print('DONE',len(evidence),flush=True)
