"""A delayed status read cannot replace the result of a newer explicit check."""
import json
from pathlib import Path
import pytest

from playwright.sync_api import expect

from test_forge_project_checks import project,demo,isolated
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workbench_browser import page_for
from test_forge_web_redesign import project_info
from demiflow.workbench.projects import registry as console


@pytest.mark.parametrize('late_error',[False,True])
def test_late_project_status_cannot_replace_newer_check_result(project,browser,monkeypatch,tmp_path,late_error):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    with service(project) as app,page_for(browser,app) as (page,errors):
        project_info(page)
        with page.expect_response('**/project-check') as checked:
            page.get_by_role('button',name='检查当前项目',exact=True).click()
        assert checked.value.json()['static_status']=='checked'
        source=project/'processing.py';source.write_text('import subprocess\n'+source.read_text())
        page.evaluate('''() => {
          const native=fetch.bind(window);window.__heldStatus=false;
          window.fetch=async(...args)=>{
            const response=await native(...args),url=new URL(String(args[0]),location.href);
            if(url.pathname.endsWith('/project-status')&&!window.__heldStatus){
              window.__heldStatus=true;window.__oldStatus=await response.clone().json();
              return new Promise((resolve,reject)=>{window.__releaseStatus=fail=>fail?reject(new Error('owned old project status fault')):resolve(response);});
            }
            return response;
          };
        }''')
        with page.expect_request('**/project-status'):
            page.get_by_role('button',name='刷新当前视图',exact=True).click()
        page.wait_for_function('() => typeof window.__releaseStatus==="function"')
        old=page.evaluate('window.__oldStatus')
        with page.expect_response('**/project-check') as violation:
            page.get_by_role('button',name='检查当前项目',exact=True).click()
        fresh=violation.value.json()
        assert fresh['status']==fresh['static_status']=='invalid'
        page.evaluate('fail => window.__releaseStatus(fail)',late_error)
        expect(page.get_by_role('button',name='刷新当前视图',exact=True)).to_be_enabled(timeout=65000)
        evidence=dict(old_status=old['status'],old_static_status=old['static_status'],
                      explicit_check_status=fresh['status'],explicit_static_status=fresh['static_status'],
                      shown=page.locator('.workbench > details').last.inner_text())
        out=Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/workflow_workbench_20261105/P1')
        (out/('project-check-status-'+('error' if late_error else 'success')+'.json')).write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+'\n')
        expect(page.get_by_text('整体：不符合规范；静态检查：不符合规范',exact=True)).to_be_visible(timeout=5000)
        expect(page.locator('.wb-notice')).not_to_contain_text('owned old project status fault')
        # A genuinely newer source change must still make the current status stale.
        source.write_text(source.read_text()+'\n# newer author revision\n')
        with page.expect_response('**/project-status'):
            page.get_by_role('button',name='刷新当前视图',exact=True).click()
        expect(page.get_by_text('整体：依据已变化；静态检查：依据已变化',exact=True)).to_be_visible(timeout=65000)
        assert not errors,errors


@pytest.mark.parametrize('late_error',[False,True])
def test_project_switch_discards_old_status_and_detached_check(project,browser,monkeypatch,tmp_path,late_error):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    registry=tmp_path/'registry.json'
    key=console.add(registry,project)['workspace_id']
    other=tmp_path/'other-project';other.mkdir();(other/'README.md').write_text('# Other project\nSeparate context.')
    other_key=console.add(registry,other)['workspace_id']
    with service(None,registry=registry) as app,page_for(browser,app) as (page,errors):
        page.locator('#project-list [data-workspace="'+key+'"]').click()
        project_info(page)
        page.get_by_role('button',name='检查当前项目',exact=True).evaluate('(node)=>window.__oldProjectCheck=node')
        page.evaluate('''() => {
          const native=fetch.bind(window);window.__heldStatus=false;
          window.fetch=async(...args)=>{
            const response=await native(...args),url=new URL(String(args[0]),location.href);
            if(url.pathname.endsWith('/project-status')&&!window.__heldStatus){
              window.__heldStatus=true;
              return new Promise((resolve,reject)=>{window.__releaseStatus=fail=>fail?reject(new Error('owned old project status fault')):resolve(response);});
            }
            return response;
          };
        }''')
        page.get_by_role('button',name='刷新当前视图',exact=True).click()
        page.wait_for_function('() => typeof window.__releaseStatus==="function"')
        page.locator('#project-list [data-workspace="'+other_key+'"]').click()
        page.get_by_role('heading',name='Other project',exact=True).wait_for()
        writes=[];page.on('request',lambda r:writes.append(r.url) if r.method=='POST' else None)
        page.evaluate('() => window.__oldProjectCheck.click()')
        page.evaluate('fail => window.__releaseStatus(fail)',late_error)
        page.wait_for_timeout(150)
        assert writes==[]
        assert page.evaluate('ForgeView.decodeView(location.hash).workspace')==other_key
        expect(page.locator('main')).to_contain_text('Separate context.')
        expect(page.locator('main')).not_to_contain_text('owned old project status fault')
        assert not errors,errors
