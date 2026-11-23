"""Real tab navigation retains confirmed scoped references, never controller state.

FORGE_APP_CANDIDATE is a diagnostic-only asset overlay while production is
frozen. Final acceptance must run without it against the actual served asset.
All API data, runs, version facts, Stores and controllers remain real.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from playwright.sync_api import expect

from d19_fixtures import owned_fixture
from demiflow.workbench.projects import registry as console
from test_forge_markdown_reading import browser


@contextmanager
def tab_page(fx, browser):
    context=browser.new_context(viewport={'width':1440,'height':1050})
    candidate=os.environ.get('FORGE_APP_CANDIDATE')
    if candidate:
        asset=Path(candidate).read_bytes()
        context.route('**/app.js',lambda route:route.fulfill(status=200,content_type='text/javascript',body=asset))
    page=context.new_page();page.set_default_timeout(65000);expect.set_options(timeout=65000)
    errors=[];requests=[]
    page.on('pageerror',lambda error:errors.append(str(error)))
    page.on('request',lambda request:requests.append(dict(method=request.method,url=request.url))
            if '/api/' in request.url and len(requests)<1024 else None)
    try:
        page.goto(fx.app.origin+'/#token='+fx.app.token)
        page.locator('#project-switcher').click()
        page.locator('#project-list [data-workspace="'+fx.project_id+'"]').click()
        page.locator('.function-nav [data-page="flows"]').wait_for()
        page.evaluate('() => Object.defineProperty(document,"hidden",{configurable:true,get:()=>true})')
        yield page,requests
        assert not errors,errors
    except BaseException:
        fx.shot(page,'tab-restore-failure')
        raise
    finally:
        fx.record('tab-restore-browser',dict(errors=errors,requests=requests,
                  diagnostic_asset_overlay=candidate,asset_sha256=hashlib.sha256(asset).hexdigest() if candidate else None))
        context.close()


def tab(page,label):
    page.locator('.function-nav').get_by_role('button',name=label,exact=True).click()


def reference(page):
    return page.evaluate('ForgeView.decodeView(location.hash)')


def expect_reference(page,expected):
    page.wait_for_function('''expected => {
      const actual=ForgeView.decodeView(location.hash);
      return actual && Object.keys(actual).length===Object.keys(expected).length &&
        Object.entries(expected).every(([key,value])=>JSON.stringify(actual[key])===JSON.stringify(value));
    }''',arg=expected)
    assert reference(page)==expected


def endpoint(response,name,**query):
    url=urlsplit(response.url);values=parse_qs(url.query)
    return url.path.endswith('/'+name) and all(values.get(key)==[str(value)] for key,value in query.items())


@pytest.mark.parametrize('owned_fixture',[{'execution_format':'v3'}],indirect=True)
def test_tabs_restore_exact_run_version_and_project_despite_late_response(owned_fixture,browser):
    fx=owned_fixture
    other=console.create(fx.registry,fx.tmp_path/'other-project',name='Tab isolation project')['workspace_id']
    run_id=fx.run_facts['run_id'];vf=fx.version_facts
    with tab_page(fx,browser) as (page,requests):
        # The unified pipeline tab owns both current definition and sealed runs.
        tab(page,'管道')
        page.locator('.wb-actions').get_by_role('button',name='运行记录',exact=True).click()
        page.get_by_label('运行记录',exact=True).select_option(run_id)
        page.locator('.wb-node').first.click()
        run_ref=reference(page)
        assert run_ref['run_id']==run_id and run_ref['attempt']==fx.run_facts['attempt']
        assert all(run_ref.get(key) is not None for key in (
            'action_id','logical_node_id','occurrence_id','dag_id','plan_id','plan_revision','plan_format','node_id','report_digest'))
        assert run_ref['plan_format']=='v3'
        tab(page,'数据')
        fixed_ref=dict(v=2,workspace=fx.project_id,kind='table',alias=vf['alias'],version=1,
                       binding_digest=vf['versions'][1]['binding'],alias_binding=vf['alias_binding'])
        page.evaluate('ref=>{location.hash=ForgeView.encodeView(ref)}',fixed_ref)
        expect(page.locator('.dictionary-table[open] .dictionary-version')).to_contain_text('v1')
        expect(page.locator('.dictionary-table[open] .dictionary-meta')).to_contain_text('2 行')
        expect_reference(page,fixed_ref)

        # Return to this exact run and attempt, rather than a current source graph.
        tab(page,'管道')
        expect_reference(page,run_ref)
        expect(page.locator('.wb-node.selected')).to_have_count(1)
        tab(page,'数据');expect_reference(page,fixed_ref)
        expect(page.locator('.dictionary-table[open] .dictionary-version')).to_contain_text('v1')

        # Hold a genuine completed graph read, then leave while restoration is
        # loading. The late response must neither repaint nor replace tab memory.
        page.evaluate('''run => {
          const native=fetch.bind(window);window.tabLate={held:false,returned:false};
          window.fetch=async(...args)=>{
            const response=await native(...args),url=new URL(String(args[0]),location.href);
            if(url.pathname.endsWith('/graph')&&url.searchParams.get('run_id')===run&&!tabLate.held){
              const body=await response.clone().json();
              if(!response.ok||body.binding.run_id!==run)throw Error('real run graph identity differs');
              tabLate.held=true;
              return new Promise(resolve=>{window.releaseTabGraph=()=>resolve(response);})
                .then(value=>{tabLate.returned=true;return value;});
            }
            return response;
          };
        }''',run_id)
        tab(page,'管道');page.wait_for_function('() => tabLate.held')
        tab(page,'Readme');expect(page.locator('#project-readme')).to_be_visible()
        readme_ref=reference(page)
        page.evaluate('() => releaseTabGraph()');page.wait_for_function('() => tabLate.returned')
        expect_reference(page,readme_ref)
        tab(page,'管道');expect_reference(page,run_ref)
        expect(page.locator('.wb-node.selected')).to_have_count(1)

        # A separate project gets its own default page; returning restores A's
        # complete run locator and its independent fixed historical data locator.
        page.locator('#project-switcher').click()
        page.locator('#project-list [data-workspace="'+other+'"]').click()
        page.wait_for_function('workspace=>ForgeView.decodeView(location.hash)?.workspace===workspace',arg=other)
        assert reference(page)==dict(v=2,workspace=other,kind='overview')
        page.locator('#project-switcher').click()
        page.locator('#project-list [data-workspace="'+fx.project_id+'"]').click()
        expect_reference(page,run_ref)
        tab(page,'数据');expect_reference(page,fixed_ref)
        expect(page.locator('.dictionary-table[open] .dictionary-version')).to_contain_text('v1')
        tab(page,'管道');expect_reference(page,run_ref)
        assert not [item for item in requests if item['method']=='POST' and not item['url'].endswith('/plan-preview')],requests
        fx.record('tab-restore-facts',dict(run=run_ref,fixed_version=fixed_ref,other_project=other,
                  late_real_graph_rejected=True,post_requests=0))


@pytest.mark.parametrize('owned_fixture',[{'execution_format':'v3'}],indirect=True)
def test_flows_tab_never_inherits_run_and_clears_settled_pending_intent(owned_fixture,browser):
    fx=owned_fixture
    run_id=fx.run_facts['run_id']
    attempt=fx.run_facts['attempt']
    with tab_page(fx,browser) as (page,_):
        # 1. Open a historical run inside 管道, leave to a non-pipeline tab, then
        # return to 管道 (restoring the run), and switch to 当前 for a clean flow view.
        tab(page,'管道')
        page.locator('.wb-actions').get_by_role('button',name='运行记录',exact=True).click()
        page.get_by_label('运行记录',exact=True).select_option(run_id)
        expect(page.locator('.wb-node')).not_to_have_count(0)
        run_ref=reference(page)
        assert run_ref['kind']=='run' and run_ref['run_id']==run_id

        tab(page,'数据')
        expect(page.locator('.data-dictionary')).to_be_visible()

        tab(page,'管道')
        expect_reference(page,run_ref)

        page.locator('.wb-actions').get_by_role('button',name='返回当前配置',exact=True).click()
        expect(page.locator('.wb-flow-record').first).to_be_visible()
        expect(page.locator('.wb-flow-record').first).to_be_enabled()
        expect(page.locator('.wb-actions').get_by_role('button',name='运行',exact=True)).to_be_visible()
        expect(page.locator('.wb-actions').get_by_role('button',name='业务参数',exact=True)).to_have_count(0)
        expect(page.locator('.wb-toolbar')).to_be_hidden()
        page.wait_for_function("()=>ForgeView.decodeView(location.hash)?.kind==='flows'")
        flow_ref=reference(page)
        assert flow_ref['kind']=='flows' and flow_ref.get('panel')=='flows'

        # 2. A stale hash combining kind=run with panel=flows must still open a
        # clean 管道 view without locking the flow sidebar.
        stale=dict(v=2,workspace=fx.project_id,kind='run',run_id=run_id,attempt=attempt,panel='flows')
        page.evaluate('ref=>{location.hash=ForgeView.encodeView(ref)}',stale)
        expect(page.locator('.wb-flow-record').first).to_be_enabled()
        expect(page.locator('.wb-actions').get_by_role('button',name='运行',exact=True)).to_be_visible()

        # 3. A pending start intent whose run is already terminal auto-settles
        # without leaving a stale "提交待核对" banner.
        page.evaluate('''([project,runId,attempt])=>{
          sessionStorage.setItem('demiflow-workbench-run-intents-v1',JSON.stringify([{
            project,request_id:'p1795402058595-31021d262496d03d52260f9883e08642',
            fingerprint:'0'.repeat(64),body:{entry:'flows/demo.py:run'},
            phase:'start',run_id:runId,attempt,digest:'0'.repeat(64),created_at:Date.now()
          }]));
        }''',[fx.project_id,run_id,attempt])
        page.reload()
        expect(page.locator('.wb-flow-record').first).to_be_visible()
        page.wait_for_function("()=>JSON.parse(sessionStorage.getItem('demiflow-workbench-run-intents-v1')||'[]').length===0")
        expect(page.locator('.wb-pending')).to_be_hidden()
