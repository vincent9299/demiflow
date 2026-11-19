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
        # The current run module owns sealed-run navigation. Current pipeline
        # configuration no longer doubles as the historical monitoring page.
        tab(page,'运行')
        page.locator('.wb-run-record[data-run="'+run_id+'"]').click()
        page.locator('.wb-node').first.click()
        run_ref=reference(page)
        assert run_ref['run_id']==run_id and run_ref['attempt']==fx.run_facts['attempt']
        assert all(run_ref.get(key) is not None for key in (
            'action_id','logical_node_id','occurrence_id','dag_id','plan_id','plan_revision','plan_format','node_id','report_digest'))
        assert run_ref['plan_format']=='v3'
        tab(page,'数据')
        with page.expect_response(lambda response:endpoint(response,'sample',alias=vf['alias'])) as initial:
            page.get_by_label('当前数据表',exact=True).select_option(vf['alias'])
        assert initial.value.status==200
        expect(page.locator('.data-source-context')).to_contain_text('v2')
        page.locator('details.bound-versions > summary').click()
        with page.expect_response(lambda response:endpoint(response,'sample',version=1)) as historical:
            page.locator('button.version-row[data-version="1"]').click()
        assert historical.value.status==200 and historical.value.json()['binding']['digest']==vf['versions'][1]['binding']
        expect(page.locator('.data-source-context')).to_contain_text('v1')
        expect(page.locator('.case-record-selector button')).to_have_count(2)
        chosen=page.locator('.case-record-selector button').nth(1)
        row_id=chosen.get_attribute('data-row-id')
        chosen.click()
        page.wait_for_function('row=>ForgeView.decodeView(location.hash)?.row_id===row',arg=row_id)
        fixed_ref=reference(page)
        assert fixed_ref['version']==1 and fixed_ref['alias_binding']==vf['alias_binding']
        assert fixed_ref['binding_digest']==vf['versions'][1]['binding']

        # Return to this exact run and attempt, rather than a current source graph.
        tab(page,'运行')
        expect_reference(page,run_ref)
        expect(page.locator('.wb-node.selected')).to_have_count(1)
        tab(page,'数据');expect_reference(page,fixed_ref)
        expect(page.locator('.data-source-context')).to_contain_text('v1')

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
        tab(page,'运行');page.wait_for_function('() => tabLate.held')
        tab(page,'Readme');expect(page.locator('#project-readme')).to_be_visible()
        readme_ref=reference(page)
        page.evaluate('() => releaseTabGraph()');page.wait_for_function('() => tabLate.returned')
        expect_reference(page,readme_ref)
        tab(page,'运行');expect_reference(page,run_ref)
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
        expect(page.locator('.data-source-context')).to_contain_text('v1')
        tab(page,'运行');expect_reference(page,run_ref)
        assert not [item for item in requests if item['method']=='POST'],requests
        fx.record('tab-restore-facts',dict(run=run_ref,fixed_version=fixed_ref,other_project=other,
                  late_real_graph_rejected=True,post_requests=0))


@pytest.mark.parametrize('owned_fixture',[{}],indirect=True)
def test_blocked_tab_and_project_departure_preserve_notebook_editor(owned_fixture,browser):
    fx=owned_fixture
    other=console.create(fx.registry,fx.tmp_path/'other-project',name='Blocked departure project')['workspace_id']
    with tab_page(fx,browser) as (page,requests):
        tab(page,'数据');page.get_by_role('button',name='Notebook',exact=True).click()
        source='print("retain the original editor until capture succeeds")'
        # Fill the real bounded draft Store: capture must refuse a fifth draft.
        page.evaluate('''project=>{const store=new ForgeAnalysisState.Store();
          for(let n=0;n<4;n++)store.saveDraft(project,'tab-capacity-'+n,
            {revision:0,document:{title:'retained '+n,cells:[]}});
        }''',fx.project_id)
        page.get_by_label('python 单元代码',exact=True).fill(source)
        current=reference(page)
        editor_id=page.get_by_label('选择 Notebook',exact=True).input_value()
        tab(page,'管道')
        expect(page.locator('.analysis-notice')).to_contain_text('暂停离开')
        expect(page.get_by_label('python 单元代码',exact=True)).to_have_value(source)
        assert reference(page)==current
        page.locator('#project-switcher').click()
        page.locator('#project-list [data-workspace="'+other+'"]').click()
        expect(page.get_by_label('选择 Notebook',exact=True)).to_have_value(editor_id)
        expect(page.get_by_label('python 单元代码',exact=True)).to_have_value(source)
        assert reference(page)==current
        page.evaluate('project=>new ForgeAnalysisState.Store().removeDraft(project,"tab-capacity-0")',fx.project_id)
        tab(page,'管道');page.locator('.workbench').wait_for()
        tab(page,'数据');page.get_by_role('button',name='Notebook',exact=True).click()
        page.get_by_label('选择 Notebook',exact=True).select_option(editor_id)
        expect(page.get_by_label('python 单元代码',exact=True)).to_have_value(source)
        assert not [item for item in requests if item['method']=='POST'],requests
        fx.record('tab-departure-facts',dict(editor=editor_id,source=source,
                  blocked_tab_preserved=True,blocked_project_preserved=True,capture_then_return_preserved=True))
