"""Explicit fixed-result navigation owns the existing page epoch immediately."""
import json
import os
import re
from pathlib import Path

import pytest
from playwright.sync_api import expect

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import registry as console
from test_forge_workspace import demo,isolated
from test_forge_runs import wait
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workbench_browser import page_for,pipeline


@pytest.fixture
def completed(isolated,tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    ids=[]
    for variant in ('a','b'):
        prepared=runs.prepare(isolated,'flows/questions.py:select',params='flows/select_'+variant+'.json',new_outputs=True)
        runs.start(isolated,prepared['run_id'],expected=prepared['digest'])
        assert wait(isolated,prepared['run_id'])['status']=='succeeded'
        ids.append(prepared['run_id'])
    return isolated,ids


def hold_reads(page,run_id,*,graph):
    page.evaluate('''({run,graph}) => {
      const native=fetch.bind(window),alias='run:'+run+':target';
      window.navRead={graphHeld:false,tableHeld:false};
      window.fetch=async(...args)=>{
        const response=await native(...args),url=new URL(String(args[0]),location.href);
        if(graph&&url.pathname.endsWith('/graph')&&url.searchParams.get('run_id')===run&&!window.navRead.graphHeld){
          const body=await response.clone().json();
          if(!response.ok||body.binding.run_id!==run)throw new Error('Owned graph response was not bound');
          window.navRead.graphHeld=true;
          return new Promise(resolve=>{window.releaseRunGraph=()=>resolve(response);});
        }
        if(url.pathname.endsWith('/table')&&url.searchParams.get('alias')===alias&&!window.navRead.tableHeld){
          const body=await response.clone().json();
          if(!response.ok||body.binding.alias!==alias)throw new Error('Owned table response was not bound');
          window.navRead.tableHeld=true;
          return new Promise(resolve=>{window.releaseResultTable=()=>resolve(response);})
            .then(value=>{window.navRead.tableReturned=true;return value;});
        }
        return response;
      };
    }''',dict(run=run_id,graph=graph))


def select_run(page,run_id):
    page.locator('.function-nav').get_by_role('button',name='运行监控',exact=True).click()
    if not page.locator('.wb-history').is_visible():
        page.get_by_role('button',name='运行记录',exact=True).click()
    page.locator('.wb-run-record[data-run="'+run_id+'"]').click()
    expect(page.get_by_role('combobox',name='运行记录',exact=True)).to_have_value(run_id)
    page.get_by_role('button',name='输出结果',exact=True).click()


def test_background_graph_cannot_cancel_clicked_fixed_result(completed,browser,tmp_path):
    root,(a,b)=completed
    with service(root) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.evaluate('() => Object.defineProperty(document,"hidden",{configurable:true,get:()=>true})')
        hold_reads(page,a,graph=True)
        select_run(page,a)
        page.wait_for_function('() => window.navRead.graphHeld')
        page.get_by_role('button',name='查看固定结果',exact=True).click()
        page.wait_for_function('() => window.navRead.tableHeld')
        page.evaluate('() => window.releaseRunGraph()')
        page.wait_for_function('''() => !document.querySelector('.workbench') ||
          document.querySelector('.wb-basis')?.textContent.startsWith('封存运行图')''')
        page.evaluate('() => window.releaseResultTable()')
        page.wait_for_function('() => window.navRead.tableReturned === true')
        out=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path)));out.mkdir(parents=True,exist_ok=True)
        (out/'result-navigation-release.json').write_text(json.dumps(dict(run=a,other=b,
            graph_response_held=True,table_response_held=True,graph_released_before_table=True,
            actual_view=page.evaluate('ForgeView.decodeView(location.hash)'),main_class=page.locator('main').get_attribute('class')),ensure_ascii=False,indent=2)+'\n')
        expect(page.locator('main')).to_have_class(re.compile(r'\bdata-active\b'),timeout=10000)
        page.locator('.case-record-selector button').first.wait_for()
        assert page.locator('.case-record-selector button').count()==2
        reference=page.evaluate('ForgeView.decodeView(location.hash)')
        assert reference['alias']=='run:'+a+':target' and reference['binding_digest']
        (out/'result-navigation-completed.json').write_text(json.dumps(dict(reference=reference,
            case_rows=2,older_graph_cannot_replace_explicit_navigation=True,errors=errors),ensure_ascii=False,indent=2)+'\n')
        assert not errors,errors


@pytest.mark.parametrize('later',['Readme','different_run','different_project'])
def test_later_explicit_selection_discards_held_fixed_result(completed,browser,tmp_path,later):
    root,(a,b)=completed
    registry=tmp_path/'registry.json';key=console.add(registry,root)['workspace_id']
    other=tmp_path/'other-project';other.mkdir();(other/'README.md').write_text('# Other navigation scope\nSeparate project.')
    other_key=console.add(registry,other)['workspace_id']
    with service(None,registry=registry) as app,page_for(browser,app) as (page,errors):
        page.locator('#project-list [data-workspace="'+key+'"]').click()
        pipeline(page)
        page.evaluate('() => Object.defineProperty(document,"hidden",{configurable:true,get:()=>true})')
        hold_reads(page,a,graph=False)
        select_run(page,a)
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        page.get_by_role('button',name='查看固定结果',exact=True).click()
        page.wait_for_function('() => window.navRead.tableHeld')
        if later=='Readme':
            page.locator('.function-nav').get_by_role('button',name='Readme',exact=True).click()
            expect(page.locator('#project-readme')).to_be_visible()
        elif later=='different_run':
            pipeline(page);select_run(page,b)
            expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        else:
            page.locator('#project-list [data-workspace="'+other_key+'"]').click()
            page.get_by_role('heading',name='Other navigation scope',exact=True).wait_for()
        position=page.url
        page.evaluate('() => window.releaseResultTable()')
        page.wait_for_function('() => window.navRead.tableReturned === true')
        assert page.url==position
        reference=page.evaluate('ForgeView.decodeView(location.hash)')
        if later=='Readme':
            assert reference['kind']=='overview'
            expect(page.locator('#project-readme')).to_be_visible()
        elif later=='different_run':
            assert reference['kind']=='run' and reference['run_id']==b
            expect(page.get_by_role('combobox',name='运行记录',exact=True)).to_have_value(b)
        else:
            assert reference['workspace']==other_key and reference['kind']=='overview'
            page.get_by_role('heading',name='Other navigation scope',exact=True).wait_for()
        assert page.locator('.case-record-selector').count()==0 and not errors,errors
        out=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path)));out.mkdir(parents=True,exist_ok=True)
        (out/('result-navigation-later-'+later+'.json')).write_text(json.dumps(dict(later=later,
            actual=reference,url_unchanged_after_old_response=True,errors=errors),ensure_ascii=False,indent=2)+'\n')
