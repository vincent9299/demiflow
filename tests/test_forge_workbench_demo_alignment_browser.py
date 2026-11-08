"""D7 interaction regression; UI API doubles do not prove kernel isolation."""
from pathlib import Path
import hashlib

from playwright.sync_api import expect

from test_forge_markdown_reading import browser
from test_forge_outputs_browser import page_for,WEB,PROJECT,witness_and_raw,output_reader


def test_explicit_cell_run_opens_its_output_but_navigation_never_replays(browser,tmp_path):
    witness,raw=witness_and_raw();output=output_reader.project_document(raw,witness)
    source='print(1)';source_sha=hashlib.sha256(source.encode()).hexdigest()
    output['provenance'].update(notebook_id='book',cell_id='cell',source_sha256=source_sha)
    with page_for(browser) as (page,state,errors):
        page.add_style_tag(path=str(WEB/'analysis.css'))
        for name in ('analysis-state.js','analysis.js'):page.add_script_tag(path=str(WEB/name))
        page.evaluate('''({project,output,source,source_sha})=>{
          window.apiCalls=[];window.submitted=false;
          const store=new ForgeAnalysisState.Store();store.saveReference(project,'book',{session:{id:'session',generation:'generation'},bindings:{}});
          window.editor=ForgeAnalysis.mount({parent:document.getElementById('mount'),project,connected:true,
            permissions:{analysis_execute:true},mountText,disposeReaders,
            api:async(name,query={},body)=>{
              apiCalls.push({name,query,body});
              const execution={id:'execution',cell_id:'cell',generation:'generation',source_sha256:source_sha,notebook_revision:1,state:'finished',status:'succeeded',output_id:output.artifact.id};
              if(name==='analysis-documents')return {items:[{id:'book',revision:1}]};
              if(name==='analysis-notebook')return {id:'book',revision:1,document:{title:'SQL 与 Python 分析',cells:[{id:'cell',language:'python',source}]},next_offset:null};
              if(name==='analysis-session')return {id:'session',generation:'generation',state:'idle'};
              if(name==='analysis-executions')return {items:submitted?[execution]:[],next_cursor:null};
              if(name==='analysis-cell-submit'){submitted=true;return execution;}
              if(name==='analysis-output')return output;
              throw Error('unexpected '+name);
            }});
        }''',dict(project=PROJECT,output=output,source=source,source_sha=source_sha))
        page.get_by_label('选择 Notebook',exact=True).select_option('book')
        expect(page.get_by_role('button',name='运行',exact=True)).to_be_enabled()
        assert page.evaluate("apiCalls.filter(call=>call.name==='analysis-output').length")==0
        page.get_by_role('button',name='运行',exact=True).click()
        expect(page.locator('.analysis-cell .analysis-output')).to_be_visible()
        expect(page.locator('.analysis-cell-status')).to_contain_text('完成')
        payload=page.evaluate("apiCalls.filter(call=>call.name==='analysis-cell-submit').map(call=>call.body)")
        assert len(payload)==1 and payload[0]['source_sha256']==source_sha and payload[0]['expected_generation']=='generation'
        assert payload[0]['revision']==1 and payload[0]['cell_id']=='cell'
        page.screenshot(path=str(tmp_path/'notebook-inline-explicit-output.png'),full_page=True)
        page.evaluate('editor.setMode(false)');expect(page.locator('.analysis-output-slot')).to_have_count(0)
        page.evaluate('editor.setMode(true)')
        assert page.locator('.analysis-output-slot').evaluate('(element)=>!element.open')
        assert page.evaluate("apiCalls.filter(call=>call.name==='analysis-cell-submit').length")==1
        page.evaluate('editor.dispose()')
        assert page.evaluate('ForgeAnalysisRender.stats().fields')==0
        assert not errors,errors


def test_notebook_source_projection_is_explicit_bounded_and_late_reply_does_not_rebind(browser,tmp_path):
    with page_for(browser) as (page,state,errors):
        page.add_style_tag(path=str(WEB/'analysis.css'))
        for name in ('analysis-state.js','analysis.js'):page.add_script_tag(path=str(WEB/name))
        page.evaluate('''project=>{
          window.apiCalls=[];window.waitBinding=false;window.replyBinding=null;
          window.editor=ForgeAnalysis.mount({parent:document.getElementById('mount'),project,connected:true,
            mountText,disposeReaders,api:async(name,query={},body)=>{
              apiCalls.push({name,query,body:body?structuredClone(body):body});
              if(name==='analysis-documents')return {items:[]};
              if(name==='analysis-source-register'){
                if(waitBinding)return await new Promise(resolve=>window.replyBinding=resolve);
                return {digest:'b'.repeat(64)};
              }
              throw Error('unexpected request '+name);
            }});
          editor.setSource({label:'Fixed cases v1',locator:{kind:'workspace_table',alias:'cases',binding:'a'.repeat(64),columns:['case_id','chart','a_evaluation']}});
        }''',PROJECT)
        editor=page.locator('.analysis-notebook');editor.locator('.analysis-sources > summary').click()
        alias=editor.get_by_label('内核中的表名',exact=True);projection=editor.get_by_label('字段投影',exact=True)
        alias.fill('scores');projection.fill('case_id, a_evaluation.score')
        editor.get_by_role('button',name='添加当前表',exact=True).click()
        expect(editor.locator('.analysis-sources')).to_contain_text('scores')
        assert page.evaluate('apiCalls.filter(item=>item.name==="analysis-source-register").at(-1).body.locator.columns')==['case_id','a_evaluation.score']
        assert page.evaluate('apiCalls.some(item=>/session-start|cell-submit/.test(item.name))') is False
        calls=page.evaluate('apiCalls.length');projection.fill('case_id, case_id')
        editor.get_by_role('button',name='添加当前表',exact=True).click()
        expect(editor.locator('.analysis-notice')).to_contain_text('互不重复')
        assert page.evaluate('apiCalls.length')==calls
        alias.fill('pictures');projection.fill('case_id, chart.path')
        page.evaluate('waitBinding=true')
        editor.get_by_role('button',name='添加当前表',exact=True).click()
        page.wait_for_function('()=>typeof replyBinding==="function"')
        projection.fill('case_id, other.path');page.evaluate('replyBinding({digest:"c".repeat(64)})')
        expect(editor.locator('.analysis-notice')).to_contain_text('字段投影或表名已变化')
        assert page.evaluate('apiCalls.filter(item=>item.name==="analysis-source-register").at(-1).body.locator.columns')==['case_id','chart.path']
        assert editor.locator('.analysis-sources > .analysis-source-row').count()==2  # Editor controls plus the one successful binding.
        for width in (1440,390):
            page.set_viewport_size(dict(width=width,height=900))
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
            page.screenshot(path=str(tmp_path/('notebook-source-projection-'+str(width)+'.png')),full_page=True)
        page.evaluate('editor.dispose()')
        assert not errors,errors


def test_ordinary_pipeline_overview_keeps_output_visible_and_manual_zoom_on_resize(browser):
    with page_for(browser) as (page,state,errors):
        page.add_style_tag(path=str(WEB/'workbench.css'))
        page.add_script_tag(path=str(WEB/'workbench-graph.js'))
        page.evaluate('''()=>{
          const host=document.getElementById('mount');host.className='wb-graph';host.style.width='min(778px, 100%)';host.style.boxSizing='border-box';
          const nodes=Array.from({length:7},(_,index)=>({id:'n'+index,api:index===6?'write_lance':'step_'+index,kind:index===6?'write_table':'map'}));
          const edges=Array.from({length:6},(_,index)=>({from:'n'+index,to:'n'+(index+1),type:'data'}));
          // Two input branches joining the five-step downstream chain: the
          // ordinary seven-node native pipeline used by run-monitor proofs.
          edges[0]={from:'n0',to:'n2',type:'data'};
          window.graph=ForgeWorkbenchGraph.mount(host);graph.update({binding:{id:'ordinary'},nodes,edges},null,'n6');
        }''')
        page.wait_for_timeout(100)
        assert page.evaluate('''()=>{
          const host=document.getElementById('mount').getBoundingClientRect();
          return [...document.querySelectorAll('.wb-node')].every(node=>{const box=node.getBoundingClientRect();return box.left>=host.left&&box.right<=host.right+1;});
        }''')
        page.evaluate('graph.zoom(1.4)');width=page.locator('#mount > svg').evaluate('(node)=>node.getBoundingClientRect().width')
        page.set_viewport_size(dict(width=1200,height=900));page.wait_for_timeout(100)
        assert page.locator('#mount > svg').evaluate('(node)=>node.getBoundingClientRect().width')==width
        page.set_viewport_size(dict(width=390,height=900));page.evaluate('graph.fit()');page.wait_for_timeout(100)
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        page.evaluate('graph.dispose()')
        assert not errors,errors
