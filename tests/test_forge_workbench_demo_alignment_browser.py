"""D7 interaction regression; UI API doubles do not prove kernel isolation."""
from pathlib import Path


from test_forge_markdown_reading import browser
from contextlib import contextmanager
WEB=Path(__file__).resolve().parents[1]/'demiflow/workbench/web'

@contextmanager
def page_for(browser):
    context=browser.new_context(viewport={'width':1440,'height':900})
    page=context.new_page();errors=[]
    page.on('pageerror',lambda error:errors.append(str(error)))
    page.set_content('<div id="mount"></div>')
    try: yield page,{},errors
    finally: context.close()






def test_ordinary_pipeline_overview_keeps_output_visible_and_manual_zoom_on_resize(browser):
    with page_for(browser) as (page,state,errors):
        page.add_style_tag(path=str(WEB/'workbench.css'))
        page.add_style_tag(path=str(WEB/'workbench-graph.css'))
        page.add_script_tag(path=str(WEB/'workbench-presentation.js'))
        page.add_script_tag(path=str(WEB/'workbench-graph.js'))
        page.evaluate('''()=>{
          const host=document.getElementById('mount');host.className='wb-graph';host.style.width='min(778px, 100%)';host.style.boxSizing='border-box';
          const nodes=Array.from({length:7},(_,index)=>({id:'n'+index,api:index===6?'write_lance':'step_'+index,kind:index===6?'write_table':'map'}));
          const edges=Array.from({length:6},(_,index)=>({from:'n'+index,to:'n'+(index+1),type:'data'}));
          // Two input branches joining the five-step downstream chain: the
          // ordinary seven-node native pipeline used by run-monitor proofs.
          edges[0]={from:'n0',to:'n2',type:'data'};
          window.graph=ForgeWorkbenchGraph.mount(host);
          graph.update(ForgeWorkbenchPresentation.buildGraphModel({binding:{basis_digest:'a'.repeat(64)},nodes,edges},{choice:{kind:'report',label:'Fixture'}}));graph.fit();
        }''')
        page.wait_for_timeout(100)
        assert page.evaluate('''()=>{
          const host=document.getElementById('mount').getBoundingClientRect();
          return [...document.querySelectorAll('.wb-node')].every(node=>{const box=node.getBoundingClientRect();return box.left>=host.left&&box.right<=host.right+1;});
        }''')
        page.evaluate('graph.zoom(1.4)');width=page.locator('#mount svg').evaluate('(node)=>node.getBoundingClientRect().width')
        page.set_viewport_size(dict(width=1200,height=900));page.wait_for_timeout(100)
        assert page.locator('#mount svg').evaluate('(node)=>node.getBoundingClientRect().width')==width
        page.set_viewport_size(dict(width=390,height=900));page.evaluate('graph.fit()');page.wait_for_timeout(100)
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        page.evaluate('graph.dispose()')
        assert not errors,errors
