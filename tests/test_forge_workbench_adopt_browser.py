"""Keep recovery facts independent of overlapping, real graph projections."""
import hashlib
import json

from playwright.sync_api import expect

from test_forge_workspace import demo,isolated
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workbench_browser import page_for,pipeline


def test_adopted_file_intent_stays_cleared_when_a_newer_graph_read_is_pending(isolated,browser,monkeypatch,tmp_path):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'domain'))
    writes=[]
    with service(isolated,enable_config_write=True,workspace_config_write=True) as app,page_for(browser,app) as (page,errors):
        pipeline(page)
        page.get_by_role('button',name='整图配置',exact=True).click()
        page.get_by_label('配置 /arguments/minimum',exact=True).fill('0.7')
        def lose_reply(route):
            writes.append(route.request.post_data_json)
            response=route.fetch(timeout=65000);assert response.ok,response.text()
            route.abort('failed')
        page.route('**/parameters-update',lose_reply)
        page.get_by_role('button',name='保存配置',exact=True).click()
        expect(page.locator('.wb-file-pending')).to_contain_text('配置保存待核对')
        file=isolated/'flows/select_a.json';expected_sha=hashlib.sha256(file.read_bytes()).hexdigest()
        assert json.loads(file.read_text())['arguments']['minimum']==0.7
        page.evaluate("() => {Object.defineProperty(document,'hidden',{configurable:true,value:true});document.dispatchEvent(new Event('visibilitychange'));}")
        page.get_by_role('button',name='刷新当前视图',exact=True).click()
        expect(page.get_by_role('button',name='刷新当前视图',exact=True)).to_be_enabled(timeout=65000)
        page.get_by_role('button',name='读回当前配置',exact=True).click()
        expect(page.locator('.wb-file-pending')).to_contain_text('写入归属未确认')
        expect(page.locator('.wb-file-pending')).to_contain_text('不能由读回证明持久性')
        page.evaluate('''() => {
          const native=fetch.bind(window);window.__cfgGraphs=[];
          window.fetch=async(...args)=>{
            const response=await native(...args),url=new URL(String(args[0]),location.href);
            if(url.pathname.endsWith('/graph')&&window.__cfgGraphs.length<2){
              const item={body:await response.clone().json()};
              return new Promise(resolve=>{item.release=()=>resolve(response);window.__cfgGraphs.push(item);});
            }return response;
          };
        }''')
        page.get_by_role('button',name='采用当前内容',exact=True).click()
        page.wait_for_function('() => window.__cfgGraphs.length===1')
        page.get_by_role('button',name='刷新当前视图',exact=True).click()
        page.wait_for_function('() => window.__cfgGraphs.length===2')
        page.evaluate('() => window.__cfgGraphs[0].release()')
        expect(page.locator('.wb-notice')).to_contain_text('已采用当前内容')
        intents=page.evaluate("JSON.parse(sessionStorage.getItem('demiflow-workbench-parameter-intents-v1')||'[]')")
        assert intents==[]
        before=dict(persisted_intents=len(intents),displayed_pending=page.locator('.wb-file-pending').count(),
                    graph_reads=page.evaluate('window.__cfgGraphs.map(row=>({binding:row.body.binding,sha256:row.body.parameters.sha256}))'))
        assert all(row['sha256']==expected_sha for row in before['graph_reads'])
        page.evaluate('() => window.__cfgGraphs[1].release()')
        expect(page.get_by_role('button',name='刷新当前视图',exact=True)).to_be_enabled(timeout=65000)
        expect(page.locator('.wb-file-pending')).to_have_count(0,timeout=65000)
        page.get_by_role('button',name='整图配置',exact=True).click()
        expect(page.get_by_label('配置 /arguments/minimum',exact=True)).to_have_value('0.7')
        expect(page.get_by_label('配置 /arguments/minimum',exact=True)).to_be_enabled()
        page.get_by_role('button',name='保存配置',exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('没有待保存的配置修改')
        assert len(writes)==1 and 'request_id' not in writes[0]
        page.reload();page.locator('.wb-node').first.wait_for()
        expect(page.locator('.wb-file-pending')).to_have_count(0)
        page.get_by_role('button',name='整图配置',exact=True).click()
        expect(page.get_by_label('配置 /arguments/minimum',exact=True)).to_have_value('0.7')
        page.get_by_role('button',name='保存配置',exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('没有待保存的配置修改')
        assert len(writes)==1
        assert hashlib.sha256(file.read_bytes()).hexdigest()==expected_sha
        out=tmp_path/'evidence'
        out.mkdir(exist_ok=True)
        (out/'CFG2-deterministic-final.json').write_text(json.dumps(dict(before_newer_graph=before,
            after_newer_graph_pending=0,observed_file_sha256=expected_sha,minimum=0.7,draft_dirty=False,
            write_requests=len(writes),pending_after_reload=0,attribution_or_fsync_proven=False),ensure_ascii=False,indent=2)+'\n')
        assert not errors,errors
