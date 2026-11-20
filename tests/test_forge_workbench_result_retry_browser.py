"""Unbound read requests survive explicit recovery without changing identity."""
import json
import os
from pathlib import Path
from urllib.parse import parse_qs,urlsplit

from playwright.sync_api import expect

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from test_forge_workbench_result_navigation_browser import completed,select_run
from test_forge_workspace import demo,isolated
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workbench_browser import page_for,pipeline


def test_fixed_result_request_survives_failure_refresh_reload_history_and_conflict(completed,browser,tmp_path):
    root,(a,b)=completed;alias='run:'+a+':target'
    expected=project_ops.describe_table(root,alias)['binding']['digest']
    out=Path(os.environ.get('FORGE_ACCEPTANCE_DIR',str(tmp_path)));out.mkdir(parents=True,exist_ok=True)
    records=[];reads=[]
    with service(root) as app,page_for(browser,app) as (page,errors):
        def pause():
            page.evaluate('''() => {
              Object.defineProperty(document,'hidden',{configurable:true,get:()=>true});
              document.dispatchEvent(new Event('visibilitychange'));
            }''')
        def read(request):
            parts=urlsplit(request.url)
            if parts.path.endswith('/table') or parts.path.endswith('/sample'):
                assert len(reads)<256
                reads.append(dict(path=parts.path.rsplit('/',1)[-1],query=parse_qs(parts.query)))
        page.on('request',read)
        pipeline(page);pause()
        contract=page.evaluate('''({workspace,legacy,alias,digest}) => {
          const rejects=value=>{try{ForgeView.validateView(value);return false;}catch{return true;}};
          return {
            fixed_links_still_require_binding:[1,2].every(v=>rejects({v,workspace:v===1?legacy:workspace,kind:'table',alias})&&rejects({v,workspace:v===1?legacy:workspace,kind:'case',alias,row_id:'1'})),
            v1_request_rejected:rejects({v:1,workspace:legacy,kind:'table_request',alias}),
            request_does_not_claim_binding:rejects({v:2,workspace,kind:'table_request',alias,binding_digest:digest}),
            valid_request:ForgeView.validateView({v:2,workspace,kind:'table_request',alias,expected_binding:digest})
          };
        }''',dict(workspace=page.evaluate('ForgeView.decodeView(location.hash).workspace'),legacy=str(root),alias=alias,digest=expected))
        assert contract['fixed_links_still_require_binding'] and contract['v1_request_rejected'] and contract['request_does_not_claim_binding']

        def refuse_once(wanted):
            used=[]
            def route_read(route):
                query=parse_qs(urlsplit(route.request.url).query)
                if query.get('alias')!=[wanted] or used:
                    route.continue_();return
                used.append(query)
                assert app.workers.acquire(timeout=65)
                try:response=route.fetch(timeout=65000)
                finally:app.workers.release()
                assert response.status==503 and response.json()['operation_started'] is False
                route.fulfill(response=response)
            page.route('**/table?*',route_read)
            return route_read,used

        for action in ('refresh','retry','reload','history'):
            pipeline(page);pause();select_run(page,a)
            expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
            route_read,used=refuse_once(alias)
            page.get_by_role('button',name='查看固定结果',exact=True).click()
            page.get_by_role('button',name='重试打开数据表',exact=True).wait_for()
            page.unroute('**/table?*',route_read)
            failed=page.evaluate('ForgeView.decodeView(location.hash)')
            assert failed['kind']=='table_request' and failed['alias']==alias and 'binding_digest' not in failed
            assert len(used)==1
            if action=='refresh':
                before=len([r for r in reads if r['path']=='table'])
                page.evaluate('''() => {
                  window.recoverySyncDone=false;
                  const node=document.querySelector('#sync-status .sync-description');
                  const observer=new MutationObserver(()=>{observer.disconnect();window.recoverySyncDone=true;});
                  observer.observe(node,{childList:true,characterData:true,subtree:true});
                  Object.defineProperty(document,'hidden',{configurable:true,get:()=>false});
                  document.dispatchEvent(new Event('visibilitychange'));
                }''')
                page.wait_for_function('() => window.recoverySyncDone === true')
                assert len([r for r in reads if r['path']=='table'])==before
                assert page.evaluate('ForgeView.decodeView(location.hash)')==failed
                pause()
                page.get_by_role('button',name='刷新当前视图',exact=True).click()
            elif action=='retry':page.get_by_role('button',name='重试打开数据表',exact=True).click()
            elif action=='reload':page.reload()
            else:
                page.locator('.function-nav').get_by_role('button',name='Readme',exact=True).click()
                page.locator('#project-readme').wait_for();page.go_back()
            page.locator('.case-record-selector button').first.wait_for()
            actual=page.evaluate('ForgeView.decodeView(location.hash)')
            assert actual['kind']=='table' and actual['alias']==alias and actual['binding_digest']==expected,actual
            assert page.locator('.case-record-selector button').count()==2
            if action=='history':
                page.go_forward();page.locator('#project-readme').wait_for()
                page.go_back();page.locator('.case-record-selector button').first.wait_for()
                assert page.evaluate('ForgeView.decodeView(location.hash).binding_digest')==expected
            records.append(dict(action=action,refused_http=503,failed_request=failed,restored=actual,rows=2))
            pause()

        candidate=project_ops.describe_table(root,'candidates')['binding']['digest']
        page.get_by_role('button',name='展开数据目录',exact=True).click()
        route_read,used=refuse_once('candidates')
        page.get_by_role('cell',name='candidates',exact=True).click()
        page.get_by_role('button',name='重试打开数据表',exact=True).wait_for()
        page.unroute('**/table?*',route_read)
        failed=page.evaluate('ForgeView.decodeView(location.hash)')
        assert failed['kind']=='table_request' and failed['alias']=='candidates' and failed['expected_binding']==candidate
        assert used[0]['expected_binding']==[candidate]
        control=root/'tables/candidates.json';assert control.stat().st_size<65536
        original=control.read_bytes()
        replacement=json.loads((root/'tables/comparison_fixture.json').read_text());replacement['alias']='candidates'
        try:
            control.write_text(json.dumps(replacement))
            sample_before=len([r for r in reads if r['path']=='sample'])
            with page.expect_response(lambda r:urlsplit(r.url).path.endswith('/table')) as mismatch:
                page.get_by_role('button',name='刷新当前视图',exact=True).click()
            assert mismatch.value.status==409
            page.get_by_role('button',name='重试打开数据表',exact=True).wait_for()
            assert page.evaluate('ForgeView.decodeView(location.hash)')==failed
            assert page.locator('.case-record-selector').count()==0
            assert len([r for r in reads if r['path']=='sample'])==sample_before
        finally:control.write_bytes(original)
        page.reload();page.locator('.case-record-selector button').first.wait_for()
        restored=page.evaluate('ForgeView.decodeView(location.hash)')
        assert restored['alias']=='candidates' and restored['binding_digest']==candidate
        records.append(dict(action='known_expected_binding',failed_request=failed,conflict_http=409,
            no_sample_after_mismatch=True,expected_binding_not_adopted_or_dropped=True,restored=restored))
        assert not errors,errors
        (out/'fixed-result-recovery.json').write_text(json.dumps(dict(contract=contract,actions=records,
            ordinary_sync_did_not_retry=True,errors=errors),ensure_ascii=False,indent=2)+'\n')
