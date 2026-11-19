"""Reading/navigation acceptance on real HTTP and bounded owned Lance fixtures."""
from contextlib import contextmanager
import hashlib
import json
import os
import shutil
from pathlib import Path
import urllib.error
import urllib.request

import lance
import pyarrow as pa
import pytest

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.api import ASSETS
from demiflow.workbench.api import export_report
from demiflow.workbench import api as workspace_view
from test_forge_markdown_reading import browser, TRACK_WORKERS
from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes
from test_forge_public_api import service
from test_forge_workspace import demo, isolated, attach


@contextmanager
def opened(browser, app, *, width=1440):
    context=browser.new_context(viewport={'width':width,'height':1000})
    context.set_default_timeout(15000)
    page=context.new_page();errors=[];requests=[];csp=[]
    page.on('pageerror',lambda e:errors.append(str(e)))
    page.on('request',lambda r:requests.append((r.method,r.url)))
    page.on('console',lambda m:csp.append(m.text) if 'Content Security Policy' in m.text else None)
    page.goto(app.origin+'/#token='+app.token)
    page.locator('#sync-status').filter(has_text='最近同步').or_(page.get_by_role('heading',name='开始使用 DemiFlow')).first.wait_for()
    try:
        yield page,requests
        assert not errors,errors
        assert not csp,csp
        assert app.token not in page.url
    finally:
        context.close()


def artifact(page,name):
    folder=os.environ.get('FORGE_ACCEPTANCE_DIR')
    if folder:
        Path(folder).mkdir(parents=True,exist_ok=True)
        page.evaluate('scrollTo(0,0)')
        page.screenshot(path=str(Path(folder)/(name+'.png')),full_page=True)


def navigate(page,label):
    page.get_by_role('navigation',name='项目功能').get_by_role('button',name=label,exact=True).click()


def project_info(page):
    if not page.locator('.workbench').count():navigate(page,'管道')
    page.locator('.wb-more > summary').click()
    page.locator('.wb-more').get_by_role('button',name='检查问题',exact=True).click()
    disclosure=page.locator('.wb-checks details').filter(has=page.locator('summary').filter(has_text='当前项目检查与来源'))
    if disclosure.get_attribute('open') is None:disclosure.locator(':scope > summary').click()
    page.get_by_role('button',name='检查当前项目',exact=True).wait_for()
    return disclosure


def test_readme_safe_links_exact_source_and_responsive_layout(original,tmp_path,browser):
    text='''# Shipping guide

Read **existing content** before checking.

1. Open data
2. Check evidence

> Registration does not certify a pipeline.

| Key | Meaning |
| --- | --- |
| fixed | Version stays fixed |

```python
print("never executed")
```

[small](settings/small.json) · [source](dispatch.py) · [unknown](notes/no.md)

[local heading](#shipping-guide) · [external](https://example.org/guide)

[credential](https://example.org/?accessToken=secret)

![not loaded](https://example.org/never-fetch.png)

<script>globalThis.compromised=true</script>
'''
    (original/'README.md').write_text(text)
    registry=tmp_path/'registry.json';console.add(registry,original)
    before=business_bytes(original)
    with service(None,registry=registry) as app,opened(browser,app) as (page,requests):
        assert page.locator('#workspace-name').text_content()=='shipping'
        reader=page.locator('#project-readme .forge-reader')
        reader.get_by_role('heading',name='Shipping guide',exact=True).wait_for()
        assert reader.locator('ol,blockquote,table,pre code').count()==4
        assert reader.locator('script,img,iframe,svg').count()==0
        assert reader.locator('a').count()==1
        assert not page.evaluate('globalThis.compromised===true')
        assert not any('example.org' in url for _,url in requests)
        old_hash=page.url
        reader.get_by_role('button',name='local heading',exact=True).click()
        assert page.url==old_hash
        reader.get_by_role('button',name='small',exact=True).click()
        dialog=page.get_by_role('dialog');dialog.get_by_role('heading',name='settings/small.json').wait_for()
        assert dialog.locator('.reading-source-chunk').all_text_contents()==[(original/'settings/small.json').read_text()]
        assert dialog.locator('.reading-preview').count()==0
        dialog.get_by_role('button',name='关闭',exact=True).click()
        reader.get_by_role('button',name='source',exact=True).click()
        assert 'def run_pipeline' in page.get_by_role('dialog').inner_text()
        page.get_by_role('dialog').get_by_role('button',name='关闭',exact=True).click()
        for width in [1440,390,320]:
            page.set_viewport_size({'width':width,'height':1000})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
            artifact(page,f'readme-{width}')
        reader.get_by_role('button',name='原文',exact=True).click()
        assert ''.join(reader.locator('.reading-source-chunk').all_text_contents())==text
        artifact(page,'readme-source')
        assert not any(method=='POST' for method,_ in requests)
        assert business_bytes(original)==before


def test_both_case_sources_use_field_reading_with_fixed_versions(original,isolated,tmp_path,browser):
    text='## Arbitrary field\n\nA paragraph with **emphasis**.\n\n'+('Body content. '*30)
    rows=[{'identifier':'one','alpha':text,'config':'{\n  "enabled": true\n}',
           'nested':{'n':1},'image_uri':'images/c01.png'},
          {'identifier':'two','alpha':'## Second field\n\nSecond Case.','config':'{}','nested':{'n':2},'image_uri':'images/c02.png'}]
    schema=pa.schema([('identifier',pa.string()),('alpha',pa.string()),('config',pa.string()),
                      ('nested',pa.struct([('n',pa.int64())])),('image_uri',pa.string())])
    attach(isolated,'reading_fixture',rows,schema)
    description=ws.describe_table(isolated,'reading_fixture')
    (isolated/'README.md').write_text('# Bound project\n\n[fixed table](data/reading_fixture.lance)\n')
    original_data=original/'records/notes.lance'
    lance.write_dataset(pa.Table.from_pylist(rows,schema=schema),str(original_data))
    (original/'README.md').write_text('# Original\n\n[notes](records/notes.lance)\n`records/notes.lance`\n')
    registry=tmp_path/'registry.json';orig_id=console.add(registry,original)['workspace_id']
    bound_id=console.add(registry,isolated)['workspace_id']
    before=business_bytes(original)
    with service(None,registry=registry) as app,opened(browser,app) as (page,requests):
        page.locator('#project-switcher').click()
        page.locator(f'#project-list [data-workspace="{bound_id}"]').click()
        page.locator('#workspace-name').filter(has_text='question-experiments').wait_for()
        with page.expect_request(lambda r:'/table?' in r.url and 'alias=reading_fixture' in r.url) as bound_link:
            page.locator('#project-readme').get_by_role('button',name='fixed table',exact=True).click()
        assert ('expected_binding='+description['binding']['digest']) in bound_link.value.url
        page.get_by_role('heading',name='单条 case · 0',exact=True).wait_for()
        reference=dict(v=2,workspace=bound_id,kind='case',alias='reading_fixture',binding_digest=description['binding']['digest'],row_id='0')
        page.goto(app.origin+'/#view='+urllib.parse.quote(json.dumps(reference)))
        page.get_by_role('heading',name='单条 case · 0',exact=True).wait_for()
        page.get_by_role('heading',name='Arbitrary field',exact=True).wait_for()
        assert page.get_by_role('tab',name='alpha',exact=True).get_attribute('aria-selected')=='true'
        for width in [1440,390,320]:
            page.set_viewport_size({'width':width,'height':1000})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
            artifact(page,f'bound-case-{width}')
        page.locator('.case-field').get_by_role('button',name='原文',exact=True).click()
        assert ''.join(page.locator('.case-field .reading-source-chunk').all_text_contents())==text
        page.get_by_role('tab',name='config',exact=True).click()
        assert page.locator('.case-field .reading-preview').count()==0
        assert page.locator('.case-field .reading-source-chunk').inner_text()==rows[0]['config']
        page.get_by_role('tab',name='nested',exact=True).click()
        page.get_by_text('展开结构化内容',exact=True).click()
        assert json.loads(page.locator('.case-field .reading-source-chunk').inner_text())=={'n':1}
        page.get_by_role('tab',name='alpha',exact=True).click()
        assert page.locator('.case-field .forge-reader').get_attribute('data-mode')=='source'
        assert ''.join(page.locator('.case-field .reading-source-chunk').all_text_contents())==text
        page.get_by_role('tab',name='nested',exact=True).click()
        assert page.locator('.case-field details').get_attribute('open')==''
        page.get_by_role('tab',name='identifier',exact=True).click()
        assert page.locator('.case-field .scalar-value').inner_text()=='one'
        page.get_by_role('tab',name='identifier',exact=True).press('Home')
        assert page.get_by_role('tab').first.get_attribute('aria-selected')=='true'
        page.locator('.case-record-selector button').nth(1).click()
        page.get_by_role('heading',name='Second field',exact=True).wait_for()
        page.get_by_role('button',name='返回当前表',exact=True).click()
        assert page.evaluate('ForgeView.decodeView(location.hash).kind')=='table'
        # Explicit text/json declarations take precedence over content heuristics.
        ws.set_table_view(isolated,'reading_fixture',expected=description['digest'],display={'fields':[
            {'path':['config'],'kind':'text','label':'Declared text'},
            {'path':['alpha'],'kind':'json','label':'Declared raw'}]},provenance={})
        page.reload()
        page.get_by_role('tab',name='Declared text',exact=True).wait_for()
        assert page.locator('.case-field .forge-reader').get_attribute('data-mode')=='preview'
        page.get_by_role('tab',name='Declared raw',exact=True).click()
        assert page.locator('.case-field .reading-preview').count()==0
        # Original-project Case uses its existing project-sample route and version.
        page.set_viewport_size({'width':1440,'height':1000})
        page.locator('#project-switcher').click()
        page.locator(f'#project-list [data-workspace="{orig_id}"]').click()
        page.locator('#workspace-name').filter(has_text='shipping').wait_for()
        navigate(page,'数据')
        page.get_by_role('cell',name='records/notes.lance',exact=True).click()
        page.get_by_text('本次查看固定为 v1 · 2 行',exact=True).wait_for()
        # A newer head does not replace the open version or sample binding.
        lance.write_dataset(pa.Table.from_pylist([dict(rows[0],alpha='NEW HEAD')],schema=schema),str(original_data),mode='overwrite')
        page.locator('.original-viewer table tbody tr').first.click()
        page.get_by_role('heading',name='Arbitrary field',exact=True).wait_for()
        for width in [1440,390,320]:
            page.set_viewport_size({'width':width,'height':1000})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
            artifact(page,f'original-case-{width}')
        assert page.get_by_role('button',name='查看本地图片',exact=True).count()==0
        page.locator('.case-field').get_by_role('button',name='原文',exact=True).click()
        assert ''.join(page.locator('.case-field .reading-source-chunk').all_text_contents())==text
        page.get_by_role('button',name='返回当前表',exact=True).click()
        assert '#view=' in page.url and 'project-sample' not in page.url
        # The only business edit above was the explicitly owned fixture head.
        after=business_bytes(original)
        assert all(after[k]==v for k,v in before.items() if not k.startswith('records/notes.lance/'))
        assert not any(method=='POST' for method,_ in requests)


def test_four_first_states_and_single_project_permissions(tmp_path,isolated,browser):
    registry=tmp_path/'registry.json';registry.write_text(json.dumps({'format':console.FORMAT,'projects':{}}))
    with service(None,registry=registry) as app,opened(browser,app) as (page,_):
        assert page.locator('[data-onboarding=empty]').count()==1
        assert not page.get_by_role('navigation',name='项目功能').is_visible()
        artifact(page,'empty-1440')
        page.set_viewport_size({'width':390,'height':844});artifact(page,'empty-390')
        page.get_by_role('button',name='新建项目',exact=True).click()
        dialog=page.get_by_role('dialog');dialog.get_by_label('项目目录').fill(str(tmp_path/'new'))
        dialog.get_by_label('项目名称').fill('Created project');dialog.get_by_role('button',name='新建项目',exact=True).click()
        page.locator('#project-readme').wait_for()
        artifact(page,'created-390')
        assert '尚无 README.md' in page.locator('main').inner_text()
        navigate(page,'管道')
        expect=pytest.importorskip('playwright.sync_api').expect
        expect(page.get_by_role('button',name='运行',exact=True)).to_be_disabled()
        expect(page.locator('.wb-graph')).to_contain_text('尚无已声明入口')
        assert not page.get_by_text('识别失败',exact=True).count()
    with service(isolated) as app,opened(browser,app) as (page,_):
        assert page.locator('#workspace-name').text_content()=='question-experiments'
        assert page.get_by_text('可查看、检查 · 运行未开启',exact=True).count()==1
        assert page.get_by_role('button',name='注册已有项目',exact=True).count()==0
        artifact(page,'existing-1440')


def test_static_asset_allowlist_csp_api_only_and_offline_file(tmp_path,demo,browser,monkeypatch):
    root,reports=demo
    names=['reading.css','reading.js','markdown-worker.js','markdown-it.min.js','markdown-it.LICENSE.txt','markdown-it.NOTICES.txt','markdown-it.provenance.json']
    with service(root) as app:
        for name in names:
            with urllib.request.urlopen(app.origin+'/'+name) as response:
                assert response.read()==(ASSETS/name).read_bytes()
                assert "worker-src 'self'" in response.headers['Content-Security-Policy']
                assert 'unsafe-' not in response.headers['Content-Security-Policy']
                assert response.headers['Cache-Control']=='no-store'
                assert response.headers['X-Content-Type-Options']=='nosniff'
        with pytest.raises(urllib.error.HTTPError):urllib.request.urlopen(app.origin+'/../workspace.py')
    with service(root,api_only=True) as app:
        for name in names:
            with pytest.raises(urllib.error.HTTPError) as error:urllib.request.urlopen(app.origin+'/'+name)
            assert error.value.code in [403,404]
    # Exercise the final exporter with a malicious report string, no new samples.
    report=ws.load_report(root,reports['a'])
    report['description']='</script><script>globalThis.offlineExecuted=true</script>'
    monkeypatch.setattr(ws,'load_report',lambda *_:report)
    assets=tmp_path/'export-assets';shutil.copytree(ASSETS,assets)
    with (assets/'app.js').open('a') as script:script.write("\nglobalThis.assetTerminator='</ScRiPt>';\n")
    monkeypatch.setattr(workspace_view,'ASSETS',assets)
    output=tmp_path/'final-offline.html';result=export_report(root,reports['a'],output)
    assert result['samples_included'] is False and result['current_binding']=='not_live'
    content=output.read_text();assert 'src="/' not in content and 'href="/' not in content
    context=browser.new_context();page=context.new_page();network=[];errors=[]
    page.add_init_script("Object.defineProperty(globalThis,'sessionStorage',{get(){throw Error('offline must not read storage')}});globalThis.Worker=function(){throw Error('offline worker is forbidden')}")
    page.on('request',lambda r:network.append(r.url) if r.url.startswith(('http:','https:')) else None)
    page.on('pageerror',lambda e:errors.append(str(e)))
    page.goto(output.as_uri());page.get_by_role('heading',name='接线检查报告',exact=True).wait_for()
    assert page.title()=='DemiFlow · 项目工作台'
    assert page.locator('.brand strong').inner_text()=='DemiFlow'
    assert page.locator('.wb-graph .wb-node').count()==7
    page.get_by_text('第三方许可证与通知',exact=True).click()
    assert page.locator('.export-notices pre').text_content()==(ASSETS/'markdown-it.LICENSE.txt').read_text()+'\n\n'+(ASSETS/'markdown-it.NOTICES.txt').read_text()
    assert not page.evaluate('globalThis.offlineExecuted===true')
    assert page.evaluate('globalThis.assetTerminator')=='</ScRiPt>'
    assert not network and not errors
    artifact(page,'offline-report');context.close()


def test_case_change_disposes_reader_and_discards_late_worker(isolated,tmp_path,browser):
    schema=pa.schema([('payload',pa.string())])
    attach(isolated,'worker_fixture',[{'payload':'# First document\n\n'+('first '*80)},
        {'payload':'# Current document\n\n'+('current '*80)},
        {'payload':'x'*70000}],schema)
    description=ws.describe_table(isolated,'worker_fixture')
    with service(isolated) as app:
        context=browser.new_context();context.set_default_timeout(15000)
        page=context.new_page();page.add_init_script(TRACK_WORKERS)
        page.goto(app.origin+'/#token='+app.token)
        page.locator('#workspace-name').filter(has_text='question-experiments').wait_for()
        page.wait_for_function('() => workerStats.results.length>0')
        page.evaluate('globalThis.holdFirstResult=true;globalThis.didHold=false')
        reference=dict(v=2,workspace=ws.load_workspace(isolated)[1]['id'],kind='case',alias='worker_fixture',binding_digest=description['binding']['digest'],row_id='0')
        # Keep the same document/worker instrumentation and dispatch normal navigation.
        page.evaluate("value=>{history.pushState(null,'',ForgeView.encodeView(value));dispatchEvent(new PopStateEvent('popstate'));}",reference)
        page.wait_for_function('() => workerStats.held.length===1')
        page.locator('.case-record-selector button').nth(1).click()
        page.get_by_role('heading',name='Current document',exact=True).wait_for()
        page.evaluate('flushHeld()')
        assert page.get_by_role('heading',name='First document',exact=True).count()==0
        assert page.get_by_role('heading',name='Current document',exact=True).count()==1
        assert page.evaluate('workerStats.maxAlive')==1
        page.locator('.case-record-selector button').nth(2).click()
        page.locator('.case-field .forge-reader[data-mode=source]').wait_for()
        assert '预算' in page.locator('.reading-status').inner_text()
        assert sum(len(s.encode()) for s in page.locator('.reading-source-chunk').all_text_contents())<=65536
        assert page.evaluate('workerStats.maxAlive')==1
        context.close()


def test_narrow_navigation_keyboard_and_history_preserve_read_only_context(isolated,browser):
    with service(isolated) as app,opened(browser,app,width=390) as (page,requests):
        toggle=page.locator('#project-switcher')
        assert not page.locator('#project-picker').is_visible()
        toggle.focus();toggle.press('Enter')
        assert toggle.get_attribute('aria-expanded')=='true'
        projects=page.get_by_role('navigation',name='项目',exact=True)
        assert projects.is_visible()
        artifact(page,'navigation-390')
        projects.get_by_role('button',name='question-experiments',exact=True).press('Enter')
        assert not page.locator('#project-picker').is_visible()
        assert toggle.evaluate('(button)=>document.activeElement===button')
        page.get_by_role('navigation',name='项目功能').get_by_role('button',name='管道',exact=True).focus()
        page.keyboard.press('Enter')
        page.locator('.workbench').wait_for()
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='flows'")
        assert not page.locator('#project-picker').is_visible()
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='flows'")
        toggle.click()
        projects.get_by_role('button',name='question-experiments',exact=True).focus()
        page.keyboard.press('Escape')
        assert toggle.get_attribute('aria-expanded')=='false'
        assert toggle.evaluate('(button)=>document.activeElement===button')
        page.go_back();page.locator('#workspace-name').filter(has_text='question-experiments').wait_for()
        assert page.evaluate('ForgeView.decodeView(location.hash).kind')=='overview'
        page.set_viewport_size({'width':320,'height':740})
        navigate(page,'数据')
        page.get_by_role('heading',name='单条 case · 8589934592',exact=True).wait_for()
        first=page.get_by_role('tab').first;first.focus();first.press('End')
        assert page.get_by_role('tab').last.get_attribute('aria-selected')=='true'
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        assert not any(method=='POST' for method,_ in requests)
        assert page.locator('img.case-image').count()==0


def test_compact_header_keeps_status_and_actions_accessible(isolated,browser):
    expect=pytest.importorskip('playwright.sync_api').expect
    with service(isolated) as app,opened(browser,app) as (page,requests):
        status=page.locator('#workspace-status');summary=page.locator('#workspace-status > summary')
        top=page.locator('.workspace-top')
        assert not page.locator('.workspace-status-panel').is_visible()
        assert page.locator('#sync-status .sync-caption').inner_text()=='已同步'
        for width in [1440,900,760,390,320]:
            page.set_viewport_size({'width':width,'height':1000})
            before=top.bounding_box()['height']
            assert before<=84 if width>760 else before<=92
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            for label in ['项目设置','刷新当前视图','帮助']:
                assert page.get_by_role('button',name=label,exact=True).is_visible()
            artifact(page,f'compact-header-{width}')
            summary.click()
            assert page.get_by_text('可查看、检查 · 运行未开启',exact=True).is_visible()
            assert page.locator('#workspace-sync-detail').is_visible()
            assert top.bounding_box()['height']==before
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            artifact(page,f'compact-status-{width}')
            summary.press('Escape')
            assert status.get_attribute('open') is None
            expect(summary).to_be_focused()
        # Outside actions close the disclosure and keep the original operation.
        summary.click()
        page.get_by_role('button',name='帮助',exact=True).click()
        page.get_by_role('dialog').get_by_role('heading',name='使用 DemiFlow',exact=True).wait_for()
        assert status.get_attribute('open') is None
        page.get_by_role('dialog').get_by_role('button',name='关闭',exact=True).click()
        page.get_by_role('button',name='项目设置',exact=True).click()
        page.get_by_role('dialog').get_by_role('heading',name='项目设置',exact=True).wait_for()
        page.get_by_role('dialog').get_by_role('button',name='关闭',exact=True).click()
        # A failed refresh remains visible without expanding the compact header.
        page.route('**/changes',lambda route:route.fulfill(status=503,content_type='text/html',body='busy'))
        page.get_by_role('button',name='刷新当前视图',exact=True).click()
        page.locator('#sync-status.sync-error').wait_for()
        assert status.get_attribute('data-state')=='error'
        assert page.locator('#sync-status .sync-caption').inner_text()=='同步异常'
        assert top.bounding_box()['height']<=92
        assert page.locator('.workspace-top .error-box').count()==0
        summary.click()
        assert 'HTTP 503' in page.locator('#workspace-sync-detail').inner_text()
        page.unroute('**/changes')
        page.get_by_role('button',name='刷新当前视图',exact=True).click()
        expect(page.get_by_role('button',name='刷新当前视图',exact=True)).to_be_enabled(timeout=30000)
        expect(status).to_have_attribute('data-state','ready')
        assert not any(method=='POST' for method,_ in requests)


def test_project_picker_preserves_tabs_and_releases_the_full_workspace(isolated,original,tmp_path,browser):
    expect=pytest.importorskip('playwright.sync_api').expect
    registry=tmp_path/'registry.json'
    bound_id=console.add(registry,isolated)['workspace_id']
    original_id=console.add(registry,original)['workspace_id']
    with service(None,registry=registry) as app,opened(browser,app) as (page,requests):
        assert page.title()=='DemiFlow · 项目工作台'
        assert page.locator('.brand strong').inner_text()=='DemiFlow'
        toggle=page.locator('#project-switcher')
        def choose(identity):
            toggle.click()
            page.locator(f'#project-list [data-workspace="{identity}"]').click()
            page.wait_for_function("id=>ForgeView.decodeView(location.hash)?.workspace===id",arg=identity)
        choose(bound_id)
        page.locator('#workspace-name').filter(has_text='question-experiments').wait_for()
        navigate(page,'管道')
        page.locator('.workbench').wait_for()
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='flows'")
        assert page.locator('#project-sidebar').count()==0
        assert page.locator('.workspace-content').bounding_box()['x']==0
        choose(original_id)
        page.locator('#workspace-name').filter(has_text='shipping').wait_for()
        choose(bound_id)
        page.wait_for_function("identity => {const value=ForgeView.decodeView(location.hash);return value?.kind==='flows'&&value.workspace===identity;}",arg=bound_id)
        page.reload()
        page.locator('.workbench').wait_for()
        for width in [1440,390]:
            page.set_viewport_size({'width':width,'height':1000})
            toggle.click()
            assert page.locator('#project-list .project-name').first.is_visible()
            assert page.evaluate('ForgeView.decodeView(location.hash).kind')=='flows'
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            artifact(page,'demiflow-project-picker-'+str(width))
            page.keyboard.press('Escape')
            expect(toggle).to_be_focused()
        for label in ['新建项目','注册已有项目']:
            toggle.click()
            page.locator('.project-header').get_by_role('button',name=label,exact=True).click()
            page.get_by_role('dialog').get_by_role('button',name='取消',exact=True).click()
        assert all(url.endswith(('/plan-preview','/plan-node-detail')) for method,url in requests if method=='POST')


def test_project_rows_have_independent_tabs_and_restore_history(isolated,original,tmp_path,browser):
    expect=pytest.importorskip('playwright.sync_api').expect
    registry=tmp_path/'registry.json'
    bound_id=console.add(registry,isolated)['workspace_id']
    original_name='shared/projects/a-long-project-name-that-stays-on-one-row'
    original_id=console.add(registry,original,name=original_name)['workspace_id']
    broken=tmp_path/'unavailable';broken.mkdir()
    with service(None,registry=registry) as app,opened(browser,app) as (page,requests):
        projects=page.locator('#project-list')
        tabs=page.get_by_role('navigation',name='项目功能',exact=True)
        row=lambda identity:projects.locator(f'[data-workspace="{identity}"]')
        # A CLI registration appears in the same list during automatic sync.
        broken_id=console.add(registry,broken)['workspace_id']
        (broken/'workspace.json').write_text('{invalid')
        row(broken_id).wait_for(state='attached')
        page.locator('#project-switcher').click()
        row(bound_id).click()
        page.locator('#workspace-name').filter(has_text='question-experiments').wait_for()
        page.locator('#project-switcher').click()
        assert projects.get_by_role('button').count()==3
        assert page.locator('#project-sidebar select').count()==0
        boxes=[item.bounding_box() for item in projects.get_by_role('button').all()]
        assert all(box['height']<=50 and box['x']==boxes[0]['x'] for box in boxes)
        assert all(a['y']+a['height']<=b['y'] for a,b in zip(boxes,boxes[1:]))
        assert row(original_id).get_attribute('title')==original_name+'\n'+str(original)
        assert row(broken_id).get_by_text('暂不可用',exact=True).is_visible()
        tab_boxes=[item.bounding_box() for item in tabs.get_by_role('button').all()]
        assert len(tab_boxes)==6 and all(box['y']==tab_boxes[0]['y'] for box in tab_boxes)
        assert page.locator('.workspace-content').bounding_box()['x']==0
        assert page.locator('.workspace-top').bounding_box()['y']==0
        artifact(page,'projects-and-tabs-1440')

        navigate(page,'管道')
        page.locator('.workbench').wait_for()
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='flows'")
        page.locator('#project-switcher').click()
        row(original_id).click()
        page.locator('#workspace-name').filter(has_text=original_name).wait_for()
        page.wait_for_function("id => {const v=ForgeView.decodeView(location.hash);return v?.workspace===id&&v.kind==='flows';}",arg=original_id)
        assert page.evaluate('ForgeView.decodeView(location.hash).kind')=='flows'
        navigate(page,'数据')
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='tables'")
        page.locator('#project-switcher').click()
        row(bound_id).click()
        page.locator('.workbench').wait_for()
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='flows'")
        assert row(bound_id).get_attribute('aria-current')=='true'
        assert tabs.get_by_role('button',name='管道',exact=True).get_attribute('aria-current')=='page'
        page.locator('#project-switcher').click()
        row(original_id).click()
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='tables'")
        assert row(original_id).get_attribute('aria-current')=='true'
        artifact(page,'projects-and-tabs-original-1440')
        page.go_back()
        page.locator('.workbench').wait_for()
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='flows'")
        assert row(bound_id).get_attribute('aria-current')=='true'
        page.go_forward()
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='tables'")
        assert row(original_id).get_attribute('aria-current')=='true'

        # An unavailable row stays visible and cannot trap navigation.
        page.locator('#project-switcher').click()
        row(broken_id).click()
        page.locator('main > .load-failure').first.wait_for()
        page.locator('#project-switcher').click()
        row(bound_id).click()
        try:
            page.locator('.workbench').wait_for()
        except Exception:
            (tmp_path/'project-recovery-failure.txt').write_text(page.locator('main').inner_text()+'\n'+json.dumps(dict(view=page.evaluate('ForgeView.decodeView(location.hash)'),requests=requests[-40:]),ensure_ascii=False))
            page.screenshot(path=str(tmp_path/'project-recovery-failure.png'))
            raise
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='flows'")
        assert not page.locator('main > .load-failure').count()

        # A deep link selects its explicit project/tab ahead of session memory.
        reference=dict(v=2,workspace=original_id,kind='runs')
        page.goto(app.origin+'/#view='+urllib.parse.quote(json.dumps(reference)))
        page.locator('.wb-history').wait_for()
        assert row(original_id).get_attribute('aria-current')=='true'
        assert tabs.get_by_role('button',name='运行监控',exact=True).get_attribute('aria-current')=='page'
        for width in [390,320]:
            page.set_viewport_size({'width':width,'height':844})
            assert tabs.is_visible() and not page.locator('#project-picker').is_visible()
            expect(tabs.get_by_role('button',name='运行监控',exact=True)).to_be_in_viewport()
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            artifact(page,f'projects-and-tabs-{width}')
        assert all(url.endswith(('/plan-preview','/plan-node-detail')) for method,url in requests if method=='POST')


def test_original_table_link_keeps_its_selection_in_a_mixed_project(isolated,browser):
    records=isolated/'records';records.mkdir()
    lance.write_dataset(pa.table({'payload':['## Original selection\n\n'+('original '*40)]}),str(records/'notes.lance'))
    (isolated/'README.md').write_text('# Mixed project\n\n[original table](records/notes.lance)\n`records/notes.lance`\n')
    with service(isolated) as app,opened(browser,app) as (page,requests):
        page.locator('#project-readme').get_by_role('button',name='original table',exact=True).click()
        page.wait_for_function("() => ForgeView.decodeView(location.hash)?.kind==='tables'")
        page.get_by_text('本次查看固定为 v1 · 1 行',exact=True).wait_for()
        assert page.locator('#case-browser').count()==0
        page.locator('.original-viewer table tbody tr').first.click()
        page.get_by_role('heading',name='Original selection',exact=True).wait_for()
        assert page.evaluate('ForgeView.decodeView(location.hash).kind')=='tables'
        assert not any(method=='POST' for method,_ in requests)
