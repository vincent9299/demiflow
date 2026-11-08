"""Browser local source editing on inactive analysis; no kernel or helper."""
from playwright.sync_api import expect
from contextlib import contextmanager
from pathlib import Path
import pytest

from test_forge_workspace import demo,isolated
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from demiflow.workbench.projects import registry as console


@contextmanager
def page_for(browser,app,tmp_path):
    context=browser.new_context(viewport=dict(width=1440,height=1050));page=context.new_page()
    page.set_default_timeout(65000);errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
    try:
        page.goto(app.origin+'/#token='+app.token)
        page.locator('.function-nav').get_by_role('button',name='数据',exact=True).wait_for()
        yield page,errors
    except BaseException:
        page.screenshot(path=str(tmp_path/'analysis-browser-failure.png'),full_page=True)
        raise
    finally:context.close()


@pytest.mark.parametrize('failure',[None,'QuotaExceededError','SecurityError'])
def test_local_notebook_source_survives_project_navigation_without_execution(isolated,browser,tmp_path,failure):
    with service(isolated) as app,page_for(browser,app,tmp_path) as (page,errors):
        writes=[];page.on('request',lambda request:writes.append(request.url) if request.method=='POST' else None)
        page.get_by_role('button',name='数据',exact=True).click()
        page.get_by_role('button',name='Notebook',exact=True).click()
        editor=page.locator('.analysis-notebook')
        expect(editor).to_be_visible()
        expect(editor.get_by_role('button',name='保存 Notebook',exact=True)).to_be_disabled()
        expect(editor.get_by_role('button',name='启动内核',exact=True)).to_be_disabled()
        area=editor.get_by_label('python 单元代码',exact=True)
        if failure:page.evaluate('name=>Storage.prototype.setItem=function(){throw new DOMException("injected",name);}',failure)
        area.fill('print("browser draft retained")')
        page.get_by_role('button',name='管道',exact=True).click()
        expect(page.locator('.wb-selectors')).to_be_visible(timeout=65000)
        page.get_by_role('button',name='数据',exact=True).click()
        page.get_by_role('button',name='Notebook',exact=True).click()
        expect(page.get_by_label('python 单元代码',exact=True)).to_have_value('print("browser draft retained")')
        assert writes==[]
        assert not errors,errors


def test_source_import_keeps_old_draft_distinct_and_narrow_layout_fits(isolated,browser,tmp_path):
    with service(isolated) as app,page_for(browser,app,tmp_path) as (page,errors):
        page.get_by_role('button',name='数据',exact=True).click()
        page.get_by_role('button',name='Notebook',exact=True).click()
        editor=page.locator('.analysis-notebook')
        area=editor.get_by_label('python 单元代码',exact=True);area.fill('original draft')
        old=editor.get_by_label('选择 Notebook',exact=True).input_value()
        source='{"nbformat":4,"cells":[{"cell_type":"code","source":"print(123)","metadata":{},"outputs":[{"data":{"text/html":"<script>bad()</script>"}}]}]}'
        editor.locator('input[type=file]').set_input_files(dict(name='imported.ipynb',mimeType='application/json',buffer=source.encode()))
        expect(editor.get_by_label('python 单元代码',exact=True)).to_have_value('print(123)')
        assert editor.get_by_label('选择 Notebook',exact=True).input_value()!=old
        editor.get_by_label('选择 Notebook',exact=True).select_option(old)
        expect(editor.get_by_label('python 单元代码',exact=True)).to_have_value('original draft')
        editor.locator('.analysis-file-actions > summary').click()
        for width in (1440,390,320):
            page.set_viewport_size(dict(width=width,height=900))
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
            expect(editor.get_by_role('button',name='导出 .ipynb',exact=True)).to_be_visible()
            page.screenshot(path=str(tmp_path/('analysis-notebook-'+str(width)+'.png')),full_page=True)
        assert not errors,errors


@pytest.mark.parametrize('failure',['QuotaExceededError','SecurityError'])
def test_denied_storage_preserves_invalid_source_across_new_import_and_dispose(browser,tmp_path,failure):
    context=browser.new_context(viewport=dict(width=1100,height=850));page=context.new_page()
    page.route('**/*',lambda route:route.fulfill(status=200,content_type='text/html',body='<main id="mount"></main>'))
    assets=Path(__file__).resolve().parents[1]/'demiflow/workbench/web'
    key='w'+'1'*32;other='w'+'2'*32
    try:
        page.goto('https://source-editor-test.invalid/')
        for name in ('analysis-state.js','analysis.js'):page.add_script_tag(path=str(assets/name))
        page.evaluate('''project=>{
          window.calls=[];
          window.mountEditor=project=>window.editor=ForgeAnalysis.mount({parent:document.getElementById('mount'),project,
            connected:false,permissions:{},api:async(...args)=>{calls.push(args);throw Error('unexpected API');},
            mountText:()=>{},disposeReaders:()=>{}});
          mountEditor(project);
        }''',key)
        page.evaluate('''name=>{
          window.restoreSetItem=Storage.prototype.setItem;
          Storage.prototype.setItem=function(){throw new DOMException('injected write refusal',name);};
        }''',failure)
        old=page.get_by_label('选择 Notebook',exact=True).input_value()
        page.get_by_label('Notebook 名称',exact=True).fill('')
        page.get_by_label('python 单元代码',exact=True).fill('print("must survive denied storage")')
        page.get_by_role('button',name='新建',exact=True).click()
        expect(page.get_by_label('选择 Notebook',exact=True)).not_to_have_value(old)
        page.get_by_label('选择 Notebook',exact=True).select_option(old)
        expect(page.get_by_label('python 单元代码',exact=True)).to_have_value('print("must survive denied storage")')
        expect(page.get_by_label('Notebook 名称',exact=True)).to_have_value('')
        page.locator('.analysis-file-actions > summary').click()
        with page.expect_download() as download:page.get_by_role('button',name='导出草稿',exact=True).click()
        target=tmp_path/'exported-invalid-draft.json';download.value.save_as(target)
        import json
        exported=json.loads(target.read_bytes())
        assert exported['document']['title']=='' and exported['document']['cells'][0]['source']=='print("must survive denied storage")'
        page.locator('input[type=file]').set_input_files(dict(name='recovered.json',mimeType='application/json',buffer=target.read_bytes()))
        expect(page.get_by_label('python 单元代码',exact=True)).to_have_value('print("must survive denied storage")')
        page.evaluate('''other=>{if(!editor.dispose())throw Error('must be captured');document.getElementById('mount').replaceChildren();mountEditor(other);}''',other)
        page.evaluate('''project=>{if(!editor.dispose())throw Error('other project must be captured');document.getElementById('mount').replaceChildren();mountEditor(project);}''',key)
        expect(page.get_by_label('python 单元代码',exact=True)).to_have_value('print("must survive denied storage")')
        assert page.evaluate('calls.length')==0
        assert page.evaluate('()=>{const event=new Event("beforeunload",{cancelable:true});dispatchEvent(event);return event.defaultPrevented;}')
        # Storage is restored explicitly; all retained envelopes can now be
        # written, and a fresh Store reads the exact source/revision/binding.
        page.evaluate('Storage.prototype.setItem=restoreSetItem;editor.canLeave()')
        values=page.evaluate('project=>new ForgeAnalysisState.Store().drafts(project)',key)
        assert any(item['document']['cells'] and item['document']['cells'][0]['source']=='print("must survive denied storage")' for item in values)
    finally:context.close()


def test_full_bounded_draft_capacity_blocks_departure_without_overwriting_editor(browser):
    context=browser.new_context();page=context.new_page()
    page.route('**/*',lambda route:route.fulfill(status=200,content_type='text/html',body='<main id="mount"></main>'))
    assets=Path(__file__).resolve().parents[1]/'demiflow/workbench/web';key='w'+'3'*32
    try:
        page.goto('https://source-capacity-test.invalid/')
        for name in ('analysis-state.js','analysis.js'):page.add_script_tag(path=str(assets/name))
        page.evaluate('''project=>{
          window.editor=ForgeAnalysis.mount({parent:document.getElementById('mount'),project,connected:false,permissions:{},
            api:async()=>{throw Error('must not call API');},mountText:()=>{},disposeReaders:()=>{}});
          const store=new ForgeAnalysisState.Store();for(let n=0;n<4;n++)store.saveDraft(project,'existing-'+n,
            {revision:0,document:{title:'existing',cells:[]}});
        }''',key)
        page.get_by_label('python 单元代码',exact=True).fill('unrecorded source stays in original editor')
        old=page.get_by_label('选择 Notebook',exact=True).input_value()
        page.get_by_role('button',name='新建',exact=True).click()
        expect(page.get_by_label('选择 Notebook',exact=True)).to_have_value(old)
        expect(page.get_by_label('python 单元代码',exact=True)).to_have_value('unrecorded source stays in original editor')
        assert page.evaluate('editor.canLeave()') is False
        assert page.evaluate('editor.dispose()') is False
        expect(page.get_by_label('python 单元代码',exact=True)).to_be_visible()
        assert page.evaluate('()=>{const event=new Event("beforeunload",{cancelable:true});dispatchEvent(event);return event.defaultPrevented;}')
        page.get_by_role('button',name='放弃当前未保存编辑',exact=True).click()
        assert page.evaluate('editor.canLeave()') is True
        assert page.evaluate('editor.dispose()') is True
    finally:context.close()


def test_quota_failure_project_switch_keeps_original_project_source(isolated,browser,tmp_path):
    registry=tmp_path/'registry.json'
    first=console.add(registry,isolated)['workspace_id']
    second=console.create(registry,tmp_path/'second-project',name='second')['workspace_id']
    with service(None,registry=registry) as app,page_for(browser,app,tmp_path) as (page,errors):
        page.locator('[data-workspace="'+first+'"]').click()
        page.get_by_role('button',name='数据',exact=True).click()
        page.get_by_role('button',name='Notebook',exact=True).click()
        page.evaluate('()=>{Storage.prototype.setItem=function(){throw new DOMException("injected","QuotaExceededError");};}')
        page.get_by_label('python 单元代码',exact=True).fill('print("project A source")')
        page.locator('[data-workspace="'+second+'"]').click()
        expect(page.locator('[data-workspace="'+second+'"]')).to_have_attribute('aria-current','true')
        page.locator('[data-workspace="'+first+'"]').click()
        page.get_by_role('button',name='数据',exact=True).click()
        page.get_by_role('button',name='Notebook',exact=True).click()
        expect(page.get_by_label('python 单元代码',exact=True)).to_have_value('print("project A source")')
        assert not errors,errors
