"""Real browser/API/engine compiler; independent project definitions and per-flow inputs."""
import json
from pathlib import Path
from playwright.sync_api import expect
from demiflow.workbench.projects import registry
from test_workbench_project_definitions import project, original, snapshot, cached_snapshot
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_web_redesign import opened, artifact


def test_new_operator_and_prompt_appear_without_inspection_or_manual_refresh(project,browser):
    (project/'operators/clean.py').write_text('__demiflow_operators__ = ["clean"]\ndef clean(row):\n    return row\n')
    with service(project) as app, opened(browser,app) as (page,_):
        page.locator('.function-nav').get_by_role('button',name='算子',exact=True).click()
        page.get_by_role('button',name='ƒ clean',exact=True).click()
        (project/'operators/normalize.py').write_text('__demiflow_operators__ = ["normalize"]\ndef normalize(value):\n    return value\n')
        expect(page.get_by_role('button',name='ƒ normalize',exact=True)).to_be_visible(timeout=30000)
        page.get_by_role('tab',name='Prompt 算子',exact=True).click()
        (project/'prompts/summary.json').write_text(json.dumps({
            'schema_version': 'demiflow_prompt_pack_v2',
            'prompts': {'summarize': {'version': 'v1', 'template': 'Summarize {{ value }}'}},
        }))
        expect(page.get_by_role('button',name='P summarize',exact=True)).to_be_visible(timeout=30000)
        expect(page.get_by_role('tab',name='Prompt 算子',exact=True)).to_have_attribute('aria-selected','true')


def test_project_tree_prompt_flow_switch_arguments_reload(project,tmp_path,browser):
    for name, default in [('select',3),('evaluate',8)]:
        (project/f'flows/{name}.py').write_text(f'''from demiflow import data
from operators.clean import clean

def run(*, limit: int = {default}, label: str = "demo", note: str | None = "default"):
    """{name}"""
    return data.from_items([{{"item":label}}]).map(clean).limit(limit).take_all()
''')
    parameters=project/'settings/select.json'
    value=json.loads(parameters.read_text());value['arguments']['note']=None
    parameters.write_text(json.dumps(value))
    index=tmp_path/'console.json';registry.add(index,project)
    from demiflow.forge.platform import content_digest
    inventory=registry.load_registry(index)
    registry.migrate(index,expected=content_digest(inventory))
    inventory=registry.load_registry(index)
    registry.set_capabilities(index,next(iter(inventory['projects'])),expected=content_digest(inventory),capabilities={k:k=='config_write' for k in registry.CAPABILITIES})
    with service(None,registry=index,enable_config_write=True) as app,opened(browser,app) as (page,requests):
        rejected=[]
        def busy_once(route):
            if not rejected:
                rejected.append(True)
                route.fulfill(status=503,content_type='application/json',body=json.dumps(dict(error='Worker capacity exceeded',code='busy',operation_started=False)))
            else:
                route.continue_()
        page.route('**/plan-preview',busy_once)
        nav=page.locator('.function-nav')
        nav.get_by_role('button',name='算子',exact=True).click()
        expect(page.locator('.op-browser')).to_be_visible()
        expect(page.get_by_role('searchbox',name='查找算子')).to_be_hidden()
        page.get_by_role('button',name='查找算子',exact=True).click()
        page.get_by_role('searchbox',name='查找算子').fill('missing')
        expect(page.locator('.op-tree-items')).to_contain_text('没有匹配的算子')
        page.keyboard.press('Escape')
        expect(page.get_by_role('button',name='查找算子',exact=True)).to_be_focused()
        page.get_by_role('button',name='ƒ clean',exact=True).click()
        expect(page.locator('.op-content')).to_contain_text('return row')
        assert page.locator('.wb-node:visible').count()==0
        page.get_by_role('button',name='版本历史',exact=True).click()
        expect(page.locator('.op-history')).to_contain_text('未保存 Git')
        page.locator('.op-history').get_by_role('button',name='关闭').click()
        page.get_by_role('tab',name='代码算子',exact=True).focus()
        page.keyboard.press('ArrowRight')
        expect(page.get_by_role('tab',name='Prompt 算子',exact=True)).to_have_attribute('aria-selected','true')
        expect(page.get_by_role('tab',name='Prompt 算子',exact=True)).to_be_focused()
        expect(page.get_by_role('button',name='ƒ clean',exact=True)).to_have_count(0)
        page.get_by_role('button',name='P review',exact=True).click()
        expect(page.locator('.op-content')).to_contain_text('Review {{ value }}')
        page.get_by_role('button',name='算子目录',exact=True).click()
        expect(page.locator('.op-tree')).to_be_hidden()
        expect(page.locator('.op-content')).to_contain_text('Review {{ value }}')
        page.get_by_role('button',name='算子目录',exact=True).click()
        expect(page.get_by_role('tab',name='Prompt 算子',exact=True)).to_have_attribute('aria-selected','true')
        page.get_by_role('button',name='接口与默认值',exact=True).click()
        expect(page.locator('.op-content')).to_contain_text('输出 Schema')
        artifact(page,'project-prompts')
        nav.get_by_role('button',name='管线',exact=True).click()
        page.locator('.wb-flow-record[data-entry="flows/select.py:run"]').click()
        expect(page.locator('.wb-node')).to_have_count(4,timeout=45000)
        assert page.get_by_role('combobox',name='入参文件',exact=True).count()==0
        page.locator('.wb-node').filter(has_text='clean').first.click()
        page.locator('.wb-source-tabs').get_by_role('button',name='代码算子',exact=True).click()
        page.get_by_role('button',name='查看当前定义',exact=True).click()
        expect(page.locator('.op-heading')).to_contain_text('clean')
        expect(page.get_by_role('tab',name='代码算子',exact=True)).to_have_attribute('aria-selected','true')
        nav.get_by_role('button',name='管线',exact=True).click()
        page.get_by_role('button',name='业务参数',exact=True).click()
        expect(page.locator('.flow-arguments')).to_be_visible()
        expect(page.get_by_label('limit',exact=True)).to_have_value('3')
        page.get_by_label('limit',exact=True).fill('5')
        page.get_by_label('label',exact=True).fill('')
        expect(page.get_by_role('button',name='解析执行计划',exact=True)).to_be_disabled()
        # Switch without discarding this flow's draft.
        page.locator('.wb-flow-record[data-entry="flows/evaluate.py:run"]').click()
        page.get_by_role('button',name='业务参数',exact=True).click()
        expect(page.get_by_label('limit',exact=True)).to_have_value('8',timeout=30000)
        page.locator('.wb-flow-record[data-entry="flows/select.py:run"]').click()
        page.get_by_role('button',name='业务参数',exact=True).click()
        expect(page.get_by_label('limit',exact=True)).to_have_value('5',timeout=30000)
        expect(page.get_by_label('label',exact=True)).to_have_value('')
        page.get_by_role('button',name='保存参数',exact=True).click()
        expect(page.locator('.flow-argument-status')).to_contain_text('已保存',timeout=45000)
        page.get_by_role('button',name='关闭业务参数',exact=True).click()
        expect(page.locator('.flow-arguments')).to_be_hidden()
        page.get_by_role('button',name='解析执行计划',exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(4,timeout=45000)
        artifact(page,'project-flow')
        page.reload()
        expect(page.locator('.wb-node')).to_have_count(4,timeout=45000)
        page.get_by_role('button',name='业务参数',exact=True).click()
        expect(page.get_by_label('limit',exact=True)).to_have_value('5',timeout=30000)
        nav.get_by_role('button',name='算子',exact=True).click()
        expect(page.locator('.op-heading')).to_contain_text('clean')
        page.set_viewport_size({'width':780,'height':950})
        artifact(page,'project-operators-narrow')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth+1')
        assert rejected
    assert json.loads(parameters.read_text())['arguments']=={'limit':5,'label':'','note':None}
    assert json.loads((project/'settings/evaluate.json').read_text())['arguments']['limit']==8


def test_unwired_operator_editor_and_data_tools_in_their_own_pages(project,browser):
    import subprocess
    code=('def ready(row):\n    """读取一条记录的审核状态。"""\n'
          '    # 旧数据可以缺少审核状态。\n    return row.get("status", "<script>")\n')
    path=project/'operators/quality.py'
    path.write_text('__demiflow_operators__ = ["ready"]\n\n'+code+'\ndef card(row):\n    return str(row)\n')
    subprocess.run(['git','init','-q',str(project)],check=True)
    subprocess.run(['git','-C',str(project),'add','operators/quality.py'],check=True)
    subprocess.run(['git','-C',str(project),'-c','user.name=fixture','-c','user.email=test@example.invalid','commit','-qm','fixture'],check=True)
    path.write_text('# second revision\n'+path.read_text())
    data_tools=project/'data_tools'
    data_tools.mkdir()
    (data_tools/'review.py').write_text('__demiflow_data_tools__ = {"ReviewPage": "annotation"}\n'
        'class ReviewPage:\n    """分页浏览候选图。"""\n    pass\n')
    with service(project) as app,opened(browser,app) as (page,_):
        nav=page.locator('.function-nav')
        nav.get_by_role('button',name='算子',exact=True).click()
        page.get_by_role('button',name='ƒ ready',exact=True).click()
        expect(page.locator('.op-eyebrow')).to_contain_text('已声明算子')
        expect(page.get_by_text('未发现直接流程引用',exact=True)).to_be_visible()
        expect(page.locator('.op-tree')).not_to_contain_text('card')
        expect(page.locator('.op-tree')).not_to_contain_text('ReviewPage')
        editor=page.get_by_role('region',name='Python 源码')
        expect(editor.locator('.tok-keyword').first).to_have_text('def')
        expect(editor.locator('.tok-comment')).to_contain_text('旧数据')
        assert editor.locator('.python-lines').text_content()=='4\n5\n6\n7'
        assert editor.locator('.reading-source-page').text_content()==code
        assert not editor.locator('script').count()
        page.evaluate("Object.defineProperty(navigator, 'clipboard', {value:{writeText:async text=>window.copied=text}})")
        page.get_by_role('button',name='复制代码').click()
        assert page.evaluate('window.copied')==code
        artifact(page,'declared-operator')
        page.get_by_role('button',name='版本历史').click()
        page.locator('.op-version').last.click()
        expect(page.locator('.op-history-notice')).to_be_visible()
        expect(editor.locator('.python-lines')).to_have_text('3\n4\n5\n6',use_inner_text=False)
        page.set_viewport_size({'width':390,'height':800})
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        nav.get_by_role('button',name='数据',exact=True).click()
        page.get_by_role('tab',name='工具',exact=True).click()
        page.get_by_text('data_tools/review.py',exact=True).click()
        page.get_by_role('button',name='ReviewPage 标注',exact=True).click()
        dialog=page.get_by_role('dialog',name='标注工具')
        expect(dialog.get_by_role('heading',name='ReviewPage')).to_be_visible()
        dialog.get_by_text('查看源码',exact=True).click()
        expect(dialog.locator('.tok-keyword').first).to_have_text('class')
        expect(dialog).to_contain_text('分页浏览候选图')
        expect(dialog.get_by_role('button',name='插入 Notebook')).to_be_disabled()
        artifact(page,'project-data-tools')
        dialog.get_by_role('button',name='关闭').click()
        expect(page.locator('.native-notebook-host')).to_be_visible()


def test_python_editor_paging_keeps_multiline_syntax_and_exact_source(project,browser):
    # Put a page cut inside a multiline string, with a non-BMP character near the cut.
    source='def inspect_row(row):\n    return """'+('a'*16344)+'😀\n'+('z'*256)+'"""\n'
    (project/'operators/page.py').write_text('__demiflow_operators__=["inspect_row"]\n'+source)
    with service(project) as app,opened(browser,app,width=500) as (page,_):
        page.locator('.function-nav').get_by_role('button',name='算子',exact=True).click()
        editor=page.get_by_role('region',name='Python 源码')
        expect(editor.locator('.tok-string').first).to_be_visible()
        first=editor.locator('.reading-source-page').text_content()
        assert editor.evaluate('n=>n.scrollWidth>n.clientWidth')
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
        page.get_by_role('button',name='下一页原文').click()
        expect(editor.locator('.tok-string').first).to_be_visible()
        second=editor.locator('.reading-source-page').text_content()
        assert first+second==source
        assert editor.locator('.python-lines').text_content().splitlines()[0]==str(2+first.count('\n'))
