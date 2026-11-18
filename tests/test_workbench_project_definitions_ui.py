"""Real browser/API/engine compiler; independent project definitions and per-flow inputs."""
import json
from pathlib import Path
from playwright.sync_api import expect
from demiflow.workbench.projects import registry
from test_workbench_project_definitions import project, original, snapshot, cached_snapshot
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_web_redesign import opened, artifact


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
        page.get_by_role('button',name='ƒ clean',exact=True).click()
        expect(page.locator('.op-content')).to_contain_text('return row')
        assert page.locator('.wb-node:visible').count()==0
        page.get_by_role('button',name='版本历史',exact=True).click()
        expect(page.locator('.op-history')).to_contain_text('未保存 Git')
        page.locator('.op-history').get_by_role('button',name='关闭').click()
        page.get_by_role('button',name='P review',exact=True).click()
        expect(page.locator('.op-content')).to_contain_text('Review {{ value }}')
        page.get_by_role('button',name='接口与默认值',exact=True).click()
        expect(page.locator('.op-content')).to_contain_text('输出 Schema')
        artifact(page,'project-prompts')
        nav.get_by_role('button',name='管道',exact=True).click()
        page.get_by_role('combobox',name='流程',exact=True).select_option('flows/select.py:run')
        expect(page.locator('.wb-node')).to_have_count(4,timeout=45000)
        assert page.get_by_role('combobox',name='入参文件',exact=True).count()==0
        page.locator('.wb-node').filter(has_text='clean').first.click()
        page.locator('.wb-source-tabs').get_by_role('button',name='代码算子',exact=True).click()
        page.get_by_role('button',name='查看当前定义',exact=True).click()
        expect(page.locator('.op-heading')).to_contain_text('clean')
        nav.get_by_role('button',name='管道',exact=True).click()
        page.get_by_role('button',name='业务参数',exact=True).click()
        expect(page.locator('.flow-arguments')).to_be_visible()
        expect(page.get_by_label('limit',exact=True)).to_have_value('3')
        page.get_by_label('limit',exact=True).fill('5')
        page.get_by_label('label',exact=True).fill('')
        expect(page.get_by_role('button',name='解析执行计划',exact=True)).to_be_disabled()
        # Switch without discarding this flow's draft.
        page.get_by_role('combobox',name='流程',exact=True).select_option('flows/evaluate.py:run')
        page.get_by_role('button',name='业务参数',exact=True).click()
        expect(page.get_by_label('limit',exact=True)).to_have_value('8',timeout=30000)
        page.get_by_role('combobox',name='流程',exact=True).select_option('flows/select.py:run')
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
