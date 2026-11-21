"""Project operator definitions and materials bound to a compiled flow."""
import hashlib
import json

from playwright.sync_api import expect

from demiflow.forge.project import create_role_file, initialize_project, register_flow
from demiflow.workbench.projects import registry
from test_forge_markdown_reading import browser
from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes
from test_forge_public_api import service
from test_forge_web_redesign import navigate, opened
from test_workbench_plan_node_details_ui import setup_materials


def test_directory_real_source_prompt_config_graph_link_reload_and_narrow(original, tmp_path, browser):
    setup_materials(original)
    before = business_bytes(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        navigate(page, '管道')
        assert page.locator('.function-nav > button').all_text_contents() == [
            '开发', '算子', '管道', '运行', '数据', 'Readme']
        with page.expect_response('**/plan-preview') as compiled:
            page.get_by_role('button', name='解析执行计划', exact=True).click()
        assert compiled.value.ok, compiled.value.text()
        expect(page.locator('.wb-node')).to_have_count(4)
        page.locator('.wb-node').filter(has_text='整理输入').click()
        tabs = page.locator('.wb-source-tabs')
        tabs.get_by_role('button', name='代码算子', exact=True).click()
        source = page.locator('.wb-source-body')
        expect(source).to_contain_text('def clean(row):')
        expect(tabs.get_by_role('button', name='Prompt 算子', exact=True)).to_be_hidden()
        page.get_by_role('button', name='查看当前定义', exact=True).click()
        expect(page.locator('.op-content')).to_contain_text('准确的业务函数正文')
        sidebar = page.get_by_role('navigation', name='项目算子目录')
        box = sidebar.bounding_box()
        viewer = page.get_by_role('region', name='算子定义').bounding_box()
        assert viewer['x'] >= box['x'] + box['width'] - 1
        page.get_by_role('tab', name='Prompt 算子').click()
        page.get_by_role('button', name='查找算子', exact=True).click()
        search = page.get_by_role('searchbox', name='查找算子')
        search.fill('不存在的定义')
        expect(sidebar).to_contain_text('没有匹配的算子')
        search.fill('enrich')
        page.get_by_role('button', name='P enrich', exact=True).focus()
        page.keyboard.press('Enter')
        expect(page.locator('.op-content')).to_contain_text('{{ payload | json }}')
        expect(sidebar.locator('[data-definition][aria-pressed=true]')).to_have_count(1)
        page.get_by_role('button', name='接口与默认值', exact=True).click()
        expect(page.locator('.op-content')).to_contain_text('输出 Schema')
        assert sidebar.bounding_box() == box
        navigate(page, '管道')
        page.locator('.wb-node').filter(has_text='联合出题').click()
        tabs.get_by_role('button', name='Prompt 算子', exact=True).click()
        expect(source).to_contain_text('{{ payload | json }}')
        tabs.get_by_role('button', name='流程接线', exact=True).click()
        expect(source).to_contain_text('prompt')
        tabs.get_by_role('button', name='Prompt 算子', exact=True).click()
        for _ in range(2):
            with page.expect_response('**/plan-preview') as restored:
                page.reload()
            assert restored.value.ok, restored.value.text()
            expect(page.locator('.wb-node')).to_have_count(4)
            expect(source).to_contain_text('{{ payload | json }}')
            expect(page.locator('.wb-node').filter(has_text='联合出题')).to_have_attribute('aria-pressed', 'true')
        page.get_by_role('button', name='查看当前定义', exact=True).click()
        expect(page.locator('.op-heading')).to_contain_text('enrich')
        for width in (780, 390):
            page.set_viewport_size({'width': width, 'height': 1000})
            expect(page.locator('.op-content')).to_contain_text('{{ payload | json }}')
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        navigate(page, '管道')
        page.locator('.wb-more > summary').click()
        page.locator('.wb-more').get_by_role('button', name='检查问题', exact=True).click()
        provenance = page.locator('.wb-checks details').filter(has_text='当前项目检查与来源')
        expect(provenance).to_be_visible()
        navigate(page, '算子')
        expect(provenance).to_be_hidden()
        assert all(url.endswith(('/plan-preview', '/plan-node-detail'))
                   for method, url in requests if method == 'POST')
    assert business_bytes(original) == before


def test_directory_configuration_switch_clears_old_rows_and_keeps_tab(tmp_path, browser):
    root = tmp_path / 'project'
    initialize_project(root)
    create_role_file(root, 'operator', 'keep')
    (root/'operators/keep.py').write_text('from typing import TypedDict\nclass Row(TypedDict):\n    id: int\n'
                                          'def keep(row: Row) -> bool:\n    return row["id"] >= 0\n')
    create_role_file(root, 'flow', 'select_rows')
    (root/'flows/select_rows.py').write_text(
        'from demiflow import data\nfrom operators.keep import keep\n'
        'def run(*, extra: bool):\n'
        '    rows = data.range(2)\n'
        '    if extra: rows = rows.filter(keep)\n'
        '    return rows.count()\n')
    params = root/'parameters/current.json'
    params.parent.mkdir(exist_ok=True)
    params.write_text(json.dumps({'format': 'demiflow_forge_flow_parameters_v2',
                                  'arguments': {'extra': False}}))
    register_flow(root, 'flows/select_rows.py:run', params='parameters/current.json')
    with service(root, enable_config_write=True, workspace_config_write=True) as app, opened(browser, app) as (page, _):
        navigate(page, '管道')
        expect(page.locator('.wb-node')).to_have_count(2, timeout=45000)
        for extra in (True, False, True):
            page.get_by_role('button', name='业务参数', exact=True).click()
            page.get_by_label('extra', exact=True).set_checked(extra)
            expect(page.get_by_role('button', name='解析执行计划', exact=True)).to_be_disabled()
            page.get_by_role('button', name='保存参数', exact=True).click()
            expect(page.locator('.flow-argument-status')).to_contain_text('已保存', timeout=45000)
            page.get_by_role('button', name='关闭业务参数', exact=True).click()
            for _ in range(3):
                with page.expect_response('**/plan-preview') as compiled:
                    page.get_by_role('button', name='解析执行计划', exact=True).click()
                response = compiled.value
                if response.status != 503:
                    break
                assert response.json()['code'] == 'busy'
                assert response.json()['operation_started'] is False
                expect(page.locator('.wb-notice')).to_be_visible()
            assert response.ok, response.text()
            assert response.json()['basis']['params_sha256'] == hashlib.sha256(params.read_bytes()).hexdigest()
            expect(page.locator('.wb-node')).to_have_count(3 if extra else 2)
            conditional = page.locator('.wb-node').filter(has_text='keep')
            if extra:
                conditional.click()
                page.locator('.wb-source-tabs').get_by_role('button', name='代码算子', exact=True).click()
                expect(page.locator('.wb-source-body')).to_contain_text('def keep(row: Row)')
            else:
                expect(conditional).to_have_count(0)
            navigate(page, '算子')
            expect(page.get_by_role('tab', name='代码算子')).to_have_attribute('aria-selected', 'true')
            expect(page.get_by_role('button', name='ƒ keep', exact=True)).to_be_visible()
            navigate(page, '管道')
        expect(page.locator('.wb-actions').get_by_role('button', name='运行', exact=True)).to_be_disabled()


def test_directory_late_material_cannot_replace_other_selected_operator(original, tmp_path, browser):
    setup_materials(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, _):
        navigate(page, '算子')
        expect(page.locator('.op-content')).to_contain_text('def clean(row)')
        held = []

        def delay(route):
            response = route.fetch()
            assert response.ok, response.text()
            held.append((route, response))

        page.route('**/operator-definition?**', delay, times=1)
        code = page.get_by_role('button', name='ƒ clean', exact=True)
        with page.expect_request('**/operator-definition?**'):
            code.click()
        expect(page.locator('.op-viewer')).to_contain_text('读取定义')
        page.get_by_role('tab', name='Prompt 算子').click()
        page.get_by_role('button', name='P enrich', exact=True).click()
        expect(page.locator('.op-content')).to_contain_text('{{ payload | json }}')
        assert len(held) == 1
        with page.expect_response(lambda r: r.url == held[0][0].request.url):
            held[0][0].fulfill(response=held[0][1])
        expect(page.locator('.op-heading')).to_contain_text('enrich')
        expect(page.locator('.op-content')).not_to_contain_text('def clean')
        expect(page.locator('.op-content')).to_contain_text('{{ payload | json }}')
        navigate(page, '管道')
        expect(page.locator('.wb-node')).to_have_count(4, timeout=45000)
        page.locator('.wb-node').filter(has_text='联合出题').click()
        tabs = page.locator('.wb-source-tabs')
        tabs.get_by_role('button', name='Prompt 算子', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        app.plan_snapshots._ttl = 0
        tabs.get_by_role('button', name='流程接线', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('解析快照已过期')
        expect(page.locator('.wb-source-body')).not_to_contain_text('{{ payload | json }}')
