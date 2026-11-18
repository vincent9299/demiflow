"""The operator directory navigates the real compiler and existing bound reader."""
import json

from playwright.sync_api import expect

from demiflow.workbench.projects import registry, workspace as ws
from test_forge_markdown_reading import browser
from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes
from test_forge_public_api import service
from test_forge_web_redesign import opened
from test_workbench_plan_node_details_ui import setup_materials


def operator(page, name):
    return page.locator('.wb-operator-row').filter(
        has=page.locator('.wb-operator-name', has_text=name))


def test_directory_real_source_prompt_config_graph_link_reload_and_narrow(original, tmp_path, browser):
    setup_materials(original)
    before = business_bytes(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        expect(page.get_by_role('button', name='解析执行计划', exact=True)).to_be_enabled()
        assert page.locator('.function-nav > button').all_text_contents() == [
            '开发', '算子', '管道', '运行监控', '数据', 'Readme']
        page.get_by_role('button', name='算子', exact=True).click()
        expect(page.get_by_role('region', name='算子目录')).to_contain_text('编译计划')
        expect(page.locator('.wb-operator-row')).to_have_count(4)
        expect(page.locator('.wb-body')).to_be_hidden()
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.locator('.wb-operator-row')).to_have_count(4)
        expect(page.get_by_role('button', name='算子', exact=True)).to_have_attribute('aria-pressed', 'true')
        clean = operator(page, '整理输入')
        before_list = page.locator('.wb-operator-sidebar').bounding_box()
        clean.click()
        after_list = page.locator('.wb-operator-sidebar').bounding_box()
        assert before_list == after_list
        reader_box = page.locator('.wb-operator-viewer').bounding_box()
        assert reader_box['x'] >= after_list['x'] + after_list['width']
        expect(page.locator('.wb-operator-viewer .wb-source-body')).to_contain_text('def clean(row):')
        expect(page.locator('.wb-operator-viewer .wb-source-body')).to_contain_text('准确的业务函数正文')
        expect(page.locator('.wb-operator-viewer').get_by_role('button', name='Prompt', exact=True)).to_be_disabled()
        expect(page.locator('.wb-operator-row[aria-pressed=true]')).to_have_count(1)
        page.get_by_role('searchbox', name='搜索算子').fill('Prompt')
        expect(page.locator('.wb-operator-row:visible')).to_have_count(1)
        prompt = operator(page, '联合出题')
        prompt.focus()
        page.keyboard.press('Enter')
        expect(page.locator('.wb-operator-viewer .wb-source-body')).to_contain_text('{{ payload | json }}')
        expect(page.locator('.wb-operator-viewer .wb-prompt-summary')).to_contain_text('请求上限：1')
        expect(page.locator('.wb-operator-row[aria-pressed=true]')).to_have_count(1)
        page.locator('.wb-operator-viewer').get_by_role('button', name='配置', exact=True).click()
        expect(page.locator('.wb-operator-viewer .wb-source-body')).to_contain_text('prompt_name')
        page.locator('.wb-operator-viewer').get_by_role('button', name='在运行图中定位', exact=True).click()
        expect(page.locator('.wb-body')).to_be_visible()
        expect(page.locator('.wb-node').filter(has_text='联合出题')).to_have_attribute('aria-pressed', 'true')
        page.get_by_role('button', name='算子', exact=True).click()
        page.get_by_role('searchbox', name='搜索算子').fill('不存在的算子')
        expect(page.get_by_role('region', name='算子目录')).to_contain_text('没有匹配的算子')
        page.get_by_role('searchbox', name='搜索算子').fill('')
        # Switch nodes in place, then reload the same compiled selection.
        clean.click()
        prompt.click()
        expect(page.locator('.wb-operator-viewer .wb-source-body')).to_contain_text('{{ payload | json }}')
        # Reload re-compiles against current source. Await the real response,
        # rather than treating the default 5 s DOM assertion as a compiler SLA.
        for _ in range(2):
            with page.expect_response(lambda r: r.url.endswith('/plan-preview')) as restored:
                page.reload()
            assert restored.value.status == 200, restored.value.text()
            expect(page.locator('.wb-operator-row')).to_have_count(4)
            expect(page.get_by_role('button', name='算子', exact=True)).to_have_attribute('aria-pressed', 'true')
            expect(page.locator('.wb-operator-viewer .wb-source-body')).to_contain_text('{{ payload | json }}')
        page.set_viewport_size({'width': 780, 'height': 1000})
        expect(operator(page, '联合出题')).to_be_visible()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.set_viewport_size({'width': 390, 'height': 1000})
        source_header = page.locator('.wb-operator-viewer .wb-source-header').bounding_box()
        source_tabs = page.locator('.wb-operator-viewer .wb-source-tabs').bounding_box()
        assert source_tabs['height'] < 90
        assert source_tabs['y'] - source_header['y'] - source_header['height'] <= 2
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        page.locator('.wb-more > summary').click()
        page.locator('.wb-more').get_by_role('button', name='检查问题', exact=True).click()
        provenance = page.locator('.wb-checks details').filter(has=page.locator('summary', has_text='当前项目检查与来源'))
        expect(provenance).to_be_visible()
        page.get_by_role('button', name='算子', exact=True).click()
        expect(provenance).to_be_hidden()
        page.get_by_role('button', name='管道', exact=True).click()
        expect(provenance).to_be_hidden()
        assert not errors
        assert all(url.endswith(('/plan-preview', '/plan-node-detail'))
                   for method, url in requests if method == 'POST')
    assert business_bytes(original) == before


def test_directory_configuration_switch_clears_old_rows_and_keeps_tab(tmp_path, browser):
    root = tmp_path / 'plain-project'
    ws.init_workspace(root, name='Directory configurations')
    (root / 'flow.py').write_text(
        'from demiflow import data\n'
        'def keep(row): raise AssertionError("must not execute rows")\n'
        'def run(*, extra):\n'
        '    rows = data.from_items([{"id":1}])\n'
        '    if extra: rows = rows.map(keep, label="仅此配置的转换")\n'
        '    return rows.take_all()\n')
    for name, extra in [('base.json', False), ('extra.json', True)]:
        (root / name).write_text(json.dumps({'arguments': {'extra': extra}}))
        ws.register_flow(root, 'flow.py:run', params=name)
    with service(root) as app, opened(browser, app) as (page, requests):
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        expect(page.get_by_role('button', name='解析执行计划', exact=True)).to_be_enabled()
        page.get_by_role('button', name='算子', exact=True).click()
        for name, expected in [('extra.json', 3), ('base.json', 2), ('extra.json', 3)]:
            selector = page.get_by_role('combobox', name='配置', exact=True)
            if selector.input_value() != name:
                selector.select_option(name)
                expect(page.locator('.wb-operator-row')).to_have_count(0)
            expect(page.get_by_role('button', name='算子', exact=True)).to_have_attribute('aria-pressed', 'true')
            with page.expect_response(lambda r: r.url.endswith('/plan-preview')) as compiled:
                page.get_by_role('button', name='解析执行计划', exact=True).click()
            assert compiled.value.status == 200, compiled.value.text()
            expect(page.locator('.wb-operator-row')).to_have_count(expected)
            if name == 'extra.json':
                operator(page, '仅此配置的转换').click()
                expect(page.locator('.wb-operators .wb-source-body')).to_contain_text('def keep(row)')
            else:
                expect(page.locator('.wb-operators')).not_to_contain_text('def keep(row)')
        expect(page.get_by_role('button', name='运行', exact=True)).to_be_disabled()


def test_directory_late_material_cannot_replace_other_selected_operator(original, tmp_path, browser):
    setup_materials(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        page.get_by_role('button', name='算子', exact=True).click()
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.locator('.wb-operator-row')).to_have_count(4)
        pending = []
        def delay(route):
            if route.request.post_data_json['kind'] == 'implementation':
                pending.append((route, route.fetch()))
            else:
                route.continue_()
        page.route('**/plan-node-detail', delay)
        operator(page, '整理输入').click()
        expect(page.locator('.wb-operators .wb-source-body')).to_contain_text('正在读取解析时快照')
        operator(page, '联合出题').click()
        expect(page.locator('.wb-operators .wb-source-body')).to_contain_text('{{ payload | json }}')
        assert pending
        for route, response in pending:
            route.fulfill(response=response)
        expect(page.locator('.wb-operators .wb-source-body')).not_to_contain_text('def clean')
        expect(page.locator('.wb-operators .wb-source-body')).to_contain_text('{{ payload | json }}')
        page.unroute('**/plan-node-detail', delay)
        app.plan_snapshots._ttl = 0
        page.locator('.wb-operators').get_by_role('button', name='配置', exact=True).click()
        expect(page.locator('.wb-operators .wb-source-body')).to_contain_text('解析快照已过期')
        expect(page.locator('.wb-operators .wb-source-body')).not_to_contain_text('{{ payload | json }}')
