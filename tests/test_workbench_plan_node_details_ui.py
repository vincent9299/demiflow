"""Real HTTP/compiler/materials browser journey; no model or row execution."""
import json

from playwright.sync_api import expect

from demiflow.workbench.projects import registry
from test_forge_markdown_reading import browser
from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes
from test_forge_public_api import service
from test_forge_web_redesign import opened
from test_map_prompt_async import PACK


def setup_materials(original):
    (original / 'operators/clean.py').write_text('def clean(row):\n'
        '    """准确的业务函数正文。"""\n'
        '    raise AssertionError("must not execute rows")\n')
    (original / 'settings/prompt.yaml').write_text(PACK)
    (original / 'dispatch.py').write_text('from demiflow import data\n'
        'from shipping.operators.clean import clean\n'
        'def config(): return {}\n'
        'def run_pipeline():\n'
        '    rows = data.from_items([{\"item\": \"x\"}]).map(clean, label="整理输入")\n'
        '    return rows.map_prompt_async("enrich", config="settings/prompt.yaml",\n'
        '        inputs={"payload":"item"}, output="answer", max_requests=1, label="联合出题").take_all()\n')


def test_real_nodes_open_matching_function_and_prompt_with_unavailable_tabs_disabled(original, tmp_path, browser):
    setup_materials(original)
    before = business_bytes(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        with page.expect_response(lambda r: r.url.endswith('/plan-preview')) as pending:
            page.get_by_role('button', name='解析执行计划', exact=True).click()
        assert pending.value.status == 200, pending.value.text()
        expect(page.locator('.wb-node')).to_have_count(4)
        page.locator('.wb-node').filter(has_text='整理输入').click()
        expect(page.locator('.wb-source-body')).to_contain_text('def clean(row):')
        expect(page.locator('.wb-source-body')).to_contain_text('准确的业务函数正文')
        expect(page.locator('.wb-source-body')).to_contain_text('解析时快照')
        expect(page.locator('.wb-source-tabs').get_by_role('button', name='Prompt', exact=True)).to_be_disabled()
        expect(page.locator('.wb-source-tabs').get_by_role('button', name='调用点', exact=True)).to_be_disabled()
        page.locator('.wb-node').filter(has_text='联合出题').click()
        expect(page.locator('.wb-source > .wb-source-header')).to_contain_text('Prompt 模板 · 联合出题')
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        expect(page.locator('.wb-prompt-summary')).to_contain_text('请求上限：1')
        expect(page.locator('.wb-node').filter(has_text='联合出题')).to_have_attribute('aria-pressed', 'true')
        expect(page.locator('.wb-source-tabs').get_by_role('button', name='实现', exact=True)).to_be_disabled()
        page.locator('.wb-source-tabs').get_by_role('button', name='配置', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('prompt_name')
        page.locator('.wb-source-tabs').get_by_role('button', name='Prompt', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        page.locator('.wb-node').filter(has_text='整理输入').click()
        expect(page.locator('.wb-source-body')).to_contain_text('def clean(row):')
        assert '{{ payload | json }}' not in page.locator('.wb-source-body').inner_text()
        page.get_by_role('button', name='收起代码', exact=True).click()
        expect(page.locator('.wb-source-body')).to_be_hidden()
        page.get_by_role('button', name='展开代码', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('def clean(row):')
        with page.expect_response(lambda r: r.url.endswith('/plan-preview')):
            page.reload()
        expect(page.locator('.wb-node')).to_have_count(4)
        expect(page.locator('.wb-source-body')).to_contain_text('def clean(row):')
        expect(page.get_by_role('combobox', name='定位算子')).to_be_enabled()
        page.get_by_role('button', name='改为纵向', exact=True).click()
        expect(page.get_by_role('button', name='改为横向', exact=True)).to_be_visible()
        page.get_by_role('button', name='适应全图', exact=True).click()
        assert page.locator('.wb-graph-canvas').bounding_box()['height'] <= page.locator('.wb-graph-scroll').bounding_box()['height']
        page.get_by_role('button', name='可读布局', exact=True).click()
        page.get_by_role('button', name='改为横向', exact=True).click()
        page.get_by_role('button', name='展开画布', exact=True).click()
        expect(page.get_by_role('button', name='退出展开', exact=True)).to_be_visible()
        page.locator('.wb-node').filter(has_text='联合出题').focus()
        page.keyboard.press('Enter')
        expect(page.locator('.wb-node').filter(has_text='联合出题')).to_have_attribute('aria-pressed', 'true')
        page.get_by_role('button', name='退出展开', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        assert not errors
        assert all(url.endswith(('/plan-preview', '/plan-node-detail')) for method, url in requests if method == 'POST')
    assert business_bytes(original) == before


def test_late_material_does_not_replace_new_selection_and_expiry_clears_old_text(original, tmp_path, browser):
    setup_materials(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(4)
        pending = []
        def delay(route):
            if route.request.post_data_json['kind'] == 'implementation':
                response = route.fetch()
                pending.append((route, response))
            else:
                route.continue_()
        page.route('**/plan-node-detail', delay)
        page.locator('.wb-node').filter(has_text='整理输入').click()
        expect(page.locator('.wb-source-body')).to_contain_text('正在读取解析时快照')
        page.locator('.wb-node').filter(has_text='联合出题').click()
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        assert pending
        for route, response in pending:
            route.fulfill(response=response)
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        assert 'def clean' not in page.locator('.wb-source-body').inner_text()
        page.unroute('**/plan-node-detail', delay)
        app.plan_snapshots._ttl = 0
        page.locator('.wb-source-tabs').get_by_role('button', name='配置', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('解析快照已过期')
        assert '{{ payload | json }}' not in page.locator('.wb-source-body').inner_text()
