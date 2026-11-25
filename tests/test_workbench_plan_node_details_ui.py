"""Real HTTP/compiler/materials browser journey; no model or row execution."""
import json

from playwright.sync_api import expect

from demiflow.workbench.projects import registry
from test_forge_markdown_reading import browser
from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes, register_fixture_flow
from test_forge_public_api import service
from test_forge_web_redesign import opened
from test_map_prompt_async import PACK


def setup_materials(original):
    (original / 'operators/clean.py').write_text('def clean(row):\n'
        '    """准确的业务函数正文。"""\n'
        '    raise AssertionError("must not execute rows")\n')
    (original / 'prompts').mkdir()
    (original / 'prompts/prompt.yaml').write_text(PACK)
    (original / 'dispatch.py').write_text('from demiflow import data\n'
        'from operators.clean import clean\n'
        'def config(): return {}\n'
        'def run_pipeline():\n'
        '    rows = data.from_items([{\"item\": \"x\"}]).map(clean, label="整理输入")\n'
        '    return rows.map_prompt_async("enrich", config="prompts/prompt.yaml",\n'
        '        inputs={"payload":"item"}, output="answer", max_requests=1, label="联合出题").take_all()\n')

    register_fixture_flow(original)

def test_real_nodes_open_matching_function_and_prompt_with_unreferenced_tabs_hidden(original, tmp_path, browser):
    setup_materials(original)
    before = business_bytes(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        with page.expect_response(lambda r: r.url.endswith('/plan-preview')) as pending:
            page.get_by_role('button', name='解析执行计划', exact=True).click()
        assert pending.value.status == 200, pending.value.text()
        expect(page.locator('.wb-node')).to_have_count(4)
        page.locator('.wb-node').filter(has_text='整理输入').click()
        page.locator('.wb-source-tabs').get_by_role('button', name='代码算子', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('def clean(row):')
        expect(page.locator('.wb-source-body')).to_contain_text('准确的业务函数正文')
        expect(page.locator('.wb-source-body')).to_contain_text('计划绑定版本')
        expect(page.locator('.wb-source-tabs').get_by_role('button', name='Prompt 算子', exact=True)).to_be_hidden()
        expect(page.locator('.wb-source-tabs').get_by_role('button', name='Dataset API', exact=True)).to_be_enabled()
        page.locator('.wb-node').filter(has_text='联合出题').click()
        page.locator('.wb-source-tabs').get_by_role('button', name='Prompt 算子', exact=True).click()
        expect(page.locator('.wb-source > .wb-source-header')).to_contain_text('Prompt 算子 · 联合出题')
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        page.locator('.definition-contract > summary').click()
        expect(page.locator('.definition-contract')).to_contain_text('payload')
        expect(page.locator('.definition-contract')).to_contain_text('输出 Schema')
        expect(page.locator('.wb-node').filter(has_text='联合出题')).to_have_attribute('aria-pressed', 'true')
        expect(page.locator('.wb-source-tabs').get_by_role('button', name='代码算子', exact=True)).to_be_hidden()
        page.locator('.wb-source-tabs').get_by_role('button', name='流程接线', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('prompt')
        page.locator('.wb-source-tabs').get_by_role('button', name='Prompt 算子', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        page.locator('.wb-node').filter(has_text='整理输入').click()
        page.locator('.wb-source-tabs').get_by_role('button', name='代码算子', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('def clean(row):')
        assert '{{ payload | json }}' not in page.locator('.wb-source-body').inner_text()
        page.get_by_role('button', name='收起详情', exact=True).click()
        expect(page.locator('.wb-source-body')).to_be_hidden()
        page.get_by_role('button', name='展开详情', exact=True).click()
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
        page.locator('.wb-source-tabs').get_by_role('button', name='Prompt 算子', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        assert not errors
        assert all(url.endswith(('/plan-preview', '/plan-node-detail')) for method, url in requests if method == 'POST')
    assert business_bytes(original) == before


def test_late_material_does_not_replace_new_selection_and_expiry_clears_old_text(original, tmp_path, browser):
    setup_materials(original)
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(4)
        pending = []
        held_occurrence = []
        def delay(route):
            body=route.request.post_data_json
            if body['kind'] in ('configuration', 'schema') and (not held_occurrence or body['occurrence_id']==held_occurrence[0]):
                if not held_occurrence:held_occurrence.append(body['occurrence_id'])
                response = route.fetch()
                pending.append((route, response))
            else:
                route.continue_()
        page.route('**/plan-node-detail', delay)
        page.locator('.wb-node').filter(has_text='整理输入').click()
        expect(page.locator('.wb-source-body')).to_contain_text('正在读取解析时快照')
        page.locator('.wb-node').filter(has_text='联合出题').click()
        page.locator('.wb-source-tabs').get_by_role('button', name='Prompt 算子', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        assert len(pending) == 1  # stale selection stops before starting its next material read
        for route, response in pending:
            route.fulfill(response=response)
        expect(page.locator('.wb-source-body')).to_contain_text('{{ payload | json }}')
        assert 'def clean' not in page.locator('.wb-source-body').inner_text()
        page.unroute('**/plan-node-detail', delay)
        app.plan_snapshots._ttl = 0
        page.locator('.wb-source-tabs').get_by_role('button', name='流程接线', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('解析快照已过期')
        assert '{{ payload | json }}' not in page.locator('.wb-source-body').inner_text()


def test_referenced_but_over_budget_prompt_keeps_an_honest_tab(original, tmp_path, browser):
    import yaml
    setup_materials(original)
    pack = yaml.safe_load(PACK)
    schema = {'type': 'string'}
    for _ in range(8):
        schema = {'type': 'object', 'properties': {'child': schema}, 'required': ['child'], 'additionalProperties': False}
    pack['prompts']['enrich']['response_schema'] = schema
    (original / 'prompts/prompt.yaml').write_text(json.dumps(pack))
    path = tmp_path / 'registry.json'
    registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(4, timeout=45000)
        page.locator('.wb-node').filter(has_text='联合出题').click()
        tab = page.locator('.wb-source-tabs').get_by_role('button', name='Prompt 算子', exact=True)
        expect(tab).to_be_visible()
        expect(tab).to_be_enabled()
        tab.click()
        expect(page.locator('.wb-source-body')).to_contain_text('预算')
        assert page.locator('.wb-source-body .definition-reader').count() == 0
