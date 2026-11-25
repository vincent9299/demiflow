"""The primary canvas never substitutes Python calls or a report for execution."""
import pytest
from playwright.sync_api import expect

from d19_fixtures import owned_fixture
from test_forge_markdown_reading import browser
from test_forge_project_discovery import original, snapshot, cached_snapshot, business_bytes, register_fixture_flow
from test_forge_public_api import service
from test_forge_web_redesign import opened
from test_ui_d19_projection_fixes import project
from demiflow.workbench.projects import registry
from demiflow.workbench.graph import model


def test_original_calls_are_not_execution_nodes_and_canvas_expands(original, tmp_path, browser):
    path = original / 'dispatch.py'
    path.write_text(path.read_text().replace('return clean(cfg)', 'return clean(type(cfg))'))
    register_fixture_flow(original)
    before = business_bytes(original)
    registry_path = tmp_path / 'registry.json'
    registry.add(registry_path, original)
    # The read-only source API may still support diagnostics. It is not the canvas.
    source = model.get(original, basis_kind='source', entry='dispatch.py:run_pipeline')
    assert any(n['api'] == 'type' for n in source['nodes'])
    with service(None, registry=registry_path) as app, opened(browser, app) as (page, requests):
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        expect(page.locator('.wb-graph-empty')).to_have_text('尚无运行执行图')
        expect(page.locator('.wb-node')).to_have_count(0)
        expect(page.locator('.wb-graph-notices')).to_contain_text('解析执行计划')
        assert page.get_by_role('button', name='当时检查图', exact=True).count() == 0
        assert page.get_by_role('button', name='查看源码引用（诊断）', exact=True).count() == 0
        canvas = page.locator('.wb-graph')
        assert canvas.bounding_box()['height'] >= 500
        page.get_by_role('button', name='展开画布', exact=True).click()
        page.wait_for_function("document.fullscreenElement?.classList.contains('wb-body')")
        assert canvas.bounding_box()['height'] >= 700
        page.get_by_role('button', name='退出展开', exact=True).click()
        page.wait_for_function('!document.fullscreenElement')
        for width in (320, 390, 768, 1440):
            page.set_viewport_size({'width': width, 'height': 1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        # Opening a registered entry may request its read-only plan preview.
        # It still must not submit a run or any workspace mutation.
        assert all(url.endswith('/plan-preview') for method, url in requests if method == 'POST')
    assert business_bytes(original) == before


@pytest.mark.parametrize('owned_fixture', [{'execution_format': 'legacy'}, {'execution_format': 'v3'}], indirect=True)
def test_native_sealed_graph_is_the_only_canvas_source(owned_fixture, browser):
    fx = owned_fixture
    run = fx.run_facts
    code, graph = fx.api('/api/graph?basis_kind=run&run_id=' + run['run_id']
                         + '&attempt=' + str(run['attempt']))
    assert code == 200
    projected = project({'graph': graph}, """
const choice = P.chooseGraphView(payload.graph, {preference: 'report'});
const model = P.buildGraphModel(payload.graph, {choice});
console.log(JSON.stringify(model));
""")
    assert projected['execution'] and projected['choice']['kind'] == 'sealed'
    assert projected['execution']['runId'] == run['run_id']
    report = model.get(fx.root, basis_kind='report', report_digest=graph['report_digest'])
    preview = project({'graph': report}, """
console.log(JSON.stringify(P.buildGraphModel(payload.graph, {choice: P.chooseGraphView(payload.graph)})));
""")
    assert report['nodes'] and not preview['nodes'] and preview['execution'] is None
    with fx.open_page(browser) as (page, log):
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        expect(page.locator('.wb-graph-empty')).to_have_text('尚无运行执行图')
        page.locator('.function-nav').get_by_role('button', name='运行', exact=True).click()
        page.locator(f'.wb-run-flow[data-entry="{graph["binding"]["entry"]}"]').click()
        page.get_by_label('运行记录',exact=True).select_option(run['run_id'])
        expect(page.locator('.wb-node')).to_have_count(len(projected['nodes']))
        expect(page.get_by_role('group', name='运行封存计划', exact=True)).to_be_visible()
        assert page.locator('.wb-edge').count() == len(projected['edges'])
        assert page.get_by_role('button', name='当时检查图', exact=True).count() == 0
        assert not log['errors']
    fx.record('execution-only-canvas', {'run_id': run['run_id'], 'model': projected})
