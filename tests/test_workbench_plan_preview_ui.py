"""Native preview compilation and lifecycle through registered flow parameters.

Only lifecycle/rejection tests inject preview transport; parameter edits use the
real HTTP API, and the first browser test also uses the real native compiler.
"""
from demiflow.forge.project import initialize_project
from _forge_project_fixtures import declare_existing_flow
import copy
import json

import pytest
from playwright.sync_api import expect

from demiflow import data
from demiflow.execution.pipeline.managed_native import capture_pipeline
from demiflow.execution.journal.program_materials import build_logical_plan_view
from demiflow.workbench.projects import registry
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from test_forge_markdown_reading import browser
from test_forge_project_discovery import original, snapshot, cached_snapshot
from test_forge_project_discovery import business_bytes
from test_forge_public_api import service
from test_forge_web_redesign import opened
from test_ui_d19_projection_fixes import project


def register_preview_project(original, registry_path, *, writable=False):
    """Use the current declaration and a unique, explicitly registered input."""
    (original / 'dispatch.py').write_text('''from demiflow import data
def forbidden(row): raise AssertionError('must not execute rows')
def run_pipeline(*, stage: str):
    rows = data.from_items([{'id': 1}]).map(forbidden)
    if stage == 'combined': rows = rows.union(data.from_items([{'id': 2}]))
    return rows.take_all()
''')
    (original / 'settings/small.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1', 'arguments': {'stage': 'plain'}}))
    initialize_project(original, existing=True)
    declare_existing_flow(original, 'dispatch.py:run_pipeline', params='settings/small.json')
    registry.add(registry_path, original)
    if writable:
        registry.migrate(registry_path, expected=content_digest(registry.load_registry(registry_path)))
        inventory = registry.load_registry(registry_path)
        registry.set_capabilities(registry_path, next(iter(inventory['projects'])),
                                  expected=content_digest(inventory),
                                  capabilities={k: k == 'config_write' for k in registry.CAPABILITIES})


def test_sidebar_and_graph_text_copy_preserves_navigation(original, tmp_path, browser):
    registry_path = tmp_path / 'registry.json'
    register_preview_project(original, registry_path)
    (original / 'other.py').write_bytes((original / 'dispatch.py').read_bytes())
    (original / 'settings/other.json').write_bytes((original / 'settings/small.json').read_bytes())
    declare_existing_flow(original, 'other.py:run_pipeline', params='settings/other.json')
    with service(None, registry=registry_path) as app, opened(browser, app) as (page, _):
        page.context.grant_permissions(['clipboard-read', 'clipboard-write'], origin=app.origin)
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(3)
        row = page.get_by_role('button', name='other.py:run_pipeline', exact=True)
        expect(row).to_have_attribute('aria-pressed', 'false')

        def drag_and_copy(locator, expected):
            locator.scroll_into_view_if_needed()
            rect = locator.evaluate("""element => {
                const range = document.createRange();
                range.selectNodeContents(element);
                const {left, right, top, height} = range.getBoundingClientRect();
                return {left, right, y: top + height / 2};
            }""")
            page.mouse.move(rect['left'] + 1, rect['y'])
            page.mouse.down()
            page.mouse.move(rect['right'] - 1, rect['y'], steps=12)
            page.mouse.up()
            page.keyboard.press('Control+c')
            assert expected in page.evaluate('navigator.clipboard.readText()')

        drag_and_copy(row.get_by_text('other.py:run_pipeline', exact=True), 'other.py:run_pipelin')
        expect(row).to_have_attribute('aria-pressed', 'false')
        with page.expect_response(lambda response: response.status == 200 and response.url.endswith('/plan-preview')
                                  and response.request.post_data_json['entry'] == 'other.py:run_pipeline'):
            row.click()
        expect(row).to_have_attribute('aria-current', 'page')
        expect(page.get_by_role('button', name='解析执行计划', exact=True)).to_be_enabled()
        node = page.locator('.wb-node').nth(1)
        expect(node).to_have_attribute('aria-pressed', 'false')
        title = node.locator('.wb-node-title').first
        drag_and_copy(title, title.text_content()[:3])
        expect(node).to_have_attribute('aria-pressed', 'false')
        node.click()
        expect(node).to_have_attribute('aria-pressed', 'true')


def test_unbound_project_explains_missing_entry_before_compile(original, tmp_path, browser):
    registry_path = tmp_path / 'registry.json'
    registry.add(registry_path, original)
    with service(None, registry=registry_path) as app, opened(browser, app) as (page, requests):
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        expect(page.get_by_text('未识别到受支持的入口声明', exact=False).first).to_be_visible()
        expect(page.get_by_role('button', name='解析执行计划', exact=True)).to_be_disabled()
        assert not any(method == 'POST' and url.endswith('/plan-preview') for method, url in requests)


def test_original_configuration_selector_compiles_through_real_http(original, tmp_path, browser):
    registry_path = tmp_path / 'registry.json'
    register_preview_project(original, registry_path, writable=True)
    before = business_bytes(original)
    with service(None, registry=registry_path, enable_config_write=True) as app, opened(browser, app) as (page, requests):
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        compile_button = page.get_by_role('button', name='解析执行计划', exact=True)
        expect(compile_button).to_be_enabled()
        initial_requests = len([url for method, url in requests if method == 'POST' and url.endswith('/plan-preview')])
        with page.expect_response(lambda response: response.url.endswith('/plan-preview')) as pending:
            compile_button.click()
        response = pending.value
        assert response.status == 200, response.text()
        plain = response.json()
        assert plain['basis']['entry'] == 'dispatch.py:run_pipeline'
        assert plain['basis']['params'] == 'settings/small.json'
        expect(page.locator('.wb-node')).to_have_count(3)
        page.get_by_role('button', name='业务参数', exact=True).click()
        expect(page.get_by_label('stage', exact=True)).to_have_value('plain')
        page.get_by_label('stage', exact=True).fill('combined')
        expect(compile_button).to_be_disabled()
        expect(page.locator('.wb-node')).to_have_count(0)
        page.get_by_role('button', name='保存参数', exact=True).click()
        expect(page.locator('.flow-argument-status')).to_contain_text('已保存', timeout=45000)
        page.get_by_role('button', name='关闭业务参数', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(0)
        with page.expect_response(lambda response: response.url.endswith('/plan-preview')) as pending:
            compile_button.click()
        response = pending.value
        assert response.status == 200, response.text()
        combined = response.json()
        assert combined['basis']['params'] == plain['basis']['params']
        assert combined['basis']['params_sha256'] != plain['basis']['params_sha256']
        expect(page.locator('.wb-node')).to_have_count(5)
        assert len([url for method, url in requests if method == 'POST' and url.endswith('/plan-preview')]) == initial_requests + 2
        expect(page.locator('.wb-actions').get_by_role('button', name='运行', exact=True)).to_be_disabled()
    after = business_bytes(original)
    assert after.pop('settings/small.json') != before.pop('settings/small.json')
    assert after == before
    assert json.loads((original / 'settings/small.json').read_text())['arguments'] == {'stage': 'combined'}


def test_registered_multivariant_flow_selects_and_restores_explicit_parameters(original, tmp_path, browser):
    registry_path = tmp_path/'registry.json'
    register_preview_project(original, registry_path)
    (original/'settings/combined.json').write_text(json.dumps({
        'format':'demiflow_forge_flow_parameters_v1','arguments':{'stage':'combined'}}))
    declare_existing_flow(original, 'dispatch.py:run_pipeline', params='settings/combined.json')
    with service(None, registry=registry_path) as app, opened(browser, app) as (page, requests):
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        choice = page.get_by_label('参数配置', exact=True)
        expect(choice).to_be_visible()
        expect(page.get_by_text('此流程已登记多个参数文件，请选择参数配置。', exact=True)).to_be_visible()
        with page.expect_response(lambda response: response.url.endswith('/plan-preview')) as pending:
            choice.select_option('settings/combined.json')
        assert pending.value.json()['basis']['params'] == 'settings/combined.json'
        expect(page.locator('.wb-node')).to_have_count(5)
        page.reload()
        expect(page.get_by_label('参数配置', exact=True)).to_have_value('settings/combined.json')
        # Reload restores and compiles the current selection before the picker
        # accepts another one. Allow both native graph reads and compilations.
        with page.expect_response(lambda response: response.url.endswith('/plan-preview') and response.request.post_data_json['params']=='settings/small.json', timeout=45000) as pending:
            page.get_by_label('参数配置', exact=True).select_option('settings/small.json')
        assert pending.value.json()['basis']['params'] == 'settings/small.json'
        expect(page.locator('.wb-node')).to_have_count(3)


def _transform(row):
    raise AssertionError('compile preview must not evaluate rows')


def _pipeline():
    first = data.from_items([{'id': 1}]).map(_transform)
    return first.union(data.from_items([{'id': 2}])).take_all()


@pytest.fixture
def preview():
    compiled = capture_pipeline(_pipeline, {})
    return dict(format='demiflow.plan_preview.v1', available=True,
                basis=dict(entry='dispatch.py:run_pipeline', params='settings/small.json',
                           entry_sha256='a'*64, params_sha256='b'*64,
                           sources={'dispatch.py': 'a'*64}, digest='c'*64),
                kind=compiled.kind, provider=compiled.provider,
                entry_completed=False, scope='first_action',
                dag=build_logical_plan_view(compiled.compiled.graph), limitations=[])


def project_preview(value, selection=None):
    return project({'preview': value, 'selection': selection}, """
try {console.log(JSON.stringify({model:P.buildPlanPreviewModel(payload.preview,{selection:payload.selection})}));}
catch(error) {console.log(JSON.stringify({error:error.message}));}
""")


def test_native_compiler_graph_projects_without_runtime_or_static_facts(preview):
    result = project_preview(preview)['model']
    assert result['execution'] is None
    assert result['choice']['kind'] == 'preview'
    assert len(result['nodes']) == len(preview['dag']['nodes'])
    assert len(result['edges']) == len(preview['dag']['edges'])
    assert all(n['selection']['reportNodeId'] is None for n in result['nodes'])
    assert all(n['selection']['execution'] is None for n in result['nodes'])
    assert all(n['state'] == {'label': '尚未运行', 'tone': 'neutral'} for n in result['nodes'])
    assert {e['fromPort'] for e in result['edges']} == {e['from']['port'] for e in preview['dag']['edges']}
    assert {n['selection']['occurrenceId'] for n in result['nodes']} == {n['occurrence_id'] for n in preview['dag']['nodes']}
    chosen = result['nodes'][0]['selection']
    assert project_preview(preview, chosen)['model']['selected'] == chosen['key']
    other = copy.deepcopy(preview)
    other['basis']['digest'] = 'd'*64
    assert project_preview(other, chosen)['model']['selected'] is None


@pytest.mark.parametrize('corrupt', ['endpoint', 'occurrence', 'port', 'budget', 'scope', 'schema'])
def test_bad_preview_fails_closed_without_static_fallback(preview, corrupt):
    value = copy.deepcopy(preview)
    if corrupt == 'endpoint':
        value['dag']['edges'][0]['from']['occurrence_id'] = 'missing'
    elif corrupt == 'occurrence':
        value['dag']['nodes'][1]['occurrence_id'] = value['dag']['nodes'][0]['occurrence_id']
    elif corrupt == 'port':
        value['dag']['edges'][0]['from']['port'] = 'invented'
    elif corrupt == 'budget':
        value['dag']['nodes'] *= 257
    elif corrupt == 'scope':
        value['entry_completed'] = True
    elif corrupt == 'schema':
        value['dag']['schema_version'] = 'static_python_calls'
    assert 'error' in project_preview(value)


def test_preview_ui_is_explicit_and_clears_on_configuration_change(original, tmp_path, browser, preview):
    registry_path = tmp_path / 'registry.json'
    register_preview_project(original, registry_path, writable=True)
    with service(None, registry=registry_path, enable_config_write=True) as app, opened(browser, app) as (page, requests):
        received = []
        def compiled(route):
            received.append(route.request.post_data_json)
            value = copy.deepcopy(preview)
            if len(received) > 1:
                value['basis']['params'] = 'settings/other.json'
            route.fulfill(status=200, content_type='application/json', body=json.dumps(value))
        page.route('**/plan-preview', compiled)
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(len(preview['dag']['nodes']))
        expect(page.locator('.wb-basis')).to_have_text('编译计划 · 尚未运行 · 仅首个执行动作')
        assert received == [{'entry': 'dispatch.py:run_pipeline', 'params': 'settings/small.json'}]
        page.locator('.wb-node').first.click()
        expect(page.locator('.wb-inspector')).to_contain_text('尚未运行')
        page.get_by_role('button', name='业务参数', exact=True).click()
        page.get_by_label('stage', exact=True).fill('combined')
        expect(page.locator('.wb-node')).to_have_count(0)
        expect(page.get_by_role('button', name='解析执行计划', exact=True)).to_be_disabled()
        expect(page.locator('.wb-inspector')).to_contain_text('编译依据已改变')
        assert len(received) == 1
        page.get_by_role('button', name='保存参数', exact=True).click()
        expect(page.locator('.flow-argument-status')).to_contain_text('已保存', timeout=45000)
        page.get_by_role('button', name='关闭业务参数', exact=True).click()
        expect(page.locator('.wb-graph-empty')).to_have_text('尚无运行执行图')
        expect(page.locator('.wb-inspector').get_by_role('heading', name='整图配置', exact=True)).to_have_count(1)
        expect(page.get_by_role('textbox', name='配置 /arguments/stage', exact=True)).to_have_value('combined')
        # Deliberately mismatch the response binding: do not leave the old graph.
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('解析结果与所选入口、配置不一致')
        expect(page.locator('.wb-node')).to_have_count(0)
        for width in (320, 768, 1440):
            page.set_viewport_size({'width': width, 'height': 1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        assert received == [{'entry': 'dispatch.py:run_pipeline', 'params': 'settings/small.json'}] * 2
        # Preview detail/source reads use POST too; saving these parameters is
        # the only permitted mutation, never a run/check submission.
        posts = [url for method, url in requests if method == 'POST']
        assert all(url.endswith(('/plan-preview', '/plan-node-detail',
                                 '/plan-pipeline-source', '/flow-arguments-save'))
                   for url in posts), posts


def test_unmounted_preview_response_cannot_repopulate_a_new_view(original, tmp_path, browser, preview):
    registry_path = tmp_path / 'registry.json'
    register_preview_project(original, registry_path)
    with service(None, registry=registry_path) as app, opened(browser, app) as (page, requests):
        held = []
        received = []
        def compiled(route):
            received.append(route.request.post_data_json)
            if len(received) == 1:
                route.fulfill(status=200, content_type='application/json', body=json.dumps(preview))
            else:
                held.append(route)
        page.route('**/plan-preview', compiled)
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(len(preview['dag']['nodes']))
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.get_by_role('button', name='正在解析…', exact=True)).to_be_disabled()
        page.locator('.function-nav').get_by_role('button', name='文档', exact=True).click()
        expect(page.locator('.workbench')).to_have_count(0)
        assert len(held) == 1
        page.locator('.function-nav').get_by_role('button', name='管线', exact=True).click()
        expect(page.get_by_role('button', name='正在解析…', exact=True)).to_be_disabled(timeout=65000)
        assert len(held) == 2
        # A new mount has its own in-flight compile. The old response must not
        # paint its graph or clear the new mount's busy state.
        held[0].fulfill(status=200, content_type='application/json', body=json.dumps(preview))
        expect(page.locator('.wb-node')).to_have_count(0)
        expect(page.locator('.wb-graph-empty')).to_have_text('尚无运行执行图')
        expect(page.get_by_role('button', name='正在解析…', exact=True)).to_be_disabled()
        held[1].fulfill(status=200, content_type='application/json', body=json.dumps(preview))
        expect(page.locator('.wb-node')).to_have_count(len(preview['dag']['nodes']))
        expect(page.get_by_role('button', name='解析执行计划', exact=True)).to_be_enabled()
        assert len(received) == 3


def test_optional_compiler_label_does_not_change_operator_kind_or_identity(preview):
    before = project_preview(preview)['model']['nodes'][0]
    value = copy.deepcopy(preview)
    value['dag']['nodes'][0]['label'] = 'business.operators.prepare'
    after = project_preview(value)['model']['nodes'][0]
    assert after['title'] == 'business.operators.prepare'
    assert after['kindLabel'] == before['kindLabel']
    assert after['selection'] == before['selection']
    # A UDF/business label may coincide with a platform operator name.
    value['dag']['nodes'][0]['label'] = 'join'
    assert project_preview(value)['model']['nodes'][0]['title'] == 'join'
    for invalid in ('', ' ', 'x'*257, {'from_static_ast': True}):
        value['dag']['nodes'][0]['label'] = invalid
        assert project_preview(value)['model']['nodes'][0]['title'] == before['kindLabel']


def test_graph_plain_language_preserves_selection_ports_and_observed_failure(preview, browser):
    """Real compiler union; additional branch/state samples test rendering only."""
    from test_forge_markdown_reading import WEB

    model = project_preview(preview)['model']
    with browser.new_page(viewport={'width': 1200, 'height': 800}) as page:
        page.set_content('<main id="graph"></main>')
        page.add_style_tag(path=str(WEB / 'workbench-graph.css'))
        page.add_script_tag(path=str(WEB / 'workbench-graph.js'))
        page.evaluate('''model => {
          globalThis.model=model;
          globalThis.renderer=ForgeWorkbenchGraph.mount(document.querySelector('#graph'),
            {onSelect: selection => globalThis.selected=selection});
          renderer.update(model);
        }''', model)
        expect(page.locator('.wb-node')).to_have_count(len(model['nodes']))
        expect(page.locator('.wb-edge')).to_have_count(len(model['edges']))
        assert sorted(page.locator('.wb-edge-port').all_text_contents()) == ['合并输入 1', '合并输入 2']
        assert all(not text for text in page.locator('.wb-node-state').all_text_contents())
        assert 'main' not in page.locator('.wb-graph-canvas').text_content()
        page.locator('.wb-node').first.click()
        assert page.evaluate('selected') == model['nodes'][0]['selection']
        assert page.evaluate('model') == model  # Simplifying labels never mutates the facts.

        # Nondefault/custom ports remain distinguishable. This is not runtime evidence.
        page.evaluate('''() => {
          model.choice.kind='sealed';
          model.edges[0].fromPort='error'; model.edges[0].toPort='left';
          model.edges[1].fromPort='success'; model.edges[1].toPort='right';
          model.edges[2].fromPort='review'; model.edges[2].toPort='main';
          model.nodes[0].state={label:'执行失败',tone:'failed'};
          model.nodes[0].summary=[{label:'端口',value:'main → main'},
            {label:'阶段',value:'internal-stage-digest'}, {label:'版本',value:'7'}];
          renderer.update(model);
        }''')
        labels = page.locator('.wb-edge-port').all_text_contents()
        assert '错误分支 → 左侧输入' in labels
        assert '成功分支 → 右侧输入' in labels
        assert '分支：review' in labels
        expect(page.locator('.wb-node-state').first).to_contain_text('执行失败')
        expect(page.locator('.wb-node-summary').first).to_have_text('版本：7')
        assert 'internal-stage-digest' not in page.locator('.wb-graph-canvas').text_content()
        page.get_by_role('button', name='改为纵向', exact=True).click()
        assert page.locator('.wb-edge-port').all_text_contents() == labels
        page.locator('.wb-node').first.focus()
        page.keyboard.press('Enter')
        assert page.evaluate('selected') == model['nodes'][0]['selection']
