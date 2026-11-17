"""Compile preview projection and UI lifecycle; transport fixture is explicit.

The graph comes from the native compiler. Browser transport is injected only to
exercise preview/clear/rejection without admitting the unrelated legacy fixture.
Original business pipeline/HTTP integration is covered by the service acceptance.
"""
import copy
import json

import pytest
from playwright.sync_api import expect

from demiflow import data
from demiflow.execution.plan_capture import capture_pipeline, build_logical_plan_view
from demiflow.workbench.projects import registry
from test_forge_markdown_reading import browser
from test_forge_project_discovery import original, snapshot, cached_snapshot
from test_forge_public_api import service
from test_forge_web_redesign import opened
from test_ui_d19_projection_fixes import project


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
    registry.add(registry_path, original)
    with service(None, registry=registry_path) as app, opened(browser, app) as (page, requests):
        received = []
        def compiled(route):
            received.append(route.request.post_data_json)
            route.fulfill(status=200, content_type='application/json', body=json.dumps(preview))
        page.route('**/plan-preview', compiled)
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(0)
        assert not received
        path = page.get_by_role('combobox', name='解析配置路径')
        path.fill('settings/small.json')
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.locator('.wb-node')).to_have_count(len(preview['dag']['nodes']))
        expect(page.locator('.wb-basis')).to_have_text('编译计划 · 尚未运行 · 仅首个执行动作')
        assert received == [{'entry': 'dispatch.py:run_pipeline', 'params': 'settings/small.json'}]
        page.locator('.wb-node').first.click()
        expect(page.locator('.wb-inspector')).to_contain_text('尚未运行')
        expect(page.locator('.wb-source-body')).to_contain_text('不从静态检查节点推测')
        path.fill('settings/other.json')
        expect(page.locator('.wb-node')).to_have_count(0)
        expect(page.locator('.wb-graph-empty')).to_have_text('尚无运行执行图')
        expect(page.locator('.wb-inspector')).to_contain_text('编译依据已改变')
        # Deliberately mismatch the response binding: do not leave the old graph.
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.locator('.wb-notice')).to_contain_text('解析结果与所选入口、配置不一致')
        expect(page.locator('.wb-node')).to_have_count(0)
        for width in (320, 768, 1440):
            page.set_viewport_size({'width': width, 'height': 1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        assert all('/plan-preview' in url for method, url in requests if method == 'POST')


def test_unmounted_preview_response_cannot_repopulate_a_new_view(original, tmp_path, browser, preview):
    registry_path = tmp_path / 'registry.json'
    registry.add(registry_path, original)
    with service(None, registry=registry_path) as app, opened(browser, app) as (page, requests):
        held = []
        page.route('**/plan-preview', lambda route: held.append(route))
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        page.get_by_role('combobox', name='解析配置路径').fill('settings/small.json')
        page.get_by_role('button', name='解析执行计划', exact=True).click()
        expect(page.get_by_role('button', name='正在解析…', exact=True)).to_be_disabled()
        page.locator('.function-nav').get_by_role('button', name='Readme', exact=True).click()
        expect(page.locator('.workbench')).to_have_count(0)
        assert len(held) == 1
        held[0].fulfill(status=200, content_type='application/json', body=json.dumps(preview))
        page.locator('.function-nav').get_by_role('button', name='管道', exact=True).click()
        expect(page.get_by_role('button', name='解析执行计划', exact=True)).to_be_enabled()
        expect(page.locator('.wb-node')).to_have_count(0)
        expect(page.locator('.wb-graph-empty')).to_have_text('尚无运行执行图')
        assert len(held) == 1


def test_optional_compiler_label_does_not_change_operator_kind_or_identity(preview):
    before = project_preview(preview)['model']['nodes'][0]
    value = copy.deepcopy(preview)
    value['dag']['nodes'][0]['label'] = 'business.operators.prepare'
    after = project_preview(value)['model']['nodes'][0]
    assert after['title'] == 'business.operators.prepare'
    assert after['kindLabel'] == before['kindLabel']
    assert after['selection'] == before['selection']
    for invalid in ('', ' ', 'x'*257, {'from_static_ast': True}):
        value['dag']['nodes'][0]['label'] = invalid
        assert project_preview(value)['model']['nodes'][0]['title'] == before['kindLabel']
