"""D package browser self-test: real prepare/start/show on an independent port.

Runs the genuine workbench execution path (service auto-allocates port=0, never
the reserved 55971/55972/59865/59866), opens the sealed run graph, and checks the
F10 sealed_at rendering plus the actual identity bridge to source nodes and
recorded observations. The merged engine must produce the binding; a missing
bridge is not accepted as a successful integration.

The executed sealed plan is the main graph. Its action bar exposes the seal
instant; source may be open or collapsed when entering run monitoring. Assertions bind these
surfaces to the real v3 graph, URL occurrence, bridge and observation page;
whole-run success alone cannot prove a selected node completed.

Requires Chromium (FORGE_CHROMIUM or the Playwright cache); skips otherwise.
Evidence and pytest tmp are confined to /tmp/demiflow-d-* via FORGE_WORKBENCH_EVIDENCE
and --basetemp. No shared coordination directory is written.
"""
from datetime import datetime, timezone
import json
import os
import re
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

os.environ.setdefault('FORGE_WORKBENCH_EVIDENCE', '/tmp/demiflow-d-browser-evidence')

import pytest
from playwright.sync_api import expect

import demiflow
from demiflow.workbench.graph import model
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.runs import facts
from test_forge_workspace import demo, isolated
from test_forge_markdown_reading import browser
from test_forge_public_api import request, service
import test_forge_workbench_browser as harness
from test_forge_workbench_browser import page_for, pipeline


EVIDENCE = Path(os.environ['FORGE_WORKBENCH_EVIDENCE'])
TREE = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def private_domain(tmp_path, monkeypatch):
    assert tmp_path.resolve().is_relative_to(Path('/tmp'))
    assert EVIDENCE.resolve().is_relative_to(Path('/tmp'))
    for module in (demiflow, model, runs):
        assert Path(module.__file__).resolve().is_relative_to(TREE), module.__file__
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    monkeypatch.setattr(harness, 'EVIDENCE', EVIDENCE)
    EVIDENCE.mkdir(parents=True, exist_ok=True)


def shot(page, name):
    path = EVIDENCE / 'screenshots' / (name + '.png')
    path.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(path), full_page=True)


def record(name, value):
    out = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(EVIDENCE)))
    assert out.resolve().is_relative_to(Path('/tmp'))
    out.mkdir(parents=True, exist_ok=True)
    (out / (name + '.json')).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def run_selected_flow(page, app, root):
    page.get_by_role('button', name='运行', exact=True).click()
    expect(page.locator('.wb-run-bar')).to_be_visible()
    # Wait for a genuine terminal state: a real cold runtime can exceed the
    # ordinary 65-second UI interaction timeout without being a failed run.
    expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text(
        re.compile(r'成功|失败|已取消|中断'), timeout=240000)
    run_id = page.evaluate('ForgeView.decodeView(location.hash)')['run_id']
    shown = runs.show(root, run_id)
    assert shown['status'] == 'succeeded', shown
    expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功')
    assert shown['outputs'][0]['result']['ref']['row_count'] == 2
    expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
    status, graph = request(app, '/api/graph?' + urlencode(dict(
        basis_kind='run', run_id=run_id, attempt=shown['attempt'])))
    assert status == 200, graph
    assert graph['binding']['run_id'] == run_id
    assert graph['binding']['attempt'] == shown['attempt']
    action_id = page.get_by_label('执行步骤', exact=True).input_value()
    action = next(item for item in graph['logical']['actions'] if item['action_id'] == action_id)
    assert action['status'] == 'sealed' and action['version'] == 'v3'
    assert action['dag']['schema_version'] == 'demiflow.dataflow_graph.v3'
    assert len(action['dag']['nodes']) == 7 and len(action['dag']['edges']) == 6
    expect(page.locator('.wb-graph .wb-node')).to_have_count(len(action['dag']['nodes']))
    return shown, graph, action


def selected_occurrence(page, graph, action, operator):
    selection = page.evaluate('ForgeView.decodeView(location.hash)')
    expected = dict(run_id=graph['binding']['run_id'], attempt=graph['binding']['attempt'],
                    report_digest=graph['report_digest'], action_id=action['action_id'],
                    dag_id=action['dag']['dag_id'], plan_id=action['plan_id'],
                    plan_revision=action['physical']['plan_revision'], plan_format='v3')
    assert {key: selection.get(key) for key in expected} == expected
    assert action['identity_bridge']['available'] is True
    bridge = action['identity_bridge']['bridge']
    assert bridge['action_id'] == action['action_id']
    assert bridge['dag_id'] == action['dag']['dag_id']
    assert bridge['report_digest'] == graph['report_digest']
    members = [item for item in bridge['entries']
               if item['logical_node_id'] == selection['logical_node_id']
               and item['occurrence_id'] == selection['occurrence_id']]
    assert len(members) == 1
    member = members[0]
    assert member['report_node_id'] == selection['node_id']
    assert any(node['id'] == selection['node_id'] and
               (node.get('operator') or {}).get('name') == operator for node in graph['nodes'])
    assert any(node['logical_node_id'] == selection['logical_node_id'] and
               node['occurrence_id'] == selection['occurrence_id'] for node in action['dag']['nodes'])
    return selection, member


def assert_source_response(response, selection, graph):
    assert response.status == 200
    query = parse_qs(urlsplit(response.url).query)
    assert json.loads(query['graph_binding'][0]) == graph['binding']
    assert query['node_id'] == [selection['node_id']]
    assert query['logical_node_id'] == [selection['logical_node_id']]
    assert query['occurrence_id'] == [selection['occurrence_id']]
    source = response.json()
    assert source['graph_binding'] == graph['binding']
    assert source['node_id'] == selection['node_id']
    assert source['historical'] is True and source['read_only'] is True
    assert source['source_basis'] == 'sealed' and source['availability'] == 'available'
    return source


def select_registered_flow(page):
    pipeline(page)
    page.get_by_label('流程', exact=True).select_option('flows/questions.py:select')
    expect(page.get_by_label('配置', exact=True)).to_have_value('flows/select_a.json')
    expect(page.locator('.wb-source-body')).to_contain_text('def _select')
    # The bounded service may reject an inspect before admission while graph
    # source reads are still completing. Retry only an explicit not-started reply.
    for attempt in range(5):
        page.get_by_role('button', name='检查', exact=True).click()
        page.wait_for_function("() => document.querySelector('.wb-basis')?.textContent.includes('checked') || document.querySelector('.wb-notice')?.textContent.includes('尚未开始')")
        if 'checked' in page.locator('.wb-basis').inner_text():
            break
        page.wait_for_timeout(500)
    expect(page.locator('.wb-basis')).to_contain_text('检查报告图 · checked')
    page.get_by_role('button', name='读图与配置', exact=True).click()
    expect(page.locator('.wb-node').filter(has_text='normalize_question')).to_be_visible()


def test_real_run_sealed_graph_f10_and_honest_bridge(isolated, browser):
    with service(isolated, enable_runs=True, enable_config_write=True, workspace_config_write=True) as app, page_for(browser, app) as (page, errors):
        # Independent, auto-allocated port (never the reserved coordination ports).
        assert app.server_port not in (55971, 55972, 59865, 59866)
        select_registered_flow(page)
        shown, graph, action = run_selected_flow(page, app, isolated)
        shot(page, 'd-real-run-sealed-graph')

        page.locator('.wb-graph .wb-node').filter(has_text='normalize_question').click()
        selection, member = selected_occurrence(page, graph, action, 'normalize_question')
        expect(page.locator('.wb-inspector')).to_contain_text('normalize_question')
        expect(page.locator('.wb-inspector')).to_contain_text('执行完成')
        expect(page.locator('.wb-inspector')).not_to_contain_text('未桥接')
        expect(page.locator('.wb-inspector')).not_to_contain_text('D1b')
        with page.expect_response(lambda response: '/run-observations?' in response.url) as observed:
            page.get_by_role('button', name='节点证据', exact=True).click()
        assert observed.value.status == 200
        observations = observed.value.json()
        scope = dict(action_id=action['action_id'], dag_id=action['dag']['dag_id'],
                     plan_id=action['plan_id'], plan_revision=action['physical']['plan_revision'])
        assert observations['run_id'] == shown['run_id'] and observations['attempt'] == shown['attempt']
        assert {key: observations.get(key) for key in scope} == scope
        rows = [row for row in observations['observations']
                if row['node_id'] == selection['node_id'] and row['occurrence'] == member['report_occurrence']]
        assert rows
        for row in rows:
            assert {key: row.get(key) for key in scope} == scope
            assert row['occurrence_id'] == selection['occurrence_id']
            assert row['state'] == 'succeeded'
        expect(page.locator('.wb-run-detail-body')).to_contain_text('节点关联观测')
        expect(page.locator('.wb-run-detail-body')).to_contain_text('动作 ' + action['action_id'])
        expect(page.locator('.wb-run-detail-body .wb-observation')).to_have_count(len(rows))
        shot(page, 'd-main-graph-node-bound-observations')

        # F10 binds the rendered ISO instant to this action's original epoch-ms
        # seal; neither today's date nor an unrelated diagnostic text can pass.
        assert type(action['sealed_at']) is int
        instant = datetime.fromtimestamp(action['sealed_at'] / 1000, tz=timezone.utc)
        expected_iso = instant.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        sealed_time = page.locator('.wb-action-bar time.wb-sealed-time')
        expect(sealed_time).to_be_visible()
        expect(sealed_time).to_have_attribute('datetime', expected_iso)
        expect(sealed_time).to_contain_text('封存于')
        text = sealed_time.inner_text()
        assert 'Invalid Date' not in text and '未提供' not in text
        shot(page, 'd-main-graph-sealed-at-f10')
        record('execution-binding-f10', dict(graph=graph, selection=selection,
            observation_page=observations, sealed_at=action['sealed_at'], rendered_datetime=expected_iso))
        assert not errors, errors


def test_real_run_data_graph_source_and_params_still_load(isolated, browser):
    with service(isolated, enable_runs=True, enable_config_write=True, workspace_config_write=True) as app, page_for(browser, app) as (page, errors):
        select_registered_flow(page)
        shown, graph, action = run_selected_flow(page, app, isolated)
        with page.expect_response(lambda response: '/graph-source?' in response.url) as loaded:
            page.locator('.wb-graph .wb-node').filter(has_text='normalize_question').click()
            expand = page.get_by_role('button', name='展开代码', exact=True)
            if expand.is_visible():
                expand.click()
        selection, _ = selected_occurrence(page, graph, action, 'normalize_question')
        source = assert_source_response(loaded.value, selection, graph)
        assert source['sha256'] == graph['sources'][source['path']]
        assert 'def normalize_question' in source['text']
        bound = model.bound_source(isolated, graph['binding'], selection['node_id'],
            logical_node_id=selection['logical_node_id'], occurrence_id=selection['occurrence_id'])
        assert bound['sealed_code_pin_ref'] == action['code_pin_ref']
        expect(page.locator('.wb-source-body')).to_be_visible()
        expect(page.locator('.wb-source-body')).to_contain_text('封存来源')
        expect(page.locator('.wb-source-body')).to_contain_text('def normalize_question')
        shot(page, 'd-real-run-sealed-source')
        page.locator('.wb-graph .wb-node').filter(has_text='above_threshold').click()
        parameter_selection, _ = selected_occurrence(page, graph, action, 'above_threshold')
        minimum = next(field for field in graph['parameters']['fields'] if field['pointer'] == '/arguments/minimum')
        assert minimum['value'] == 0.8 and minimum['writable'] is False
        expect(page.get_by_label('配置 /arguments/minimum', exact=True)).to_have_value(str(minimum['value']))
        expect(page.get_by_label('配置 /arguments/minimum', exact=True)).to_be_disabled()
        with page.expect_response(lambda response: '/graph-source?' in response.url and
                                  parse_qs(urlsplit(response.url).query).get('kind') == ['configuration']) as configured:
            page.locator('.wb-source').get_by_role('button', name='入参', exact=True).click()
        parameters = assert_source_response(configured.value, parameter_selection, graph)
        assert any(field['pointer'] == minimum['pointer'] and field['value'] == minimum['value']
                   for field in parameters['fields'])
        record('execution-binding-source-params', dict(run_id=shown['run_id'], action_id=action['action_id'],
            selection=selection, source=source, sealed_code_pin_ref=bound['sealed_code_pin_ref'],
            parameter_selection=parameter_selection, parameters=parameters))
        assert not errors, errors
