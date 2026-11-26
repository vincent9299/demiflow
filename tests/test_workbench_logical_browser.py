"""Real-browser E2 logical view: explicit absence, report projection, sealed runs, multi-action."""
from _forge_project_fixtures import declare_existing_flow
import json

import pytest
from playwright.sync_api import expect

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.runs import manager as runs

from test_forge_workspace import demo, isolated
from test_forge_public_api import service
from test_forge_markdown_reading import browser
from test_forge_workbench_browser import page_for, pipeline, screenshot

ENTRY = 'flows/questions.py:select'
PARAMS = 'flows/select_a.json'
TWO_ENTRY = 'flows/two_outputs.py:run'
TWO_PARAMS = 'flows/two_outputs.json'

TWO_FLOW = '''from demiflow import data
from operators.question_rules import (
    normalize_question,
    above_threshold,
    keep_selected,
)


def run(*, candidates, scores, target, marked_target, minimum):
    """Two outputs sharing one marked stream."""
    source = data.read_lance(
        candidates["uri"], version=candidates["version"],
        columns=["case_id", "question", "image_uri"],
    )
    normalized = source.map(
        normalize_question,
        inputs={"text": "question"},
        output="normalized_question",
        fn_kwargs={"max_chars": 4096},
    )
    grades = data.read_lance(
        scores["uri"], version=scores["version"], columns=["case_id", "score"],
    )
    joined = normalized.join(grades, on="case_id", how="inner")
    marked = joined.map(
        above_threshold,
        inputs={"score": "score"},
        output="selected",
        fn_kwargs={"minimum": minimum},
    )
    selected = marked.filter(keep_selected)
    receipt = selected.write_lance(target["uri"], mode="create", return_receipt=True)
    marked.write_lance(marked_target["uri"], mode="create", return_receipt=True)
    return receipt
'''


@pytest.fixture(autouse=True)
def private_domain(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))


def two_action_workspace(root):
    (root / 'flows/two_outputs.py').write_text(TWO_FLOW)
    (root / TWO_PARAMS).write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {'candidates': {'kind': 'table', 'alias': 'candidates'},
                      'scores': {'kind': 'table', 'alias': 'scores'},
                      'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'two_a.lance'},
                      'marked_target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'two_marked.lance'},
                      'minimum': 0.8}}))
    declare_existing_flow(root, TWO_ENTRY, params=TWO_PARAMS)
    report = project_ops.inspect_flow(root, TWO_ENTRY, params=TWO_PARAMS)
    assert report['status'] == 'checked', report.get('diagnostics')
    return report


def test_logical_view_source_absence_then_report_projection(isolated, browser):
    with service(isolated) as app, page_for(browser, app) as (page, errors):
        pipeline(page)
        expect(page.locator('.wb-basis')).to_contain_text('源码观察图')
        page.get_by_role('button', name='声明图投影（E2）', exact=True).click()
        expect(page.locator('.wb-logical-panel')).to_contain_text('语义未检查')
        expect(page.locator('.wb-logical-panel')).to_contain_text('不投影声明图')
        assert page.locator('.wb-graph .wb-node').count() == 0
        screenshot(page, 'logical-source-unchecked')
        page.get_by_role('button', name='返回数据算子图', exact=True).click()
        expect(page.locator('.wb-graph .wb-node').first).to_be_visible()
        page.get_by_role('button', name='检查', exact=True).click()
        expect(page.locator('.wb-basis')).to_contain_text('检查报告图 · checked')
        page.get_by_role('button', name='读图与配置', exact=True).click()
        page.get_by_role('button', name='声明图投影（E2）', exact=True).click()
        expect(page.locator('.wb-logical-bar')).to_contain_text('声明投影结构（未执行）')
        expect(page.locator('.wb-logical-bar')).to_contain_text('未绑定本次执行计划')
        expect(page.locator('.wb-logical-bar')).to_contain_text('demiflow.logical_dag.v1')
        expect(page.locator('.wb-logical-bar')).to_contain_text('dag_id')
        expect(page.locator('.wb-graph .wb-node')).to_have_count(7)
        expect(page.locator('.wb-graph .wb-edge')).to_have_count(6)
        expect(page.locator('.wb-graph .wb-node-shared')).to_have_count(1)
        page.locator('.wb-graph .wb-node').filter(has_text='above_threshold').click()
        expect(page.locator('.wb-inspector')).to_contain_text('声明逻辑节点（E2）')
        expect(page.locator('.wb-inspector')).to_contain_text('traits')
        expect(page.locator('.wb-inspector')).to_contain_text('声明节点')
        expect(page.locator('.wb-source-body')).to_contain_text('def above_threshold')
        screenshot(page, 'logical-report-projected')
        for width in (1920, 1440, 1200, 1024, 390, 320):
            page.set_viewport_size({'width': width, 'height': 1050})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1'), width
            screenshot(page, 'logical-layout-' + str(width))
        page.set_viewport_size({'width': 1440, 'height': 1050})
        page.get_by_role('button', name='返回数据算子图', exact=True).click()
        expect(page.locator('.wb-graph .wb-node')).to_have_count(7)
        expect(page.locator('.wb-logical-bar')).not_to_be_visible()
        assert not errors, errors


def test_logical_view_multi_action_selector(isolated, browser):
    two_action_workspace(isolated)
    with service(isolated) as app, page_for(browser, app) as (page, errors):
        pipeline(page)
        page.get_by_label('流程', exact=True).select_option(TWO_ENTRY)
        expect(page.locator('.wb-graph .wb-node')).to_have_count(8)
        expect(page.locator('.wb-graph')).not_to_have_attribute('aria-busy', 'true')
        page.get_by_role('button', name='检查', exact=True).click()
        expect(page.locator('.wb-basis')).to_contain_text('检查报告图 · checked')
        page.get_by_role('button', name='读图与配置', exact=True).click()
        page.get_by_role('button', name='声明图投影（E2）', exact=True).click()
        choice = page.get_by_label('逻辑图 action', exact=True)
        expect(choice).to_be_visible()
        assert choice.locator('option').count() == 2
        expect(page.locator('.wb-logical-bar')).to_contain_text('共 2 个 action')
        expect(page.locator('.wb-graph .wb-node')).to_have_count(7)
        first_dag = page.locator('.wb-logical-bar').text_content()
        choice.select_option(index=1)
        expect(page.locator('.wb-graph .wb-node')).to_have_count(6)
        assert page.locator('.wb-logical-bar').text_content() != first_dag
        page.locator('.wb-graph .wb-node').first.click()
        expect(page.locator('.wb-inspector')).to_contain_text('声明逻辑节点（E2）')
        screenshot(page, 'logical-multi-action')
        assert not errors, errors


def test_logical_view_projects_sealed_run_report(isolated, browser):
    run = runs.prepare(isolated, ENTRY, params=PARAMS)
    source = isolated / 'operators/question_rules.py'
    original = source.read_text()
    source.write_text(original.replace('def normalize_question', 'def changed_after_prepare'))
    with service(isolated, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        pipeline(page)
        page.get_by_role('button', name='运行记录', exact=True).click()
        page.locator('.wb-run-record[data-run="' + run['run_id'] + '"]').click()
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        page.get_by_role('button', name='声明图投影（E2）', exact=True).click()
        expect(page.locator('.wb-logical-bar')).to_contain_text('声明投影结构（未执行）')
        expect(page.locator('.wb-logical-bar')).to_contain_text('未绑定本次执行计划')
        expect(page.locator('.wb-graph .wb-node')).to_have_count(7)
        page.locator('.wb-graph .wb-node').filter(has_text='normalize_question').click()
        expect(page.locator('.wb-inspector')).to_contain_text('声明逻辑节点（E2）')
        page.get_by_role('button', name='展开代码', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('def normalize_question')
        expect(page.locator('.wb-source-body')).not_to_contain_text('def changed_after_prepare')
        expect(page.locator('.wb-source-body')).to_contain_text('封存来源')
        screenshot(page, 'logical-sealed-run')
        assert not errors, errors