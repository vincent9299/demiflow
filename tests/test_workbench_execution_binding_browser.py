"""D package browser self-test: real prepare/start/show on an independent port.

Runs the genuine workbench execution path (service auto-allocates port=0, never
the reserved 55971/55972/59865/59866), opens the sealed run graph, and checks the
F10 sealed_at rendering plus the actual identity bridge to source nodes and
recorded observations. The merged engine must produce the binding; a missing
bridge is not accepted as a successful integration.

Migrated to the C main-graph entry: the executed sealed plan IS the main graph
(the old "运行封存计划（E2）" logical-projection toggle and .wb-logical-bar were
removed by the A-F merge). The business/identity/exception assertions are kept;
only the replaced DOM/文案 selectors move to the main graph, the node inspector
and the 节点证据 panel. The F10 sealed_at identity assertion is preserved
unchanged in intent: the merged main graph currently renders no sealed_at
instant although the server still publishes it, so that assertion fails honestly
and is recorded as a production defect (not weakened to pass).

Requires Chromium (FORGE_CHROMIUM or the Playwright cache); skips otherwise.
Evidence and pytest tmp are confined to /tmp/demiflow-d-* via FORGE_WORKBENCH_EVIDENCE
and --basetemp. No shared coordination directory is written.
"""
import os
import re
from pathlib import Path

os.environ.setdefault('FORGE_WORKBENCH_EVIDENCE', '/tmp/demiflow-d-browser-evidence')

import pytest
from playwright.sync_api import expect

from demiflow.workbench.runs import manager as runs
from test_forge_workspace import demo, isolated
from test_forge_markdown_reading import browser
from test_forge_public_api import service
import test_forge_workbench_browser as harness
from test_forge_workbench_browser import page_for, pipeline


EVIDENCE = Path(os.environ['FORGE_WORKBENCH_EVIDENCE'])


@pytest.fixture(autouse=True)
def private_domain(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    monkeypatch.setattr(harness, 'EVIDENCE', EVIDENCE)
    EVIDENCE.mkdir(parents=True, exist_ok=True)


def shot(page, name):
    path = EVIDENCE / 'screenshots' / (name + '.png')
    path.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(path), full_page=True)


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
        page.get_by_role('button', name='运行', exact=True).click()
        expect(page.locator('.wb-run-bar')).to_be_visible()
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功')
        run_id = page.evaluate('ForgeView.decodeView(location.hash)')['run_id']
        assert runs.show(isolated, run_id)['outputs'][0]['result']['ref']['row_count'] == 2
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        shot(page, 'd-real-run-sealed-graph')

        # The executed sealed plan IS the main graph now: the A-F merge removed
        # the old "运行封存计划（E2）" logical-projection toggle and .wb-logical-bar.
        # The integrated engine must publish a real identity bridge and recorded
        # observations on the main graph (a missing bridge is not a pass).
        page.locator('.wb-graph .wb-node').filter(has_text='normalize_question').click()
        expect(page.locator('.wb-inspector')).to_contain_text('normalize_question')
        # A bound execution identity proves the node is bridged to this run's plan.
        expect(page.locator('.wb-inspector')).to_contain_text('执行身份')
        expect(page.locator('.wb-inspector')).not_to_contain_text('未桥接')
        expect(page.locator('.wb-inspector')).not_to_contain_text('D1b')
        # Real recorded observations for this node (节点证据 panel, not inferred
        # from the whole-run status).
        page.get_by_role('button', name='节点证据', exact=True).click()
        expect(page.locator('.wb-run-detail-body')).to_contain_text('节点关联观测')
        expect(page.locator('.wb-run-detail-body')).to_contain_text('动作 ')
        shot(page, 'd-main-graph-node-bound-observations')

        # F10 (identity assertion, preserved unchanged in intent): the sealed-plan
        # view must render the seal time as a real ISO instant. The server still
        # publishes sealed_at in the run facts, but the merged C main graph dropped
        # the old .wb-logical-bar rendering and no replacement surface (advanced
        # diagnostics execution identity / run seal) carries it. This fails honestly
        # until the rendering is restored -- recorded as a production defect for the
        # fix lane, never weakened to pass.
        page.get_by_text('高级诊断', exact=True).click()
        advanced = page.locator('.wb-advanced')
        expect(advanced).to_be_visible()
        text = advanced.inner_text()
        assert re.search(r'sealed_at\D+2026-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', text), (
            'F10 regression: merged main graph renders no sealed_at ISO instant '
            '(server still exposes sealed_at); advanced diagnostics text=' + text[:2000])
        assert 'Invalid Date' not in text and '未提供' not in text
        shot(page, 'd-main-graph-sealed-at-f10')
        assert not errors, errors


def test_real_run_data_graph_source_and_params_still_load(isolated, browser):
    with service(isolated, enable_runs=True, enable_config_write=True, workspace_config_write=True) as app, page_for(browser, app) as (page, errors):
        select_registered_flow(page)
        page.get_by_role('button', name='运行', exact=True).click()
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功')
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        # Sealed report-node source/params remain reachable directly (no bridge needed).
        page.locator('.wb-node').filter(has_text='normalize_question').click()
        page.get_by_role('button', name='展开代码', exact=True).click()
        expect(page.locator('.wb-source-body')).to_contain_text('def normalize_question')
        shot(page, 'd-real-run-sealed-source')
        assert not errors, errors
