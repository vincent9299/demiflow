"""D package browser self-test: real prepare/start/show on an independent port.

Runs the genuine workbench execution path (service auto-allocates port=0, never
the reserved 55971/55972/59865/59866), opens the sealed run graph, and checks the
logical projection's F10 sealed_at rendering plus the honest identity-bridge
degradation. In the merged version C produces the bridge; until then a real run
carries no bridge ref and the UI must say so rather than guess a correspondence.

Requires Chromium (FORGE_CHROMIUM or the Playwright cache); skips otherwise.
Evidence and pytest tmp are confined to /tmp/demiflow-d-* via FORGE_WORKBENCH_EVIDENCE
and --basetemp. No shared coordination directory is written.
"""
import os
from pathlib import Path

os.environ['FORGE_WORKBENCH_EVIDENCE'] = '/tmp/demiflow-d-browser-evidence'

import pytest
from playwright.sync_api import expect

from demiflow.workbench.runs import manager as runs
from test_forge_workspace import isolated
from test_forge_markdown_reading import browser
from test_forge_public_api import service
import test_forge_workbench_browser as harness
from test_forge_workbench_browser import page_for, pipeline


EVIDENCE = Path('/tmp/demiflow-d-browser-evidence')


@pytest.fixture(autouse=True)
def private_domain(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    monkeypatch.setattr(harness, 'EVIDENCE', EVIDENCE)
    EVIDENCE.mkdir(parents=True, exist_ok=True)


def shot(page, name):
    path = EVIDENCE / 'screenshots' / (name + '.png')
    path.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(path), full_page=True)


def test_real_run_sealed_graph_f10_and_honest_bridge(isolated, browser):
    with service(isolated, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        # Independent, auto-allocated port (never the reserved coordination ports).
        assert app.server_port not in (55971, 55972, 59865, 59866)
        pipeline(page)
        page.get_by_role('button', name='运行', exact=True).click()
        expect(page.locator('.wb-run-bar')).to_be_visible()
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功')
        run_id = page.evaluate('ForgeView.decodeView(location.hash)')['run_id']
        assert runs.show(isolated, run_id)['outputs'][0]['result']['ref']['row_count'] == 2
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        shot(page, 'd-real-run-sealed-graph')

        # Logical projection of the executed sealed plan.
        page.get_by_role('button', name='执行封存计划（E2）', exact=True).click()
        expect(page.locator('.wb-logical-bar')).to_contain_text('结构校验通过')
        expect(page.locator('.wb-logical-bar')).to_contain_text('已绑定本次执行计划')
        # F10: sealed_at is epoch milliseconds rendered as a real ISO instant.
        expect(page.locator('.wb-logical-bar')).to_contain_text('sealed_at 2026-')
        expect(page.locator('.wb-logical-bar')).not_to_contain_text('Invalid Date')
        expect(page.locator('.wb-logical-bar')).not_to_contain_text('sealed_at 未提供')
        shot(page, 'd-logical-sealed-bar-f10')

        # A real run has no C-produced bridge yet: the node inspector must say so.
        page.locator('.wb-graph .wb-node').filter(has_text='normalize_question').click()
        expect(page.locator('.wb-inspector')).to_contain_text('执行逻辑节点（E2）')
        expect(page.locator('.wb-inspector')).to_contain_text('尚无节点身份桥')
        expect(page.locator('.wb-inspector')).to_contain_text('未桥接，无法对应执行观测')
        expect(page.locator('.wb-inspector')).not_to_contain_text('D1b')
        shot(page, 'd-logical-node-honest-bridge-missing')
        assert not errors, errors


def test_real_run_data_graph_source_and_params_still_load(isolated, browser):
    with service(isolated, enable_runs=True) as app, page_for(browser, app) as (page, errors):
        pipeline(page)
        page.get_by_role('button', name='运行', exact=True).click()
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('成功')
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        # Sealed report-node source/params remain reachable directly (no bridge needed).
        page.locator('.wb-node').filter(has_text='normalize_question').click()
        expect(page.locator('.wb-source-body')).to_contain_text('def normalize_question')
        shot(page, 'd-real-run-sealed-source')
        assert not errors, errors