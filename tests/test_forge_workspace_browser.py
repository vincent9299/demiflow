"""Actual Chromium acceptance; no mocked case API or pre-rendered graph."""

import json
import os
from pathlib import Path
import threading

import pytest
from test_forge_workspace import demo
from demiflow.workbench.projects import workspace as api
from demiflow.workbench.api import WorkspaceServer
from demiflow.workbench.api import export_report


def test_workspace_browser_cases_graph_source_compare_and_export(demo, tmp_path):
    playwright = pytest.importorskip("playwright.sync_api")
    browser_path = Path(
        os.environ.get(
            "FORGE_CHROMIUM",
            str(
                Path.home() / ".cache/ms-playwright/chromium-1243/chrome-linux64/chrome"
            ),
        )
    )
    if not browser_path.exists():
        pytest.skip("Set FORGE_CHROMIUM to an installed Chromium binary")
    root, reports = demo
    output = Path(os.environ.get("FORGE_ACCEPTANCE_DIR", str(tmp_path)))
    output.mkdir(parents=True, exist_ok=True)
    server = WorkspaceServer(root)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    errors = []
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch(
                executable_path=str(browser_path),
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            page = browser.new_page(
                viewport={"width": 1600, "height": 1050}, device_scale_factor=1
            )
            page.set_default_timeout(60000)
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(server.origin + "/#token=" + server.token)
            page.locator("#workspace-name").filter(has_text="question-experiments").wait_for()
            page.screenshot(path=str(output / "01-workspace.png"), full_page=True)
            page.get_by_role("button", name="数据", exact=True).click()
            page.get_by_role("heading", name="单条 case · 8589934592").wait_for()
            page.get_by_role('tab', name='image_uri', exact=True).first.click()
            page.get_by_role("button", name="查看本地图片").first.click()
            page.locator(".case-image").wait_for()
            if not page.locator('.case-comparison').get_attribute('open') == '':
                page.locator('.case-comparison > summary').click()
            page.get_by_label("对照表", exact=True).select_option("comparison_fixture")
            page.get_by_label("业务键", exact=True).select_option("case_id")
            page.get_by_role("button", name="查找对应 case").click()
            page.get_by_text("匹配 1 条记录", exact=True).wait_for()
            page.get_by_role(
                "heading", name="单条 case · 8589934592"
            ).scroll_into_view_if_needed()
            page.screenshot(path=str(output / "02-real-case.png"), full_page=True)
            # The real locator is version-bound; it is never used as a join key.
            case_table = page.locator(".case-record-selector")
            case_table.locator("button").nth(1).click()
            page.get_by_role("heading", name="单条 case · 8589934593").wait_for()
            if not page.locator('.case-comparison').get_attribute('open') == '':
                page.locator('.case-comparison > summary').click()
            page.get_by_label("对照表", exact=True).select_option("comparison_fixture")
            page.get_by_label("业务键", exact=True).select_option("case_id")
            page.get_by_role("button", name="查找对应 case").click()
            page.get_by_text("匹配 2 条记录；业务键不唯一", exact=True).wait_for()
            case_table.locator("button").nth(2).click()
            page.get_by_role("heading", name="单条 case · 8589934594").wait_for()
            if not page.locator('.case-comparison').get_attribute('open') == '':
                page.locator('.case-comparison > summary').click()
            page.get_by_label("对照表", exact=True).select_option("comparison_fixture")
            page.get_by_label("业务键", exact=True).select_option("case_id")
            page.get_by_role("button", name="查找对应 case").click()
            page.get_by_text("未找到匹配记录", exact=True).wait_for()
            page.get_by_role("button",name="管道",exact=True).click()
            page.get_by_role("button",name="检查",exact=True).click()
            page.locator('.wb-basis').filter(has_text='检查报告图 · checked').wait_for()
            assert page.evaluate('ForgeView.decodeView(location.hash).report_digest')==reports['a']
            assert page.locator('.wb-graph .wb-node').count()==7
            assert page.locator('.wb-graph .wb-edge').count()==6
            page.screenshot(path=str(output/'03-flow-graph.png'),full_page=True)
            page.locator('.wb-node').filter(has_text='normalize_question').click()
            page.locator('.wb-source-body').filter(has_text='def normalize_question').wait_for()
            page.get_by_role('button',name='读图与配置',exact=True).click()
            page.locator('.wb-node').filter(has_text='above_threshold').click()
            assert page.get_by_label('配置 /arguments/minimum',exact=True).input_value()=='0.8'
            page.get_by_label('配置',exact=True).select_option('flows/select_b.json')
            page.get_by_role('button',name='检查',exact=True).click()
            page.locator('.wb-basis').filter(has_text='检查报告图 · checked').wait_for()
            assert page.evaluate('ForgeView.decodeView(location.hash).report_digest')==reports['b']
            page.get_by_role('button',name='读图与配置',exact=True).click()
            page.locator('.wb-node').filter(has_text='above_threshold').click()
            assert page.get_by_label('配置 /arguments/minimum',exact=True).input_value()=='0.9'
            assert not (root/'data/selected_a.lance').exists()
            offline=output/'flow-a.html';export_report(root,reports['a'],offline)
            page.goto(offline.as_uri())
            page.get_by_role('heading',name='接线检查报告',exact=True).wait_for()
            assert page.locator('.wb-graph .wb-node').count()==7
            assert page.get_by_role('button',name='检查',exact=True).count()==0
            assert '固定快照' in page.locator('main').inner_text()
            assert not errors, errors
            browser.close()
        (output / "browser-evidence.json").write_text(
            json.dumps(
                {
                    "workspace": str(root),
                    "reports": reports,
                    "nodes": 7,
                    "edges": 6,
                    "browser_errors": errors,
                    "candidate_executed": False,
                    "screenshots": [
                        "01-workspace.png",
                        "02-real-case.png",
                        "03-flow-graph.png",
                    ],
                    "checks": [
                        "real image",
                        "fixed row locator",
                        "explicit key",
                        "duplicate keys",
                        "missing keys",
                        "report graph",
                        "operator source",
                        "A/B params",
                        "same inspection digest",
                        "offline export",
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n"
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
