"""Actual Chromium runtime view, durable prepare replay, and narrow screens."""

import json
import os
from pathlib import Path
import threading
import time

import pytest
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.api import WorkspaceServer
from test_forge_runs import base, root, wait
from test_forge_run_faults import _slow, _first_committed


@pytest.fixture
def browser_page(root,tmp_path,monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN',str(tmp_path/'runtime-domain'))
    from playwright.sync_api import sync_playwright

    executable = Path(
        os.environ.get(
            "FORGE_CHROMIUM",
            "/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome",
        )
    )
    assert executable.is_file(), "Actual Chromium is required for runtime acceptance"
    server = WorkspaceServer(root, enable_runs=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    with sync_playwright() as engine:
        browser = engine.chromium.launch(
            executable_path=str(executable),
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.set_default_timeout(60000)
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        try:
            page.goto(server.origin + "/#token=" + server.token)
            page.get_by_role("button", name="管道", exact=True).wait_for()
            yield page, server, errors
        finally:
            browser.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


def test_rm19_real_runtime_metrics_stop_and_responsive_tabs(
    root, browser_page, tmp_path
):
    from playwright.sync_api import expect
    page, server, errors = browser_page
    value = _slow(root)
    try:
        _first_committed(root, value)
        page.get_by_role("button", name="管道", exact=True).click()
        page.get_by_role("button",name="运行记录",exact=True).click()
        page.locator('.wb-run-record[data-run="'+value["run_id"]+'"]').click()
        page.locator(".wb-run-bar > strong").wait_for()
        page.get_by_text("性能趋势 · 整次运行", exact=True).click()
        page.get_by_text("控制事件", exact=True).click()
        deadline = time.monotonic() + 15
        while (
            time.monotonic() < deadline
            and "未测量" in page.locator(".wb-run-facts > div").first.inner_text()
        ):
            page.wait_for_timeout(250)
        assert "核" in page.locator(".wb-run-facts > div").first.inner_text()
        page.get_by_role("img", name="整次运行的 CPU 核数趋势").wait_for()
        page.evaluate("scrollTo(0,0)")
        assert page.locator(".wb-run-facts").bounding_box()["y"] < 1000
        evidence = Path(os.environ.get("FORGE_RUNTIME_SCREENSHOTS", str(tmp_path)))
        evidence.mkdir(parents=True, exist_ok=True)
        for width in (1440, 390, 320):
            page.set_viewport_size({"width": width, "height": 1000})
            assert page.evaluate(
                "document.documentElement.scrollWidth<=window.innerWidth+1"
            )
            assert page.get_by_role("navigation", name="项目功能").is_visible()
            page.evaluate("scrollTo(0,0)")
            page.screenshot(
                path=str(evidence / f"runtime-running-{width}.png"), full_page=True
            )
        page.get_by_role("button", name="停止", exact=True).focus()
        page.keyboard.press("Enter")
        expect(page.locator('.wb-run-bar > .wb-tag')).to_have_text('已取消',timeout=60000)
        assert "部分输出已提交" in page.locator(".workbench").inner_text()
        assert "所属执行已停止" in page.locator(".workbench").inner_text()
        page.screenshot(
            path=str(evidence / "runtime-cancelled-320.png"), full_page=True
        )
        assert not errors, errors
        assert runs.show(root, value["run_id"])["state"]["execution_stopped"] is True
    finally:
        if runs.show(root, value["run_id"])["status"] in runs.ACTIVE:
            runs.cancel(root, value["run_id"])
            wait(root, value["run_id"], timeout=20)


def test_rm03_browser_lost_prepare_response_reuses_persistent_identity(
    root, browser_page
):
    page, server, errors = browser_page
    page.get_by_role("button", name="管道", exact=True).click()
    bodies = []

    def drop_first(route):
        bodies.append(route.request.post_data_json)
        response = route.fetch(timeout=60000)
        assert response.ok, response.text()
        route.abort("failed")
        page.unroute("**/run-prepare", drop_first)

    page.route("**/run-prepare", drop_first)
    page.get_by_role("button", name="运行", exact=True).click()
    page.get_by_role("button", name="运行", exact=True).wait_for(state="visible")
    page.locator(".error-box").last.wait_for()
    page.get_by_role("button",name="以原请求重试准备",exact=True).click()
    page.get_by_role("button", name="开始这次运行", exact=True).wait_for()
    assert len(list((root / ".forge/runs").iterdir())) == 1
    request_id = bodies[0]["request_id"]
    receipt = json.loads(
        (root / ".forge/requests" / (request_id + ".json")).read_text()
    )
    assert receipt["status"] == "ready"
    assert receipt["run_id"] in page.locator(".workbench").inner_text()
    assert not errors, errors
