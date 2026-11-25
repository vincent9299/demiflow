"""One real page journey from a configuration error to a committed result."""
import json
import time
from urllib.parse import urlparse

import lance
from playwright.sync_api import expect

from demiflow.workbench.runs import manager as runs
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workspace import demo, isolated


def test_configuration_error_repair_and_submit_reaches_real_result(isolated, browser, tmp_path, monkeypatch):
    monkeypatch.setenv("DEMIFLOW_FORGE_DOMAIN", str(tmp_path / "runtime-domain"))
    prepares, errors = [], []
    active = None
    with service(isolated, enable_runs=True, enable_config_write=True,
                 workspace_config_write=True) as app:
        context = browser.new_context(viewport={"width": 1440, "height": 1050})
        page = context.new_page()
        page.set_default_timeout(65000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("request", lambda request: prepares.append(request.post_data_json)
                if request.method == "POST" and request.url.endswith("/run-prepare") else None)
        try:
            page.goto(app.origin + "/#token=" + app.token)
            page.locator(".function-nav").get_by_role("button", name="管线", exact=True).click()
            page.locator(".wb-node").first.wait_for()
            page.locator(".wb-more > summary").click()
            page.get_by_role("button", name="整图配置", exact=True).click()
            page.get_by_label("配置 /arguments/scores/alias", exact=True).fill("candidates")
            page.get_by_role("button", name="保存配置", exact=True).click()
            expect(page.locator(".wb-notice")).to_contain_text("配置已保存", timeout=65000)
            with page.expect_response(lambda response: response.request.method == "POST" and
                                      urlparse(response.url).path.endswith("/inspect")) as checked:
                page.locator(".wb-more > summary").click()
                page.get_by_role("button", name="检查", exact=True).click()
            assert checked.value.json()["status"] == "invalid"
            expect(page.locator(".wb-check-issues")).to_contain_text("Missing field(s): ['score']", timeout=65000)
            assert prepares == []

            page.get_by_role("button", name="返回配置与图", exact=True).click()
            page.locator(".wb-more > summary").click()
            page.get_by_role("button", name="整图配置", exact=True).click()
            page.get_by_label("配置 /arguments/scores/alias", exact=True).fill("scores")
            page.get_by_label("配置 /arguments/minimum", exact=True).fill("0.9")
            page.get_by_role("button", name="保存配置", exact=True).click()
            expect(page.locator(".wb-notice")).to_contain_text("配置已保存", timeout=65000)
            with page.expect_response(lambda response: response.request.method == "POST" and
                                      urlparse(response.url).path.endswith("/inspect")) as checked:
                page.locator(".wb-more > summary").click()
                page.get_by_role("button", name="检查", exact=True).click()
            assert checked.value.json()["status"] == "checked"
            expect(page.locator(".wb-check-issues")).to_contain_text("当前配置的接线检查通过", timeout=65000)
            parameters = json.loads((isolated / "flows/select_a.json").read_text())
            assert parameters["arguments"]["minimum"] == 0.9
            assert parameters["arguments"]["scores"]["alias"] == "scores"

            page.get_by_role("button", name="运行", exact=True).click()
            page.wait_for_function("() => ForgeView.decodeView(location.hash)?.run_id")
            active = page.evaluate("ForgeView.decodeView(location.hash).run_id")
            expect(page.locator(".wb-run-bar > .wb-tag")).to_have_text("成功", timeout=120000)
            run = runs.show(isolated, active)
            assert run["attempt"] == 1 and run["status"] == "succeeded"
            output = run["outputs"][0]
            assert output["result"]["ref"]["row_count"] == 1
            rows = lance.dataset(output["uri"]).to_table().to_pylist()
            assert [(row["case_id"], row["normalized_question"]) for row in rows] == [
                ("c01", "三角形的面积是多少？")]
            page.get_by_role("button", name="输出结果", exact=True).click()
            expect(page.locator(".wb-results")).to_contain_text("1 行", timeout=65000)
            page.get_by_role("button", name="查看固定结果", exact=True).click()
            expect(page.locator("main")).to_contain_text("c01", timeout=65000)
            assert len(prepares) == 1 and not errors
        finally:
            context.close()
            if active:
                value = runs.show(isolated, active)
                if value["status"] in runs.ACTIVE:
                    runs.cancel(isolated, active, expected_attempt=value["attempt"])
                    deadline = time.monotonic() + 20
                    while time.monotonic() < deadline and runs.show(isolated, active)["status"] in runs.ACTIVE:
                        time.sleep(0.1)
                    assert runs.show(isolated, active)["status"] not in runs.ACTIVE
