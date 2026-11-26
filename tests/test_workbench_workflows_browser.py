"""Real Chromium browser tests for multi-pipeline cross-project Workflow DAG UI and two-level drill-down."""
from __future__ import annotations

from contextlib import contextmanager
import threading

from playwright.sync_api import expect

from demiflow.workbench.api import WorkspaceServer
from test_forge_markdown_reading import browser
from test_workbench_workflows import _setup_two_projects, _write_cross_project_workflow


@contextmanager
def registry_service(registry_path):
    server = WorkspaceServer(
        registry=registry_path,
        port=0,
        enable_runs=True,
        enable_config_write=True,
    )
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_workflow_dag_ui_cross_project_drilldown_and_live_run(tmp_path, browser, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    proj_a, proj_b, reg_path, ws_a, ws_b = _setup_two_projects(tmp_path)
    wf_rel = _write_cross_project_workflow(proj_b)

    with registry_service(reg_path) as server:
        context = browser.new_context(viewport={'width': 1366, 'height': 860})
        page = context.new_page()
        js_errors = []
        workflow_http_errors = []
        launch_requests = []
        page.on('pageerror', lambda exc: js_errors.append(str(exc)))
        page.on(
            'response',
            lambda res: workflow_http_errors.append((res.status, res.url.split('#')[0]))
            if res.status >= 400 and 'workflow' in res.url
            else None,
        )
        try:
            url = f'{server.origin}/#token={server.token}'
            page.goto(url, wait_until='domcontentloaded')

            # Switch to proj_b_eval (host project containing the cross-project workflow).
            page.locator('#project-switcher').click()
            page.locator(f'#project-list [data-workspace="{ws_b}"]').click()

            # 1. Left sidebar under「管线」shows「编排」section above「单条管线」with cross-project subtitle.
            wf_item = page.locator(f'.wf-nav-item[data-workflow-path="{wf_rel}"]')
            expect(wf_item).to_be_visible(timeout=15000)
            expect(wf_item).to_contain_text('跨项目合成与双分支对比评测')
            expect(wf_item).to_contain_text('跨 2 项目')

            # 2. Clicking the workflow opens Level-1 Workflow DAG canvas rendered via ForgeWorkbenchGraph.
            wf_item.click()
            expect(page.locator('.wf-title')).to_have_text('跨项目合成与双分支对比评测', timeout=10000)
            expect(page.locator('.wf-graph-host svg.wb-graph-canvas .wb-node')).to_have_count(4, timeout=10000)
            expect(page.locator('.wf-graph-host svg.wb-graph-canvas .wb-edge')).to_have_count(4, timeout=10000)
            expect(page.locator('.wf-chip-ext')).to_contain_text('跨项目: proj_a_synth')

            # 3. Clicking the cross-project node (gen_synth) opens the right detail drawer.
            page.locator('.wf-graph-host svg.wb-graph-canvas .wb-node', has_text='跨项目生成数据').click()
            expect(page.locator('.wf-detail')).to_be_visible(timeout=5000)
            expect(page.locator('.wf-detail-title-wrap strong')).to_have_text('跨项目生成数据')
            expect(page.locator('.wf-btn-drill')).to_be_visible()

            # 4. Two-level cross-project drill-down: click「查看子管线详情 →」switches to proj_a and mounts return bar!
            page.locator('.wf-btn-drill').click()
            expect(page.locator('#workflow-return-bar')).to_be_visible(timeout=10000)
            expect(page.locator('#workflow-return-bar')).to_contain_text('返回编排：proj_b_eval / 跨项目合成与双分支对比评测')
            expect(page.locator('#workspace-name')).to_have_text('proj_a_synth', timeout=10000)

            # 5. Click「← 返回编排」returns to proj_b and restores the Workflow DAG cleanly!
            page.locator('.wf-return-btn').click()
            expect(page.locator('#workflow-return-bar')).to_be_hidden(timeout=10000)
            expect(page.locator('#workspace-name')).to_have_text('proj_b_eval', timeout=10000)
            expect(page.locator('.wf-title')).to_have_text('跨项目合成与双分支对比评测', timeout=10000)
            expect(page.locator('.wf-graph-host svg.wb-graph-canvas .wb-node')).to_have_count(4, timeout=10000)

            # 6. Click「运行编排」, verify launch modal shows both projects + input, submit, and poll live to「已完成」!
            page.locator('.wf-btn-run').click()
            dialog = page.locator('dialog.wf-launch-dialog')
            expect(dialog).to_be_visible(timeout=5000)
            expect(dialog).to_contain_text('proj_a_synth — 已授权运行')
            expect(dialog).to_contain_text('proj_b_eval — 已授权运行')
            dialog.locator('input[data-input-key="multiplier"]').fill('10')
            def admit(route):
                launch_requests.append(route.request.post_data)
                if len(launch_requests) == 1:
                    route.fulfill(status=503, json={'code':'busy','error':'Busy','operation_started':False})
                else:
                    route.continue_()
            page.route('**/workflow-start', admit)
            dialog.locator('button[type="submit"]').click()
            expect(dialog).to_be_hidden(timeout=10000)

            # Wait for real cross-project DAG execution to complete live in the browser!
            expect(page.locator('.wf-meta-row .wf-pill')).to_have_text('已完成', timeout=150000)
            expect(page.locator('.wf-footer-summary')).to_contain_text('4/4 完成', timeout=10000)

            # Verify node detail drawer now shows verified Lance output artifact and「查看子管线运行详情 →」!
            page.locator('.wf-graph-host svg.wb-graph-canvas .wb-node', has_text='双分支效果汇总').click()
            expect(page.locator('.wf-artifact-card')).to_contain_text('表 · output_table (v1)', timeout=5000)
            expect(page.locator('.wf-btn-drill')).to_have_text('查看子管线运行详情 →')

            # 7. Drill down into the completed child run and return back to the workflow run!
            page.locator('.wf-btn-drill').click()
            expect(page.locator('#workflow-return-bar')).to_be_visible(timeout=10000)
            page.locator('.wf-return-btn').click()
            expect(page.locator('#workflow-return-bar')).to_be_hidden(timeout=10000)
            expect(page.locator('.wf-meta-row .wf-pill')).to_have_text('已完成', timeout=10000)

            assert len(launch_requests) == 2 and launch_requests[0] == launch_requests[1]
            expect(page.locator('.wf-run-picker option')).to_have_count(2)  # preview and one real run

            # A lost write acknowledgement cannot become a second submit from this dialog.
            page.unroute('**/workflow-start', admit)
            unconfirmed = []
            def unknown(route):
                unconfirmed.append(route.request.post_data)
                route.fulfill(status=404, json={'code':'not_found','error':'回执未确认'})
            page.route('**/workflow-start', unknown)
            page.locator('.wf-btn-run').click()
            dialog.locator('button[type="submit"]').click()
            expect(dialog.locator('button[type="submit"]')).to_be_disabled()
            dialog.get_by_role('button', name='查看运行记录').click()
            expect(dialog).to_be_hidden()
            assert len(unconfirmed) == 1
            assert not js_errors, f'Unexpected browser page errors: {js_errors}'
            assert [(status, url.rsplit('/', 1)[-1]) for status, url in workflow_http_errors] == [(503, 'workflow-start'), (404, 'workflow-start')]
        finally:
            context.close()
