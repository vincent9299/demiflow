"""A real browser can inspect node lifecycle and open native call evidence."""
import os
import re
import shutil
from pathlib import Path
import threading

import pytest

from test_forge_runs import base, root, custom, execute
from test_map_prompt_async import server
from test_operator_foundation_models import model_files
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.api import WorkspaceServer


def test_model_node_observation_and_case_evidence_browser(root, server, tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    executable = Path(os.environ.get('FORGE_CHROMIUM', '/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome'))
    if not executable.is_file():
        pytest.skip('An installed Chromium is required')
    if os.environ.get('FORGE_FOUNDATION_DEMO_ROOT'):
        destination = Path(os.environ['FORGE_FOUNDATION_DEMO_ROOT'])
        shutil.copytree(root, destination)
        root = destination
    # This model fixture needs the optional HTTP clients in its sealed snapshot.
    _, workspace, _ = ws.load_workspace(root)
    ws.bind_platform(root, expected=workspace['platform']['digest'], extras=['llm'])
    model_files(root)
    text = (root/'flows/model.py').read_text().removeprefix('from demiflow import data\n')
    text = text.replace("data.read_lance(source, version=1)", "data.read_lance(source['uri'], version=source['version'], columns=['case_id']).limit(2).rename_columns({'case_id': 'item'}).materialize()")
    text = text.replace('write_lance(target,', "write_lance(target['uri'],")
    value = execute(root, custom(root, text))
    assert value['status'] == 'succeeded', value
    output = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(tmp_path)))
    output.mkdir(parents=True, exist_ok=True)
    app = WorkspaceServer(root, enable_runs=False)
    thread = threading.Thread(target=app.serve_forever, daemon=True)
    thread.start()
    errors = []
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch(executable_path=str(executable), headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage'])
            page = browser.new_page(viewport={'width': 1600, 'height': 1100})
            page.set_default_timeout(90000)
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.goto(app.origin + '/#token=' + app.token)
            page.get_by_role('button', name='运行管理', exact=True).click()
            page.get_by_role('cell', name=value['run_id'], exact=True).click()
            page.get_by_role('button', name=re.compile(r'^prompt .*\.map_prompt_async$')).click()
            page.get_by_role('columnheader', name='关闭钩子', exact=True).wait_for()
            assert '执行完成' in page.locator('#run-detail').inner_text()
            page.screenshot(path=str(output/'foundation-model-node.png'), full_page=True)
            page.get_by_role('button', name='查看结果', exact=True).click()
            page.get_by_role('button', name='查看调用证据 · request', exact=True).wait_for()
            assert page.get_by_role('button', name='查看调用证据 · request', exact=True).count() == 1
            page.get_by_role('button', name='查看调用证据 · request', exact=True).first.click()
            page.get_by_text('原生调用证据 · request', exact=True).wait_for()
            page.get_by_text('原生调用证据 · request', exact=True).click()
            page.screenshot(path=str(output/'foundation-case-evidence.png'), full_page=True)
            assert len(server['requests']) == 2 and not errors
            browser.close()
    finally:
        app.shutdown(); app.server_close(); thread.join(timeout=3)
