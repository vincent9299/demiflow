"""Browser acceptance on an unrelated original project and two observing clients."""
import json
import os
from pathlib import Path

import pytest

from demiflow.workbench.projects import registry as console
from test_forge_project_discovery import original, business_bytes, snapshot, cached_snapshot
from test_forge_public_api import service
from test_forge_web_redesign import project_info


def test_original_project_review_and_sync(original, tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    executable = Path(os.environ.get('FORGE_CHROMIUM', ''))
    if not executable.is_file():
        pytest.skip('FORGE_CHROMIUM must point to an installed browser')
    registry = tmp_path / 'console.json'
    console.add(registry, original)
    before = business_bytes(original)
    artifact = Path(os.environ.get('FORGE_ACCEPTANCE_DIR', str(tmp_path / 'screenshots')))
    artifact.mkdir(parents=True, exist_ok=True)
    with service(None, registry=registry) as app, playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch(executable_path=str(executable), headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage'])
        page = browser.new_page(viewport={'width': 1440, 'height': 1100})
        other = browser.new_page(viewport={'width': 1440, 'height': 1100})
        errors = []
        for client in [page, other]:
            client.set_default_timeout(45000)
            client.on('pageerror', lambda error: errors.append(str(error)))
            client.goto(app.origin + '/#token=' + app.token)
            client.get_by_role('button',name='管道',exact=True).click()
            project_info(client)
            client.get_by_role('heading', name='原项目识别', exact=True).wait_for()
            client.get_by_role('cell', name='dispatch.py:run_pipeline', exact=True).wait_for()
        page.get_by_role('cell', name='dispatch.py:run_pipeline', exact=True).click()
        page.locator('pre').filter(has_text='def run_pipeline').wait_for()
        page.get_by_role('cell', name='settings/small.json', exact=True).click()
        page.locator('pre').filter(has_text='records/orders.lance').wait_for()
        page.screenshot(path=str(artifact / 'recognized-original.png'), full_page=True)
        page.get_by_role('button', name='管道', exact=True).click()
        project_info(page)
        page.get_by_role('cell', name='dispatch.py:run_pipeline', exact=True).wait_for()
        assert page.get_by_role('button', name='运行', exact=True).is_disabled()
        page.get_by_role('button', name='数据', exact=True).click()
        page.get_by_text('records/orders.lance',exact=True).click()
        page.locator('.dictionary-table[open] .dictionary-version').filter(has_text='v1').wait_for()
        page.locator('.dictionary-meta').filter(has_text='2 行').wait_for()
        page.screenshot(path=str(artifact / 'original-dictionary.png'), full_page=True)
        page.get_by_role('button', name='管道', exact=True).click()
        project_info(page)
        page.get_by_role('button', name='检查当前项目', exact=True).click()
        page.get_by_text('整体：尚未完成；静态检查：尚未完成', exact=True).wait_for()
        page.get_by_text('dispatch.py\noperators/clean.py', exact=True).wait_for()
        page.screenshot(path=str(artifact / 'original-check-coverage.png'), full_page=True)
        project_info(page)
        page.get_by_role('cell', name='history/RUN/calls.sqlite', exact=True).wait_for()
        assert business_bytes(original) == before
        (original / 'settings/second.json').write_text('{}')
        with (original / 'README.md').open('a') as target:
            target.write('[second](settings/second.json)\n')
        other.get_by_role('cell', name='settings/second.json', exact=True).wait_for()
        page.get_by_role('button', name='管道', exact=True).click()
        project_info(page)
        page.get_by_role('cell', name='settings/second.json', exact=True).wait_for()
        assert not errors, errors
        assert app.token not in page.url and app.token not in other.url
        browser.close()
