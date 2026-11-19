"""The project picker shares existing project navigation and never reserves a rail."""
from pathlib import Path
import os
import json

import pytest
from playwright.sync_api import expect

from demiflow.workbench.projects import registry
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_web_redesign import opened


@pytest.mark.parametrize('width', [1440, 768, 390, 320])
def test_project_picker_search_keyboard_and_scoped_back_navigation(tmp_path, monkeypatch, browser, width):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path/'runtime'))
    path = tmp_path/'projects.json'
    keys = {}
    for name in ['Alpha', 'Beta']:
        root = tmp_path/name
        root.mkdir()
        (root/'README.md').write_text('# '+name+' content\nOriginal project text.')
        keys[name] = registry.add(path, root, name=name)['workspace_id']
    with service(None, registry=path) as app, opened(browser, app, width=width) as (page, requests):
        page.evaluate("sessionStorage.setItem('demiflow-sidebar-collapsed','1')")
        page.reload()
        switcher = page.locator('#project-switcher')
        expect(switcher).to_contain_text('Alpha')
        expect(page.locator('#project-sidebar')).to_have_count(0)
        expect(page.locator('#project-picker')).to_be_hidden()
        expect(page.get_by_role('heading', name='Alpha content', exact=True)).to_be_visible()
        current = page.url
        switcher.click()
        search = page.get_by_role('searchbox', name='搜索项目')
        expect(search).to_be_focused()
        search.fill('missing')
        expect(page.get_by_text('没有匹配的项目', exact=True)).to_be_visible()
        assert page.url == current
        page.keyboard.press('Escape')
        expect(switcher).to_be_focused()
        expect(page.locator('#project-picker')).to_be_hidden()
        switcher.click()
        search.fill('Beta')
        assert page.locator('#project-list button:visible').count() == 1
        # The popup fits within the viewport and does not resize the workspace.
        bounds = page.locator('#project-picker').bounding_box()
        assert bounds['x'] >= 0 and bounds['x']+bounds['width'] <= width
        page.keyboard.press('ArrowDown')
        expect(page.locator('#project-list [data-workspace="'+keys['Beta']+'"]').first).to_be_focused()
        page.keyboard.press('Enter')
        expect(page.get_by_role('heading', name='Beta content', exact=True)).to_be_visible()
        expect(switcher).to_contain_text('Beta')
        expect(page.locator('#project-picker')).to_be_hidden()
        page.reload()
        expect(page.get_by_role('heading', name='Beta content', exact=True)).to_be_visible()
        page.go_back()
        expect(page.get_by_role('heading', name='Alpha content', exact=True)).to_be_visible()
        expect(switcher).to_contain_text('Alpha')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert page.locator('.workspace-content').bounding_box()['x'] == 0
        assert not [(method, url) for method, url in requests if method == 'POST']
        folder = os.environ.get('FORGE_NAVIGATION_EVIDENCE')
        if folder:
            Path(folder).mkdir(parents=True, exist_ok=True)
            switcher.click()
            page.screenshot(path=str(Path(folder)/('project-picker-'+str(width)+'.png')), full_page=True)


def test_empty_project_picker_retains_authorized_registration(tmp_path, monkeypatch, browser):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path/'runtime'))
    (tmp_path/'projects.json').write_text(json.dumps({'format':registry.FORMAT_V3,'projects':{}}))
    with service(None, registry=tmp_path/'projects.json') as app:
        with opened(browser, app) as (page, requests):
            expect(page.get_by_role('heading', name='开始使用 DemiFlow')).to_be_visible()
            page.locator('#project-switcher').click()
            picker=page.locator('#project-picker')
            expect(picker.get_by_role('button', name='注册已有项目', exact=True)).to_be_visible()
            picker.get_by_role('button', name='注册已有项目', exact=True).click()
            expect(page.get_by_role('dialog')).to_be_visible()
            expect(page.get_by_label('项目目录', exact=True)).to_be_visible()
            assert not [(method, url) for method, url in requests if method == 'POST']
