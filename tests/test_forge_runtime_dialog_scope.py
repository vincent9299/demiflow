"""Real two-project inline settings: immutable targets and stale navigation fences."""
import json
import shutil
import uuid

import pytest
from playwright.sync_api import expect

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from test_forge_runtime_core_flow import base, root, unregistered, browser
from test_forge_public_api import service
from test_forge_web_redesign import opened


@pytest.fixture
def projects(unregistered, tmp_path):
    second = tmp_path / 'second'
    shutil.copytree(unregistered, second)
    for path, name in ((unregistered, 'Project A'), (second, 'Project B')):
        value = ws.read_control(path/'workspace.json')
        expected = content_digest(value)
        value.update(name=name, id='w'+uuid.uuid4().hex)
        ws.write_control(path/'workspace.json', value, expected=expected)
    registry = tmp_path/'registry.json'
    return registry, unregistered, second


def register_projects(projects, kind):
    registry, first, second = projects
    ids = [console.add(registry, path, allow_runs=kind == 'setup')['workspace_id']
           for path in (first, second)]
    return ids


def go(page, project, kind='flows'):
    page.evaluate('value => location.hash=ForgeView.encodeView(value)',
                  dict(v=2, workspace=project, kind=kind))
    page.wait_for_function('v => document.querySelector(".project-item[aria-current=true]")?.dataset.workspace===v.workspace && ForgeView.decodeView(location.hash)?.kind===v.kind',
                           arg=dict(workspace=project, kind=kind))
    if kind == 'flows':page.locator('.workbench').wait_for()
    else:page.locator('#project-readme').wait_for()


def open_dialog(page, kind):
    dialog = page.locator('.wb-settings')
    if not dialog.is_visible():page.get_by_role('button',name='接入与权限',exact=True).click()
    if kind == 'setup':
        dialog.get_by_label('登记流程入口', exact=True).fill('flows/questions.py:select')
        dialog.get_by_label('登记参数文件', exact=True).fill('flows/select_a.json')
    else:
        dialog.get_by_label('接入与提交', exact=True).check()
    dialog.evaluate('(node)=>window.oldSettingsNode=node')
    dialog.get_by_role('button',name=action(kind)[0],exact=True).evaluate('(node)=>window.oldRuntimeButton=node')
    return dialog


def action(kind):
    return ('登记原生流程', 'flow-register') if kind == 'setup' else ('保存运行权限', 'project-permissions')


def assert_unchanged_second(projects, ids, kind):
    registry, _, second = projects
    assert not ws.read_control(second/'workspace.json')['entries']
    assert console.permissions(console.load_registry(registry)['projects'][ids[1]]) == {
        key: kind == 'setup' for key in console.PERMISSIONS}


@pytest.mark.parametrize('kind', ['setup', 'permissions'])
@pytest.mark.parametrize('navigation', ['hash', 'back'])
def test_navigation_invalidates_dialog_and_detached_submit(projects, browser, kind, navigation):
    registry, first, second = projects
    ids = register_projects(projects, kind)
    with service(None, registry=registry, enable_runs=True) as app, opened(browser, app) as (page, requests):
        page.set_default_timeout(30000)
        go(page, ids[1]);go(page, ids[0])
        dialog = open_dialog(page, kind)
        if navigation == 'back':
            page.go_back()
            page.get_by_text('Project B', exact=True).first.wait_for()
        else:
            go(page, ids[1])
        expect(page.locator('#workspace-name')).to_have_text('Project B')
        assert not page.evaluate('window.oldSettingsNode.isConnected')
        # A retained old DOM callback must also be incapable of writing after navigation.
        page.evaluate('window.oldRuntimeButton.click()')
        page.wait_for_timeout(100)
        assert not any(method == 'POST' and url.endswith(action(kind)[1]) for method, url in requests)
        assert_unchanged_second(projects, ids, kind)
        assert not ws.read_control(first/'workspace.json')['entries']
        assert not list((first/'.forge/runs').glob('*'))


def wait_held(page, held):
    for _ in range(100):
        if held:
            return held[0]
        page.wait_for_timeout(50)
    raise AssertionError('Expected bounded held response')


@pytest.mark.parametrize('kind', ['setup', 'permissions'])
def test_late_mutation_response_does_not_restore_departed_view(projects, browser, kind):
    registry, first, _ = projects
    ids = register_projects(projects, kind)
    with service(None, registry=registry, enable_runs=True) as app, opened(browser, app) as (page, _):
        page.set_default_timeout(30000)
        go(page, ids[0]);dialog = open_dialog(page, kind)
        label, endpoint = action(kind)
        held = []
        page.route('**/'+endpoint, lambda route: held.append(route))
        dialog.get_by_role('button', name=label, exact=True).click()
        route = wait_held(page, held)
        response = route.fetch(timeout=30000)
        assert response.ok, response.text()
        # Same-project history navigation is allowed even while a POST waits.
        go(page, ids[0], 'overview')
        assert not page.evaluate('window.oldSettingsNode.isConnected')
        route.fulfill(response=response)
        page.wait_for_timeout(750)
        assert page.evaluate('ForgeView.decodeView(location.hash).kind') == 'overview'
        assert_unchanged_second(projects, ids, kind)
        if kind == 'setup':
            assert ws.read_control(first/'workspace.json')['entries']['flows/questions.py:select'] == ['flows/select_a.json']
        else:
            assert console.permissions(console.load_registry(registry)['projects'][ids[0]])['run_submit']


@pytest.mark.parametrize('kind', ['setup', 'permissions'])
def test_late_context_cannot_replace_other_project_or_save_its_draft(projects, browser, kind):
    registry, first, second = projects
    ids = register_projects(projects, kind)
    with service(None, registry=registry, enable_runs=True) as app, opened(browser, app) as (page, _):
        page.set_default_timeout(30000)
        go(page, ids[0]);dialog = open_dialog(page, kind)
        held = []
        pattern = '**/api/workspaces/'+ids[0]+'/context'
        page.route(pattern, lambda route: held.append(route))
        dialog.get_by_role('button', name=action(kind)[0], exact=True).click()
        route = wait_held(page, held)
        response = route.fetch(timeout=30000)
        assert response.ok, response.text()
        go(page, ids[1])
        assert not page.evaluate('window.oldSettingsNode.isConnected')
        route.fulfill(response=response)
        page.wait_for_timeout(750)
        assert page.evaluate('ForgeView.decodeView(location.hash).workspace') == ids[1]
        expect(page.locator('#workspace-name')).to_have_text('Project B')
        assert page.locator('.wb-run-bar > strong').count() == 0
        assert_unchanged_second(projects, ids, kind)
        assert not list((second/'.forge/runs').glob('*'))
