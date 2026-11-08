"""Removing registration revokes console access without deleting project state."""
import hashlib
import json
import os
from pathlib import Path
import threading

import pytest
from click.testing import CliRunner

from demiflow.cli import main
from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.projects import watch
from demiflow.forge.platform import content_digest
from test_forge_project_management import snapshot, cached_snapshot
from test_forge_public_api import request, service


def files(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file()}


def project(registry, root):
    root.mkdir()
    (root/'pipeline.py').write_text('raise RuntimeError("never execute during removal")\n')
    (root/'history').mkdir()
    (root/'history/result.json').write_text('{"old_result":"keep"}')
    return console.add(registry, root)


@pytest.mark.parametrize('broken', [False, True])
def test_http_removal_preserves_every_file_and_revokes_routes(tmp_path, broken):
    registry, root = tmp_path/'console.json', tmp_path/'pipeline'
    added = project(registry, root)
    key = added['workspace_id']
    if broken:
        (root/'workspace.json').write_text('{broken')
    before = files(root)
    with service(None, registry=registry) as app:
        inventory = request(app, '/api/projects')[1]
        body = dict(workspace_id=key, expected=inventory['digest'])
        assert 'project-remove' in request(app, '/api/capabilities')[1]['operations']['POST']
        assert request(app, '/api/project-remove', body, headers={'X-Forge-Token':'wrong'})[0] == 403
        assert request(app, '/api/project-remove', {**body, 'delete_files':True})[0] == 400
        assert request(app, '/api/project-remove', {**body, 'expected':'0'*64})[0] == 409
        assert key in console.load_registry(registry)['projects']
        assert request(app, '/api/project-remove', body) == (200, dict(status='removed', workspace_id=key))
        assert request(app, '/api/projects')[1]['projects'] == []
        assert request(app, f'/api/workspaces/{key}/context')[0] == 404
        assert request(app, f'/api/workspaces/{key}/project-check', {})[0] == 404
        assert request(app, f'/api/workspaces/{key}/run-prepare', {})[0] == 404
        assert request(app, '/api/project-remove', body)[0] == 409
        assert files(root) == before
    if not broken:
        assert console.add(registry, root)['workspace_id'] == key
        assert files(root) == before


def test_single_project_service_cannot_remove_registration(tmp_path):
    registry, root = tmp_path/'console.json', tmp_path/'pipeline'
    key = project(registry, root)['workspace_id']
    expected = content_digest(console.load_registry(registry))
    with service(root) as app:
        assert request(app, '/api/project-remove', dict(workspace_id=key,expected=expected))[0] == 422
    assert key in console.load_registry(registry)['projects']


def test_registry_watcher_releases_removed_project(tmp_path, monkeypatch):
    registry, root = tmp_path/'console.json', tmp_path/'pipeline'
    key = project(registry, root)['workspace_id']
    stop, observed_empty = threading.Event(), threading.Event()
    visited = []
    load = console.load_registry
    def inventory(path):
        value = load(path)
        if not value['projects']:
            observed_empty.set()
        return value
    def step(path, *args, **kwargs):
        visited.append(str(path))
        console.remove(registry,key,expected=content_digest(load(registry)))
        return False
    monkeypatch.setattr(console,'load_registry',inventory)
    monkeypatch.setattr(watch,'_step',step)
    thread = threading.Thread(target=watch.watch,args=(registry,),kwargs=dict(interval=1,full_interval=2,stop=stop))
    thread.start()
    try:
        assert observed_empty.wait(10)
    finally:
        stop.set();thread.join(timeout=5)
    assert not thread.is_alive()
    assert visited == [str(root)]
    with ws.audit_lease(root,'watch'):
        pass


def test_browser_cancel_conflict_two_clients_remove_last_and_register_again(tmp_path):
    playwright=pytest.importorskip('playwright.sync_api')
    executable=Path(os.environ.get('FORGE_CHROMIUM',''))
    if not executable.is_file():
        pytest.skip('FORGE_CHROMIUM must point to an installed browser')
    registry=tmp_path/'console.json'
    alpha,beta=tmp_path/'alpha',tmp_path/'beta'
    alpha_id=project(registry,alpha)['workspace_id']; before=files(alpha)
    with service(None,registry=registry) as app, playwright.sync_playwright() as runtime:
        browser=runtime.chromium.launch(executable_path=str(executable),headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
        pages=[browser.new_page(viewport={'width':1440,'height':1000}) for _ in range(2)]
        errors=[]
        for page in pages:
            page.set_default_timeout(30000)
            page.on('pageerror',lambda error:errors.append(str(error)))
            page.goto(app.origin+'/#token='+app.token)
            page.locator('#workspace-name').filter(has_text='alpha').wait_for()
        first,second=pages
        first.get_by_role('button',name='项目设置',exact=True).click()
        first.get_by_role('button',name='移除项目 alpha',exact=True).click()
        dialog=first.get_by_role('dialog')
        dialog.get_by_role('button',name='取消',exact=True).click()
        assert alpha_id in console.load_registry(registry)['projects']
        first.get_by_role('button',name='项目设置',exact=True).click()
        first.get_by_role('button',name='移除项目 alpha',exact=True).click()
        beta_id=project(registry,beta)['workspace_id']; beta_before=files(beta)
        dialog.get_by_role('button',name='确认移除',exact=True).click()
        dialog.locator('.error-box').filter(has_text='Console registry changed').wait_for()
        assert alpha_id in console.load_registry(registry)['projects']
        dialog.get_by_role('button',name='取消',exact=True).click()
        first.locator('#project-list button').filter(has_text='beta').wait_for(state='attached')
        first.get_by_role('button',name='项目设置',exact=True).click()
        first.get_by_role('button',name='移除项目 alpha',exact=True).click()
        artifact=os.environ.get('FORGE_ACCEPTANCE_DIR')
        if artifact:
            Path(artifact).mkdir(parents=True,exist_ok=True)
            first.screenshot(path=str(Path(artifact)/'remove-project.png'))
        dialog.get_by_role('button',name='确认移除',exact=True).click()
        for page in pages:
            page.locator('#workspace-name').filter(has_text='beta').wait_for()
            assert page.get_by_role('button',name='移除项目 alpha',exact=True).count()==0
            assert page.locator(f'#project-list [data-workspace="{alpha_id}"]').count()==0
            playwright.expect(page.locator('main')).not_to_contain_text('alpha',timeout=30000)
        assert files(alpha)==before
        # Removing the last project through CLI clears both clients' content/URLs.
        expected=content_digest(console.load_registry(registry))
        cli=CliRunner().invoke(main,['workbench','console','--registry',str(registry),'remove',beta_id,'--expected',expected])
        assert cli.exit_code==0,cli.output
        for page in pages:
            page.get_by_role('heading',name='开始使用 DemiFlow').wait_for()
            assert page.locator('#project-list button').count()==0
            assert page.locator('#project-list .project-empty').inner_text()=='尚未登记项目'
            assert '#view=' not in page.url
        assert files(beta)==beta_before
        assert console.add(registry,alpha)['workspace_id']==alpha_id
        for page in pages:
            page.locator('#workspace-name').filter(has_text='alpha').wait_for()
        assert files(alpha)==before
        assert not errors,errors
        browser.close()
