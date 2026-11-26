"""One approved runtime and development deployment, fixed shared host envelope."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import time

import pytest

from demiflow.workbench.projects import registry as console
from demiflow.forge import host_partition

from demiflow.forge.runs import domain as run_domain

from demiflow.forge.runs import manager as runs

from demiflow.workbench.conversations.policy import Deployment
from demiflow.workbench.conversations.service import Service
from demiflow.forge.schema import ForgeError
from demiflow.workbench.api import WorkspaceServer
from test_forge_development_chat import configuration, wait_turn
from test_forge_development_chat_lifecycle import wait_running
from test_forge_development_chat_http import request
from test_forge_runs import base, root, wait
from test_forge_run_faults import _slow, _first_committed
from test_forge_runtime_integration import _server, _request

GiB = 1024**3


def combined_configuration(tmp_path, project, monkeypatch):
    _, config, value = configuration(tmp_path)
    (project / 'AGENTS.md').write_text('Preserve all run outputs. Development writes only notes.py.')
    (project / 'notes.py').write_text('# fixture note\n')
    value['projects']['wfixture'].update(root=str(project), writes=['notes.py'], watch=['notes.py'])
    value['limits']['turn_seconds'] = 120
    legacy = Deployment(value)
    registry = tmp_path / 'registry.json'
    workspace = console.add(registry, project, allow_runs=True)['workspace_id']
    value['projects'][workspace] = value['projects'].pop('wfixture')
    domain = tmp_path / 'runtime-domain'; domain.mkdir()
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(domain))
    (domain / 'policy.json').write_text(json.dumps(host_partition.DEFAULTS))
    envelope = dict(format='demiflow_forge_host_budget_v1', memory_bytes=24*GiB, other_memory_bytes=5*GiB,
        development_memory_bytes=legacy.memory_reservation, development_disk_bytes=legacy.disk_reservation,
        development_state_root=value['state_root'], workspaces=[str(project)],
        devices=[dict(path=str(tmp_path), bytes=32*GiB, other_bytes=GiB)])
    (domain / 'host-budget.json').write_text(json.dumps(envelope))
    checked = host_partition.load(domain, host_partition.DEFAULTS, required=True)
    value['runtime_domain'] = str(domain)
    value['host_budget'].update(memory_bytes=24*GiB,run_memory_bytes=checked['run_memory_bytes'],
        other_memory_bytes=5*GiB,disk_bytes=32*GiB,run_disk_bytes=checked['run_disk_bytes'],other_disk_bytes=GiB)
    config.write_text(json.dumps(value))
    return config, registry, workspace, domain, envelope


@pytest.mark.parametrize('resource', ['memory', 'disk', 'history', 'provider_memory', 'provider_disk', 'other_memory'])
def test_actual_runtime_and_history_are_not_free_capacity(tmp_path, monkeypatch, resource):
    project = tmp_path / 'native'; project.mkdir()
    config, registry, workspace, domain, envelope = combined_configuration(tmp_path, project, monkeypatch)
    checked = Deployment.load(config).validate_runtime(admit=True)
    expected_disk = 16*GiB + 4*GiB + 16*1024**2 + 4*1024**2
    assert checked['run_memory_bytes'] == 8*GiB and checked['run_disk_bytes'] == expected_disk
    assert checked['memory_reserved_bytes'] == 8*GiB + envelope['development_memory_bytes'] + 5*GiB
    if resource == 'memory': envelope['memory_bytes'] = checked['memory_reserved_bytes'] - 1
    elif resource == 'disk': envelope['devices'][0]['bytes'] = checked['devices'][checked['development_device']]['reserved_bytes'] - 1
    elif resource == 'history':
        extra = tmp_path / 'second-runtime-project'; extra.mkdir()
        envelope['workspaces'].append(str(extra))
        envelope['devices'][0]['bytes'] = checked['devices'][checked['development_device']]['reserved_bytes']
    elif resource == 'provider_memory': envelope['development_memory_bytes'] -= 1
    elif resource == 'provider_disk': envelope['development_disk_bytes'] -= 1
    else: envelope['other_memory_bytes'] = host_partition.OTHER_MEMORY_MINIMUM - 1
    (domain / 'host-budget.json').write_text(json.dumps(envelope))
    if resource in {'memory', 'disk', 'history'}:
        with pytest.raises(ForgeError, match='partition'): host_partition.policy(domain)
    with pytest.raises(ForgeError): WorkspaceServer(registry=registry, enable_runs=True, development_config=config)


def test_combined_domain_owner_scope_and_available_space_fail_closed(tmp_path, monkeypatch):
    from types import SimpleNamespace
    project = tmp_path / 'native'; project.mkdir()
    config, registry, workspace, domain, envelope = combined_configuration(tmp_path, project, monkeypatch)
    with monkeypatch.context() as m:
        m.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'wrong-domain'))
        with pytest.raises(ForgeError, match='domain differs'):
            WorkspaceServer(registry=registry, enable_runs=True, development_config=config)
    other = tmp_path / 'outside-history'; other.mkdir()
    with pytest.raises(ForgeError, match='history partition'): run_domain.history_admission(other)
    with pytest.raises(ForgeError, match='device'):
        host_partition.load(domain, host_partition.DEFAULTS, devices=[-1])
    service = Service(config)
    try:
        sid = service.create(workspace, project, 'create', 'codex')['id']
        with monkeypatch.context() as m:
            real = os.statvfs
            def full(path):
                fs = real(path)
                return SimpleNamespace(f_blocks=fs.f_blocks, f_frsize=fs.f_frsize, f_bavail=1)
            m.setattr(os, 'statvfs', full)
            with pytest.raises(ForgeError, match='combined host'):
                service.start(workspace, project, sid, 'first', 0, 'normal')
        assert service.store.show(workspace, sid)['turn'] is None and not service.workers
        # Growing the actual runtime cap cannot steal the fixed chat allocation.
        (domain / 'policy.json').write_text(json.dumps(dict(host_partition.DEFAULTS,max_rss_bytes=24*GiB)))
        with pytest.raises(ForgeError, match='RSS'): host_partition.policy(domain)
        with pytest.raises(ForgeError, match='RSS'): service.start(workspace, project, sid, 'first', 0, 'normal')
    finally: service.close()


def test_combined_read_worker_tree_uses_reserved_protection(tmp_path, monkeypatch):
    import lance
    import pyarrow as pa
    from demiflow.forge import platform_worker
    from demiflow.workbench.worker import operation
    project=tmp_path/'native';project.mkdir()
    combined_configuration(tmp_path,project,monkeypatch)
    table = project / 'source.lance'
    lance.write_dataset(pa.table({'value': [1]}), str(table))
    assert operation('describe', table, version=1)['row_count'] == 1
    actual = platform_worker.subprocess.Popen
    children = []
    def launch(*args, **kwargs):
        child = actual(*args, **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(platform_worker.subprocess, 'Popen', launch)
    monkeypatch.setattr(host_partition,'READONLY_RSS',1)
    with pytest.raises(ForgeError,match='RSS reservation'):
        operation('describe', table, version=1)
    assert len(children) == 1 and children[0].poll() is not None


@pytest.mark.parametrize('configured', [False, True])
def test_read_worker_explicit_smaller_reservation_reaps_child(tmp_path, monkeypatch, configured):
    import sys
    from demiflow.forge import platform_worker
    from demiflow.forge.platform import limits_with
    if configured:
        project = tmp_path / 'native'; project.mkdir()
        combined_configuration(tmp_path, project, monkeypatch)
    else:
        monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'unconfigured'))
    actual = platform_worker.subprocess.Popen
    children = []
    def launch(*args, **kwargs):
        child = actual(*args, **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(platform_worker.subprocess, 'Popen', launch)
    with pytest.raises(ForgeError, match='RSS reservation'):
        platform_worker.capture([sys.executable, '-B', '-c', 'import time;time.sleep(30)'],
            {}, limits_with({'timeout_seconds': 2}), tree_rss_limit=1)
    assert len(children) == 1 and children[0].poll() is not None


def test_standalone_forge_capture_does_not_read_workbench_deployment(tmp_path, monkeypatch):
    import sys
    from demiflow.forge.platform_worker import capture
    from demiflow.forge.platform import limits_with
    project = tmp_path / 'native'; project.mkdir()
    combined_configuration(tmp_path, project, monkeypatch)
    monkeypatch.setattr(host_partition, 'READONLY_RSS', 1)
    code = 'import json; print(json.dumps({"format":"demiflow_forge_platform_catalog_v1"}))'
    assert capture([sys.executable, '-B', '-c', code], {}, limits_with())['format'] == 'demiflow_forge_platform_catalog_v1'


def test_combined_run_and_chat_keep_separate_stop_lanes_and_receipts(root, tmp_path, monkeypatch):
    config, registry, workspace, domain, envelope = combined_configuration(tmp_path, root, monkeypatch)
    value = _slow(root)
    try:
        _first_committed(root, value)
        with _server(registry=registry, control_only=True, development_config=config) as server:
            assert not server.enable_runs and server.control_only
            prefix = '/api/workspaces/' + workspace + '/'
            assert _request(server,prefix+'run-prepare',dict(entry='flows/questions.py:select',params='flows/select_a.json'))[0] == 403
            status, session = request(server,workspace,'dev-session-create',dict(request_id='chat',provider_id='codex'))
            assert status == 200
            sid = session['id']
            status, turn = request(server,workspace,'dev-turn-start',dict(session_id=sid,submission_id='first',expected_revision=0,text='hang fixture'))
            assert status == 202
            wait_running(server.development, turn['id'])
            # Existing ordinary-worker and development-reader pressure must not
            # consume the runtime control worker or the chat cancellation path.
            assert server.workers.acquire(blocking=False)
            assert not server.workers.acquire(blocking=False)
            try:
                seq = server.development.store.show(workspace,sid)['sequence']
                query=f'?session_id={sid}&after_sequence={seq}&wait_ms=1500'
                with ThreadPoolExecutor(max_workers=2) as pool:
                    readers=[pool.submit(request,server,workspace,'dev-events',query=query) for _ in range(2)]
                    time.sleep(.15)
                    status,snapshot = _request(server,prefix+'run-snapshot?run_id='+value['run_id'])
                    assert status == 200 and snapshot['run_id'] == value['run_id']
                    assert server.snapshots
                    started=time.monotonic()
                    status,stop = _request(server,prefix+'run-cancel',dict(run_id=value['run_id'],expected_attempt=1))
                    assert status == 200 and time.monotonic()-started < 3
                    assert stop['state']['cancel_requested_at']
                    assert server.development.store.turn(turn['id'])['stop_at'] is None
                    assert all(item.result()[0] == 200 for item in readers)
                status,stopped=request(server,workspace,'dev-cancel',dict(session_id=sid,turn_id=turn['id'],request_id='stop-chat'))
                assert status == 200 and stopped['state']=='stopping'
            finally: server.workers.release()
            assert wait_turn(server.development,turn['id'])['cleanup'] == 'complete'
        done=wait(root,value['run_id'],timeout=20)
        assert done['status']=='cancelled' and done['state']['execution_stopped']
        assert sum(o['result'] is not None for o in done['outputs']) == 1
    finally:
        if runs.show(root,value['run_id'])['status'] in runs.ACTIVE:
            runs.cancel(root,value['run_id']);wait(root,value['run_id'],timeout=20)


def test_combined_browser_keeps_development_and_runtime_views(root, tmp_path, monkeypatch):
    from playwright.sync_api import sync_playwright,expect
    config, registry, workspace, domain, envelope = combined_configuration(tmp_path, root, monkeypatch)
    value = _slow(root)
    try:
        _first_committed(root,value)
        with _server(registry=registry,enable_runs=True,development_config=config) as server, sync_playwright() as engine:
            browser=engine.chromium.launch(executable_path=os.environ['FORGE_CHROMIUM'],headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
            page=browser.new_page(viewport=dict(width=1440,height=1000));page.set_default_timeout(60000)
            errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
            page.goto(server.origin+'/#token='+server.token)
            page.get_by_role('button',name='开发',exact=True).click()
            assert not server.development.store.list(workspace)['sessions']
            page.get_by_label('开发要求').fill('normal combined fixture')
            page.get_by_role('button',name='发送',exact=True).click()
            page.wait_for_url('**/*development_session*')
            page.get_by_text('本轮已完成',exact=True).wait_for()
            assert len(server.development.store.list(workspace)['sessions'])==1
            shots=Path(os.environ.get('FORGE_COMBINED_SCREENSHOTS',str(tmp_path/'screenshots')));shots.mkdir(parents=True,exist_ok=True)
            for width in (1440,390,320):
                page.set_viewport_size(dict(width=width,height=1000))
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
                page.screenshot(path=str(shots/f'combined-development-{width}.png'),full_page=True)
            page.locator('.function-nav').get_by_role('button',name='运行监控',exact=True).click()
            assert page.evaluate('ForgeDevelopment.activeCount()') == 0
            page.locator('.wb-run-record[data-run=\"'+value['run_id']+'\"]').click()
            expect(page.locator('.wb-run-bar').get_by_label('运行记录')).to_have_value(value['run_id'])
            for width in (1440,390,320):
                page.set_viewport_size(dict(width=width,height=1000))
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
                page.screenshot(path=str(shots/f'combined-runtime-{width}.png'),full_page=True)
            page.get_by_role('button',name='停止',exact=True).focus();page.keyboard.press('Enter')
            expect(page.locator('.wb-run-bar .wb-tag')).to_have_text('已取消',timeout=60000)
            page.get_by_role('button',name='开发',exact=True).click()
            page.get_by_text('本轮已完成',exact=True).wait_for()
            assert len(server.development.store.list(workspace)['sessions'])==1
            assert not errors,errors
            browser.close()
        assert wait(root,value['run_id'],timeout=20)['state']['execution_stopped']
    finally:
        if runs.show(root,value['run_id'])['status'] in runs.ACTIVE:
            runs.cancel(root,value['run_id']);wait(root,value['run_id'],timeout=20)
