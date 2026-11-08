"""Independent observation, content expiry, cache recovery, and API parity."""
import json
import subprocess
import sys
import time

from click.testing import CliRunner
import pytest

from demiflow.cli import main
from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.projects import watch
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from test_forge_public_api import service, request


@pytest.fixture
def root(tmp_path):
    root = tmp_path / 'workspace'
    ws.init_workspace(root)
    for directory in ('flows', 'operators', 'tables', 'data'):
        (root / directory).mkdir()
    ws.add_location(root, 'local', root / 'data', writable=True)
    (root / 'operators/a.py').write_text('# initial')
    ws.begin_change_scope(root, writes=[], expected=content_digest(ws.load_workspace(root)[1]))
    return root


def test_scope_http_cli_and_corrupt_cache_recovery(root):
    policy = ws.audit_status(root)['policy_digest']
    with service(root, api_only=True) as app:
        code, before = request(app, '/api/audit')
        assert code == 200 and before['observations'][0]['freshness'] == 'unknown'
        code, result = request(app, '/api/audit-check', dict(kind='scope', expected_policy=policy))
        assert code == 200 and result['result']['status'] == 'within_scope'
        assert request(app, '/api/audit-check', dict(kind='scope', expected_policy=policy, writes=[]))[0] == 400
        (root / 'operators/a.py').write_text('# changed')
        (root / '.forge/audit/status.json').write_text('{broken')
        code, result = request(app, '/api/audit-check', dict(kind='scope', expected_policy=policy))
        assert code == 200 and result['result']['status'] == 'violated'
        code, observed = request(app, '/api/audit')
        cli = CliRunner().invoke(main, ['workbench', 'audit', 'status', str(root)])
        assert cli.exit_code == 0
        cli_value = json.loads(cli.output)
        assert cli_value.pop('observed_at') >= observed.pop('observed_at')
        assert cli_value == observed
        assert request(app, '/api/audit-check', dict(kind='scope', expected_policy='a'*64))[0] == 409


def test_expiry_and_heartbeat_cannot_refresh_content(root):
    policy = ws.audit_status(root)['policy_digest']
    completed = ws.audit_check(root, kind='scope', expected_policy=policy, full_interval=1)
    observed = ws.audit_observe(root, expected_policy=policy, session='a'*32)
    row = ws.audit_status(root)['observations'][0]
    assert row['verified_at'] == completed['verified_at'] and row['freshness'] == 'pending'
    path = root / '.forge/audit/status.json'
    cache = ws.read_control(path)
    cache['observations']['scope']['pending'] = False
    cache['observations']['scope']['verified_at'] -= 10
    cache['observations']['scope']['due_at'] -= 10
    path.write_text(json.dumps(cache))
    assert ws.audit_status(root)['observations'][0]['freshness'] == 'overdue'
    with ws.audit_lease(root, 'check'):
        with pytest.raises(ForgeError) as caught:
            ws.audit_check(root, kind='scope', expected_policy=policy)
        assert caught.value.code == 'busy'
    with ws.audit_lease(root, 'watch'):
        with pytest.raises(ForgeError):
            with ws.audit_lease(root, 'watch'):
                pass


def test_real_watch_without_ui_detects_saved_change_and_stops(root, tmp_path):
    registry = tmp_path / 'registry.json'
    console.add(registry, root)
    output = (tmp_path / 'watch.log').open('w')
    process = subprocess.Popen([sys.executable, '-B', '-m', 'demiflow.cli', 'workbench', 'console',
        '--registry', str(registry), 'watch', '--interval', '1', '--full-interval', '2'], stdout=output, stderr=output)
    def wait_for(status):
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            assert process.poll() is None, (tmp_path / 'watch.log').read_text()
            value = ws.audit_status(root)
            if value['observations'][0]['result']['status'] == status:
                return value
            time.sleep(.15)
        pytest.fail('Watcher did not observe ' + status + ': ' + (tmp_path / 'watch.log').read_text())
    try:
        first = wait_for('within_scope')
        (root / 'operators/a.py').write_text('# unexpected')
        second = wait_for('violated')
        assert second['observations'][0]['verified_at'] > first['observations'][0]['verified_at']
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        finally:
            if process.poll() is None:
                process.kill(); process.wait()
            output.close()
    with ws.audit_lease(root, 'watch'):
        pass


def test_round_robin_does_not_starve_other_targets(monkeypatch):
    value = dict(format=ws.FORMAT_V3, audit=dict(reports=['b'*64, 'c'*64], change_scope=None))
    rows = [dict(kind='report', identity=d, freshness='pending') for d in value['audit']['reports']]
    monkeypatch.setattr(ws, 'load_workspace', lambda root: (root, value, None))
    monkeypatch.setattr(ws, 'audit_observe', lambda *a, **kw: dict(hint='unchanged'))
    monkeypatch.setattr(ws, 'audit_status', lambda root: dict(observations=rows))
    seen = []
    monkeypatch.setattr(ws, 'audit_check', lambda root, **kw: seen.append(kw['report_digest']))
    state = {}
    for _ in range(6):
        watch._step('fixture', state, session='a'*32, interval=30, full_interval=300)
    assert seen == ['b'*64, 'c'*64]*3


@pytest.mark.parametrize('damage', ['status_type', 'nonfinite_time', 'change_type'])
def test_invalid_cached_observations_become_unknown(root, damage):
    policy = ws.audit_status(root)['policy_digest']
    def populate(cache):
        now = time.time()
        row = dict(kind='scope', identity='scope', result=dict(status='within_scope', complete=True),
                   verified_at=now, due_at=now+300, pending=False)
        cache['observations']['scope'] = row
    ws._audit_publish(root, policy, populate)
    path = root / '.forge/audit/status.json'
    cached = ws.read_control(path)
    row = cached['observations']['scope']
    if damage == 'status_type':
        row['result']['status'] = []
    elif damage == 'nonfinite_time':
        row['due_at'] = float('nan')
    else:
        row['result']['changes'] = [None]
    path.write_text(json.dumps(cached))
    assert ws.audit_status(root)['observations'][0]['freshness'] == 'unknown'
