"""Public use cases: headless operation, exact identities and transport parity."""
import json
import os
from contextlib import ExitStack, contextmanager
from pathlib import Path
import socket
import threading
import urllib.error
import urllib.request

from click.testing import CliRunner
import pytest

from demiflow.cli import main
from demiflow.execution.run_journal import RunJournal
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.workbench.api import WorkspaceServer
from demiflow.workbench.api import HTTP_CONTRACT
from demiflow.lance.refs import DatasetRef
from test_forge_runs import base, root, prepared, execute


def test_public_source_parameters_and_cli_without_http(root):
    source = ws.source_text(root, 'flows/questions.py')
    runner = CliRunner()
    value = runner.invoke(main, ['workbench', 'source', str(root), 'flows/questions.py',
                                '--expected-sha256', source['sha256'], '--json'])
    assert value.exit_code == 0, value.output
    assert json.loads(value.output) == source
    assert ws.read_parameters(root, 'flows/select_a.json')['arguments']
    changes = runner.invoke(main, ['workbench', 'changes', str(root), '--json'])
    assert json.loads(changes.output)['workspace_id'] == ws.load_workspace(root)[1]['id']
    (root / 'flows/questions.py').write_text(source['text'] + '\n# saved edit\n')
    stale = runner.invoke(main, ['workbench', 'source', str(root), 'flows/questions.py',
                                '--expected-sha256', source['sha256']])
    assert stale.exit_code == 1 and json.loads(stale.output)['code'] == 'conflict'
    with pytest.raises(ForgeError):
        ws.read_parameters(root, '../workspace.json')


def next_attempt(root, value, *, running=False):
    journal = runs.journal(root, value['run_id'])
    old = journal.execution_status()
    journal.transition({old['status']}, 'failed', recoverable=True)
    old = journal.execution_status()
    target, observed = runs._load(root, value['run_id'], detail='identity')
    manifest = observed['manifest']
    new = journal.begin_attempt(expected_attempt=old['attempt'], expected_revision=old['revision'],
        nodes=[(item['action_id'], item['node_id'], item['occurrence']) for item in manifest['observation_instances']],
        actions=runs._fact_actions(runs._report(target, manifest), manifest['outputs']))
    if running:
        journal.transition({'prepared'}, 'running')
    return new['attempt']


def test_cancel_is_fenced_inside_transaction(root, monkeypatch):
    value = prepared(root)
    runs.journal(root,value['run_id']).transition({'prepared'},'running')
    original = RunJournal.transition

    def racing_transition(self, expected, status, **details):
        if status == 'stopping':
            next_attempt(root, value, running=True)
        return original(self, expected, status, **details)

    monkeypatch.setattr(RunJournal, 'transition', racing_transition)
    with pytest.raises(ForgeError) as caught:
        runs.cancel(root, value['run_id'], expected_attempt=1)
    assert caught.value.code == 'conflict'
    current = runs.show(root, value['run_id'])
    assert current['attempt'] == 2 and current['status'] == 'running'
    assert 'cancel_requested_at' not in current['state']


def test_null_bool_and_stale_attempts_cannot_control_modern_run(root):
    value = prepared(root)
    for expected, code in [(None, 'conflict'), (True, 'invalid_request'), (2, 'conflict')]:
        for action in [lambda: runs.cancel(root, value['run_id'], expected_attempt=expected),
                       lambda: runs.reconcile(root, value['run_id'], expected_attempt=expected),
                       lambda: runs.start(root, value['run_id'], expected=value['digest'], expected_attempt=expected)]:
            with pytest.raises(ForgeError) as caught:
                action()
            assert caught.value.code == code
    assert runs.show(root, value['run_id'])['status'] == 'prepared'
    # This conflict originates inside the real isolated workspace worker.
    with pytest.raises(ForgeError) as caught:
        runs.resume(root, value['run_id'], expected=value['digest'], expected_attempt=2)
    assert caught.value.code == 'conflict'


def test_attach_preserves_exact_result_and_rejects_catalog_identity_changes(root):
    result = execute(root, prepared(root))
    assert result['status'] == 'succeeded'
    output = result['outputs'][0]
    assert output['result_digest'] == content_digest(output['result']['ref'])
    next_attempt(root, result)
    runner = CliRunner()
    response = runner.invoke(main, ['workbench', 'run', 'attach', str(root), result['run_id'],
        output['name'], 'reviewed', '--attempt', '1', '--expected-result', output['result_digest'], '--json'])
    assert response.exit_code == 0, response.output
    assert json.loads(response.output)['table']['ref'] == output['result']['ref']
    assert project_ops.describe_table(root, 'reviewed')['reference']['ref'] == output['result']['ref']
    with pytest.raises(ForgeError) as caught:
        project_ops.attach_result(root, result['run_id'], output['name'], 'wrong', attempt=1, expected_result='0' * 64)
    assert caught.value.code == 'conflict' and not (root / 'tables/wrong.json').exists()
    with pytest.raises(ForgeError) as caught:
        project_ops.attach_result(root, result['run_id'], output['name'], 'reviewed', attempt=1,
                           expected_result=output['result_digest'])
    assert caught.value.code == 'conflict'
    ref = DatasetRef.from_dict(output['result']['ref'])
    altered = DatasetRef.from_dict({**ref.to_dict(), 'schema_name': 'another_schema'})
    with pytest.raises(ForgeError) as caught:
        project_ops.attach_table(root, 'catalog_changed', relative_uri=ref.relative_uri, version=ref.lance_version,
            location_id=ref.store_id, dataset_id=ref.dataset_id, schema_name=ref.schema_name,
            schema_version=ref.schema_version, expected_ref=altered)
    assert caught.value.code == 'conflict' and not (root / 'tables/catalog_changed.json').exists()


@contextmanager
def service(root, **options):
    app = WorkspaceServer(root, **options)
    thread = threading.Thread(target=app.serve_forever, daemon=True)
    thread.start()
    try:
        yield app
    finally:
        app.shutdown()
        thread.join(timeout=10)
        app.server_close()


def request(app, endpoint, body=None, *, headers=None):
    payload = json.dumps(body).encode() if isinstance(body, dict) else body
    req = urllib.request.Request(app.origin + endpoint, data=payload,
        headers={'X-Forge-Token': app.token, **(headers or {})})
    try:
        response = urllib.request.urlopen(req, timeout=65)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        raw = response.read()
        return response.status, json.loads(raw) if 'application/json' in response.headers.get('Content-Type', '') else raw


def test_page_asset_served_after_short_connection_burst(root, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from time import monotonic

    with service(root) as app, ExitStack() as connections:
        # Hold ten real connections before sending their HTTP request lines.
        held = [connections.enter_context(socket.create_connection(app.server_address, timeout=4))
                for _ in range(10)]
        started = monotonic()
        assert request(app, '/operator-browser.js') == (503, {
            'code': 'busy', 'error': 'Request capacity exceeded', 'operation_started': False})
        assert monotonic() - started < 4

        accepted = threading.Event()
        process_request = app.process_request
        def notify_accept(client, address):
            accepted.set()
            return process_request(client, address)
        monkeypatch.setattr(app, 'process_request', notify_accept)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(request, app, '/operator-browser.js')
            assert accepted.wait(4)
            held[0].close()
            status, script = pending.result(timeout=4)
        assert status == 200 and b'DemiFlowOperators' in script


def test_api_only_token_and_closed_protocol(root, tmp_path):
    token = tmp_path / 'access.token'
    token.write_text('a' * 43 + '\n')
    token.chmod(0o600)
    with service(root, api_only=True, token_file=token) as app:
        with pytest.raises(OSError):
            WorkspaceServer(root, port=app.server_port)
        assert app.token == 'a' * 43
        assert request(app, '/')[0] == 404
        code, value = request(app, '/api/capabilities')
        assert code == 200 and value['contract'] == HTTP_CONTRACT and not value['web_enabled']
        assert value['permissions'] == dict(read=True, inspect=True, audit_check=True, runs=False,
                                           project_manage=False, project_check=True,run_submit=False,run_control=False,attach=False)
        assert request(app, '/api/project-create', {'root':str(tmp_path/'forbidden'),'name':'forbidden'})[0] == 422
        assert not (tmp_path/'forbidden').exists()
        assert request(app, '/api/context')[1]['workspace_id'] == ws.load_workspace(root)[1]['id']
        code, error = request(app, '/api/capabilities', headers={'X-Forge-Token': 'wrong'})
        assert code == 403 and error['code'] == 'unauthorized'
        assert request(app, '/api/context', headers={'Origin': 'https://foreign.example'})[0] == 403
        for path in ['/api/context?extra=1', '/api/run?run_id=x&run_id=y', '/api/source',
                     '/api/parameters?path=flows/select_a.json&extra=1', '/api/run?run_id=x&report=maybe',
                     '/api/sample?alias=candidates&limit=-1', '/api/media?alias=candidates&row_id=1']:
            code, value = request(app, path)
            assert code == 400 and value['code'] == 'invalid_request', (path, code, value)
        assert request(app, '/api/run-cancel', dict(run_id='x', expected_attempt=1))[0] == 403
        held = [app.requests.acquire(timeout=3) for _ in range(10)]
        try:
            assert all(held)
            assert request(app, '/api/context') == (503, {'code': 'busy', 'error': 'Request capacity exceeded', 'operation_started': False})
        finally:
            for acquired in held:
                if acquired:
                    app.requests.release()
    # A new server uses the same protected credential, not an implicit rotation.
    with service(root, token_file=token) as restarted:
        assert restarted.token == 'a' * 43
    token.chmod(0o644)
    with pytest.raises(ForgeError):
        WorkspaceServer(root, token_file=token)
    token.chmod(0o600)
    if os.geteuid() == 0:
        try:
            os.chown(token, os.getuid() + 1, -1)
            with pytest.raises(ForgeError):
                WorkspaceServer(root, token_file=token)
        finally:
            os.chown(token, os.getuid(), -1)
    link = tmp_path / 'link.token'
    link.symlink_to(token)
    with pytest.raises(OSError):
        WorkspaceServer(root, token_file=link)
    token.write_text('x' * 257)
    with pytest.raises(ForgeError):
        WorkspaceServer(root, token_file=token)


def test_http_requires_attempt_and_does_not_replay_writes(root):
    value = prepared(root)
    with service(root, enable_runs=True) as app:
        for body, code in [(dict(run_id=value['run_id']), 400),
                           (dict(run_id=value['run_id'], expected_attempt=None), 409),
                           (dict(run_id=value['run_id'], expected_attempt=True), 400),
                           (dict(run_id=value['run_id'], expected_attempt=2), 409)]:
            assert request(app, '/api/run-cancel', body)[0] == code
        assert request(app, '/api/inspect', b'{"entry":"a","entry":"b","params":"c"}')[0] == 400
        assert request(app, '/api/inspect', b'x' * 65537)[0] == 413
        for number in ['NaN', 'Infinity', '1e400', '-1e400']:
            payload = ('{"entry":"flows/questions.py:select","params":"flows/select_a.json",'
                       '"limits":{"timeout_s":' + number + '}}').encode()
            code, body = request(app, '/api/run-prepare', payload)
            assert code == 400 and body['code'] == 'invalid_request'
        # Resumption conflict travels through a real child process to HTTP 409.
        code, body = request(app, '/api/run-resume', dict(run_id=value['run_id'],
                            expected=value['digest'], expected_attempt=2))
        assert code == 409 and body['code'] == 'conflict'
        assert runs.show(root, value['run_id'])['status'] == 'prepared'


def test_direct_nonconforming_edits_cannot_bypass_public_admission(root):
    value = prepared(root)
    source = root / 'flows/questions.py'
    source.write_text(source.read_text() + '\nopen("unauthorized-side-effect", "w").write("bad")\n')
    with service(root, enable_runs=True) as app:
        code, body = request(app, '/api/run-start', dict(run_id=value['run_id'],
                            expected=value['digest'], expected_attempt=1))
        assert code == 409 and body['code'] == 'conflict'
        code, body = request(app, '/api/run-prepare', dict(entry='flows/questions.py:select', params='flows/select_a.json'))
        assert code == 422 and body['code'] == 'operation_rejected'
    cli = CliRunner().invoke(main, ['workbench', 'run', 'prepare', str(root),
        'flows/questions.py:select', '--params', 'flows/select_a.json'])
    assert cli.exit_code == 1 and json.loads(cli.output)['code'] == 'operation_rejected'
    assert runs.show(root, value['run_id'])['status'] == 'prepared'
    assert not (root / 'unauthorized-side-effect').exists()
    assert all(not Path(output['uri']).exists() for output in value['outputs'])
