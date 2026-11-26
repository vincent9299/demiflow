"""DeepSeek's review session is a fresh native agent with no registered tools."""

import http.server
import json
from pathlib import Path
import shutil
import subprocess
import sys
import threading

import pytest

from demiflow.forge import review_deepseek as backend
from demiflow.forge.schema import ForgeError


def _model_response(write_to=None):
    if write_to is None:
        start = dict(type='text', text='')
        delta = dict(type='text_delta', text='{"reviewed":true}')
        stop = 'end_turn'
    else:
        start = dict(type='tool_use', id='case-1', name='write', input={})
        args = dict(file_path=str(write_to), content='unexpected')
        delta = dict(type='input_json_delta', partial_json=json.dumps(args))
        stop = 'tool_use'
    events = (
        dict(type='message_start', message=dict(usage=dict(input_tokens=10))),
        dict(type='content_block_start', index=0, content_block=start),
        dict(type='content_block_delta', index=0, delta=delta),
        dict(type='content_block_stop', index=0),
        dict(type='message_delta', delta=dict(stop_reason=stop), usage=dict(output_tokens=10)),
        dict(type='message_stop'),
    )
    return ''.join(f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events).encode()


def test_real_headless_session_has_no_file_or_shell_tools(tmp_path, monkeypatch):
    cli = shutil.which('dsh')
    if not cli:
        pytest.skip('No installed DeepSeek CLI for the headless integration')
    executable = Path(cli).resolve()
    node = Path(cli).with_name('node')
    if executable.name != 'bin.js' or not node.is_file():
        pytest.skip('DeepSeek headless npm installation required')
    package = json.loads((executable.parents[1] / 'package.json').read_bytes())
    route = []
    marker = tmp_path / 'must-not-be-created'

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            route.append(body)
            # Simulate a provider emitting a write even when no tools were offered.
            payload = _model_response(marker if len(route) == 2 else None)
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    monkeypatch.setenv('FORGE_TEST_FAKE_KEY', 'synthetic-test-key')
    with http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
        runner = threading.Thread(target=server.serve_forever, daemon=True)
        runner.start()
        config = dict(executable=cli, node=str(node), version=package['version'], model='deepseek-v4-flash',
                      credential_env='FORGE_TEST_FAKE_KEY', base_url=f'http://127.0.0.1:{server.server_port}')
        try:
            first = backend.run_reviewer('Review this example', config=config, timeout=20)
            assert first['report'] == {'reviewed': True}
            with pytest.raises(ForgeError, match='unavailable tool|turn failed'):
                backend.run_reviewer('Second example', config=config, timeout=20)
            last = backend.run_reviewer('Third example', config=config, timeout=20)
            assert last['session_id'] != first['session_id']
            assert last['model'] == config['model']
            assert len(route) >= 3
            assert all(not message.get('tools') for message in route)
            assert not marker.exists()
        finally:
            server.shutdown()
            runner.join()


def test_cancel_reaps_process_before_return(tmp_path):
    def cancel():
        if (tmp_path / 'started').is_file():
            raise InterruptedError

    command = [sys.executable, '-c', "from pathlib import Path; import os,time; Path('started').write_text(str(os.getpid())); time.sleep(30)"]
    proc = subprocess.Popen(command, cwd=tmp_path, env={}, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    with pytest.raises(InterruptedError):
        backend.collect_review(proc, 'input', cwd=tmp_path, model='test', timeout=10, check_cancel=cancel)
    assert not Path('/proc', (tmp_path / 'started').read_text()).exists()


def test_partial_or_malformed_cli_cannot_publish_report(tmp_path):
    session = dict(type='session', sessionId='session-11111111-2222-3333-4444-555555555555', cwd=str(tmp_path))
    complete = dict(type='status', phase='turn_end', reason=dict(kind='completed'))
    final = dict(type='final', text='{}')
    for events in ([final], [session, final], [session, complete], [session, complete, dict(final, text='invalid')]):
        with pytest.raises(ForgeError) as error:
            backend._result(0, '\n'.join(json.dumps(e) for e in events).encode(), 'model', tmp_path)
        assert error.value.code == 'inspection_incomplete'


def test_confined_profile_uses_only_fixed_model_channel(tmp_path):
    cli = tmp_path / 'deepseek/lib/bin.js'
    cli.parent.mkdir(parents=True)
    cli.touch()
    metadata = cli.parents[1] / 'package.json'
    version = '0.2.1-rc.2'
    metadata.write_text(json.dumps(dict(name='@deepseek-ai/dsh', version=version)))
    proxy = tmp_path / 'model-proxy.cjs'
    proxy.touch()
    private = tmp_path / 'review'
    private.mkdir(mode=0o700)
    config = dict(executable=str(cli), node=sys.executable, model='test-model', version=version,
                  model_provider='deepseek-account', _boundary={'token': 'a' * 64}, _request_seconds=2700)
    for supplied in (dict(config, credential_env='secret'), dict(config, native_credentials='/private/creds'),
                     dict(config, base_url='https://upstream.invalid'), dict(config, inherit_proxy=True)):
        with pytest.raises(ForgeError, match='fixed model channel'):
            backend.prepare_launch(supplied, private, model_channel=proxy)
    _, env = backend.prepare_launch(config, private, model_channel=proxy)
    profile = private / 'profiles' / backend.PROFILE
    assert json.loads((profile / 'package.json').read_text())['dsh']['profile']['bundles'] == []
    plugins = json.loads((profile / 'cordis.patch.yml').read_text())[0]['insert']
    channel = next(r['config'] for r in plugins if r['id'] == 'review-model')
    assert channel['requestSeconds'] == 2700 and channel['reasoningEffort'] == 'high'
    assert {r['name'] for r in plugins} == {str(proxy)} | {
        '@deepseek-ai/dsh-' + name for name in (
            'llm', 'session', 'session-projection', 'agent', 'system-prompt', 'tools',
            'agent-loop', 'agent-default-model', 'headless')}
    assert not (private / '.credentials.yaml').exists()
    assert not any(name in env for name in ('DEEPSEEK_API_KEY', 'HTTPS_PROXY', 'NODE_OPTIONS'))
    metadata.write_text(json.dumps(dict(name='@deepseek-ai/dsh', version='unqualified')))
    with pytest.raises(ForgeError, match='not qualified'):
        backend.prepare_launch(dict(config, version='unqualified'), private, model_channel=proxy)


@pytest.mark.parametrize('route', ['deepseek-official', 'deepseek-account'])
@pytest.mark.parametrize('seconds', [None, 2700])
def test_review_budget_reaches_native_idle_timeout(tmp_path, monkeypatch, route, seconds):
    cli = tmp_path/'deepseek/lib/bin.js'
    cli.parent.mkdir(parents=True)
    cli.touch()
    (cli.parents[1]/'package.json').write_text(json.dumps(dict(name='@deepseek-ai/dsh', version='0.2.1-rc.2')))
    credential = tmp_path/'credentials.yaml'
    credential.write_text('version: 1\n')
    monkeypatch.setenv('FORGE_TEST_FAKE_KEY', 'synthetic-test-key')
    config = dict(executable=str(cli), node=sys.executable, model='test-model', version='0.2.1-rc.2',
                  model_provider=route, **(dict(native_credentials=str(credential)) if route == 'deepseek-account'
                    else dict(credential_env='FORGE_TEST_FAKE_KEY')))
    if seconds is not None:
        config['_request_seconds'] = seconds
    home = tmp_path/'review'
    home.mkdir()
    backend.prepare_launch(config, home)
    plugins = json.loads((home/'profiles'/backend.PROFILE/'cordis.patch.yml').read_text())[0]['insert']
    plugin = next(p['config'] for p in plugins if p['id'] in ('llm-deepseek-api-key', 'llm-deepseek-account'))
    assert plugin['streamIdleTimeoutMs'] == (seconds or 1800) * 1000
    assert plugin['reasoningEffort'] == 'high'


def test_installed_restricted_author_and_reviewer_use_the_host_channel(tmp_path, monkeypatch):
    import time
    from contextlib import closing
    from click.testing import CliRunner
    from demiflow.cli import main
    from demiflow.forge.bridge.model_channel import local_channel
    from demiflow.forge.project import initialize_project

    cli = shutil.which('dsh')
    if not cli or not Path(cli).with_name('node').is_file():
        pytest.skip('Installed DeepSeek headless npm and Node required')
    calls = []

    class Model(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            calls.append(bool(body.get('tools')))
            payload = _model_response()
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    root = tmp_path / 'project'
    initialize_project(root, name='fixture')
    monkeypatch.setenv('FORGE_TEST_MODEL_KEY', 'fixture')
    node = Path(cli).with_name('node')
    with http.server.ThreadingHTTPServer(('127.0.0.1', 0), Model) as server:
        runner = threading.Thread(target=server.serve_forever, daemon=True)
        runner.start()
        config = dict(executable=str(Path(cli).resolve()), node=str(node), version='0.2.1-rc.2',
                      model='deepseek-v4-flash', credential_env='FORGE_TEST_MODEL_KEY',
                      base_url=f'http://127.0.0.1:{server.server_port}')
        host = tmp_path / 'providers.json'
        host.write_text(json.dumps({'providers': {'deepseek': config}}))
        try:
            result = CliRunner().invoke(main, ['forge', 'develop', '--agent', 'deepseek', '--goal', 'Read this project',
                '--root', str(root), '--state-dir', str(tmp_path / 'state'), '--model-config', str(host),
                '--no-review', '--timeout', '60', '--json'])
            assert result.exit_code == 0, result.output
            with closing(local_channel('deepseek', config, tmp_path, 'review', home=tmp_path / 'model',
                                       deadline=time.time() + 60, request_seconds=45)) as channel:
                outcome = backend.run_reviewer('Return one JSON object.', config=config, timeout=45,
                                               restricted=True, channel=channel)
            assert outcome['report'] == {'reviewed': True}
            assert calls[0] and not calls[-1]
        finally:
            server.shutdown()
            runner.join(5)
