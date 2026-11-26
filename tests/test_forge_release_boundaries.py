"""Behavioral release guards for Forge's project dependency and interpreter boundaries."""
import json
from pathlib import Path
import shutil
import sys
import threading
from contextlib import contextmanager

import pytest
from click.testing import CliRunner

from demiflow.cli import main
from demiflow.forge import dependencies as deps
from demiflow.forge.project import initialize_project, register_flow
from demiflow.forge.schema import ForgeError


def project(tmp_path, *names):
    root = tmp_path / 'project'
    initialize_project(root, name='Example')
    (root / 'pyproject.toml').write_text('[tool.demiflow.packages]\n' + ''.join(
        f'{name} = {{source="vendored", path="packages/{name}"}}\n' for name in names))
    return root


def package(path):
    path.mkdir(parents=True)
    (path / '__init__.py').write_text('from .model import VALUE\n')
    (path / 'model.py').write_text('VALUE = 42\n')
    return path


def test_authorized_vendor_survives_two_public_terminal_turns(tmp_path, monkeypatch):
    root = project(tmp_path)
    (root / 'flows').mkdir(exist_ok=True)
    (root / 'flows/main.py').write_text("from demiflow.data import from_items\ndef run():\n    return from_items([{'id': 1}]).materialize()\n")
    register_flow(root, 'flows/main.py:run')
    with (root / 'pyproject.toml').open('a') as config:
        config.write('samplelib = {source="vendored", path="packages/samplelib"}\n')
    source = package(tmp_path / 'upstream' / 'samplelib')
    # The native harness fixture uses only stdin and its documented completion frames.
    author = tmp_path / 'author'
    author.write_text(f'''#!{sys.executable}
import json, os, subprocess, sys
from pathlib import Path
goal = sys.stdin.read()
assert "FORGE_TEST_HOST_ONLY" not in os.environ
if "acquire" in goal:
    subprocess.run(["demiflow", "forge", "dep", "vendor", "samplelib", {str(source)!r}, "--root", "."], check=True, stdout=subprocess.DEVNULL)
check = subprocess.run(["python", "-c", "from samplelib import VALUE; assert VALUE == 42"])
assert check.returncode == 0
for frame in [{{"type":"thread.started","thread_id":"fixture-session"}},{{"type":"turn.started"}},{{"type":"item.completed","item":{{"type":"agent_message","text":"ok"}}}},{{"type":"turn.completed"}}]:
    print(json.dumps(frame),flush=True)
''')
    author.chmod(0o755)
    monkeypatch.setenv('FORGE_TEST_HOST_ONLY', 'fixture')
    runner = CliRunner()
    for goal in ('acquire the package', 'read the existing package'):
        result = runner.invoke(main, ['forge', 'develop', '--root', str(root), '--state-dir', str(tmp_path / goal),
            '--executable', str(author), '--python', sys.executable, '--read-root', str(source),
            '--goal', goal, '--no-review', '--json'])
        assert result.exit_code == 0, result.output
        answer = json.loads(result.output)
        assert answer['author']['completed'] and answer['author']['cleanup'] == 'complete', answer
        assert (root / 'packages/samplelib/model.py').read_text() == 'VALUE = 42\n'


def test_default_terminal_requires_the_restricted_backend(tmp_path, monkeypatch):
    from demiflow.forge import development, sandbox
    root = project(tmp_path)
    monkeypatch.setattr(sandbox, 'backend_readiness', lambda: {'available': False})
    def dispatch(*args, **kwargs):
        pytest.fail('An unavailable isolation backend must stop before author dispatch')
    monkeypatch.setattr(development, 'run_development', dispatch)
    result = CliRunner().invoke(main, ['forge', 'develop', '--root', str(root), '--state-dir', str(tmp_path / 'state'),
        '--executable', sys.executable, '--python', sys.executable, '--goal', 'check', '--no-review', '--json'])
    assert result.exit_code != 0 and 'sandbox' in result.output.lower(), result.output


def test_restricted_terminal_network_is_off_without_a_model_channel(tmp_path, monkeypatch):
    from demiflow.forge import development, sandbox
    root = project(tmp_path)
    seen = []
    original = sandbox.build_development_sandbox_spec
    def build(*args, **kwargs):
        spec = original(*args, **kwargs)
        seen.append(spec.direct_network)
        return spec
    monkeypatch.setattr(sandbox, 'backend_readiness', lambda: {'available': True})
    monkeypatch.setattr(sandbox, 'build_development_sandbox_spec', build)
    monkeypatch.setattr(development, 'run_development', lambda *a, **kw: {'status':'checked'})
    result = CliRunner().invoke(main, ['forge','develop','--root',str(root),'--state-dir',str(tmp_path/'state'),
        '--executable',sys.executable,'--python',sys.executable,'--goal','check','--no-review','--json'])
    assert result.exit_code == 0, result.output
    assert seen == [False]


def test_partial_deletion_keeps_all_original_bytes(tmp_path, monkeypatch):
    root = project(tmp_path, 'samplelib')
    managed = package(root / 'packages/samplelib')
    config = root / 'pyproject.toml'
    before = config.read_bytes(), config.stat().st_ino
    files = {f.name:f.read_bytes() for f in managed.iterdir()}
    rmtree = shutil.rmtree
    def partial(path, *args, **kwargs):
        if Path(path).parent == managed.parent:
            (Path(path) / 'model.py').unlink()
            raise OSError('mid-cleanup IO failure')
        return rmtree(path, *args, **kwargs)
    monkeypatch.setattr(deps.shutil, 'rmtree', partial)
    with pytest.raises(OSError):
        deps.remove(root, 'samplelib')
    assert (config.read_bytes(), config.stat().st_ino) == before
    assert {f.name:f.read_bytes() for f in managed.iterdir()} == files


@pytest.mark.parametrize('replace_file', [True, False])
def test_remove_preserves_concurrent_editor_save(tmp_path, monkeypatch, replace_file):
    root = project(tmp_path, 'samplelib')
    managed = package(root / 'packages/samplelib')
    config = root / 'pyproject.toml'
    saved = config.read_text() + '\n[tool.other]\nkeep = "user edit"\n'
    rename = os.replace
    def editor(src, dst, *args, **kwargs):
        value = rename(src, dst, *args, **kwargs)
        if Path(src) == managed:
            if replace_file:
                edit = config.with_suffix('.edited')
                edit.write_text(saved)
                rename(edit, config)
            else:
                config.write_text(saved)
        return value
    monkeypatch.setattr(deps.os, 'replace', editor)
    with pytest.raises(ForgeError, match='changed'):
        deps.remove(root, 'samplelib')
    assert config.read_text() == saved
    assert (managed / 'model.py').read_text() == 'VALUE = 42\n'


def test_project_budget_is_rechecked_at_publication(tmp_path, monkeypatch):
    root = project(tmp_path, 'alpha', 'bravo')
    first, second = (package(tmp_path / name) for name in ('alpha', 'bravo'))
    monkeypatch.setattr(deps, 'MAX_FILES', 3)
    ready, proceed = threading.Event(), threading.Event()
    control_lock = deps.control_lock
    @contextmanager
    def schedule(path):
        if threading.current_thread() is thread:
            ready.set()
            assert proceed.wait(10)
        with control_lock(path):
            yield
    errors = []
    def vendor_second():
        try:
            deps.vendor(root, 'bravo', second)
        except ForgeError as exc:
            errors.append(exc.code)
    thread = threading.Thread(target=vendor_second)
    monkeypatch.setattr(deps, 'control_lock', schedule)
    thread.start()
    try:
        assert ready.wait(10)
        deps.vendor(root, 'alpha', first)
    finally:
        proceed.set()
        thread.join(10)
    assert not thread.is_alive()
    assert errors == ['resource_limit']
    assert not (root / 'packages/bravo').exists()


def test_oversize_opted_in_declaration_reports_its_limit(tmp_path):
    root = project(tmp_path)
    with (root / 'pyproject.toml').open('a') as output:
        output.write('#' + 'x' * deps.MAX_DECLARATION_BYTES)
    result = CliRunner().invoke(main, ['forge','dep','list','--root',str(root),'--json'])
    assert result.exit_code != 0 and 'resource_limit' in result.output, result.output


@pytest.mark.parametrize('system', ['Linux','Darwin'])
def test_readonly_notebook_probes_after_confinement(tmp_path, monkeypatch, system):
    from demiflow.workbench.notebooks import kernel_launcher as kernel
    root = project(tmp_path)
    (root/'pyproject.toml').unlink()
    config = tmp_path/'kernel.json'
    cfg = dict(root=str(root), python=sys.executable, code_root=str(Path(__file__).resolve().parents[1]),
               runtime_dir=str(tmp_path), tmp_dir=str(tmp_path/'temp'), sandbox_mode='readonly', write_paths=['data'])
    config.write_text(json.dumps(cfg))
    monkeypatch.setattr(sys, 'argv', ['kernel_launcher', str(config), str(tmp_path/'connection')])
    monkeypatch.setattr(kernel.platform, 'system', lambda: system)
    events = []
    monkeypatch.setattr(kernel, '_apply_landlock', lambda **kw: events.append('landlock'))
    def observe(command, **kwargs):
        events.append('seatbelt-probe' if command[0] == '/usr/bin/sandbox-exec' else 'probe')
        return type('Checked', (), {'returncode':0})()
    monkeypatch.setattr(kernel.subprocess, 'run', observe)
    monkeypatch.setattr(kernel.os, 'execve', lambda exe, argv, env: events.append('exec'))
    is_file = Path.is_file
    monkeypatch.setattr(Path, 'is_file', lambda p: str(p) == '/usr/bin/sandbox-exec' or is_file(p))
    monkeypatch.chdir(tmp_path)
    kernel.main()
    assert events == (['landlock','probe','exec'] if system == 'Linux' else ['seatbelt-probe','exec'])


def test_configured_native_author_uses_host_model_channel(tmp_path, monkeypatch):
    from http.server import BaseHTTPRequestHandler, HTTPServer

    root = project(tmp_path)
    calls = []

    class Model(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            calls.append((self.path, self.headers['Authorization'], body['model']))
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"received":true}')

        def log_message(self, *args):
            pass

    server = HTTPServer(('127.0.0.1', 0), Model)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    monkeypatch.setenv('FORGE_UPSTREAM_FIXTURE', 'host-fixture')
    cfg = tmp_path / 'host.json'
    cfg.write_text(json.dumps({'providers': {'codex': dict(model='test-model', credential_env='FORGE_UPSTREAM_FIXTURE',
        base_url=f'http://127.0.0.1:{server.server_port}/v1')}}))
    author = tmp_path / 'native'
    author.write_text(f'''#!{sys.executable}
import json, os, sys, urllib.request
sys.stdin.read()
assert 'FORGE_UPSTREAM_FIXTURE' not in os.environ
endpoint = 'http://' + os.environ['DEMIFLOW_MODEL_ENDPOINT'] + '/v1'
assert any(endpoint in option for option in sys.argv)
req = urllib.request.Request(endpoint + '/responses', data=b'{{"model":"test-model","input":"hello"}}',
    headers={{'Authorization':'Bearer ' + os.environ['DEMIFLOW_MODEL_TOKEN']}})
with urllib.request.urlopen(req, timeout=15) as reply:
    assert json.load(reply)['received']
for frame in [{{"type":"thread.started","thread_id":"native"}},{{"type":"turn.started"}},
    {{"type":"item.completed","item":{{"type":"agent_message","text":"ok"}}}},{{"type":"turn.completed"}}]:
    print(json.dumps(frame), flush=True)
''')
    author.chmod(0o755)
    try:
        result = CliRunner().invoke(main, ['forge', 'develop', '--root', str(root), '--state-dir', str(tmp_path / 'receipt'),
            '--executable', str(author), '--python', sys.executable, '--model-config', str(cfg),
            '--goal', 'read the project', '--no-review', '--json'])
        assert result.exit_code == 0, result.output
        assert calls == [('/v1/responses', 'Bearer host-fixture', 'test-model')]
        assert json.loads(result.output)['author']['completed']
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
