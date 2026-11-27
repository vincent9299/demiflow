"""The author sees ordinary references while the OS protects host and account data."""
import json
from dataclasses import replace
from pathlib import Path
import sys

import pytest

from demiflow.forge.project import initialize_project
from demiflow.forge.sandbox.policy import build_development_sandbox_spec, default_runtime_paths
from demiflow.forge.sandbox.sandbox import backend_readiness, launch_sandbox, specification
from demiflow.forge.sandbox.backends.container_oci import build_oci_command


@pytest.mark.parametrize('scoped', (False, True))
def test_private_paths_and_public_project_commands(tmp_path, monkeypatch, scoped):
    if sys.platform != 'linux' or not backend_readiness()['available']:
        pytest.skip('Requires the Linux development sandbox')
    project = tmp_path / 'project'
    initialize_project(project)
    (project / '.env').write_text('sample only')
    (project / '.env.example').write_text('template')
    reference = tmp_path / 'reference'
    reference.mkdir()
    (reference / 'model.py').write_text('example')
    state = tmp_path / 'host-state'
    state.mkdir()
    (state / 'receipt').write_text('sample only')
    account = tmp_path / 'account'
    (account / '.ssh').mkdir(parents=True)
    (account / '.ssh' / 'fixture').write_text('sample only')
    monkeypatch.setenv('HOME', str(account))
    monkeypatch.setenv('CODEX_HOME', str(account / '.codex'))
    home = state / 'session' / 'author'
    home.mkdir(mode=0o700, parents=True)
    later = tmp_path / 'new-state'
    source = Path(__file__).resolve().parents[1]
    spec = build_development_sandbox_spec(
        project, private_home=home, writes=('flows',),
        runtime_paths=default_runtime_paths(project_python=sys.executable),
        denied_paths=(state, later), host_state_root=state, read_roots=(reference,) if scoped else (), seconds=30,
    )
    script = '''
import json, sys
from pathlib import Path
project, reference, state, account, later, source = map(Path, sys.argv[1:])
sys.path.insert(0, str(source))
sys.stdin.readline()
(Path.home() / 'continued-session').write_text('scratch')
assert 'workspace.json' in {p.name for p in project.iterdir()}
assert {p.name for p in reference.iterdir()} == {'model.py'}
assert (reference / 'model.py').read_text() == 'example'
assert (project / '.env.example').read_text() == 'template'
for private in (project / '.env', state / 'receipt', state / 'late-session' / 'receipt',
                account / '.ssh' / 'fixture', later / 'receipt'):
    try:
        private.read_bytes()
    except PermissionError:
        continue
    raise AssertionError('Private path was readable: ' + private.name)
for private_dir in (state, state / 'late-session', account / '.ssh', later):
    try:
        list(private_dir.iterdir())
    except PermissionError:
        continue
    raise AssertionError('Private directory was listable')
from click.testing import CliRunner
from demiflow.cli import main
for command in ('context', 'check'):
    result = CliRunner().invoke(main, ['forge', command, '--root', str(project)])
    assert result.exit_code == 0, (command, str(result.exception), result.output[-256:])
print(json.dumps(['context', 'check']))
'''
    proc = launch_sandbox(
        [sys.executable, '-I', '-B', '-c', script, *map(str, (project, reference, state, account, later, source))],
        spec=spec, env={'PATH': '/usr/bin:/bin', 'OPENBLAS_NUM_THREADS': '1'}, cwd=project,
    )
    try:
        (state / 'late-session').mkdir()
        (state / 'late-session' / 'receipt').write_text('sample only')
        later.mkdir()
        (later / 'receipt').write_text('sample only')
        out, err = proc.communicate(input=b'go\n', timeout=35)
        assert proc.returncode == 0, err.decode(errors='replace')
        assert json.loads(out) == ['context', 'check']
        assert (home / 'continued-session').read_text() == 'scratch'
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate(timeout=5)


def test_oci_masks_nested_private_paths_under_a_readable_project(tmp_path):
    project = tmp_path / 'project'
    project.mkdir()
    private = project / '.env'
    private.write_text('sample only')
    state = project / 'host-state'
    state.mkdir()
    home = tmp_path / 'home'
    home.mkdir(mode=0o700)
    spec = build_development_sandbox_spec(project, private_home=home, denied_paths=(state,))
    command = build_oci_command('docker', 'fixture', ['/bin/sh'], spec=spec, env={}, cwd=project)
    assert f'/dev/null:{private}:ro' in command
    assert f'{state}:ro,nodev,nosuid,noexec,mode=000' in command
    assert f'{private}:{private}:ro' not in command


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux launch specification')
def test_host_state_exception_requires_only_the_bound_home(tmp_path):
    project = tmp_path / 'project'
    project.mkdir()
    state = tmp_path / 'state'
    home = state / 'session-home'
    home.mkdir(parents=True, mode=0o700)
    spec = build_development_sandbox_spec(project, private_home=home, host_state_root=state, denied_paths=(state,))
    for invalid in (replace(spec, host_state_root=None), replace(spec, host_state_root=str(project)),
                    replace(spec, private_home=str(state)), replace(spec, denied_paths=(*spec.denied_paths, str(home / 'secret')))):
        with pytest.raises(ValueError):
            specification(invalid, ['/bin/sh'], {}, project)


def test_explicit_direct_model_credential_remains_a_single_readable_file(tmp_path, monkeypatch):
    if sys.platform != 'linux' or not backend_readiness()['available']:
        pytest.skip('Requires the Linux development sandbox')
    project = tmp_path / 'project'
    project.mkdir()
    home = tmp_path / 'author'
    home.mkdir(mode=0o700)
    account = tmp_path / 'account'
    config = account / '.codex'
    config.mkdir(parents=True)
    selected = config / 'auth.json'
    selected.write_text('fixture')
    sibling = config / 'other'
    sibling.write_text('fixture')
    monkeypatch.setenv('HOME', str(account))
    monkeypatch.setenv('CODEX_HOME', str(config))
    spec = build_development_sandbox_spec(project, private_home=home, direct_network=True,
                                          active_credential_path=selected, runtime_paths=('/usr', '/lib', '/lib64'))
    script = """import sys
from pathlib import Path
assert Path(sys.argv[1]).read_text() == 'fixture'
try:
    Path(sys.argv[2]).read_bytes()
except PermissionError:
    pass
else:
    raise AssertionError('Unselected account file was readable')
"""
    proc = launch_sandbox(['/usr/bin/python3', '-I', '-B', '-c', script, str(selected), str(sibling)],
                          spec=spec, env={}, cwd=project)
    _, err = proc.communicate(timeout=10)
    assert proc.returncode == 0, err.decode(errors='replace')
