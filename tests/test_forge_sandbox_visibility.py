"""The author sees ordinary references while the OS protects host and account data."""
from dataclasses import replace
import sys

import pytest

from demiflow.forge.sandbox.policy import build_development_sandbox_spec
from demiflow.forge.sandbox.sandbox import backend_readiness, launch_sandbox, specification
from demiflow.forge.sandbox.backends.container_oci import build_oci_command


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
