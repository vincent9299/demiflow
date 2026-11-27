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


def test_sandbox_pid1_reaps_orphaned_grandchildren_without_exhausting_process_budget(tmp_path):
    if sys.platform != 'linux' or not backend_readiness()['available']:
        pytest.skip('Requires the Linux development sandbox')
    project = tmp_path / 'project'
    project.mkdir()
    home = tmp_path / 'author'
    home.mkdir(mode=0o700)
    spec = build_development_sandbox_spec(
        project, private_home=home, runtime_paths=('/usr', '/lib', '/lib64'),
        processes=6, seconds=20,
    )
    # Spawn 14 orphaned grandchildren sequentially (well over processes=6).
    # Namespace PID 1 must reap them so dead tasks never exhaust the live process budget.
    reap_script = """import os, time
for _ in range(14):
    mid = os.fork()
    if mid == 0:
        grand = os.fork()
        if grand == 0:
            time.sleep(0.02)
            os._exit(0)
        os._exit(0)
    os.waitpid(mid, 0)
    time.sleep(0.08)
print('REAPED_OK')
"""
    proc = launch_sandbox(['/usr/bin/python3', '-I', '-B', '-c', reap_script],
                          spec=spec, env={'PATH': '/usr/bin:/bin'}, cwd=project)
    out, err = proc.communicate(timeout=20)
    assert proc.returncode == 0, (proc.returncode, err.decode(errors='replace'))
    assert out.strip() == b'REAPED_OK'

    # Live concurrent processes exceeding the budget must still be terminated (exit 125).
    live_script = """import os, time
for _ in range(10):
    if os.fork() == 0:
        time.sleep(10)
        os._exit(0)
time.sleep(10)
"""
    proc = launch_sandbox(['/usr/bin/python3', '-I', '-B', '-c', live_script],
                          spec=spec, env={'PATH': '/usr/bin:/bin'}, cwd=project)
    _, err = proc.communicate(timeout=20)
    assert proc.returncode == 125, (proc.returncode, err.decode(errors='replace'))
    assert b'Sandbox process budget exceeded' in err


def test_inherit_os_keeps_shared_tmp_non_ambient_and_bounded(tmp_path):
    import shutil
    import tempfile
    import time
    from pathlib import Path

    if sys.platform != 'linux' or not backend_readiness()['available']:
        pytest.skip('Requires the Linux development sandbox')
    project = tmp_path / 'project'
    project.mkdir()
    reference = tmp_path / 'reference'
    reference.mkdir()
    (reference / 'model.py').write_text('ok\n')
    home = tmp_path / 'author'
    home.mkdir(mode=0o700)
    # Create two unrelated top-level /tmp siblings: one explicitly denied (like
    # host notebooks/registry temp state), one unmentioned ambient scratch dir.
    denied_tmp = Path(tempfile.mkdtemp(prefix='demiflow-denied-tmp-', dir='/tmp'))
    ambient_tmp = Path(tempfile.mkdtemp(prefix='demiflow-ambient-tmp-', dir='/tmp'))
    try:
        (denied_tmp / 'token').write_text('secret-denied')
        (ambient_tmp / 'other').write_text('secret-ambient')
        spec = build_development_sandbox_spec(
            project, private_home=home, runtime_paths=('/usr', '/lib', '/lib64'),
            denied_paths=(denied_tmp,), seconds=20,
        )
        script = """import sys
from pathlib import Path
ref_file, denied_file, ambient_file = map(Path, sys.argv[1:])
assert ref_file.read_text() == 'ok\\n'
for blocked in (denied_file, ambient_file):
    try:
        blocked.read_bytes()
    except PermissionError:
        continue
    raise AssertionError('Shared /tmp path was readable: ' + str(blocked))
try:
    list(Path('/tmp').iterdir())
except PermissionError:
    pass
else:
    raise AssertionError('Shared /tmp directory was listable')
print('TMP_BOUNDED_OK')
"""
        t0 = time.monotonic()
        proc = launch_sandbox(
            ['/usr/bin/python3', '-I', '-B', '-c', script,
             str(reference / 'model.py'), str(denied_tmp / 'token'), str(ambient_tmp / 'other')],
            spec=spec, env={'PATH': '/usr/bin:/bin'}, cwd=project,
        )
        launch_secs = time.monotonic() - t0
        out, err = proc.communicate(timeout=20)
        assert proc.returncode == 0, (proc.returncode, err.decode(errors='replace'))
        assert out.strip() == b'TMP_BOUNDED_OK'
        assert launch_secs < 5.0, f'inherit_os admission took {launch_secs:.2f}s'
    finally:
        shutil.rmtree(denied_tmp, ignore_errors=True)
        shutil.rmtree(ambient_tmp, ignore_errors=True)
