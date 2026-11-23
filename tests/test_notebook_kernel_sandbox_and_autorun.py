"""Tests for notebook assistant CLI-only sandbox, Kernel Landlock confinement,
project Python reuse, and sample/describe/count helpers."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

import demiflow


def test_notebook_assistant_sandbox_excludes_project_python(tmp_path):
    from demiflow.workbench.conversations.worker import sandbox_spec
    from demiflow.workbench.conversations.policy import Deployment

    proj = tmp_path / 'proj'
    proj.mkdir()
    (proj / 'workspace.json').write_text(json.dumps({'name': 'demo'}), encoding='utf-8')
    fake_venv = tmp_path / 'custom_venv'
    fake_bin = fake_venv / 'bin'
    fake_bin.mkdir(parents=True)
    fake_py = fake_bin / 'python'
    fake_py.write_text('#!/bin/sh\nexit 0\n', encoding='utf-8')
    fake_py.chmod(0o755)
    fake_exec = tmp_path / 'fake-cli'
    fake_exec.write_text('#!/bin/sh\nexit 0\n', encoding='utf-8')
    fake_exec.chmod(0o755)

    state = tmp_path / 'state'
    state.mkdir(mode=0o700)
    priv_home = state / 's1' / 'provider'
    priv_home.mkdir(parents=True, mode=0o700)

    deployment = type('Dep', (), {
        'mode': 'restricted',
        'direct_network': False,
        'state': state,
        'value': {},
        'providers': {'deepseek': {'executable': str(fake_exec)}},
        'limits': type('Lim', (), {
            'sessions': 8, 'turns': 8, 'turn_seconds': 60,
            'rss_bytes': 2 * 1024**3, 'processes': 32,
            'provider_bytes': 64 * 1024**2, 'private_total_bytes': 128 * 1024**2,
        })(),
    })()
    project = {
        'root': str(proj), 'kind': 'pipeline', 'writes': ['flows'],
        'protected': [], 'python': str(fake_py),
    }
    session = {'id': 's1', 'provider': 'deepseek'}
    config = {'executable': str(fake_exec)}

    dev_spec = sandbox_spec(deployment, project, session, {'operation': 'message', 'created': 10**12}, {'sources': []}, config, priv_home)
    nb_spec = sandbox_spec(deployment, project, session, {'operation': 'notebook_cell_cli', 'created': 10**12}, {'sources': []}, config, priv_home)

    assert str(fake_venv.resolve()) in dev_spec.runtime_paths
    assert str(fake_venv.resolve()) not in nb_spec.runtime_paths
    assert str(fake_bin.resolve()) not in nb_spec.runtime_paths
    assert str(proj.resolve()) in dev_spec.read_paths
    assert str(proj.resolve()) not in nb_spec.read_paths
    assert nb_spec.role == 'readonly'
    assert nb_spec.write_paths == ()


def test_registry_notebook_sandbox_validation_and_persistence(tmp_path):
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.projects import registry

    proj = tmp_path / 'proj'
    proj.mkdir()
    reg_path = tmp_path / 'console.json'
    created = registry.create(reg_path, proj, name='Demo')
    wid = created['workspace_id']

    listed = registry.projects(registry.load_registry(reg_path))
    assert listed['projects'][0]['notebook_sandbox'] == {'mode': 'readonly', 'write_paths': []}

    for bad in ('../outside', '/tmp/abs', '.git', 'flows', 'operators/sub', 'notebooks', 'notebooks/a.ipynb'):
        with pytest.raises(ForgeError):
            registry.set_notebook_sandbox(reg_path, wid, expected=listed['digest'], mode='readonly', write_paths=[bad])

    updated = registry.set_notebook_sandbox(
        reg_path, wid, expected=listed['digest'], mode='readonly',
        write_paths=['notebooks/outputs', 'data/annotations'],
    )
    assert updated['notebook_sandbox'] == {'mode': 'readonly', 'write_paths': ['notebooks/outputs', 'data/annotations']}

    listed2 = registry.projects(registry.load_registry(reg_path))
    assert listed2['projects'][0]['notebook_sandbox'] == {'mode': 'readonly', 'write_paths': ['notebooks/outputs', 'data/annotations']}


def test_kernel_launcher_rejects_disabled_or_missing_ipykernel(tmp_path):
    launcher = Path(demiflow.__file__).resolve().parent / 'workbench/notebooks/kernel_launcher.py'
    proj = tmp_path / 'proj'
    proj.mkdir()
    runtime_dir = tmp_path / 'runtime'
    runtime_dir.mkdir()
    tmp_dir = tmp_path / 'ktmp'
    conn = runtime_dir / 'conn.json'
    conn.write_text('{}', encoding='utf-8')

    # 1. Disabled project python -> explicit error, never falls back to workbench python.
    cfg_disabled = tmp_path / 'cfg-disabled.json'
    cfg_disabled.write_text(json.dumps({
        'python': '',
        'root': str(proj),
        'code_root': str(Path(demiflow.__file__).resolve().parents[1]),
        'runtime_dir': str(runtime_dir),
        'tmp_dir': str(tmp_dir),
        'sandbox_mode': 'readonly',
        'write_paths': [],
        'locations': [],
    }), encoding='utf-8')
    res = subprocess.run([sys.executable, '-I', '-B', str(launcher), str(cfg_disabled), str(conn)],
                         capture_output=True, text=True, timeout=15)
    assert res.returncode != 0
    assert '不继承平台 Python' in res.stderr

    # 2. Fake python missing ipykernel -> explicit error guiding user to install ipykernel.
    fake_py = tmp_path / 'no_ipy_python'
    fake_py.write_text('#!/bin/sh\nexit 1\n', encoding='utf-8')
    fake_py.chmod(0o755)
    cfg_missing = tmp_path / 'cfg-missing.json'
    cfg_missing.write_text(json.dumps({
        'python': str(fake_py),
        'root': str(proj),
        'code_root': str(Path(demiflow.__file__).resolve().parents[1]),
        'runtime_dir': str(runtime_dir),
        'tmp_dir': str(tmp_dir),
        'sandbox_mode': 'readonly',
        'write_paths': [],
        'locations': [],
    }), encoding='utf-8')
    res2 = subprocess.run([sys.executable, '-I', '-B', str(launcher), str(cfg_missing), str(conn)],
                          capture_output=True, text=True, timeout=15)
    assert res2.returncode != 0
    assert 'ipykernel' in res2.stderr


def test_kernel_landlock_enforces_readonly_root_and_allows_whitelisted_subdir(tmp_path):
    proj = tmp_path / 'proj'
    (proj / 'flows').mkdir(parents=True)
    (proj / 'flows' / 'main.py').write_text('x = 1\n', encoding='utf-8')
    (proj / 'notebooks').mkdir(parents=True)
    (proj / 'notebooks' / 'a.ipynb').write_text('{}', encoding='utf-8')
    allowed_out = proj / 'notebooks' / 'outputs'
    allowed_out.mkdir(parents=True)
    ktmp = tmp_path / 'ktmp'
    ktmp.mkdir(mode=0o700)
    runtime_dir = tmp_path / 'runtime'
    runtime_dir.mkdir(mode=0o700)

    code_root = str(Path(demiflow.__file__).resolve().parents[1])
    script = f"""
import sys
sys.path.insert(0, {code_root!r})
from pathlib import Path
from demiflow.workbench.notebooks.kernel_launcher import _apply_landlock, _python_prefixes

_apply_landlock(
    read_paths=[{str(proj)!r}, {str(runtime_dir)!r}, '/etc', '/proc', '/sys'],
    exec_paths=['/usr', '/bin', '/sbin', '/lib', '/lib64', {code_root!r}, *_python_prefixes(sys.executable)],
    write_paths=[{str(ktmp)!r}, {str(allowed_out)!r}, '/dev/null', '/dev/zero', '/dev/urandom'],
)

# 1. Reading project source succeeds
assert Path({str(proj / 'flows' / 'main.py')!r}).read_text() == 'x = 1\\n'

# 2. Writing project source or another notebook is hard-blocked by Linux Landlock
for forbidden in ({str(proj / 'flows' / 'main.py')!r}, {str(proj / 'notebooks' / 'a.ipynb')!r}, {str(proj / 'hack.txt')!r}):
    try:
        Path(forbidden).write_text('tampered')
    except PermissionError:
        pass
    else:
        raise AssertionError('Expected PermissionError for ' + forbidden)

# 3. Writing private tmp and whitelisted output subdir succeeds
Path({str(ktmp / 'ok.tmp')!r}).write_text('ok')
Path({str(allowed_out / 'result.json')!r}).write_text('{{"ok": true}}')
print('LANDLOCK_OK')
"""
    res = subprocess.run([sys.executable, '-I', '-B', '-c', script], capture_output=True, text=True, timeout=15)
    assert res.returncode == 0, res.stderr
    assert 'LANDLOCK_OK' in res.stdout
    assert (proj / 'flows' / 'main.py').read_text(encoding='utf-8') == 'x = 1\n'
    assert (allowed_out / 'result.json').read_text(encoding='utf-8') == '{"ok": true}'


def test_data_sample_limit_guard_and_count_describe():
    from demiflow.workbench.notebooks import data as nb_data

    with pytest.raises(ValueError, match='count\\(\\)'):
        nb_data.sample('runs/demo/table.lance', version=1, limit=1_000_000)

    with pytest.raises(ValueError, match='100'):
        nb_data.project_sample('runs/demo/table.lance', version=1, limit=101)
