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


def test_developer_sandbox_includes_declared_readonly_external_locations_only(tmp_path):
    from demiflow.workbench.conversations.worker import sandbox_spec

    proj = tmp_path / 'proj'
    proj.mkdir()
    ext_ro = tmp_path / 'external_ro_datasets'
    ext_ro.mkdir()
    ext_rw = tmp_path / 'external_rw_datasets'
    ext_rw.mkdir()
    state = tmp_path / 'state'
    state.mkdir(mode=0o700)
    priv_home = state / 's1' / 'provider'
    priv_home.mkdir(parents=True, mode=0o700)
    fake_exec = tmp_path / 'fake-cli'
    fake_exec.write_text('#!/bin/sh\nexit 0\n', encoding='utf-8')
    fake_exec.chmod(0o755)

    (proj / 'workspace.json').write_text(json.dumps({
        'name': 'demo',
        'locations': {
            'ext_ro': {'root': str(ext_ro), 'writable': False},
            'ext_rw': {'root': str(ext_rw), 'writable': True},
            'bad_ancestor': {'root': str(tmp_path), 'writable': False},
            'bad_state': {'root': str(state), 'writable': False},
        },
    }), encoding='utf-8')

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
    project = {'root': str(proj), 'kind': 'pipeline', 'phase': 'implementation', 'writes': ['flows', 'moved_root_script.py'], 'protected': []}
    (proj / 'flows').mkdir()
    session = {'id': 's1', 'provider': 'deepseek'}
    config = {'executable': str(fake_exec)}

    dev_spec = sandbox_spec(deployment, project, session, {'operation': 'message', 'created': 10**12}, {'sources': []}, config, priv_home)
    nb_spec = sandbox_spec(deployment, project, session, {'operation': 'notebook_cell_cli', 'created': 10**12}, {'sources': []}, config, priv_home)

    assert str(ext_ro.resolve()) in dev_spec.read_paths
    assert str(ext_ro.resolve()) not in dev_spec.write_paths
    assert str(proj.resolve()) in dev_spec.write_paths
    assert dev_spec.write_policy == 'project_root'
    assert str((proj / 'moved_root_script.py').resolve()) not in dev_spec.write_paths
    assert str(ext_rw.resolve()) not in dev_spec.read_paths
    assert str(tmp_path.resolve()) not in dev_spec.read_paths
    assert str(state.resolve()) not in dev_spec.read_paths
    assert str(ext_ro.resolve()) not in nb_spec.read_paths
    assert dev_spec.read_policy == 'inherit_os'
    assert dev_spec.direct_network is False
    assert nb_spec.read_policy == 'scoped'
    assert nb_spec.write_policy == 'scoped'
    assert nb_spec.direct_network is False

    net_project = {**project, 'dev_network': True}
    net_dev_spec = sandbox_spec(deployment, net_project, session, {'operation': 'message', 'created': 10**12}, {'sources': []}, config, priv_home)
    net_nb_spec = sandbox_spec(deployment, net_project, session, {'operation': 'notebook_cell_cli', 'created': 10**12}, {'sources': []}, config, priv_home)
    assert net_dev_spec.direct_network is True
    assert net_nb_spec.direct_network is False


def test_registry_notebook_sandbox_validation_and_persistence(tmp_path):
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.projects import registry

    proj = tmp_path / 'proj'
    proj.mkdir()
    reg_path = tmp_path / 'console.json'
    created = registry.create(reg_path, proj, name='Demo')
    wid = created['workspace_id']

    listed = registry.projects(registry.load_registry(reg_path))
    assert listed['projects'][0]['notebook_sandbox'] == {'mode': 'off', 'write_paths': []}

    for bad in ('../outside', '/tmp/abs', '.git', 'flows', 'operators/sub', 'notebooks/a.ipynb'):
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


def test_validate_python_accepts_venv_directory_and_resolves_interpreter(tmp_path):
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.projects import registry

    env_dir = tmp_path / 'myenv'
    (env_dir / 'bin').mkdir(parents=True)
    py_bin = env_dir / 'bin' / 'python'
    py_bin.write_text('#!/bin/sh\nexit 0\n', encoding='utf-8')
    py_bin.chmod(0o755)

    assert registry.validate_python(str(env_dir)) == str(py_bin)
    assert registry.validate_python(str(env_dir) + '/') == str(py_bin)
    assert registry.validate_python(str(py_bin)) == str(py_bin)

    empty_dir = tmp_path / 'empty_env'
    empty_dir.mkdir()
    with pytest.raises(ForgeError, match='bin/python'):
        registry.validate_python(str(empty_dir))


def test_data_tool_semantic_exports_and_catalog_metadata(tmp_path):
    from demiflow.workbench.projects import definitions, registry

    proj = tmp_path / 'proj'
    reg = tmp_path / 'reg.json'
    registry.create(reg, proj, name='Demo')
    tools_dir = proj / 'data_tools'
    tools_dir.mkdir(exist_ok=True)
    (tools_dir / 'review.py').write_text(
        '__demiflow_data_tools__ = {\n'
        '    "PoolReviewer": {\n'
        '        "kind": "annotation",\n'
        '        "title": "训练池全量审核",\n'
        '        "summary": "分页看图并人工通过/拒绝",\n'
        '        "when_to_use": "人工复核训练池候选图时使用",\n'
        '    },\n'
        '    "quick_stat": "analysis",\n'
        '}\n'
        'class PoolReviewer:\n    pass\n'
        'def quick_stat(df):\n    """统计通过率。"""\n    return len(df)\n',
        encoding='utf-8',
    )
    cat = definitions.catalog(proj, data_tools=True, include_platform=False)
    by_symbol = {item['symbol']: item for item in cat['definitions']}
    assert by_symbol['PoolReviewer']['tool_kind'] == 'annotation'
    assert by_symbol['PoolReviewer']['title'] == '训练池全量审核'
    assert by_symbol['PoolReviewer']['summary'] == '分页看图并人工通过/拒绝'
    assert by_symbol['PoolReviewer']['when_to_use'] == '人工复核训练池候选图时使用'
    assert by_symbol['quick_stat']['tool_kind'] == 'analysis'
    assert by_symbol['quick_stat']['title'] == 'quick_stat'
    assert '统计通过率' in by_symbol['quick_stat']['summary']


def test_write_data_tool_enforces_path_boundary_and_mandatory_semantics(tmp_path):
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.projects import definitions, registry

    proj = tmp_path / 'proj'
    reg = tmp_path / 'reg.json'
    registry.create(reg, proj, name='Demo')

    valid_source = (
        '__demiflow_data_tools__ = {\n'
        '    "inspect_batch": {\n'
        '        "kind": "view",\n'
        '        "title": "批次看图",\n'
        '        "summary": "按批次分页浏览合成图与评分",\n'
        '        "when_to_use": "查看某次管线产出的合成图质量时使用",\n'
        '    }\n'
        '}\n'
        'def inspect_batch(df):\n    return df.head(20)\n'
    )

    # 1. Path boundary: never allows escaping data_tools/
    for bad_path in ('../flows/hack.py', 'flows/hack.py', 'sub/mod.py', '_private.py', 'def.py'):
        with pytest.raises(ForgeError):
            definitions.write_data_tool(proj, path=bad_path, source=valid_source)

    # 2. Forge-aligned docstring semantics: rejects missing docstring / no public defs,
    #    accepts plain public function with docstring (no __demiflow_data_tools__ needed),
    #    and supports optional __demiflow_data_tools__ for finer kind/title/when_to_use.
    with pytest.raises(ForgeError, match='docstring'):
        definitions.write_data_tool(proj, path='no_doc.py', source='def f():\n    return 1\n')
    with pytest.raises(ForgeError, match='至少定义一个公开函数或类'):
        definitions.write_data_tool(proj, path='private_only.py', source='def _helper():\n    """internal"""\n    return 1\n')
    with pytest.raises(ForgeError, match='docstring'):
        definitions.write_data_tool(
            proj, path='no_sem.py',
            source='__demiflow_data_tools__ = {"f": "analysis"}\ndef f():\n    return 1\n',
        )

    plain_doc_source = (
        'def quick_preview(df):\n'
        '    """快速抽样查看数据表前 20 行与列分布。"""\n'
        '    return df.head(20)\n'
    )
    plain_res = definitions.write_data_tool(proj, path='quick_preview.py', source=plain_doc_source)
    assert plain_res['status'] == 'written'
    assert plain_res['tools'] == [{
        'symbol': 'quick_preview',
        'kind': 'tool',
        'title': 'quick_preview',
        'summary': '快速抽样查看数据表前 20 行与列分布。',
        'when_to_use': '',
    }]

    tagged_doc_source = (
        '__demiflow_data_tools__ = {"score_hist": "analysis"}\n'
        'def score_hist(df):\n'
        '    """统计评测得分直方图。"""\n'
        '    return df\n'
    )
    tagged_res = definitions.write_data_tool(proj, path='score_hist.py', source=tagged_doc_source)
    assert tagged_res['tools'][0]['kind'] == 'analysis'
    assert tagged_res['tools'][0]['summary'] == '统计评测得分直方图。'

    # 3. Full optional semantic dict continues to work and is immediately discoverable in catalog
    result = definitions.write_data_tool(proj, path='batch_viewer.py', source=valid_source)
    assert result['status'] == 'written'
    assert result['path'] == 'data_tools/batch_viewer.py'
    assert result['module'] == 'data_tools.batch_viewer'
    assert (proj / 'data_tools' / 'batch_viewer.py').is_file()
    cat = definitions.catalog(proj, data_tools=True, include_platform=False)
    by_symbol = {item['symbol']: item for item in cat['definitions']}
    assert by_symbol['quick_preview']['summary'] == '快速抽样查看数据表前 20 行与列分布。'
    assert by_symbol['inspect_batch']['title'] == '批次看图'
    assert by_symbol['inspect_batch']['when_to_use'] == '查看某次管线产出的合成图质量时使用'


def test_dev_sandbox_and_grant_keep_data_tools_readonly(tmp_path):
    from demiflow.workbench.conversations.global_settings import automatic_project
    from demiflow.workbench.conversations.worker import sandbox_spec

    proj = tmp_path / 'proj'
    (proj / 'flows').mkdir(parents=True)
    (proj / 'data_tools').mkdir(parents=True)
    (proj / 'workspace.json').write_text(json.dumps({'name': 'demo'}), encoding='utf-8')
    state = tmp_path / 'state'
    state.mkdir(mode=0o700)
    priv_home = state / 's1' / 'provider'
    priv_home.mkdir(parents=True, mode=0o700)
    fake_exec = tmp_path / 'fake-cli'
    fake_exec.write_text('#!/bin/sh\nexit 0\n', encoding='utf-8')
    fake_exec.chmod(0o755)

    grant = automatic_project(proj)
    assert 'flows' in grant['writes']
    assert 'data_tools' not in grant['writes']

    deployment = type('Dep', (), {
        'mode': 'restricted', 'direct_network': False, 'state': state, 'value': {},
        'providers': {'deepseek': {'executable': str(fake_exec)}},
        'limits': type('Lim', (), {
            'sessions': 8, 'turns': 8, 'turn_seconds': 60, 'rss_bytes': 2 * 1024**3,
            'processes': 32, 'provider_bytes': 64 * 1024**2, 'private_total_bytes': 128 * 1024**2,
        })(),
    })()
    project = {'root': str(proj), 'kind': 'pipeline', 'writes': ['flows', 'data_tools'], 'protected': []}
    spec = sandbox_spec(deployment, project, {'id': 's1', 'provider': 'deepseek'},
                        {'operation': 'message', 'created': 10**12}, {'sources': []}, {'executable': str(fake_exec)}, priv_home)
    assert str((proj / 'flows').resolve()) in spec.write_paths
    assert str((proj / 'data_tools').resolve()) not in spec.write_paths
    assert str((proj / 'data_tools').resolve()) in spec.protected_paths
