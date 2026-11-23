"""Unit & real-kernel tests for cross-platform SandboxBackend, chmod ENOSYS shim, and on-demand diffs."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from demiflow.workbench.conversations.changes import capture, differences
from demiflow.workbench.conversations.execution import execution_facts
from demiflow.workbench.conversations.sandbox import SandboxSpec, backend_readiness, launch_sandbox
from demiflow.workbench.conversations.sandbox_backends import (
    select_backend,
    validate_spec_portable,
)
from demiflow.workbench.conversations.sandbox_backends.container_oci import build_oci_command
from demiflow.workbench.conversations.sandbox_backends.macos_seatbelt import build_seatbelt_profile
from demiflow.workbench.conversations.sandbox_backends.windows_wsl import (
    build_wsl_spec_payload,
    windows_to_wsl_path,
)


def _sample_spec(tmp_path: Path, *, direct_network: bool = False) -> SandboxSpec:
    home = tmp_path / 'home'
    home.mkdir(mode=0o700, exist_ok=True)
    proj = tmp_path / 'proj'
    (proj / 'flows').mkdir(parents=True, exist_ok=True)
    (proj / 'workspace.json').write_text('{}', encoding='utf-8')
    secret = tmp_path / 'secret.json'
    secret.write_text('{"k":"v"}', encoding='utf-8')
    return SandboxSpec(
        read_paths=(str(proj),),
        write_paths=(str(proj / 'flows'),),
        runtime_paths=('/usr',),
        protected_paths=(str(proj / 'workspace.json'),),
        denied_paths=(str(secret),),
        private_home=str(home),
        role='developer',
        seconds=30,
        direct_network=direct_network,
    )


def test_select_backend_routes_across_platforms_and_overrides(monkeypatch):
    monkeypatch.delenv('DEMIFLOW_SANDBOX_BACKEND', raising=False)
    assert select_backend(system='Linux').name == 'linux_landlock'
    assert select_backend(system='Darwin').name == 'macos_seatbelt'
    assert select_backend('container_oci', system='Linux').name == 'container_oci'
    assert select_backend('macos_seatbelt', system='Linux').name == 'macos_seatbelt'
    assert select_backend('windows_wsl', system='Linux').name == 'windows_wsl'
    monkeypatch.setenv('DEMIFLOW_SANDBOX_BACKEND', 'container_oci')
    assert select_backend(system='Linux').name == 'container_oci'
    with pytest.raises(ValueError, match='Unsupported sandbox backend'):
        select_backend('unknown_backend')


def test_validate_spec_portable_enforces_invariants(tmp_path):
    spec = _sample_spec(tmp_path)
    data = validate_spec_portable(spec, ['/bin/sh', '-c', 'true'], {'PATH': '/usr/bin', 'HTTP_PROXY': 'http://x'}, str(tmp_path))
    assert 'HTTP_PROXY' not in data['env']
    assert data['env']['HOME'] == spec.private_home

    with pytest.raises(ValueError, match='loader injection'):
        validate_spec_portable(spec, ['/bin/sh'], {'DYLD_INSERT_LIBRARIES': '/bad.dylib'}, str(tmp_path))
    with pytest.raises(ValueError, match='PYTHONPATH'):
        validate_spec_portable(spec, ['/bin/sh'], {'PYTHONPATH': '/bad'}, str(tmp_path))
    bad_spec = SandboxSpec(
        read_paths=(str(tmp_path),),
        write_paths=(str(tmp_path / 'proj'),),
        runtime_paths=('/usr',),
        protected_paths=(str(tmp_path / 'proj/workspace.json'),),
        denied_paths=(),
        private_home=spec.private_home,
    )
    with pytest.raises(ValueError, match='protected read-only path'):
        validate_spec_portable(bad_spec, ['/bin/sh'], {}, str(tmp_path))


def test_macos_seatbelt_profile_enforces_directory_boundary_and_precedence(tmp_path):
    spec = _sample_spec(tmp_path, direct_network=False)
    profile = build_seatbelt_profile(spec)
    assert '(version 1)' in profile
    assert '(deny default)' in profile
    assert '(deny network*)' in profile
    write_idx = profile.index(f'(allow file-write* (subpath "{tmp_path / "proj" / "flows"}")')
    prot_idx = profile.index(f'(deny file-write* (subpath "{tmp_path / "proj" / "workspace.json"}")')
    deny_idx = profile.index(f'(deny file-read* (subpath "{tmp_path / "secret.json"}")')
    assert write_idx < prot_idx < deny_idx

    net_profile = build_seatbelt_profile(_sample_spec(tmp_path, direct_network=True))
    assert '(allow network-outbound)' in net_profile


def test_container_oci_command_hardening_and_mounts(tmp_path):
    spec = _sample_spec(tmp_path, direct_network=False)
    argv = build_oci_command(
        '/usr/bin/docker',
        'python:3.11-slim',
        ['python3', '-c', 'print(1)'],
        spec=spec,
        env={'PATH': '/usr/bin:/bin', 'HTTPS_PROXY': 'http://leak'},
        cwd=str(tmp_path / 'proj'),
    )
    assert argv[:6] == ['/usr/bin/docker', 'run', '--rm', '-i', '--init', '--read-only']
    assert '--cap-drop=ALL' in argv
    assert '--security-opt=no-new-privileges:true' in argv
    assert '--network=none' in argv
    assert f'{tmp_path / "proj"}:{tmp_path / "proj"}:ro' in argv
    assert f'{tmp_path / "proj" / "flows"}:{tmp_path / "proj" / "flows"}:rw' in argv
    assert f'{tmp_path / "proj" / "workspace.json"}:{tmp_path / "proj" / "workspace.json"}:ro' in argv
    assert not any('secret.json' in item for item in argv)
    assert not any('HTTPS_PROXY' in item for item in argv)

    with pytest.raises(ValueError, match='Invalid OCI sandbox image'):
        build_oci_command('/usr/bin/docker', '--privileged', ['sh'], spec=spec, env={}, cwd=str(tmp_path))


def test_windows_wsl_path_translation_and_payload(tmp_path):
    assert windows_to_wsl_path(r'C:\Users\dev\project') == '/mnt/c/Users/dev/project'
    assert windows_to_wsl_path('D:/work/pipe/') == '/mnt/d/work/pipe'
    assert windows_to_wsl_path('/already/posix') == '/already/posix'
    with pytest.raises(ValueError, match='absolute drive'):
        windows_to_wsl_path('relative\\path')

    spec = SandboxSpec(
        read_paths=(r'C:\work\proj',),
        write_paths=(r'C:\work\proj\flows',),
        runtime_paths=('/usr',),
        protected_paths=(r'C:\work\proj\workspace.json',),
        denied_paths=(r'C:\work\secret.json',),
        private_home=r'C:\work\home',
    )
    payload = build_wsl_spec_payload(spec, ['python3', '-V'], {'LANG': 'C.UTF-8'}, r'C:\work\proj')
    assert payload['cwd'] == '/mnt/c/work/proj'
    assert payload['write_paths'] == ['/mnt/c/work/proj/flows']
    assert payload['denied_paths'] == ['/mnt/c/work/secret.json']


def test_linux_landlock_execution_facts_and_deepseek_chmod_enosys_shim(tmp_path):
    facts = execution_facts('restricted')
    if not backend_readiness()['available']:
        pytest.skip('Linux Landlock sandbox unavailable')
    assert facts['available'] is True
    assert facts['filesystem_enforcement'] == 'landlock_seccomp'

    node = Path('/yzp/zhaozy/yangzepeng/0905/.dsh/node/bin/node')
    if not node.is_file():
        found = shutil.which('node')
        if not found:
            pytest.skip('node binary unavailable')
        node = Path(found)
    stdio_cjs = Path(__file__).resolve().parents[1] / 'demiflow/workbench/conversations/deepseek_stdio.cjs'
    proj = tmp_path / 'proj'
    (proj / 'configs').mkdir(parents=True)
    home = tmp_path / 'home'
    home.mkdir(mode=0o700)
    openssl = Path('/etc/ssl/openssl.cnf')
    read = (str(proj), str(openssl)) if openssl.is_file() else (str(proj),)
    spec = SandboxSpec(
        read_paths=read,
        write_paths=(str(proj / 'configs'),),
        runtime_paths=tuple(sorted({'/usr', '/lib', '/lib64', str(node.resolve().parent.parent), str(stdio_cjs.parent)})),
        protected_paths=(),
        denied_paths=(),
        private_home=str(home),
        role='developer',
        seconds=15,
    )
    js = (
        'const fs = require("fs");\n'
        'const target = process.argv[1];\n'
        '(async () => {\n'
        '  fs.writeFileSync(target, "{\\"ok\\":true}\\n");\n'
        '  fs.chmodSync(target, 0o644);\n'
        '  await fs.promises.chmod(target, 0o600);\n'
        '  const fh = await fs.promises.open(target, "r+");\n'
        '  await fh.chmod(0o644);\n'
        '  await fh.close();\n'
        '  process.stdout.write("DONE\\n");\n'
        '})().catch(e => { console.error(e); process.exit(1); });\n'
    )
    target = proj / 'configs/out.json'
    proc = launch_sandbox(
        [str(node), '--require', str(stdio_cjs), '-e', js, str(target)],
        spec=spec, env={'PATH': '/usr/bin:/bin'}, cwd=str(proj),
    )
    out, err = proc.communicate(timeout=15)
    assert proc.returncode == 0, f'out={out!r} err={err!r}'
    assert out.strip() == b'DONE'
    assert target.read_text(encoding='utf-8') == '{"ok":true}\n'


def test_differences_generates_ondemand_diff_even_when_preview_budget_exhausted(tmp_path):
    (tmp_path / 'configs').mkdir()
    # Create enough existing files (> 128 KiB total) to exhaust capture()'s preview budget.
    for idx in range(15):
        (tmp_path / f'configs/base_{idx:02d}.json').write_text('{"x":"' + ('a' * 12000) + '"}\n', encoding='utf-8')
    before = capture(tmp_path, ['configs'])
    added = tmp_path / 'configs/new_manifest.json'
    added.write_text('{\n  "concepts": ["a", "b"]\n}\n', encoding='utf-8')
    after = capture(tmp_path, ['configs'])
    # Without root, preview budget might skip preview on new_manifest; with root, on-demand preview produces diff!
    assert after['configs/new_manifest.json']['sha256'] == hashlib.sha256(added.read_bytes()).hexdigest()
    changes = list(differences(before, after, ['configs'], root=tmp_path))
    assert len(changes) == 1
    assert changes[0]['path'] == 'configs/new_manifest.json'
    assert '+  "concepts": ["a", "b"]' in changes[0]['diff']


def test_documents_search_files_finds_nested_files_and_skips_hidden(tmp_path):
    from demiflow.workbench.projects import documents
    (tmp_path / 'parameters').mkdir()
    (tmp_path / 'parameters/reference_delivery.json').write_text('{}\n', encoding='utf-8')
    (tmp_path / 'flows').mkdir()
    (tmp_path / 'flows/reference_delivery.py').write_text('def run(): pass\n', encoding='utf-8')
    (tmp_path / '__pycache__').mkdir()
    (tmp_path / '__pycache__/reference_delivery.cpython-311.pyc').write_bytes(b'x')
    (tmp_path / '.hidden').mkdir()
    (tmp_path / '.hidden/reference_delivery.json').write_text('{}\n', encoding='utf-8')
    res = documents.search_files(tmp_path, 'reference_delivery.json', limit=20)
    assert [e['path'] for e in res['entries']] == ['parameters/reference_delivery.json']
    res2 = documents.search_files(tmp_path, 'reference_delivery', limit=20)
    assert sorted(e['path'] for e in res2['entries']) == [
        'flows/reference_delivery.py',
        'parameters/reference_delivery.json',
    ]


def test_static_check_drops_op06_and_denoises_incomplete_op03(tmp_path):
    from demiflow.forge.pipeline_check import result as check_result
    from demiflow.forge.source import InspectionError, SourceTree
    (tmp_path / 'mod_ok.py').write_text('from pathlib import Path\nX = 1\n', encoding='utf-8')
    (tmp_path / 'mod_bad.py').write_text('import requests\nX = 1\n', encoding='utf-8')
    tree = SourceTree(tmp_path)
    ns = tree.load('mod_ok')
    assert 'Path' in ns
    with pytest.raises(InspectionError) as exc_info:
        tree.load('mod_bad')
    assert exc_info.value.diagnostic['rule'] == 'OP-05'
    assert exc_info.value.diagnostic['status'] == 'invalid'

    # OP-06 / OP-03 incomplete warnings are suppressed; hard invalid errors are preserved.
    clean = check_result([
        {'rule': 'OP-06', 'status': 'incomplete', 'path': 'a.py', 'detail': 'unknown'},
        {'rule': 'OP-03', 'status': 'incomplete', 'path': 'a.py', 'detail': 'untyped'},
    ], ['a.py'])
    assert clean['status'] == 'passed'
    assert clean['diagnostic_count'] == 0

    hard = check_result([
        {'rule': 'OP-03', 'status': 'invalid', 'path': 'a.py', 'detail': 'wrong return'},
    ], ['a.py'])
    assert hard['status'] == 'failed'
    assert hard['diagnostic_count'] == 1


def test_reviewer_strips_deleted_author_runtime_paths(tmp_path, monkeypatch):
    from demiflow.workbench.conversations import pipeline_review
    stale = tmp_path / 'demiflow-dev-shims-gone'
    existing = tmp_path / 'existing-runtime'
    existing.mkdir()
    author_home = tmp_path / 'author-home'
    author_home.mkdir()
    scratch = tmp_path / 'scratch' / 'work'
    scratch.mkdir(parents=True)

    author_spec = SandboxSpec(
        read_paths=(str(tmp_path),),
        write_paths=(),
        runtime_paths=(str(existing), str(stale)),
        protected_paths=(),
        denied_paths=(),
        private_home=str(author_home),
        role='developer',
        seconds=30,
    )
    captured = {}

    class DummyBroker:
        socket_path = str(tmp_path / 'sock')
        child_config = {'model': 'deepseek-chat'}
        failure = None
        def close(self):
            pass

    class DummyDeployment:
        mode = 'restricted'
        providers = {'deepseek': {'model': 'deepseek-chat', 'credential_env': 'KEY', 'model_provider': 'deepseek-official'}}

    monkeypatch.setattr('demiflow.workbench.conversations.model_broker.start_model_broker', lambda *a, **kw: DummyBroker())
    def fake_launch(**kw):
        captured['spec'] = kw['sandbox_spec']
        return object()
    monkeypatch.setattr('demiflow.workbench.conversations.execution.launch_provider', fake_launch)
    monkeypatch.setattr('demiflow.forge.review_deepseek.collect_review', lambda *a, **kw: {'status': 'passed'})

    out = pipeline_review.run_deployment_reviewer(
        'prompt', deployment=DummyDeployment(), turn={'id': 't1'},
        session={'id': 's1', 'provider': 'deepseek'}, scratch=scratch,
        broker_home=tmp_path / 'broker', author_spec=author_spec,
        lease_fd=None, deadline=9999999999.0, check_cancel=lambda: None,
    )
    assert out == {'status': 'passed'}
    assert str(existing) in captured['spec'].runtime_paths
    assert str(stale) not in captured['spec'].runtime_paths


def test_forge_unifies_configs_role_and_keeps_legacy_parameters_compatible(tmp_path):
    from demiflow.forge import pipeline_check, project
    from demiflow.forge.project_rules import ROLE_DIRS
    from demiflow.workbench.runs import manager as run_manager
    assert ROLE_DIRS == ('flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks')
    proj = tmp_path / 'proj'
    project.initialize_project(proj, name='Demo')
    (proj / 'flows').mkdir()
    (proj / 'flows/demo.py').write_text('from demiflow import data\ndef run():\n    return data.range(2).count()\n')
    reg = project.register_flow(proj, 'flows/demo.py:run')
    assert reg['params'] == 'configs/demo.json'
    assert (proj / 'configs/demo.json').is_file()
    (proj / 'notebooks').mkdir()
    (proj / 'notebooks/explore.ipynb').write_text('{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":5}\n')
    # Discovery surfaces configs/ JSON and notebooks/ ipynb files
    disc = project.discover_project_sources(proj, include_roles=True)
    assert 'configs/demo.json' in disc['roles']['configs']
    assert 'notebooks/explore.ipynb' in disc['roles']['notebooks']
    # Standalone configs JSON and notebooks ipynb validate cleanly
    (proj / 'configs/extra.json').write_text('{"items": [1, 2]}\n')
    chk = pipeline_check.check_project(proj, paths=('configs/extra.json', 'notebooks/explore.ipynb'))
    assert chk['status'] == 'passed', chk
    # Visible logs/<run_id>/attempt-1 path
    run_dir = run_manager.directory(proj, 'r' + 'a' * 24)
    assert run_manager.attempt_directory(run_dir, 'logs', 1) == proj / 'logs' / ('r' + 'a' * 24) / 'attempt-1'
    # Legacy parameters/ binding remains accepted
    (proj / 'flows/legacy.py').write_text('from demiflow import data\ndef run():\n    return data.range(1).count()\n')
    (proj / 'parameters').mkdir()
    (proj / 'parameters/legacy.json').write_text('{"format":"demiflow_forge_flow_parameters_v2","arguments":{}}\n')
    reg_legacy = project.register_flow(proj, 'flows/legacy.py:run', params='parameters/legacy.json')
    assert reg_legacy['params'] == 'parameters/legacy.json'


def test_automatic_project_skips_pycache_without_exploding_writes(tmp_path):
    from demiflow.workbench.conversations.global_settings import automatic_project

    root = tmp_path / 'proj'
    root.mkdir()
    flows = root / 'flows'
    flows.mkdir()
    (flows / '__pycache__').mkdir()
    (flows / '__pycache__' / 'a.cpython-311.pyc').write_bytes(b'pyc')
    for idx in range(30):
        (flows / f'flow_{idx}.py').write_text('def run(data): return data\n')
    (root / 'operators').mkdir()
    (root / 'operators' / '__pycache__').mkdir()
    (root / 'operators' / 'op.py').write_text('x = 1\n')
    (root / 'logs').mkdir()
    grant = automatic_project(root)
    assert 'flows' in grant['writes']
    assert 'operators' in grant['writes']
    assert 'logs' not in grant['writes']
    assert all(not p.startswith('flows/') for p in grant['writes'])
    assert all('__pycache__' not in p for p in grant.get('protected', ()))


def test_manage_project_layout_bridge_tool(tmp_path):
    import pytest
    from demiflow.forge.project import initialize_project
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.conversations.context import _run_project_layout

    root = tmp_path / 'demo_proj'
    initialize_project(root)
    ensured = _run_project_layout(root, {'action': 'ensure_dirs', 'dirs': ['flows', 'configs', 'data_tools', 'data', 'logs']})
    assert ensured['status'] == 'ok'
    for name in ('flows', 'configs', 'data_tools', 'data', 'logs'):
        assert (root / name).is_dir()

    (root / 'flows' / 'alpha.py').write_text('from demiflow import data\ndef run():\n    return data.range(10).count()\n')
    (root / 'configs' / 'alpha.json').write_text('{"format": "demiflow_forge_flow_parameters_v2", "arguments": {}}\n')
    (root / 'parameters').mkdir()
    (root / 'parameters' / 'legacy.json').write_text('{"format": "demiflow_forge_flow_parameters_v2", "arguments": {}}\n')

    reg = _run_project_layout(root, {
        'action': 'register_flow',
        'entry': 'flows/alpha.py:run',
        'params': 'configs/alpha.json',
    })
    assert reg['params'] == 'configs/alpha.json'

    rebound = _run_project_layout(root, {
        'action': 'bind_params',
        'entry': 'flows/alpha.py:run',
        'params': 'parameters/legacy.json',
        'old_params': 'configs/alpha.json',
    })
    assert rebound['status'] == 'updated'
    assert rebound['params'] == ['parameters/legacy.json']

    with pytest.raises(ForgeError):
        _run_project_layout(root, {'action': 'ensure_dirs', 'dirs': ['.forge']})


def test_changes_capture_ignores_lance_and_runtime_artifacts(tmp_path):
    from demiflow.workbench.conversations.changes import capture

    root = tmp_path / 'proj'
    root.mkdir()
    (root / 'flows').mkdir()
    (root / 'flows' / 'main.py').write_text('x = 1\n')
    (root / 'tests' / '_runtime_run' / '1' / 'pool.lance' / 'data').mkdir(parents=True)
    (root / 'tests' / '_runtime_run' / '1' / 'pool.lance' / 'data' / 'a.lance').write_bytes(b'123')
    (root / 'tests' / '_runtime_run' / '1' / 'small.png').write_bytes(b'png')
    (root / 'logs' / 'run1').mkdir(parents=True)
    (root / 'logs' / 'run1' / 'run.log').write_text('hi\n')
    (root / 'data' / 'out.lance').mkdir(parents=True)
    (root / 'data' / 'out.lance' / 'meta.manifest').write_bytes(b'm')
    snap = capture(root, ['flows', 'tests', 'logs', 'data'])
    assert list(snap.keys()) == ['flows/main.py']


def test_store_replay_before_sequence_compacted_window(tmp_path):
    from demiflow.workbench.conversations.policy import Limits
    from demiflow.workbench.conversations.store import Store

    state_dir = tmp_path / 'state'
    project_dir = tmp_path / 'proj'
    project_dir.mkdir()
    identity = {'path': str(project_dir), 'dev': 1, 'ino': 1}
    store = Store(state_dir, Limits(), initialize=True)
    session = store.create('ws1', identity, 'codex', 'req1', 'ph1')
    sid = session['id']
    rev = session['revision']
    for idx in range(1, 6):
        turn, _ = store.submit('ws1', sid, f'sub-{idx}', rev, f'q{idx}', identity, 'ph1')
        tid = turn['id']
        rev += 1
        for ch in ('hello ', f'world {idx}'):
            store.event(tid, 'message.delta', {'item_id': f'a{idx}', 'text': ch})
        store.event(tid, 'message.completed', {'item_id': f'a{idx}', 'text': f'hello world {idx}'})
        store.update(tid, state='completed', cleanup='complete')

    latest = store.replay('ws1', sid, max_turns=2)
    assert latest['has_earlier'] is True
    assert [e['data']['text'] for e in latest['events'] if e['type'] == 'message.user'] == ['q4', 'q5']
    earlier = store.replay('ws1', sid, max_turns=2, before_sequence=latest['first_sequence'])
    assert earlier['has_earlier'] is True
    assert [e['data']['text'] for e in earlier['events'] if e['type'] == 'message.user'] == ['q2', 'q3']
    assert [e['data']['text'] for e in earlier['events'] if e['type'] == 'message.completed'] == ['hello world 2', 'hello world 3']
    assert not [e for e in earlier['events'] if e['type'] == 'message.delta']
    oldest = store.replay('ws1', sid, max_turns=2, before_sequence=earlier['first_sequence'])
    assert oldest['has_earlier'] is False
    assert [e['data']['text'] for e in oldest['events'] if e['type'] == 'message.user'] == ['q1']


def test_flow_check_allows_config_expr_and_ancestor_prefixed_imports(tmp_path):
    from demiflow.forge.pipeline_check import check_project
    from demiflow.forge.project import initialize_project, register_flow

    (tmp_path / 'pipelines').mkdir()
    outer = tmp_path / 'pipelines' / 'demo_pkg'
    initialize_project(outer)
    for d in ('flows', 'operators', 'configs'):
        (outer / d).mkdir(exist_ok=True)
    (outer / 'operators' / 'helpers.py').write_text(
        'from pathlib import Path\n'
        'PROJECT = Path(__file__).resolve().parents[1]\n'
        'def prompt_path(name: str) -> str:\n'
        '    return str(PROJECT / "prompts" / name)\n'
    )
    flow_file = outer / 'flows' / 'sample.py'
    flow_file.write_text(
        'from demiflow import data\n'
        'from pipelines.demo_pkg.operators.helpers import prompt_path\n'
        'def run(count=3, scale=2, policy="codex"):\n'
        '    expected = count * scale\n'
        '    if type(expected) is not int or expected <= 0:\n'
        '        raise ValueError("bad")\n'
        '    if policy not in ("codex", "retained"):\n'
        '        raise ValueError("bad policy")\n'
        '    p = prompt_path("x.md")\n'
        '    if policy == "codex":\n'
        '        return data.range(expected).count()\n'
        '    return data.range(1).count()\n'
    )
    (outer / 'configs' / 'sample.json').write_text(
        '{"format": "demiflow_forge_flow_parameters_v2", "arguments": {"count": 3, "scale": 2, "policy": "codex"}}\n'
    )
    report = check_project(outer, entry='flows/sample.py:run', params='configs/sample.json')
    entry_report = report['entries'][0]
    assert entry_report['status'] != 'invalid', report
    assert not [d for d in report.get('diagnostics', []) if d.get('code') == 'FL-07'], report


def test_tree_usage_tolerates_missing_and_transient_files(tmp_path, monkeypatch):
    from demiflow.workbench.conversations import sandbox_native

    root = tmp_path / 'work'
    root.mkdir()
    (root / 'keep.txt').write_text('hello')
    vanishing = root / 'vanish.tmp'
    vanishing.write_text('bye')

    real_scandir = sandbox_native.os.scandir

    class _VanishScandir:
        def __init__(self, ctx):
            self._ctx = ctx
        def __enter__(self):
            entries = list(self._ctx.__enter__())
            if vanishing.exists():
                vanishing.unlink()
            return iter(entries)
        def __exit__(self, *args):
            return self._ctx.__exit__(*args)

    monkeypatch.setattr(sandbox_native.os, 'scandir', lambda fd: _VanishScandir(real_scandir(fd)))
    used, count = sandbox_native.tree_usage([str(root), str(tmp_path / 'already_deleted')])
    assert used == 5
    assert count >= 1


def test_flow_check_supports_static_dict_str_methods_and_comprehensions(tmp_path):
    from demiflow.forge.pipeline_check import check_project
    from demiflow.forge.project import initialize_project

    proj = tmp_path / 'comp_proj'
    initialize_project(proj)
    for d in ('flows', 'configs'):
        (proj / d).mkdir(exist_ok=True)
    (proj / 'flows' / 'demo.py').write_text(
        'from demiflow import data\n'
        'def run(source=None, items=None):\n'
        '    ver = source.get("version")\n'
        '    names = {item["name"].strip() for item in items if item.get("enabled")}\n'
        '    if type(ver) is not int or ver <= 0 or not names:\n'
        '        raise ValueError("bad config")\n'
        '    if any(n.startswith("x") for n in names):\n'
        '        return data.range(len(names)).count()\n'
        '    return data.range(1).count()\n'
    )
    (proj / 'configs' / 'demo.json').write_text(
        '{"format": "demiflow_forge_flow_parameters_v2", "arguments": {'
        '"source": {"version": 2}, '
        '"items": [{"name": " x1 ", "enabled": true}, {"name": "y2", "enabled": false}]}}\n'
    )
    report = check_project(proj, entry='flows/demo.py:run', params='configs/demo.json')
    assert report['status'] == 'passed', report
