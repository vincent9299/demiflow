"""Unit & real-kernel tests for cross-platform SandboxBackend, chmod ENOSYS shim, and on-demand diffs."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

import pytest

from demiflow.forge.source_snapshot import capture, differences
from demiflow.workbench.conversations.execution import execution_facts
from demiflow.forge.sandbox.sandbox import SandboxSpec, backend_readiness, launch_sandbox
from demiflow.forge.sandbox.backends import (
    select_backend,
    validate_spec_portable,
)
from demiflow.forge.sandbox.backends.container_oci import build_oci_command
from demiflow.forge.sandbox.backends.macos_seatbelt import build_seatbelt_profile
from demiflow.forge.sandbox.backends.windows_wsl import (
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
        providers = {'deepseek': {'model': 'deepseek-chat', 'credential_env': 'KEY', 'model_provider': 'deepseek-official', 'executable': sys.executable}}

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
        lease_fd=None, deadline=time.time()+1800, check_cancel=lambda: None,
    )
    assert out == {'status': 'passed'}
    assert str(existing) in captured['spec'].runtime_paths
    assert str(stale) not in captured['spec'].runtime_paths


def test_reviewer_resets_developer_sandbox_policies_for_readonly_review(tmp_path, monkeypatch):
    from demiflow.workbench.conversations import pipeline_review
    from demiflow.forge.sandbox import sandbox
    from demiflow.forge.sandbox.backends import validate_spec_portable

    project_dir = tmp_path / 'proj'
    project_dir.mkdir()
    author_home = tmp_path / 'author-home'
    author_home.mkdir(mode=0o700)
    scratch = tmp_path / 'scratch' / 'work'
    scratch.mkdir(parents=True)

    author_spec = SandboxSpec(
        read_paths=(str(project_dir),),
        write_paths=(str(project_dir),),
        runtime_paths=(),
        protected_paths=(),
        denied_paths=(),
        private_home=str(author_home),
        role='developer',
        read_policy='inherit_os',
        write_policy='project_root',
        direct_network=True,
        seconds=0,
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
        providers = {'deepseek': {'model': 'deepseek-chat', 'credential_env': 'KEY', 'model_provider': 'deepseek-official', 'executable': sys.executable}}

    monkeypatch.setattr('demiflow.workbench.conversations.model_broker.start_model_broker', lambda *a, **kw: DummyBroker())

    def fake_launch(**kw):
        import sys
        spec = kw['sandbox_spec']
        captured['spec'] = spec
        # Validate against BOTH Linux sandbox specification and portable backend invariants
        # while temporary private_home still exists.
        validate_spec_portable(spec, [sys.executable, '-V'], {}, str(kw['home']))
        sandbox.specification(spec, [sys.executable, '-V'], {}, str(kw['home']))
        return object()

    monkeypatch.setattr('demiflow.workbench.conversations.execution.launch_provider', fake_launch)
    monkeypatch.setattr('demiflow.forge.review_deepseek.collect_review', lambda *a, **kw: {'status': 'passed'})

    out = pipeline_review.run_deployment_reviewer(
        'prompt', deployment=DummyDeployment(), turn={'id': 't1'},
        session={'id': 's1', 'provider': 'deepseek'}, scratch=scratch,
        broker_home=tmp_path / 'broker', author_spec=author_spec,
        lease_fd=None, deadline=time.time()+1800, check_cancel=lambda: None,
    )
    assert out == {'status': 'passed'}
    spec = captured['spec']
    assert spec.role == 'readonly'
    assert spec.read_policy == 'scoped'
    assert spec.write_policy == 'scoped'
    assert spec.direct_network is False
    assert spec.write_paths == ()


def test_forge_unifies_configs_role_and_keeps_legacy_parameters_compatible(tmp_path):
    from demiflow.forge import pipeline_check, project
    from demiflow.forge.project_rules import ROLE_DIRS
    from demiflow.workbench.runs import manager as run_manager
    assert {'flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks'}.issubset(ROLE_DIRS)
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
    from demiflow.forge.bridge.tools import run_project_layout as _run_project_layout

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
    from demiflow.forge.source_snapshot import capture

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
    from demiflow.forge.sandbox import sandbox_native

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


def test_source_opaque_constant_no_recursive_alias_and_static_ifexp(tmp_path):
    from demiflow.forge.pipeline_check import check_project
    from demiflow.forge.project import initialize_project
    from demiflow.forge.source import SourceTree, Symbol

    proj = tmp_path / 'opaque_proj'
    initialize_project(proj)
    for d in ('flows', 'operators', 'configs'):
        (proj / d).mkdir(exist_ok=True)
    (proj / 'operators' / 'base.py').write_text(
        'from pathlib import Path\n'
        'PROJECT = Path(__file__).resolve().parents[1]\n'
        'OPAQUE = object()\n'
        'def helper(x):\n'
        '    return x\n'
    )
    (proj / 'operators' / 'reexport.py').write_text(
        'from . import base\n'
        'helper = base.helper\n'
    )
    tree = SourceTree(proj)
    resolved_proj = tree.resolve(Symbol('operators.base.PROJECT'))
    assert resolved_proj == proj.resolve()
    resolved_const = tree.resolve(Symbol('operators.base.OPAQUE'))
    assert isinstance(resolved_const, Symbol) and resolved_const.opaque is True
    resolved_fn = tree.resolve(Symbol('operators.reexport.helper'))
    assert resolved_fn.module == 'operators.base'

    (proj / 'flows' / 'ifexp.py').write_text(
        'from demiflow import data\n'
        'def run(mode="small"):\n'
        '    n = 3 if mode == "small" else 10\n'
        '    return data.range(n).count()\n'
    )
    (proj / 'configs' / 'ifexp.json').write_text(
        '{"format": "demiflow_forge_flow_parameters_v2", "arguments": {"mode": "small"}}\n'
    )
    report = check_project(proj, entry='flows/ifexp.py:run', params='configs/ifexp.json')
    assert report['status'] == 'passed', report
    assert report['entries'][0]['status'] == 'checked', report


def test_flow_check_project_confined_static_config_read_and_security_boundary(tmp_path):
    from demiflow.forge.pipeline_check import check_project
    from demiflow.forge.project import initialize_project

    proj = tmp_path / 'cfg_proj'
    initialize_project(proj)
    for d in ('flows', 'configs', 'prompts', 'data'):
        (proj / d).mkdir(exist_ok=True)
    (proj / 'deployment.json').write_text('{"demo": {"count": 4}}\n')
    (proj / 'prompts' / 'p.yaml').write_text('version: 1\n')
    (proj / 'data' / 'secret.json').write_text('{"count": 9}\n')
    outside = tmp_path / 'outside.json'
    outside.write_text('{"count": 9}\n')

    (proj / 'flows' / 'good.py').write_text(
        'from copy import deepcopy\n'
        'import hashlib, json, yaml\n'
        'from pathlib import Path\n'
        'from demiflow import data\n'
        'ROOT = Path(__file__).resolve().parents[1]\n'
        'def run():\n'
        '    cfg = deepcopy(json.loads((ROOT / "deployment.json").read_bytes()))\n'
        '    base, extra = cfg["demo"], {"bias": 1}\n'
        '    merged = {**base, **extra}\n'
        '    p = (ROOT / "prompts" / "p.yaml").resolve()\n'
        '    if not p.is_relative_to(ROOT / "prompts") or not p.is_file():\n'
        '        raise ValueError("missing prompt")\n'
        '    raw = p.read_bytes()\n'
        '    _ = hashlib.sha256(raw).hexdigest()\n'
        '    meta = yaml.safe_load(raw)\n'
        '    return data.range(merged["count"] + merged["bias"] + meta["version"]).count()\n'
    )
    (proj / 'configs' / 'good.json').write_text('{"format": "demiflow_forge_flow_parameters_v2", "arguments": {}}\n')
    good = check_project(proj, entry='flows/good.py:run', params='configs/good.json')
    assert good['status'] == 'passed', good
    assert good['entries'][0]['status'] == 'checked', good
    assert 'deployment.json' in good['basis']['sources']
    assert 'prompts/p.yaml' in good['basis']['sources']

    # Security boundary 1: reading from protected data/ dir must NOT pass
    (proj / 'flows' / 'bad_data.py').write_text(
        'import json\n'
        'from pathlib import Path\n'
        'from demiflow import data\n'
        'ROOT = Path(__file__).resolve().parents[1]\n'
        'def run():\n'
        '    cfg = json.loads((ROOT / "data" / "secret.json").read_bytes())\n'
        '    return data.range(cfg["count"]).count()\n'
    )
    (proj / 'configs' / 'bad_data.json').write_text('{"format": "demiflow_forge_flow_parameters_v2", "arguments": {}}\n')
    bad_data = check_project(proj, entry='flows/bad_data.py:run', params='configs/bad_data.json')
    assert bad_data['status'] != 'passed', bad_data

    # Security boundary 2: reading outside project root must NOT pass
    (proj / 'flows' / 'bad_out.py').write_text(
        'import json\n'
        'from pathlib import Path\n'
        'from demiflow import data\n'
        'def run(target=""):\n'
        '    cfg = json.loads(Path(target).read_bytes())\n'
        '    return data.range(cfg["count"]).count()\n'
    )
    (proj / 'configs' / 'bad_out.json').write_text(
        f'{{"format": "demiflow_forge_flow_parameters_v2", "arguments": {{"target": "{outside.as_posix()}"}}}}\n'
    )
    bad_out = check_project(proj, entry='flows/bad_out.py:run', params='configs/bad_out.json')
    assert bad_out['status'] != 'passed', bad_out


def test_deepseek_stdio_exdev_rename_is_atomic(tmp_path):
    import subprocess
    from pathlib import Path
    shim = Path(__file__).resolve().parents[1] / 'demiflow/workbench/conversations/deepseek_stdio.cjs'
    src = tmp_path / 'src.txt'
    dst = tmp_path / 'dst.txt'
    src.write_text('new-content')
    dst.write_text('old-content')
    script = (
        f"const fs = require('fs');\n"
        f"const origSync = fs.renameSync;\n"
        f"let first = true;\n"
        f"fs.renameSync = function(a, b) {{ if (first) {{ first = false; const e = new Error('EXDEV'); e.code = 'EXDEV'; throw e; }} return origSync.call(this, a, b); }};\n"
        f"require({str(shim)!r});\n"
        f"fs.renameSync({str(src)!r}, {str(dst)!r});\n"
    )
    res = subprocess.run(['node', '-e', script], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr
    assert dst.read_text() == 'new-content'
    assert not src.exists()


def test_manage_project_layout_bind_table_and_pow_encode(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow.forge.pipeline_check import check_project
    from demiflow.forge.project import initialize_project
    from demiflow.forge.project_bindings import bind_table
    from demiflow.forge.schema import ForgeError
    from demiflow.forge.bridge.tools import PROJECT_LAYOUT_TOOL, run_project_layout as _run_project_layout

    ext = tmp_path / 'external_inputs'
    ext.mkdir()
    tbl = ext / 'items.lance'
    lance.write_dataset(pa.table({'x': [1, 2, 3]}), str(tbl))

    proj = tmp_path / 'tbl_proj'
    initialize_project(proj)
    for d in ('flows', 'configs'):
        (proj / d).mkdir(exist_ok=True)

    # bind_table is retired from manage_project_layout so dev assistants cannot recreate tables/.
    assert 'bind_table' not in PROJECT_LAYOUT_TOOL['inputSchema']['properties']['action']['enum']
    with pytest.raises(ForgeError, match='Unsupported manage_project_layout action'):
        _run_project_layout(proj, {'action': 'bind_table', 'alias': 'items_in', 'path': str(tbl), 'version': 1})
    assert not (proj / 'tables').exists()

    (proj / 'flows' / 'read_demo.py').write_text(
        'import hashlib\n'
        'from demiflow import data\n'
        'def run(source=None):\n'
        '    limit = 2 ** 3\n'
        '    tag = hashlib.sha256("ok".encode()).hexdigest()\n'
        '    if limit != 8 or len(tag) != 64:\n'
        '        raise ValueError("bad")\n'
        '    return data.read_lance(uri=source["uri"], version=source["version"]).count()\n'
    )
    # Simplified flat URI input works without tables/ or locations.
    (proj / 'configs' / 'read_demo.json').write_text(
        json.dumps({'source': {'uri': str(tbl), 'version': 1}}) + '\n'
    )
    report = check_project(proj, entry='flows/read_demo.py:run', params='configs/read_demo.json')
    assert report['status'] == 'passed', report
    assert report['entries'][0]['status'] == 'checked', report

    # Legacy table bindings remain readable for backward compatibility.
    legacy_proj = tmp_path / 'legacy_tbl_proj'
    initialize_project(legacy_proj)
    for d in ('flows', 'configs'):
        (legacy_proj / d).mkdir(exist_ok=True)
    bound = bind_table(legacy_proj, 'items_in', str(tbl), version=1)
    assert bound['status'] == 'bound'
    assert (legacy_proj / 'tables' / 'items_in.json').is_file()
    (legacy_proj / 'flows' / 'read_demo.py').write_text((proj / 'flows' / 'read_demo.py').read_text())
    (legacy_proj / 'configs' / 'read_demo.json').write_text(
        '{"format": "demiflow_forge_flow_parameters_v2", "arguments": {"source": {"kind": "table", "alias": "items_in"}}}\n'
    )
    legacy_report = check_project(legacy_proj, entry='flows/read_demo.py:run', params='configs/read_demo.json')
    assert legacy_report['status'] == 'passed', legacy_report
    assert legacy_report['entries'][0]['status'] == 'checked', legacy_report


def test_deepseek_tool_call_clips_oversized_write_payload():
    from demiflow.workbench.conversations.deepseek import DeepSeek
    from demiflow.workbench.conversations.common import wire

    adapter = DeepSeek.__new__(DeepSeek)
    adapter.thread = 'sess-1'
    adapter.items = {}
    adapter.message_kind = None
    big_content = 'x' * 50000
    events = adapter.event({
        'method': 'session/update',
        'params': {
            'sessionId': 'sess-1',
            'update': {
                'sessionUpdate': 'tool_call',
                'toolCallId': 'call_1',
                'title': 'write',
                'status': 'in_progress',
                'rawInput': {'file_path': 'configs/big.json', 'content': big_content},
            },
        },
    })
    assert events[1][0] == 'item.updated'
    wire(events[1][1], 65536)
    assert len(events[1][1]['rawInput']['content']) < 5000


def test_resolve_local_uri_tolerates_unreadable_parent_and_loop_continue_break(tmp_path, monkeypatch):
    from pathlib import Path
    from demiflow.lance import storage
    from demiflow.forge.pipeline_check import check_project
    from demiflow.forge.project import initialize_project

    target = tmp_path / 'sub' / 't.lance'
    target.mkdir(parents=True)
    orig_is_file = Path.is_file
    def guarded_is_file(self):
        if self.name == 'lance_locations.json':
            raise PermissionError('EACCES under Landlock')
        return orig_is_file(self)
    monkeypatch.setattr(Path, 'is_file', guarded_is_file)
    assert storage.resolve_local_uri(target) == target.resolve()
    monkeypatch.undo()

    proj = tmp_path / 'loop_proj'
    initialize_project(proj)
    for d in ('flows', 'configs'):
        (proj / d).mkdir(exist_ok=True)
    (proj / 'flows' / 'loop_demo.py').write_text(
        'from demiflow import data\n'
        'def run(items=None):\n'
        '    picked = 0\n'
        '    for v in items:\n'
        '        if v < 0:\n'
        '            continue\n'
        '        if v == 99:\n'
        '            break\n'
        '        picked = picked + v\n'
        '    return data.range(picked).count()\n'
    )
    (proj / 'configs' / 'loop_demo.json').write_text(
        '{"format": "demiflow_forge_flow_parameters_v2", "arguments": {"items": [-1, 2, 3, 99, 100]}}\n'
    )
    report = check_project(proj, entry='flows/loop_demo.py:run', params='configs/loop_demo.json')
    assert report['status'] == 'passed', report
    assert report['entries'][0]['status'] == 'checked', report


def test_read_policy_inherit_os_cross_platform_validation_and_backends(tmp_path):
    spec = _sample_spec(tmp_path, direct_network=False)
    inherit_spec = SandboxSpec(
        read_paths=spec.read_paths,
        write_paths=spec.write_paths,
        runtime_paths=spec.runtime_paths,
        protected_paths=spec.protected_paths,
        denied_paths=spec.denied_paths,
        private_home=spec.private_home,
        role='developer',
        seconds=30,
        direct_network=False,
        read_policy='inherit_os',
    )
    data = validate_spec_portable(inherit_spec, ['/bin/sh', '-c', 'true'], {'PATH': '/usr/bin'}, str(tmp_path))
    assert data['read_policy'] == 'inherit_os'

    # Non-developer roles must never inherit host read permissions.
    for bad_role in ('readonly', 'verifier'):
        bad = SandboxSpec(
            read_paths=spec.read_paths,
            write_paths=(),
            runtime_paths=spec.runtime_paths,
            protected_paths=spec.protected_paths,
            denied_paths=spec.denied_paths,
            private_home=spec.private_home,
            role=bad_role,
            read_policy='inherit_os',
        )
        with pytest.raises(ValueError, match='Only developer role'):
            validate_spec_portable(bad, ['/bin/sh'], {}, str(tmp_path))

    # macOS Seatbelt emits (allow file-read*) while keeping strict write & denied overrides.
    profile = build_seatbelt_profile(inherit_spec)
    assert '(allow file-read*)' in profile
    assert '(deny default)' in profile
    assert '(deny network*)' in profile
    read_idx = profile.index('(allow file-read*)')
    write_idx = profile.index(f'(allow file-write* (subpath "{tmp_path / "proj" / "flows"}")')
    prot_idx = profile.index(f'(deny file-write* (subpath "{tmp_path / "proj" / "workspace.json"}")')
    deny_idx = profile.index(f'(deny file-read* (subpath "{tmp_path / "secret.json"}")')
    assert read_idx < write_idx < prot_idx < deny_idx

    # Windows WSL2 propagates read_policy verbatim alongside POSIX-translated paths.
    win_spec = SandboxSpec(
        read_paths=(r'C:\work\proj',),
        write_paths=(r'C:\work\proj\flows',),
        runtime_paths=('/usr',),
        protected_paths=(r'C:\work\proj\workspace.json',),
        denied_paths=(r'C:\work\secret.json',),
        private_home=r'C:\work\home',
        role='developer',
        read_policy='inherit_os',
    )
    wsl_payload = build_wsl_spec_payload(win_spec, ['python3', '-V'], {'LANG': 'C.UTF-8'}, r'C:\work\proj')
    assert wsl_payload['read_policy'] == 'inherit_os'
    assert wsl_payload['write_paths'] == ['/mnt/c/work/proj/flows']


def test_linux_landlock_inherit_os_reads_external_and_urandom_while_blocking_writes(tmp_path):
    if not backend_readiness()['available']:
        pytest.skip('Linux Landlock sandbox unavailable')

    proj = tmp_path / 'proj'
    (proj / 'flows').mkdir(parents=True)
    ws = proj / 'workspace.json'
    ws.write_text('{"name":"demo"}\n', encoding='utf-8')
    external = tmp_path / 'external_ref'
    external.mkdir()
    ext_file = external / 'legacy.py'
    ext_file.write_text('# legacy reference\n', encoding='utf-8')
    home = tmp_path / 'home'
    home.mkdir(mode=0o700)

    # 1. inherit_os developer sandbox: external dir NOT in read_paths is readable,
    # /dev/urandom is readable, writes inside flows/ work, writes to external & workspace.json fail.
    inherit_spec = SandboxSpec(
        read_paths=(str(proj),),
        write_paths=(str(proj / 'flows'),),
        runtime_paths=('/usr', '/lib', '/lib64'),
        protected_paths=(str(ws),),
        denied_paths=(),
        private_home=str(home),
        role='developer',
        seconds=15,
        read_policy='inherit_os',
    )
    script = (
        'import os, pathlib, sys\n'
        'flows, ws, ext_dir, ext_file = [pathlib.Path(p) for p in sys.argv[1:]]\n'
        'assert ext_file.read_text() == "# legacy reference\\n"\n'
        'with open("/dev/urandom", "rb") as f:\n'
        '    assert len(f.read(16)) == 16\n'
        'with open("/dev/zero", "rb") as f:\n'
        '    assert f.read(8) == b"\\x00" * 8\n'
        '(flows / "ok.py").write_text("ok")\n'
        'for bad in (\n'
        '    lambda: (ext_dir / "escape.txt").write_text("no"),\n'
        '    lambda: ext_file.write_text("mutated"),\n'
        '    lambda: os.unlink(ext_file),\n'
        '    lambda: ws.write_text("mutated"),\n'
        '):\n'
        '    try:\n'
        '        bad()\n'
        '    except OSError:\n'
        '        continue\n'
        '    raise RuntimeError("Forbidden write was allowed under inherit_os")\n'
        'print("INHERIT_OK")\n'
    )
    proc = launch_sandbox(
        ['/usr/bin/python3', '-I', '-B', '-c', script, str(proj / 'flows'), str(ws), str(external), str(ext_file)],
        spec=inherit_spec,
        env={'PATH': '/usr/bin:/bin'},
        cwd=str(proj),
    )
    out, err = proc.communicate(timeout=15)
    assert proc.returncode == 0, f'out={out!r} err={err!r}'
    assert out.strip() == b'INHERIT_OK'
    assert (proj / 'flows' / 'ok.py').read_text() == 'ok'
    assert ext_file.read_text() == '# legacy reference\n'
    assert ws.read_text() == '{"name":"demo"}\n'

    # 2. scoped readonly sandbox still allows /dev/urandom & /dev/zero while denying external reads.
    scoped_spec = SandboxSpec(
        read_paths=(str(proj),),
        write_paths=(),
        runtime_paths=('/usr', '/lib', '/lib64'),
        protected_paths=(str(ws),),
        denied_paths=(),
        private_home=str(home),
        role='readonly',
        seconds=15,
        read_policy='scoped',
    )
    scoped_script = (
        'import pathlib, sys\n'
        'ext_file = pathlib.Path(sys.argv[1])\n'
        'with open("/dev/urandom", "rb") as f:\n'
        '    assert len(f.read(16)) == 16\n'
        'try:\n'
        '    ext_file.read_text()\n'
        '    raise RuntimeError("Scoped sandbox read external file")\n'
        'except OSError:\n'
        '    pass\n'
        'print("SCOPED_OK")\n'
    )
    proc2 = launch_sandbox(
        ['/usr/bin/python3', '-I', '-B', '-c', scoped_script, str(ext_file)],
        spec=scoped_spec,
        env={'PATH': '/usr/bin:/bin'},
        cwd=str(proj),
    )
    out2, err2 = proc2.communicate(timeout=15)
    assert proc2.returncode == 0, f'out={out2!r} err={err2!r}'
    assert out2.strip() == b'SCOPED_OK'


def test_project_root_write_policy_cross_platform_and_forge_protected_hook(tmp_path):
    from demiflow.forge.development import build_rework_prompt
    from demiflow.forge.project_guard import capture_protected_files, enforce_protected_files

    proj = tmp_path / 'empty_proj'
    (proj / '.forge').mkdir(parents=True)
    (proj / '.forge' / 'state.json').write_text('{"id":"w1"}\n', encoding='utf-8')
    (proj / '.env').write_text('CONCEPT_EVAL_API_KEY=real-secret\n', encoding='utf-8')
    (proj / '.env').chmod(0o600)
    (proj / 'AGENTS.md').write_text('# Rules\n', encoding='utf-8')
    (proj / 'workspace.json').write_text('{"name":"empty_proj"}\n', encoding='utf-8')
    ext_dir = tmp_path / 'external_legacy'
    ext_dir.mkdir()
    (ext_dir / 'old.py').write_text('# legacy\n', encoding='utf-8')
    home = tmp_path / 'home'
    home.mkdir(mode=0o700)

    spec = SandboxSpec(
        read_paths=(str(proj),),
        write_paths=(str(proj),),
        runtime_paths=('/usr', '/lib', '/lib64'),
        protected_paths=(str(proj / '.env'), str(proj / '.forge'), str(proj / 'AGENTS.md')),
        denied_paths=(str(tmp_path / 'control.db'),),
        private_home=str(home),
        role='developer',
        seconds=15,
        read_policy='inherit_os',
        write_policy='project_root',
    )
    validated = validate_spec_portable(spec, ['/bin/sh'], {'PATH': '/usr/bin'}, str(proj))
    assert validated['write_policy'] == 'project_root'

    # Non-developer role must never use project_root write policy.
    bad_ro = SandboxSpec(
        read_paths=(str(proj),), write_paths=(), runtime_paths=('/usr',),
        protected_paths=(), denied_paths=(), private_home=str(home),
        role='readonly', write_policy='project_root',
    )
    with pytest.raises(ValueError, match='project_root'):
        validate_spec_portable(bad_ro, ['/bin/sh'], {}, str(proj))

    # macOS Seatbelt allows project root writes while still denying protected paths.
    sb = build_seatbelt_profile(spec)
    assert f'(allow file-write* (subpath "{proj}")' in sb
    assert f'(deny file-write* (subpath "{proj / ".env"}")' in sb

    # Windows WSL2 forwards write_policy='project_root'.
    wsl = build_wsl_spec_payload(
        SandboxSpec(
            read_paths=(r'C:\work\proj',), write_paths=(r'C:\work\proj',),
            runtime_paths=('/usr',), protected_paths=(r'C:\work\proj\.env',),
            denied_paths=(), private_home=r'C:\work\home',
            role='developer', read_policy='inherit_os', write_policy='project_root',
        ),
        ['python3', '-V'], {'LANG': 'C.UTF-8'}, r'C:\work\proj',
    )
    assert wsl['write_policy'] == 'project_root'

    # Real Linux Landlock test: empty project can create flows/ & files natively while external writes stay blocked.
    if backend_readiness()['available']:
        script = (
            'import pathlib, sys\n'
            'proj, ext = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])\n'
            '(proj / "flows").mkdir()\n'
            '(proj / "flows" / "train.py").write_text("def run(): return 1\\n")\n'
            '(proj / ".env.example").write_text("CONCEPT_EVAL_API_KEY=\\n")\n'
            'assert (ext / "old.py").read_text() == "# legacy\\n"\n'
            'try:\n'
            '    (ext / "escape.txt").write_text("no")\n'
            '    raise RuntimeError("External write was allowed!")\n'
            'except OSError:\n'
            '    pass\n'
            'print("PROJ_ROOT_OK")\n'
        )
        proc = launch_sandbox(
            ['/usr/bin/python3', '-I', '-B', '-c', script, str(proj), str(ext_dir)],
            spec=spec, env={'PATH': '/usr/bin:/bin'}, cwd=str(proj),
        )
        out, err = proc.communicate(timeout=15)
        assert proc.returncode == 0, f'out={out!r} err={err!r}'
        assert out.strip() == b'PROJ_ROOT_OK'
        assert (proj / 'flows' / 'train.py').is_file()
        assert (proj / '.env.example').is_file()
        assert not (ext_dir / 'escape.txt').exists()

    # Forge post-turn protected-file hook detects & restores mutated/deleted protected files across all OSes.
    snap = capture_protected_files(proj)
    assert {'.env', '.forge/state.json', 'AGENTS.md'} <= set(snap)
    (proj / '.env').write_text('WIPED\n', encoding='utf-8')
    (proj / 'AGENTS.md').unlink()
    (proj / '.forge' / 'state.json').write_text('{"id":"tampered"}\n', encoding='utf-8')
    (proj / '.env.example').write_text('CONCEPT_EVAL_API_KEY=\n', encoding='utf-8')
    violations = enforce_protected_files(proj, snap)
    assert violations == ['.env', '.forge/state.json', 'AGENTS.md']
    assert (proj / '.env').read_text(encoding='utf-8') == 'CONCEPT_EVAL_API_KEY=real-secret\n'
    assert (proj / '.forge' / 'state.json').read_text(encoding='utf-8') == '{"id":"w1"}\n'
    assert (proj / 'AGENTS.md').read_text(encoding='utf-8') == '# Rules\n'
    assert (proj / '.env.example').is_file()
    rework = build_rework_prompt(None, None, project_root=proj, protected_violations=violations)
    assert rework is not None and '.env' in rework and '受保护文件钩子' in rework


def test_unlimited_dev_turn_seconds_and_sandbox_seconds_zero(tmp_path):
    import math
    from demiflow.workbench.conversations.policy import Limits, turn_deadline
    from demiflow.forge.sandbox.sandbox import SandboxSpec, backend_readiness, launch_sandbox
    from demiflow.forge.sandbox.backends import validate_spec_portable

    # Default turn_seconds is 0 (no 15-min wall-clock cutoff); explicit positive values still work.
    default_limits = Limits.parse({})
    assert default_limits.turn_seconds == 0
    assert math.isinf(turn_deadline({'created': 1000.0}, default_limits))

    finite_limits = Limits.parse({'turn_seconds': 45})
    assert finite_limits.turn_seconds == 45
    assert turn_deadline({'created': 1000.0}, finite_limits) == 1045.0

    proj = tmp_path / 'proj'
    home = tmp_path / 'home'
    proj.mkdir()
    home.mkdir(mode=0o700)

    dev_spec = SandboxSpec(
        read_paths=(str(proj),),
        write_paths=(str(proj),),
        runtime_paths=(),
        protected_paths=(),
        denied_paths=(),
        private_home=str(home),
        role='developer',
        seconds=0,
        read_policy='inherit_os',
        write_policy='project_root',
    )
    validated = validate_spec_portable(dev_spec, ['/usr/bin/python3', '-c', 'pass'], {'PATH': '/usr/bin:/bin'}, str(proj))
    assert validated['write_policy'] == 'project_root'

    # Readonly / verifier sandboxes still require a positive finite wall-clock bound.
    for role in ('readonly', 'verifier'):
        bad = SandboxSpec(
            read_paths=(str(proj),),
            write_paths=(),
            runtime_paths=(),
            protected_paths=(),
            denied_paths=(),
            private_home=str(home),
            role=role,
            seconds=0,
        )
        with pytest.raises(ValueError, match='Sandbox resource limit is invalid'):
            validate_spec_portable(bad, ['/usr/bin/python3', '-c', 'pass'], {'PATH': '/usr/bin:/bin'}, str(proj))

    if backend_readiness()['available']:
        script = (
            'import resource\n'
            'soft, hard = resource.getrlimit(resource.RLIMIT_CPU)\n'
            'assert soft == resource.RLIM_INFINITY or soft > 60, (soft, hard)\n'
            'print("UNLIMITED_OK")\n'
        )
        proc = launch_sandbox(
            ['/usr/bin/python3', '-I', '-B', '-c', script],
            spec=dev_spec, env={'PATH': '/usr/bin:/bin'}, cwd=str(proj),
        )
        out, err = proc.communicate(timeout=15)
        assert proc.returncode == 0, f'out={out!r} err={err!r}'
        assert out.strip() == b'UNLIMITED_OK'


def test_sandbox_sample_ignores_exiting_process_lookup_error(monkeypatch):
    import builtins
    import errno
    import time
    from demiflow.forge.sandbox import sandbox_child

    real_open = builtins.open

    class _VanishingStat:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, n=-1):
            raise ProcessLookupError(errno.ESRCH, 'No such process')

    class _FakeStat:
        def __init__(self, line):
            self._data = line.encode()
            self._pos = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, n=-1):
            chunk = self._data[self._pos:] if n < 0 else self._data[self._pos:self._pos + n]
            self._pos += len(chunk)
            return chunk

    class _Entry:
        def __init__(self, path, fail=False):
            self.path = path
            self._fail = fail

        def stat(self):
            if self._fail:
                raise ProcessLookupError(errno.ESRCH, 'No such process')
            raise FileNotFoundError()

    class _Scandir:
        def __init__(self, items):
            self._items = items

        def __enter__(self):
            return self

        def __iter__(self):
            return iter(self._items)

        def __exit__(self, *args):
            return False

    owner_stat = '100 (python) S ' + ' '.join(['0'] * 20) + ' 4 0 0'

    def fake_open(path, *args, **kwargs):
        if path == '/proc/100/stat':
            return _FakeStat(owner_stat)
        if path in ('/proc/200/stat', '/proc/201/stat'):
            return _VanishingStat()
        if path == '/proc/100/task/100/children':
            return _FakeStat('200 201')
        if path == '/proc/100/task/101/children':
            raise ProcessLookupError(errno.ESRCH, 'No such process')
        return real_open(path, *args, **kwargs)

    def fake_scandir(path):
        if path == '/proc/100/fd':
            return _Scandir([_Entry('/proc/100/fd/3', fail=True)])
        if path == '/proc/100/task':
            return _Scandir([_Entry('/proc/100/task/100'), _Entry('/proc/100/task/101')])
        raise FileNotFoundError(path)

    monkeypatch.setattr(builtins, 'open', fake_open)
    monkeypatch.setattr(sandbox_child.os, 'scandir', fake_scandir)
    rss, anonymous = sandbox_child.sample(100, 16, files=True, deadline=time.monotonic() + 5)
    assert rss == 4 * sandbox_child.os.sysconf('SC_PAGE_SIZE')
    assert anonymous == 0

    # Process count budget enforcement must remain strict.
    with pytest.raises(RuntimeError, match='Sandbox process budget exceeded'):
        sandbox_child.sample(100, 1, files=False, deadline=time.monotonic() + 5)


def test_store_replay_preserves_prior_turns_past_4096_and_during_active_turn(tmp_path):
    from demiflow.workbench.conversations.policy import Limits
    from demiflow.workbench.conversations.store import Store

    proj = tmp_path / 'proj'
    proj.mkdir()
    identity = {'path': str(proj), 'dev': 1, 'ino': 1}
    store = Store(tmp_path / 'state', Limits(), initialize=True)
    sess = store.create('ws1', identity, 'deepseek', 'req-long', 'pol1')
    sid = sess['id']

    # Turn 1: completed, >2500 events so it sits far beyond the 4096-event tail scan.
    t1, _ = store.submit('ws1', sid, 'sub-1', 0, '第一轮需求', identity, 'pol1')
    b1 = [('message.delta', {'item_id': 'p1', 'channel': 'process', 'text': f't{i} '}, False) for i in range(2500)]
    b1.append(('usage.observed', {'kind': 'step'}, False))
    b1.append(('message.completed', {'item_id': 'p1', 'channel': 'process', 'text': 'think-1'}, False))
    b1.append(('message.completed', {'item_id': 'a1', 'text': '第一轮最终结论'}, False))
    b1.append(('turn.state', {'state': 'completed', 'cleanup': 'complete'}, False))
    store.events_batch(t1['id'], b1)
    store.update(t1['id'], state='completed', cleanup='complete')

    # Turn 2: completed, >2500 events + file changes.
    t2, _ = store.submit('ws1', sid, 'sub-2', 1, '第二轮需求', identity, 'pol1')
    b2 = [('message.delta', {'item_id': 'p2', 'channel': 'process', 'text': f's{i} '}, False) for i in range(2500)]
    b2.append(('change.observed', {'files': [{'path': 'flows/demo.py'}]}, False))
    b2.append(('turn.state', {'state': 'completed', 'cleanup': 'complete'}, False))
    store.events_batch(t2['id'], b2)
    store.update(t2['id'], state='completed', cleanup='complete')

    # Turn 3: active, with closed deltas + 130 tool updates + 1 live streaming delta.
    t3, _ = store.submit('ws1', sid, 'sub-3', 2, '第三轮进行中', identity, 'pol1')
    b3 = []
    for i in range(20):
        b3.append(('message.delta', {'item_id': f'tp-{i}', 'channel': 'process', 'text': 'x'}, False))
        b3.append(('message.completed', {'item_id': f'tp-{i}', 'channel': 'process', 'text': f'done-{i}'}, False))
    b3.append(('usage.observed', {'kind': 'step'}, False))
    for i in range(130):
        b3.append(('item.updated', {'id': f'call-{i}', 'type': 'tool', 'title': f'cmd {i}', 'status': 'completed'}, False))
    b3.append(('message.delta', {'item_id': 'live', 'text': '正在输出第三轮'}, False))
    store.events_batch(t3['id'], b3)

    # While Turn 3 is active, switching tabs back (before_sequence=None) must keep Turns 1 & 2 AND Turn 3 live tail.
    active = store.replay('ws1', sid, limit=50, max_turns=12)
    users = [e['data']['text'] for e in active['events'] if e['type'] == 'message.user']
    assert users == ['第一轮需求', '第二轮需求', '第三轮进行中']
    assert any(e['type'] == 'message.completed' and e['data'].get('text') == '第一轮最终结论' for e in active['events'])
    assert any(e['type'] == 'change.observed' and e['turn_id'] == t2['id'] for e in active['events'])
    assert any(e['type'] == 'message.delta' and e['data'].get('text') == '正在输出第三轮' for e in active['events'])
    assert not any(e['type'] == 'usage.observed' for e in active['events'])
    t3_user_seq = next(e['sequence'] for e in active['events'] if e['type'] == 'message.user' and e['turn_id'] == t3['id'])
    assert active['first_sequence'] > t3_user_seq
    assert active['has_earlier'] is True

    # Upward pagination into Turn 3's middle steps does not duplicate Turns 1 & 2.
    older = store.replay('ws1', sid, limit=50, max_turns=8, before_sequence=active['first_sequence'])
    assert {e['turn_id'] for e in older['events']} == {t3['id']}
    assert any(e['type'] == 'message.user' and e['turn_id'] == t3['id'] for e in older['events'])

    # Once Turn 3 completes, initial replay covers all 3 turns cleanly from sequence 1 with has_earlier=False.
    store.event(t3['id'], 'message.completed', {'item_id': 'live', 'text': '第三轮完成'})
    store.event(t3['id'], 'turn.state', {'state': 'completed', 'cleanup': 'complete'})
    store.update(t3['id'], state='completed', cleanup='complete')
    done = store.replay('ws1', sid, limit=50, max_turns=12)
    assert [e['data']['text'] for e in done['events'] if e['type'] == 'message.user'] == ['第一轮需求', '第二轮需求', '第三轮进行中']
    assert [e['data']['text'] for e in done['events'] if e['type'] == 'message.completed' and e['data'].get('channel') not in ('process', 'summary')] == ['第一轮最终结论', '第三轮完成']
    assert done['first_sequence'] == 1
    assert done['has_earlier'] is False


def test_check_project_memoizes_table_describer_and_allows_high_budget(tmp_path):
    import time
    from demiflow.forge import operation_budget
    from demiflow.forge.pipeline_check import CHECK_SECONDS, MAX_VARIANTS, check_project
    from demiflow.forge.project import initialize_project, register_flow

    assert operation_budget.OPERATION_SECONDS == 60
    assert CHECK_SECONDS >= 180
    assert MAX_VARIANTS >= 64

    proj = tmp_path / 'proj'
    initialize_project(proj, name='budget-cache')
    (proj / 'flows').mkdir(exist_ok=True)
    (proj / 'configs').mkdir(exist_ok=True)
    shared_table = proj / 'data' / 'shared.lance'
    shared_table.mkdir(parents=True)

    for name in ('flow_a', 'flow_b'):
        (proj / 'flows' / f'{name}.py').write_text(
            'from demiflow import data\n\n'
            'def run(source: dict, out: dict):\n'
            '    ds = data.read_lance(source["uri"], version=source.get("version"))\n'
            '    return ds.write_lance(out["uri"], mode=out.get("mode", "create"), return_receipt=True)\n',
            encoding='utf-8',
        )
        (proj / 'configs' / f'{name}.json').write_text(
            json.dumps({
                'source': {'kind': 'table', 'uri': 'data/shared.lance'},
                'out': {'kind': 'output', 'uri': f'data/{name}_out.lance', 'mode': 'create'},
            }),
            encoding='utf-8',
        )

    from demiflow.data.flow_contract import field, scalar

    calls = []

    def counting_describer(uri, version):
        calls.append((str(uri), version))
        return {
            'version': 1,
            'schema_hash': 'sha256:demo',
            'row_count': 4,
            'fields': {'id': field(scalar('int64', nullable=False), evidence='metadata')},
        }

    # Register directly in workspace.json so check_project inspects both flows in one call.
    ws_path = proj / 'workspace.json'
    ws = json.loads(ws_path.read_text(encoding='utf-8'))
    ws['entries'] = {
        'flows/flow_a.py:run': ['configs/flow_a.json'],
        'flows/flow_b.py:run': ['configs/flow_b.json'],
    }
    ws_path.write_text(json.dumps(ws, indent=2), encoding='utf-8')

    report = check_project(proj, seconds=180, max_variants=64, table_describer=counting_describer)
    assert report['status'] == 'passed', report['diagnostics']
    assert len(report['entries']) == 2
    assert len(calls) == 1, f'Expected shared table to be described once, got {calls}'

    # Enclosing tighter scope must still bound an inner wider max_seconds scope.
    with operation_budget.scope(deadline=time.monotonic() + 2) as outer:
        with operation_budget.scope(deadline=time.monotonic() + 300, max_seconds=300) as inner:
            assert inner <= outer + 1e-6
