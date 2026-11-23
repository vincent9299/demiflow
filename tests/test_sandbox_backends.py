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
        'fs.writeFileSync(target, "{\\"ok\\":true}\\n");\n'
        'fs.chmodSync(target, 0o644);\n'
        'fs.promises.chmod(target, 0o600).then(() => console.error("CHMOD_OK"))\n'
        '  .then(() => process.stdout.write("DONE\\n"));\n'
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
