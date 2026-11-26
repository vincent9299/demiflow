"""Batch 1 end-to-end acceptance tests for lock-free Forge project dependencies,
static checking, redacted review, runtime/sandbox isolation, Workbench integration,
and CPU trainer + Predictor B-class package regression.
"""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import lance
import pytest

import demiflow
from demiflow.forge import dependencies as deps
from demiflow.cli import main as demiflow_cli
from demiflow.forge.context import build_context, render_context
from demiflow.forge.pipeline_check import check_project
from demiflow.forge.project import initialize_project, register_flow
from demiflow.forge.review_evidence import capture_evidence, verify_evidence
from demiflow.forge.runtime import (
    ENVIRONMENT as SHARED_SOURCES_ENV,
    FORMAT as SHARED_SOURCES_FORMAT,
    install_bound_imports,
    interpreter_identity,
    probe_environment,
)
from demiflow.forge.sandbox.sandbox import SandboxSpec, backend_readiness, launch_sandbox
from demiflow.forge.sandbox.sandbox_native import landlock_abi
from demiflow.forge.sandbox.sandbox_shims import SandboxShims
from demiflow.forge.schema import ForgeError


def _make_pkg(path: Path, files: dict[str, str | bytes]) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    for rel, content in files.items():
        target = path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            target.write_bytes(content)
        else:
            target.write_text(content, encoding='utf-8')
    return path


def _init_pipeline_project(root: Path, *, name: str = 'demo_proj') -> Path:
    initialize_project(root, name=name)
    (root / 'flows').mkdir(exist_ok=True)
    (root / 'operators').mkdir(exist_ok=True)
    (root / 'configs').mkdir(exist_ok=True)
    (root / 'tests').mkdir(exist_ok=True)
    return root


# ---------------------------------------------------------------------------
# 1. Lock-free pyproject.toml declaration + vendor / remove / list / verify / install
# ---------------------------------------------------------------------------

def test_deps_lock_free_incremental_vendor_remove_and_content_bounds(tmp_path):
    root = _init_pipeline_project(tmp_path / 'proj')
    ext_root = tmp_path / 'ext'
    pkg_a = _make_pkg(ext_root / 'alpha_src', {
        '__init__.py': 'VALUE = 10\n',
        'sub.py': 'def bump(x): return x + 1\n',
        '__pycache__/cached.cpython-311.pyc': b'\x00\x01\x02',  # must be ignored
    })
    pkg_b = _make_pkg(ext_root / 'beta_src', {
        '__init__.py': 'FACTOR = 3\n',
    })
    ext_ref = _make_pkg(ext_root / 'gamma_ref', {
        '__init__.py': 'TAG = "v1"\n',
    })

    # Declare 2 missing vendored packages, 1 local_ref, and 1 missing A-class requirement
    (root / 'pyproject.toml').write_text(
        '[project]\n'
        'name = "demo_proj"\n'
        'dependencies = ["definitely-not-installed-demiflow-pkg-xyz>=9.9.9"]\n\n'
        '[tool.demiflow.packages]\n'
        'alpha_pkg = {source = "vendored", path = "packages/alpha_pkg"}\n'
        f'gamma_pkg = {{source = "local_ref", path = "{ext_ref}"}}\n\n'
        '[tool.demiflow.packages.beta_pkg]\n'
        'source = "vendored"\n'
        'path = "packages/beta_pkg"\n',
        encoding='utf-8',
    )

    # Incremental vendor succeeds even while beta_pkg and A-class requirement are missing
    res1 = deps.vendor(root, 'alpha_pkg', pkg_a, allowed_roots=(ext_root,))
    assert res1['status'] == 'vendored'
    assert not (root / '.forge/deps.lock.json').exists()
    assert not (root / 'packages/alpha_pkg/__pycache__').exists()

    # Unauthorized source rejected BEFORE enumeration/hashing
    outside = tmp_path / 'outside_forbidden'
    _make_pkg(outside, {'__init__.py': 'X = 1\n'})
    with pytest.raises(ForgeError, match='授权|scope|read'):
        deps.vendor(root, 'beta_pkg', outside, allowed_roots=(ext_root,))
    assert not (root / 'packages/beta_pkg').exists()

    # Vendor beta_pkg cleanly
    res2 = deps.vendor(root, 'beta_pkg', pkg_b, allowed_roots=(ext_root,))
    assert res2['status'] == 'vendored'

    # Failure during re-vendor (e.g. forbidden weight file or reverse import) preserves existing packages/alpha_pkg
    bad_alpha = _make_pkg(ext_root / 'bad_alpha', {
        '__init__.py': 'VALUE = 999\n',
        'model.safetensors': b'weights',
    })
    with pytest.raises(ForgeError):
        deps.vendor(root, 'alpha_pkg', bad_alpha, allowed_roots=(ext_root,))
    assert (root / 'packages/alpha_pkg/__init__.py').read_text(encoding='utf-8') == 'VALUE = 10\n'

    bad_reverse = _make_pkg(ext_root / 'bad_reverse', {
        '__init__.py': 'from flows import main\n',
    })
    with pytest.raises(ForgeError, match='project role|反向|imports'):
        deps.vendor(root, 'alpha_pkg', bad_reverse, allowed_roots=(ext_root,))
    assert (root / 'packages/alpha_pkg/__init__.py').read_text(encoding='utf-8') == 'VALUE = 10\n'

    # remove vendored package (multiline table) deletes declaration + managed copy
    rem_b = deps.remove(root, 'beta_pkg')
    assert rem_b['status'] == 'removed'
    assert not (root / 'packages/beta_pkg').exists()
    assert 'beta_pkg' not in deps.declaration(root)['packages']

    # remove local_ref deletes ONLY declaration, NEVER the external source directory
    rem_g = deps.remove(root, 'gamma_pkg')
    assert rem_g['status'] == 'removed'
    assert (ext_ref / '__init__.py').read_text(encoding='utf-8') == 'TAG = "v1"\n'
    assert 'gamma_pkg' not in deps.declaration(root)['packages']

    # CLI rejects removed sync & link subcommands, and install requires explicit separate python
    from click.testing import CliRunner
    runner = CliRunner()
    for removed_cmd in ('sync', 'link'):
        out = runner.invoke(demiflow_cli, ['forge', 'dep', removed_cmd, '--root', str(root)])
        assert out.exit_code != 0
    bad_install = runner.invoke(demiflow_cli, ['forge', 'dep', 'install', '--root', str(root)])
    assert bad_install.exit_code != 0
    same_py = runner.invoke(demiflow_cli, ['forge', 'dep', 'install', '--root', str(root), '--python', sys.executable])
    assert same_py.exit_code != 0


# ---------------------------------------------------------------------------
# 2. Static check: 4 error classes, exact line numbers, TYPE_CHECKING/ImportError
# ---------------------------------------------------------------------------

def test_forge_check_four_dependency_error_classes_and_guards(tmp_path):
    root = _init_pipeline_project(tmp_path / 'proj')
    pkg_dir = _make_pkg(tmp_path / 'ok_pkg', {
        '__init__.py': 'from .real import fn\n',
        'real.py': 'def fn(x): return x\n',
    })
    # Orphan directory under packages/
    _make_pkg(root / 'packages/orphan_pkg', {'__init__.py': 'X = 1\n'})

    (root / 'pyproject.toml').write_text(
        '[project]\n'
        'name = "proj"\n'
        'dependencies = ["missing-a-dep-xyz>=1.0"]\n\n'
        '[tool.demiflow.packages]\n'
        'ok_pkg = {source = "vendored", path = "packages/ok_pkg"}\n'
        'missing_pkg = {source = "vendored", path = "packages/missing_pkg"}\n'
        'pytest = {source = "vendored", path = "packages/pytest"}\n',
        encoding='utf-8',
    )
    deps.vendor(root, 'ok_pkg', pkg_dir)
    # Put a valid package dir for `pytest` so it triggers installed-module shadowing when --python is given
    _make_pkg(root / 'packages/pytest', {'__init__.py': 'X = 1\n'})

    (root / 'flows/demo.py').write_text(
        'from typing import TYPE_CHECKING\n'
        'from ok_pkg.missing_sub import nope\n'
        'import definitely_missing_third_party_abc\n'
        'if TYPE_CHECKING:\n'
        '    import typing_only_missing_xyz\n'
        '    from demiflow.workbench.runs import manager\n'
        'try:\n'
        '    import optional_missing_pkg_xyz\n'
        'except ImportError:\n'
        '    optional_missing_pkg_xyz = None\n'
        'def run():\n'
        '    return 1\n',
        encoding='utf-8',
    )

    # Without --python: environment checks explicitly marked skipped; B-class & orphan & submodule & boundary still checked
    no_py = check_project(root)
    assert bool(no_py['coverage'].get('skipped_environment_checks'))
    diags_no_py = no_py['diagnostics']
    # Missing B-class package pinpointed to pyproject.toml line
    assert any('missing_pkg' in d['detail'] and d.get('path') == 'pyproject.toml' and d.get('line') for d in diags_no_py)
    # Orphan packages/orphan_pkg reported
    assert any('orphan_pkg' in d['detail'] for d in diags_no_py)
    # Missing B-class submodule pinpointed to flows/demo.py line 2
    assert any('ok_pkg.missing_sub' in d['detail'] and d.get('path') == 'flows/demo.py' and d.get('line') == 2 for d in diags_no_py)
    # TYPE_CHECKING boundary violation to demiflow.workbench MUST NOT be exempted!
    assert any('demiflow.workbench' in d['detail'] and d.get('path') == 'flows/demo.py' and d.get('line') == 6 for d in diags_no_py)
    # TYPE_CHECKING & try/except ImportError missing imports MUST be exempted
    assert not any('typing_only_missing_xyz' in d['detail'] for d in diags_no_py)
    assert not any('optional_missing_pkg_xyz' in d['detail'] for d in diags_no_py)

    # With explicit --python: A-class missing, installed shadowing, AND statically missing third-party import reported
    with_py = check_project(root, python=sys.executable)
    assert not with_py['coverage'].get('skipped_environment_checks')
    diags_py = with_py['diagnostics']
    assert any('missing-a-dep-xyz' in d['detail'] and d.get('path') == 'pyproject.toml' and d.get('line') for d in diags_py)
    assert any('pytest' in d['detail'] and d.get('path') == 'pyproject.toml' and d.get('line') for d in diags_py)
    assert any('definitely_missing_third_party_abc' in d['detail'] and d.get('path') == 'flows/demo.py' and d.get('line') == 3 for d in diags_py)
    assert not any('typing_only_missing_xyz' in d['detail'] for d in diags_py)
    assert not any('optional_missing_pkg_xyz' in d['detail'] for d in diags_py)
    assert any('demiflow.workbench' in d['detail'] and d.get('line') == 6 for d in diags_py)

    # forge context still succeeds when packages are missing and tells Agent what is missing
    ctx = build_context(root)
    assert ctx['status'] == 'ready'
    rendered = render_context(ctx)
    assert 'missing_pkg' in rendered
    assert 'vendor' in rendered


# ---------------------------------------------------------------------------
# 3. Review: redacted line-preserving TOML, 500 KiB+ non-expansion, evidence_id
# ---------------------------------------------------------------------------

def test_review_redacts_toml_preserves_lines_summarizes_large_pkg_and_invalidates(tmp_path):
    root = _init_pipeline_project(tmp_path / 'proj')
    secret_host = tmp_path / 'private_host_secret_dir' / 'big_lib'
    _make_pkg(secret_host, {
        '__init__.py': 'def run_helper(): return 1\n',
        'part1.py': '# ' + ('A' * (300 * 1024)) + '\nVALUE = 1\n',
        'part2.py': '# ' + ('B' * (300 * 1024)) + '\nVALUE = 2\n',
    })
    raw_toml = (
        '[project]\n'
        'name = "proj"\n'
        'dependencies = ["pkg @ https://alice:supersecretpass@packages.example.com/pkg-1.0.whl"]\n'
        '\n'
        '[tool.demiflow.packages]\n'
        f'big_lib = {{source = "local_ref", path = "{secret_host}"}}\n'
    )
    (root / 'pyproject.toml').write_text(raw_toml, encoding='utf-8')
    (root / 'flows/main.py').write_text(
        'from big_lib import run_helper\n'
        'def run():\n'
        '    return run_helper()\n',
        encoding='utf-8',
    )

    ev1 = capture_evidence(root, entry='flows/main.py:run', allowed_roots=(secret_host,))
    # pyproject.toml present, redacted, exact line count preserved
    assert 'pyproject.toml' in ev1['files']
    redacted_toml = ev1['files']['pyproject.toml']['after']
    assert 'supersecretpass' not in redacted_toml
    assert 'private_host_secret_dir' not in redacted_toml
    assert len(redacted_toml.splitlines()) == len(raw_toml.splitlines())
    # Large external package body NOT expanded in files; only metadata summary in dependencies
    assert not any('big_lib' in path for path in ev1['files'])
    assert 'AAA' * 100 not in json.dumps(ev1)
    pkg_summary = ev1['dependencies']['packages']['big_lib']
    assert pkg_summary['source'] == 'local_ref'
    assert pkg_summary['file_count'] == 3
    assert pkg_summary['bytes'] > 500 * 1024
    assert len(pkg_summary['tree_sha256']) == 64
    assert 'private_host_secret_dir' not in json.dumps(ev1['dependencies'])
    assert verify_evidence(root, ev1, allowed_roots=(secret_host,))

    # Changing a byte inside the B-class package invalidates the old identity
    (secret_host / '__init__.py').write_text('def run_helper(): return 2\n', encoding='utf-8')
    assert not verify_evidence(root, ev1, allowed_roots=(secret_host,))
    ev2 = capture_evidence(root, entry='flows/main.py:run', allowed_roots=(secret_host,))
    assert ev2['identity'] != ev1['identity']

    # Changing pyproject.toml invalidates ev2
    (root / 'pyproject.toml').write_text(raw_toml + '# comment\n', encoding='utf-8')
    assert not verify_evidence(root, ev2, allowed_roots=(secret_host,))


# ---------------------------------------------------------------------------
# 4. Runtime & Sandbox: Landlock write refusal, live local_ref v1->v2, bridge vendor/remove
# ---------------------------------------------------------------------------

def test_sandbox_landlock_live_local_ref_and_bridge_vendor_remove(tmp_path):
    if sys.platform != 'linux' or landlock_abi() < 1 or not backend_readiness()['available']:
        pytest.skip('Linux Landlock sandbox required')

    from demiflow.forge.bridge.server import BridgeServer
    from demiflow.workbench.conversations.execution import runtime_paths
    from demiflow.workbench.conversations.worker import sandbox_spec

    root = _init_pipeline_project(tmp_path / 'proj')
    (root / 'packages').mkdir(exist_ok=True)
    # Start WITHOUT pyproject.toml (developer creates & saves it via turn-scoped empty placeholder)
    toml_path = root / 'pyproject.toml'
    toml_path.write_text('', encoding='utf-8')

    shared_root = tmp_path / 'authorized_libs'
    v1 = _make_pkg(shared_root / 'mylib_v1', {'__init__.py': 'VERSION = "v1"\n'})
    v2 = _make_pkg(shared_root / 'mylib_v2', {'__init__.py': 'VERSION = "v2"\n'})
    sibling_secret = _make_pkg(shared_root / 'sibling_unlisted', {'__init__.py': 'SECRET = 1\n'})
    vend_src = _make_pkg(shared_root / 'vend_src', {'__init__.py': 'KIND = "vendored"\n'})

    (root / 'tests/test_deps.py').write_text(
        'import pytest\n'
        'def test_imports():\n'
        '    import mylib, vendpkg\n'
        '    assert vendpkg.KIND == "vendored"\n'
        '    with pytest.raises(ModuleNotFoundError):\n'
        '        import sibling_unlisted\n'
        '    print("MYLIB_VERSION=" + mylib.VERSION)\n',
        encoding='utf-8',
    )

    class _ActiveTurn:
        tid = 't-turn-1'
        epoch = 'e-epoch-1'
        def require_active(self):
            return None

    bridge = BridgeServer(
        _ActiveTurn(),
        project_root=root,
        allowed_read_roots=(shared_root,),
    )
    project = {
        'root': str(root),
        'kind': 'pipeline',
        'phase': 'implementation',
        'writes': ['flows', 'operators', 'tests'],
        'protected': [],
        'python': sys.executable,
        'read_roots': [str(shared_root)],
    }
    shims = SandboxShims(
        project,
        execution_mode='restricted',
        project_python=sys.executable,
        allowed_read_roots=(shared_root,),
    )
    shims.set_control_binding(bridge.token, 't-turn-1', 'e-epoch-1')

    priv_home = tmp_path / 'priv_home'
    priv_home.mkdir(mode=0o700)
    (priv_home / 'tmp').mkdir(mode=0o700)
    sdk_root = Path(demiflow.__file__).resolve().parents[1]
    spec = SandboxSpec(
        read_paths=(str(root.resolve()), str(shared_root.resolve())),
        write_paths=(str((root / 'flows').resolve()), str((root / 'tests').resolve()), str(toml_path.resolve())),
        runtime_paths=tuple(sorted({
            *runtime_paths({'executable': sys.executable}, project_python=sys.executable),
            str(sdk_root),
            *(str(p) for p in shims.runtime_paths),
        })),
        protected_paths=(str((root / 'packages').resolve()), str((root / '.forge').resolve()), str(shared_root.resolve())),
        private_home=str(priv_home),
        role='developer',
        seconds=45,
        write_policy='scoped',
    )
    script = f'''set -eu
python -c '
from pathlib import Path
for forbidden in ("packages/direct_write.py", ".forge/direct_write.json"):
    try:
        Path(forbidden).write_text("bad")
    except PermissionError:
        pass
    else:
        raise AssertionError("OS did not block direct write to " + forbidden)
Path("pyproject.toml").write_text("""[tool.demiflow.packages]
mylib = {{source = "local_ref", path = "{v1}"}}
vendpkg = {{source = "vendored", path = "packages/vendpkg"}}
""", encoding="utf-8")
'
# Restricted dep install must be blocked (offline)
if demiflow forge dep install --root . --python {sys.executable} >/dev/null 2>&1; then
    echo "INSTALL_SHOULD_FAIL" >&2
    exit 1
fi
# Restricted dep vendor succeeds via control bridge and takes effect in same turn
demiflow forge dep vendor vendpkg "{vend_src}" --root . --json > /dev/null
OUT1=$(pytest -s tests/test_deps.py)
echo "$OUT1" | grep -q "MYLIB_VERSION=v1"
# Edit pyproject.toml from v1 -> v2 with NO extra Forge command; next pytest immediately loads v2
python -c '
from pathlib import Path
Path("pyproject.toml").write_text("""[tool.demiflow.packages]
mylib = {{source = "local_ref", path = "{v2}"}}
vendpkg = {{source = "vendored", path = "packages/vendpkg"}}
""", encoding="utf-8")
'
OUT2=$(pytest -s tests/test_deps.py)
echo "$OUT2" | grep -q "MYLIB_VERSION=v2"
# Remove vendpkg via control bridge
demiflow forge dep remove vendpkg --root . --json > /dev/null
'''
    try:
        proc = launch_sandbox(
            ['/bin/sh', '-c', script],
            spec=spec,
            env={'PATH': f'{shims.bin}:/usr/bin:/bin', 'LANG': 'C.UTF-8'},
            cwd=str(root),
            control_socket=bridge.path,
        )
        stdout, stderr = proc.communicate(timeout=45)
        assert proc.returncode == 0, f'stdout={stdout.decode(errors="replace")}\nstderr={stderr.decode(errors="replace")}'
        # Persists after turn ends; vendpkg removed; v2 kept; external v1/v2 untouched
        assert not (root / 'packages/vendpkg').exists()
        assert (v1 / '__init__.py').is_file() and (v2 / '__init__.py').is_file()
        assert 'vendpkg' not in deps.declaration(root)['packages']
        assert deps.declaration(root)['packages']['mylib']['path'] == str(v2)
        assert 'packages/vendpkg' in bridge.finish_turn()
    finally:
        shims.close()
        bridge.close()


# ---------------------------------------------------------------------------
# 5. Terminal trusted_local forge develop --python + startup .pth trust check
# ---------------------------------------------------------------------------

def test_terminal_develop_shims_and_startup_pth_trust_boundary(tmp_path):
    root = _init_pipeline_project(tmp_path / 'proj')
    lib = _make_pkg(tmp_path / 'local_lib', {'__init__.py': 'NUM = 77\n'})
    (root / 'pyproject.toml').write_text(
        '[tool.demiflow.packages]\n'
        f'local_lib = {{source = "local_ref", path = "{lib}"}}\n',
        encoding='utf-8',
    )
    shims = SandboxShims({'root': str(root), 'kind': 'pipeline'}, execution_mode='trusted_local', project_python=sys.executable)
    try:
        res = subprocess.run(
            [str(shims.bin / 'python'), '-B', '-c', 'import local_lib; print(local_lib.NUM)'],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert res.returncode == 0, res.stderr
        assert res.stdout.strip() == '77'
    finally:
        shims.close()

    # Verify startup_sha256 is recorded in interpreter identity
    ident = interpreter_identity()
    assert 'startup_sha256' in ident and len(ident['startup_sha256']) == 64


# ---------------------------------------------------------------------------
# 6. Workbench: legacy registry compat, preview + shared-sources conflict,
#    sealed run/resume across upgrade, Notebook old-vs-new kernel isolation
# ---------------------------------------------------------------------------

def test_workbench_legacy_registry_preview_and_shared_sources_conflict(tmp_path, monkeypatch):
    from demiflow.workbench.graph.preview import preview
    from demiflow.workbench.projects import registry

    proj = _init_pipeline_project(tmp_path / 'proj')
    ext = _make_pkg(tmp_path / 'ext_pkg', {'__init__.py': 'OFFSET = 5\n'})
    (proj / 'pyproject.toml').write_text(
        '[tool.demiflow.packages]\n'
        f'ext_pkg = {{source = "local_ref", path = "{ext}"}}\n',
        encoding='utf-8',
    )
    (proj / 'flows/demo.py').write_text(
        'import pyarrow as pa\n'
        'from demiflow import data\n'
        'from ext_pkg import OFFSET\n'
        'def run(*, output):\n'
        '    return data.from_items([{"x": OFFSET}]).write_lance(output, mode="create", schema=pa.schema([("x", pa.int64())]))\n',
        encoding='utf-8',
    )
    (proj / 'configs/demo.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {'output': str(proj / 'data/out.lance')},
    }))
    from _forge_project_fixtures import declare_existing_flow
    declare_existing_flow(proj, 'flows/demo.py:run', params='configs/demo.json')

    # Register project & inject legacy `dependency_lock` field into registry.json to verify read compatibility
    reg_path = tmp_path / 'console.json'
    created = registry.add(reg_path, proj)
    wid = created['workspace_id']
    raw_reg = json.loads(reg_path.read_text(encoding='utf-8'))
    raw_reg['projects'][wid]['dependency_lock'] = 'a' * 64
    raw_reg['projects'][wid]['python'] = sys.executable
    raw_reg['projects'][wid]['read_roots'] = [str(ext)]
    reg_path.write_text(json.dumps(raw_reg), encoding='utf-8')
    loaded = registry.load_registry(reg_path)
    assert wid in loaded['projects']
    # Updating read_roots strips legacy dependency_lock cleanly
    updated = registry.set_read_roots(reg_path, wid, expected=registry.projects(loaded)['digest'], read_roots=[str(ext)])
    assert 'dependency_lock' not in json.loads(reg_path.read_text(encoding='utf-8'))['projects'][wid]
    assert updated['read_roots'] == [str(ext.resolve())]

    # Preview works with B-class package via WebAuthority
    from demiflow.workbench.runs.authority import WebAuthority
    auth = WebAuthority(str(proj.resolve()), wid, str(reg_path.resolve()), 'read', True, True)
    plan = preview(proj, entry='flows/demo.py:run', params='configs/demo.json', authority=auth)
    assert plan['available'] is True

    # Shared-sources name collision with B-class package is rejected
    conflict_src = _make_pkg(tmp_path / 'conflict_parent' / 'ext_pkg', {'__init__.py': 'OFFSET = 99\n'})
    shared_file = tmp_path / 'shared-sources.json'
    shared_file.write_text(json.dumps({
        'format': SHARED_SOURCES_FORMAT,
        'workspaces': {str(proj.resolve()): {'ext_pkg': str(conflict_src / '__init__.py')}},
    }))
    monkeypatch.setenv(SHARED_SOURCES_ENV, str(shared_file))
    with pytest.raises(ForgeError, match='collid|conflict|duplicate|both|同名|重复'):
        preview(proj, entry='flows/demo.py:run', params='configs/demo.json', authority=auth)


def test_workbench_sealed_run_and_resume_preserve_original_package_bytes(tmp_path, monkeypatch):
    from demiflow.workbench.projects import registry
    from demiflow.workbench.runs import manager as runs
    from demiflow.workbench.runs.authority import WebAuthority
    from test_forge_runs import wait
    from test_forge_resume import stopped

    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    proj = _init_pipeline_project(tmp_path / 'proj')
    ext = _make_pkg(tmp_path / 'live_pkg', {'__init__.py': 'ADD = 100\n'})
    (proj / 'pyproject.toml').write_text(
        '[tool.demiflow.packages]\n'
        f'live_pkg = {{source = "local_ref", path = "{ext}"}}\n',
        encoding='utf-8',
    )
    (proj / 'operators/step.py').write_text(
        'import time\n'
        'from pathlib import Path\n'
        'from live_pkg import ADD\n'
        'def enrich(row):\n'
        '    marker = Path(row["marker"])\n'
        '    if row["x"] == 2 and not marker.exists():\n'
        '        marker.write_text("seen")\n'
        '        time.sleep(30)\n'
        '    return {"x": row["x"] + ADD, "marker": row["marker"]}\n',
        encoding='utf-8',
    )
    (proj / 'flows/pipe.py').write_text(
        'import pyarrow as pa\n'
        'from demiflow import data\n'
        'from operators.step import enrich\n'
        'def run(*, stage, target, marker):\n'
        '    schema = pa.schema([("x", pa.int64()), ("marker", pa.string())])\n'
        '    rows = data.from_items([{"x": 1, "marker": marker}, {"x": 2, "marker": marker}])\n'
        '    return rows.map(enrich).save_lance(stage, schema=schema, key="x", stage="s", mode="append", max_rows=2, max_index_bytes=8388608).write_lance(target, schema=schema, mode="create", return_receipt=True)\n',
        encoding='utf-8',
    )
    marker = str(proj / 'data/pause.flag')
    (proj / 'configs/pipe.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {
            'stage': str(proj / 'data/stage.lance'),
            'target': str(proj / 'data/target.lance'),
            'marker': marker,
        },
    }))
    from _forge_project_fixtures import declare_existing_flow
    declare_existing_flow(proj, 'flows/pipe.py:run', params='configs/pipe.json')

    reg_path = tmp_path / 'console.json'
    created = registry.add(reg_path, proj, allow_runs=True)
    wid = created['workspace_id']
    digest = registry.projects(registry.load_registry(reg_path))['digest']
    registry.set_python(reg_path, wid, expected=digest, python=sys.executable)
    digest = registry.projects(registry.load_registry(reg_path))['digest']
    registry.set_read_roots(reg_path, wid, expected=digest, read_roots=[str(ext)])

    submit_auth = WebAuthority(str(proj.resolve()), wid, str(reg_path.resolve()), 'run_submit', True, True)
    ctrl_auth = WebAuthority(str(proj.resolve()), wid, str(reg_path.resolve()), 'run_control', True, True)

    prepared = runs.prepare(proj, 'flows/pipe.py:run', params='configs/pipe.json',
                            limits={'cancel_grace_s': 1, 'timeout_s': 180}, authority=submit_auth)
    run_id = prepared['run_id']

    # First-start drift check: mutating external package before start MUST fail
    (ext / '__init__.py').write_text('ADD = 200\n', encoding='utf-8')
    with pytest.raises(ForgeError, match='changed|conflict'):
        runs.start(proj, run_id, expected=prepared['digest'], authority=submit_auth)
    # Restore v1 and start attempt 1, then cancel once marker appears
    (ext / '__init__.py').write_text('ADD = 100\n', encoding='utf-8')
    runs.start(proj, run_id, expected=prepared['digest'], authority=submit_auth)
    deadline = time.monotonic() + 30
    while not Path(marker).exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert Path(marker).exists(), runs.logs(proj, run_id)
    runs.cancel(proj, run_id, authority=ctrl_auth)
    first = stopped(proj, run_id)
    assert first['status'] in {'cancelled', 'interrupted'}

    # Now upgrade external package on disk to ADD = 999 AFTER attempt 1 stopped
    (ext / '__init__.py').write_text('ADD = 999\n', encoding='utf-8')

    # Resume MUST ignore upgraded live disk copy and run with sealed original bytes (ADD = 100)!
    resumed = runs.resume(proj, run_id, expected=prepared['digest'], expected_attempt=1, authority=submit_auth)
    final = wait(proj, run_id, timeout=90)
    assert final['status'] == 'succeeded', (final, runs.logs(proj, run_id, attempt=2))
    assert final['attempt'] == 2
    out_rows = sorted(lance.dataset(str(proj / 'data/target.lance')).to_table()['x'].to_pylist())
    assert out_rows == [101, 102]


def test_notebook_old_kernel_keeps_frozen_bytes_while_new_kernel_gets_v2(tmp_path):
    if sys.platform != 'linux' or landlock_abi() < 1:
        pytest.skip('Linux Landlock required')
    from jupyter_client import BlockingKernelClient
    from jupyter_client.connect import write_connection_file
    from queue import Empty
    from demiflow.workbench.notebooks import kernel_launcher

    root = tmp_path / 'nb_proj'
    (root / '.forge').mkdir(parents=True)
    pkg = _make_pkg(tmp_path / 'nb_lib', {'__init__.py': 'VAL = "v1"\n'})
    (root / 'pyproject.toml').write_text(
        '[tool.demiflow.packages]\n'
        f'nb_lib = {{source = "local_ref", path = "{pkg}"}}\n',
        encoding='utf-8',
    )

    def start_kernel(tag: str):
        state = tmp_path / f'state_{tag}'
        runtime = state / 'runtime'
        runtime.mkdir(parents=True)
        cfg = state / 'kernel.json'
        cfg.write_text(json.dumps(dict(
            python=sys.executable, root=str(root), code_root=str(Path(demiflow.__file__).parents[1]),
            runtime_dir=str(runtime), tmp_dir=str(state / 'tmp'), sandbox_mode='readonly',
            locations=[], write_paths=['data'], dependencies_active=True, allowed_read_roots=[str(pkg)],
        )))
        conn = str(runtime / 'connection.json')
        write_connection_file(fname=conn)
        client = BlockingKernelClient(connection_file=conn)
        client.load_connection_file()
        proc = subprocess.Popen(
            [sys.executable, '-IB', kernel_launcher.__file__, str(cfg), conn],
            cwd=root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        client.start_channels()
        req = client.kernel_info()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and proc.poll() is None:
            try:
                reply = client.get_shell_msg(timeout=0.5)
            except Empty:
                continue
            if reply['parent_header'].get('msg_id') == req:
                break
        else:
            raise AssertionError('Kernel did not answer kernel_info')
        return proc, client

    def exec_ok(client, code: str):
        msg = client.execute(code)
        while True:
            reply = client.get_shell_msg(timeout=10)
            if reply['parent_header'].get('msg_id') == msg:
                assert reply['content']['status'] == 'ok', reply['content']
                return

    p1, c1 = start_kernel('k1')
    p2, c2 = None, None
    try:
        # Upgrade external package to v2 while k1 is running
        (pkg / '__init__.py').write_text('VAL = "v2"\n', encoding='utf-8')
        # k1 MUST still see v1
        exec_ok(c1, 'import nb_lib; assert nb_lib.VAL == "v1"')
        # New kernel k2 MUST see v2
        p2, c2 = start_kernel('k2')
        exec_ok(c2, 'import nb_lib; assert nb_lib.VAL == "v2"')
    finally:
        for proc, client in ((p1, c1), (p2, c2)):
            if client is not None:
                client.stop_channels()
            if proc is not None:
                proc.terminate()
                proc.communicate(timeout=7)


# ---------------------------------------------------------------------------
# 7. CPU Regression: trainer (Dataset.fit) & Predictor importing B-class pkg
# ---------------------------------------------------------------------------

def test_cpu_trainer_and_predictor_import_b_class_package_and_read_weights(tmp_path, monkeypatch):
    from demiflow.workbench.projects import registry
    from demiflow.workbench.runs import manager as runs
    from demiflow.workbench.runs.authority import WebAuthority
    from test_forge_runs import wait
    from test_training_domain import setup_request

    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path / 'private-resources'))
    proj = _init_pipeline_project(tmp_path / 'train_proj')
    (proj / 'data').mkdir(exist_ok=True)
    lib = _make_pkg(tmp_path / 'train_codec_src', {
        '__init__.py': (
            'def encode_weight(step: int) -> bytes:\n'
            '    return f"W:{step + 17}".encode("utf-8")\n'
            'def decode_weight(raw: bytes) -> int:\n'
            '    return int(raw.decode("utf-8").split(":", 1)[1])\n'
        ),
    })
    (proj / 'pyproject.toml').write_text(
        '[tool.demiflow.packages]\n'
        'train_codec = {source = "vendored", path = "packages/train_codec"}\n',
        encoding='utf-8',
    )
    deps.vendor(proj, 'train_codec', lib)

    (proj / 'operators/training.py').write_text('''from hashlib import sha256
from pathlib import Path
from demiflow.execution.contracts.training import FitResult, ProjectObject, TrainingSnapshotRef
from train_codec import encode_weight, decode_weight

def managed_trainer(request, context):
    snapshots = []
    for step in request.semantics['save_steps']:
        path = context.model_dir / f'ckpt_{step}.bin'
        payload = encode_weight(step)
        path.write_bytes(payload)
        obj = ProjectObject(str(path), sha256(payload).hexdigest(), len(payload))
        snap = TrainingSnapshotRef(step, 'native.test', {'weights': obj})
        context.report_snapshot(snap)
        snapshots.append(snap)
    return FitResult(request.instance_id, request.fit_key, tuple(snapshots), step)

class WeightPredictor:
    def __init__(self, *, model, inputs, parameters):
        weights_uri = model['snapshot']['files']['weights']['uri']
        raw = Path(weights_uri).read_bytes()
        self.decoded = decode_weight(raw)
    def __call__(self, batch):
        import pyarrow as pa
        return batch.append_column('decoded', pa.array([self.decoded] * len(batch)))
''', encoding='utf-8')

    (proj / 'flows/train.py').write_text('''import pyarrow as pa
from demiflow import data
from operators.training import managed_trainer, WeightPredictor

def run(*, digest, target):
    predictor = data.PredictorSpec(
        factory=WeightPredictor, inputs={'x': 'x'}, input_schema={'x': 'int64'},
        output_schema={'x': 'int64', 'decoded': 'int64'}, batch_format='pyarrow',
    )
    recipe = data.TrainingRecipe(
        trainer=managed_trainer, predictor=predictor,
        train_schema={'x': 'int64'}, base_model_digest=digest, environment_digest=digest,
        model_format='native.test', budget={'optimizer_updates': 200}, save_steps=[100, 200],
        topology={'nodes': 1, 'world_size': 1, 'gpus_per_node': 0},
    )
    fitted = data.from_items([{'x': 1}]).fit(recipe=recipe)
    holdout = data.from_items([{'x': 5}])
    scores = fitted.checkpoints.select(steps=[100, 200]).predict(on=holdout)
    return scores.write_lance(target, mode='create', schema=pa.schema([('x', pa.int64()), ('decoded', pa.int64())]), return_receipt=True)
''', encoding='utf-8')

    _, binding = setup_request(tmp_path, managed=True)
    binding = replace(binding, provider_options={'cpu_millis': 100, 'memory_bytes': 16 << 20})
    deployment = tmp_path / 'deployment.json'
    deployment.write_text(json.dumps(dict(project_id='train_proj', project_root=str(proj), binding=binding.to_dict())))
    monkeypatch.setenv('DEMIFLOW_TRAINING_DEPLOYMENT', str(deployment))

    (proj / 'configs/train.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {'digest': 'a' * 64, 'target': str(proj / 'data/scores.lance')},
    }))
    from _forge_project_fixtures import declare_existing_flow
    declare_existing_flow(proj, 'flows/train.py:run', params='configs/train.json')

    reg_path = tmp_path / 'console.json'
    created = registry.add(reg_path, proj, allow_runs=True)
    wid = created['workspace_id']
    digest = registry.projects(registry.load_registry(reg_path))['digest']
    registry.set_python(reg_path, wid, expected=digest, python=sys.executable)
    auth = WebAuthority(str(proj.resolve()), wid, str(reg_path.resolve()), 'run_submit', True, True)

    prepared = runs.prepare(proj, 'flows/train.py:run', params='configs/train.json', authority=auth)
    assert prepared['status'] == 'prepared'
    monkeypatch.delenv('DEMIFLOW_TRAINING_DEPLOYMENT')
    runs.start(proj, prepared['run_id'], expected=prepared['digest'], authority=auth)
    result = wait(proj, prepared['run_id'], timeout=90)
    assert result['status'] == 'succeeded', (result, runs.logs(proj, prepared['run_id']))
    rows = lance.dataset(str(proj / 'data/scores.lance')).to_table().to_pylist()
    assert sorted((r['x'], r['decoded']) for r in rows) == [(5, 117), (5, 217)]
