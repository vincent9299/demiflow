"""Actual app entrypoints share facts and rules without reverse imports."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest
from click.testing import CliRunner

from demiflow.forge.bridge import run_tools as tools
from demiflow.forge.schema import ForgeError
from demiflow.workbench.runs import context
from test_forge_runtime_review_fixes import control_fixture


def test_forge_imports_without_product_or_interactive_runtime():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, '-I', '-B', '-c',
        'import sys;sys.path.insert(0,sys.argv[1]);import demiflow.forge.cli;import demiflow.forge.api_reference;import demiflow.forge.pipeline_check;'
        'assert not any(n.startswith(("demiflow.workbench", "jupyter", "ipykernel")) for n in sys.modules)', str(root)],
        capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr


def test_forge_guidance_and_check_use_the_same_public_catalogue(tmp_path):
    from demiflow.forge.api_reference import build_reference, render_reference
    from demiflow.forge.pipeline_check import check_project

    flows = tmp_path / 'flows'
    flows.mkdir()
    source = flows / 'example.py'
    source.write_text("from demiflow.data import from_items\ndef example():\n    return from_items([{'id': 1}]).materialize()\n")
    before = source.read_bytes()
    reference = build_reference()
    text, symbols = render_reference(reference)
    assert 'demiflow.data:Dataset.materialize' in symbols
    assert 'Dataset.materialize' in text
    inspected = check_project(tmp_path, paths=['flows/example.py'], reference=reference)
    assert inspected['catalog_id'] == reference['catalog_id']
    assert inspected['native_flow']['status'] == 'passed', inspected
    assert inspected['project_layout']['status'] == 'passed'
    assert list(tmp_path.iterdir()) == [flows] and source.read_bytes() == before


def test_run_context_is_project_read_only_and_does_not_reconcile(tmp_path, monkeypatch):
    from demiflow.workbench.runs import manager
    manifest, target, journal = control_fixture(tmp_path)
    journal.transition({'prepared'}, 'starting', launch_phase='spawning', execution_stopped=False)
    def forbidden(*a, **kw): raise AssertionError('Read must not reconcile or initialize')
    monkeypatch.setattr(manager, 'reconcile', forbidden)
    before = hashlib.sha256((target / 'run.sqlite').read_bytes()).hexdigest()
    result = context.query(tmp_path, manifest['run_id'])
    value = result['facts'][0]['value']
    assert value['state']['status'] == 'starting' and value['state']['launch_phase'] == 'spawning'
    assert value['attempt'] == 1 and value['seal_digest'] == manifest['digest']
    assert hashlib.sha256((target / 'run.sqlite').read_bytes()).hexdigest() == before
    assert not (tmp_path / '.forge/index.sqlite').exists()
    with pytest.raises(ForgeError): context.query(tmp_path, manifest['run_id'], attempt=True)
    with pytest.raises(ForgeError): tools.invoke('run.start', tmp_path, {'run_id':manifest['run_id'],'expected':manifest['digest']})


def test_run_context_rechecks_access(tmp_path):
    manifest, target, journal = control_fixture(tmp_path)
    class Denied:
        def require(self, root, permission): raise PermissionError('revoked')
    with pytest.raises(PermissionError, match='revoked'):
        context.query(tmp_path, manifest['run_id'], authority=Denied())


def test_registry_authority_rechecks_current_grant_and_project(tmp_path, monkeypatch):
    from demiflow.workbench.projects import registry, workspace
    from demiflow.workbench.runs.authority import WebAuthority, from_bootstrap
    from demiflow.forge.platform import content_digest
    identity = 'w' + '1' * 32
    project = tmp_path / 'project'; project.mkdir()
    path = tmp_path / 'registry.json'
    value = dict(format=registry.FORMAT, projects={identity: dict(root=str(project), name='project', allow_runs=True)})
    path.write_text(json.dumps(value))
    monkeypatch.setattr(workspace, 'load_workspace', lambda root: (root, {'id':identity}, {}))
    authority = WebAuthority(str(project), identity, str(path), 'run_submit', True, True)
    authority.require(project, 'run_submit')
    registry.set_permissions(path, identity, expected=content_digest(value), run_submit=False, run_control=False, attach=False)
    with pytest.raises(PermissionError):
        with authority.guard(project, 'run_submit'): raise AssertionError('revoked grant committed')
    authority.require(project, 'read')
    with pytest.raises(PermissionError): authority.require(tmp_path, 'read')
    with pytest.raises(ForgeError): from_bootstrap({**authority.bootstrap(), 'extra':True}, project)


def test_forge_zero_workbench_imports_ast():
    import ast
    repo = Path(__file__).resolve().parents[1]
    violations = []
    for path in sorted((repo / 'demiflow' / 'forge').rglob('*.py')):
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        rel = path.relative_to(repo).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == 'demiflow.workbench' or alias.name.startswith('demiflow.workbench.'):
                        violations.append(f'{rel}:{node.lineno}: import {alias.name}')
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module == 'demiflow.workbench' or node.module.startswith('demiflow.workbench.'):
                    violations.append(f'{rel}:{node.lineno}: from {node.module} import ...')
    assert not violations, 'Forge must never import demiflow.workbench:\n' + '\n'.join(violations)


def test_workbench_zero_agent_tool_or_notebook_cli_contracts_ast():
    import ast
    repo = Path(__file__).resolve().parents[1]
    forbidden_names = {'INPUT_TOOL', 'CATALOG_TOOL', 'PROJECT_LAYOUT_TOOL', 'REVIEW_TOOL'}
    violations = []
    for path in sorted((repo / 'demiflow' / 'workbench').rglob('*.py')):
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        rel = path.relative_to(repo).as_posix()
        for node in ast.walk(tree):
            # Reject defining Agent MCP tool schemas in Workbench
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in forbidden_names:
                        violations.append(f'{rel}:{node.lineno}: defines {target.id}')
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == 'inputSchema':
                        violations.append(f'{rel}:{node.lineno}: defines Agent tool inputSchema')
                    if kw.arg == 'input_schema' and any(
                        isinstance(k.value, ast.BinOp) for k in node.keywords if k.arg == 'name'
                    ):
                        violations.append(f'{rel}:{node.lineno}: defines run.* tool input_schema')
                if isinstance(node.func, ast.Attribute) and node.func.attr == 'ArgumentParser':
                    for kw in node.keywords:
                        if kw.arg == 'prog' and isinstance(kw.value, ast.Constant) and kw.value.value == 'data':
                            violations.append(f'{rel}:{node.lineno}: defines Notebook assistant data CLI')
            if isinstance(node, ast.Dict):
                for key in node.keys:
                    if isinstance(key, ast.Constant) and key.value == 'inputSchema':
                        violations.append(f'{rel}:{node.lineno}: defines Agent tool inputSchema dict')
    assert not violations, 'Workbench must not define Agent tool or Notebook assistant CLI contracts:\n' + '\n'.join(violations)


def test_forge_bridge_and_runtime_import_with_workbench_blocked():
    repo = Path(__file__).resolve().parents[1]
    script = (
        'import importlib.abc, sys\n'
        'sys.path.insert(0, sys.argv[1])\n'
        'class BlockWB(importlib.abc.MetaPathFinder):\n'
        '    def find_spec(self, fullname, *a):\n'
        '        if fullname == "demiflow.workbench" or fullname.startswith("demiflow.workbench."):\n'
        '            raise ImportError("blocked: " + fullname)\n'
        'sys.meta_path.insert(0, BlockWB())\n'
        'import demiflow.forge.bridge\n'
        'import demiflow.forge.sandbox\n'
        'import demiflow.forge.runtime\n'
        'import demiflow.forge.dependencies\n'
        'from demiflow.forge.bridge.run_tools import describe\n'
        'assert len(describe()) == 7\n'
        'assert not any(m == "demiflow.workbench" or m.startswith("demiflow.workbench.") for m in sys.modules)\n'
    )
    proc = subprocess.run([sys.executable, '-I', '-B', '-c', script, str(repo)], capture_output=True, text=True, timeout=20)
    assert proc.returncode == 0, proc.stderr

