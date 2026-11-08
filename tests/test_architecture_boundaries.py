"""Actual app entrypoints share facts and rules without reverse imports."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest
from click.testing import CliRunner

from demiflow.forge.schema import ForgeError
from demiflow.forge.tools import invoke
from demiflow.workbench.runs import context, tools
from test_forge_runtime_review_fixes import control_fixture
from forge_support import create_project


def test_forge_imports_without_product_or_interactive_runtime():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, '-I', '-B', '-c',
        'import sys;sys.path.insert(0,sys.argv[1]);import demiflow.forge.cli;import demiflow.forge.context;'
        'assert not any(n.startswith(("demiflow.workbench", "jupyter", "ipykernel")) for n in sys.modules)', str(root)],
        capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr


def test_forge_facts_do_not_create_task_or_receipt_and_closed_fields_fail(tmp_path):
    (tmp_path / 'contract.lock.json').write_text('{"schema_version":"saved","rules":["original"]}')
    before = {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    facts = invoke('forge.context', tmp_path)
    assert facts['read_only'] and facts['current_evidence'] == 'unverified'
    assert facts['facts'][0]['source'] == str(tmp_path / 'contract.lock.json')
    assert facts['facts'][0]['version'] and facts['facts'][0]['observed_at']
    assert {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()} == before
    with pytest.raises(ForgeError): invoke('forge.context', tmp_path, {'run': True})
    with pytest.raises(ValueError): invoke('forge.context', tmp_path, {'task': '../escape'})


def test_forge_cli_and_tool_use_same_current_checker(tmp_path):
    from demiflow.cli import main
    project = create_project(tmp_path)
    direct = invoke('forge.check', project)
    result = CliRunner().invoke(main, ['forge', 'check', str(project), '--json'])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == direct
    # Actual invalid source gets the same failure, not a separate tool policy.
    (project / 'pipeline.py').write_text('raise RuntimeError("must not execute")\n')
    direct = invoke('forge.check', project)
    result = CliRunner().invoke(main, ['forge', 'check', str(project), '--json'])
    assert result.exit_code == 1 and json.loads(result.output) == direct


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
