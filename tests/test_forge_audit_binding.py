"""Pinned reports and task baselines are independent, versioned authorities."""
import json

import pytest

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.runs import manager as runs

from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from test_forge_runs import base, root, prepared, wait


def pin(root, params='flows/select_a.json'):
    report = project_ops.inspect_flow(root, 'flows/questions.py:select', params=params)
    assert report['status'] == 'checked'
    project_ops.bind_audit_report(root, report['digest'], expected=content_digest(ws.load_workspace(root)[1]))
    return report


def test_report_tamper_without_index_revision_and_explicit_unbind(root):
    report = pin(root)
    before = project_ops.workspace_changes(root)['reports']
    path = root / '.forge/inspections' / (report['digest'] + '.json')
    modified = dict(report, status='invalid')
    path.write_text(json.dumps(modified))
    assert project_ops.workspace_changes(root)['reports'] == before
    result = project_ops.audit_check(root, kind='report', report_digest=report['digest'],
                            expected_policy=ws.audit_status(root)['policy_digest'])
    assert result['result']['status'] == 'unverifiable'
    ws.unbind_audit_report(root, report['digest'], expected=content_digest(ws.load_workspace(root)[1]))
    assert path.exists() and ws.audit_status(root)['coverage']['tracked'] == 0


def test_reinspection_does_not_erase_scope_violation(root):
    pin(root)
    project_ops.begin_change_scope(root, writes=[], expected=content_digest(ws.load_workspace(root)[1]))
    original = ws.load_workspace(root)[1]['audit']['change_scope']
    path = root / 'operators/question_rules.py'
    path.write_text(path.read_text() + '\n# outside declared writes\n')
    report = pin(root)
    policy = ws.audit_status(root)['policy_digest']
    assert project_ops.audit_check(root, kind='report', report_digest=report['digest'], expected_policy=policy)['result']['status'] == 'checked'
    assert project_ops.audit_check(root, kind='scope', expected_policy=policy)['result']['status'] == 'violated'
    assert ws.load_workspace(root)[1]['audit']['change_scope'] == original
    with pytest.raises(ForgeError, match='outside'):
        prepared(root)


def test_report_selection_does_not_invalidate_prepared_run_and_preflight_holds_locks(root, monkeypatch):
    project_ops.begin_change_scope(root, writes=[], expected=content_digest(ws.load_workspace(root)[1]))
    run = prepared(root)
    pin(root, 'flows/select_b.json')
    from demiflow.workbench import worker as workspace_worker
    operation = workspace_worker.operation
    observed = []
    def verify_locks(name, target, **arguments):
        if name == 'run_preflight':
            with pytest.raises(ForgeError):
                with ws.control_lock(root):
                    pass
            with pytest.raises(ForgeError):
                with runs.execution_lock(root):
                    pass
            observed.append(name)
        return operation(name, target, **arguments)
    monkeypatch.setattr(workspace_worker, 'operation', verify_locks)
    runs.start(root, run['run_id'], expected=run['digest'])
    assert wait(root, run['run_id'])['status'] == 'succeeded'
    assert observed == ['run_preflight']
    assert ws._check_change_scope(root)['status'] == 'within_scope'


def test_v2_upgrade_is_explicit_and_inline_reports_cannot_be_pinned(root):
    path = root / 'workspace.json'
    control = ws.load_workspace(root)[1]
    control['format'] = ws.FORMAT_V2
    del control['audit']
    path.write_text(json.dumps(control))
    assert not ws.audit_status(root)['enabled']
    with pytest.raises(ForgeError, match='enable'):
        project_ops.begin_change_scope(root, writes=[], expected=content_digest(control))
    ws.enable_audit(root, expected=content_digest(control))
    report = project_ops.inspect_flow(root, 'flows/questions.py:select', params=ws.read_parameters(root, 'flows/select_a.json'))
    assert report['status'] == 'checked'
    with pytest.raises(ForgeError, match='registered'):
        project_ops.bind_audit_report(root, report['digest'], expected=content_digest(ws.load_workspace(root)[1]))


def test_inspect_track_control_conflict_keeps_report_and_existing_pins(root):
    from click.testing import CliRunner
    from demiflow.cli import main
    before = ws.load_workspace(root)[1]['audit']
    result = CliRunner().invoke(main, ['workbench', 'flow', 'inspect', str(root),
        'flows/questions.py:select', '--params', 'flows/select_a.json',
        '--track', '--expected-workspace', '0'*64, '--json'])
    value = json.loads(result.output)
    assert value['status'] == 'report_saved_binding_failed'
    assert value['error']['code'] == 'conflict'
    assert ws.load_report(root, value['report']['digest']) == value['report']
    assert ws.load_workspace(root)[1]['audit'] == before
