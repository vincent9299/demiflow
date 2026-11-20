"""Admission always checks actual scope, independently of clients and caches."""
import json
from pathlib import Path

import pytest

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.runs import manager as runs
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from test_forge_runs import base, root, prepared, execute


def scope(root, writes=None):
    return project_ops.begin_change_scope(root, expected=content_digest(ws.load_workspace(root)[1]),
                                 writes=writes or [])['audit']['change_scope']


def test_out_of_scope_prepare_rejected_even_with_fake_green_cache(root):
    scope(root)
    (root / 'operators/unexpected.py').write_text('# outside')
    (root / '.forge/audit/status.json').write_text(json.dumps({'status': 'checked'}))
    with pytest.raises(ForgeError, match='outside'):
        prepared(root)
    assert runs.list_runs(root)['total'] == 0


def test_start_rechecks_scope_and_cancel_remains_available(root):
    scope(root)
    run = prepared(root)
    (root / 'operators/unexpected.py').write_text('# outside')
    with pytest.raises(ForgeError, match='outside'):
        runs.start(root, run['run_id'], expected=run['digest'], expected_attempt=1)
    assert runs.show(root, run['run_id'])['status'] == 'prepared'
    # Existing cancel only affects active execution; a merely prepared run is
    # left unchanged. Model a stopped attempt to exercise the resume gate.
    assert runs.cancel(root, run['run_id'], expected_attempt=1)['status'] == 'prepared'
    (runs.directory(root,run['run_id'])/'unit-1.lock').touch()
    control = runs.journal(root, run['run_id'])
    control.transition({'prepared'}, 'cancelled',execution_stopped=True,cleanup='complete')
    # This control-only fixture has never executed. Public resume must retain
    # the earlier history gate; it cannot invent an engine recovery contract.
    assert control.recovery_identity() is None
    before = control.execution_status()
    with pytest.raises(ForgeError, match='没有统一恢复契约'):
        runs.resume(root, run['run_id'], expected=run['digest'], expected_attempt=1)
    from demiflow.execution.recovery_contract import RecoveryContractError
    with pytest.raises(RecoveryContractError, match='Unsupported or missing runtime identity format'):
        runs._resume(root, run['run_id'], expected=run['digest'], expected_attempt=1)
    assert control.execution_status() == before
    assert control.recovery_identity() is None
    # Supply a legitimate protocol for the scope-specific control fixture. This
    # claim/release constructs no native execution, artifact or successful run.
    import time
    from demiflow.execution.runtime_limits import execution_limits
    configuration = dict(format=1, stream_protocol='aligned-barrier-v2', run_id='4'*32,
                         entry_identity=None, started_at=time.time(), limits_explicit=[],
                         platform_revision='original-platform', limits=execution_limits(None))
    control.claim_execution(configuration, 'f'+'1'*32)
    control.release_execution('f'+'1'*32)
    # The valid protocol must expose the original business scope rejection.
    with pytest.raises(ForgeError, match='outside'):
        runs._resume(root, run['run_id'], expected=run['digest'], expected_attempt=1)
    assert control.execution_status() == before
    assert control.recovery_identity() == configuration
    assert runs.show(root, run['run_id'])['attempt'] == 1
    assert len(control.list_attempts()) == 1
    assert all(not control.execution_commits(output['action_id']) for output in run['outputs'])
    assert all(not Path(output['uri']).exists() for output in run['outputs'])


def test_scope_change_requires_new_preparation_and_baseline_is_sealed(root):
    original = scope(root)
    run = prepared(root)
    target, value = runs._load(root, run['run_id'])
    assert value['manifest']['format'] == 'demiflow_forge_run_manifest_v5'
    assert value['manifest']['change_scope'] == original
    assert ws.read_control(target / 'change-baseline.json')['digest'] == original['baseline']
    ws.end_change_scope(root, expected=content_digest(ws.load_workspace(root)[1]), reason='explicit end')
    with pytest.raises(ForgeError, match='scope differs'):
        runs.start(root, run['run_id'], expected=run['digest'])
    runs.verify_seal(target, value['manifest'])
    legacy = dict(value['manifest'], format=runs.LEGACY_EXECUTABLE_FORMAT)
    with pytest.raises(ForgeError, match='contract changed'):
        runs._check_basis(root, target, legacy)


def test_allowed_edit_fresh_prepare_executes_native_output(root):
    original = scope(root, [dict(path='operators/question_rules.py', kind='file')])
    path = root / 'operators/question_rules.py'
    path.write_text(path.read_text() + '\n# permitted development edit\n')
    result = execute(root, prepared(root))
    assert result['status'] == 'succeeded', result
    assert ws.load_workspace(root)[1]['audit']['change_scope'] == original
    assert result['outputs'][0]['receipt']['written_rows'] == 2


def test_missing_scope_baseline_rejects_start_but_not_history(root):
    original = scope(root)
    run = prepared(root)
    (root / '.forge/audit/baselines' / (original['baseline'] + '.json')).unlink()
    with pytest.raises(ForgeError):
        runs.start(root, run['run_id'], expected=run['digest'])
    assert runs.show(root, run['run_id'])['status'] == 'prepared'
    assert runs.cancel(root, run['run_id'])['status'] == 'prepared'
