"""Admission always checks actual scope, independently of clients and caches."""
import json

import pytest

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from test_forge_runs import base, root, prepared, execute


def scope(root, writes=None):
    return ws.begin_change_scope(root, expected=content_digest(ws.load_workspace(root)[1]),
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
    runs.journal(root, run['run_id']).transition({'prepared'}, 'cancelled',execution_stopped=True,cleanup='complete')
    with pytest.raises(ForgeError, match='outside'):
        runs.resume(root, run['run_id'], expected=run['digest'], expected_attempt=1)
    assert runs.show(root, run['run_id'])['attempt'] == 1


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
