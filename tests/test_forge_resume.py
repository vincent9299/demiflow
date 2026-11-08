"""Resume the same sealed Python Flow; retain facts and cumulative budgets."""
import time

import lance
import pytest

from demiflow.workbench.runs import manager as runs
from test_forge_runs import base, root, prepared, custom, execute, wait
from test_forge_run_faults import _slow, _first_committed


def stopped(root, run_id):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        value = runs.show(root, run_id)
        if value['status'] in runs.TERMINAL and not any(
                runs.process_matches(value['state'].get(name)) for name in ('supervisor', 'worker')):
            return value
        time.sleep(.05)
    raise AssertionError('Owned run processes did not stop')


def test_cancel_resume_reuses_owned_commit_and_retains_attempt_history(root):
    first = _slow(root)
    active = _first_committed(root, first)
    runs.cancel(root, first['run_id'])
    previous = stopped(root, first['run_id'])
    assert previous['state']['usage_complete'] is True
    committed = next(o for o in previous['outputs'] if o['result'])
    version = committed['receipt']['committed_version']
    try:
        resumed = runs.resume(root, first['run_id'], expected=first['digest'], expected_attempt=1)
        assert resumed['attempt'] == 2
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            resumed = runs.show(root, first['run_id'])
            if resumed['actions'].get(committed['action_id'], {}).get('receipt'):
                break
            assert resumed['status'] not in runs.TERMINAL, (resumed, runs.logs(root, first['run_id']))
            time.sleep(.1)
        assert resumed['actions'][committed['action_id']]['intent']['reused_from_attempt'] == 1
        assert lance.dataset(committed['uri']).version == version
        assert runs.show(root, first['run_id'], attempt=1) == previous
        with pytest.raises(ValueError, match='attempt changed|owns this workspace'):
            runs.resume(root, first['run_id'], expected=first['digest'], expected_attempt=1)
    finally:
        runs.cancel(root, first['run_id'])
        current = stopped(root, first['run_id'])
    assert current['status'] == 'cancelled'
    assert current['state']['usage']['elapsed_ms'] > previous['state']['usage']['elapsed_ms']
    assert current['state']['usage']['output_bytes'] >= previous['state']['usage']['output_bytes']
    assert current['state']['usage_complete'] is True
    assert runs.show(root, first['run_id'], attempt=1) == previous


@pytest.mark.parametrize('failure,needle', [('unknown', 'accounting'), ('exhausted', 'exhausted'), ('changed', 'stale')])
def test_resume_never_grants_unknown_reset_or_changed_binding(root, failure, needle):
    value = prepared(root)
    control = runs.journal(root, value['run_id'])
    # Fault fixture models a positively stopped execution under runtime v1.
    (runs.directory(root,value['run_id'])/'unit-1.lock').touch()
    control.transition({'prepared'}, 'interrupted',execution_stopped=True,cleanup='complete')
    if failure == 'unknown':
        control.record_usage(elapsed_ms=100, output_bytes=0)
    elif failure == 'exhausted':
        control.record_usage(elapsed_ms=value['limits']['timeout_s'] * 1000, output_bytes=0, complete=True)
    else:
        path = root / 'operators/question_rules.py'
        path.write_text(path.read_text() + '\n# changed binding\n')
    with pytest.raises(ValueError, match=needle):
        runs.resume(root, value['run_id'], expected=value['digest'], expected_attempt=1)
    assert runs.show(root, value['run_id'])['attempt'] == 1


def test_failed_business_guard_is_not_a_technical_retry(root):
    value = custom(root, '''
def run(*, source, target):
    rows = data.read_lance(source['uri'], version=source['version'])
    data.require(rows.count() > 100, 'business coverage incomplete')
    return rows.write_lance(target['uri'], mode='create', return_receipt=True)
''')
    result = execute(root, value)
    assert result['status'] == 'failed'
    stopped(root, value['run_id'])
    with pytest.raises(ValueError, match='technical failures'):
        runs.resume(root, value['run_id'], expected=value['digest'], expected_attempt=1)
