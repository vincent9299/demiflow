"""Small owned experiments exercise actual cancellation and resource boundaries."""
import os
from pathlib import Path
import signal
import time
import pytest

from test_forge_runs import base, root, prepared, custom, execute, wait
from demiflow.workbench.runs import manager as runs


def test_join_expansion_stops_without_truncation(root):
    value = custom(root, '''
def run(*, source, target):
    d = data.read_lance(source['uri'], version=source['version'], columns=['case_id'])
    a = d.union(d, d)
    b = d.union(d, d)
    return a.join(b, on='case_id').write_lance(target['uri'], mode='create', return_receipt=True)
''', limits={'max_output_rows': 10})
    result = execute(root, value)
    assert result['status'] == 'failed', result
    assert not result['outputs'][0]['result']
    assert 'budget' in runs.logs(root, result['run_id'])['text']


@pytest.mark.parametrize('size,needle', [(12000000, 'byte budget'), (4000000000, 'MemoryError')])
def test_single_value_and_allocation_guards(root, size, needle):
    value = custom(root, '''
from operators.custom import expand
def run(*, source, target):
    d = data.read_lance(source['uri'], version=source['version'])
    return d.map(expand, inputs={'text': 'question'}, output='wide').write_lance(target['uri'], mode='create', return_receipt=True)
''', operators=f'''def expand(text: str | None) -> str:
    return 'x' * {size}
''', limits={'max_row_bytes': 4 * 1024**2, 'max_batch_bytes': 4 * 1024**2})
    result = execute(root, value)
    assert result['status'] == 'failed', result
    assert not result['outputs'][0]['result']
    assert needle in runs.logs(root, result['run_id'])['text']


def test_scratch_guard_and_owned_cleanup(root):
    value = prepared(root, limits={'max_scratch_bytes': 1, 'cancel_grace_s': 1})
    result = execute(root, value)
    assert result['status'] == 'failed', result
    assert result['state'].get('reason') == 'scratch_budget' or 'scratch' in runs.logs(root, result['run_id'])['text'].lower()
    deadline = time.monotonic() + 5
    work = runs.attempt_directory(runs.directory(root, result['run_id']), 'work', result['attempt'])
    while work.exists() and time.monotonic() < deadline:
        time.sleep(.05)
    assert not work.exists()
    assert (root / 'data/candidates.lance').exists()


def _slow(root):
    return custom(root, '''
from operators.custom import slow
def run(*, source, target, other):
    d = data.read_lance(source['uri'], version=source['version'])
    first = d.write_lance(target['uri'], mode='create', return_receipt=True)
    second = d.map(slow, inputs={'text': 'question'}, output='waiting').write_lance(other['uri'], mode='create', return_receipt=True)
    return first, second
''', operators='''def slow(text: str | None) -> str | None:
    n = 0
    for i in range(2000000000):
        n = n + i
    return text
''', arguments={'source': {'kind': 'table', 'alias': 'candidates'},
               'target': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'first.lance'},
               'other': {'kind': 'output', 'location_id': 'local', 'relative_uri': 'slow.lance'}},
        limits={'timeout_s': 120, 'cancel_grace_s': 1})


def _first_committed(root, value):
    runs.start(root, value['run_id'], expected=value['digest'])
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        result = runs.show(root, value['run_id'])
        if any(o['result'] for o in result['outputs']):
            return result
        assert result['status'] not in runs.TERMINAL, (result, runs.logs(root, value['run_id']))
        time.sleep(.1)
    raise AssertionError('First independent output did not commit')


def test_partial_commit_cancel_and_workspace_admission(root):
    first = _slow(root)
    second = prepared(root)
    try:
        result = _first_committed(root, first)
        with pytest.raises(ValueError, match='owns|active'):
            runs.start(root, second['run_id'], expected=second['digest'])
        assert runs.start(root, first['run_id'], expected=first['digest'])['run_id'] == first['run_id']
    finally:
        runs.cancel(root, first['run_id'])
    result = wait(root, first['run_id'])
    assert result['status'] == 'cancelled', result
    assert sum(o['result'] is not None for o in result['outputs']) == 1
    assert not runs.process_matches(result['state']['worker'])


def test_supervisor_loss_stops_owned_worker_and_requires_explicit_reconcile(root):
    first = _slow(root)
    result = _first_committed(root, first)
    identity = result['state']['supervisor']
    assert runs.process_matches(identity)
    os.kill(identity['pid'], signal.SIGKILL)
    deadline = time.monotonic() + 10
    while runs.process_matches(result['state']['worker']) and time.monotonic() < deadline:
        time.sleep(.1)
    assert not runs.process_matches(result['state']['worker'])
    current = runs.show(root, first['run_id'])
    assert current['status'] in runs.ACTIVE and current['supervisor_alive'] is False
    recovered = runs.reconcile(root, first['run_id'])
    assert recovered['status'] == 'interrupted'
    assert sum(o['result'] is not None for o in recovered['outputs']) == 1
