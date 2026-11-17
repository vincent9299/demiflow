"""Cache result sharing cannot impersonate this owner's acknowledged state."""
import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from demiflow.data.result_cache import CachedSyncMap
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.kv import SQLiteKV


class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, value):
        self.count += 1
        return self.count


def cache(root, owner='a', counter=None, **kwargs):
    actor = CachedSyncMap(counter or Counter(), root, 'v1', inputs=['value'], output='count', **kwargs)
    actor.bind_checkpoint_cache(hashlib.sha256(owner.encode()).hexdigest())
    return actor


def test_native_frontier_replays_only_own_unacknowledged_state(tmp_path):
    first = cache(tmp_path)
    assert first({'value': 0})['count'] == 1
    image = asyncio.run(first.snapshot_state())
    assert first({'value': 1})['count'] == 2
    assert first({'value': 2})['count'] == 3
    # Other owners share the exact completed result without advancing state.
    other = cache(tmp_path, 'other-run-or-partition')
    assert other({'value': 2})['count'] == 3
    assert other.fn.count == 0
    resumed = cache(tmp_path)
    asyncio.run(resumed.restore_state(image))
    assert resumed({'value': 0})['count'] == 1
    assert resumed.fn.count == 1
    assert resumed({'value': 1})['count'] == 2
    assert resumed({'value': 1})['count'] == 2  # once per actual miss, including replay
    assert resumed.fn.count == 2
    assert resumed({'value': 2})['count'] == 3
    assert resumed({'value': 3})['count'] == 4
    assert first.prepare({'value': 2}) == resumed.prepare({'value': 2})


@pytest.mark.parametrize('failure', ['different', 'exception'])
def test_replay_never_replaces_a_shared_result_or_error_diagnostic(tmp_path, failure):
    first = cache(tmp_path)
    image = asyncio.run(first.snapshot_state())
    assert first({'value': 1})['count'] == 1
    _, key = first.prepare({'value': 1})
    before = first.store.get(key, _with_origin=True)
    error = tmp_path / (key[:3] + '.error.json')
    error.write_text('prior diagnostic')
    resumed = cache(tmp_path)
    asyncio.run(resumed.restore_state(image))
    if failure == 'different':
        resumed.fn.count = 10
        expected = DataflowError
    else:
        def rejected(value):
            raise ValueError('state replay rejected')
        resumed.fn = rejected
        expected = ValueError
    with pytest.raises(expected):
        resumed({'value': 1})
    assert first.store.get(key, _with_origin=True) == before
    assert error.read_text() == 'prior diagnostic'


def test_old_stateful_image_rejected_and_old_stateless_result_readable(tmp_path):
    actor = cache(tmp_path)
    image = asyncio.run(actor.snapshot_state())
    del image['cache_frontier']
    with pytest.raises(DataflowError) as failure:
        asyncio.run(cache(tmp_path).restore_state(image))
    assert failure.value.code == 'INCOMPATIBLE_HISTORY'
    plain = CachedSyncMap(lambda value: value + 10, tmp_path / 'plain', 'v1', inputs=['value'], output='count')
    plain.bind_checkpoint_cache('b' * 64)
    plain_image = asyncio.run(plain.snapshot_state())
    assert 'cache_frontier' not in plain_image
    assert plain({'value': 0})['count'] == 10
    asyncio.run(plain.restore_state(plain_image))
    assert plain({'value': 0})['count'] == 10
    assert 'origin' not in plain.store.get(plain.prepare({'value': 0})[1])


def test_origin_and_value_share_capacity_transaction_and_plain_writes_keep_origin(tmp_path):
    store = SQLiteKV(tmp_path / 'cache.sqlite', 'n', max_value_bytes=128, max_entries=1, max_bytes=128)
    first = json.dumps({'owner': 'first', 'generation': 1})
    replayed = json.dumps({'owner': 'first', 'generation': 2})
    store.put_many([('a', 1)], _origin=first)
    with pytest.raises(ValueError, match='capacity'):
        store.put_many([('b', 2)], _origin='rejected')
    assert store.get('b', _with_origin=True) == {'hit': False, 'value': None}
    assert store.get('a', _with_origin=True) == {'hit': True, 'value': 1, 'origin': first}
    with pytest.raises(ValueError, match='changed'):
        store.put_many([('a', 2)], _origin=replayed, _origin_only=True)
    assert store.get('a', _with_origin=True) == {'hit': True, 'value': 1, 'origin': first}
    store.put_many([('a', 1)], _origin=replayed, _origin_only=True)
    assert store.get('a', _with_origin=True)['origin'] == replayed
    with pytest.raises(ValueError, match='provenance'):
        store.put_many([('a', 3)])
    store.put_many([('a', 1)])
    assert store.get('a', _with_origin=True) == {'hit': True, 'value': 1, 'origin': replayed}


def test_stateful_rejected_cache_input_cannot_be_silently_skipped_on_replay(tmp_path):
    class Selection:
        def __init__(self):
            self.calls = 0
        def __call__(self, value):
            self.calls += 1
            return value != 99
    selection = Selection()
    first = cache(tmp_path, cache_when=selection)
    _, key = first.prepare({'value': 1})
    first.store.put_many([(key, 99)])
    image = asyncio.run(first.snapshot_state())
    assert first({'value': 1})['count'] == 1
    assert selection.calls == 2  # rejected cached input, then accepted computation
    completed = first.store.get(key, _with_origin=True)
    resumed = cache(tmp_path, cache_when=Selection())
    asyncio.run(resumed.restore_state(image))
    with pytest.raises(DataflowError, match='rejected predicate input'):
        resumed({'value': 1})
    assert first.store.get(key, _with_origin=True) == completed
    assert resumed.fn.count == resumed.cache_when.calls == 0


def test_cache_creator_metadata_obeys_original_byte_capacity(tmp_path):
    store = SQLiteKV(tmp_path / 'cache.sqlite', 'n', max_value_bytes=128, max_bytes=128)
    with pytest.raises(ValueError, match='capacity'):
        store.put_many([('a', 1)], _origin='x' * 128)
    assert store.get('a') == {'hit': False, 'value': None}
    store.put_many([('a', 1)], _origin='x' * 127)
    with pytest.raises(ValueError, match='capacity'):
        store.put_many([('b', 2)])
    store.put_many([('a', 1)])  # an identical write cannot discard provenance/its charge
    with pytest.raises(ValueError, match='capacity'):
        store.put_many([('b', 2)])
    assert store.get('a', _with_origin=True) == {'hit': True, 'value': 1, 'origin': 'x' * 127}
    plain = SQLiteKV(tmp_path / 'plain.sqlite', 'n', max_value_bytes=128, max_bytes=128)
    plain.put_many([('a', 1)])
    plain.put_many([('a', 3), ('b', 2)])
    assert plain.get('a') == {'hit': True, 'value': 3}


def test_native_kv_sink_keeps_creator_metadata_and_refuses_replacement(tmp_path):
    import sqlite3
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    store = SQLiteKV(tmp_path / 'index.sqlite', 'n', max_value_bytes=128, max_bytes=128)
    store.put_many([('a', 1)], _origin='x' * 126)
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        data.from_items([{'key': 'a', 'value': 1}, {'key': 'b', 'value': 3}]).write_datasink(store.datasink())
    assert store.get('a', _with_origin=True) == {'hit': True, 'value': 1, 'origin': 'x' * 126}
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT entries,bytes FROM kv_usage WHERE namespace=?', ('n',)).fetchone() == (2, 128)
    with pytest.raises(ValueError, match='provenance'):
        with RunContext.start(tmp_path / 'rejected'), data.local_execution():
            data.from_items([{'key': 'a', 'value': 22}]).write_datasink(store.datasink())
    assert store.get('a', _with_origin=True) == {'hit': True, 'value': 1, 'origin': 'x' * 126}


def test_other_owner_cannot_erase_post_cut_creator_state_evidence(tmp_path):
    first = cache(tmp_path)
    image = asyncio.run(first.snapshot_state())
    assert first({'value': 1})['count'] == 1
    _, key = first.prepare({'value': 1})
    before = first.store.get(key, _with_origin=True)
    other = cache(tmp_path, 'another-owner', cache_when=lambda value: False)
    with pytest.raises(DataflowError, match='another state owner'):
        other({'value': 1})
    assert other.fn.count == 0
    with pytest.raises(ValueError, match='provenance'):
        first.store.put_many([(key, 99)])
    first.store.put_many([(key, 1)])
    assert first.store.get(key, _with_origin=True) == before
    resumed = cache(tmp_path)
    asyncio.run(resumed.restore_state(image))
    assert resumed({'value': 1})['count'] == 1
    assert resumed({'value': 2})['count'] == 2


def test_actual_task_lifetimes_do_not_replay_other_tasks_cache_hits(tmp_path):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    # Each two-input task owns a fresh counter. The second task hits a result
    # made by task 0, which must not count as an invocation on the fresh owner.
    with RunContext.start(tmp_path / 'run'), data.local_execution(batch_rows=2):
        rows = data.from_items([{'value': n} for n in (0, 1, 1, 2, 2, 3)]).map_cached(
            Counter(), cache_dir=tmp_path / 'cache', version='v1', synchronous=True,
            inputs=['value'], output='count').take_all()
    assert [row['count'] for row in rows] == [1, 2, 2, 1, 1, 1]


CRASH_DRIVER = r'''
import json, os, sqlite3, sys, time
from pathlib import Path
import demiflow
from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RunRef
root=Path(sys.argv[1]); mode=sys.argv[2]
assert Path(demiflow.__file__).resolve().is_relative_to(Path.cwd())
class Counter:
    def __init__(self): self.count=0
    def __call__(self, value):
        self.count+=1
        return self.count
pump=DataflowExecution.pump
def crash_after_original_ack(execution, **kwargs):
    result=pump(execution, **kwargs)
    if execution._restore_image is not None and not execution._restore_image.cut.final:
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            with sqlite3.connect(root/'cache/results.sqlite') as db:
                count=db.execute('SELECT COUNT(*) FROM kv').fetchone()[0]
            if count>=4:
                (root/'crashed.json').write_text(json.dumps({'cache_entries':count, 'cut':str(execution._restore_image.cut)}))
                os._exit(79)
            time.sleep(.01)
        raise AssertionError('suffix cache write did not reach process crash boundary')
    return result
if mode=='crash': DataflowExecution.pump=crash_after_original_ack
identity=RunJournal(root/'run').recovery_identity() if (root/'run').exists() else None
scope=(RunContext.resume(RunRef(str(root/'run'),identity['run_id'])) if identity else
       RunContext.start(root/'run',limits={'checkpoint_rows':2}))
with scope as run, data.local_execution():
    rows=data.from_items([{'value':n} for n in range(6)]).map_cached(Counter(),cache_dir=root/'cache',
        version='v1',inputs=['value'],output='count').take_all()
    (root/'result.json').write_text(json.dumps({'rows':rows,'commits':run.journal.execution_commits('action-1')}))
'''


def test_real_driver_process_death_recovers_original_ack_and_suffix_cache(tmp_path):
    import os
    import subprocess
    import sys
    tree = Path(__file__).resolve().parents[1]
    environment = dict(os.environ, PYTHONPATH=str(tree) + ':' + str(tree / 'tests'))
    def run(mode):
        return subprocess.run([sys.executable, '-B', '-c', CRASH_DRIVER, str(tmp_path), mode],
            cwd=tree, env=environment, capture_output=True, text=True, timeout=90)
    first = run('crash')
    assert first.returncode == 79, first.stderr
    assert json.loads((tmp_path / 'crashed.json').read_text())['cache_entries'] >= 4
    second = run('resume')
    assert second.returncode == 0, second.stderr
    result = json.loads((tmp_path / 'result.json').read_text())
    assert result['rows'] == [{'value': n, 'count': n + 1} for n in range(6)]
    assert all(c['status'] == 'acked' for c in result['commits'])
