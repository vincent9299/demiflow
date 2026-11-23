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


def bind_cache_state(actor, owner):
    from demiflow.execution.kernels.task_scope import snapshot_resident_function, restore_resident_function
    from demiflow.execution.kernels.operator_state import snapshot_auto_state, restore_auto_state
    actor.bind_checkpoint_cache(owner,
        snapshot_function=snapshot_resident_function, restore_function=restore_resident_function,
        snapshot_instance=snapshot_auto_state, restore_instance=restore_auto_state)


def cache(root, owner='a', counter=None, **kwargs):
    actor = CachedSyncMap(counter or Counter(), root, 'v1', inputs=['value'], output='count', **kwargs)
    bind_cache_state(actor, hashlib.sha256(owner.encode()).hexdigest())
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
    bind_cache_state(plain, 'b' * 64)
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
import functools, json, os, sqlite3, sys, time
from pathlib import Path
import demiflow
from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RunRef
from demiflow.execution.seal.sealed_plans import load_sealed_plan
root=Path(sys.argv[1]); mode=sys.argv[2]; form=sys.argv[3]
assert Path(demiflow.__file__).resolve().is_relative_to(Path.cwd())
class Counter:
    def __init__(self): self.count=0
    def __call__(self, value):
        self.count+=1
        return self.count
class BoundCounter:
    def __call__(self, value, *, state):
        assert RunJournal(root/'run').source_binding('action-1-state-recovery-restriction')['recovery']=='attempt_only'
        state[0]+=1
        return state[0]
def callback():
    if form=='actor': return Counter()
    if form=='partial_actor': return functools.partial(BoundCounter(),state=[0])
    state=[0]
    if form=='closure':
        def count(value):
            state[0]+=1
            return state[0]
    elif form=='default':
        def count(value, state=[0]):
            state[0]+=1
            return state[0]
    elif form=='kwdefault':
        def count(value, *, state=[0]):
            state[0]+=1
            return state[0]
    elif form=='partial_args':
        def count(state, value):
            state[0]+=1
            return state[0]
        return functools.partial(count,state)
    elif form=='partial_kwargs':
        def count(value, *, state):
            state['count']+=1
            return state['count']
        return functools.partial(count,state={'count':0})
    elif form=='nested_helper':
        def helper():
            state[0]+=1
            return state[0]
        def count(value):
            assert RunJournal(root/'run').source_binding('action-1-state-recovery-restriction')['recovery']=='attempt_only'
            return helper()
    else: raise AssertionError(form)
    return count
pump=DataflowExecution.pump
crash_deadline=None
def crash_after_original_ack(execution, **kwargs):
    global crash_deadline
    result=pump(execution, **kwargs)
    if execution._restore_image is not None and not execution._restore_image.cut.final:
        if crash_deadline is None: crash_deadline=time.monotonic()+15
        with sqlite3.connect(root/'cache/results.sqlite') as db:
            count=db.execute('SELECT COUNT(*) FROM kv').fetchone()[0]
        if count>=4:
            fact=RunJournal(root/'run').execution_commit(execution._restore_image.commit_id)
            assert fact['status']=='acked'
            (root/'crashed.json').write_text(json.dumps({'cache_entries':count,
                'cut':str(execution._restore_image.cut),'original_ack':fact}))
            os._exit(79)
        # Return to the real driver so source credits and an overlapping
        # publisher can advance. Blocking here prevents the boundary we await.
        assert time.monotonic()<crash_deadline, 'suffix cache write did not reach process crash boundary'
    return result
if mode=='crash': DataflowExecution.pump=crash_after_original_ack
identity=RunJournal(root/'run').recovery_identity() if (root/'run').exists() else None
scope=(RunContext.resume(RunRef(str(root/'run'),identity['run_id'])) if identity else
       RunContext.start(root/'run',limits={'checkpoint_rows':2}))
with scope as run, data.local_execution():
    rows=data.from_items([{'value':n} for n in range(6)]).map_cached(callback(),cache_dir=root/'cache',
        version='v1',inputs=['value'],output='count').take_all()
    bundle=load_sealed_plan(run.ref.directory,'action-1')
    (root/'result.json').write_text(json.dumps({'rows':rows,'commits':run.journal.execution_commits('action-1'),
        'restriction':run.journal.source_binding('action-1-state-recovery-restriction'),
        'map_states':[node.kernel.state for node in bundle['graph'].nodes if node.kernel.kind=='AsyncMapOp']}))
'''


@pytest.mark.parametrize('form', ['actor', 'closure', 'default', 'kwdefault', 'partial_args', 'partial_kwargs'])
def test_real_driver_process_death_recovers_original_ack_and_suffix_cache(tmp_path, form):
    import os
    import subprocess
    import sys
    tree = Path(__file__).resolve().parents[1]
    environment = dict(os.environ, PYTHONPATH=str(tree) + ':' + str(tree / 'tests'))
    def run(mode):
        return subprocess.run([sys.executable, '-B', '-c', CRASH_DRIVER, str(tmp_path), mode, form],
            cwd=tree, env=environment, capture_output=True, text=True, timeout=90)
    first = run('crash')
    assert first.returncode == 79, first.stderr
    assert json.loads((tmp_path / 'crashed.json').read_text())['cache_entries'] >= 4
    second = run('resume')
    assert second.returncode == 0, second.stderr
    result = json.loads((tmp_path / 'result.json').read_text())
    assert result['rows'] == [{'value': n, 'count': n + 1} for n in range(6)]
    assert all(c['status'] == 'acked' for c in result['commits'])
    original_ack = json.loads((tmp_path / 'crashed.json').read_text())['original_ack']
    assert next(c for c in result['commits'] if c['commit_id'] == original_ack['commit_id']) == original_ack
    assert result['restriction'] is None
    assert result['map_states'] == ['adapter']


@pytest.mark.parametrize('form', ['partial_actor', 'nested_helper'])
def test_public_cache_unadapted_bound_or_nested_state_records_restriction_before_calls(tmp_path, form):
    import os
    import subprocess
    import sys
    tree = Path(__file__).resolve().parents[1]
    environment = dict(os.environ, PYTHONPATH=str(tree) + ':' + str(tree / 'tests'))
    completed = subprocess.run([sys.executable, '-B', '-c', CRASH_DRIVER, str(tmp_path), 'normal', form],
        cwd=tree, env=environment, capture_output=True, text=True, timeout=90)
    assert completed.returncode == 0, completed.stderr
    result = json.loads((tmp_path / 'result.json').read_text())
    assert result['rows'] == [{'value': n, 'count': n + 1} for n in range(6)]
    assert result['restriction']['recovery'] == 'attempt_only'
    assert result['map_states'] == ['nonrecoverable']


def test_old_function_image_cannot_drop_admitted_closure_state(tmp_path):
    def callback():
        state = [0]
        def count(value):
            state[0] += 1
            return state[0]
        return count
    actor = cache(tmp_path, counter=callback())
    image = asyncio.run(actor.snapshot_state())
    assert image['fn']['mode'] == 'function_slots_v1'
    image['fn'] = None
    with pytest.raises(DataflowError, match='state adapter differs'):
        asyncio.run(cache(tmp_path, counter=callback()).restore_state(image))


def test_function_predicate_state_uses_the_same_acknowledged_cache_frontier(tmp_path):
    def predicate():
        calls = [0]
        def accepted(value):
            calls[0] += 1
            return True
        return accepted
    def build():
        return cache(tmp_path, counter=lambda value: value, when=predicate(), cache_when=predicate())
    first = build()
    assert first({'value': 0})['count'] == 0
    image = asyncio.run(first.snapshot_state())
    assert first({'value': 1})['count'] == 1
    resumed = build()
    asyncio.run(resumed.restore_state(image))
    assert resumed({'value': 1})['count'] == 1
    assert resumed({'value': 2})['count'] == 2
    state = asyncio.run(resumed.snapshot_state())
    assert state['when']['value']['cell:calls'] == [3]
    assert state['cache_when']['value']['cell:calls'] == [3]


def test_unadapted_and_shared_function_data_remain_usable_but_cannot_restore(tmp_path):
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.dataflow_model_factory import CacheFactory
    namespace = {'counter': [0]}
    exec('def count(value):\n counter[0]+=1\n return counter[0]', namespace)
    actor = cache(tmp_path / 'global', counter=namespace['count'])
    assert actor.validate_runtime_state() is not None
    assert actor({'value': 0})['count'] == 1
    image = asyncio.run(actor.snapshot_state())
    assert image['fn']['mode'] == 'nonrecoverable'
    with pytest.raises(DataflowError, match='attempt_only'):
        asyncio.run(actor.restore_state(image))
    shared = [0]
    def compute(value):
        return shared[0]
    def when(row):
        shared[0] += 1
        return True
    actor = CacheFactory(CallableSpec.create(compute), tmp_path / 'shared', 'v1',
        {'inputs': ['value'], 'output': 'count', 'when': when, 'max_input_bytes': 4096}, True)
    assert actor.validate_runtime_state() is not None
    assert actor({'value': 0})['count'] == 1
    assert actor({'value': 1})['count'] == 2
    with pytest.raises(DataflowError, match='attempt_only'):
        asyncio.run(actor.restore_state(asyncio.run(actor.snapshot_state())))
    scalar = 0
    def read_scalar(value):
        return scalar
    def write_scalar(row):
        nonlocal scalar
        scalar += 1
        return True
    actor = CacheFactory(CallableSpec.create(read_scalar), tmp_path / 'shared-cell', 'v1',
        {'inputs': ['value'], 'output': 'count', 'when': write_scalar, 'max_input_bytes': 4096}, True)
    assert actor.validate_runtime_state() is not None
    assert actor({'value': 0})['count'] == 1
    assert actor({'value': 1})['count'] == 2
    with pytest.raises(DataflowError, match='attempt_only'):
        asyncio.run(actor.restore_state(asyncio.run(actor.snapshot_state())))
