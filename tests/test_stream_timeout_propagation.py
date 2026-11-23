"""Callback timeouts, declared misses and runtime deadlines have distinct origins.

Fixtures use at most 515 small rows, two workers and local bounded waits. No
model, network or GPU is involved. Native actions use their real commit path.
"""
import asyncio
import threading

import lance
import pyarrow as pa
import pytest

from demiflow import data
from _worker_event_fixtures import emit, events
from demiflow.execution import RunContext
from demiflow.execution.run_journal import RunJournal


def mapped(rows, kind, callback, **options):
    source = data.from_items(rows)
    options = dict(concurrency=1, queue_depth=1, label='probe', **options)
    if kind == 'batch':
        return source.batch_map(callback, max_batch=2, **options)
    return source.map_async(callback, **options)


@pytest.mark.parametrize('kind', ['row', 'batch'])
@pytest.mark.parametrize('hard_timeout', [None, 30])
@pytest.mark.parametrize('form', ['function', 'instance', 'class'])
def test_callback_timeout_propagates_original_error(tmp_path, kind, hard_timeout, form):
    message = 'callback in asyncio.to_thread'
    closed = str(tmp_path / 'closed.jsonl')

    async def callback(value):
        def fail():
            raise TimeoutError(message)
        return await asyncio.to_thread(fail)

    class Actor:
        async def __call__(self, value):
            return await callback(value)

        async def aclose(self):
            emit(closed, 'closed')

    fn = callback if form == 'function' else Actor() if form == 'instance' else Actor
    drained = []
    threads = {t.ident for t in threading.enumerate()}
    with pytest.raises(TimeoutError) as failure:
        mapped([{'id': 0}], kind, fn, hard_timeout=hard_timeout).run_stream(
            on_drain=drained.append)
    assert str(failure.value) == message
    assert len(drained) == 1
    assert drained[0].miss == {} and drained[0].dead_batches == []
    assert drained[0].emitted == 0
    assert drained[0].timing_summary()['probe']['count'] == 1
    assert [e['event'] for e in events(closed)] == ([] if form == 'function' else ['closed'])
    assert not [t.name for t in threading.enumerate() if t.ident not in threads]


@pytest.mark.parametrize('kind', ['row', 'batch'])
def test_synchronous_callback_timeout_is_fatal_with_unexpired_deadline(kind):
    message = 'synchronous callback'

    def callback(value):
        raise TimeoutError(message)

    with pytest.raises(TimeoutError) as failure:
        mapped([{'id': 0}], kind, callback, hard_timeout=30,
               catch=(ValueError,)).run_stream()
    assert str(failure.value) == message


@pytest.mark.parametrize('kind', ['row', 'batch'])
@pytest.mark.parametrize('hard_timeout', [None, 30])
@pytest.mark.parametrize('catch', [(TimeoutError,), (Exception,)])
def test_explicit_catch_keeps_callback_timeout_policy(kind, hard_timeout, catch):
    async def callback(value):
        raise TimeoutError('declared miss')

    stats = mapped([{'id': i} for i in range(3)], kind, callback,
                   catch=catch, hard_timeout=hard_timeout).run_stream()
    assert stats.emitted == 0
    assert stats.miss == {'probe:TimeoutError': 2 if kind == 'batch' else 3}
    if kind == 'batch':
        assert [b['reason'] for b in stats.dead_batches] == ['TimeoutError'] * 2
        assert [len(b['rows']) for b in stats.dead_batches] == [2, 1]
    else:
        assert stats.dead_batches == []


@pytest.mark.parametrize('kind', ['row', 'batch'])
@pytest.mark.parametrize('form', ['function', 'class'])
@pytest.mark.parametrize('catch', [(), (TimeoutError,)])
def test_real_deadline_keeps_miss_and_declared_catch_precedence(tmp_path, kind, form, catch):
    cancelled = str(tmp_path / "cancelled.jsonl")

    async def callback(value):
        try:
            await asyncio.Event().wait()
        finally:
            emit(cancelled, 'cancelled')

    class Actor:
        async def __call__(self, value):
            return await callback(value)

    fn = callback if form == 'function' else Actor
    stats = mapped([{'id': i} for i in range(3)], kind, fn,
                   catch=catch, hard_timeout=.01).run_stream()
    calls = 2 if kind == 'batch' else 3
    reason = 'TimeoutError' if catch else 'hard_timeout'
    assert stats.emitted == 0 and [e['event'] for e in events(cancelled)] == ['cancelled'] * calls
    assert stats.miss == {'probe:' + reason: calls}
    if kind == 'batch':
        assert [b['reason'] for b in stats.dead_batches] == [reason] * calls
        assert [len(b['rows']) for b in stats.dead_batches] == [2, 1]


@pytest.mark.parametrize('kind', ['row', 'batch'])
def test_prior_deadline_does_not_reclassify_later_callback_error(tmp_path, kind):
    calls = []
    observed = str(tmp_path / "calls.jsonl")
    message = 'second callback failed'

    async def callback(value):
        calls.append(value)
        emit(observed, 'call')
        if len(calls) == 1:
            await asyncio.Event().wait()
        raise TimeoutError(message)

    drained = []
    with pytest.raises(TimeoutError) as failure:
        mapped([{'id': i} for i in range(3)], kind, callback,
               hard_timeout=.01).run_stream(on_drain=drained.append)
    assert str(failure.value) == message and len(events(observed)) == 2
    assert drained[0].miss == {'probe:hard_timeout': 1}
    assert len(drained[0].dead_batches) == (1 if kind == 'batch' else 0)


@pytest.mark.parametrize('kind', ['row', 'batch'])
@pytest.mark.parametrize('return_on_cancel', [False, True])
def test_wait_for_preserves_callback_outcome_during_deadline_cancellation(kind, return_on_cancel):
    message = 'callback translated cancellation'

    async def callback(value):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if return_on_cancel:
                return value
            raise TimeoutError(message)

    ds = mapped([{'id': 0}], kind, callback, hard_timeout=.01)
    if return_on_cancel:
        stats = ds.run_stream()
        assert stats.emitted == 1 and stats.miss == {}
    else:
        with pytest.raises(TimeoutError) as failure:
            ds.run_stream()
        assert str(failure.value) == message


@pytest.mark.parametrize('kind', ['row', 'batch'])
def test_unexpired_deadline_keeps_normal_none_and_empty_list(kind):
    async def callback(value):
        rows = value if kind == 'batch' else [value]
        first = rows[0]['id']
        return None if first == 0 else [] if first == 2 else value

    stats = mapped([{'id': i} for i in range(6)], kind, callback,
                   hard_timeout=30).run_stream()
    assert stats.emitted == (2 if kind == 'batch' else 4)
    assert stats.miss == {'probe:drop': 1}
    assert [b['reason'] for b in stats.dead_batches] == (['drop'] if kind == 'batch' else [])


@pytest.mark.parametrize('kind', ['row', 'batch'])
@pytest.mark.parametrize('hard_timeout', [None, 30])
@pytest.mark.parametrize('action', ['materialize', 'create', 'overwrite'])
def test_native_failure_after_output_never_completes_action_or_publishes(tmp_path, kind, hard_timeout, action):
    message = 'synthetic second batch failure'
    observed = str(tmp_path / "native-callbacks.jsonl")
    target = str(tmp_path / 'out.lance')
    schema = pa.schema([('id', pa.int64())])
    previous = pa.table({'id': [99]})
    if action == 'overwrite':
        lance.write_dataset(previous, target)

    class Actor:
        def snapshot_state(self):
            return {}

        def restore_state(self, state):
            assert state == {}

        async def __call__(self, value):
            rows = value if kind == 'batch' else [value]
            for row in rows:
                emit(observed, 'call', value=row['id'])
            if rows[0]['id'] >= 2:
                def fail():
                    raise TimeoutError(message)
                return await asyncio.to_thread(fail)
            return value

        async def aclose(self):
            emit(observed, 'closed')

    before_threads = {t.ident for t in threading.enumerate()}
    with pytest.raises(TimeoutError) as failure:
        with RunContext.start(tmp_path / 'run'):
            ds = mapped([{'id': i} for i in range(5)], kind, Actor(), hard_timeout=hard_timeout)
            if action == 'materialize':
                ds.materialize()
            else:
                ds.write_lance(target, mode=action, expected_version=1 if action == 'overwrite' else None,
                               schema=schema, return_receipt=True)
    assert str(failure.value) == message
    actual = events(observed)
    calls = [e['value'] for e in actual if e['event'] == 'call']
    assert calls[:2] == [0, 1] and len(calls) in (3, 4)
    assert [e['event'] for e in actual if e['event'] == 'closed'] == ['closed']
    journal = RunJournal(tmp_path / 'run')
    assert journal.execution_status()['status'] == 'failed'
    assert journal.execution_action('action-1')['result'] is None
    facts = journal.execution_commits('action-1')
    # A native sink may retain its pending intent for recovery. It must have no
    # publication receipt or ACK, and the action must remain incomplete.
    assert all(fact['status'] == 'pending' and not fact['receipts'] for fact in facts)
    if action == 'overwrite':
        assert lance.dataset(target).version == 1
        assert lance.dataset(target).to_table().equals(previous)
    else:
        assert not (tmp_path / 'out.lance').exists()
    assert not [t.name for t in threading.enumerate() if t.ident not in before_threads]


def test_failed_row_materialization_keeps_confirmed_prefix(tmp_path):
    message = 'after confirmed source prefix'

    async def callback(row):
        if row['id'] == 513:
            raise TimeoutError(message)
        return row

    with pytest.raises(TimeoutError) as failure:
        # Isolate the original 512-row frontier. The independent time trigger
        # may legitimately seal an earlier prefix on a slower managed host.
        with RunContext.start(tmp_path / 'run', limits={
                'checkpoint_rows': 512, 'checkpoint_interval_ms': 60000}) as run:
            mapped([{'id': i} for i in range(515)], 'row', callback, hard_timeout=30).materialize()
    assert str(failure.value) == message
    journal = RunJournal(tmp_path / 'run')
    assert journal.execution_status()['status'] == 'failed'
    assert journal.execution_action('action-1')['result'] is None
    facts = journal.execution_commits('action-1')
    assert len(facts) == 1 and facts[0]['status'] == 'acked'
    from demiflow.execution.state_artifacts import StateArtifacts
    from _native_receipt_fixtures import committed_rows
    image = StateArtifacts(run).read(facts[0]['prepared']['state']['dataflow-cut-v3'])
    assert image['cut']['final'] is False
    assert [cursor['next_sequence'] for cursor in image['dataflow.sources.v3']] == [512]
    receipt = facts[0]['receipts']['materialize']['data']
    assert receipt['rows'] == 512
    assert committed_rows(run, 'action-1', facts[0]) == [{'id': i} for i in range(512)]


@pytest.mark.parametrize('kind', ['row', 'batch'])
@pytest.mark.parametrize('action', ['materialize', 'write_lance'])
@pytest.mark.parametrize('outcome', ['normal', 'empty', 'caught', 'deadline'])
def test_native_intentional_results_remain_successful(tmp_path, kind, action, outcome):
    async def callback(value):
        if outcome == 'empty':
            return []
        if outcome == 'caught':
            raise TimeoutError('explicit native miss')
        if outcome == 'deadline':
            await asyncio.Event().wait()
        return value

    with RunContext.start(tmp_path / 'run'):
        ds = mapped([{'id': i} for i in range(3)], kind, callback,
            hard_timeout=.01 if outcome == 'deadline' else 30,
            catch=(TimeoutError,) if outcome == 'caught' else ())
        if action == 'materialize':
            count = ds.materialize().count()
        else:
            target = str(tmp_path / 'out.lance')
            receipt = ds.write_lance(target, mode='create', return_receipt=True,
                                    schema=pa.schema([('id', pa.int64())]))
            assert receipt.status == 'committed'
            count = lance.dataset(target).count_rows()
    assert count == (3 if outcome == 'normal' else 0)
    journal = RunJournal(tmp_path / 'run')
    assert journal.execution_status()['status'] == 'succeeded'
    assert journal.execution_action('action-1')['result'] is not None
