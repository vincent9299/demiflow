"""Journal throughput mode: batching, leases, bounds, recovery and faults.

The white-box helpers build one batch explicitly so partial and whole-batch
failures are deterministic instead of depending on batch timing.
"""
import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from demiflow.operator_llm.call_ref import (
    PromptRecordRef, _journal_paths, call_snapshot, journal_observation, journal_totals,
)
from demiflow.operator_llm.errors import PromptBudgetExceededError
from demiflow.operator_llm.journal import UncertainPromptCall, canonical, request_key
from demiflow.operator_llm.sqlite_journal import (
    SQLitePromptJournal, _BatchItem, _DatabaseLease, _LEASE_INSERT, _process_start,
)


def options(**overrides):
    return {'batch_size': 8, 'flush_interval_s': .001, 'queue_size': 32, 'wal': True, **overrides}


def one_batch(journal, operations, shard_index=0):
    """Run exactly these operations through one deterministic commit attempt."""
    shard = journal._shards[shard_index]
    items = []
    for operation, payload, size in operations:
        shard._bytes.reserve(size, shard._stopped)
        items.append(_BatchItem(operation, payload, size))
    shard._commit_batch(items)
    return items


class _CommitFailure:
    """Connection proxy whose context-manager commit fails after a real rollback."""
    def __init__(self, db, error):
        self._db, self._error = db, error

    def __getattr__(self, name):
        return getattr(self._db, name)

    def __enter__(self):
        self._db.__enter__()
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self._db.rollback()
            raise self._error
        return self._db.__exit__(exc_type, exc, traceback)


def test_batch_reserve_response_wal_and_database_leases(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options())
    requests = [{'item': i} for i in range(32)]
    with ThreadPoolExecutor(max_workers=16) as pool:
        assert all(pool.map(journal.reserve, requests))
    with ThreadPoolExecutor(max_workers=16) as pool:
        refs = list(pool.map(lambda request: journal.response(request, {'body': {'ok': True}}), requests))
    assert journal.stats() == {'requests': 32, 'responses': 32, 'uncertain': 0}
    assert len(refs) == 32
    assert journal.inspection_stats() == {
        'requests': 32, 'responses': 32, 'saved_errors': 0, 'without_response_or_error': 0,
    }
    assert not (tmp_path / 'calls.sqlite.leases').exists()
    journal.close()
    with sqlite3.connect(path) as db:
        assert db.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'wal'
        assert db.execute('PRAGMA synchronous').fetchone()[0] == 2
        assert db.execute('SELECT count(*) FROM writer_leases').fetchone()[0] == 0
    assert journal_totals(path)['requests'] == 32
    assert len(call_snapshot(path=path)['responses']) == 32


def test_duplicate_request_reuses_saved_response(tmp_path):
    path = tmp_path / 'calls.sqlite'
    request = {'item': 'same'}
    journal = SQLitePromptJournal(path, throughput=options())
    assert journal.reserve(request)
    journal.response(request, {'body': {'value': 7}})
    assert not journal.reserve(request)
    assert journal.lookup(request)['body']['value'] == 7
    journal.close()


def test_concurrent_budget_is_atomic(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, max_requests=3, throughput=options())

    def reserve(index):
        try:
            return journal.reserve({'item': index})
        except PromptBudgetExceededError:
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(reserve, range(16))) == 3
    assert journal.stats() == {'requests': 3, 'responses': 0, 'uncertain': 3}
    journal.close()


def test_active_writer_blocks_requeue_until_owner_closes(tmp_path):
    path = tmp_path / 'calls.sqlite'
    request = {'item': 'recover'}
    key = request_key(request)
    owner = SQLitePromptJournal(path, throughput=options())
    other = SQLitePromptJournal(path, throughput=options())
    owner.reserve(request)
    with pytest.raises(UncertainPromptCall, match='active writer'):
        other.requeue_uncertain([key], actor='tester', reason='restart')
    owner.close()
    other.requeue_uncertain([key], actor='tester', reason='restart')
    assert other.reserve(request)
    with pytest.raises(UncertainPromptCall, match='no longer owns'):
        owner.response(request, {'body': {'stale': True}})
    owner.close()
    other.close()
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM recovery_events').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM writer_leases').fetchone()[0] == 0


def test_default_mode_keeps_file_leases(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path)
    journal.reserve({'item': 1})
    assert (tmp_path / 'calls.sqlite.leases').is_dir()
    journal.close()


def test_sharded_budget_references_and_aggregate_readers(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, max_requests=12, throughput=options(shards=3))

    def reserve(index):
        try:
            return index if journal.reserve({'item': index}) else None
        except PromptBudgetExceededError:
            return None

    with ThreadPoolExecutor(max_workers=12) as pool:
        reserved = [index for index in pool.map(reserve, range(24)) if index is not None]
    assert len(reserved) == 12

    def respond(index):
        record = {'status_code': 200, 'elapsed_s': .1 + index / 100,
                  'body': {'created': 1000 + index, 'value': index,
                           'usage': {'prompt_tokens': 10, 'completion_tokens': 5}}}
        return index, record, journal.response({'item': index}, record)

    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(respond, reserved))
    assert journal.stats() == {'requests': 12, 'responses': 12, 'uncertain': 0}
    assert len(journal.registered_keys()) == 12
    assert len(journal.request_ids()) == 12
    assert len(journal.records()) == 12
    assert len(journal.calls()) == 12
    shard_paths = {str(tmp_path / 'calls.sqlite.d' / f'{shard:03d}.sqlite') for shard in range(3)}
    for index, record, refs in responses:
        assert refs['journal_path'] in shard_paths
        assert PromptRecordRef.from_dict(refs['response_ref']).read() == record
    journal.close()

    manifest = tmp_path / 'calls.sqlite.d' / 'manifest.json'
    assert manifest.exists()
    for shard in range(3):
        shard_path = tmp_path / 'calls.sqlite.d' / f'{shard:03d}.sqlite'
        with sqlite3.connect(shard_path) as db:
            assert db.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'wal'
            assert db.execute('PRAGMA synchronous').fetchone()[0] == 2
            assert db.execute('SELECT count(*) FROM writer_leases').fetchone()[0] == 0
        assert not (tmp_path / 'calls.sqlite.d' / f'{shard:03d}.sqlite.leases').exists()

    totals = journal_totals(path)
    assert totals['requests'] == totals['responses'] == totals['http_successes'] == 12
    assert totals['input_tokens'] == 120
    assert totals['output_tokens'] == 60
    assert len(call_snapshot(path=path)['responses']) == 12
    observation = journal_observation(path)
    assert observation['completed_in_window'] == 12
    assert observation['pending_reservations'] == 0
    assert observation['response_p95_s'] is not None

    journal = SQLitePromptJournal(path, max_requests=12, throughput=options(shards=3))
    assert not journal.reserve({'item': reserved[0]})
    with pytest.raises(PromptBudgetExceededError):
        journal.reserve({'item': 'new'})
    journal.close()
    readonly = SQLitePromptJournal(path, read_only=True, throughput=options(shards=3))
    for index in reserved:
        assert readonly.lookup({'item': index})['body']['value'] == index
    assert readonly.stats() == {'requests': 12, 'responses': 12, 'uncertain': 0}
    readonly.close()


def test_empty_throughput_enables_batching_and_validates_shards(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput={})
    assert journal.reserve({'item': 1})
    journal.close()
    assert journal._shards[0]._batch_thread is None
    with sqlite3.connect(path) as db:
        assert db.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'delete'
        assert db.execute('PRAGMA synchronous').fetchone()[0] == 2
    for shards in (0, 9, 1.5):
        with pytest.raises(ValueError, match='shards'):
            SQLitePromptJournal(tmp_path / 'invalid.sqlite', throughput={'shards': shards})


# --- database lease ownership -------------------------------------------------

def test_database_lease_stores_owner_and_replaces_dead_writer(tmp_path):
    path = tmp_path / 'calls.sqlite'
    request = {'item': 'dead-owner'}
    key = request_key(request)
    journal = SQLitePromptJournal(path, throughput=options())
    journal._connect()
    # A dead process left an unanswered reservation and its lease row behind.
    with sqlite3.connect(path) as db:
        db.execute('INSERT INTO calls(request_key,request_json) VALUES (?,?)', (key, canonical(request)))
        db.execute("UPDATE journal_state SET value='1' WHERE name='request_count'")
        db.execute(_LEASE_INSERT, (key, 'dead-token', 999999, '1'))
    # A plain reserve never resends an unanswered row, alive owner or not.
    with pytest.raises(UncertainPromptCall, match='without complete response'):
        journal.reserve(request)
    journal.requeue_uncertain([key], actor='tester', reason='dead process')
    assert journal.reserve(request)
    with sqlite3.connect(path) as db:
        pid, start = db.execute('SELECT owner_pid, owner_start FROM writer_leases WHERE request_key=?',
                                (key,)).fetchone()
    assert pid == os.getpid() and start == _process_start(os.getpid())
    journal.close()


def test_live_owner_pid_blocks_explicit_recovery(tmp_path):
    path = tmp_path / 'calls.sqlite'
    request = {'item': 'live-owner'}
    key = request_key(request)
    journal = SQLitePromptJournal(path, throughput=options())
    journal.reserve(request)
    # Another live process still owns the unanswered reservation.
    with sqlite3.connect(path) as db:
        db.execute('DELETE FROM writer_leases WHERE request_key=?', (key,))
        db.execute(_LEASE_INSERT, (key, 'other-token', os.getpid(), _process_start(os.getpid())))
    other = SQLitePromptJournal(path, throughput=options())
    with pytest.raises(UncertainPromptCall, match='active writer'):
        other.requeue_uncertain([key], actor='tester', reason='must not steal')
    with sqlite3.connect(path) as db:
        db.execute('UPDATE writer_leases SET owner_pid=999999, owner_start=? WHERE request_key=?',
                   ('1', key))
    other.requeue_uncertain([key], actor='tester', reason='owner is gone')
    assert other.reserve(request)
    other.close()
    journal.close()


def test_legacy_writer_lease_table_and_calls_are_migrated(tmp_path):
    path = tmp_path / 'calls.sqlite'
    request = {'item': 'legacy-lease'}
    key = request_key(request)
    with sqlite3.connect(path) as db:
        # The intermediate uncommitted layout had no owner identity columns.
        db.execute('CREATE TABLE writer_leases (request_key TEXT PRIMARY KEY, owner_token TEXT NOT NULL)')
        db.execute('INSERT INTO writer_leases VALUES (?,?)', (key, 'legacy-token'))
    journal = SQLitePromptJournal(path, throughput=options())
    journal._connect()
    with sqlite3.connect(path) as db:
        leases = {row[1] for row in db.execute('PRAGMA table_info(writer_leases)')}
        calls = {row[1] for row in db.execute('PRAGMA table_info(calls)')}
    assert {'owner_pid', 'owner_start'} <= leases
    assert 'ordinal' in calls
    # A fresh key has no calls row, so the unverifiable legacy owner is stale.
    assert journal.reserve(request)
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT owner_pid FROM writer_leases WHERE request_key=?',
                          (key,)).fetchone()[0] == os.getpid()
    journal.response(request, {'body': {'ok': True}})
    journal.close()


# --- batch fault injection ----------------------------------------------------

def test_partial_batch_failure_keeps_other_rows_and_no_leak(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options())
    blocked = {'item': 'already-reserved'}
    blocked_key = request_key(blocked)
    assert journal.reserve(blocked)
    shard = journal._shards[0]
    operations = []
    for request in [blocked, *({'item': f'fresh-{i}'} for i in range(4))]:
        key, metadata = request_key(request), canonical(request)
        lease = _DatabaseLease(uuid.uuid4().hex) if key == blocked_key else shard._lease(key)
        operations.append(('reserve', (key, metadata, lease, None), len(metadata) + len(key)))
    items = one_batch(journal, operations)
    assert isinstance(items[0].future.exception(), UncertainPromptCall)
    assert all(item.future.result() is True for item in items[1:])
    assert journal.stats() == {'requests': 5, 'responses': 0, 'uncertain': 5}
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM calls WHERE request_key=?',
                          (blocked_key,)).fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM writer_leases WHERE request_key=?',
                          (blocked_key,)).fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 5
        assert db.execute('SELECT count(*) FROM writer_leases').fetchone()[0] == 5
    journal.close()


def test_failed_batch_commit_fails_every_future_and_keeps_memory_consistent(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options())
    shard = journal._shards[0]
    requests = [{'item': f'batch-{i}'} for i in range(3)]
    operations = []
    for request in requests:
        key, metadata = request_key(request), canonical(request)
        operations.append(('reserve', (key, metadata, shard._lease(key), None),
                           len(metadata) + len(key)))
    real_connect = shard._connect
    db = real_connect()
    failure = sqlite3.OperationalError('injected commit failure')
    shard._connect = lambda: _CommitFailure(db, failure)
    try:
        items = one_batch(journal, operations)
    finally:
        shard._connect = real_connect
    assert all(item.future.exception() is failure for item in items)
    # A rolled-back batch must not leave an in-memory owner that blocks retry.
    assert not ({request_key(request) for request in requests} & set(shard._leases))
    with sqlite3.connect(path) as raw:
        assert raw.execute('SELECT count(*) FROM calls').fetchone()[0] == 0
        assert raw.execute('SELECT count(*) FROM writer_leases').fetchone()[0] == 0
    assert all(journal.reserve(request) for request in requests)
    assert journal.stats()['requests'] == 3
    assert shard._batch_thread.is_alive()
    journal.close()


def test_completion_failure_rolls_back_one_save_and_keeps_writer_alive(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options())
    request = {'item': 'completion-failure'}
    assert journal.reserve(request)
    shard = journal._shards[0]
    real_references = shard._references_at

    def broken(key, attempt):
        raise RuntimeError('injected reference failure')

    shard._references_at = broken
    try:
        with pytest.raises(RuntimeError, match='injected reference failure'):
            journal.response(request, {'body': {'value': 1}})
    finally:
        shard._references_at = real_references
    assert journal.stats() == {'requests': 1, 'responses': 0, 'uncertain': 1}
    refs = journal.response(request, {'body': {'value': 1}})
    assert PromptRecordRef.from_dict(refs['response_ref']).read()['body'] == {'value': 1}
    assert journal.lookup(request)['body']['value'] == 1
    assert shard._batch_thread.is_alive()
    assert journal.reserve({'item': 'after-failure'})
    journal.close()


def test_close_drains_a_partial_batch(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options(flush_interval_s=.5, batch_size=64))
    result = []
    worker = threading.Thread(target=lambda: result.append(journal.reserve({'item': 'drained'})))
    worker.start()
    time.sleep(.05)  # the writer holds the operation in its partial batch
    journal.close()
    worker.join(timeout=30)
    assert not worker.is_alive()
    assert result == [True]
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM writer_leases').fetchone()[0] == 0


# --- byte bounds and metrics --------------------------------------------------

def test_single_record_and_queue_bytes_are_bounded(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options(
        batch_size=4, queue_size=8, max_record_bytes=1024, max_queue_bytes=2048))
    with pytest.raises(PromptBudgetExceededError, match='record bound'):
        journal.reserve({'item': 'huge', 'text': 'x' * 4096})
    assert journal.stats()['requests'] == 0
    requests = [{'item': index, 'text': 'y' * 40} for index in range(24)]
    with ThreadPoolExecutor(max_workers=12) as pool:
        assert all(pool.map(journal.reserve, requests))
    metrics = journal.throughput_metrics()
    assert metrics['queued_bytes_max'] <= 2048
    assert metrics['queued_bytes'] == 0
    assert metrics['queue_bytes_limit'] == 2048
    assert journal.stats()['requests'] == 24
    journal.close()


def test_group_commit_metrics_are_bounded(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options(batch_size=8))
    requests = [{'item': index} for index in range(32)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(journal.reserve, requests))
        list(pool.map(lambda request: journal.response(request, {'body': {'ok': True}}), requests))
    metrics = journal.throughput_metrics()
    assert metrics['shards'] == 1
    assert metrics['reserve_committed'] == 32
    assert metrics['save_committed'] == 32
    assert metrics['failed_operations'] == 0
    assert metrics['batches'] >= 2
    assert 1 <= metrics['batch_rows_max'] <= 8
    assert metrics['batch_seconds_mean'] is not None
    assert metrics['queue_wait_seconds_max'] >= 0
    assert metrics['queued_bytes'] == 0
    journal.close()


def test_concurrent_same_key_has_exactly_one_reservation(tmp_path):
    for shards in (1, 4):
        path = tmp_path / f'same-key-{shards}.sqlite'
        journal = SQLitePromptJournal(path, throughput=options(shards=shards, batch_size=16))
        request = {'item': 'same-key-race'}

        def attempt(_):
            try:
                return journal.reserve(request)
            except UncertainPromptCall:
                return 'uncertain'

        with ThreadPoolExecutor(max_workers=64) as pool:
            results = list(pool.map(attempt, range(64)))
        assert results.count(True) == 1, results.count(True)
        assert results.count('uncertain') == 63
        journal.response(request, {'body': {'ok': True}})
        assert not journal.reserve(request)
        assert journal.stats() == {'requests': 1, 'responses': 1, 'uncertain': 0}
        journal.close()
        for shard_path in _journal_paths(path):
            with sqlite3.connect(shard_path) as db:
                assert db.execute('SELECT count(*) FROM writer_leases').fetchone()[0] == 0


def test_readonly_throughput_journal_rejects_writes(tmp_path):
    path = tmp_path / 'calls.sqlite'
    request = {'item': 'readonly'}
    writer = SQLitePromptJournal(path, throughput=options())
    assert writer.reserve(request)
    writer.response(request, {'body': {'ok': True}})
    writer.close()
    reader = SQLitePromptJournal(path, read_only=True, throughput=options())
    assert reader.lookup(request)['body'] == {'ok': True}
    assert reader.call_page(limit=1)[0]['response']['body'] == {'ok': True}
    with pytest.raises(PermissionError):
        reader.reserve({'item': 'new'})
    with pytest.raises(PermissionError):
        reader.requeue_uncertain([request_key(request)], actor='tester', reason='readonly')
    reader.close()


# --- multi-shard recovery and inspection --------------------------------------

def _shard_groups(journal, count):
    groups, index = {}, 0
    while len(groups) < count:
        request = {'item': f'sharded-{index}'}
        shard = journal._for_key(request_key(request))
        groups.setdefault(id(shard), (shard, []))[1].append(request)
        index += 1
        assert index < 10_000
    return list(groups.values())


def test_sharded_requeue_preflight_rejects_mixed_keys_before_writing(tmp_path):
    path = tmp_path / 'calls.sqlite'
    owner = SQLitePromptJournal(path, throughput=options(shards=4))
    groups = _shard_groups(owner, 4)
    for _, requests in groups:
        assert owner.reserve(requests[0])
    answered = groups[0][1][0]
    owner.response(answered, {'body': {'ok': True}})
    keys = [request_key(requests[0]) for _, requests in groups]
    owner.close()  # a restarted process recovers what the old one left
    journal = SQLitePromptJournal(path, throughput=options(shards=4))
    with pytest.raises(ValueError, match='unanswered'):
        journal.requeue_uncertain(keys, actor='tester', reason='mixed')
    assert not journal.recovery_history()
    assert journal.stats() == {'requests': 4, 'responses': 1, 'uncertain': 3}
    journal.close()


def test_sharded_requeue_retry_completes_remaining_shards(tmp_path):
    path = tmp_path / 'calls.sqlite'
    owner = SQLitePromptJournal(path, throughput=options(shards=4))
    groups = _shard_groups(owner, 4)
    for _, requests in groups:
        assert owner.reserve(requests[0])
    keys = [request_key(requests[0]) for _, requests in groups]
    owner.close()
    journal = SQLitePromptJournal(path, throughput=options(shards=4))
    # The wrapper executes shard groups in sorted-key order; break the last one.
    reopened = {}
    for key in sorted(keys):
        shard = journal._for_key(key)
        reopened.setdefault(id(shard), shard)
    failing = list(reopened.values())[-1]
    real = failing.requeue_uncertain

    def broken(subset, **kwargs):
        raise ValueError('injected shard outage')

    failing.requeue_uncertain = broken
    try:
        with pytest.raises(ValueError, match='injected shard outage'):
            journal.requeue_uncertain(keys, actor='tester', reason='partial', operation_id='op-1')
    finally:
        failing.requeue_uncertain = real
    # Applied shards archived their keys exactly once; the retry only applies
    # the remaining shard and reports the operation as complete afterwards.
    assert len(journal.recovery_history()) == 3
    assert not journal.requeue_uncertain(keys, actor='tester', reason='partial',
                                         operation_id='op-1')['reused']
    assert journal.requeue_uncertain(keys, actor='tester', reason='partial',
                                     operation_id='op-1')['reused']
    assert len({row['request_key'] for row in journal.recovery_history()}) == 4
    assert journal.stats() == {'requests': 0, 'responses': 0, 'uncertain': 0}
    journal.close()


def test_sharded_call_page_keeps_recency_order_and_bounds(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options(shards=4))
    requests = [{'item': index} for index in range(10)]
    for request in requests:
        assert journal.reserve(request)
        journal.response(request, {'body': {'value': request['item']}})
    keys = [request_key(request) for request in requests]
    page = journal.call_page(limit=4)
    assert [row['request_id'] for row in page] == keys[::-1][:4]
    pages = [journal.call_page(offset=offset, limit=4) for offset in (0, 4, 8)]
    assert [row['request_id'] for page in pages for row in page] == keys[::-1]
    assert journal.call_page(offset=20) == []
    with pytest.raises(ValueError, match='offset\\+limit'):
        journal.call_page(offset=1020, limit=8)
    assert len(journal.registered_keys()) == 10
    with pytest.raises(ValueError, match='key budget'):
        journal.registered_keys(max_keys=9)
    journal.close()


def test_sharded_registered_keys_zero_budget(tmp_path):
    path = tmp_path / 'calls.sqlite'
    journal = SQLitePromptJournal(path, throughput=options(shards=3))
    assert journal.registered_keys(max_keys=0) == frozenset()
    assert journal.reserve({'item': 1})
    with pytest.raises(ValueError, match='key budget'):
        journal.registered_keys(max_keys=0)
    journal.close()


# --- process crash recovery ---------------------------------------------------

_CHILD = '''
import os, sys, time
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
path, mode = sys.argv[1], sys.argv[2]
journal = SQLitePromptJournal(path, throughput={
    'batch_size': 4, 'flush_interval_s': .001, 'queue_size': 16, 'wal': True})
if mode == 'hold':
    assert journal.reserve({'item': 'held'})
    print('READY', flush=True)
    time.sleep(60)
else:
    assert journal.reserve({'item': 'answered'})
    journal.response({'item': 'answered'}, {'body': {'value': 41}})
    assert journal.reserve({'item': 'pending'})
    print('COMMITTED', flush=True)
os._exit(0)
'''


def _spawn(path, mode):
    process = subprocess.Popen([sys.executable, '-B', '-c', _CHILD, str(path), mode],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    line = process.stdout.readline().strip()
    assert line in {'READY', 'COMMITTED'}, (line, process.stderr.read())
    return process


def test_crashed_owner_lease_is_recovered_only_explicitly(tmp_path):
    path = tmp_path / 'calls.sqlite'
    held = {'item': 'held'}
    held_key = request_key(held)
    child = _spawn(path, 'hold')
    try:
        journal = SQLitePromptJournal(path, throughput=options())
        with pytest.raises(UncertainPromptCall, match='active writer'):
            journal.requeue_uncertain([held_key], actor='tester', reason='must not steal')
        child.kill()
        child.wait(timeout=30)
        journal.requeue_uncertain([held_key], actor='tester', reason='killed owner')
        assert journal.reserve(held)
        journal.close()
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=30)


def test_crash_after_committed_response_reuses_without_new_call(tmp_path):
    path = tmp_path / 'calls.sqlite'
    answered, pending = {'item': 'answered'}, {'item': 'pending'}
    process = _spawn(path, 'exit')
    assert process.wait(timeout=30) == 0
    journal = SQLitePromptJournal(path, throughput=options())
    # A committed response is reused: reserve answers False, never a second call.
    assert not journal.reserve(answered)
    assert journal.lookup(answered)['body']['value'] == 41
    # An unanswered reservation stays uncertain after a crash and is never resent.
    with pytest.raises(UncertainPromptCall, match='without complete response'):
        journal.lookup(pending)
    with pytest.raises(UncertainPromptCall, match='without complete response'):
        journal.reserve(pending)
    with sqlite3.connect(path) as db:
        pid, start = db.execute('SELECT owner_pid, owner_start FROM writer_leases WHERE request_key=?',
                                (request_key(pending),)).fetchone()
    assert pid == process.pid and start is not None
    journal.requeue_uncertain([request_key(pending)], actor='crash-test', reason='dead process')
    assert journal.reserve(pending)
    journal.response(pending, {'body': {'value': 42}})
    assert journal.stats() == {'requests': 2, 'responses': 2, 'uncertain': 0}
    journal.close()
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM writer_leases').fetchone()[0] == 0
