"""Real backend interleavings, grant conservation and concurrent public workers."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import sqlite3
import threading
import time

import pytest

from demiflow import data
from demiflow.collect import FetchPolicy, HttpSource
from demiflow.objects import LocalObjectStore
from demiflow.execution.native_queue.queue import QueueChannel, ProducerSourceSpec, task_message
from demiflow.execution.native_queue.queue_commit import task_action
from demiflow.execution.queue_contract import StaleQueueOwner
from demiflow.execution.native_queue.queue_sinks import QueueResultSink
from demiflow.execution.native_queue.raw_budget import RawBudget
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext
from test_queue_v2_protocol import channel, domain, native_parent, publish, source
from test_raw_fetch import origin_server


def test_stalled_publication_then_rollback_has_no_visible_prefix_hole(channel):
    producer = channel.open('p', producer_id='producer', source=source())
    held, release, attempting = threading.Event(), threading.Event(), threading.Event()
    other = QueueChannel(channel.config)
    other._attach()

    def first():
        with pytest.raises(RuntimeError, match='rollback'):
            with channel.database.connection(write=True) as db:
                assert channel.database.publication(db, producer.generation, 'tasks') == 1
                held.set()
                assert release.wait(3)
                raise RuntimeError('rollback')

    def second():
        assert held.wait(2)
        attempting.set()
        with other.database.connection(write=True) as db:
            return other.database.publication(db, producer.generation, 'tasks')

    with ThreadPoolExecutor(2) as pool:
        a, b = pool.submit(first), pool.submit(second)
        assert attempting.wait(2)
        assert channel.snapshot('p', kind='tasks').cursor == 0
        assert not b.done()
        release.set()
        a.result(timeout=3)
        assert b.result(timeout=3) == 1
    assert channel.snapshot('p', kind='tasks').cursor == 1


def test_cumulative_grant_lost_response_old_fence_and_unknown_disk_conservation(channel, tmp_path):
    producer = channel.open('p', producer_id='producer', source=source())
    with native_parent(tmp_path/'seed') as parent:
        publish(parent, channel, producer, 0, [task_message('x', {'value': 1}, pool='p')])
    policy = FetchPolicy(max_object_bytes=32, max_attempts=2)
    with native_parent(tmp_path/'worker') as parent:
        limits = {**parent.context.limits, 'run_budget_owner': parent.context.ref.run_id}
        first = channel.claim(channel.register_worker('p', domain(), {'contract': 'fixture'}, limits))
        action = task_action(parent, channel, 'p', first, 'acquisition', 'raw', {'contract': 'fixture'})
        before = RawBudget(action.context, action.action_id, policy)
        assert before.open({'request': {'uri': 'https://example.org/x'}}, 32, first.domain)
        attempt, remaining = before.start_attempt()
        assert remaining > 0
        assert before.extend(attempt, 0, 128, 1) == 1
        # The first reply was lost. Repeating its exact expected/cumulative grant
        # returns the persisted authorization, with no second body allowance.
        assert before.extend(attempt, 0, 128, 1) == 1
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?', (producer.generation,))
        second = channel.claim(channel.register_worker('p', domain(), {'contract': 'fixture'}, limits))
        with pytest.raises(StaleQueueOwner):
            before.finish_attempt(attempt, 0)
        action = task_action(parent, channel, 'p', second, 'acquisition', 'raw', {'contract': 'fixture'})
        after = RawBudget(action.context, action.action_id, policy)
        assert after.open({'request': {'uri': 'https://example.org/x'}}, 32, second.domain)
        attempt, _ = after.start_attempt()
        assert attempt == 2
        after.extend(attempt, 0, 128, 1)
        after.finish_attempt(attempt, 5)
        after.close_attempt_disk(attempt)
        after.settle_disk(0)
        after.settle_disk(0)
        with channel.database.connection() as db:
            prefix = 'pool-'+producer.generation+'-'
            assert db.execute('SELECT consumed FROM runtime_budgets WHERE name=?', (prefix+'raw_wire_bytes',)).fetchone()[0] == 133
            assert db.execute('SELECT consumed FROM runtime_budgets WHERE name=?', (prefix+'raw_disk_bytes',)).fetchone()[0] == 32
            rows = db.execute('SELECT * FROM runtime_raw_attempts').fetchall()
            assert len(rows) == 2 and max(len(json.dumps(row).encode()) for row in rows) <= 512


def test_control_capacity_is_shared_across_pools(channel, tmp_path):
    limits = replace(channel.config.limits, max_control_bytes=1024**2)
    channel = QueueChannel(replace(channel.config, limits=limits))
    a = channel.open('p', producer_id='a', source=source())
    b = channel.open('q', producer_id='b', source=source())
    value = 'x'*(200*1024)
    try:
        with native_parent(tmp_path/'first') as parent:
            for index in range(2):
                publish(parent, channel, a, index, [task_message('a'+str(index), {'value': value}, pool='p')])
        with native_parent(tmp_path/'second') as parent:
            publish(parent, channel, b, 0, [task_message('b', {'value': value}, pool='q')])
    except (RecoveryContractError, sqlite3.OperationalError) as exc:
        if channel.config.backend == 'postgresql':
            assert 'database-control-bytes' in str(exc)
        else:
            assert 'control' in str(exc) or 'full' in str(exc)
    else:
        pytest.fail('independent pools reset the database control budget')
    with channel.database.connection() as db:
        consumed = db.execute("SELECT consumed FROM runtime_budgets WHERE name='database-control-bytes'").fetchone()[0]
        assert consumed <= limits.max_control_bytes
        assert db.execute("SELECT count(*) FROM queue_tasks WHERE generation=?", (b.generation,)).fetchone()[0] == 0


def test_public_workers_consume_before_seal_without_duplicate_effects(channel, tmp_path):
    spec = ProducerSourceSpec(kind='records', source_id='literals', uri='memory:literals', version=1)
    producer = channel.open('p', producer_id='public', source=spec)
    with origin_server() as (url, calls):
        def consume(index):
            with RunContext.start(tmp_path/('worker-'+str(index))):
                # This checks concurrent consumption before seal, not an idle
                # deadline. Include producer compilation and twelve durable
                # publications; a waiting worker may receive no early task.
                # The 30-second future deadline below still detects a stall.
                (data.read_queue(channel.config, pool='p', max_tasks=4, max_inflight=2, idle_timeout_s=60)
                    .fetch(store=LocalObjectStore(tmp_path/('objects-'+str(index))), concurrency=2,
                        policy=FetchPolicy(max_object_bytes=128*1024, http=HttpSource(allow_private=True)))
                    .write_datasink(QueueResultSink(channel.config, pool='p')))
        with ThreadPoolExecutor(3) as pool:
            futures = [pool.submit(consume, i) for i in range(3)]
            deadline = time.monotonic()+5
            while True:
                with channel.database.connection() as db:
                    started = db.execute('SELECT count(*) FROM queue_workers').fetchone()[0]
                if started == 3:
                    break
                assert time.monotonic() < deadline
                threading.Event().wait(.02)
            rows = [{'messages': [task_message('x'+str(i), {'request': {'uri': url+'/file'}}, pool='p')]} for i in range(12)]
            with RunContext.start(tmp_path/'producer'):
                data.from_items(rows).enqueue(channel.config, producer=producer).run_stream()
            for future in futures:
                future.result(timeout=30)
        assert calls['/file'] == 12
        state = channel.state('p')
        assert state['counts'] == {'done_ok': 12} and not state['queue_drained']
        assert channel.seal(producer)['actual_tasks'] == 12 and channel.state('p')['queue_drained']
        with channel.database.connection() as db:
            assert db.execute('SELECT claimed,settled FROM queue_workers ORDER BY worker_id').fetchall() == [(4,4)]*3
            assert db.execute('SELECT count(*) FROM queue_completions').fetchone()[0] == 12
