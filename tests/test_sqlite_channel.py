import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from demiflow import data
from demiflow.execution.sqlite_channel import SQLiteChannel, channel_config


def message(key, value, pool='work'):
    return {'task_id':key,'fingerprint':key,'pool':pool,'payload':{'value':value}}


@pytest.mark.parametrize('results', [False, True])
def test_archive_next_record_has_bounded_sql_work_for_large_pool(tmp_path, monkeypatch, results):
    """A narrow cursor must not sort thousands of unrelated tasks per record."""
    from contextlib import contextmanager
    from itertools import islice
    queue = SQLiteChannel(**channel_config(tmp_path / 'archive.sqlite'))
    queue.open('work')
    # A compact platform fixture isolates scan cost from HTTP and producer cost.
    with queue._connection(write=True) as db:
        db.executemany('INSERT INTO tasks(task_id,payload_json,pool,budget_class,cost,max_attempts) '
                       'VALUES (?, ?, ?, ?, 0, 3)',
                       [(str(i), '{"value":' + str(i) + '}', 'work' if i % 2 else 'other', 'default')
                        for i in range(5000)])
        db.executemany('INSERT INTO completions(task_id,worker,token,completed_at,cost,result_json) '
                       'VALUES (?, ?, ?, 0, 0, ?)',
                       [(str(i), 'fixture', 'fixture', '{"value":' + str(i) + '}') for i in range(5000)])
    connection = queue._connection

    @contextmanager
    def bounded_connection(**kwargs):
        with connection(**kwargs) as db:
            # The old plan exceeds this budget before yielding the first row.
            db.set_progress_handler(lambda: 1, 10000)
            yield db

    monkeypatch.setattr(queue, '_connection', bounded_connection)
    records = list(islice(queue.scan('work', through=4000, results=results), 3))
    assert [r['value']['value'] for r in records] == [1, 3, 5]
    assert all(r['sequence'] <= 4000 for r in records)


def test_paused_archive_releases_wal_snapshot(tmp_path):
    import sqlite3
    settings = channel_config(tmp_path / 'wal.sqlite')
    queue = SQLiteChannel(**settings)
    queue.open('work')
    queue.put([message('a', 1), message('b', 2)])
    rows = queue.scan('work', through=queue.high_water('work'))
    assert next(rows)['value'] == {'value': 1}
    queue.put([message('c', 3)])
    with sqlite3.connect(settings['path'], timeout=.1) as db:
        assert db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone() == (0, 0, 0)
    assert [r['value'] for r in rows] == [{'value': 2}]


def test_archive_iterator_can_transfer_between_dataset_workers(tmp_path):
    queue = SQLiteChannel(**channel_config(tmp_path / 'threads.sqlite'))
    queue.open('work')
    queue.put([message('a', 1), message('b', 2)])
    rows = queue.scan('work', through=queue.high_water('work'))
    with ThreadPoolExecutor(1) as first, ThreadPoolExecutor(1) as second:
        assert first.submit(next, rows).result()['value'] == {'value': 1}
        assert second.submit(next, rows).result()['value'] == {'value': 2}
    rows.close()  # The action owner may close a generator pulled by an IO worker.


def test_live_consumer_and_archive_have_independent_progress(tmp_path):
    settings=channel_config(tmp_path/'queue.sqlite')
    queue=SQLiteChannel(**settings);queue.open('work')
    processed=threading.Event();seen=[]
    def operate(row):
        if row['_queue_result'] is not None:
            return {**row,'queue_result':row['_queue_result']}
        seen.append(row['value']);processed.set()
        return {**row,'queue_result':{'answer':row['value']*2}}
    def consume():
        return data.read_queue(settings,pool='work').map(operate).ack_queue(settings).run_stream()
    with ThreadPoolExecutor(1) as pool:
        future=pool.submit(consume)
        data.from_items([{'messages':[message('a',3)]}]).enqueue(settings).run_stream()
        assert processed.wait(5),'consumer must run before producer seals'
        assert not future.done()
        frozen=queue.high_water('work')
        assert list(queue.scan('work',through=frozen))[0]['value']=={'value':3}
        queue.seal('work');future.result(timeout=10)
    assert queue.state('work')['done']==1
    results=data.read_queue_records(settings,pool='work',through=queue.high_water('work',results=True),results=True).take_all()
    assert results[0]['value']=={'answer':6}
    consume()
    assert seen==[3],'replaying a completed task must use its stored result'
    assert queue.snapshot()['workers']==[]


def test_atomic_capacity_identity_and_immutable_high_water(tmp_path):
    settings=channel_config(tmp_path/'queue.sqlite',max_tasks=2,max_payload_bytes=1024)
    queue=SQLiteChannel(**settings);queue.open('work')
    queue.put([message('a',1)]);upper=queue.high_water('work')
    queue.put([message('a',2)])  # explicitly equivalent fingerprint retains first provenance
    with pytest.raises(ValueError,match='identity'):
        queue.put([{**message('a',2),'fingerprint':'changed'}])
    with pytest.raises(ValueError,match='budget'):
        queue.put([message('b',2),message('c',3)])
    assert queue.state('work')['pending']==1
    with pytest.raises(ValueError,match='byte budget'):
        queue.put([message('b','x'*1024)])
    queue.put([message('b',2)])
    assert [r['value'] for r in queue.scan('work',through=upper)]==[{'value':1}]
    queue.seal('work')
    with pytest.raises(ValueError,match='not open'):queue.put([message('c',3)])


def test_failure_before_ack_retains_task_and_retry_is_explicit(tmp_path):
    settings=channel_config(tmp_path/'queue.sqlite')
    queue=SQLiteChannel(**settings);queue.open('work');queue.put([message('a',1)]);queue.seal('work')
    def fail(row):raise RuntimeError('failure before acknowledgement')
    with pytest.raises(RuntimeError,match='before acknowledgement'):
        data.read_queue(settings,pool='work').map(fail).ack_queue(settings).run_stream(source_batch_size=1)
    assert queue.state('work')['pending']==1
    assert queue.high_water('work',results=True)==0
    assert queue.snapshot()['workers']==[]
    (data.read_queue(settings,pool='work').map(lambda r:{**r,'queue_result':{'ok':True}})
        .ack_queue(settings).run_stream(source_batch_size=1))
    assert queue.state('work')['done']==1


def test_open_idle_channel_has_finite_wait_and_closes_worker(tmp_path):
    settings=channel_config(tmp_path/'queue.sqlite')
    queue=SQLiteChannel(**settings);queue.open('work')
    with pytest.raises(TimeoutError,match='idle deadline'):
        data.read_queue(settings,pool='work',idle_timeout_s=.1).ack_queue(settings).run_stream(source_batch_size=1)
    assert queue.snapshot()['workers']==[]


def test_oversized_result_is_not_acknowledged(tmp_path):
    settings=channel_config(tmp_path/'queue.sqlite',max_result_bytes=1024)
    queue=SQLiteChannel(**settings);queue.open('work');queue.put([message('a',1)]);queue.seal('work')
    with pytest.raises(ValueError,match='byte budget'):
        (data.read_queue(settings,pool='work').map(lambda r:{**r,'queue_result':{'text':'x'*1024}})
            .ack_queue(settings).run_stream(source_batch_size=1))
    assert queue.state('work')['pending']==1
    assert queue.high_water('work',results=True)==0


def test_readonly_archive_never_creates_missing_queue(tmp_path):
    import sqlite3
    from demiflow.queue import queue_status
    settings=channel_config(tmp_path/'missing.sqlite')
    with pytest.raises(sqlite3.OperationalError):queue_status(settings,pool='work')
    assert not (tmp_path/'missing.sqlite').exists()


def test_sealed_budget_blocked_queue_fails_without_dispatch(tmp_path):
    settings=channel_config(tmp_path/'queue.sqlite')
    queue=SQLiteChannel(**settings);queue.open('work');queue.set_budget('calls',0)
    queue.put([{**message('a',1),'budget_class':'calls','cost':1}]);queue.seal('work')
    with pytest.raises(RuntimeError,match='budget-blocked'):
        data.read_queue(settings,pool='work').ack_queue(settings).run_stream()
    assert queue.state('work')['pending']==1
    assert queue.snapshot()['workers']==[]


def test_process_crash_reclaims_only_dead_worker(tmp_path):
    import subprocess
    import sys
    import time
    settings=channel_config(tmp_path/'queue.sqlite')
    queue=SQLiteChannel(**settings);queue.open('work')
    queue.put([message('a',1)])
    child=subprocess.run([sys.executable,'-c',
        'import sys; from demiflow.queue import SQLiteChannel; q=SQLiteChannel(sys.argv[1]); '
        'w=q.register_worker("work"); assert len(q.claim(w,limit=1))==1',settings['path']],
        timeout=30,capture_output=True,text=True)
    assert child.returncode==0,child.stderr
    assert queue.state('work')['running']==1
    time.sleep(1.05)
    queue.seal('work')
    (data.read_queue(settings,pool='work').map(lambda r:{**r,'queue_result':{'value':r['value']}})
        .ack_queue(settings).run_stream())
    assert queue.state('work')['done']==1
    assert queue.snapshot()['workers']==[]


def test_filtered_claim_hits_idle_deadline_and_is_retained(tmp_path):
    settings=channel_config(tmp_path/'queue.sqlite')
    queue=SQLiteChannel(**settings);queue.open('work')
    queue.put([message('a',1)]);queue.seal('work')
    with pytest.raises(TimeoutError,match='idle deadline'):
        (data.read_queue(settings,pool='work',idle_timeout_s=.2).filter(lambda r:False)
            .ack_queue(settings).run_stream())
    assert queue.state('work')['pending']==1
    assert queue.snapshot()['workers']==[]


def test_database_full_keeps_prior_transactions(tmp_path):
    import sqlite3
    settings=channel_config(tmp_path/'queue.sqlite',max_file_bytes=1024**2,max_payload_bytes=1024**2)
    queue=SQLiteChannel(**settings);queue.open('work')
    written=0
    with pytest.raises(sqlite3.OperationalError,match='full'):
        for number in range(16):
            queue.put([message(str(number),'x'*120000)])
            written+=1
    assert 0<written<16
    assert queue.state('work')['pending']==written
    assert len(list(queue.scan('work',through=queue.high_water('work'))))==written


def test_partial_publication_keeps_bounded_commits_replayable(tmp_path):
    settings=channel_config(tmp_path/'queue.sqlite',max_tasks=32)
    queue=SQLiteChannel(**settings);queue.open('work')
    messages=[message(str(i),i) for i in range(33)]
    with pytest.raises(ValueError,match='task budget'):
        data.from_items([{'messages':messages}]).enqueue(settings).run_stream()
    assert queue.state('work')['pending']==32
    data.from_items([{'messages':messages[:32]}]).enqueue(settings).run_stream()
    assert queue.state('work')['pending']==32
