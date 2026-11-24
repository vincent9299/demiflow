"""Resource admission across windows, transactions and one native run."""
from contextlib import ExitStack
from dataclasses import replace
import threading
import time

import pytest

from demiflow import data
from demiflow.collect import FetchPolicy, HttpSource
from demiflow.objects import LocalObjectStore
from demiflow.execution.native_queue.queue import QueueChannel, ProducerSourceSpec, task_message
from demiflow.execution.queue_contract import QueueIncomplete
from demiflow.execution.native_queue.queue_sinks import QueueResultSink
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext
from test_queue_v2_protocol import channel, native_parent, publish, source, worker
from test_raw_fetch import origin_server


def bounded(channel, **limits):
    return QueueChannel(replace(channel.config, limits=replace(channel.config.limits, **limits)))


def literal_source():
    return ProducerSourceSpec(kind='records', source_id='literal-records', uri='memory:records', version=1)


def seed(channel, tmp_path, url='https://example.org', n=2):
    producer = channel.open('p', producer_id='seed', source=source())
    with native_parent(tmp_path) as parent:
        publish(parent, channel, producer, 0, [task_message('x'+str(i),
            {'request': {'uri': url+'/file'}}, pool='p') for i in range(n)])
        publish(parent, channel, producer, 1, [], eos=True)
    channel.seal(producer)
    return producer


def consume(channel, root):
    return data.read_queue(channel.config, pool='p', max_tasks=1, max_inflight=1,
        idle_timeout_s=.2, lease_s=30).fetch(store=LocalObjectStore(root), concurrency=1,
        policy=FetchPolicy(max_object_bytes=128*1024, http=HttpSource(allow_private=True))
    ).write_datasink(QueueResultSink(channel.config, pool='p'))


def test_pool_active_limit_allows_expired_recovery_but_no_new_lease(channel, tmp_path):
    channel = bounded(channel, max_active_tasks=1)
    producer = seed(channel, tmp_path)
    one, two = worker(channel, 'one'), worker(channel, 'two')
    first = channel.claim(one)
    assert channel.claim(two) is None
    with channel.database.connection(write=True) as db:
        db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=? AND task_id=?',
            (producer.generation, first.task_id))
    replacement = channel.claim(two)
    assert replacement.task_id == first.task_id and replacement.epoch == first.epoch+1
    assert channel.state('p')['counts'] == {'leased': 1, 'pending': 1}


def test_claim_budget_and_backend_deadline_cannot_be_reset(channel, tmp_path):
    channel = bounded(channel, max_claims_per_task=1)
    producer = seed(channel, tmp_path)
    first = channel.claim(worker(channel, 'one'))
    with channel.database.connection(write=True) as db:
        db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?', (producer.generation,))
    with pytest.raises(QueueIncomplete, match='claim budget'):
        channel.claim(worker(channel, 'two'))
    with channel.database.connection(write=True) as db:
        assert db.execute('SELECT epoch FROM queue_tasks WHERE generation=? AND task_id=?',
            (producer.generation, first.task_id)).fetchone()[0] == 1
        db.execute('UPDATE queue_pools SET deadline=0 WHERE generation=?', (producer.generation,))
    with pytest.raises(QueueIncomplete, match='elapsed budget'):
        channel.claim(worker(channel, 'three'))
    assert channel.state('p')['counts'] == {'leased': 1, 'pending': 1}


def test_result_total_limit_keeps_raw_but_never_acknowledges(channel, tmp_path):
    channel = bounded(channel, max_result_total_bytes=1)
    with origin_server() as (url, calls):
        producer = seed(channel, tmp_path, url, n=1)
        with pytest.raises(RecoveryContractError, match='result_row_bytes'):
            consume(channel, tmp_path/'objects')
        assert calls['/file'] == 1 and channel.completion(producer.generation, 'x0') is None
        with channel.database.connection() as db:
            assert db.execute('SELECT count(*) FROM queue_results').fetchone()[0] == 0
            raw = db.execute("SELECT status FROM runtime_commits WHERE action_id=(SELECT acquisition_scope FROM queue_tasks WHERE generation=?)",
                (producer.generation,)).fetchone()
            assert raw == ('acked',)


def test_one_run_shares_producer_and_consumer_commit_allowance(channel, tmp_path):
    with origin_server() as (url, calls):
        producer = channel.open('p', producer_id='same-run', source=literal_source())
        rows = [{'messages': [task_message('x'+str(i), {'request': {'uri': url+'/file'}}, pool='p') for i in range(2)]}]
        with pytest.raises(RecoveryContractError, match='commit capacity'):
            with RunContext.start(tmp_path/'same-run', limits={'max_commits': 5}) as run:
                data.from_items(rows).enqueue(channel.config, producer=producer).run_stream()
                channel.seal(producer)
                consume(channel, tmp_path/'objects')
                consume(channel, tmp_path/'objects')
        with channel.database.connection() as db:
            assert db.execute('SELECT consumed FROM runtime_budgets WHERE name=?',
                ('run-'+run.ref.run_id+'-queue_commit_admission',)).fetchone()[0] == 5
        assert calls['/file'] == 1
        assert channel.state('p')['counts'] == {'done_ok': 1, 'pending': 1}


def test_one_run_cannot_mix_queue_database_authorities(channel, tmp_path):
    first = channel.open('p', producer_id='first', source=literal_source())
    config = channel.config
    if config.backend == 'sqlite':
        other = QueueChannel(replace(config, path=str(tmp_path/'other.sqlite')))
    else:
        other = QueueChannel(replace(config, namespace=config.namespace+'_b'))
    second = other.open('p', producer_id='second', source=literal_source())
    with pytest.raises(RecoveryContractError, match='independent queue budget'):
        with RunContext.start(tmp_path/'one-run'):
            data.from_items([]).enqueue(channel.config, producer=first).run_stream()
            data.from_items([]).enqueue(other.config, producer=second).run_stream()
    with other.database.connection() as db:
        assert db.execute('SELECT eos FROM queue_pools').fetchone()[0] == 0


def test_queue_authority_does_not_grant_local_durable_effects(channel, tmp_path):
    producer = channel.open('p', producer_id='producer', source=literal_source())
    with RunContext.start(tmp_path/'one-run'):
        data.from_items([]).enqueue(channel.config, producer=producer).run_stream()
        with pytest.raises(RecoveryContractError, match='local durable actions'):
            data.from_items([{'value': 1}]).materialize()
    assert not list((tmp_path/'one-run').glob('**/*.arrow'))


def test_database_connection_admission_is_bounded_and_released(channel):
    channel = bounded(channel, db_timeout_s=.05)
    channel.open('p', producer_id='producer', source=source())
    with ExitStack() as stack:
        for _ in range(2):
            db = stack.enter_context(channel.database.connection())
            if db.backend == 'postgresql':
                # Isolate connection admission from the shorter idle-TX timeout.
                db.execute("SELECT set_config('idle_in_transaction_session_timeout','1000',true)")
        started = time.monotonic()
        with pytest.raises(RecoveryContractError, match='connection budget'):
            with channel.database.connection():
                pytest.fail('third simultaneous connection was admitted')
        assert time.monotonic()-started < 1
    with channel.database.connection() as db:
        assert db.execute('SELECT count(*) FROM queue_pools').fetchone()[0] == 1


@pytest.mark.parametrize('terminal', ['materialize', 'take_all', 'count'])
def test_enqueue_rejects_source_capture_terminals_before_publication(channel, tmp_path, terminal):
    producer = channel.open('p', producer_id='producer', source=literal_source())
    ds = data.from_items([{'messages': [task_message('x', {'value': 1}, pool='p')]}]).enqueue(channel.config, producer=producer)
    with pytest.raises(RecoveryContractError, match='requires run_stream'):
        getattr(ds, terminal)()
    assert channel.state('p')['tasks'] == 0
    with channel.database.connection() as db:
        assert db.execute('SELECT count(*) FROM runtime_commits').fetchone()[0] == 0


def test_hash_verification_does_not_block_lease_renewal_or_other_claim(channel, tmp_path, monkeypatch):
    entered, renewed = threading.Event(), threading.Event()
    original_verify = QueueResultSink.verify_acquisition
    original_renew = QueueChannel.renew
    claims = []

    def renew(self, delivery, **kwargs):
        result = original_renew(self, delivery, **kwargs)
        if entered.is_set():
            renewed.set()
        return result

    def verify(self, prepared):
        original_verify(self, prepared)
        if entered.is_set():
            return
        store = self.action.context.journal
        other = store.channel.register_worker('p', store.delivery.domain, store.declaration['consumer'], {'max_commits': 10})
        started = time.monotonic()
        delivery = store.channel.claim(other, lease_s=30)
        claims.append((delivery.task_id, time.monotonic()-started))
        store.channel.release(delivery)
        entered.set()
        assert renewed.wait(12), 'long verification blocked the independent lease I/O pool'

    monkeypatch.setattr(QueueChannel, 'renew', renew)
    # The fault is driver-owned. Install it after the immutable worker program
    # has been sealed, so this probe does not change the program's code pin.
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    start = DataflowExecution.start
    def activate(self):
        self.prepare()
        monkeypatch.setattr(QueueResultSink, 'verify_acquisition', verify)
        return start(self)
    monkeypatch.setattr(DataflowExecution, 'start', activate)
    with origin_server() as (url, calls):
        seed(channel, tmp_path, url)
        consume(channel, tmp_path/'objects')
        assert calls['/file'] == 1 and renewed.is_set()
        assert claims[0][0] == 'x1' and claims[0][1] < 2
        assert channel.state('p')['counts'] == {'done_ok': 1, 'pending': 1}


def test_stop_after_request_admission_prevents_new_body_grants(channel,tmp_path,monkeypatch):
    from demiflow.execution.native_queue.raw_budget import RawBudget
    stop=tmp_path/'stop'
    original=RawBudget.extend
    def interrupt(self,attempt,expected_grant,granted,requests):
        result=original(self,attempt,expected_grant,granted,requests)
        if requests==1 and granted==0:stop.touch()
        return result
    monkeypatch.setattr(RawBudget,'extend',interrupt)
    with origin_server() as (url,calls):
        producer=seed(channel,tmp_path,url,n=1)
        with pytest.raises(QueueIncomplete):
            (data.read_queue(channel.config,pool='p',max_tasks=1,max_inflight=1,stop=str(stop))
                .fetch(store=LocalObjectStore(tmp_path/'objects'),concurrency=1,
                    policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True)))
                .write_datasink(QueueResultSink(channel.config,pool='p')))
        assert calls['/file']<=1 and channel.completion(producer.generation,'x0') is None
        assert not list((tmp_path/'objects').glob('**/.pending/*'))
