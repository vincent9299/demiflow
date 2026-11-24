"""Original queue authority survives buffered dataflow cuts and lost replies."""
from dataclasses import replace

import pytest

from demiflow.execution.native_queue.queue import QueueChannel, QueueConfig, task_message
from demiflow.execution.contracts.queue_contract import StaleQueueOwner, QueueIncomplete
from test_queue_v2_protocol import native_parent, publish, source, worker


def _claimed(tmp_path):
    channel = QueueChannel(QueueConfig(backend='sqlite', queue_id='restore', path=str(tmp_path/'queue.sqlite')))
    producer = channel.open('p', producer_id='producer', source=source())
    with native_parent(tmp_path/'seed') as parent:
        publish(parent, channel, producer, 0, [task_message('first', {'value':1}, pool='p'),
                                              task_message('second', {'value':2}, pool='p')])
        publish(parent, channel, producer, 1, [], eos=True)
    channel.seal(producer)
    owner = worker(channel)
    delivery = channel.claim(owner)
    return channel, delivery


def _restore(channel, delivery, **changes):
    kwargs = dict(generation=delivery.generation, task_id=delivery.task_id,
                  fingerprint=delivery.fingerprint, epoch=delivery.epoch)
    kwargs.update(changes)
    return channel.restore_delivery(delivery.worker_id, **kwargs)


@pytest.mark.parametrize('expired', [False, True])
def test_native_restore_keeps_original_window_and_commit_capacity(tmp_path, expired):
    channel, delivery = _claimed(tmp_path)
    with channel.database.connection() as db:
        before = db.execute('SELECT name,reservation_id,amount FROM runtime_reservations ORDER BY name,reservation_id').fetchall()
    if expired:
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=? AND task_id=?',
                       (delivery.generation, delivery.task_id))
    restored = _restore(channel, delivery)
    assert restored.task_id == delivery.task_id and restored.payload == delivery.payload
    assert restored.epoch == delivery.epoch + int(expired)
    assert (restored.token == delivery.token) is (not expired)
    channel.renew(restored)
    if expired:
        with pytest.raises(StaleQueueOwner): channel.renew(delivery)
    with channel.database.connection() as db:
        assert db.execute('SELECT claimed,settled FROM queue_workers WHERE worker_id=?',
                          (delivery.worker_id,)).fetchone() == (1,0)
        assert db.execute('SELECT name,reservation_id,amount FROM runtime_reservations ORDER BY name,reservation_id').fetchall() == before
        assert db.execute('SELECT task_id,status,epoch FROM queue_tasks ORDER BY ordinal').fetchall() == [
            ('first','leased',1+int(expired)), ('second','pending',0)]


@pytest.mark.parametrize('field,value', [('generation','different'), ('task_id','second'),
    ('fingerprint','0'*64), ('epoch',2)])
def test_native_restore_rejects_changed_identity_without_claim(tmp_path, field, value):
    channel, delivery = _claimed(tmp_path)
    with pytest.raises(StaleQueueOwner):
        _restore(channel, delivery, **{field:value})
    with channel.database.connection() as db:
        assert db.execute('SELECT task_id,status,epoch FROM queue_tasks ORDER BY ordinal').fetchall() == [
            ('first','leased',1), ('second','pending',0)]


def test_native_restore_cannot_adopt_another_live_owner(tmp_path):
    channel, delivery = _claimed(tmp_path)
    stranger = worker(channel)
    with pytest.raises(StaleQueueOwner):
        _restore(channel, replace(delivery, worker_id=stranger))
    channel.renew(delivery)


def test_native_restore_preserves_generation_deadline(tmp_path):
    channel, delivery = _claimed(tmp_path)
    with channel.database.connection(write=True) as db:
        db.execute('UPDATE queue_pools SET deadline=0 WHERE generation=?', (delivery.generation,))
    with pytest.raises(QueueIncomplete, match='elapsed budget'):
        _restore(channel, delivery)


@pytest.mark.parametrize('worker_mode', ['thread', 'process'])
@pytest.mark.parametrize('change_type', [False, True])
def test_completed_native_ack_before_reply_is_reused_by_original_cut(tmp_path, monkeypatch, worker_mode, change_type):
    from demiflow import data
    from demiflow.execution.dataflow_queue import NativeAcquisitionService
    from demiflow.execution.native_queue.queue_sinks import QueueResultSink
    from demiflow.execution.pipeline.run_context import RunContext
    from test_mvp2_queue_unified import _config, _seed_tasks, _make_builder, _records_by_task
    from test_raw_fetch import origin_server
    from runtime_v3_owned_crash import assert_released
    cfg, root = _config(tmp_path, 'native-ack-gap'), tmp_path/'consumer'
    original = NativeAcquisitionService._finalize_io
    fired = []
    async def lose_reply(service, delivery, row):
        fact = await original(service, delivery, row)
        if not fired:
            fired.append(fact['commit_id'])
            raise RuntimeError('native-final-ACK-before-dataflow-reply')
        return fact
    build = _make_builder(1)
    with origin_server() as (url, calls):
        channel, producer = _seed_tasks(cfg, tmp_path/'seed', url, 1)
        with monkeypatch.context() as patch:
            patch.setattr(NativeAcquisitionService, '_finalize_io', lose_reply)
            with pytest.raises(Exception, match='native-final-ACK-before-dataflow-reply'):
                with RunContext.start(root) as run:
                    ref = run.ref
                    with data.local_execution(workers=1, worker_mode=worker_mode, partitions=1):
                        build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
        assert fired and calls['/file'] == 1
        assert channel.completion(producer.generation, 't0')['final_commit'] == fired[0]
        def resume():
            with RunContext.resume(ref) as run:
                with data.local_execution(workers=1, worker_mode=worker_mode, partitions=1):
                    build(cfg, root, url).write_datasink(QueueResultSink(cfg, pool='p'))
                from demiflow.execution.storage.state_artifacts import StateArtifacts
                with run.journal._connection() as db:
                    actions = [r[0] for r in db.execute('SELECT action_id FROM runtime_actions')]
                states = [StateArtifacts(run).read(f['prepared']['state']['dataflow-cut-v3'])
                          for action in actions for f in run.journal.execution_commits(action)
                          if f['status']=='acked' and 'dataflow-cut-v3' in f['prepared']['state']]
                entries = [entry for state in states for entry in state.get('native_queue', {}).get('claims', [])]
                assert entries
                import json
                assert 'token' not in json.dumps(entries)
        if change_type:
            from demiflow.execution.contracts.dataflow_errors import DataflowError
            finalize = NativeAcquisitionService._finalize_impl
            def changed_final_reply(service, command, lease):
                # Fault the newly received finalizer output, not historical
                # bytes. The authenticated original store still binds this new
                # artifact. Python equality alone would accept False -> 0.
                row = service._read_json(command.row_ref, max_bytes=service._services.bounds.max_row_bytes)
                assert row['business_accepted'] is False
                ref = service._put_json({**row, 'business_accepted':0}, media_type='application/json',
                                        max_bytes=service._services.bounds.max_row_bytes)
                return finalize(service, replace(command, row_ref=ref), lease)
            with monkeypatch.context() as patch:
                patch.setattr(NativeAcquisitionService, '_finalize_impl', changed_final_reply)
                with pytest.raises(DataflowError, match='Restored final row differs') as failure:
                    resume()
            assert failure.value.code == 'UNKNOWN_EFFECT'
        else:
            resume()
        assert calls['/file'] == 1
    records = _records_by_task(cfg, channel)
    assert list(records) == ['t0'] and records['t0']['business_accepted'] is False
    assert channel.state('p')['counts'] == {'done_ok':1}
    with channel.database.connection() as db:
        assert db.execute('SELECT epoch FROM queue_tasks').fetchone() == (1,)
        assert db.execute('SELECT claimed,settled FROM queue_workers').fetchone() == (1,1)
    if worker_mode == 'process':
        assert_released(root)
    else:
        from types import SimpleNamespace
        from demiflow.resources.thread import ThreadResourceProvider
        from demiflow.execution.hosts.capacity import local_capacity
        from demiflow.resources.capacity import LocalCapacityPool
        from demiflow.execution.journal.run_journal import RunJournal
        domain, capacity = local_capacity()
        pool = LocalCapacityPool(domain/'capacity', capacity=capacity)
        provider = ThreadResourceProvider(pool, root.with_name(root.name+'-driver-resources')/'thread-units',
                                          control=SimpleNamespace(units={}))
        with RunJournal(root)._connection() as db:
            allocations = db.execute('SELECT scope_id,allocation_id,cleanup FROM resource_allocations').fetchall()
            assert allocations and all(r[2]=='CONFIRMED' for r in allocations)
            assert all(r[0]=='CLOSED' for r in db.execute('SELECT state FROM resource_scopes'))
        for scope in {r[0] for r in allocations}:
            observed = provider.inspect(scope, None)
            assert observed['next_page_token'] is None
            assert observed['allocations'] and all(r['phase']=='TERMINATED' and r['observation']=='FRESH'
                                                   for r in observed['allocations'])
        snapshot = pool.snapshot()
        assert not snapshot['active'] and snapshot['charged'] == {'cpu_millis':0,'memory_bytes':0}
