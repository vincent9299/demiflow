"""Backend time must be rechecked after waiting for the common budget row."""
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest
from demiflow.execution.queue_commit import task_action
from demiflow.execution.queue_contract import StaleQueueOwner, QueueIncomplete
from demiflow.execution.native_queue.queue_sql import QueueDatabase
from test_queue_v2_protocol import channel, domain, native_parent, publish, source, worker
from demiflow.execution.queue import task_message


@pytest.mark.parametrize('operation',['scope','claim'])
def test_budget_row_wait_cannot_extend_lease_or_generation_deadline(channel,tmp_path,monkeypatch,operation):
    if channel.config.backend!='postgresql':pytest.skip('row-level lock wait interleaving is PostgreSQL-specific')
    producer=channel.open('p',producer_id='producer',source=source())
    with native_parent(tmp_path/'seed') as parent:
        publish(parent,channel,producer,0,[task_message('x',{'value':1},pool='p')])
    worker_id=worker(channel)
    delivery=channel.claim(worker_id) if operation=='scope' else None
    # Accelerate only test-owned clock facts; the public lease minimum stays 30s.
    with channel.database.connection(write=True) as db:
        until=float(db.now())+.5
        if operation=='scope':db.execute('UPDATE queue_tasks SET lease_until=? WHERE generation=?',(until,producer.generation))
        else:db.execute('UPDATE queue_pools SET deadline=? WHERE generation=?',(until,producer.generation))
    original=QueueDatabase.lock_budgets;waiting=threading.Event()
    def observed(db):
        waiting.set()
        original(db)
    monkeypatch.setattr(QueueDatabase,'lock_budgets',staticmethod(observed))
    with native_parent(tmp_path/'owner') as parent:
        def mutate():
            if operation=='scope':return task_action(parent,channel,'p',delivery,'acquisition','raw',{'contract':'fixture'})
            return channel.claim(worker_id)
        with ThreadPoolExecutor(1) as pool:
            with channel.database.connection(write=True) as held:
                original(held)
                future=pool.submit(mutate)
                assert waiting.wait(2)
                threading.Event().wait(.65)
            with pytest.raises(StaleQueueOwner if operation=='scope' else QueueIncomplete):
                future.result(timeout=2)
    with channel.database.connection() as db:
        assert db.execute('SELECT count(*) FROM runtime_commit_scopes WHERE parent_action_id LIKE ?',('task-%',)).fetchone()[0]==0
        if operation=='claim':
            assert db.execute('SELECT status,epoch FROM queue_tasks').fetchone()==('pending',0)
