from concurrent.futures import ThreadPoolExecutor
import sqlite3
import pytest
from demiflow.collect.sqlite_queue import SQLiteQueue, require_local_storage
from demiflow.collect.embedded_worker import run_worker, PoolPolicy


def test_48_workers_claim_exactly_once(tmp_path):
    queue = SQLiteQueue(tmp_path/'tasks.sqlite')
    assert queue.add({'task_id': str(i), 'payload': {'i': i}} for i in range(600)) == 600
    def consume(_):
        worker = queue.register_worker()
        seen=[]
        while handles := queue.claim(worker, limit=8):
            for handle in handles:
                queue.complete(handle, {'artifact': handle.task_id})
                seen.append(handle.task_id)
        queue.unregister_worker(worker)
        return seen
    with ThreadPoolExecutor(max_workers=48) as pool:
        rows=[task for batch in pool.map(consume,range(48)) for task in batch]
    assert len(rows)==len(set(rows))==600
    assert queue.reconcile(lambda artifact: True)==[]


def test_budget_retry_dead_letter_and_pool_isolation(tmp_path):
    queue=SQLiteQueue(tmp_path/'tasks.sqlite',clock=lambda:1000)
    queue.set_budget('net', 10)
    queue.add([{'task_id': str(i), 'payload': i, 'pool':'a' if i<3 else 'b', 'budget_class':'net','cost':6} for i in range(4)],max_attempts=2)
    a,b=queue.register_worker('a'),queue.register_worker('b')
    handles=queue.claim(a)
    assert len(handles)==1
    assert not queue.claim(b)
    assert queue.fail(handles[0],'retry',backoff_s=1)=='pending'
    assert queue.claim(a)[0].task_id != handles[0].task_id
    queue.set_budget('net',30)
    handle=queue.claim(b)[0]
    queue.complete(handle,{'ok':True})
    with pytest.raises(ValueError):queue.fail(handle,'stale')


def test_recovery_needs_dead_pid_and_stale_heartbeat(tmp_path,monkeypatch):
    import demiflow.collect.sqlite_queue as module
    now=[1000]
    queue=SQLiteQueue(tmp_path/'tasks.sqlite',clock=lambda:now[0])
    queue.add([{'task_id':'a','payload':1}])
    worker=queue.register_worker();old=queue.claim(worker)[0]
    now[0]+=1000
    assert queue.reclaim()==[]  # current pid remains alive
    monkeypatch.setattr(module,'process_identity',lambda pid:None)
    assert queue.reclaim()==['a']
    with pytest.raises(ValueError):queue.complete(old,{'bad':True})


def test_snapshot_is_consistent_and_validates_artifacts(tmp_path):
    queue=SQLiteQueue(tmp_path/'local.sqlite')
    queue.add([{'task_id':'x','payload':1}])
    run_worker(queue,lambda h: {'id':h.task_id},lambda value: True,exit_when_idle=True)
    queue.backup(tmp_path/'snapshot.sqlite')
    with sqlite3.connect(tmp_path/'snapshot.sqlite') as db:
        assert db.execute("SELECT count(*) FROM tasks WHERE state='done'").fetchone()[0]==1
    assert queue.reconcile(lambda value:False)==[{'task_id':'x','reason':'artifact_missing_or_invalid'}]


def test_failed_validation_never_completes(tmp_path):
    queue=SQLiteQueue(tmp_path/'local.sqlite')
    queue.add([{'task_id':'x','payload':1}],max_attempts=1)
    run_worker(queue,lambda h:{},lambda value:False,exit_when_idle=True)
    assert queue.snapshot()['states']==[{'pool':'default','state':'failed','count':1}]


@pytest.mark.parametrize('filesystem', ['nfs4', 'dpc'])
def test_network_disk_rejected_before_creation(tmp_path, filesystem):
    mountinfo=tmp_path/'mountinfo';mountinfo.write_text(f'1 0 0:1 / / rw - {filesystem} server:/ rw\n')
    with pytest.raises(ValueError,match='local storage'):
        require_local_storage(tmp_path/'queue.sqlite',mountinfo=mountinfo)
    assert not (tmp_path/'queue.sqlite').exists()


def test_pool_policies_do_not_share_counts():
    a,b=PoolPolicy(maximum=4),PoolPolicy(maximum=2)
    assert a.observe(10)==2
    assert b.target==1
    assert a.observe(9)==1


def test_fallback_budget_transfer_is_atomic_and_retry_definition_is_immutable(tmp_path):
    queue = SQLiteQueue(tmp_path/'queue.sqlite')
    queue.set_budget('primary',10)
    queue.set_budget('fallback',5)
    task = {'task_id':'x','payload':1,'budget_class':'primary','cost':6}
    queue.add([task]);worker=queue.register_worker();handle=queue.claim(worker)[0]
    with pytest.raises(ValueError, match='budget exhausted'):
        queue.transfer_budget(handle,'fallback',6)
    budgets={row['class']:row for row in queue.snapshot()['budgets']}
    assert budgets['primary']['reserved']==6 and budgets['fallback']['reserved']==0
    queue.set_budget('fallback',10)
    queue.transfer_budget(handle,'fallback',6)
    queue.complete(handle,{'ok':True},actual_cost=5)
    queue.complete(handle,{'ok':True},actual_cost=5)
    with pytest.raises(ValueError):
        queue.complete(handle,{'ok':True},actual_cost=4)
    budgets={row['class']:row for row in queue.snapshot()['budgets']}
    assert budgets['primary']['spent']==0 and budgets['fallback']['spent']==5
    assert all(row['reserved']==0 for row in budgets.values())
    assert queue.add([task])==0
