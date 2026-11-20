"""Platform provenance cannot authorize or refuse an original business contract."""
import copy
import json
from types import SimpleNamespace

import pytest

from demiflow.execution.queue import QueueChannel, QueueConfig
from demiflow.execution.queue_contract import resolve_consumer_contract, resolve_producer_contract
from demiflow.execution.recovery_contract import RecoveryContractError, identity, document
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from test_queue_v2_protocol import source, native_parent, publish, domain, task_message


def parent(revision, *, replaying=False):
    return SimpleNamespace(replaying=replaying,context=SimpleNamespace(journal=SimpleNamespace(
        recovery_identity=lambda: {'platform_revision': revision})))


@pytest.fixture
def queue(tmp_path):
    channel=QueueChannel(QueueConfig(backend='sqlite',queue_id='fixture',path=str(tmp_path/'q.sqlite')))
    producer=channel.open('p',producer_id='producer',source=source())
    return channel,producer


def consumer():
    return dict(protocol='demiflow.queue.consumer.v3',operations=[{'business':'original'}],result='result',settlement='sink')


def test_resume_uses_original_configuration_without_current_source_scan(tmp_path, monkeypatch):
    from demiflow.execution import recovery_identity
    with pytest.raises(RuntimeError,match='interrupt'):
        with RunContext.start(tmp_path/'run',entry_identity='business') as run:
            original=run.journal.recovery_identity()
            reference=run.ref
            raise RuntimeError('interrupt')
    monkeypatch.setattr(recovery_identity,'platform_identity',lambda: (_ for _ in ()).throw(AssertionError('resume scanned platform')))
    with pytest.raises(RecoveryContractError,match='entry identity'):
        with RunContext.resume(reference,entry_identity='changed'): pass
    with RunContext.resume(reference,entry_identity='business') as run:
        assert run.journal.recovery_identity()==original
        assert run.ref==reference
    assert RunJournal(reference.directory).execution_status()['status']=='succeeded'


def test_new_consumer_and_producer_contracts_do_not_depend_on_platform(queue):
    channel,p=queue
    src=SimpleNamespace(config=channel.config,pool='p')
    body=consumer()
    assert resolve_consumer_contract(src,body,action=parent('old'))==body
    channel.register_worker('p',domain(),body,{'max_commits':10})
    assert resolve_consumer_contract(src,body,action=parent('new'))==body
    producer_body=dict(protocol='demiflow.queue.producer-transform.v3',source={'sha':'fixed'},operations=[],specification={},messages='messages')
    sha=resolve_producer_contract(channel,p,producer_body,parent('old'))
    with channel.database.connection(write=True) as db:
        db.execute('UPDATE queue_pools SET producer_contract=? WHERE generation=?',(sha,p.generation))
    assert resolve_producer_contract(channel,p,producer_body,parent('new'))==sha


def test_old_hash_only_original_run_and_business_refusal(queue):
    channel,p=queue;src=SimpleNamespace(config=channel.config,pool='p');body=consumer()
    old={**body,'protocol':'demiflow.queue.consumer.v2','platform':'original'}
    channel.register_worker('p',domain(),old,{'max_commits':10})
    with channel.database.connection() as db:
        before=list(db.execute('SELECT * FROM queue_pools')); counts=list(db.execute('SELECT * FROM queue_workers'))
    assert resolve_consumer_contract(src,body,action=parent('original',replaying=True))==old
    changed={**body,'result':'different'}
    with pytest.raises(RecoveryContractError,match='business contract'):
        resolve_consumer_contract(src,changed,action=parent('original',replaying=True))
    with pytest.raises(RecoveryContractError,match='material is unavailable'):
        resolve_consumer_contract(src,body,action=parent('new'))
    with channel.database.connection() as db:
        assert list(db.execute('SELECT * FROM queue_pools'))==before
        assert list(db.execute('SELECT * FROM queue_workers'))==counts
        assert not db.execute('SELECT 1 FROM runtime_commit_scopes').fetchall()


def test_old_full_consumer_cross_run_uses_original_scope(queue,tmp_path):
    from demiflow.execution.queue_commit import task_action
    channel,p=queue;src=SimpleNamespace(config=channel.config,pool='p');body=consumer()
    old={**body,'protocol':'demiflow.queue.consumer.v2','platform':'original'}
    with native_parent(tmp_path/'fixture') as action:
        publish(action,channel,p,0,[task_message('task',{'request':{'uri':'https://invalid.example'}},pool='p')])
        worker=channel.register_worker('p',domain(),old,action.context.limits)
        delivery=channel.claim(worker)
        scoped=task_action(action,channel,'p',delivery,'acquisition','node-0',old)
        with channel.database.connection() as db:
            original_scope=db.execute('SELECT declaration FROM runtime_commit_scopes WHERE scope_id=?',(scoped.action_id,)).fetchone()[0]
        assert resolve_consumer_contract(src,body,action=parent('another'))==old
        with pytest.raises(RecoveryContractError,match='business contract'):
            resolve_consumer_contract(src,{**body,'settlement':'explicit'},action=parent('another'))
        with channel.database.connection() as db:
            assert db.execute('SELECT declaration FROM runtime_commit_scopes WHERE scope_id=?',(scoped.action_id,)).fetchone()[0]==original_scope


def test_old_producer_reconstructs_hash_and_preserves_business(queue):
    channel,p=queue
    body=dict(protocol='demiflow.queue.producer-transform.v3',source={'sha':'fixed'},operations=[],specification={},messages='messages')
    old={**body,'protocol':'demiflow.queue.producer-transform.v2','platform':'original'}
    sha=identity(old)
    with channel.database.connection(write=True) as db:
        db.execute('UPDATE queue_pools SET producer_contract=? WHERE generation=?',(sha,p.generation))
    assert resolve_producer_contract(channel,p,body,parent('original',replaying=True))==sha
    with pytest.raises(RecoveryContractError,match='contract changed'):
        resolve_producer_contract(channel,p,{**body,'source':{'sha':'changed'}},parent('original',replaying=True))
    with pytest.raises(RecoveryContractError,match='material is unavailable'):
        resolve_producer_contract(channel,p,body,parent('other'))
    with channel.database.connection() as db:
        assert db.execute('SELECT producer_contract FROM queue_pools').fetchone()[0]==sha
        assert not db.execute('SELECT 1 FROM runtime_commit_scopes').fetchall()


@pytest.mark.parametrize('payload',[b'not-text','x'*65537])
def test_old_scope_read_is_bounded_and_text_only(queue,tmp_path,payload):
    from demiflow.execution.queue_commit import task_action
    channel,p=queue;src=SimpleNamespace(config=channel.config,pool='p');body=consumer()
    old={**body,'protocol':'demiflow.queue.consumer.v2','platform':'original'}
    with native_parent(tmp_path/'fixture') as action:
        publish(action,channel,p,0,[task_message('task',{},pool='p')])
        worker=channel.register_worker('p',domain(),old,action.context.limits)
        delivery=channel.claim(worker)
        scoped=task_action(action,channel,'p',delivery,'acquisition','node-0',old)
        with channel.database.connection(write=True) as db:
            db.execute('UPDATE runtime_commit_scopes SET declaration=? WHERE scope_id=?',(payload,scoped.action_id))
        with pytest.raises(RecoveryContractError,match='byte budget or is not text'):
            resolve_consumer_contract(src,body,action=parent('another'))


@pytest.mark.parametrize('changes',[{'format':99},{'format':True},{'stream_protocol':'unknown'}])
def test_resume_rejects_unknown_durable_protocol_before_attempt(tmp_path,changes):
    from demiflow.execution.recovery_contract import RunRef
    target=tmp_path/'run';target.mkdir();journal=RunJournal(target)
    configuration=dict(format=1,stream_protocol='aligned-barrier-v2',run_id='r'+'1'*24,
        platform_revision='unrelated',limits=dict(max_control_bytes=8*1024**2,max_commits=1024,admission_timeout_s=30))
    configuration.update(changes)
    journal.initialize(dict(run_id=configuration['run_id'],object_locations=[]),attempts=True)
    journal.claim_execution(configuration,'f'+'1'*32)
    before=journal.execution_status()
    with pytest.raises(RecoveryContractError,match='Unsupported or missing runtime'):
        RunContext.resume(RunRef(str(target),configuration['run_id']))
    with pytest.raises(RecoveryContractError,match='Unsupported or missing runtime'):
        with RunContext(target,resume=True):
            pytest.fail('unsupported protocol entered runtime')
    assert journal.execution_status()==before
    assert journal.recovery_identity()==configuration


@pytest.mark.parametrize('changes',[{'format':999},{'stream_protocol':'unknown'}])
def test_manager_rejects_unknown_protocol_before_dispatch_or_attempt(tmp_path,monkeypatch,changes):
    from types import SimpleNamespace
    from demiflow.workbench.runs import manager
    import demiflow.workbench.worker as worker
    run_id='r'+'4'*24
    target=manager.directory(tmp_path,run_id);target.mkdir(parents=True)
    journal=RunJournal(target)
    configuration=dict(format=1,stream_protocol='aligned-barrier-v2',run_id=run_id,
        platform_revision='unrelated',limits=dict(max_control_bytes=8*1024**2,max_commits=1024,admission_timeout_s=30))
    configuration.update(changes)
    journal.initialize(dict(run_id=run_id,object_locations=[]),attempts=True)
    journal.claim_execution(configuration,'f'+'2'*32)
    before=journal.execution_status()
    authority=SimpleNamespace(require=lambda *a,**k:None)
    monkeypatch.setattr(worker,'operation',lambda *a,**k:pytest.fail('protocol reached dispatch'))
    with pytest.raises(RecoveryContractError,match='Unsupported or missing runtime'):
        manager.resume(tmp_path,run_id,expected='d',expected_attempt=1,authority=authority)
    # Private worker dispatch is also fenced before any cleanup/index/attempt work.
    monkeypatch.setattr(manager,'_load',lambda *a,**k:(target,{}))
    with pytest.raises(RecoveryContractError,match='Unsupported or missing runtime'):
        manager._resume(tmp_path,run_id,expected='d',expected_attempt=1,authority=authority)
    assert journal.execution_status()==before
    assert journal.recovery_identity()==configuration
