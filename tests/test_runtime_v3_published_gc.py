"""Published GC keeps native history and requires an exited physical owner."""
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import ArtifactCommand, ArtifactRef, Record
from demiflow.execution.storage.gc import collect_published_orphans, recover_published_gc
from demiflow.execution.storage.lease import ObjectLease, exclusive_access
from demiflow.execution.storage.objects import RunArtifactStore
from demiflow.execution.contracts import state_codec
from demiflow.execution.pipeline.run_context import RunContext
from test_runtime_v3_artifact_accounting import store_fixture, consumed
from test_runtime_v3_artifact_recovery import _open, _store
from test_runtime_v3_real_seams import PreparationOnly


def put(store, value, media='application/octet-stream'):
    raw = value if type(value) is bytes else json.dumps(value).encode()
    return store.put(iter((raw,)), max_bytes=max(1,len(raw)), media_type=media)


def collect(store, port, **kwargs):
    store.close()
    return collect_published_orphans(port._action, bounds=port._bounds, **kwargs)


def test_released_unrooted_publication_refunds_once_and_reput_charges_again(tmp_path):
    with store_fixture(tmp_path) as (run,store,port):
        ref = put(store,b'released-data')
        store.retain(ref,owner='temporary-owner')
        assert collect_published_orphans(port._action,bounds=port._bounds)['blocked']
        store.release(ref,owner='temporary-owner')
        before = consumed(run)
        with run.journal._connection() as db:
            receipts = db.execute('SELECT * FROM runtime_dataflow_settlements').fetchall()
        result = collect(store,port)
        assert result == {'removed':1,'bytes':before,'blocked':None}
        assert consumed(run) == 0 and not store.inspect(ref)
        assert collect(store,port)['removed'] == 0
        with run.journal._connection() as db:
            assert db.execute('SELECT * FROM runtime_dataflow_settlements').fetchall() == receipts
        again = RunArtifactStore(root=store.root,store_id=store.store_id,journal=port,bounds=port._bounds)
        try:
            assert put(again,b'released-data') == ref
            assert consumed(run) == before
        finally:
            again.close()


def test_old_and_pending_native_state_roots_keep_nested_record_payload_schema_and_borrowed(tmp_path):
    from demiflow.execution.storage.state_artifacts import StateArtifacts
    from demiflow.execution.storage.segments import SegmentForest, FINITE_RECORDS_MEDIA
    from demiflow.execution.contracts.record_codec import _encode_payload, _encode_record_document
    with store_fixture(tmp_path) as (run,store,port):
        payload = put(store,_encode_payload({'value':-0.0},max_bytes=4096),'application/vnd.demiflow.record.v3')
        schema = put(store,b'opaque-test-schema','application/vnd.demiflow.arrow-schema.v1')
        record = Record(hashlib.sha256(b'record-1').hexdigest(),{'i':0},payload,None,payload.size_bytes,0,schema_ref=schema)
        raw = (json.dumps({'target':'p0','record_id':record.record_id,'sequence':0,
                          'document':_encode_record_document(record,max_bytes=4096).decode()})+'\n').encode()
        leaf = put(store,raw,FINITE_RECORDS_MEDIA)
        forest = SegmentForest(store,port._bounds);forest.append(leaf,rows=1);manifest=forest.seal()
        seal = put(store,{'schema':'demiflow.seal.v3','producers':[{'manifest_ref':asdict(manifest)}]},
                   'application/vnd.demiflow.seal.v3')
        image = put(store,state_codec.dumps({'ports':{'input':{'borrowed':[{'position':0,'input':{
            'seal_ref':seal,'manifest_ref':manifest}}]}},'batch':[record]},max_bytes=65536),
            'application/x-demiflow-kernel-image-v3')
        from demiflow.execution.commit import CommitCoordinator
        from demiflow.execution.contracts.recovery_contract import SourcePosition
        from demiflow.execution.outputs.sinks import _DataflowArtifactSink
        sink=_DataflowArtifactSink(sink_id='test',store=store,action=port._action,
                                   bounds=port._bounds,configuration={})
        old = CommitCoordinator(port._action).commit(1,outputs={'test':(sink,{
            'schema':'demiflow.dataflow_sink_combined.v3','sink_id':'test','partitions':[],
            'total_rows':0,'total_bytes':0})},position=SourcePosition('fixture',0),
            states={'dataflow-cut-v3':{'contributions':[{'operator_states':{'node':asdict(image)}}]}})
        assert old['status']=='acked'
        native = StateArtifacts(run).put('pending-state',{'contributions':[{'operator_states':{'node':asdict(image)}}]})
        run.journal.begin_commit(run.fence,port._action.action_id,2,
            {'position':SourcePosition('fixture',0).to_dict(),'states':{'dataflow-cut-v3':native},'sinks':[]})
        garbage = put(store,b'never-referenced')
        result=collect(store,port)
        assert result['blocked'] is None and result['bytes'] == garbage.size_bytes
        assert not store.inspect(garbage)
        for ref in (payload,schema,leaf,manifest,seal,image):
            assert store.inspect(ref)
        assert len(run.journal.execution_commits(port._action.action_id)) == 2


@pytest.mark.parametrize('case',['owner','unknown_carrier','trace_budget','missing_root'])
def test_unknown_or_owned_roots_never_become_a_deletion_proof(tmp_path,case):
    with store_fixture(tmp_path) as (run,store,port):
        protected = put(store,b'protected','application/future-codec' if case=='unknown_carrier' else 'application/octet-stream')
        garbage=put(store,b'garbage')
        if case=='owner':
            store.retain(protected,owner='old-history-owner')
        else:
            run.journal.bind_source(run.fence,'original-root',{'ref':asdict(protected)})
        if case=='missing_root':
            store._object_path(protected).unlink()  # Corruption in this test's private /tmp namespace.
        before=consumed(run)
        store.close()
        bounds=replace(port._bounds,max_spill_bytes=1) if case=='trace_budget' else port._bounds
        result=collect_published_orphans(port._action,bounds=bounds)
        if case=='owner':
            assert result['blocked'] is None and store.inspect(protected)
            assert result['bytes']==garbage.size_bytes
        else:
            assert result['blocked'] and result['removed']==0
            assert consumed(run)==before and store.inspect(garbage)


def test_open_reader_fd_remains_protected_after_writer_store_close(tmp_path):
    with store_fixture(tmp_path) as (run,store,port):
        ref=put(store,b'x'*(2*1024**2))
        reader=store.read(ref,max_bytes=ref.size_bytes)
        assert next(reader)
        store.close()
        before=consumed(run)
        result=collect_published_orphans(port._action,bounds=port._bounds)
        assert result['blocked'] and result['removed']==0 and consumed(run)==before
        reader.close()
        assert collect(store,port)['bytes']==ref.size_bytes


def test_live_store_owner_with_no_open_record_fd_blocks_gc(tmp_path):
    with store_fixture(tmp_path) as (run,store,port):
        ref=put(store,b'live-memory-reference')
        lease=ObjectLease(store.root)
        try:
            assert collect(store,port)['blocked']
            assert consumed(run)==ref.size_bytes and store.inspect(ref)
        finally:
            lease.close()
        assert collect(store,port)['bytes']==ref.size_bytes


def test_existing_namespace_cannot_be_retroactively_certified(tmp_path):
    directory=tmp_path/'legacy'
    with pytest.raises(PreparationOnly):
        with _open(directory) as run:
            (run.directory/'.dataflow-store').mkdir()
            store,port,_=_store(run,ceiling=65536)
            ref=put(store,b'legacy')
            assert run.journal.dataflow_gc_protocol()[2] == 0
            assert collect(store,port)['blocked']
            assert store.inspect(ref) and consumed(run)==ref.size_bytes
            raise PreparationOnly()


def test_replaced_original_lease_during_gc_never_deletes_or_refunds(tmp_path,monkeypatch):
    from demiflow.execution.storage import gc as dataflow_gc
    from demiflow.execution.storage.lease import NAME
    from demiflow.errors import ExecutionConflict
    # The normal run finalizer must also reject this deliberately corrupted
    # namespace; no force-cleanup or repaired replacement inode is supplied.
    with pytest.raises(ExecutionConflict,match='reconciliation'):
        with store_fixture(tmp_path) as (run,store,port):
            ref=put(store,b'old-physical-owner')
            replacement=[]
            def replace_lock(point):
                if point=='after_prepare':
                    (store.root/NAME).unlink()
                    replacement.append(ObjectLease(store.root,create=True))
            monkeypatch.setattr(dataflow_gc,'_fault',replace_lock)
            result=collect(store,port)
            assert result['blocked'] and result['removed']==0
            assert consumed(run)==ref.size_bytes and store._object_path(ref).read_bytes()==b'old-physical-owner'
            for lease in replacement:lease.close()


def test_concurrent_pin_cannot_cross_exclusive_gc_snapshot(tmp_path,monkeypatch):
    import threading
    from demiflow.execution.storage import gc as dataflow_gc
    with store_fixture(tmp_path) as (run,store,port):
        ref=put(store,b'pin-race')
        failures=[]
        def at_prepare(point):
            if point!='after_prepare':return
            def pin():
                try:store.retain(ref,owner='racing-reader')
                except Exception as exc:failures.append(exc)
            thread=threading.Thread(target=pin);thread.start();thread.join(5)
            assert not thread.is_alive()
        monkeypatch.setattr(dataflow_gc,'_fault',at_prepare)
        assert collect(store,port)['removed']==1
        assert failures and 'reconciled' in str(failures[0])
        assert not run.journal.artifact_owners(ref,limit=1)[0]


def test_stop_flag_does_not_prove_journal_thread_exit():
    import threading
    from demiflow.execution.journal.dataflow_authority import DriverJournalService
    gate=threading.Event()
    thread=threading.Thread(target=gate.wait);thread.start()
    service=object.__new__(DriverJournalService)
    service._closed=True;service._workers=[thread]
    try:assert not service.physically_stopped()
    finally:gate.set();thread.join(5)
    assert service.physically_stopped()


_CHILD=r'''
import os,sys
from pathlib import Path
from test_runtime_v3_artifact_recovery import _open,_store
from test_runtime_v3_published_gc import put
from demiflow.execution.storage import gc as dataflow_gc
root,point=Path(sys.argv[1]),sys.argv[2]
with _open(root) as run:
 store,port,bounds=_store(run,ceiling=1024*1024)
 put(store,b'crash-published-garbage')
 store.close()
 def fault(actual):
  if actual==point:os._exit(79)
 dataflow_gc._fault=fault
 dataflow_gc.collect_published_orphans(port._action,bounds=bounds)
 raise AssertionError('real GC fault window not reached')
'''


@pytest.mark.parametrize('fault',['after_prepare','after_rename','after_quarantine','after_unlink','after_refund'])
def test_actual_process_crash_reconciles_original_intent_and_refunds_once(tmp_path,fault):
    directory=tmp_path/'run'
    for _ in range(2):
        child=subprocess.run([sys.executable,'-B','-c',_CHILD,str(directory),fault],
                             capture_output=True,text=True,timeout=30)
        assert child.returncode==79,child.stdout+child.stderr
        with pytest.raises(PreparationOnly):
            with _open(directory) as run:
                assert consumed(run)==0
                assert not run.journal.pending_dataflow_gc()
                assert recover_published_gc(run.journal,run.fence)['removed']==0
                raise PreparationOnly()


def test_real_dataset_cleanup_keeps_acked_result_readable_and_reclaims_unrooted_bytes(tmp_path,monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    prepare,close=DataflowExecution.prepare,DataflowExecution.close
    observed=[]
    def prepared(execution):
        result=prepare(execution)
        execution._test_orphan=put(execution._store,b'private-unrooted-test-object')
        return result
    def closed(execution):
        try:return close(execution)
        finally:
            if hasattr(execution,'_test_orphan'):
                observed.append((execution,getattr(execution,'_published_gc',None)))
    monkeypatch.setattr(DataflowExecution,'prepare',prepared)
    monkeypatch.setattr(DataflowExecution,'close',closed)
    with RunContext.start(tmp_path/'run') as run:
        handle=data.from_items([{'i':i} for i in range(12)]).map(lambda row:{'i':row['i']*2}).materialize()
        expected=[{'i':i*2} for i in range(12)]
        assert handle.take_all()==expected
        execution,result=observed[-1]
        assert result is not None and result['blocked'] is None and result['removed']>0
        assert not execution._store.inspect(execution._test_orphan)
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'
        # An independent result reread uses stored historical blocks after GC.
        assert handle.take_all()==expected
        handle.release()


@pytest.mark.parametrize('removed',['lock','header','both'])
def test_registered_protocol_loss_rejects_read_pin_put_and_new_writer(tmp_path,removed):
    from demiflow.execution.storage.lease import NAME,HEADER
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError,match='missing|differs'):
        with store_fixture(tmp_path) as (run,store,port):
            ref=put(store,b'cannot-downgrade')
            before=consumed(run)
            if removed in ('lock','both'):(store.root/NAME).unlink()
            if removed in ('header','both'):(store.root/HEADER).unlink()
            for operation in (lambda:list(store.read(ref,max_bytes=1024)),
                    lambda:store.retain(ref,owner='reader'),
                    lambda:put(store,b'new'),
                    lambda:RunArtifactStore(root=store.root,store_id=store.store_id,journal=port,bounds=port._bounds)):
                with pytest.raises((DataflowError,FileNotFoundError)):
                    operation()
            assert consumed(run)==before
            assert store._object_path(ref).read_bytes()==b'cannot-downgrade'
            store.close()


def test_default_metadata_budget_traces_more_than_32_live_roots(tmp_path):
    with store_fixture(tmp_path) as (run,store,port):
        refs=[put(store,('history-'+str(i)).encode()) for i in range(48)]
        for i,ref in enumerate(refs):
            run.journal.bind_source(run.fence,'historical-root-'+str(i),{'ref':asdict(ref)})
        garbage=put(store,b'unreferenced')
        result=collect(store,port)
        assert result=={'removed':1,'bytes':garbage.size_bytes,'blocked':None}
        assert all(store.inspect(ref) for ref in refs)
        assert consumed(run)==sum(ref.size_bytes for ref in refs)


def test_native_journal_thread_keeps_lease_until_original_run_close(tmp_path):
    from test_runtime_v3_native_rpc_replay import interrupted_run, service, event
    from demiflow.execution.storage.gc import defer_run_gc
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority,store,lease):
        reply=authority.dispatch(event(run,store))
        assert reply.status=='ok' and reply.payload['value'] is True
        native=run._native_prompt_journal
        assert native._batch_thread is not None and native._batch_thread.is_alive()
        orphan=put(store,b'after-model-unreferenced')
        store.close()
        assert collect_published_orphans(authority._action,bounds=authority._bounds)['blocked']
        defer_run_gc(run,authority._bounds)
        assert run._published_gc_pending is authority._bounds
    assert native._batch_thread is None
    assert native._dataflow_object_lease is None
    assert run._published_gc_result['blocked'] is None
    assert run._published_gc_result['removed']>0 and not store.inspect(orphan)
    with run.journal._connection() as db:
        assert db.execute('SELECT count(*) FROM calls').fetchone()[0]==1


def test_json_container_amplification_is_rejected_before_decode(tmp_path):
    with store_fixture(tmp_path) as (run,store,port):
        ref=put(store,{'schema':'demiflow.worker-frontier.v3','many':[[] for _ in range(300)]},'application/json')
        run.journal.bind_source(run.fence,'large-container-root',{'ref':asdict(ref)})
        garbage=put(store,b'keep-on-incomplete-scan')
        store.close()
        bounds=replace(port._bounds,max_resident_bytes=128*1024)
        result=collect_published_orphans(port._action,bounds=bounds)
        assert result['removed']==0 and 'workspace' in result['blocked']
        assert store.inspect(garbage)


@pytest.mark.parametrize('clock',['wall','monotonic'])
def test_optional_gc_near_original_deadline_preserves_successful_run(tmp_path,monkeypatch,clock):
    import time
    from demiflow.execution.storage import gc as dataflow_gc
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    def unexpected(*args,**kwargs):
        raise AssertionError('Optional collection must not start near the original timeout')
    monkeypatch.setattr(dataflow_gc,'_collect',unexpected)
    with RunContext.start(tmp_path/'near-deadline') as run:
        _,bounds=resolve_runtime_policy({},run.limits,provider='local')
        dataflow_gc.defer_run_gc(run,bounds)
        if clock=='wall':
            run.deadline=time.time()+3
        else:
            run.started=time.monotonic()-run.limits['timeout_s']+3
        run.check_stop()
    assert run._published_gc_result['removed']==0
    assert 'original run time' in run._published_gc_result['blocked']
    assert run.journal.execution_status()['status']=='succeeded'


def test_optional_gc_skip_does_not_bypass_original_timeout(tmp_path):
    import time
    from demiflow.execution.storage.gc import defer_run_gc
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    with pytest.raises(RecoveryContractError,match='time budget exhausted'):
        with RunContext.start(tmp_path/'expired') as run:
            _,bounds=resolve_runtime_policy({},run.limits,provider='local')
            defer_run_gc(run,bounds)
            run.deadline=time.time()-1
    assert run._published_gc_result['removed']==0
    assert run.journal.execution_status()['status']=='failed'


def test_action_close_near_timeout_retains_orphan_and_original_charge(tmp_path):
    import time
    with store_fixture(tmp_path) as (run,store,port):
        ref=put(store,b'optional-near-timeout')
        before=consumed(run)
        run.deadline=time.time()+3
        result=collect(store,port)
        assert result['removed']==0 and 'original run time' in result['blocked']
        assert store.inspect(ref) and consumed(run)==before


def test_optional_gc_never_waits_for_a_live_publication_owner(tmp_path):
    import threading,time
    from demiflow.execution.run_journal import RunJournal
    with store_fixture(tmp_path) as (run,store,port):
        ref=put(store,b'publication-in-progress')
        owner=RunJournal(run.directory,attempt=run.attempt)
        ready,stop=threading.Event(),threading.Event()
        def hold_publication():
            with owner._publication_lease():
                ready.set()
                stop.wait(5)
        thread=threading.Thread(target=hold_publication);thread.start()
        try:
            assert ready.wait(2)
            started=time.monotonic()
            result=collect(store,port)
            assert time.monotonic()-started<1
            assert result['removed']==0 and 'Publication ownership' in result['blocked']
            assert store.inspect(ref) and consumed(run)==ref.size_bytes
        finally:
            stop.set();thread.join(5)
