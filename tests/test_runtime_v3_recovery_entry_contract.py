"""Unknown publication never replays; recovery requires original commit evidence."""
import json
import pickle
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal, ExecutionBudgetExceeded
from demiflow.execution.recovery_contract import RecoveryContractError, UnresolvedCommit
from demiflow.execution.contracts.dataflow_errors import DataflowError, DataflowUnresolvedCommit
from demiflow.execution.dataflow_failures import failure_payload, restore_failure
from test_execution_recovery_entrypoints import FormalSink


@pytest.fixture(autouse=True)
def _actual_worktree():
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


def test_plain_stream_seal_without_native_cut_does_not_authorize_recovery(tmp_path,monkeypatch):
    from demiflow.execution.dataflow import DataflowExecution
    events=tmp_path/'calls'
    async def callback(row):
        with events.open('a') as output: output.write('call\n')
        return row
    def execute(): return data.range(2).map_async(callback).run_stream()
    class Interrupted(Exception): pass
    def stop_before_workers(execution): raise Interrupted()
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run:
            reference=run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution,'start',stop_before_workers)
                execute()
    journal=RunJournal(reference.directory)
    assert journal.source_binding('action-1-dataflow-sealed')['schema']=='demiflow.dataflow_seal.v3'
    assert journal.execution_commits('action-1')==[] and not events.exists()
    with pytest.raises(RecoveryContractError,match='delivery-only'):
        with RunContext.resume(reference): execute()
    assert journal.execution_commits('action-1')==[] and not events.exists()


@pytest.mark.parametrize('corrupt_marker',[False,True])
def test_prepared_not_published_is_recoverable_only_with_matching_original_receipts(tmp_path,monkeypatch,corrupt_marker):
    from demiflow.execution import commit
    inputs,calls=tmp_path/'inputs',tmp_path/'commits'
    def observed(row):
        with inputs.open('a') as output: output.write('row\n')
        return row
    class Sink(FormalSink):
        def commit(self,prepared,context):
            with calls.open('a') as output: output.write('commit\n')
            return super().commit(prepared,context)
    class NeverPrepare(Sink):
        def prepare(self,blocks,context):
            pytest.fail('prepared original input must never be recalculated')
    def execute(sink): data.from_items([{'x':1}]).map(observed).write_datasink(sink)
    class Interrupted(Exception): pass
    def interrupt(name,commit_id):
        if name=='after_prepared': raise Interrupted()
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run:
            reference=run.ref
            with monkeypatch.context() as patch:
                patch.setattr(commit,'fault_point',interrupt)
                execute(Sink())
    journal=RunJournal(reference.directory)
    fact=journal.execution_commits('action-1')[0]
    assert fact['status']=='prepared' and not fact['receipts'] and not calls.exists()
    if corrupt_marker:
        prepared=fact['prepared']['sinks']['write_datasink']
        marker=Path(reference.directory)/'materializations/dataflow-sinks/write_datasink/.receipts'/f"{fact['commit_id']}.json"
        marker.parent.mkdir(parents=True,exist_ok=True)
        marker.write_text(json.dumps({'commit_id':'0'*64,'sink_id':'write_datasink','status':'committed',
            'data':{'ref':prepared['ref'],'rows':prepared['rows'],'bytes':prepared['bytes']}}))
        with pytest.raises(DataflowUnresolvedCommit,match='identity differs'):
            with RunContext.resume(reference): execute(NeverPrepare())
        assert journal.execution_commits('action-1')[0]==fact and not calls.exists()
    else:
        with RunContext.resume(reference): execute(NeverPrepare())
        completed=journal.execution_commits('action-1')[0]
        assert completed['status']=='acked'
        assert completed['receipts']['write_datasink']['data']['native_receipt']['data']['written_rows']==1
        assert calls.read_text().splitlines()==['commit']
    assert inputs.read_text().splitlines()==['row']


@pytest.mark.parametrize('error',[UnresolvedCommit('inspect evidence absent'),
                                DataflowUnresolvedCommit('inspect evidence absent')])
def test_unknown_failure_wire_preserves_both_catch_surfaces_without_retry(error):
    payload=json.loads(json.dumps(failure_payload(error)))
    assert payload['code']=='UNKNOWN_EFFECT'
    restored=restore_failure(payload)
    assert isinstance(restored,UnresolvedCommit) and isinstance(restored,DataflowError)
    assert restored.code=='UNKNOWN_EFFECT' and not restored.retryable
    assert restored._message=='inspect evidence absent'
    copied=pickle.loads(pickle.dumps(restored))
    assert isinstance(copied,UnresolvedCommit) and copied.code=='UNKNOWN_EFFECT' and not copied.retryable
    payload['code']='CAPACITY_PENDING'
    with pytest.raises(DataflowError,match='unknown-effect classification'):restore_failure(payload)


def test_budget_rejection_wire_preserves_named_ceiling_and_original_message():
    error=ExecutionBudgetExceeded('disk_bytes',message='The admitted storage ceiling was exceeded')
    payload=json.loads(json.dumps(failure_payload(error)))
    assert payload['code']=='BUDGET_EXCEEDED' and payload['budget']=='disk_bytes'
    restored=restore_failure(payload)
    assert type(restored) is ExecutionBudgetExceeded and restored.budget=='disk_bytes'
    assert str(restored)==str(error)
    group=restore_failure(failure_payload(ExceptionGroup('failures',[error,UnresolvedCommit('unknown')])))
    assert isinstance(group.exceptions[0],ExecutionBudgetExceeded)
    assert isinstance(group.exceptions[1],UnresolvedCommit) and group.exceptions[1].code=='UNKNOWN_EFFECT'


@pytest.mark.parametrize('budget',[None,False,{},'','x'*129,'disk bytes','../disk'])
def test_malformed_budget_rejection_cannot_be_used_as_definite_admission_failure(budget):
    payload=failure_payload(ExecutionBudgetExceeded('disk_bytes'))
    payload['budget']=budget
    with pytest.raises(DataflowError,match='bounded identifier'):restore_failure(payload)


def test_unknown_transport_failure_cannot_be_reclassified_as_budget_rejection():
    payload=failure_payload(ExecutionBudgetExceeded('disk_bytes'))
    payload['code']='UNKNOWN_EFFECT'
    with pytest.raises(DataflowError,match='definite budget rejection'):restore_failure(payload)
    ordinary=restore_failure(failure_payload(UnresolvedCommit('transport outcome unknown')))
    assert not isinstance(ordinary,ExecutionBudgetExceeded)


def test_real_worker_budget_failure_keeps_class_name_and_message(tmp_path):
    events=tmp_path/'calls'
    def reject(row):
        from demiflow.execution.run_journal import ExecutionBudgetExceeded
        with events.open('a') as output: output.write('call\n')
        raise ExecutionBudgetExceeded('row_bytes',message='worker admitted row budget is exhausted')
    with pytest.raises(ExecutionBudgetExceeded,match='worker admitted row budget is exhausted') as caught:
        with RunContext.start(tmp_path/'run'),data.local_execution(workers=1,worker_mode='process'):
            data.range(1).map(reject).take_all()
    assert caught.value.budget=='row_bytes'
    assert events.read_text().splitlines()==['call']


@pytest.mark.parametrize('phase',['commit','recover'])
def test_native_coordinator_budget_rejection_is_not_mislabeled_unknown(tmp_path,monkeypatch,phase):
    from demiflow.execution.commit import CommitCoordinator
    def reject(*args,**kwargs):
        raise ExecutionBudgetExceeded('disk_bytes',message='original native storage admission rejected')
    if phase=='commit':
        monkeypatch.setattr(CommitCoordinator,'commit',reject)
        with pytest.raises(ExecutionBudgetExceeded,match='original native storage admission rejected'):
            with RunContext.start(tmp_path/'run'),data.local_execution(workers=1):
                data.range(1).take_all()
        assert RunJournal(tmp_path/'run').execution_commits('action-1')==[]
    else:
        class Finished(Exception):pass
        with pytest.raises(Finished):
            with RunContext.start(tmp_path/'run') as run,data.local_execution(workers=1):
                reference=run.ref
                assert data.range(1).take_all()==[{'id':0}]
                raise Finished()
        original=RunJournal(reference.directory).execution_commits('action-1')
        monkeypatch.setattr(CommitCoordinator,'recover',reject)
        with pytest.raises(ExecutionBudgetExceeded,match='original native storage admission rejected'):
            with RunContext.resume(reference),data.local_execution(workers=1):
                data.range(1).take_all()
        assert RunJournal(reference.directory).execution_commits('action-1')==original
