"""Actual public streaming writes: original child scopes, CAS and crash recovery."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import threading
import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext

SCHEMA=pa.schema([('id',pa.string()),('value',pa.int64())])


def scopes(run):
    with run.journal._connection() as db:
        return [(key,json.loads(raw)) for key,raw in db.execute('SELECT scope_id,declaration FROM runtime_commit_scopes')
                if json.loads(raw).get('schema')=='demiflow.stream_lance.v3']


def test_when_and_output_ref_are_visible_only_from_original_confirmed_receipts(tmp_path):
    rows=[{'id':'a','value':1,'keep':True},{'id':'b','value':2,'keep':False},{'id':'c','value':3,'keep':True}]
    target=tmp_path/'out.lance'
    with RunContext.start(tmp_path/'run') as run,data.local_execution():
        result=data.from_items(rows).save_lance(target,schema=SCHEMA,key='id',stage='saved',max_batch=1,
            when=lambda row:row['keep'],output_ref='snapshot').take_all()
        assert 'snapshot' not in result[1]
        assert [result[i]['snapshot']['version'] for i in (0,2)]==[1,2]
        assert lance.dataset(**result[0]['snapshot']).to_table().to_pylist()==[{'id':'a','value':1}]
        assert lance.dataset(**result[2]['snapshot']).to_table().to_pylist()==[{'id':'a','value':1},{'id':'c','value':3}]
        for key,declaration in scopes(run):
            assert declaration['request']['records'] or declaration['request']['finish']
            assert run.journal.execution_commits(key)[0]['status']=='acked'
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM runtime_actions').fetchone()[0]==1
        assert all(decl['node_id'] for _,decl in scopes(run))
    assert lance.dataset(str(target)).version==2


@pytest.mark.parametrize('mode',['overwrite','append'])
def test_empty_input_gets_a_confirmed_native_table_receipt(tmp_path,mode):
    target=tmp_path/'empty.lance'
    with RunContext.start(tmp_path/'run') as run,data.local_execution():
        stats=data.from_arrow(pa.Table.from_pylist([],schema=SCHEMA)).save_lance(target,schema=SCHEMA,
            key='id',stage='empty',mode=mode).run_stream()
        assert stats.outputs['empty']=={'uri':str(target),'version':1}
        assert lance.dataset(**stats.outputs['empty']).count_rows()==0
        assert any(decl['request']['finish'] for _,decl in scopes(run))


def test_overwrite_preparation_failure_preserves_original_table_version_and_bytes(tmp_path):
    target=tmp_path/'old.lance'
    old=lance.write_dataset(pa.Table.from_pylist([{'id':'old','value':7}],schema=SCHEMA),str(target),mode='create')
    before={str(path.relative_to(target)):path.read_bytes() for path in target.rglob('*') if path.is_file()}
    with pytest.raises(Exception,match='int64|convert|integer'):
        with RunContext.start(tmp_path/'run') as run,data.local_execution():
            data.from_items([{'id':'bad','value':'not an integer'}]).save_lance(target,schema=SCHEMA,key='id',stage='out').run_stream()
    assert lance.dataset(str(target)).version==old.version
    assert {str(path.relative_to(target)):path.read_bytes() for path in target.rglob('*') if path.is_file()}==before
    assert lance.dataset(str(target)).to_table().to_pylist()==[{'id':'old','value':7}]


def test_append_identical_keys_reuse_prior_version_and_conflicts_cannot_publish(tmp_path):
    target=tmp_path/'existing.lance'
    lance.write_dataset(pa.Table.from_pylist([{'id':'a','value':1}],schema=SCHEMA),str(target),mode='create')
    with RunContext.start(tmp_path/'run') as run,data.local_execution():
        output=data.from_items([{'id':'a','value':1},{'id':'a','value':1},{'id':'b','value':2}]).save_lance(
            target,schema=SCHEMA,key='id',stage='out',mode='append',max_batch=1,output_ref='ref').take_all()
        assert [row['ref']['version'] for row in output]==[1,1,2]
    with pytest.raises(Exception,match='conflicting saved content'):
        with RunContext.start(tmp_path/'conflict'),data.local_execution():
            data.from_items([{'id':'a','value':9}]).save_lance(target,schema=SCHEMA,key='id',stage='out',mode='append').run_stream()
    assert lance.dataset(str(target)).version==2
    assert lance.dataset(str(target)).to_table().to_pylist()==[{'id':'a','value':1},{'id':'b','value':2}]


def test_later_failure_retains_visible_confirmed_microbatch(tmp_path):
    target=tmp_path/'partial.lance'
    with pytest.raises(Exception,match='unique'):
        with RunContext.start(tmp_path/'run') as run,data.local_execution():
            data.from_items([{'id':'a','value':1},{'id':'a','value':1}]).save_lance(target,schema=SCHEMA,
                key='id',stage='out',max_batch=1).run_stream()
    assert lance.dataset(str(target)).version==1
    assert lance.dataset(str(target)).to_table().to_pylist()==[{'id':'a','value':1}]
    assert sum(f['status']=='acked' for key,_ in scopes(run) for f in run.journal.execution_commits(key))==1


CHILD=r'''
import json,os,sys
from pathlib import Path
import pyarrow as pa
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.contracts.recovery_contract import RunRef
from demiflow.execution.checkpoint import commit
root=Path(sys.argv[1]);boundary=sys.argv[2];mode=sys.argv[3]
directory=root/'run';prior=RunJournal(directory).recovery_identity() if directory.exists() else None
context=RunContext.resume(RunRef(str(directory),prior['run_id'])) if prior else RunContext.start(directory,limits={'timeout_s':45})
with context as run,data.local_execution():
    if mode=='crash':
        def fault(name,commit_id):
            if name!=boundary:return
            fact=run.journal.execution_commit(commit_id)
            with run.journal._connection() as db:
                own=db.execute('SELECT declaration FROM runtime_commit_scopes WHERE scope_id=?',(fact['action_id'],)).fetchone()
            if not own or json.loads(own[0]).get('schema')!='demiflow.stream_lance.v3':return
            (root/'boundary.json').write_text(json.dumps({'commit_id':commit_id,'scope':fact['action_id'],'fence':run.fence}))
            os._exit(77)
        commit.fault_point=fault
    schema=pa.schema([('id',pa.string()),('value',pa.int64())])
    stats=data.from_items([{'id':'a','value':1},{'id':'b','value':2}]).save_lance(root/'output.lance',schema=schema,
        key='id',stage='saved',max_batch=1).run_stream()
    with run.journal._connection() as db:
        child=[(key,json.loads(d)) for key,d in db.execute('SELECT scope_id,declaration FROM runtime_commit_scopes')]
        budgets=dict(db.execute('SELECT name,consumed FROM runtime_budgets'))
    (root/'result.json').write_text(json.dumps({'outputs':stats.outputs,'attempt':run.attempt,'fence':run.fence,
        'commits':[f for key,_ in child for f in run.journal.execution_commits(key)],'budgets':budgets}))
'''
WORKTREE=Path(__file__).resolve().parents[1]


def child(root,boundary,mode):
    env=dict(os.environ,PYTHONPATH=str(WORKTREE)+':'+str(WORKTREE/'tests'),PYTHONDONTWRITEBYTECODE='1')
    return subprocess.run([sys.executable,'-B','-c',CHILD,str(root),boundary,mode],cwd=WORKTREE,env=env,
                          capture_output=True,text=True,timeout=75)


@pytest.mark.parametrize('boundary',['after_intent','after_prepare_before_record','after_prepared',
    'after_sink_commit_before_receipt','after_sink_receipt','after_checkpoint_committed'])
def test_real_native_microbatch_crash_reuses_original_scope_and_never_reoverwrites(tmp_path,boundary):
    target=tmp_path/'output.lance'
    lance.write_dataset(pa.Table.from_pylist([{'id':'old','value':-1}],schema=SCHEMA),str(target),mode='create')
    first=child(tmp_path,boundary,'crash')
    assert first.returncode==77,first.stderr
    boundary_fact=json.loads((tmp_path/'boundary.json').read_text())
    second=child(tmp_path,boundary,'resume')
    assert second.returncode==0,second.stderr
    result=json.loads((tmp_path/'result.json').read_text())
    assert result['attempt']==2 and result['fence']!=boundary_fact['fence']
    assert result['outputs']['saved']['version']==3  # old=1, overwrite a=2, append b=3
    assert lance.dataset(str(target)).to_table().to_pylist()==[{'id':'a','value':1},{'id':'b','value':2}]
    assert len(result['commits'])==3 and all(f['status']=='acked' for f in result['commits'])
    assert any(f['commit_id']==boundary_fact['commit_id'] for f in result['commits'])
    assert result['budgets']['output_rows']==2


def test_unknown_foreign_successor_is_never_adopted_or_overwritten(tmp_path):
    first=child(tmp_path,'after_prepared','crash')
    assert first.returncode==77,first.stderr
    target=tmp_path/'output.lance'
    lance.write_dataset(pa.Table.from_pylist([{'id':'foreign','value':9}],schema=SCHEMA),str(target),mode='create')
    second=child(tmp_path,'after_prepared','resume')
    assert second.returncode!=0 and 'does not prove this publication' in second.stderr,second.stderr
    assert lance.dataset(str(target)).to_table().to_pylist()==[{'id':'foreign','value':9}]


def test_changed_record_membership_cannot_reuse_confirmed_invocation(tmp_path,monkeypatch):
    from dataclasses import replace,asdict
    from demiflow.execution.dataflow_stream_lance import NativeStreamLanceService
    from demiflow.execution.contracts.recovery_contract import document
    from demiflow.operator_llm.journal import request_key
    original=NativeStreamLanceService.request
    def corrupt(self,command,record):
        reply=original(self,command,record)
        if record['finish']:return reply
        changed=dict(record);changed['records']=['f'*64]*len(record['records'])
        body=document(changed,self.bounds.max_control_bytes)
        ref=self.store.put(iter([body]),max_bytes=len(body),media_type='application/json')
        altered=replace(command,request_key=request_key(changed),request_ref=ref)
        # Re-enter the real original authority after immutable request validation;
        # the successful first publication must not authorize another membership.
        request=self.ex._services.journal_service._native_request(altered)
        assert request.request_key==altered.request_key
        return original(self,altered,request)
    monkeypatch.setattr(NativeStreamLanceService,'request',corrupt)
    target=tmp_path/'membership.lance'
    with pytest.raises(Exception,match='changed payload or record membership'):
        with RunContext.start(tmp_path/'run') as run,data.local_execution():
            data.from_items([{'id':'a','value':1}]).save_lance(target,schema=SCHEMA,key='id',stage='saved').run_stream()
    assert lance.dataset(str(target)).version==1
    assert lance.dataset(str(target)).to_table().to_pylist()==[{'id':'a','value':1}]
    assert sum(f['status']=='acked' for key,_ in scopes(run) for f in run.journal.execution_commits(key))==1


def test_cancel_preserves_only_acknowledged_microbatches_and_releases_writer_lock(tmp_path):
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    from demiflow.execution.storage.artifacts import run_lock
    target=tmp_path/'cancel.lance';ready=tmp_path/'visible'
    def source():
        yield {'id':'a','value':1}
        until=time.monotonic()+12
        while not ready.exists() and time.monotonic()<until:time.sleep(.01)
        while True:
            yield {'id':'b','value':2}
            time.sleep(.02)
    def observe(row):
        ready.write_text(json.dumps(row['ref']))
        return row
    with pytest.raises(RecoveryContractError,match='[Cc]ancel'):
        with RunContext.start(tmp_path/'run',limits={'timeout_s':20}) as run,data.local_execution():
            def cancel():
                until=time.monotonic()+12
                while not ready.exists() and time.monotonic()<until:time.sleep(.01)
                RunContext.cancel(run.ref)
            thread=threading.Thread(target=cancel);thread.start()
            try:
                with pytest.raises(Exception,match='[Cc]ancel'):
                    data.from_iter(source).save_lance(target,schema=SCHEMA,key='id',stage='saved',max_batch=1,
                        mode='append',output_ref='ref').map(observe).run_stream()
            finally:thread.join(14)
            assert not thread.is_alive() and ready.exists()
            assert run.journal.execution_action('action-1')['result'] is None
    assert lance.dataset(**json.loads(ready.read_text())).to_table().to_pylist()[0]=={'id':'a','value':1}
    facts=[f for key,_ in scopes(run) for f in run.journal.execution_commits(key)]
    assert any(f['status']=='acked' for f in facts)
    assert not any(decl['request']['finish'] for _,decl in scopes(run))
    with run_lock(target.with_suffix('.stream-writer')):pass


def test_native_arrow_conversion_rejects_coexisting_copies_before_overwrite(tmp_path):
    schema=pa.schema([('id',pa.string()),('value',pa.large_string())]);target=tmp_path/'bounded.lance'
    lance.write_dataset(pa.Table.from_pylist([{'id':'old','value':'preserved'}],schema=schema),str(target),mode='create')
    with pytest.raises(Exception,match='coexistence'):
        with RunContext.start(tmp_path/'run',limits={'max_row_bytes':12288,'max_batch_bytes':16384}),data.local_execution():
            data.from_items([{'id':'a','value':'x'*10000}]).save_lance(target,schema=schema,key='id',stage='out',max_batch=1).run_stream()
    assert lance.dataset(str(target)).version==1
    assert lance.dataset(str(target)).to_table().to_pylist()==[{'id':'old','value':'preserved'}]
