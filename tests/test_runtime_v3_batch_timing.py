"""Real incremental batch timing and native-effect recovery membership."""
from contextlib import nullcontext
import json
import time
import os
from pathlib import Path
import subprocess
import sys
import pytest
import pyarrow as pa
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from runtime_v3_recovery_scenarios import _http_fixture, _provider


@pytest.mark.parametrize('kind',['batch','save','embedding'])
def test_timed_first_batch_reaches_downstream_before_slow_source_eoi(tmp_path,kind,monkeypatch):
    seen=tmp_path/'seen';source_eoi=tmp_path/'eoi';ready=tmp_path/'downstream-ready'
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    original_start=DataflowExecution.start
    def started(execution):
        original_start(execution)
        ready.write_text('physical endpoints activated')
    monkeypatch.setattr(DataflowExecution,'start',started)
    def source():
        until=time.monotonic()+15
        while not ready.exists() and time.monotonic()<until:time.sleep(.01)
        assert ready.exists(), 'physical endpoints never activated'
        yield {'id':'a','value':1,'text':'one'}
        until=time.monotonic()+4
        while not seen.exists() and time.monotonic()<until:time.sleep(.01)
        source_eoi.write_text('ended')
        yield {'id':'b','value':2,'text':'two'}
    class Observe:
        def __call__(self,row):
            if not seen.exists():seen.write_text(json.dumps({'source_eoi':source_eoi.exists()}))
            return row
    with (_http_fixture(tmp_path) if kind=='embedding' else nullcontext(([],None))) as (requests,url):
        with RunContext.start(tmp_path/'run',limits={'timeout_s':60}),_provider(os.environ.get('DF_BATCH_PROVIDER','local')):
            ds=data.from_iter(source)
            if kind=='batch':ds=ds.batch_map(lambda rows:rows,max_batch=8,flush_interval=.04)
            elif kind=='save':ds=ds.save_lance(tmp_path/'out.lance',schema=pa.schema([('id',pa.string()),('value',pa.int64())]),
                key='id',stage='saved',max_batch=8,flush_interval=.04,output_ref='ref')
            else:
                from demiflow.embeddings import EmbeddingModel
                ds=ds.map_embeddings(model=EmbeddingModel('fixture','v1',3,url+'/v1'),inputs={'text':'text'},
                    batch_size=8,concurrency=1,flush_interval=.04,max_requests=2)
            rows=ds.map_async(Observe,concurrency=1).take_all()
        assert [row['id'] for row in rows]==['a','b']
        assert json.loads(seen.read_text())=={'source_eoi':False}
        if kind=='embedding':assert [len(request['body']['input']) for request in requests]==[1,1]
        if kind=='save':assert [row['ref']['version'] for row in rows]==[1,2]


TIMED_CHILD=r'''
import json,os,sys,time
from pathlib import Path
import pyarrow as pa
from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RunRef
from runtime_v3_recovery_scenarios import _native_calls
root=Path(sys.argv[1]);mode=sys.argv[2];kind=sys.argv[3]
os.environ['DF_BATCH_TIMING_MODE']=mode
ready=root/'ready'
if ready.exists():ready.unlink()
class Slow:
    def __call__(self,row):
        if row['id']=='a':
            deadline=time.monotonic()+20
            while not ready.exists() and time.monotonic()<deadline:time.sleep(.01)
            assert ready.exists()
        elif os.environ['DF_BATCH_TIMING_MODE']=='crash':time.sleep(1.2)
        if os.environ['DF_BATCH_TIMING_MODE']=='resume_drift':return {**row,'value':99,'text':'changed'}
        return row
from demiflow.execution.action_driver.dataflow import DataflowExecution
original_start=DataflowExecution.start
def started(execution):
    original_start(execution)
    ready.write_text('physical endpoints activated')
DataflowExecution.start=started
class Observe:
    def __call__(self,row):return row
run_holder=[]
if mode=='crash':
    if kind in {'save','timer'}:
        from demiflow.execution import commit
        def fault(name,cid):
            if name!=('after_checkpoint_committed' if kind=='timer' else 'after_acked'):return
            run=run_holder[0];fact=run.journal.execution_commit(cid)
            if kind=='timer':
                if fact['action_id']!='action-1':return
                from demiflow.execution.kernels import state_codec
                images=[]
                for path in (run.directory/'.dataflow-store').rglob('*'):
                    if not path.is_file() or path.stat().st_size>run.limits['max_state_bytes']:continue
                    raw=path.read_bytes()
                    if b'"batch_remaining_ms"' not in raw:continue
                    try:image=state_codec.loads(raw,max_bytes=run.limits['max_state_bytes'])
                    except Exception:continue
                    if image.get('kind')=='BatchMapOp' and image.get('batch'):
                        images.append({'remaining':image['batch_remaining_ms'],'rows':len(image['batch']),'ordinal':image['batch_ordinal']})
                (root/'boundary.json').write_text(json.dumps({'images':images,'commit_id':cid,'fence':run.fence}))
                os._exit(77)
            with run.journal._connection() as db:
                row=db.execute('SELECT declaration FROM runtime_commit_scopes WHERE scope_id=?',(fact['action_id'],)).fetchone()
            if not row or json.loads(row[0]).get('schema')!='demiflow.stream_lance.v3':return
            declaration=json.loads(row[0])
            (root/'boundary.json').write_text(json.dumps({'record_count':len(declaration['request']['records']),
                'commit_id':cid,'fence':run.fence}))
            os._exit(77)
        commit.fault_point=fault
    else:
        from demiflow.execution.journal.prompt import RunPromptJournal
        original=RunPromptJournal.response
        def response(journal,*args,**kwargs):
            result=original(journal,*args,**kwargs)
            run=run_holder[0]
            (root/'boundary.json').write_text(json.dumps({'native':_native_calls(run.journal),'fence':run.fence}))
            os._exit(77)
        RunPromptJournal.response=response
directory=root/'run'
prior=RunJournal(directory).recovery_identity() if directory.exists() else None
context=RunContext.resume(RunRef(str(directory),prior['run_id'])) if prior else RunContext.start(directory,
    limits={'timeout_s':60,'max_row_bytes':262144,'max_batch_bytes':524288,'max_requests':2,'checkpoint_rows':1 if kind=='timer' else 64})
with context as run,data.local_execution(workers=8):
    run_holder.append(run)
    ds=data.from_items([{'id':'a','value':1,'text':'one'},{'id':'b','value':2,'text':'two'}]).map_async(Slow,concurrency=1)
    if kind=='save':
        ds=ds.save_lance(root/'table.lance',schema=pa.schema([('id',pa.string()),('value',pa.int64())]),
            key='id',stage='saved',max_batch=8,flush_interval=.04,output_ref='ref')
    elif kind=='timer':ds=ds.batch_map(lambda rows:rows,max_batch=2,flush_interval=30)
    else:
        from demiflow.embeddings import EmbeddingModel
        ds=ds.map_embeddings(model=EmbeddingModel('fixture','v1',3,sys.argv[4]),inputs={'text':'text'},
            batch_size=8,concurrency=1,flush_interval=.04,max_requests=2)
    rows=ds.map_async(Observe,concurrency=1).take_all()
    result={'rows':rows,'attempt':run.attempt,'fence':run.fence,'native':_native_calls(run.journal)}
    with run.journal._connection() as db:
        result['memberships']=[json.loads(raw) for raw, in db.execute('SELECT document FROM runtime_sources')
            if json.loads(raw).get('schema')=='demiflow.native_batch_membership.v1']
        result['budgets']=dict(db.execute('SELECT name,consumed FROM runtime_budgets'))
(root/'result.json').write_text(json.dumps(result))
'''


@pytest.mark.parametrize('kind',['save','embedding'])
def test_timed_native_confirm_before_parent_cut_recovers_original_batch_membership(tmp_path,kind):
    worktree=Path(__file__).resolve().parents[1]
    def child(mode,url):
        env=dict(os.environ,PYTHONPATH=str(worktree)+':'+str(worktree/'tests'),PYTHONDONTWRITEBYTECODE='1')
        return subprocess.run([sys.executable,'-B','-c',TIMED_CHILD,str(tmp_path),mode,kind,url],
            cwd=worktree,env=env,capture_output=True,text=True,timeout=90)
    with (_http_fixture(tmp_path) if kind=='embedding' else nullcontext(([],''))) as (requests,url):
        first=child('crash',url+'/v1')
        assert first.returncode==77,first.stderr
        before=json.loads((tmp_path/'boundary.json').read_text())
        if kind=='save':assert before['record_count']==1
        else:
            assert len(requests)==1 and len(requests[0]['body']['input'])==1
            assert before['native']['confirmed']==before['native']['requests_consumed']==1
        second=child('resume',url+'/v1')
        assert second.returncode==0,second.stderr
        result=json.loads((tmp_path/'result.json').read_text())
        assert result['attempt']==2 and result['fence']!=before['fence']
        assert [row['id'] for row in result['rows']]==['a','b']
        assert sorted(len(fact['members']) for fact in result['memberships'])==[1,1]
        assert result['budgets']['native_batch_count']==2
        if kind=='save':assert [row['ref']['version'] for row in result['rows']]==[1,2]
        else:
            assert len(requests)==2 and [len(item['body']['input']) for item in requests]==[1,1]
            assert result['native']['confirmed']==result['native']['requests_consumed']==2
            assert all(row['embedding']==[1.,0.,0.] for row in result['rows'])


@pytest.mark.parametrize('kind',['save','embedding'])
def test_recovery_rejects_changed_native_batch_payload_before_another_effect(tmp_path,kind):
    worktree=Path(__file__).resolve().parents[1]
    def child(mode,url):
        env=dict(os.environ,PYTHONPATH=str(worktree)+':'+str(worktree/'tests'),PYTHONDONTWRITEBYTECODE='1')
        return subprocess.run([sys.executable,'-B','-c',TIMED_CHILD,str(tmp_path),mode,kind,url],
            cwd=worktree,env=env,capture_output=True,text=True,timeout=90)
    with (_http_fixture(tmp_path) if kind=='embedding' else nullcontext(([],''))) as (requests,url):
        first=child('crash',url+'/v1');assert first.returncode==77,first.stderr
        second=child('resume_drift',url+'/v1')
        assert second.returncode!=0 and 'changed original record membership or payload' in second.stderr,second.stderr
        if kind=='embedding':assert len(requests)==1
        else:
            import lance
            table=lance.dataset(str(tmp_path/'table.lance'))
            assert table.version==1 and table.to_table().to_pylist()==[{'id':'a','value':1}]


def test_real_cut_stores_relative_batch_timer_and_restores_pending_rows(tmp_path):
    worktree=Path(__file__).resolve().parents[1]
    def child(mode):
        env=dict(os.environ,PYTHONPATH=str(worktree)+':'+str(worktree/'tests'),PYTHONDONTWRITEBYTECODE='1')
        return subprocess.run([sys.executable,'-B','-c',TIMED_CHILD,str(tmp_path),mode,'timer',''],
            cwd=worktree,env=env,capture_output=True,text=True,timeout=90)
    first=child('crash');assert first.returncode==77,first.stderr
    before=json.loads((tmp_path/'boundary.json').read_text())
    assert before['images'] and all(0<=image['remaining']<=30000 and image['rows']==1 for image in before['images'])
    second=child('resume');assert second.returncode==0,second.stderr
    result=json.loads((tmp_path/'result.json').read_text())
    assert result['attempt']==2 and result['fence']!=before['fence']
    assert [row['id'] for row in result['rows']]==['a','b']
