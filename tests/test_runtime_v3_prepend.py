"""Finite prefixes interleave with live upstreams without buffering the main flow."""
import time
import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext


@pytest.mark.parametrize('main,prefix', [([{'n':3}],[{'n':1},{'n':2}]),([],[]),([],[{'n':1}]),([{'n':3}],[])])
def test_prepend_exact_order_including_empty_ports(tmp_path,main,prefix):
    with RunContext.start(tmp_path/'run'),data.local_execution():
        assert data.from_items(main).prepend(data.from_items(prefix),max_rows=2).take_all()==prefix+main


def test_prefix_reaches_downstream_before_live_main_can_start(tmp_path):
    ready=tmp_path/'prefix-observed'; reads=tmp_path/'main-reads'
    def main():
        until=time.monotonic()+10
        while not ready.exists():
            if time.monotonic()>until: raise RuntimeError('prefix did not reach downstream')
            time.sleep(.01)
        for index in range(10000):
            if index>=500: raise RuntimeError('main was materialized instead of backpressured')
            with reads.open('a') as stream: stream.write(str(index)+'\n')
            yield {'n':index+3}
    def observed(row):
        if row['n']==1: ready.write_text('seen')
        return row
    with RunContext.start(tmp_path/'run'),data.local_execution():
        result=data.from_iter(main).prepend(data.from_items([{'n':1},{'n':2}]),max_rows=2).map(observed).take(3)
        assert result==[{'n':1},{'n':2},{'n':3}]
        assert 1<=len(reads.read_text().splitlines())<500


def test_prepend_bound_rejects_without_any_result_commit(tmp_path):
    with pytest.raises(Exception,match='max_rows'):
        with RunContext.start(tmp_path/'run') as run,data.local_execution():
            data.from_items([{'n':3}]).prepend(data.from_items([{'n':1},{'n':2}]),max_rows=1).take_all()
    assert not run.journal.execution_action('action-1')['result']


@pytest.mark.parametrize('shared',[False,True])
def test_prepend_driver_crash_after_native_checkpoint_does_not_repeat_prefix(tmp_path,shared):
    import os, subprocess, sys, json
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    script=r'''
import json,os,sys
from pathlib import Path
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.contracts.recovery_contract import RunRef
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.state_artifacts import StateArtifacts
from demiflow.execution.checkpoint import commit
root=Path(sys.argv[1]);mode=sys.argv[2];shared=sys.argv[3]=='True';directory=root/'run'
prior=RunJournal(directory).recovery_identity() if directory.exists() else None
scope=RunContext.resume(RunRef(str(directory),prior['run_id'])) if prior else RunContext.start(directory,limits={'checkpoint_rows':2,'timeout_s':30})
with scope as run,data.local_execution():
    if mode=='crash':
        def boundary(name,commit_id):
            if name!='after_checkpoint_committed': return
            facts=run.journal.execution_commits('action-1')
            fact=next(f for f in facts if f['commit_id']==commit_id)
            carrier=StateArtifacts(run).read(fact['prepared']['state']['dataflow-cut-v3'])
            if carrier['cut']['final']: return
            (root/'checkpoint.json').write_text(json.dumps({'commit':commit_id,'fence':run.fence}))
            os._exit(78)
        commit.fault_point=boundary
    base=data.from_items([{'n':n} for n in range(1,15)])
    ds=base.prepend(base.limit(3),max_rows=3) if shared else data.from_items([{'n':n} for n in range(3,15)]).prepend(data.from_items([{'n':1},{'n':2}]),max_rows=2)
    rows=ds.take_all()
    (root/'result.json').write_text(json.dumps({'rows':rows,'attempt':run.attempt,'fence':run.fence,'commits':run.journal.execution_commits('action-1')}))
'''
    env=dict(os.environ,PYTHONPATH=str(root)+':'+str(root/'tests'),PYTHONDONTWRITEBYTECODE='1')
    def call(mode):
        return subprocess.run([sys.executable,'-B','-c',script,str(tmp_path),mode,str(shared)],cwd=root,env=env,capture_output=True,text=True,timeout=65)
    crashed=call('crash')
    assert crashed.returncode==78,crashed.stderr
    evidence=json.loads((tmp_path/'checkpoint.json').read_text())
    resumed=call('resume')
    assert resumed.returncode==0,resumed.stderr
    result=json.loads((tmp_path/'result.json').read_text())
    assert result['rows']==([{'n':n} for n in (1,2,3)]+[{'n':n} for n in range(1,15)] if shared else [{'n':n} for n in range(1,15)])
    assert result['attempt']==2 and result['fence']!=evidence['fence']
    assert any(c['commit_id']==evidence['commit'] for c in result['commits'])
    assert all(c['status']=='acked' for c in result['commits'])


def test_prepend_cancel_after_prefix_reaches_downstream(tmp_path):
    import threading
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    ready=tmp_path/'seen'
    def endless():
        n=3
        while True:
            yield {'n':n};n+=1
            time.sleep(.005)
    def observe(row):
        if row['n']==1: ready.write_text('seen')
        return row
    with pytest.raises(RecoveryContractError,match='[Cc]ancel'):
        with RunContext.start(tmp_path/'run',limits={'timeout_s':20}) as run,data.local_execution():
            def cancel():
                until=time.monotonic()+10
                while not ready.exists() and time.monotonic()<until: time.sleep(.01)
                RunContext.cancel(run.ref)
            thread=threading.Thread(target=cancel);thread.start()
            try:
                with pytest.raises(Exception,match='[Cc]ancel'):
                    data.from_iter(endless).prepend(data.from_items([{'n':1},{'n':2}]),max_rows=2).map(observe).take_all()
            finally: thread.join(12)
            assert ready.exists() and not thread.is_alive()
            assert not run.journal.execution_action('action-1')['result']


def test_shared_source_prefix_runs_original_source_once(tmp_path):
    seen=tmp_path/'reads'
    def record(row):
        with seen.open('a') as output: output.write(str(row['n'])+'\n')
        return row
    with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':2,'timeout_s':15}),data.local_execution():
        shared=data.from_items([{'n':n} for n in range(1,5)]).map(record)
        result=shared.prepend(shared.limit(3),max_rows=3).take_all()
    assert result==[{'n':n} for n in (1,2,3,1,2,3,4)]
    assert seen.read_text().splitlines()==['1','2','3','4']
