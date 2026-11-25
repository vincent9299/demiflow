"""Kill actual Dataset file publication drivers at original commit boundaries."""
import json
import os
from pathlib import Path
import subprocess
import sys
import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError, DataflowUnresolvedCommit

WORKTREE = Path(__file__).resolve().parents[1]
CHILD = r'''
import json,os,sys
from pathlib import Path
from demiflow import data
from demiflow.execution.checkpoint import commit
from runtime_v3_recovery_scenarios import _scope
root=Path(sys.argv[1]); boundary=sys.argv[2]; mode=sys.argv[3]; kind=sys.argv[4]
if mode=='crash':
    def at(name,commit_id):
        if name==boundary:
            with (root/'boundary').open('w') as f:
                f.write(json.dumps({'name':name,'commit':commit_id}));f.flush();os.fsync(f.fileno())
            os._exit(79)
    commit.fault_point=at
with _scope(root/'run') as run, data.local_execution():
    getattr(data.from_items([{'id':2,'v':'b'},{'id':1,'v':'a'},{'id':1,'v':'a'}]),'write_'+kind)(str(root/('result.'+kind)))
    with run.journal._connection() as db:
        budget=dict(db.execute('select name,consumed from runtime_budgets'))
    (root/(mode+'.json')).write_text(json.dumps({'attempt':run.attempt,'fence':run.fence,'budgets':budget,'commits':run.journal.execution_commits('action-1')}))
'''

@pytest.mark.parametrize('boundary',['after_intent','after_prepare_before_record','after_prepared','after_sink_commit_before_receipt','after_sink_receipt','after_checkpoint_committed'])
def test_real_file_crash_and_resume_does_not_republish_or_recharge(tmp_path,boundary):
    target=tmp_path/'result.json'
    target.write_text('original protected destination\n')
    env=dict(os.environ,PYTHONPATH=str(WORKTREE)+':'+str(WORKTREE/'tests'),PYTHONDONTWRITEBYTECODE='1')
    def run(mode):
        return subprocess.run([sys.executable,'-B','-c',CHILD,str(tmp_path),boundary,mode,'json'],cwd=WORKTREE,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=90)
    first=run('crash')
    assert first.returncode==79,first.stderr
    assert json.loads((tmp_path/'boundary').read_text())['name']==boundary
    inode=target.stat().st_ino if boundary in ('after_sink_commit_before_receipt','after_sink_receipt','after_checkpoint_committed') else None
    second=run('resume')
    assert second.returncode==0,second.stderr
    result=json.loads((tmp_path/'resume.json').read_text())
    assert result['attempt']==2
    assert result['budgets']['output_rows']==3
    assert result['budgets']['row_bytes']==result['budgets']['output_bytes']==target.stat().st_size
    assert result['commits'] and all(c['status']=='acked' for c in result['commits'])
    assert len(result['commits'])==1
    if inode is not None: assert target.stat().st_ino==inode
    assert [json.loads(line) for line in target.read_text().splitlines()]==[{'id':2,'v':'b'},{'id':1,'v':'a'},{'id':1,'v':'a'}]


def test_file_failure_does_not_replace_existing_target(tmp_path):
    target=tmp_path/'out.json';target.write_text('keep old bytes')
    with pytest.raises(DataflowError,match='budget'):
        with RunContext.start(tmp_path/'run',limits={'max_output_bytes':100}) as run, data.local_execution():
            data.from_items([{'text':'x'*400}]).write_json(str(target))
    assert target.read_text()=='keep old bytes'
    assert not any(c['status']=='acked' for c in run.journal.execution_commits('action-1'))


def test_file_concurrent_destination_is_not_adopted(tmp_path,monkeypatch):
    from demiflow.execution.outputs.file_sink import FileSink
    target=tmp_path/'out.json'
    prepare=FileSink.prepare
    def replace_target(self,blocks,context):
        result=prepare(self,blocks,context)
        target.write_text('foreign publication')
        return result
    monkeypatch.setattr(FileSink,'prepare',replace_target)
    with pytest.raises(DataflowUnresolvedCommit,match='unknown or concurrent'):
        with RunContext.start(tmp_path/'run') as run, data.local_execution():
            data.from_items([{'v':1}]).write_json(str(target))
    assert target.read_text()=='foreign publication'
    assert not any(c['status']=='acked' for c in run.journal.execution_commits('action-1'))


def test_file_resume_refuses_changed_published_bytes(tmp_path):
    env=dict(os.environ,PYTHONPATH=str(WORKTREE)+':'+str(WORKTREE/'tests'),PYTHONDONTWRITEBYTECODE='1')
    args=[sys.executable,'-B','-c',CHILD,str(tmp_path),'after_sink_commit_before_receipt']
    first=subprocess.run(args+['crash','json'],cwd=WORKTREE,env=env,capture_output=True,text=True,timeout=90)
    assert first.returncode==79,first.stderr
    target=tmp_path/'result.json';target.write_text('external changed bytes')
    second=subprocess.run(args+['resume','json'],cwd=WORKTREE,env=env,capture_output=True,text=True,timeout=90)
    assert second.returncode!=0 and 'checksum differs' in second.stderr,second.stderr
    assert target.read_text()=='external changed bytes'


def test_old_file_action_without_native_binding_still_rejects_replay(tmp_path):
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    with pytest.raises(RecoveryContractError,match='unconfirmed'):
        with RunContext.start(tmp_path/'run') as run:
            with run.action({'kind':'write_file','source':None,'operations':[]}):
                pass
    with pytest.raises(RecoveryContractError,match='P3 native sink adapter'):
        with RunContext.resume(run.ref) as resumed:
            with resumed.action({'kind':'write_file','source':None,'operations':[]}):
                pass
