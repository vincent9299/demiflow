"""Real timed groups: consumer causality, retained bytes and original cut recovery."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext


def test_group_first_output_arrives_before_source_eoi_with_typed_rows(tmp_path):
    seen=tmp_path/'seen';eoi=tmp_path/'eoi'
    def source():
        yield {'key':'a','value':(-0.0,b'one')}
        until=time.monotonic()+20
        while not seen.exists() and time.monotonic()<until:time.sleep(.01)
        assert seen.exists(),'No timed group output arrived while source was still open'
        eoi.write_text('done')
        yield {'key':'a','value':(2.0,b'two')}
    with RunContext.start(tmp_path/'run',limits={'timeout_s':45}),data.local_execution(workers=4):
        iterator=iter(data.from_iter(source).group_batches('key',max_rows=8,flush_interval=.03,
            chunk_bytes=4096,buffer_bytes=8192).iter_rows())
        first=next(iterator)
        assert not eoi.exists()
        assert first=={'key':'a','items':[{'key':'a','value':(-0.0,b'one')}]}
        import math
        assert math.copysign(1,first['items'][0]['value'][0])==-1
        seen.write_text('received')
        rest=list(iterator)
    assert rest==[{'key':'a','items':[{'key':'a','value':(2.0,b'two')}]}]


def test_group_pressure_preserves_all_rows_and_oversize_refuses(tmp_path):
    rows=[{'key':str(i%3),'value':'x'*600,'id':i} for i in range(20)]
    with RunContext.start(tmp_path/'run'),data.local_execution(workers=4):
        groups=data.from_items(rows).group_batches('key',max_rows=100,flush_interval=30,
            chunk_bytes=2048,buffer_bytes=4096,max_groups=2).take_all()
    assert sorted((r for g in groups for r in g['items']),key=lambda r:r['id'])==rows
    assert all(set(g)=={'key','items'} for g in groups)
    with pytest.raises(ValueError,match='chunk_bytes'),RunContext.start(tmp_path/'oversize'),data.local_execution(workers=4):
        data.from_items([{'key':'a','value':'x'*10000}]).group_batches('key',flush_interval=.03,
            chunk_bytes=2048,buffer_bytes=4096).take_all()


CHILD=r'''
import json,os,sys
from pathlib import Path
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.contracts.recovery_contract import RunRef
root=Path(sys.argv[1]);mode=sys.argv[2]
contexts=[]
if mode=='crash':
 from demiflow.execution.checkpoint import commit
 def fault(name,cid):
  if name!='after_checkpoint_committed':return
  run=contexts[0];fact=run.journal.execution_commit(cid)
  if fact['action_id']!='action-1':return
  from demiflow.execution.contracts import state_codec
  images=[]
  for path in (run.directory/'.dataflow-store').rglob('*'):
   if not path.is_file() or path.stat().st_size>run.limits['max_state_bytes']:continue
   raw=path.read_bytes()
   if b'"group_remaining_ms"' not in raw:continue
   try:image=state_codec.loads(raw,max_bytes=run.limits['max_state_bytes'])
   except Exception:continue
   if image.get('kind')=='StreamGroupBatchesOp' and image.get('groups'):
    images.append({'remaining':image['group_remaining_ms'],'old_absolute':image['timer_deadline'],
                  'rows':sum(map(len,image['groups'].values()))})
  if not images:return
  (root/'boundary.json').write_text(json.dumps({'commit_id':cid,'images':images}))
  os._exit(77)
 commit.fault_point=fault
p=root/'run';old=RunJournal(p).recovery_identity() if p.exists() else None
ctx=RunContext.resume(RunRef(str(p),old['run_id'])) if old else RunContext.start(p,limits={'checkpoint_rows':1,'timeout_s':60})
with ctx as run,data.local_execution(workers=4):
 contexts.append(run)
 result=data.from_items([{'key':'a','id':i} for i in range(3)]).group_batches('key',max_rows=8,
     flush_interval=30,chunk_bytes=4096,buffer_bytes=8192).take_all()
 (root/'result.json').write_text(json.dumps({'rows':result,'attempt':run.attempt,
     'commits':[{'id':f['commit_id'],'status':f['status']} for f in run.journal.execution_commits('action-1')]}))
'''


def test_group_real_driver_crash_restores_original_pending_relative_timer(tmp_path):
    script=tmp_path/'child.py';script.write_text(CHILD)
    env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parents[1])+':'+str(Path(__file__).resolve().parent)}
    before=subprocess.run([sys.executable,'-B',str(script),str(tmp_path),'crash'],env=env,capture_output=True,text=True,timeout=90)
    assert before.returncode==77,before.stdout+before.stderr
    boundary=json.loads((tmp_path/'boundary.json').read_text())
    assert all(0<=v['remaining']<=30000 and v['old_absolute'] is None and v['rows']>0 for v in boundary['images'])
    after=subprocess.run([sys.executable,'-B',str(script),str(tmp_path),'resume'],env=env,capture_output=True,text=True,timeout=90)
    assert after.returncode==0,after.stdout+after.stderr
    result=json.loads((tmp_path/'result.json').read_text())
    assert result['attempt']==2
    assert result['rows']==[{'key':'a','items':[{'key':'a','id':i} for i in range(3)]}]
    assert any(c['id']==boundary['commit_id'] and c['status']=='acked' for c in result['commits'])
