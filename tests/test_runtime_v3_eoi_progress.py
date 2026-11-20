"""EOI is a recoverable frontier, never a blocking wait for co-resident work."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from demiflow import data
from _worker_event_fixtures import emit, events


@pytest.mark.parametrize('batch', [False, True], ids=['row_calls', 'final_partial_batch'])
def test_eoi_keeps_polling_a_downstream_timer_while_upstream_waits(tmp_path, batch):
    gate, observed = str(tmp_path/'delivered'), str(tmp_path/'calls')
    async def upstream(value):
        rows = value if batch else [value]
        emit(observed, 'invoke', ids=[row['id'] for row in rows])
        if any(row['id'] == 3 for row in rows):
            async def wait():
                while not Path(gate).exists():
                    await asyncio.sleep(.001)
            await asyncio.wait_for(wait(), 2)
        return value
    async def downstream(group):
        for row in group['items']:
            emit(observed, 'result', id=row['id'])
        Path(gate).touch()
        return group
    ds = data.from_items([{'key': 'a', 'id': 1}, {'key': 'b', 'id': 2}, {'key': 'a', 'id': 3}])
    if batch:
        ds = ds.batch_map(upstream, max_batch=2, concurrency=2, queue_depth=1)
    else:
        ds = ds.map_async(upstream, concurrency=3, queue_depth=1)
    ds.group_batches('key', max_rows=4, flush_interval=.5, chunk_bytes=4096, buffer_bytes=8192).map_async(
        downstream, concurrency=1, queue_depth=1).run_stream()
    observed = events(observed)
    assert sorted(event['id'] for event in observed if event['event'] == 'result') == [1, 2, 3]
    assert sorted(event['ids'] for event in observed if event['event'] == 'invoke') == (
        [[1, 2], [3]] if batch else [[1], [2], [3]])


CHILD = r'''
import json,os,sys
from pathlib import Path
from demiflow import data
from demiflow.embeddings import EmbeddingModel
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RunRef
from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
from demiflow.execution.kernels import state_codec
root=Path(sys.argv[1]);mode=sys.argv[2];url=sys.argv[3]
if mode=='crash':
 original=DataflowCheckpointCoordinator.commit
 def commit(coordinator,*args,**kwargs):
  image=original(coordinator,*args,**kwargs)
  for contribution in image.contributions:
   states=[];frontier=None
   for ref in contribution.operator_states.values():
    raw=b''.join(coordinator._store.read(ref,max_bytes=ref.size_bytes))
    if ref.media_type=='application/x-demiflow-kernel-image-v3':
     state=state_codec.loads(raw,max_bytes=coordinator._physical.bounds.max_state_bytes)
     if state.get('kind')=='embedding':states.append(state)
    elif b'demiflow.worker-frontier.v3' in raw:
     frontier=json.loads(raw)
   for state in states:
    if not state.get('inflight_calls') or not frontier or not frontier.get('finished_ports'):continue
    assert not image.cut.final and state['batch']==[]
    assert state['batch_ordinal']==1 and len(state['inflight_calls'])==1
    (root/'cut.json').write_text(json.dumps({'commit_id':image.commit_id,
        'finished_ports':frontier['finished_ports'],'inputs':frontier['inputs'],
        'batch_ordinal':state['batch_ordinal']}))
  return image
 DataflowCheckpointCoordinator.commit=commit
 from demiflow.operator_llm.stream_recovery import RunPromptJournal
 response=RunPromptJournal.response
 def confirmed(journal,*args,**kwargs):
  result=response(journal,*args,**kwargs)
  assert (root/'cut.json').exists(),'native response preceded pending EOF cut'
  os._exit(77)
 RunPromptJournal.response=confirmed
p=root/'run'
prior=RunJournal(p).recovery_identity() if p.exists() else None
ctx=RunContext.resume(RunRef(str(p),prior['run_id'])) if prior else RunContext.start(p,
 limits={'timeout_s':45,'checkpoint_rows':64,'checkpoint_interval_ms':100,'max_requests':1})
with ctx as run,data.local_execution(workers=4):
 rows=data.from_items([{'text':'one'}]).map_embeddings(
   model=EmbeddingModel('fixture','v1',3,url+'/v1'),inputs={'text':'text'},
   batch_size=8,concurrency=2,max_requests=1).take_all()
 with run.journal._connection() as db:
  requests=db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0]
 (root/'result.json').write_text(json.dumps({'rows':rows,'attempt':run.attempt,'requests':requests,
   'commits':[{'id':f['commit_id'],'status':f['status']} for f in run.journal.execution_commits('action-1')]}))
'''


def test_final_native_batch_eoi_cut_survives_crash_without_second_request(tmp_path):
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers['Content-Length'])
            assert 0 < length <= 128*1024
            body = json.loads(self.rfile.read(length))
            calls.append(body)
            until = time.monotonic()+25
            while not (tmp_path/'cut.json').exists() and time.monotonic() < until:
                time.sleep(.01)
            assert (tmp_path/'cut.json').exists(), 'EOF pending native call never reached original cut'
            raw = json.dumps({'data':[{'index':0,'embedding':[1.,0.,0.]}],
                              'usage':{'prompt_tokens':1}}).encode()
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(raw)))
            self.end_headers(); self.wfile.write(raw)
        def log_message(self,*args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
    server_thread = threading.Thread(target=server.serve_forever,daemon=True)
    server_thread.start()
    script = tmp_path/'eoi_child.py'; script.write_text(CHILD)
    tree = Path(__file__).resolve().parents[1]
    env = dict(os.environ,PYTHONPATH=str(tree)+':'+str(tree/'tests'),PYTHONDONTWRITEBYTECODE='1')
    def child(mode):
        return subprocess.run([sys.executable,'-B',str(script),str(tmp_path),mode,
            'http://127.0.0.1:'+str(server.server_port)],cwd=tree,env=env,
            capture_output=True,text=True,timeout=65)
    try:
        first = child('crash')
        assert first.returncode == 77, first.stdout+first.stderr
        cut = json.loads((tmp_path/'cut.json').read_text())
        assert cut['finished_ports'] and cut['inputs'] and cut['batch_ordinal']==1
        second = child('resume')
        assert second.returncode == 0, second.stdout+second.stderr
        result = json.loads((tmp_path/'result.json').read_text())
        assert result['attempt']==2 and result['requests']==1
        assert result['rows']==[{'text':'one','embedding':[1.,0.,0.]}]
        assert any(c['id']==cut['commit_id'] and c['status']=='acked' for c in result['commits'])
        assert len(calls)==1 and calls[0]['input']==['one']
    finally:
        server.shutdown(); server.server_close(); server_thread.join(timeout=5)
        assert not server_thread.is_alive()
