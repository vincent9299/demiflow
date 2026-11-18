"""Concurrent completion order is not native effect membership or data drift."""
import json
import os
from pathlib import Path
import subprocess
import sys

import lance
import pytest

WORKTREE = Path(__file__).resolve().parents[1]
CHILD = r'''
import json,os,sys,time
from pathlib import Path
import pyarrow as pa
import demiflow
from demiflow import data
from demiflow.execution import RunContext,RunRef,commit
root=Path(sys.argv[1]);mode=sys.argv[2]
budget_test=sys.argv[4]=='budget'
assert Path(demiflow.__file__).resolve().parents[1]==Path(sys.argv[3]).resolve()
os.environ['NATIVE_BATCH_REPLAY_ORDER']=mode
class Reorder:
    def __call__(self,row):
        mode=os.environ['NATIVE_BATCH_REPLAY_ORDER']
        # Deliberately reverse live completion order, including across the
        # original batch boundary. This is a test callback, not engine patching.
        rank=('badc' if mode=='crash' else 'cdab').index(row['id'])
        time.sleep(rank*.15)
        return {**row,'value':99 if mode=='drift' and row['id']=='a' else row['value'],
                'drop':mode=='missing' and row['id']=='b'}
def keep(row):return not row['drop']
prior=json.loads((root/'ref.json').read_text()) if mode!='crash' else None
limits={'timeout_s':45,**({'max_row_bytes':32768,'max_batch_bytes':65536} if budget_test else {})}
context=RunContext.resume(RunRef(prior['directory'],prior['run_id'])) if prior else RunContext.start(root/'native',limits=limits)
with context as run,data.local_execution(workers=8,worker_mode='thread'):
    (root/'ref.json').write_text(json.dumps({'directory':run.ref.directory,'run_id':run.ref.run_id}))
    if mode=='crash':
        def fault(name,cid):
            if name!='after_checkpoint_committed':return
            fact=run.journal.execution_commit(cid)
            with run.journal._connection() as db:
                if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():return
                scope=db.execute('SELECT declaration FROM runtime_commit_scopes WHERE scope_id=?',(fact['action_id'],)).fetchone()
            if scope and json.loads(scope[0]).get('schema')=='demiflow.stream_lance.v3':os._exit(77)
        commit.fault_point=fault
    schema=pa.schema([('id',pa.string()),('value',pa.int64())])
    rows=data.from_items([{'id':key,'value':1,'padding':'x'*22000} for key in 'abcd']).map_async(Reorder,concurrency=4).filter(keep)
    result=rows.save_lance(root/'stage.lance',schema=schema,key='id',stage='saved',max_batch=2,
        flush_interval=5,output_ref='saved').take_all()
    (root/'result.json').write_text(json.dumps(result))
'''


@pytest.mark.parametrize('mode', ['resume', 'drift', 'missing', 'budget'])
def test_native_batch_replay_keeps_original_order_and_rejects_changed_data(tmp_path, mode):
    # Crash and resume share one domain; unrelated test scenarios must not
    # inherit each other's deliberately unconfirmed hard-exit allocations.
    env = {**os.environ, 'PYTHONPATH': str(WORKTREE), 'PYTHONDONTWRITEBYTECODE': '1',
           'DEMIFLOW_RESOURCE_ROOT': str(tmp_path/'resources'),
           'DEMIFLOW_RESOURCE_HOME': str(tmp_path/'resource-home')}
    def child(value):
        return subprocess.run([sys.executable, '-B', '-c', CHILD, str(tmp_path), value, str(WORKTREE), mode],
            cwd=WORKTREE, env=env, capture_output=True, text=True, timeout=90)
    first = child('crash')
    assert first.returncode == 77, first.stderr
    original = lance.dataset(str(tmp_path/'stage.lance')).to_table().to_pylist()
    assert original == [{'id': 'b', 'value': 1}, {'id': 'a', 'value': 1}]
    second = child(mode)
    saved = lance.dataset(str(tmp_path/'stage.lance'))
    if mode == 'resume':
        assert second.returncode == 0, second.stderr
        assert saved.version == 2
        assert saved.to_table().to_pylist() == original + [{'id': 'c', 'value': 1}, {'id': 'd', 'value': 1}]
        result = json.loads((tmp_path/'result.json').read_text())
        assert [r['id'] for r in result] == ['b', 'a', 'c', 'd']
        assert [r['saved']['version'] for r in result] == [1, 1, 2, 2]
    else:
        assert second.returncode != 0, second.stderr
        if mode == 'budget':
            assert 'BUDGET_EXCEEDED' in second.stderr and 'original batch window' in second.stderr, second.stderr
        else:
            assert 'INCOMPATIBLE_HISTORY' in second.stderr, second.stderr
            assert ('changed original record membership or payload' if mode == 'drift'
                    else 'original batch membership was complete') in second.stderr
        assert saved.version == 1 and saved.to_table().to_pylist() == original
