"""Original call facts are stable data; replay metrics still count real reuse."""
import json
import os
from pathlib import Path
import subprocess
import sys

import lance
import pytest

from test_runtime_v3_native_execution import PACK, native_server
from test_codex_exec import fake_cli

WORKTREE = Path(__file__).resolve().parents[1]


def pack_call(row):
    error = row.get('error')
    call = error['call'] if error else row['call']
    return {'id': row['id'], 'value': row.get('answer', 'failed'),
            'call_json': json.dumps(call, sort_keys=True),
            'error_json': json.dumps(error, sort_keys=True)}


def pack_second(row):
    return {key: row[key] for key in ('id', 'value', 'call_json', 'error_json')}


CHILD = r'''
import json,os,sys
from pathlib import Path
import pyarrow as pa
import demiflow
from demiflow import data
from demiflow.execution import RunContext,RunRef
from demiflow.execution.checkpoint import commit
from demiflow.execution.journal.run_telemetry import CallMetrics
from demiflow.operator_llm.parser import parse_prompt_pack
from test_native_prompt_delivery_replay import pack_call,pack_second
root=Path(sys.argv[1]);mode=sys.argv[2]
assert Path(demiflow.__file__).resolve().parents[1]==Path(sys.argv[3]).resolve()
options=json.loads((root/'options.json').read_text());pack=parse_prompt_pack((root/'prompt.yaml').read_text())
prior=json.loads((root/'ref.json').read_text()) if mode=='resume' else None
context=RunContext.resume(RunRef(prior['directory'],prior['run_id'])) if prior else RunContext.start(root/'native',limits={'timeout_s':60,'memory_bytes':2*1024**3,'max_state_bytes':16*1024**2})
observe=CallMetrics.observe
def observation(self,call,*,error=False):
    observe(self,call,error=error)
    with (root/('observed-'+mode+'.jsonl')).open('a') as f:
        f.write(json.dumps({'reused':call.get('reused'),'error':error,'summary':self.summary()})+'\n')
CallMetrics.observe=observation
with context as run,data.local_execution(workers=16,worker_mode='thread',memory_bytes=2*1024**3):
    (root/'ref.json').write_text(json.dumps({'directory':run.ref.directory,'run_id':run.ref.run_id}))
    if mode=='crash':
        def fault(name,commit_id):
            if name!='after_checkpoint_committed':return
            fact=run.journal.execution_commit(commit_id)
            with run.journal._connection() as db:
                if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():return
                scope=db.execute('SELECT declaration FROM runtime_commit_scopes WHERE scope_id=?',(fact['action_id'],)).fetchone()
            if scope and json.loads(scope[0]).get('schema')=='demiflow.stream_lance.v3':os._exit(77)
        commit.fault_point=fault
    schema=pa.schema([('id',pa.string()),('value',pa.string()),('call_json',pa.string()),('error_json',pa.string())])
    rows=data.from_items([{'id':'a','item':'first call'}]).map_prompt_async('enrich',config=pack,inputs={'payload':'item'},
        output='answer',call_output='call',error_output='error',options=options,max_requests=1,concurrency=1)
    rows=rows.map(pack_call).save_lance(root/'stage.lance',schema=schema,key='id',stage='saved',mode='append',max_batch=1,output_ref='saved')
    rows=rows.map_prompt_async('enrich',config=pack,inputs={'payload':'value'},output='second',call_output='second_call',
        error_output='second_error',options=options,max_requests=1,concurrency=1)
    receipt=rows.map(pack_second).write_lance(str(root/'result.lance'),schema=schema,mode='create',return_receipt=True)
    (root/'result.json').write_text(json.dumps({'rows':receipt.input_rows,'requests':run.journal.budget_consumed('requests'),'attempt':run.attempt}))
'''


@pytest.mark.parametrize('transport', ['http', 'codex'])
@pytest.mark.parametrize('outcome', ['success', 'schema_error'])
def test_prompt_then_native_save_then_prompt_survives_hard_exit(
        tmp_path, native_server, fake_cli, monkeypatch, transport, outcome):
    cli, capture = fake_cli
    response = {'result': 'ok' if outcome == 'success' else 99}
    native_server.response = response
    monkeypatch.setenv('FAKE_CODEX_CONTENT', json.dumps(response))
    options = ({'codex_exec': {'bin': str(cli)}, 'timeout_s': 20} if transport == 'codex'
               else {'trust_env': False, 'timeout_s': 20})
    (tmp_path/'options.json').write_text(json.dumps(options))
    (tmp_path/'prompt.yaml').write_text(PACK)
    # Keep both attempts in the same capacity domain, isolated per scenario.
    env = {**os.environ, 'PYTHONPATH': str(WORKTREE)+':'+str(WORKTREE/'tests'), 'PYTHONDONTWRITEBYTECODE':'1',
           'DEMIFLOW_RESOURCE_ROOT': str(tmp_path/'resources'),
           'DEMIFLOW_RESOURCE_HOME': str(tmp_path/'resource-home')}
    def child(mode):
        return subprocess.run([sys.executable, '-B', '-c', CHILD, str(tmp_path), mode, str(WORKTREE)],
            cwd=WORKTREE, env=env, capture_output=True, text=True, timeout=100)
    first = child('crash')
    assert first.returncode == 77, first.stderr
    original = lance.dataset(str(tmp_path/'stage.lance')).to_table().to_pylist()
    assert len(original) == 1
    trace = json.loads(original[0]['call_json'])
    assert trace['reused'] is False and all(not a['reused'] for a in trace.get('attempts', []))
    assert (json.loads(original[0]['error_json']) is None) == (outcome == 'success')
    second = child('resume')
    assert second.returncode == 0, second.stderr
    assert lance.dataset(str(tmp_path/'stage.lance')).version == 1
    assert lance.dataset(str(tmp_path/'result.lance')).to_table().to_pylist() == original
    result = json.loads((tmp_path/'result.json').read_text())
    assert result == {'rows': 1, 'requests': 2, 'attempt': 2}
    observations = [json.loads(line) for line in (tmp_path/'observed-resume.jsonl').read_text().splitlines()]
    assert sorted(o['reused'] for o in observations) == [False, True]
    reused = next(o['summary'] for o in observations if o['reused'])
    assert reused['reused'] == 1 and reused['input_tokens'] == reused['output_tokens'] == 0
    if transport == 'http':
        assert len(native_server) == 2 and not capture.exists()
    else:
        assert len(capture.read_text().splitlines()) == 2 and not native_server
