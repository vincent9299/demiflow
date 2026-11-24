"""Real process loss through the checked stream and its native effect journal."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.flow_contract import arrow_fields
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.pipeline.checked import CheckedExecution
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.contracts.recovery_contract import RunRef
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules, LIMITS
from demiflow.workbench.pipeline_policy import DEFAULT_LIMITS
from demiflow.forge.project_bindings import resolve_flow_resources
from test_map_prompt_async import PACK, server


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def execute(root):
    root = Path(root)
    os.chdir(root)
    load('operators.capture', root / 'operators/capture.py')
    flow = load('flows.recovery', root / 'flows/recovery.py')
    report = json.loads((root / 'report.json').read_text())
    checked = CheckedExecution(**{k: report[k] for k in ('nodes', 'edges', 'actions', 'operators')},
        source_root=root, resource_root=root / 'runtime', limits={**DEFAULT_LIMITS, 'memory_bytes': 1024**3},
        record=lambda *args: None, check_stop=lambda: None)
    directory = root / 'runtime'
    prior = RunJournal(directory).recovery_identity()
    scope = RunContext.resume(RunRef(str(directory), prior['run_id'])) if prior else RunContext.start(directory, limits={'memory_bytes': 1024**3})
    with scope, data.local_execution(worker_mode='thread', memory_bytes=512*1024**2) as session:
        session._executor.resource_root = root
        with checked.bind_context():
            flow.run(source=str(root/'input.lance'), target=str(root/'out.lance'))


def setup(root, rows, concurrency=8):
    (root/'flows').mkdir()
    (root/'operators').mkdir()
    (root/'flows/prompt.yaml').write_text(PACK.replace('base_url_env: TEST_PROMPT_URL',
        'base_url: '+os.environ['TEST_PROMPT_URL']))
    (root/'operators/capture.py').write_text('''from typing import TypedDict
class Input(TypedDict):
    id: int | None
    answer: str
class Output(TypedDict):
    id: int | None
    value: str
def finish(row: Input) -> Output:
    return {'id': row['id'], 'value': row['answer'] + ' transformed'}
''')
    (root/'flows/recovery.py').write_text('''from demiflow import data
from operators.capture import finish
def run(*, source, target):
    rows = data.read_lance(source, version=1).map_prompt_async('enrich', config='flows/prompt.yaml',
        inputs={'payload':'item'}, output='answer', concurrency=CONCURRENCY, queue_depth=16,
        max_requests=2048, options={'trust_env':False, 'timeout_s':5,
            'max_request_bytes':1048576, 'max_response_bytes':1048576}).map(finish).materialize()
    return rows.write_lance(target, mode='create', return_receipt=True)
'''.replace('CONCURRENCY', str(concurrency)))
    table = pa.Table.from_pylist(rows)
    source, target = str(root/'input.lance'), str(root/'out.lance')
    lance.write_dataset(table, source)
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    report = FlowChecker(root, catalog, {'input': {'uri':source, 'version':1, 'fields':arrow_fields(table.schema)}},
        [target], resource_resolver=lambda tree, refs, bindings: resolve_flow_resources(root,tree,refs,bindings)).inspect(
            'flows/recovery.py:run', {'source':source, 'target':target})
    assert report['status'] == 'checked', report['diagnostics']
    (root/'report.json').write_text(json.dumps(report))


ROWS = [{'id':i, 'item':str(i)} for i in range(1093)] + [{'id':0,'item':'0'} for _ in range(7)]


def crash(root, window):
    """Run the checked flow in a child process and kill it at ``window``."""
    script = r'''import os,sys
from test_prompt_stream_recovery import execute
from demiflow.execution.journal.run_journal import RunJournal
import demiflow.execution.checkpoint.commit as protocol
from demiflow.operator_llm.runtime import PromptActor
window=sys.argv[2]
if window=='during_batch':
    original=PromptActor.__call__
    calls=[0]
    async def fail(self,row):
        result=await original(self,row)
        calls[0]+=1
        if calls[0]==603:os._exit(37)
        return result
    PromptActor.__call__=fail
else:
    def fail(name,commit_id):
        fact=RunJournal(os.path.join(sys.argv[1],'runtime')).execution_commit(commit_id)
        if name==window and fact['intent']['sinks']==['materialize'] and fact['sequence']==2:os._exit(37)
    protocol.fault_point=fail
execute(sys.argv[1])
'''
    repo = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH':os.pathsep.join([str(repo), str(repo/'tests')]),
           'OPENBLAS_NUM_THREADS':'1','OMP_NUM_THREADS':'1'}
    child = subprocess.run([sys.executable,'-B','-c',script,str(root),window],env=env,
                           capture_output=True,text=True,timeout=300)
    assert child.returncode==37, child.stdout+child.stderr
    assert not (root/'out.lance').exists()


def unanswered_calls(root):
    import sqlite3
    with sqlite3.connect(root/'runtime/run.sqlite') as db:
        return db.execute('SELECT count(*) FROM calls WHERE response_json IS NULL').fetchone()[0]


def test_process_loss_with_calls_in_flight_refuses_to_resend_them(server, tmp_path):
    setup(tmp_path, ROWS, concurrency=8)
    crash(tmp_path, 'during_batch')
    sent = len(server['requests'])
    assert unanswered_calls(tmp_path) > 0
    from demiflow.operator_llm.journal import UncertainPromptCall
    with pytest.raises(UncertainPromptCall, match='automatic regeneration is forbidden'):
        execute(tmp_path)
    assert len(server['requests']) == sent
    assert not (tmp_path/'out.lance').exists()
    assert RunJournal(tmp_path/'runtime').execution_status()['status'] == 'failed'


def test_process_loss_between_calls_resumes_without_asking_the_model_twice(server, tmp_path):
    setup(tmp_path, ROWS, concurrency=1)
    crash(tmp_path, 'after_prepared')
    sent = len(server['requests'])
    assert 0 < sent < len(ROWS) and unanswered_calls(tmp_path) == 0
    execute(tmp_path)
    result=lance.dataset(str(tmp_path/'out.lance')).to_table().to_pylist()
    assert sorted(r['id'] for r in result)==sorted(r['id'] for r in ROWS)
    assert all(r['value']=='ok transformed' for r in result)
    assert len(server['requests']) == len(ROWS)
    assert RunJournal(tmp_path/'runtime').execution_status()['status']=='succeeded'
    # Exactly one execution control database, no private request/result journal.
    assert [p.name for p in tmp_path.rglob('*.sqlite')] == ['run.sqlite']


def test_downstream_block_corruption_is_not_silently_skipped(tmp_path):
    from test_execution_recovery_stream import Counter
    with pytest.raises(RuntimeError, match='later'):
        with RunContext.start(tmp_path/'run') as run:
            reference=run.ref
            data.from_items([{'id':1}]).map_async(Counter()).materialize()
            raise RuntimeError('later')
    receipt=json.loads(next((tmp_path/'run/materializations').rglob('.receipts/*.json')).read_text())
    block=tmp_path/'run/.dataflow-store/local/objects'/receipt['data']['ref']['key']
    block.chmod(0o644)
    block.write_bytes(b'corrupt')
    with pytest.raises(DataflowError, match='artifact size differs from its reference'):
        with RunContext.resume(reference):
            data.from_items([{'id':1}]).map_async(Counter()).materialize()


def test_output_capacity_fails_before_progress(tmp_path):
    from test_execution_recovery_stream import Counter
    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED: artifact exceeds max_bytes'):
        with RunContext.start(tmp_path/'run', limits={'max_scratch_bytes':32}):
            data.from_items([{'id':1}]).map_async(Counter()).materialize()
    journal=RunJournal(tmp_path/'run')
    assert not journal.execution_commits('action-1')
    assert journal.execution_status()['status']=='failed'
