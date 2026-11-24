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
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.pipeline.checked import CheckedExecution
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
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
    with scope, data.local_execution(workers=1, worker_mode='thread', partitions=1, memory_bytes=128*1024**2) as session:
        session._executor.resource_root = root
        with checked.bind_context():
            flow.run(source=str(root/'input.lance'), target=str(root/'out.lance'))


def setup(root, rows):
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
        inputs={'payload':'item'}, output='answer', concurrency=8, queue_depth=16,
        max_requests=2048, options={'trust_env':False, 'timeout_s':5,
            'max_request_bytes':1048576, 'max_response_bytes':1048576}).map(finish).materialize()
    return rows.write_lance(target, mode='create', return_receipt=True)
''')
    table = pa.Table.from_pylist(rows)
    source, target = str(root/'input.lance'), str(root/'out.lance')
    lance.write_dataset(table, source)
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    report = FlowChecker(root, catalog, {'input': {'uri':source, 'version':1, 'fields':arrow_fields(table.schema)}},
        [target], resource_resolver=lambda tree, refs, bindings: resolve_flow_resources(root,tree,refs,bindings)).inspect(
            'flows/recovery.py:run', {'source':source, 'target':target})
    assert report['status'] == 'checked', report['diagnostics']
    (root/'report.json').write_text(json.dumps(report))


@pytest.mark.parametrize('window', ['during_batch', 'after_prepared', 'after_checkpoint_committed'])
def test_process_loss_reuses_known_responses_and_refuses_unknown_calls(server, tmp_path, window):
    rows = [{'id':i, 'item':str(i)} for i in range(1093)] + [{'id':0,'item':'0'} for _ in range(7)]
    setup(tmp_path, rows)
    script = r'''import os,sys
from test_prompt_stream_recovery import execute
from demiflow.execution.run_journal import RunJournal
import demiflow.execution.commit as protocol
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
        if name==window and fact['intent']['sinks']==['output'] and fact['sequence']==2:os._exit(37)
    protocol.fault_point=fail
execute(sys.argv[1])
'''
    repo = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH':os.pathsep.join([str(repo), str(repo/'tests')]),
           'OPENBLAS_NUM_THREADS':'1','OMP_NUM_THREADS':'1'}
    child = subprocess.run([sys.executable,'-B','-c',script,str(tmp_path),window],env=env,
                           capture_output=True,text=True,timeout=180)
    assert child.returncode==37, child.stdout+child.stderr
    assert not (tmp_path/'out.lance').exists()
    old_calls=len(server['requests'])
    import sqlite3
    with sqlite3.connect(tmp_path/'runtime/run.sqlite') as db:
        uncertain = db.execute('SELECT count(*) FROM calls WHERE response_json IS NULL').fetchone()[0]
    if uncertain:
        from demiflow.operator_llm.journal import UncertainPromptCall
        with pytest.raises(UncertainPromptCall, match='automatic regeneration is forbidden'):
            execute(tmp_path)
        assert len(server['requests']) == old_calls
        assert not (tmp_path/'out.lance').exists()
        assert RunJournal(tmp_path/'runtime').execution_status()['status'] == 'failed'
        return
    execute(tmp_path)
    result=lance.dataset(str(tmp_path/'out.lance')).to_table().to_pylist()
    assert sorted(r['id'] for r in result)==sorted(r['id'] for r in rows)
    assert all(r['value']=='ok transformed' for r in result)
    assert len(server['requests']) == len(rows)
    assert RunJournal(tmp_path/'runtime').execution_status()['status']=='succeeded'
    assert not list(tmp_path.rglob('checkpoint.json'))
    # Exactly one execution control database, no private request/result journal.
    assert [p.name for p in tmp_path.rglob('*.sqlite')] == ['run.sqlite']
    import sqlite3
    with sqlite3.connect(tmp_path/'runtime/run.sqlite') as db:
        reserved=db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0]
    assert reserved >= len(server['requests'])


def test_legacy_journal_option_is_not_a_model_execution_mode():
    with pytest.raises(ValueError,match='execution-owned'):
        data.from_items([]).map_prompt_async('x', config='unused.yaml', inputs=['x'], output='y',
                                            options={'sqlite_journal':{'path':'unused'}})


def test_downstream_block_corruption_is_not_silently_skipped(tmp_path):
    from test_execution_recovery_stream import Counter
    with pytest.raises(RuntimeError, match='later'):
        with RunContext.start(tmp_path/'run') as run:
            reference=run.ref
            data.from_items([{'id':1}]).map_async(Counter()).materialize()
            raise RuntimeError('later')
    block=next((tmp_path/'run/materializations/action-1/output').rglob('*.pkl'))
    block.write_bytes(b'corrupt')
    with pytest.raises(RuntimeContractError, match='block size differs'):
        with RunContext.resume(reference):
            data.from_items([{'id':1}]).map_async(Counter()).materialize()


def test_output_capacity_fails_before_progress(tmp_path):
    from test_execution_recovery_stream import Counter
    with pytest.raises(RuntimeContractError, match='scratch.*budget'):
        with RunContext.start(tmp_path/'run', limits={'max_scratch_bytes':32}):
            data.from_items([{'id':1}]).map_async(Counter()).materialize()
    journal=RunJournal(tmp_path/'run')
    assert not journal.execution_commits('action-1')
    assert journal.execution_status()['status']=='failed'
