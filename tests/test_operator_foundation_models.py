"""Native model contract, sealed resources and independently checked effects."""
import importlib.util
import json
import os
from pathlib import Path
import sys

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.flow_contract import field, scalar
from demiflow.execution.pipeline.checked import CheckedExecution
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules, LIMITS
from demiflow.workbench.runs.manager import DEFAULT_LIMITS
from demiflow.forge.project_bindings import resolve_flow_resources
from demiflow.execution.pipeline.local_kernel import local_execution
from demiflow.operator_llm.call_ref import PromptRecordRef
from test_map_prompt_async import PACK, server


def model_files(root):
    (root / 'flows').mkdir(exist_ok=True)
    (root / 'operators').mkdir(exist_ok=True)
    (root / 'flows/prompt.yaml').write_text(PACK.replace('base_url_env: TEST_PROMPT_URL', 'base_url: ' + os.environ['TEST_PROMPT_URL']))
    (root / 'operators/predicate.py').write_text('''from typing import TypedDict
class Row(TypedDict):
    item: str | None
def selected(row: Row) -> bool:
    return row['item'] != 'skip'
''')
    (root / 'flows/model.py').write_text('''from demiflow import data
from operators.predicate import selected
def run(*, source, target):
    result = data.read_lance(source, version=1).map_prompt_async('enrich', config='flows/prompt.yaml',
        inputs={'payload': 'item'}, output='answer', when=selected, call_output='call', error_output='error',
        max_requests=2, options={'trust_env': False, 'collect_journal_totals': False,
            'timeout_s': 2, 'max_request_bytes': 1048576, 'max_response_bytes': 1048576}).materialize()
    return result.write_lance(target, mode='create', return_receipt=True)
''')


def load(monkeypatch, name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def test_real_model_when_result_and_durable_ledger(server, tmp_path, monkeypatch):
    model_files(tmp_path)
    source, target = str(tmp_path/'input.lance'), str(tmp_path/'out.lance')
    lance.write_dataset(pa.table({'item': ['call', 'skip']}), source)
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    tables = {'input': {'uri': source, 'version': 1, 'fields': {'item': field(scalar('str', nullable=True))}}}
    report = FlowChecker(tmp_path, catalog, tables, [target],
        resource_resolver=lambda tree, refs, bindings: resolve_flow_resources(tmp_path, tree, refs, bindings)).inspect('flows/model.py:run', dict(source=source, target=target))
    assert report['status'] == 'checked', report['diagnostics']
    prompt = next(n for n in report['nodes'] if n['kind'] == 'prompt')
    assert not prompt['fields']['answer']['required']
    load(monkeypatch, 'operators.predicate', tmp_path/'operators/predicate.py')
    flow = load(monkeypatch, 'flows.model', tmp_path/'flows/model.py')
    monkeypatch.chdir(tmp_path)
    events = []
    context = CheckedExecution(nodes=report['nodes'], edges=report['edges'], actions=report['actions'],
        operators=report['operators'], source_root=tmp_path, limits=DEFAULT_LIMITS,
        record=lambda *a: events.append(a), check_stop=lambda: None)
    (tmp_path / 'work').mkdir()
    with RunContext.start(tmp_path / 'run') as run, local_execution(
                         workers=1, worker_mode='thread', partitions=1, memory_bytes=128*1024**2,
                         temp_directory=str(tmp_path/'work')) as native:
        native._executor.resource_root = tmp_path
        with context.bind_context():
            flow.run(source=source, target=target)
    assert len(server['requests']) == 1
    result = lance.dataset(target).to_table().to_pylist()
    called, skipped = sorted(result, key=lambda row: row['item'])
    assert (called['item'], called['answer']) == ('call', 'ok')
    assert (skipped['item'], skipped['answer'], skipped['call']) == ('skip', None, None)
    response = PromptRecordRef.from_dict(called['call']['response_ref'])
    assert Path(response.journal_path) == run.journal.path
    assert response.request_id == called['call']['request_id']
    record = response.read(max_bytes=1048576)
    assert record['status_code'] == 200
    assert json.loads(record['body']['choices'][0]['message']['content']) == {'result': 'ok'}
    assert run.journal.budget_consumed('requests') == 1


def test_native_ledger_disk_ceiling_preserves_prior_receipts(tmp_path):
    import sqlite3
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    path = tmp_path/'calls.sqlite'
    journal = SQLitePromptJournal(path, max_requests=10, max_disk_bytes=4*1024**2)
    completed = []
    try:
        with pytest.raises(sqlite3.OperationalError, match='full'):
            for i in range(10):
                request = {'item': i}
                journal.reserve(request)
                journal.response(request, {'body': 'x' * 700000})
                completed.append(request)
        assert completed
        assert path.stat().st_size <= 2 * 1024**2
    finally:
        journal.close()
    reader = SQLitePromptJournal(path, read_only=True)
    try:
        assert all(reader.lookup(request)['body'] == 'x' * 700000 for request in completed)
    finally:
        reader.close()


def test_native_evidence_size_checked_before_payload_fetch(tmp_path):
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    journal = SQLitePromptJournal(tmp_path/'calls.sqlite')
    request = {'item': 'large'}
    journal.reserve(request)
    journal.response(request, {'body': 'x' * 20000})
    ref = journal.references(request)['response_ref']
    journal.close()
    before = (tmp_path/'calls.sqlite').read_bytes()
    with pytest.raises(ValueError, match='byte budget'):
        PromptRecordRef(**ref).read(max_bytes=1024)
    assert (tmp_path/'calls.sqlite').read_bytes() == before


@pytest.mark.parametrize(('change', 'rejection'), [
    ('model', 'Native prompt lowering/configuration copies differ'),
    ('runtime_options', 'Native prompt lowering/configuration copies differ'),
    ('when', r'Native plan differs at \w+\.module'),
    ('bare_async', r'Native plan differs at \w+\.native'),
    ('budget', r'Native plan differs at \w+\.native'),
], ids=['model', 'runtime_options', 'when', 'bare_async', 'budget'])
def test_native_model_mutation_rejected_before_requests(server, tmp_path, monkeypatch, change, rejection):
    from dataclasses import replace
    from demiflow.data.plan import CallableSpec
    from demiflow.data.runtime_contract import RuntimeContractError
    model_files(tmp_path)
    source, target = str(tmp_path/'input.lance'), str(tmp_path/'out.lance')
    lance.write_dataset(pa.table({'item': ['call']}), source)
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    tables = {'input': {'uri': source, 'version': 1, 'fields': {'item': field(scalar('str', nullable=True))}}}
    report = FlowChecker(tmp_path, catalog, tables, [target],
        resource_resolver=lambda tree, refs, bindings: resolve_flow_resources(tmp_path, tree, refs, bindings)).inspect('flows/model.py:run', dict(source=source, target=target))
    assert report['status'] == 'checked', report['diagnostics']
    load(monkeypatch, 'operators.predicate', tmp_path/'operators/predicate.py')
    flow = load(monkeypatch, 'flows.model', tmp_path/'flows/model.py')
    monkeypatch.chdir(tmp_path)
    original = data.Dataset.map_prompt_async
    def changed(self, *args, **kwargs):
        dataset = original(self, *args, **kwargs)
        actor = dataset._stages[-1]
        if change == 'model':
            actor._prompt = replace(actor._prompt, model=replace(actor._prompt.model, name='changed'))
        elif change == 'runtime_options':
            actor._runtime.options = {**actor._runtime.options, 'timeout_s': 1}
        elif change == 'when':
            actor.when = lambda row: True
        elif change == 'budget':
            from demiflow.operator_llm.tokens import CharacterBudget
            actor._runtime.token_budget = CharacterBudget(1000)
        else:
            async def ordinary(row): return row
            operation = replace(dataset._plan.operations[-1], callable=CallableSpec(ordinary))
            dataset = data.Dataset(dataset._source, replace(dataset._plan, operations=(operation,)), dataset._executor)
        return dataset
    monkeypatch.setattr(data.Dataset, 'map_prompt_async', changed)
    context = CheckedExecution(nodes=report['nodes'], edges=report['edges'], actions=report['actions'],
        operators=report['operators'], source_root=tmp_path, limits=DEFAULT_LIMITS,
        record=lambda *a: None, check_stop=lambda: None)
    with RunContext.start(tmp_path / 'run') as run, local_execution(
            workers=1, worker_mode='thread', partitions=1, memory_bytes=128*1024**2) as native:
        native._executor.resource_root = tmp_path
        with pytest.raises((RuntimeContractError, ValueError), match=rejection):
            with context.bind_context():
                flow.run(source=source, target=target)
    assert run.journal.budget_consumed('requests') == 0
    assert not server['requests'] and not (tmp_path/'evidence').exists() and not Path(target).exists()
