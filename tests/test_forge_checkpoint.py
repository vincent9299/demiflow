"""A fixed Python Flow checks and executes native checkpoint/sink resources."""
import importlib.util
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.flow_contract import arrow_fields
from demiflow.execution.checked import CheckedExecution
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.forge.flow_check import FlowChecker
from demiflow.workbench.runs.manager import DEFAULT_LIMITS
from test_forge_native_values import catalog
from test_forge_native_semantics import native


BODY = '''from demiflow import data
def run(*, source, stage, target):
    schema = data.schema({'fields': [{'name': 'x', 'type': 'int64'}]})
    checkpoint = data.open_checkpoint('checkpoints/stages.json', identity={'source': source},
        outputs={'stage': {'uri': stage, 'schema': schema}})
    previous = data.read_checkpoint(checkpoint, 'stage')
    data.require(previous.count() >= 0, 'invalid checkpoint count')
    rows = data.read_lance(source, version=1)
    rows.save_lance(stage, schema=schema, key='x', stage='stage', mode='append',
        max_batch=1, queue_depth=1, max_row_bytes=4096, max_index_bytes=65536).run_stream(checkpoint=checkpoint)
    result = data.read_checkpoint(checkpoint, 'stage')
    data.require(result.count() == 2, 'incomplete stage')
    return result.write_lance(target, mode='create', schema=schema, return_receipt=True)
'''


def inspect(tmp_path, catalog, body=BODY):
    source = str(tmp_path / 'input.lance')
    table = pa.Table.from_pylist([{'x': 1}, {'x': 2}])
    lance.write_dataset(table, source)
    (tmp_path / 'flows').mkdir()
    (tmp_path / 'operators').mkdir()
    path = tmp_path / 'flows' / 'checkpoint.py'
    path.write_text(body)
    arguments = {'source': source, 'stage': str(tmp_path / 'stage.lance'), 'target': str(tmp_path / 'result.lance')}
    report = FlowChecker(tmp_path, catalog, {'source': {'uri': source, 'version': 1, 'fields': arrow_fields(table.schema)}},
        {arguments['stage']: {'mode': 'checkpoint', 'expected_version': None},
         arguments['target']: {'mode': 'create', 'expected_version': None}}).inspect('flows/checkpoint.py:run', arguments)
    return path, report, arguments


def test_native_checkpoint_flow_executes_with_fixed_result(tmp_path, catalog):
    path, report, arguments = inspect(tmp_path, catalog)
    assert report['status'] == 'checked', report['diagnostics']
    spec = importlib.util.spec_from_file_location('flows.checkpoint', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    events = []
    checked = CheckedExecution(**{k: report[k] for k in ('nodes', 'edges', 'actions', 'operators')},
        source_root=tmp_path, resource_root=tmp_path / 'run', limits=DEFAULT_LIMITS,
        record=lambda *args: events.append(args), check_stop=lambda: None)
    with native(tmp_path), checked.bind_context():
        receipt = module.run(**arguments)
    assert receipt.written_rows == 2
    assert lance.dataset(arguments['stage']).count_rows() == 2
    commits = [value for _, kind, value in events if kind == 'checkpoint_commits']
    assert len(commits) == 1 and commits[0]['outputs']['stage']['version'] == 3
    assert (tmp_path / 'run/checkpoints/stages.json').is_file()


@pytest.mark.parametrize('before, after', [
    ("'checkpoints/stages.json'", "'../stages.json'"),
    ("data.read_checkpoint(checkpoint, 'stage')", "data.read_checkpoint({'outputs': {}}, 'stage')"),
    ("stage='stage'", "stage='wrong'"),
    ("run_stream(checkpoint=checkpoint)", "run_stream()"),
    ("'schema': schema", "'schema': {'schema_declaration': {'fields': []}}"),
])
def test_checkpoint_contract_rejects_forged_or_unowned_resources(tmp_path, catalog, before, after):
    _, report, _ = inspect(tmp_path, catalog, BODY.replace(before, after))
    assert report['status'] == 'invalid', report['diagnostics']


def test_actual_sink_cannot_add_an_unchecked_predicate(tmp_path, catalog, monkeypatch):
    from demiflow.execution.stream_lance import StreamLanceSink
    path, report, arguments = inspect(tmp_path, catalog)
    assert report['status'] == 'checked', report['diagnostics']
    spec = importlib.util.spec_from_file_location('flows.checkpoint', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    original = StreamLanceSink.__init__
    def replaced(self, *args, **kwargs):
        original(self, *args, **kwargs)
        self.when = lambda row: False
    monkeypatch.setattr(StreamLanceSink, '__init__', replaced)
    checked = CheckedExecution(**{k: report[k] for k in ('nodes', 'edges', 'actions', 'operators')},
        source_root=tmp_path, resource_root=tmp_path / 'run', limits=DEFAULT_LIMITS,
        record=lambda *args: None, check_stop=lambda: None)
    with native(tmp_path), pytest.raises(RuntimeContractError, match='native'):
        with checked.bind_context():
            module.run(**arguments)
    assert not Path(arguments['stage']).exists()


def test_completed_write_cannot_be_reused_for_a_different_stage_version(tmp_path, catalog):
    path, report, arguments = inspect(tmp_path, catalog, BODY.replace('result.count() == 2', 'result.count() >= 0'))
    assert report['status'] == 'checked', report['diagnostics']
    spec = importlib.util.spec_from_file_location('flows.checkpoint', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    setup = {**{k: report[k] for k in ('nodes', 'edges', 'actions', 'operators')},
        'source_root': tmp_path, 'resource_root': tmp_path / 'run', 'limits': DEFAULT_LIMITS, 'check_stop': lambda: None}
    events = []
    with native(tmp_path):
        with CheckedExecution(**setup, record=lambda *args: events.append(args)).bind_context():
            module.run(**arguments)
        schema = data.schema({'fields': [{'name': 'x', 'type': 'int64'}]})
        cp = data.open_checkpoint(str(tmp_path / 'run/checkpoints/stages.json'),
            identity={'source': arguments['source']}, outputs={'stage': {'uri': arguments['stage'], 'schema': schema}})
        data.from_items([{'x': 3}]).save_lance(arguments['stage'], schema=schema, key='x',
            stage='stage', mode='append').run_stream(checkpoint=cp)
        identity = report['actions'][-1]
        previous = {kind: value for node, kind, value in events if node == identity and kind in {'receipt', 'intent'}}
        previous['attempt'] = 1
        with pytest.raises(RuntimeContractError, match='previous_dependencies'):
            with CheckedExecution(**setup, prior_writes={identity: previous}, record=lambda *args: None).bind_context():
                module.run(**arguments)
    assert lance.dataset(arguments['target']).count_rows() == 2
