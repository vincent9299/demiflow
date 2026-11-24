"""A fixed Python Flow checks and executes native checkpoint/sink resources."""
import importlib.util

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.flow_contract import arrow_fields
from demiflow.forge.flow_check import FlowChecker
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
