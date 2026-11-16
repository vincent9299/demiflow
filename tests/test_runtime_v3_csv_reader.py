"""Real Arrow streaming/parser semantics and original source recovery gates."""
import importlib
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pytest

from demiflow.data.sources import FileSource
from demiflow.execution.contracts.dataflow import ProgramDeclaration
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_seal import _stage_one_program, verify_dataflow_program
from demiflow.execution.recovery_contract import identity
from test_runtime_v3_s import (FakeStore, drain, first_split, make_bounds,
                               make_services, make_snapshot, open_reader)


def reader_for(path, *, options, bounds=None):
    bounds = bounds or make_bounds()
    source = FileSource('csv', (str(path),), options)
    store = FakeStore()
    snapshot = make_snapshot(source, store, bounds=bounds)
    split = first_split(snapshot, source, bounds=bounds)
    services = make_services(store, source, snapshot.source_id, bounds=bounds)
    return open_reader(snapshot, split, services), snapshot, split, services


def test_native_read_parse_convert_options_preserve_values_types_and_compressed_cursor(tmp_path):
    import gzip
    path = tmp_path / 'native.csv.gz'
    with gzip.open(path, 'wt', encoding='latin1', newline='') as stream:
        stream.write('discard\nid;name;flag;ignored\n0;skip;N;0\n'
                     '1;André;Y;x\n2;missing;N;x\n3;"line\nnext";Y;x\n')
    options = dict(
        read_options=pacsv.ReadOptions(block_size=64, skip_rows=1,
                                      skip_rows_after_names=1, encoding='latin1'),
        parse_options=pacsv.ParseOptions(delimiter=';', newlines_in_values=True),
        convert_options=pacsv.ConvertOptions(column_types={'id': pa.int32(), 'flag': pa.bool_()},
            null_values=['missing'], true_values=['Y'], false_values=['N'], strings_can_be_null=True,
            include_columns=['id', 'name', 'flag']),
        include_paths=True)
    reader, snapshot, split, services = reader_for(path, options=options)
    first = reader.next_record().record
    assert dict(first.inline) == {'id': 1, 'name': 'André', 'flag': True, 'path': str(path)}
    reader.acknowledge_output()
    cursor = reader.snapshot()
    schema = reader._schemas.read(first.schema_ref)
    assert schema.field('id').type == pa.int32()
    assert schema.field('flag').type == pa.bool_()
    assert schema.field('path').type == pa.string()
    remaining = drain(reader)
    reader.close()
    resumed = open_reader(snapshot, split, services, cursor=cursor)
    replayed = drain(resumed)
    resumed.close()
    assert [dict(r.inline) for r in replayed] == [
        {'id': 2, 'name': None, 'flag': False, 'path': str(path)},
        {'id': 3, 'name': 'line\nnext', 'flag': True, 'path': str(path)}]
    assert [(r.record_id, r.sequence) for r in replayed] == [(r.record_id, r.sequence) for r in remaining]


def test_no_native_options_retains_legacy_string_values(tmp_path):
    path = tmp_path / 'legacy.csv'
    path.write_text('id,value\n001,\n2,NULL\n')
    reader, *_ = reader_for(path, options={})
    try:
        assert [dict(r.inline) for r in drain(reader)] == [{'id': '001', 'value': ''}, {'id': '2', 'value': 'NULL'}]
    finally:
        reader.close()


def test_explicit_block_size_over_budget_rejected_before_native_open(tmp_path, monkeypatch):
    path = tmp_path / 'small.csv'
    path.write_text('id\n1\n')
    opened = []
    original = pacsv.open_csv
    def tracked(*args, **kwargs):
        opened.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(pacsv, 'open_csv', tracked)
    options = pacsv.ReadOptions(block_size=1024)
    reader, *_ = reader_for(path, options={'read_options': options},
                            bounds=make_bounds(max_batch_bytes=1024, max_row_bytes=256))
    try:
        with pytest.raises(DataflowError, match='CSV input blocks'):
            reader.next_record()
        assert opened == []
        assert options.block_size == 1024
    finally:
        reader.close()


def test_native_numeric_expansion_is_rejected_before_row_publication(tmp_path):
    path = tmp_path / 'numbers.csv'
    path.write_text('id\n' + '1\n' * 400)
    reader, *_ = reader_for(path,
        options={'read_options': pacsv.ReadOptions(block_size=256)},
        bounds=make_bounds(max_batch_bytes=1024, max_row_bytes=256))
    try:
        with pytest.raises(DataflowError, match='decoded buffers and input blocks'):
            reader.next_record()
        assert reader._next_sequence == 0
        assert reader._pending_record is None
    finally:
        reader.close()


def test_native_wide_row_rejected_before_publication(tmp_path):
    path = tmp_path / 'wide.csv'
    path.write_text('value\n' + 'x' * 400 + '\n')
    reader, *_ = reader_for(path,
        options={'read_options': pacsv.ReadOptions(block_size=512)},
        bounds=make_bounds(max_batch_bytes=4096, max_row_bytes=256))
    try:
        with pytest.raises(DataflowError, match='CSV row exceeds'):
            reader.next_record()
        assert reader._next_sequence == 0
    finally:
        reader.close()


@pytest.mark.parametrize('binding', ['global', 'partial'])
def test_csv_invalid_row_handler_runs_and_business_helper_drift_refuses_loaded_program(tmp_path, monkeypatch, binding):
    import functools
    import uuid
    helper_name = 'csv_helper_' + uuid.uuid4().hex
    handler_name = 'csv_handler_' + uuid.uuid4().hex
    helper = tmp_path / (helper_name + '.py')
    helper.write_text('def decision():\n    return "skip"\n')
    handler_path = tmp_path / (handler_name + '.py')
    handler_path.write_text(
        f'import {helper_name}\ndef handle(row):\n    return {helper_name}.decision()\n'
        if binding == 'global' else 'def handle(row, *, helper):\n    return helper.decision()\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    handler = importlib.import_module(handler_name).handle
    if binding == 'partial':
        handler = functools.partial(handler, helper=importlib.import_module(helper_name))
    path = tmp_path / 'invalid.csv'
    path.write_text('id,name\n1,ok\n2,bad,wide\n3,tail\n')
    options = {'parse_options': pacsv.ParseOptions(invalid_row_handler=handler),
               'read_options': pacsv.ReadOptions(block_size=64)}
    reader, _, _, services = reader_for(path, options=options)
    try:
        assert [dict(r.inline) for r in drain(reader)] == [{'id': 1, 'name': 'ok'}, {'id': 3, 'name': 'tail'}]
    finally:
        reader.close()
    source = FileSource('csv', (str(path),), options)
    declaration = ProgramDeclaration(identity('csv-source'), None, source, None, {})
    ref = _stage_one_program(declaration, store=services.store, bounds=services.bounds)
    manifest = json.loads(b''.join(services.store.read(ref, max_bytes=services.bounds.max_control_bytes)))
    assert {helper_name, handler_name} <= set(manifest['business_sources'])
    expected_pin = manifest['code_identity']
    verified = verify_dataflow_program(ref, expected_pin=expected_pin,
                                      store=services.store, bounds=services.bounds)
    restored = verified.value.options['parse_options'].invalid_row_handler
    assert restored(None) == 'skip'
    from types import SimpleNamespace
    from demiflow.data.plan import LogicalPlan
    from demiflow.execution.chain import snapshot_code_sources
    from demiflow.execution.run_context import RunContext
    with RunContext.start(tmp_path / 'archive-run') as run:
        archives = snapshot_code_sources(SimpleNamespace(context=run), LogicalPlan(()), source=source)
        assert {str(helper), str(handler_path)} <= {item['source'] for item in archives}
        for item in archives:
            assert (run.directory / item['path']).read_bytes() == Path(item['source']).read_bytes()
    helper.write_text('def decision():\n    return "error"\n')
    try:
        with pytest.raises(DataflowError, match='Loaded program code differs'):
            verify_dataflow_program(ref, expected_pin=expected_pin,
                                    store=services.store, bounds=services.bounds)
    finally:
        sys.modules.pop(handler_name, None)
        sys.modules.pop(helper_name, None)
