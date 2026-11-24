"""Compatibility declarations stay on one real, budgeted dataflow action."""

import lance
import pyarrow as pa
import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.outputs.batch_conversion import _cast_sink_batch

MiB = 2**20


def source(tmp_path, values=range(8)):
    uri = str(tmp_path/'source.lance')
    table = pa.table({'id': pa.array(list(values), type=pa.int64()), 'extra': ['x']*len(values)})
    lance.write_dataset(table, uri)
    return uri


def test_cast_admits_input_and_new_output_together_before_allocation(monkeypatch):
    from demiflow.resources.capacity import allocation_envelope
    from demiflow.execution.contracts.record_codec import cast_allocation as _cast_allocation
    from demiflow.execution.contracts.record_schema import encode
    from demiflow.planning.dataflow_policy import PREFLIGHT_BOUNDS as _PREFLIGHT_BOUNDS
    table = pa.table({'id': pa.array(range(64), type=pa.int64())})
    schema = pa.schema([('id', pa.float64())])
    encoded = encode(schema, bounds=_PREFLIGHT_BOUNDS)
    total = table.get_total_buffer_size() + _cast_allocation(table, schema)
    assert table.get_total_buffer_size() < total-1
    original_pool = pa.default_memory_pool()
    observed_pool = pa.proxy_memory_pool(original_pool)
    pa.set_memory_pool(observed_pool)
    try:
        with allocation_envelope({'allocation_id': 'test-native', 'spec_hash': '0'*64,
                'resource_directory': '/tmp/demiflow-native-unit-envelope',
                'cpu_millis': 1000, 'memory_bytes': total-1}):
            with pytest.raises(DataflowError, match='input and cast output'):
                _cast_sink_batch(table, schema_bytes=encoded, max_bytes=total-1)
        assert observed_pool.max_memory() == 0
    finally:
        pa.set_memory_pool(original_pool)
    with allocation_envelope({'allocation_id': 'test-native', 'spec_hash': '0'*64,
            'resource_directory': '/tmp/demiflow-native-unit-envelope',
            'cpu_millis': 1000, 'memory_bytes': total}):
        actual = _cast_sink_batch(table, schema_bytes=encoded, max_bytes=total)
    assert actual.equals(table.cast(schema))


def test_retired_datafusion_worker_request_keeps_its_real_row_transform(tmp_path):
    """The SQL driver's last release sent Column and finish_typed_batch to the worker."""
    import os
    import pickle
    import subprocess
    import sys

    from runtime_v3_history_scenarios import _extract_original

    old, _ = _extract_original(tmp_path, '70e357c5c11ee9a594d3b5998ae397033372d69e')
    producer = """
from pathlib import Path
import cloudpickle, lance, pyarrow as pa, sys
from demiflow import data
from demiflow.execution.dataset_native import execute
from demiflow.execution.run_context import RunContext

root = Path(sys.argv[1])
def plus(row):
    return {'id': row['id'] + 3}
encode = cloudpickle.dumps
def preserve_worker_request(value, *args, **kwargs):
    payload = encode(value, *args, **kwargs)
    if isinstance(value, dict) and value.get('batch_transform') is not None and 'dataset_spec' in value:
        (root / 'worker.pickle').write_bytes(payload)
    return payload
cloudpickle.dumps = preserve_worker_request
uri = str(root / 'source.lance')
lance.write_dataset(pa.table({'id': [1, 2, 2]}), uri)
with RunContext.start(root / 'run'), data.local_execution(
        workers=1, partitions=1, worker_mode='process', temp_directory=str(root)) as session:
    ds = data.read_lance(uri, version=1).map(plus)
    rows = execute(session._executor, ds._source, ds._plan, arrow=True, sink_schema=pa.schema([('id', pa.int64())]))
    assert pa.Table.from_batches(list(rows)).to_pylist() == [{'id': 4}, {'id': 5}, {'id': 5}]
"""
    subprocess.run([sys.executable, '-B', '-c', producer, str(tmp_path)], cwd=old, timeout=90,
                   env={**os.environ, 'PYTHONPATH': str(old),
                        'DEMIFLOW_RESOURCE_ROOT': str(tmp_path / 'resources'),
                        'DEMIFLOW_RESOURCE_HOME': str(tmp_path / 'resource-home')}, check=True)

    names = set()
    from demiflow.execution.seal.program_pickle import ProgramUnpickler

    class WorkerRequestReader(ProgramUnpickler):
        def find_class(self, module, name):
            if module.startswith('demiflow.'):
                names.add((module, name))
            return super().find_class(module, name)

    with (tmp_path / 'worker.pickle').open('rb') as handle:
        transform = WorkerRequestReader(handle).load()['batch_transform']
    assert names == {('demiflow.execution.dataset_native', 'Column'),
                     ('demiflow.execution.dataset_native', 'finish_typed_batch')}
    column, = transform.keywords['columns']
    actual = transform(pa.record_batch({column.value: [1, 2, 2], column.present: [True, True, True]}))
    assert actual.to_pylist() == [{'id': 4}, {'id': 5}, {'id': 5}]


def test_published_cast_function_keeps_its_actual_identity_and_allocation(tmp_path):
    import base64
    import json
    import os
    import subprocess
    import sys
    from types import FunctionType

    import cloudpickle

    from demiflow.execution.seal.program_identity import describe
    from demiflow.resources.capacity import allocation_envelope
    from runtime_v3_history_scenarios import _extract_original

    original, _ = _extract_original(tmp_path, '3d7b4e8c')
    payload = tmp_path / 'original-cast.pickle'
    producer = """
import cloudpickle, json, sys
from pathlib import Path
from demiflow.execution import dataset_native
from demiflow.execution.recovery_identity import describe
assert Path(dataset_native.__file__).resolve().is_relative_to(Path.cwd())
cloudpickle.register_pickle_by_value(dataset_native)
Path(sys.argv[1]).write_bytes(cloudpickle.dumps(dataset_native._cast_sink_batch))
print(json.dumps(describe(dataset_native._cast_sink_batch)))
"""
    archived_identity = subprocess.check_output(
        [sys.executable, '-B', '-c', producer, str(payload)], cwd=original,
        env={**os.environ, 'PYTHONPATH': str(original)}, timeout=30, text=True,
    )
    assert describe(_cast_sink_batch) == json.loads(archived_identity)
    substitute = FunctionType(_cast_sink_batch.__code__, dict(_cast_sink_batch.__globals__))
    assert describe(substitute) != json.loads(archived_identity)
    from demiflow.execution.seal.program_pickle import load_payload
    old_cast = load_payload([payload.read_bytes()])
    assert old_cast is _cast_sink_batch
    table = pa.table({'id': [1, 2]})
    schema = pa.schema([('id', pa.int32())])
    encoded = base64.b64encode(schema.serialize().to_pybytes()).decode()
    with allocation_envelope({'allocation_id': 'sealed', 'spec_hash': '0' * 64,
            'resource_directory': str(tmp_path), 'cpu_millis': 1000, 'memory_bytes': 32 * MiB}):
        assert old_cast(table, schema_bytes=encoded, max_bytes=32 * MiB).equals(table.cast(schema))
        with pytest.raises(DataflowError, match='input and cast output'):
            old_cast(table, schema_bytes=encoded, max_bytes=table.nbytes)
