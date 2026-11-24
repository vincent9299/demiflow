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



def test_current_cast_roundtrips_without_changing_identity_or_budget(tmp_path):
    import base64
    import io
    from demiflow.execution.seal.program_identity import describe
    from demiflow.execution.seal.program_pickle import ProgramPickler, load_payload
    from demiflow.resources.capacity import allocation_envelope
    payload = io.BytesIO()
    ProgramPickler(payload).dump(_cast_sink_batch)
    loaded = load_payload([payload.getvalue()])
    assert loaded is _cast_sink_batch
    assert describe(loaded) == describe(_cast_sink_batch)
    table = pa.table({'id': [1, 2]})
    schema = pa.schema([('id', pa.int32())])
    encoded = base64.b64encode(schema.serialize().to_pybytes()).decode()
    with allocation_envelope({'allocation_id': 'sealed', 'spec_hash': '0' * 64,
            'resource_directory': str(tmp_path), 'cpu_millis': 1000, 'memory_bytes': 32 * MiB}):
        assert loaded(table, schema_bytes=encoded, max_bytes=32 * MiB).equals(table.cast(schema))
        with pytest.raises(DataflowError, match='input and cast output'):
            loaded(table, schema_bytes=encoded, max_bytes=table.nbytes)
