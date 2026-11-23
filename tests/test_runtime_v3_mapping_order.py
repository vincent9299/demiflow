"""Business row field order survives transport without rewriting old carriers."""
from demiflow.execution.contracts import record_codec
import json
import math
import sys
import tempfile

import pytest

from demiflow.execution.contracts.dataflow import Record, thaw_record_value
from demiflow.execution.exchange import dataflow_bridge as codec


def test_both_mapping_orders_roundtrip_deterministically_with_original_types():
    rows = [dict(z=(b'\x00\xff', [-0.0, 0.0]), a={'second': None, 'first': True}),
            dict(a={'first': True, 'second': None}, z=(b'\x00\xff', [-0.0, 0.0]))]
    encoded = []
    for row in rows:
        raw = record_codec._encode_payload(row, max_bytes=4096)
        assert record_codec._encode_payload(row, max_bytes=4096) == raw
        record = Record('a' * 64, {'offset': 0}, None, row, len(raw), 0)
        wire = record_codec._encode_record_document(record, max_bytes=4096)
        restored = record_codec._decode_record_document(wire, record_id=record.record_id,
                                                sequence=0, max_bytes=4096)
        actual = thaw_record_value(restored.inline)
        assert list(actual) == list(row) and list(actual['a']) == list(row['a'])
        assert type(actual['z']) is tuple and type(actual['z'][0]) is bytes
        assert type(actual['z'][1]) is list
        assert math.copysign(1, actual['z'][1][0]) == -1
        assert math.copysign(1, actual['z'][1][1]) == 1
        assert record_codec._encode_record_document(restored, max_bytes=4096) == wire
        encoded.append(raw)
    assert encoded[0] != encoded[1]


def test_original_sorted_map_bytes_remain_readable_and_unchanged():
    # Literal original v3 bytes, independent of the current encoder.
    raw = b'{"t":"map","v":[["a",{"t":"int","v":1}],["z",{"t":"bytes","v":"AA=="}]]}'
    row = record_codec._decode_payload(raw, max_bytes=4096)
    assert row == {'a': 1, 'z': b'\x00'} and list(row) == ['a', 'z']
    assert record_codec._encode_payload(row, max_bytes=4096) == raw
    document = {'v': 3, 'sp': {'offset': 0}, 'eb': len(raw), 'p': json.loads(raw), 'r': None}
    wire = json.dumps(document, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()
    record = record_codec._decode_record_document(wire, record_id='a' * 64, sequence=0, max_bytes=4096)
    assert record_codec._encode_record_document(record, max_bytes=4096) == wire


@pytest.mark.parametrize('provider', ['local', 'ray_core'])
def test_untyped_and_declared_lance_column_order_through_real_provider(tmp_path, provider):
    import lance
    import pyarrow as pa
    from demiflow.data.api import DataAPI
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    ray = None
    with tempfile.TemporaryDirectory(prefix='demiflow-order-ray-', dir='/tmp') as ray_temp:
        if provider == 'ray_core':
            ray = pytest.importorskip('ray')
            assert not ray.is_initialized(), 'test only owns a fresh local Ray runtime'
            ray.init(address='local', num_cpus=2, num_gpus=0, include_dashboard=False,
                     object_store_memory=80 * 1024**2, _temp_dir=ray_temp)
            from demiflow.execution.executors.ray import RayDatasetExecutor
            executor = RayDatasetExecutor(workers=2)
        else:
            from demiflow.execution.executors.local import LocalDatasetExecutor
            executor = LocalDatasetExecutor(workers=2, resource_root=tmp_path)
        try:
            api = DataAPI(executor)
            rows = [{'id': 1, 'value': 'old', 'other': 'keep'},
                    {'id': 2, 'value': 'new', 'other': 'also'}]
            typed_schema = pa.schema([pa.field('z', pa.int16(), nullable=False),
                pa.field('a', pa.large_binary(), metadata={b'field': b'original'})],
                metadata={b'source': b'original'})
            typed = pa.Table.from_pylist([{'z': 1, 'a': b''}, {'z': 2, 'a': None}], schema=typed_schema)
            with RunContext.start(tmp_path / 'run') as run:
                api.from_items(rows).write_lance(str(tmp_path / 'items.lance'), mode='create')
                api.from_arrow(typed).write_lance(str(tmp_path / 'typed.lance'), mode='create')
                for action_id in ('action-1', 'action-2'):
                    sealed = load_sealed_plan(run.directory, action_id, version='v3')
                    assert sealed['physical'].policy.provider == provider
                    assert run.journal.execution_commits(action_id)[-1]['status'] == 'acked'
            untyped_result = lance.dataset(str(tmp_path / 'items.lance')).to_table()
            assert untyped_result.schema.names == ['id', 'value', 'other']
            assert untyped_result.to_pylist() == rows
            typed_result = lance.dataset(str(tmp_path / 'typed.lance')).to_table()
            assert typed_result.schema.equals(typed_schema, check_metadata=True)
            assert typed_result.equals(typed)
            assert 'ray.data' not in sys.modules
        finally:
            executor.close()
            if ray is not None:
                ray.shutdown()
