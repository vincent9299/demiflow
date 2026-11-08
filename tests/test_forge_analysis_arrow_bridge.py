"""Primitive Arrow types/nulls and bounded IPC; no real OCI/SQL acceptance."""
import copy
from unittest.mock import patch

import pyarrow as pa
import pytest

from demiflow.workbench.analysis import arrow_bridge as a
from demiflow.forge.schema import ForgeError


def table():
    return pa.table({"bool": pa.array([True, None, False], type=pa.bool_()),
                     "integer": pa.array([-2**63, None, 2**63-1], type=pa.int64()),
                     "float": pa.array([1.5, None, float("nan")], type=pa.float64()),
                     "text": pa.array(["中文🙂", None, "'\\\n"], type=pa.string()),
                     "binary": pa.array([b"\x00\x01", None, b""], type=pa.binary())})


def test_all_admitted_primitive_types_nulls_and_metadata_roundtrip():
    original = table().replace_schema_metadata({b"note": b"exact schema metadata"})
    chunks = list(a.encode_batches(original.to_reader()))
    decoded = pa.Table.from_batches(a.decoded_batches(chunks))
    assert decoded.schema.equals(original.schema, check_metadata=True)
    assert decoded.drop(["float"]).equals(original.drop(["float"]))
    assert decoded["float"].null_count == 1 and decoded["float"][2].as_py() != decoded["float"][2].as_py()
    assert a.schema_digest(decoded.schema) == a.schema_digest(original.schema)


def test_stream_splits_wide_batches_and_preserves_the_full_row_set():
    original = pa.table({"text": ["x"*8192 for _ in range(300)], "i": pa.array(range(300), type=pa.int64())})
    chunks = list(a.encode_batches(original.to_reader()))
    assert len(chunks) > 1 and all(len(chunk) <= a.CHUNK_BYTES for chunk in chunks)
    assert pa.Table.from_batches(a.decoded_batches(chunks)).equals(original)


def test_single_wide_row_refuses_before_allocating_final_ipc_stream():
    original = pa.table({"wide": ["x"*a.CHUNK_BYTES]})
    with patch.object(pa, "BufferOutputStream", side_effect=AssertionError("must reject before allocation")):
        with pytest.raises(ForgeError) as error:
            list(a.encode_batches(original.to_reader()))
    assert error.value.code == "resource_limit"


def test_compressed_or_multi_batch_input_refuses_before_array_decoder():
    original = table()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, original.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
        writer.write_table(original)
    with patch.object(pa.ipc, "read_record_batch", side_effect=AssertionError("must reject compressed batch before array decoding")):
        with pytest.raises(ForgeError) as error:
            a.decode_chunk(sink.getvalue().to_pybytes())
    assert error.value.code == "operation_rejected"
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, original.schema) as writer:
        writer.write_table(original); writer.write_table(original)
    with pytest.raises(ForgeError):
        a.decode_chunk(sink.getvalue().to_pybytes())


def test_empty_results_keep_schema_and_unsupported_types_are_rejected():
    original = table().slice(0, 0)
    chunks = list(a.encode_batches(original.to_reader()))
    assert len(chunks) == 1 and pa.Table.from_batches(a.decoded_batches(chunks)).equals(original)
    for schema in (pa.schema([("date", pa.date32())]), pa.schema([("list", pa.list_(pa.int64()))]),
                   pa.schema([("duplicate", pa.int64()), ("duplicate", pa.int64())])):
        with pytest.raises(ForgeError):
            a.schema_description(schema)
    schema = pa.schema([pa.field("x", pa.int64(), metadata={b"ARROW:extension:name": b"bad"})])
    with pytest.raises(ForgeError):
        a.schema_description(schema)


def test_schema_change_global_row_and_frame_limits_fail_explicitly(monkeypatch):
    one = list(a.encode_batches(table().to_reader()))[0]
    other = list(a.encode_batches(pa.table({"other": [1]}).to_reader()))[0]
    with pytest.raises(ForgeError, match="schemas differ"):
        list(a.decoded_batches([one, other]))
    monkeypatch.setattr(a, "MAX_ROWS", 4)
    with pytest.raises(ForgeError) as error:
        list(a.decoded_batches([one, one]))
    assert error.value.code == "resource_limit"
