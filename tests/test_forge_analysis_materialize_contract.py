"""Input commitment, framing and dispatcher refusal; no real Lance/OCI run."""
import hashlib
import struct

import pyarrow as pa
import pytest

from demiflow.workbench.analysis import materialize
from demiflow.workbench.analysis import native_worker
from demiflow.workbench.analysis import sandbox
from demiflow.workbench.analysis.arrow_bridge import encode_batches
from demiflow.workbench.analysis.arrow_bridge import decoded_batches
from demiflow.workbench.analysis.arrow_bridge import schema_digest
from demiflow.forge.schema import ForgeError


def fixture(tmp_path):
    table = pa.table({"value": pa.array([1, None, 3], type=pa.int64())})
    chunks = list(encode_batches(table.to_reader()))
    raw = b"".join(struct.pack("!I", len(chunk)) + chunk for chunk in chunks)
    path = tmp_path/"input.bin"; path.write_bytes(raw)
    config = dict(operation="materialize", input_id="i"+"a"*32, stream_sha256=hashlib.sha256(raw).hexdigest(),
        schema_sha256=schema_digest(table.schema), encoded_bytes=len(raw), chunks=len(chunks), publish_id="publish-one")
    return table, path, config


def test_committed_arrow_stream_roundtrip_without_reinterpreting_nulls(tmp_path):
    original, path, config = fixture(tmp_path)
    decoded = pa.Table.from_batches(decoded_batches(materialize.input_chunks(path, config)))
    assert decoded.equals(original)


def test_changed_bytes_fail_before_any_arrow_chunk_is_yielded(tmp_path):
    original, path, config = fixture(tmp_path)
    raw = bytearray(path.read_bytes()); raw[-1] ^= 1; path.write_bytes(raw)
    reader = materialize.input_chunks(path, config)
    with pytest.raises(ForgeError, match="committed stream"):
        next(reader)


def test_bad_frame_length_or_unexpected_count_cannot_trigger_large_read(tmp_path):
    original, path, config = fixture(tmp_path)
    bad = struct.pack("!I", 2**31)
    path.write_bytes(bad); config.update(encoded_bytes=len(bad), stream_sha256=hashlib.sha256(bad).hexdigest())
    with pytest.raises(ForgeError) as error:
        list(materialize.input_chunks(path, config))
    assert error.value.code == "resource_limit"
    config["input_id"] = "../../outside"
    with pytest.raises(ForgeError):
        materialize.materialize_config(config)


def test_native_dispatcher_has_no_plain_host_materialization_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(sandbox, "JOB_PATH", tmp_path/"missing.json")
    monkeypatch.setattr(materialize, "run_materialize", lambda _:pytest.fail("writer must not start"))
    with pytest.raises(ForgeError, match="no host execution fallback"):
        native_worker.main()
