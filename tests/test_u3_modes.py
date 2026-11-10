"""U3 MODE conformance: three exchange modes over one Local carriage (S2).

The protocol (K5 envelope/barrier/eoi/seal/credit) is identical across modes;
only payload carriage differs. memory_incremental carries rows inline;
durable_incremental and finite_sealed put a DataRef on the wire and the consumer
reads the segment file directly — large payloads never transit the socket/driver.
Segment reads verify domain/path/size/digest before deserialization.
"""
import uuid

import pytest

from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.contracts.exchange import build_data_ref
from demiflow.execution.exchange.carriage_local import (
    LocalSegmentStore, channel_pair, expand_records, inline_records, ref_records,
    send_frame,
)
from demiflow.execution.exchange.channel import ChannelProducer
from demiflow.execution.recovery_contract import RecoveryContractError, document
from demiflow.planning.dag import select_exchange

_DOMAIN_HOST = "host-a"


def _code(exc):
    return exc.value.code


def _producer():
    return {"logical_node_id": "node-1", "occurrence_id": "occ-1"}


def _credit(channel_id, rows=1024, size_bytes=64 * 1024**2):
    return {"channel_id": channel_id, "reader_id": "reader-1",
            "credits": {"rows": rows, "bytes": size_bytes}, "route_epoch": 0,
            "operation_id": uuid.uuid4().hex}


def _rows(count=6, blob=32):
    return [{"x": i, "blob": "z" * blob} for i in range(count)]


def _envelope(channel_id, records):
    p = ChannelProducer(channel_id)
    p.grant_credit(_credit(channel_id))
    return p.data_envelope(records, producer=_producer())


def test_mode_selection_is_traits_driven():
    assert select_exchange({"input_mode": "finite_scope"})[0] == "finite_sealed"
    assert select_exchange({"input_mode": "incremental"})[0] == "memory_incremental"
    assert select_exchange({"input_mode": "incremental"}, durability_declared="durable")[0] == "durable_incremental"


def test_mode_memory_inline_roundtrip_over_socket(tmp_path):
    store = LocalSegmentStore(tmp_path / "seg", host_id=_DOMAIN_HOST)
    rows = _rows()
    producer, consumer = channel_pair()
    try:
        env = _envelope("ch-m", inline_records(rows, source_id="src", occurrence_id="occ-1"))
        producer.send({"kind": "data", "sequence": env["sequence"], "envelope": env})
        received = consumer.recv()
        assert received["envelope"]["records"][0]["payload"] == rows[0]  # inline on the wire
        assert expand_records(received["envelope"]["records"], store) == rows
    finally:
        producer.close()
        consumer.close()


def test_mode_durable_ref_roundtrip_direct_read(tmp_path):
    store = LocalSegmentStore(tmp_path / "seg", host_id=_DOMAIN_HOST)
    rows = _rows()
    descriptor, refs = store.write(rows, scope_ref="scope-1")
    assert descriptor["rows"] == len(rows) and all(r["durability"] == "durable" for r in refs)
    producer, consumer = channel_pair()
    try:
        env = _envelope("ch-d", ref_records(refs, occurrence_id="occ-1"))
        producer.send({"kind": "data", "sequence": env["sequence"], "envelope": env})
        received = consumer.recv()
        # The wire carries only a DataRef; the consumer reads the segment file itself.
        assert received["envelope"]["records"][0]["payload"]["schema_version"] == "demiflow.data_ref.v1"
        assert expand_records(received["envelope"]["records"], store) == rows
    finally:
        producer.close()
        consumer.close()


def test_mode_large_payload_never_transits_the_socket(tmp_path):
    store = LocalSegmentStore(tmp_path / "seg", host_id=_DOMAIN_HOST, block_bytes=256 * 1024)
    rows = [{"x": i, "blob": "z" * 200000} for i in range(8)]  # ~1.6 MiB of payload
    payload_bytes = sum(len(document(row)) for row in rows)
    _, refs = store.write(rows, scope_ref="scope-1")
    env = _envelope("ch-d", ref_records(refs, occurrence_id="occ-1"))
    frame_bytes = len(document({"kind": "data", "sequence": env["sequence"], "envelope": env}))
    assert payload_bytes > 1024**2 and frame_bytes < 16 * 1024  # ref-only frame, payload on disk
    producer, consumer = channel_pair()
    try:
        producer.send({"kind": "data", "sequence": env["sequence"], "envelope": env})
        received = consumer.recv()
        assert expand_records(received["envelope"]["records"], store) == rows
    finally:
        producer.close()
        consumer.close()


def test_mode_sealed_seal_then_read_only_success(tmp_path):
    store = LocalSegmentStore(tmp_path / "seg", host_id=_DOMAIN_HOST)
    rows = _rows()
    _, refs = store.write(rows, scope_ref="scope-1")
    p = ChannelProducer("ch-s")
    p.grant_credit(_credit("ch-s"))
    p.data_envelope(ref_records(refs, occurrence_id="occ-1"), producer=_producer())
    p.barrier("ck-final", epoch=1, topology_revision=0)
    p.end_of_input()
    scope = {"scope_id": "scope-1", "membership": ["p0"], "generation": 1}
    seal = p.seal(seal_scope=scope, manifest_ref={"sha256": "c" * 64},
                  producer_statuses=["success"])
    assert seal["status"] == "success"
    assert store.read(refs) == rows  # sealed segments read directly after seal
    with pytest.raises(ProtocolError):
        p.seal(seal_scope=scope, manifest_ref={"sha256": "c" * 64},
               producer_statuses=["failed"])


def test_mode_read_domain_mismatch_rejected(tmp_path):
    writer = LocalSegmentStore(tmp_path / "seg", host_id="host-a")
    _, refs = writer.write(_rows(), scope_ref="scope-1")
    other = LocalSegmentStore(tmp_path / "seg", host_id="host-b")  # same path, different domain
    with pytest.raises(ProtocolError) as exc:
        other.read(refs)
    assert _code(exc) == "REF_READ_DOMAIN_MISMATCH"


def test_mode_segment_path_escape_rejected(tmp_path):
    store = LocalSegmentStore(tmp_path / "seg", host_id=_DOMAIN_HOST)
    for location in ("../escape.pkl", "/etc/passwd"):
        ref = build_data_ref(kind="file", read_domain=store.domain, location=location,
                             payload_bytes=1, sha256="a" * 64, durability="durable",
                             scope_ref="scope-1")
        with pytest.raises(RecoveryContractError):
            store.read([ref])


def test_mode_segment_digest_mismatch_rejected(tmp_path):
    store = LocalSegmentStore(tmp_path / "seg", host_id=_DOMAIN_HOST)
    _, refs = store.write(_rows(), scope_ref="scope-1")
    path = (tmp_path / "seg") / refs[0]["location"]
    raw = bytearray(path.read_bytes())
    raw[0] ^= 0xFF  # same length, different content
    path.write_bytes(raw)
    with pytest.raises(ProtocolError) as exc:
        store.read(refs)
    assert _code(exc) == "ARTIFACT_MISMATCH"


def test_mode_segment_symlink_rejected(tmp_path):
    store = LocalSegmentStore(tmp_path / "seg", host_id=_DOMAIN_HOST)
    _, refs = store.write(_rows(), scope_ref="scope-1")
    path = (tmp_path / "seg") / refs[0]["location"]
    target = tmp_path / "outside.pkl"
    target.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(target)
    with pytest.raises(RecoveryContractError):
        store.read(refs)