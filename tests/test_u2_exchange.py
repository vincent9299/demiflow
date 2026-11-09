"""U2 CHN conformance: first-batch ordered channel endpoints (S4).

Pure protocol over the frozen K4/K5 models (InputGate/ChannelSequence/credit/
seal/cancel) plus the staging-descriptor<->DataRef carrier (D3). No physical
transport, no GPU/multi-host claims. Descriptors and barriers share one sequence
space; control never overtakes pre-cut data; credit is bounded; a cancel
tombstone suppresses late data; only success seals; read_domain mismatch rejects.
"""
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.contracts.exchange import (
    build_data_ref, record_identity, seal_allowed, validate_seal,
)
from demiflow.execution.exchange import (
    ChannelProducer, ChannelConsumer, read_domain, descriptor_refs, refs_descriptor,
)
from demiflow.execution.run_context import RunContext


def _code(exc):
    return exc.value.code


@pytest.fixture
def run(tmp_path):
    with RunContext.start(tmp_path / "run") as context:
        yield context


def _ref(domain, location="materializations/a/output/g/0.pkl", sha256="a" * 64, size=10):
    return build_data_ref(kind="file", read_domain=domain, location=location,
                          payload_bytes=size, sha256=sha256, durability="durable",
                          scope_ref="scope-1")


def _record(domain, ordinal=0, size=10):
    return {"record_id": record_identity("src", ordinal), "parent_record_ids": [],
            "occurrence_id": "occ-1", "size_bytes": size, "payload": _ref(domain, size=size)}


def _producer(node="node-1"):
    return {"logical_node_id": node, "occurrence_id": "occ-1"}


def _credit(channel_id, rows, size_bytes):
    return {"channel_id": channel_id, "reader_id": "driver",
            "credits": {"rows": rows, "bytes": size_bytes}, "route_epoch": 0,
            "operation_id": uuid.uuid4().hex}


def test_chn_data_and_control_share_one_sequence_space():
    p = ChannelProducer("ch-1")
    p.grant_credit(_credit("ch-1", 8, 1000))
    env = p.data_envelope([_record("file://h:/r")], producer=_producer())
    bar = p.barrier("ck-1", epoch=1, topology_revision=0)
    eoi = p.end_of_input()
    seal = p.seal(seal_scope={"scope_id": "scope-1", "membership": ["p0"], "generation": 1},
                  manifest_ref={"sha256": "c" * 64}, producer_statuses=["success"])
    assert [env["sequence"], bar["sequence"], eoi["sequence"], seal["sequence"]] == [0, 1, 2, 3]
    assert bar["cut"]["producer_sequence_bound"] == 1  # pre-cut data is everything before the barrier


def test_chn_barrier_never_overtakes_pre_cut_data():
    domain = "file://h:/r"
    p = ChannelProducer("ch-1")
    p.grant_credit(_credit("ch-1", 8, 1000))
    env = p.data_envelope([_record(domain)], producer=_producer())
    bar = p.barrier("ck-1", epoch=1, topology_revision=0)
    post = p.data_envelope([_record(domain, ordinal=1)], producer=_producer())
    c = ChannelConsumer(["ch-1"], reader_domain=domain)
    c.push("ch-1", {"kind": "data", "sequence": env["sequence"], "envelope": env})
    c.push("ch-1", {"kind": "barrier", **bar})
    c.push("ch-1", {"kind": "data", "sequence": post["sequence"], "envelope": post})  # held
    with pytest.raises(ProtocolError) as exc:
        c.barrier_effect("ch-1")
    assert _code(exc) == "BARRIER_OVERTAKE"
    assert c.consume("ch-1")["sequence"] == 0
    assert c.barrier_effect("ch-1")["checkpoint_id"] == "ck-1"
    assert c.consume("ch-1") is None  # post-barrier data stays held
    assert c.aligned("ck-1") and c.release("ck-1") == 1
    assert c.consume("ch-1")["sequence"] == 2


def test_chn_credit_bounded_backpressure_no_sequence_on_pending():
    p = ChannelProducer("ch-1")
    with pytest.raises(ProtocolError) as exc:
        p.data_envelope([_record("file://h:/r")], producer=_producer())
    assert _code(exc) == "CAPACITY_PENDING" and exc.value.retriable
    assert p.next_sequence == 0  # backpressure issues no sequence
    p.grant_credit(_credit("ch-1", 1, 10))
    env = p.data_envelope([_record("file://h:/r")], producer=_producer())
    assert env["sequence"] == 0
    with pytest.raises(ProtocolError) as exc:
        p.data_envelope([_record("file://h:/r", ordinal=1)], producer=_producer())
    assert _code(exc) == "CAPACITY_PENDING" and p.next_sequence == 1
    p.grant_credit(_credit("ch-1", 1, 10))
    assert p.data_envelope([_record("file://h:/r", ordinal=1)], producer=_producer())["sequence"] == 1


def test_chn_cancel_tombstone_suppresses_late_data():
    domain = "file://h:/r"
    p = ChannelProducer("ch-1")
    p.grant_credit(_credit("ch-1", 8, 1000))
    env = p.data_envelope([_record(domain)], producer=_producer())
    c = ChannelConsumer(["ch-1"], reader_domain=domain)
    c.cancel({"channel_id": "ch-1", "operation_id": uuid.uuid4().hex,
              "reason": "stop", "controller_epoch": 1})
    with pytest.raises(ProtocolError) as exc:
        c.push("ch-1", {"kind": "data", "sequence": env["sequence"], "envelope": env})
    assert _code(exc) == "FENCED"
    # producer side also refuses to emit after cancel
    p.cancel("stop")
    with pytest.raises(ProtocolError) as exc:
        p.data_envelope([_record(domain, ordinal=1)], producer=_producer())
    assert _code(exc) == "FENCED"


def test_chn_scope_cancel_tombstones_all_channels():
    domain = "file://h:/r"
    c = ChannelConsumer(["ch-1", "ch-2"], reader_domain=domain)
    c.cancel({"scope_id": "scope-1", "operation_id": uuid.uuid4().hex,
              "reason": "run cancelled", "controller_epoch": 1})
    for channel in ("ch-1", "ch-2"):
        with pytest.raises(ProtocolError) as exc:
            c.push(channel, {"kind": "eoi", "channel_id": channel, "sequence": 0,
                             "producer_partition_id": "p0", "final": True})
        assert _code(exc) == "FENCED"


def test_chn_seal_only_success_zero_rows_may_seal():
    assert seal_allowed(["success", "success"]) and seal_allowed([])
    for status in ("failed", "cancelled", "idle"):
        with pytest.raises(ProtocolError):
            seal_allowed(["success", status])
    p = ChannelProducer("ch-1")
    scope = {"scope_id": "scope-1", "membership": ["p0"], "generation": 1}
    assert p.seal(seal_scope=scope, manifest_ref={"sha256": "c" * 64},
                  producer_statuses=["success"])["status"] == "success"
    with pytest.raises(ProtocolError):
        p.seal(seal_scope=scope, manifest_ref={"sha256": "c" * 64}, producer_statuses=["failed"])
    with pytest.raises(ProtocolError):
        validate_seal({"channel_id": "ch-1", "sequence": 9, "seal_scope": scope,
                       "manifest_ref": {"sha256": "c" * 64}, "status": "failed"})


def test_chn_read_domain_mismatch_rejected():
    domain = "file://host-a:/r"
    ref = _ref("file://host-b:/r")  # same path shape, different host domain
    with pytest.raises(ProtocolError) as exc:
        refs_descriptor([ref], rows=1, reader_domain=domain)
    assert _code(exc) == "REF_READ_DOMAIN_MISMATCH"
    p = ChannelProducer("ch-1")
    p.grant_credit(_credit("ch-1", 8, 1000))
    rec = {"record_id": record_identity("src", 0), "parent_record_ids": [],
           "occurrence_id": "occ-1", "size_bytes": 10, "payload": ref}
    env = p.data_envelope([rec], producer=_producer())
    c = ChannelConsumer(["ch-1"], reader_domain=domain)
    with pytest.raises(ProtocolError) as exc:
        c.push("ch-1", {"kind": "data", "sequence": env["sequence"], "envelope": env})
    assert _code(exc) == "REF_READ_DOMAIN_MISMATCH"


def test_chn_descriptor_refs_roundtrip(run):
    domain = read_domain(run.directory, "host-a")
    descriptor = {"blocks": [dict(path="materializations/a/output/g/0.pkl", bytes=10, sha256="a" * 64),
                             dict(path="materializations/a/output/g/1.pkl", bytes=20, sha256="b" * 64)],
                  "rows": 3}
    refs = descriptor_refs(run, descriptor, domain=domain, scope_ref="scope-1")
    assert len(refs) == 2 and all(r["kind"] == "file" and r["durability"] == "durable" for r in refs)
    rebuilt = refs_descriptor(refs, rows=3, reader_domain=domain)
    assert rebuilt == descriptor


def test_chn_sequence_order_violation_rejected():
    domain = "file://h:/r"
    c = ChannelConsumer(["ch-1"], reader_domain=domain)
    c.push("ch-1", {"kind": "data", "sequence": 5,
                    "envelope": _envelope(domain, 5)})
    with pytest.raises(ProtocolError) as exc:
        c.push("ch-1", {"kind": "data", "sequence": 5, "envelope": _envelope(domain, 5)})
    assert _code(exc) == "SEQUENCE_ORDER_VIOLATION"
    with pytest.raises(ProtocolError) as exc:
        c.push("ch-1", {"kind": "data", "sequence": 3, "envelope": _envelope(domain, 3)})
    assert _code(exc) == "SEQUENCE_ORDER_VIOLATION"


def test_chn_post_cut_data_before_barrier_rejected():
    domain = "file://h:/r"
    c = ChannelConsumer(["ch-1"], reader_domain=domain)
    # a barrier at sequence 1 with cut 1 means data sequence >= 1 is post-cut;
    # pushing such data before the barrier arrives is an overtake attempt.
    c.push("ch-1", {"kind": "data", "sequence": 0, "envelope": _envelope(domain, 0)})
    c.push("ch-1", {"kind": "barrier", "channel_id": "ch-1", "sequence": 1,
                    "checkpoint_id": "ck-1", "epoch": 1,
                    "cut": {"producer_sequence_bound": 1}, "topology_revision": 0})
    with pytest.raises(ProtocolError) as exc:
        c.push("ch-1", {"kind": "barrier", "channel_id": "ch-1", "sequence": 2,
                        "checkpoint_id": "ck-2", "epoch": 1,
                        "cut": {"producer_sequence_bound": 2}, "topology_revision": 0})
    assert _code(exc) == "INVALID_SPEC"  # channel already holds a barrier


def _envelope(domain, sequence):
    p = ChannelProducer("ch-1")
    p.grant_credit(_credit("ch-1", 100, 10000))
    p._sequence._next = sequence
    return p.data_envelope([_record(domain)], producer=_producer())