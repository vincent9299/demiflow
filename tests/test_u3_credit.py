"""U3 CRD conformance: per-reader credit, channel isolation, sealed early fail (S1).

Pure protocol model: a slow reader exhausts only its own physical channel and
never blocks siblings (exit gate 1); consumption dequeues immediately without
waiting for a cut; finite_sealed bypasses consumer credit and fails early
(non-retriable) on storage exhaustion instead of deadlocking; unsubscribe fences
late data and freezes cursors for the GC safe point; old route_epoch is FENCED.
"""
import uuid

import pytest

from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.contracts.exchange import record_identity, validate_credit
from demiflow.execution.exchange.graph_channel import (
    GraphChannelConsumer, GraphChannelProducer, StorageReservation,
)

_DOMAIN = "file://h:/r"


def _code(exc):
    return exc.value.code


def _record(ordinal=0, size=10):
    return {"record_id": record_identity("src", ordinal), "parent_record_ids": [],
            "occurrence_id": "occ-1", "size_bytes": size, "payload": {"v": ordinal}}


def _producer():
    return {"logical_node_id": "node-1", "occurrence_id": "occ-1"}


def _credit(channel_id, rows, size_bytes, reader="reader-1", epoch=0):
    return {"channel_id": channel_id, "reader_id": reader,
            "credits": {"rows": rows, "bytes": size_bytes}, "route_epoch": epoch,
            "operation_id": uuid.uuid4().hex}


def _data(env):
    return {"kind": "data", "sequence": env["sequence"], "envelope": env}


def _barrier(channel_id, sequence, checkpoint_id="ck-1"):
    return {"kind": "barrier", "channel_id": channel_id, "sequence": sequence,
            "checkpoint_id": checkpoint_id, "epoch": 1,
            "cut": {"producer_sequence_bound": sequence}, "topology_revision": 0}


def test_crd_slow_reader_never_blocks_sibling_channels():
    slow = GraphChannelProducer("ch-a", reader_id="reader-1", exchange_mode="memory_incremental")
    fast = GraphChannelProducer("ch-b", reader_id="reader-2", exchange_mode="memory_incremental")
    slow.grant_credit(_credit("ch-a", 1, 10))
    fast.grant_credit(_credit("ch-b", 8, 1000, reader="reader-2"))
    slow.data_envelope([_record(0)], producer=_producer())
    with pytest.raises(ProtocolError) as exc:
        slow.data_envelope([_record(1)], producer=_producer())
    assert _code(exc) == "CAPACITY_PENDING" and exc.value.retriable
    assert slow.next_sequence == 1  # backpressure issues no sequence on the slow channel
    assert fast.data_envelope([_record(0)], producer=_producer())["sequence"] == 0
    slow.grant_credit(_credit("ch-a", 1, 10))
    assert slow.data_envelope([_record(1)], producer=_producer())["sequence"] == 1


def test_crd_credit_from_non_reader_and_old_epoch_rejected():
    p = GraphChannelProducer("ch-a", reader_id="reader-1",
                             exchange_mode="durable_incremental", route_epoch=1)
    with pytest.raises(ProtocolError) as exc:
        p.grant_credit(_credit("ch-a", 1, 10, reader="intruder", epoch=1))
    assert _code(exc) == "INVALID_SPEC"
    with pytest.raises(ProtocolError) as exc:
        p.grant_credit(_credit("ch-a", 1, 10, epoch=0))
    assert _code(exc) == "FENCED"
    p.grant_credit(_credit("ch-a", 1, 10, epoch=1))
    assert p.data_envelope([_record(0)], producer=_producer())["route_epoch"] == 1


def test_crd_records_dequeue_without_waiting_for_a_cut():
    p = GraphChannelProducer("ch-a", reader_id="reader-1", exchange_mode="memory_incremental")
    p.grant_credit(_credit("ch-a", 8, 1000))
    c = GraphChannelConsumer(["ch-a"], reader_id="reader-1", reader_domain=_DOMAIN)
    env = p.data_envelope([_record(0)], producer=_producer())
    c.push("ch-a", _data(env))
    assert c.cursors == {"ch-a": -1}
    event = c.consume("ch-a")  # timely dequeue: no barrier/cut involved
    assert event["sequence"] == 0 and c.cursors == {"ch-a": 0}
    frame = c.grant_credit(channel_id="ch-a", rows=1, size_bytes=10, operation_id=uuid.uuid4().hex)
    assert frame["reader_id"] == "reader-1" and validate_credit(frame)


def test_crd_sealed_bypasses_consumer_credit_and_charges_storage():
    storage = StorageReservation(20)
    p = GraphChannelProducer("ch-a", reader_id="reader-1",
                             exchange_mode="finite_sealed", storage=storage)
    env = p.data_envelope([_record(0, size=10)], producer=_producer())  # no credit granted
    assert env["sequence"] == 0 and storage.used == 10
    assert storage.feasible(10) and not storage.feasible(11)


def test_crd_sealed_exhaustion_fails_early_not_deadlock():
    storage = StorageReservation(20)
    p = GraphChannelProducer("ch-a", reader_id="reader-1",
                             exchange_mode="finite_sealed", storage=storage)
    p.data_envelope([_record(0, size=10)], producer=_producer())
    with pytest.raises(ProtocolError) as exc:
        p.data_envelope([_record(1, size=11)], producer=_producer())
    assert _code(exc) == "QUOTA_EXCEEDED" and not exc.value.retriable
    assert p.next_sequence == 1  # the failed offer issues no sequence
    storage.release(10)
    assert p.data_envelope([_record(1, size=10)], producer=_producer())["sequence"] == 1
    with pytest.raises(ValueError):
        storage.release(storage.used + 1)
    with pytest.raises(ValueError):
        StorageReservation(-1)


def test_crd_mode_storage_pairing_validated():
    with pytest.raises(ValueError):
        GraphChannelProducer("ch-a", reader_id="reader-1", exchange_mode="finite_sealed")
    with pytest.raises(ValueError):
        GraphChannelProducer("ch-a", reader_id="reader-1",
                             exchange_mode="memory_incremental", storage=StorageReservation(1))


def test_crd_unsubscribe_fences_late_data_and_freezes_cursors():
    p = GraphChannelProducer("ch-a", reader_id="reader-1", exchange_mode="memory_incremental")
    p.grant_credit(_credit("ch-a", 8, 1000))
    c = GraphChannelConsumer(["ch-a"], reader_id="reader-1", reader_domain=_DOMAIN)
    env = p.data_envelope([_record(0)], producer=_producer())
    c.push("ch-a", _data(env))
    c.consume("ch-a")
    c.unsubscribe()
    late = p.data_envelope([_record(1)], producer=_producer())
    with pytest.raises(ProtocolError) as exc:
        c.push("ch-a", _data(late))
    assert _code(exc) == "FENCED"
    with pytest.raises(ProtocolError):
        c.consume("ch-a")
    assert c.cursors == {"ch-a": 0}


def test_crd_multi_input_alignment_finished_channel_stays_participant():
    # InputGate docstring (contracts/exchange.py): empty/finished channels stay
    # participants until the same cut — an EOI channel still owes its barrier.
    p = GraphChannelProducer("ch-a", reader_id="reader-1", exchange_mode="memory_incremental")
    p.grant_credit(_credit("ch-a", 8, 1000))
    c = GraphChannelConsumer(["ch-a", "ch-b"], reader_id="reader-1", reader_domain=_DOMAIN)
    env = p.data_envelope([_record(0)], producer=_producer())
    c.push("ch-a", _data(env))
    c.push("ch-a", _barrier("ch-a", 1))
    c.push("ch-b", {"kind": "eoi", "channel_id": "ch-b", "sequence": 0,
                    "producer_partition_id": "p1", "final": True})
    c.push("ch-b", _barrier("ch-b", 1))
    assert not c.aligned("ck-1")  # pre-cut data on ch-a is not consumed yet
    c.consume("ch-a")
    assert c.aligned("ck-1")
    assert c.release("ck-1") == 0
    assert c.cursors == {"ch-a": 0, "ch-b": -1}


def test_crd_old_route_epoch_data_fenced():
    old = GraphChannelProducer("ch-a", reader_id="reader-1",
                               exchange_mode="memory_incremental", route_epoch=0)
    old.grant_credit(_credit("ch-a", 1, 10, epoch=0))
    env = old.data_envelope([_record(0)], producer=_producer())
    c = GraphChannelConsumer(["ch-a"], reader_id="reader-1", reader_domain=_DOMAIN, route_epoch=1)
    with pytest.raises(ProtocolError) as exc:
        c.push("ch-a", _data(env))
    assert _code(exc) == "FENCED"


def test_crd_cancel_tombstone_both_ends():
    p = GraphChannelProducer("ch-a", reader_id="reader-1", exchange_mode="memory_incremental")
    p.grant_credit(_credit("ch-a", 8, 1000))
    c = GraphChannelConsumer(["ch-a"], reader_id="reader-1", reader_domain=_DOMAIN)
    c.cancel({"channel_id": "ch-a", "operation_id": uuid.uuid4().hex,
              "reason": "stop", "controller_epoch": 1})
    env = p.data_envelope([_record(0)], producer=_producer())
    with pytest.raises(ProtocolError) as exc:
        c.push("ch-a", _data(env))
    assert _code(exc) == "FENCED"
    p.cancel("stop")
    with pytest.raises(ProtocolError) as exc:
        p.data_envelope([_record(1)], producer=_producer())
    assert _code(exc) == "FENCED"
    sealed = GraphChannelProducer("ch-s", reader_id="reader-1", exchange_mode="finite_sealed",
                                  storage=StorageReservation(100))
    sealed.cancel("stop")
    with pytest.raises(ProtocolError) as exc:
        sealed.data_envelope([_record(0)], producer=_producer())
    assert _code(exc) == "FENCED"  # cancel precedes the storage charge