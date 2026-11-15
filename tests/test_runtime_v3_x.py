"""Gate G-X unit acceptance for module C (lane X): channels/routing/credit/GC.

Every test calls the real production symbols in demiflow/execution/exchange/
dataflow_*.py. Cross-lane DTOs (contracts.dataflow / dataflow_errors) are not
yet implemented at this baseline, so C14-sanctioned spec-bound fakes are
installed into sys.modules BEFORE the production import; production code uses
plain imports with no fallback. The editable-install finder that redirects
``demiflow`` to the main repository is removed in this test process only, and
every test asserts in-process that the loaded modules come from this worktree.
"""
import base64
import dataclasses
import hashlib
import json
import math
import sys
import threading
import types
from pathlib import Path

import pytest

TREE = Path(__file__).resolve().parents[1]

# -- worktree binding (test domain only; IL012) --------------------------------
sys.meta_path[:] = [finder for finder in sys.meta_path
                    if "__editable__" not in getattr(finder, "__module__", "")]
if str(TREE) not in sys.path:
    sys.path.insert(0, str(TREE))

import demiflow  # noqa: E402
from demiflow.execution.recovery_contract import document, identity  # noqa: E402

assert Path(demiflow.__file__).resolve().is_relative_to(TREE), demiflow.__file__


def _assert_tree_binding():
    for name, module in list(sys.modules.items()):
        if name == "demiflow" or name.startswith("demiflow."):
            if getattr(module, "__test_double__", False):
                assert getattr(module, "__file__", None) is None, name
                continue
            path = getattr(module, "__file__", None)
            if path:
                assert Path(path).resolve().is_relative_to(TREE), (name, path)


# -- spec-bound cross-lane fakes (C14; interfaces.pyi field sets) --------------

def _frozen(name, fields):
    cls = dataclasses.make_dataclass(name, [(f, "typing.Any") for f in fields], frozen=True)
    cls.__module__ = "demiflow.execution.contracts.dataflow"
    return cls


ArtifactRef = _frozen("ArtifactRef", ["digest", "size_bytes", "store_id", "key", "media_type"])
Bounds = _frozen("Bounds", ["max_nodes", "max_edges", "max_partitions", "max_depth",
                            "max_control_bytes", "max_row_bytes", "max_batch_bytes",
                            "max_resident_bytes", "max_spill_bytes", "max_state_bytes",
                            "max_pending_events", "max_open_files", "max_output_rows", "timeout_ms"])
Record = _frozen("Record", ["record_id", "source_position", "payload_ref", "inline",
                            "encoded_bytes", "sequence"])
EncodedRecord = _frozen("EncodedRecord", ["record_id", "encoding", "data", "ref",
                                          "encoded_bytes", "decoded_upper_bytes", "sequence"])
ExchangeSpec = _frozen("ExchangeSpec", ["channel_id", "edge_id", "producer_partitions",
                                        "consumer_partitions", "mode", "routing", "keys",
                                        "scope_id", "route_epoch", "credit_rows", "credit_bytes",
                                        "storage_bytes", "read_domain", "reason"])
RoutedRecord = _frozen("RoutedRecord", ["reader_id", "delivery_id", "record"])
ProducerCompletion = _frozen("ProducerCompletion", ["partition_id", "generation", "status",
                                                    "last_sequence", "manifest_ref"])
CreditLease = _frozen("CreditLease", ["operation_id", "channel_id", "reader_id", "rows", "size_bytes"])
CutRequest = _frozen("CutRequest", ["cut_id", "sequence", "plan_revision", "route_epoch",
                                    "participants", "final"])
SourceCursor = _frozen("SourceCursor", ["split_id", "offset", "next_sequence", "end_of_input",
                                        "lease_ref", "pending_refs"])
CutContribution = _frozen("CutContribution", ["cut_id", "participant_id", "boot_id", "sources",
                                              "operator_states", "channels", "buffers", "effects",
                                              "sinks", "final"])
RecoveryImage = _frozen("RecoveryImage", ["commit_id", "cut", "contributions", "confirmed_sinks",
                                          "source_vector"])

_DATAFLOW_CODES = frozenset({
    "INVALID_CONTRACT", "UNSUPPORTED_CAPABILITY", "RESOURCE_INFEASIBLE", "BUDGET_EXCEEDED",
    "SOURCE_CHANGED", "CODE_MISMATCH", "FENCED", "CANCELLED", "DEADLINE_EXCEEDED",
    "CORRUPT_ARTIFACT", "UNKNOWN_EFFECT", "INCOMPATIBLE_HISTORY", "INTERNAL_PROTOCOL",
    "CAPACITY_PENDING",
})


class DataflowError(Exception):
    def __init__(self, code, message, *, retryable=False, context=None):
        if code not in _DATAFLOW_CODES:
            raise ValueError("unknown dataflow error code: " + str(code))
        self.code = code
        self.message = message
        self.retryable = retryable
        self.context = dict(context or {})
        super().__init__(code + ": " + message)


_fake_dataflow = types.ModuleType("demiflow.execution.contracts.dataflow")
for _name in ("ArtifactRef", "Bounds", "Record", "EncodedRecord", "ExchangeSpec", "RoutedRecord",
              "ProducerCompletion", "CreditLease", "CutRequest", "SourceCursor", "CutContribution",
              "RecoveryImage"):
    setattr(_fake_dataflow, _name, globals()[_name])
_fake_errors = types.ModuleType("demiflow.execution.contracts.dataflow_errors")
_fake_errors.DataflowError = DataflowError
# Explicit test-double markers (C14): no __file__ is synthesized, so a double
# can never masquerade as a real tree-bound module in the evidence report.
_fake_dataflow.__test_double__ = True
_fake_dataflow.__double_spec__ = ("interfaces.pyi frozen DTO field sets; real owner lane C "
                                  "runtime_v3_c_20261114 @ 44c01c4 (AST-verified zero deviation)")
_fake_errors.__test_double__ = True
_fake_errors.__double_spec__ = ("C01 DataflowError(code, message, *, retryable, context); closed code "
                                "set plus CAPACITY_PENDING per C01 prose (adjudication pending)")
sys.modules["demiflow.execution.contracts.dataflow"] = _fake_dataflow
sys.modules["demiflow.execution.contracts.dataflow_errors"] = _fake_errors

import demiflow.execution.contracts as _contracts_pkg  # noqa: E402
_contracts_pkg.dataflow = _fake_dataflow
_contracts_pkg.dataflow_errors = _fake_errors

from demiflow.execution.contracts.exchange import (  # noqa: E402
    InputGate, ProtocolError, check_read_domain, validate_data_ref,
)
from demiflow.execution.exchange import dataflow_bridge as bridge  # noqa: E402
from demiflow.execution.exchange import dataflow_channels as channels  # noqa: E402
from demiflow.execution.exchange.dataflow_state import ChannelSetState, RoutingCounters  # noqa: E402

_assert_tree_binding()


# -- test-domain fixtures -------------------------------------------------------

class FakeStore:
    """Content-addressed in-memory ArtifactStore (H031/H035 protocol shape)."""

    def __init__(self, store_id="store-x"):
        self.store_id = store_id
        self.objects = {}
        self.refs = {}
        self.owners = {}
        self.physical_writes = 0
        self.released = []

    def put(self, chunks, *, max_bytes, media_type):
        data = b"".join(chunks)
        if len(data) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "store write budget exceeded")
        digest = hashlib.sha256(data).hexdigest()
        if digest not in self.objects:
            self.objects[digest] = data
            self.refs[digest] = ArtifactRef(digest=digest, size_bytes=len(data),
                                            store_id=self.store_id, key=digest,
                                            media_type=media_type)
            self.physical_writes += 1
        return self.refs[digest]

    def read(self, ref, *, max_bytes):
        if ref.digest not in self.objects:
            raise FileNotFoundError(ref.key)
        data = self.objects[ref.digest]
        if len(data) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "store read budget exceeded")
        yield data

    def inspect(self, ref):
        return ref.digest in self.objects

    def retain(self, ref, *, owner):
        self.owners.setdefault(ref.digest, set()).add(owner)

    def release(self, ref, *, owner):
        self.owners.get(ref.digest, set()).discard(owner)
        self.released.append(ref)
        if not self.owners.get(ref.digest):
            self.objects.pop(ref.digest, None)


def make_bounds(**overrides):
    values = dict(max_nodes=1024, max_edges=16, max_partitions=8, max_depth=32,
                  max_control_bytes=1 << 20, max_row_bytes=1 << 20, max_batch_bytes=1 << 20,
                  max_resident_bytes=1 << 20, max_spill_bytes=1 << 20, max_state_bytes=1 << 20,
                  max_pending_events=256, max_open_files=64, max_output_rows=1 << 20,
                  timeout_ms=60000)
    values.update(overrides)
    return Bounds(**values)


def make_spec(channel_id="ch-a", *, mode="memory_incremental", routing="forward",
              consumers=("r0",), producers=("p0",), keys=(), credit_rows=64,
              credit_bytes=65536, storage_bytes=0, scope_id=None, route_epoch=0,
              read_domain="dom-x"):
    edge_id = identity(["dataflow-edge-v3", "prod-node", "out-0", "cons-node", "in-0"])
    return ExchangeSpec(channel_id=channel_id, edge_id=edge_id, producer_partitions=producers,
                        consumer_partitions=consumers, mode=mode, routing=routing, keys=keys,
                        scope_id=scope_id, route_epoch=route_epoch, credit_rows=credit_rows,
                        credit_bytes=credit_bytes, storage_bytes=storage_bytes,
                        read_domain=read_domain, reason="unit-acceptance")


def make_services(store=None, bounds=None, plan_revision=1):
    return types.SimpleNamespace(store=store or FakeStore(), bounds=bounds or make_bounds(),
                                 assignment=types.SimpleNamespace(plan_revision=plan_revision),
                                 check_stop=lambda: None, clock_ms=lambda: 0,
                                 journal=None, constants={}, programs={})


def make_record(row, *, sequence=0, record_id=None, source_position=None):
    rid = record_id or hashlib.sha256(bridge._codec_dumps(bridge._encode_value(row))).hexdigest()
    raw = bridge._encode_payload(row, max_bytes=1 << 20)
    return Record(record_id=rid, source_position=source_position or {"next": sequence + 1},
                  payload_ref=None, inline=row, encoded_bytes=len(raw), sequence=sequence)


def make_ref_record(row, store, *, sequence=0):
    raw = bridge._encode_payload(row, max_bytes=1 << 20)
    ref = store.put(iter((raw,)), max_bytes=len(raw),
                    media_type=bridge.RECORD_MEDIA_TYPE)
    rid = hashlib.sha256(bridge._codec_dumps(bridge._encode_value(row))).hexdigest()
    return Record(record_id=rid, source_position={"next": sequence + 1}, payload_ref=ref,
                  inline=None, encoded_bytes=ref.size_bytes, sequence=sequence)


def make_cut(cut_id="cut-1", *, sequence=10, plan_revision=1, route_epoch=0, final=False):
    return CutRequest(cut_id=cut_id, sequence=sequence, plan_revision=plan_revision,
                      route_epoch=route_epoch, participants=("w0",), final=final)


def expect_error(code, fn, *args, **kwargs):
    with pytest.raises(DataflowError) as excinfo:
        fn(*args, **kwargs)
    assert excinfo.value.code == code, (excinfo.value.code, excinfo.value.message)
    return excinfo.value


@pytest.fixture(autouse=True)
def _tree_binding():
    _assert_tree_binding()
    yield
    _assert_tree_binding()


# -- X001 open_channels ---------------------------------------------------------

def test_x001():
    services = make_services()
    specs = (make_spec("ch-a"), make_spec("ch-b", consumers=("r0", "r1"), routing="broadcast"))
    state = channels.open_channels(specs, services=services)
    assert isinstance(state, ChannelSetState)
    assert state.channel_ids() == ("ch-a", "ch-b")
    assert state.routing is not None and state.routing.route_epoch == 0
    assert state.routing.next_ordinal("ch-a") == 0
    stopped = make_services()
    def _stop():
        raise DataflowError("CANCELLED", "stop requested")
    stopped.check_stop = _stop
    expect_error("CANCELLED", channels.open_channels, specs, services=stopped)
    expect_error("INVALID_CONTRACT", channels.open_channels,
                 (make_spec("ch-a", consumers=()),), services=services)
    expect_error("INVALID_CONTRACT", channels.open_channels,
                 (make_spec("ch-a"), make_spec("ch-a")), services=services)
    # per-connection windows shrink to the shared resident budget, never over-allocate
    tight = make_services(bounds=make_bounds(max_resident_bytes=100))
    small = channels.open_channels((make_spec("ch-a", credit_rows=64, credit_bytes=65536),),
                                   services=tight)
    credit = small.reader_credit("ch-a", "r0")
    assert credit["bytes_total"] == 100 and credit["rows_total"] == 64


# -- X002 encode_record ---------------------------------------------------------

def test_x002():
    bounds = make_bounds()
    row = {"n": [-0.0], "p": [0.0], "t": (1, "x"), "l": [1, "x"], "b": b"\x00\xff",
           "i": 7, "f": 7.0, "nan": float("nan"), "deep": {"k": [None, True]}}
    record = make_record(row)
    encoded = channels.encode_record(record, bounds=bounds)
    assert encoded.encoding == bridge.RECORD_ENCODING
    assert encoded.encoded_bytes == len(encoded.data)
    assert encoded.ref is None
    resolved = channels.resolve_record(encoded, store=FakeStore(), bounds=bounds)
    assert math.copysign(1.0, resolved.inline["n"][0]) == -1.0
    assert math.copysign(1.0, resolved.inline["p"][0]) == 1.0
    assert resolved.inline["t"] == (1, "x") and type(resolved.inline["t"]) is tuple
    assert type(resolved.inline["l"]) is list
    assert resolved.inline["b"] == b"\x00\xff"
    assert type(resolved.inline["i"]) is int and type(resolved.inline["f"]) is float
    assert math.isnan(resolved.inline["nan"])
    assert resolved.inline["n"] is not row["n"]  # independent containers
    assert resolved.record_id == record.record_id
    assert resolved.source_position == record.source_position
    lying = Record(record_id=record.record_id, source_position={}, payload_ref=None,
                   inline=row, encoded_bytes=1, sequence=0)
    expect_error("INVALID_CONTRACT", channels.encode_record, lying, bounds=bounds)
    expect_error("BUDGET_EXCEEDED", channels.encode_record, make_record({"s": "x" * 200}),
                 bounds=make_bounds(max_row_bytes=64))
    class _Code:
        pass
    bad = Record(record_id=record.record_id, source_position={}, payload_ref=None,
                 inline={"obj": _Code()}, encoded_bytes=1, sequence=0)
    expect_error("INVALID_CONTRACT", channels.encode_record, bad, bounds=bounds)
    expect_error("BUDGET_EXCEEDED", channels.encode_record,
                 make_record({"deep": [[[[[[[[[[[1]]]]]]]]]]] * 1}),
                 bounds=make_bounds(max_row_bytes=8))


# -- X003 resolve_record --------------------------------------------------------

def test_x003():
    bounds = make_bounds()
    store = FakeStore()
    record = make_record({"v": [1, 2, 3]}, sequence=4)
    encoded = channels.encode_record(record, bounds=bounds)
    assert channels.resolve_record(encoded, store=store, bounds=bounds) == record
    ref_record = make_ref_record({"big": "y" * 128}, store, sequence=5)
    ref_encoded = channels.encode_record(ref_record, bounds=bounds)
    assert ref_encoded.ref == ref_record.payload_ref
    assert channels.resolve_record(ref_encoded, store=store, bounds=bounds) == ref_record
    # small encoding with a huge declared decode ceiling is rejected before allocation
    huge = EncodedRecord(record_id=record.record_id, encoding=bridge.RECORD_ENCODING,
                         data=encoded.data, ref=None, encoded_bytes=len(encoded.data),
                         decoded_upper_bytes=1 << 40, sequence=0)
    expect_error("BUDGET_EXCEEDED", channels.resolve_record, huge, store=store, bounds=bounds)
    # wrong media type (code, not data)
    code_ref = ArtifactRef(digest=ref_record.payload_ref.digest, size_bytes=10,
                           store_id=store.store_id, key="k", media_type="application/x-python-pickle")
    bad_media = Record(record_id=record.record_id, source_position={}, payload_ref=code_ref,
                       inline=None, encoded_bytes=10, sequence=0)
    expect_error("INVALID_CONTRACT", channels.encode_record, bad_media, bounds=bounds)
    # cross-store domain reference (document and ref agree, store differs)
    other_ref = dataclasses.replace(ref_record.payload_ref, store_id="other-store")
    other_record = Record(record_id=ref_record.record_id, source_position={"next": 6},
                          payload_ref=other_ref, inline=None,
                          encoded_bytes=other_ref.size_bytes, sequence=5)
    cross = channels.encode_record(other_record, bounds=bounds)
    expect_error("INVALID_CONTRACT", channels.resolve_record, cross, store=store, bounds=bounds)
    # digest mismatch -> CORRUPT_ARTIFACT
    tampered = ArtifactRef(digest="0" * 64, size_bytes=ref_record.payload_ref.size_bytes,
                           store_id=store.store_id, key=ref_record.payload_ref.key,
                           media_type=bridge.RECORD_MEDIA_TYPE)
    store.objects[tampered.digest] = b"z" * tampered.size_bytes
    bad_digest = EncodedRecord(record_id=record.record_id, encoding=bridge.RECORD_ENCODING,
                               data=ref_encoded.data, ref=tampered,
                               encoded_bytes=len(ref_encoded.data),
                               decoded_upper_bytes=len(ref_encoded.data), sequence=5)
    expect_error("CORRUPT_ARTIFACT", channels.resolve_record, bad_digest, store=store, bounds=bounds)
    # tampered inline document
    flipped = bytearray(encoded.data)
    flipped[-2] ^= 0x01
    tamper = EncodedRecord(record_id=record.record_id, encoding=bridge.RECORD_ENCODING,
                           data=bytes(flipped), ref=None, encoded_bytes=len(flipped),
                           decoded_upper_bytes=len(flipped), sequence=4)
    expect_error("CORRUPT_ARTIFACT", channels.resolve_record, tamper, store=store, bounds=bounds)


# -- X004 route_record ----------------------------------------------------------

def test_x004():
    bounds = make_bounds()
    services = make_services(bounds=bounds)
    spec = make_spec("ch-h", routing="hash", keys=("user_id",), consumers=("r0", "r1", "r2"))
    state = channels.open_channels((spec,), services=services)
    rows = [{"user_id": "u%d" % i, "v": i} for i in range(30)]
    targets = []
    for i, row in enumerate(rows):
        routed = channels.route_record(make_record(row, sequence=i), spec=spec, state=state.routing)
        assert len(routed) == 1
        targets.append(routed[0].reader_id)
    # stable across a fresh ledger (no Python hash): same keys -> same readers
    state2 = channels.open_channels((spec,), services=make_services(bounds=bounds))
    for i, row in enumerate(rows):
        again = channels.route_record(make_record(row, sequence=i), spec=spec, state=state2.routing)
        assert again[0].reader_id == targets[i]
    assert set(targets) == {"r0", "r1", "r2"}  # balanced over replicas, record unchanged
    # rebalance rotates by ordinal and resumes the suffix after restore
    rspec = make_spec("ch-r", routing="rebalance", consumers=("r0", "r1"))
    rstate = channels.open_channels((rspec,), services=services)
    first = [channels.route_record(make_record({"i": i}, sequence=i), spec=rspec,
                                   state=rstate.routing)[0].reader_id for i in range(10)]
    assert first == ["r0", "r1"] * 5
    resumed = RoutingCounters(values={"ch-r": 10}, route_epoch=0)
    suffix = [channels.route_record(make_record({"i": i}, sequence=i), spec=rspec,
                                    state=resumed)[0].reader_id for i in range(10, 20)]
    assert suffix == ["r0", "r1"] * 5
    # broadcast: every reader, per-(record, edge, reader) delivery identity
    bspec = make_spec("ch-b", routing="broadcast", consumers=("r0", "r1"))
    bstate = channels.open_channels((bspec,), services=services)
    record = make_record({"x": 1})
    routed = channels.route_record(record, spec=bspec, state=bstate.routing)
    assert [r.reader_id for r in routed] == ["r0", "r1"]
    for r in routed:
        assert r.delivery_id == identity(["dataflow-delivery-v3", record.record_id,
                                          bspec.edge_id, r.reader_id])
        assert r.record is record
    # target routing
    tspec = make_spec("ch-t", routing="target", keys=("dest",), consumers=("r0", "r1"))
    tstate = channels.open_channels((tspec,), services=services)
    assert channels.route_record(make_record({"dest": "r1"}), spec=tspec,
                                 state=tstate.routing)[0].reader_id == "r1"
    expect_error("INVALID_CONTRACT", channels.route_record, make_record({"dest": "ghost"}),
                 spec=tspec, state=tstate.routing)
    # forward needs exactly one reader; oversize and lying sizes rejected
    expect_error("INVALID_CONTRACT", channels.route_record, make_record({"x": 1}),
                 spec=make_spec(routing="forward", consumers=("r0", "r1")),
                 state=bstate.routing)
    expect_error("BUDGET_EXCEEDED", channels.route_record, make_record({"s": "x" * 200}),
                 spec=make_spec("ch-small", credit_bytes=64), state=bstate.routing)
    # slow branch does not consume sibling credit
    cstate = channels.open_channels((make_spec("ch-c", consumers=("r0", "r1"),
                                               routing="broadcast"),), services=services)
    cstate.reserve("ch-c", "r0", rows=64, size_bytes=65536, operation_id="op-r0")
    assert cstate.reader_credit("ch-c", "r1")["rows_left"] == 64
    assert cstate.reader_credit("ch-c", "r0")["rows_left"] == 0


# -- X005 reserve_credit --------------------------------------------------------

def test_x005():
    services = make_services(bounds=make_bounds(max_resident_bytes=100))
    state = channels.open_channels((make_spec("ch-a", credit_rows=64, credit_bytes=65536),),
                                   services=services)
    lease = channels.reserve_credit("ch-a", "r0", rows=1, size_bytes=100,
                                    operation_id="op-1", state=state)
    assert isinstance(lease, CreditLease) and lease.size_bytes == 100
    again = channels.reserve_credit("ch-a", "r0", rows=1, size_bytes=100,
                                    operation_id="op-1", state=state)
    assert again == lease  # idempotent same-content retry
    expect_error("INVALID_CONTRACT", channels.reserve_credit, "ch-a", "r0", rows=2,
                 size_bytes=100, operation_id="op-1", state=state)
    pending = expect_error("CAPACITY_PENDING", channels.reserve_credit, "ch-a", "r0",
                           rows=1, size_bytes=1, operation_id="op-2", state=state)
    assert pending.retryable is True
    oversized = expect_error("BUDGET_EXCEEDED", channels.reserve_credit, "ch-a", "r0",
                             rows=1, size_bytes=200, operation_id="op-3", state=state)
    assert oversized.retryable is False
    state.release(lease)
    assert state.reader_credit("ch-a", "r0")["bytes_left"] == 100
    channels.reserve_credit("ch-a", "r0", rows=1, size_bytes=100, operation_id="op-4", state=state)
    expect_error("INVALID_CONTRACT", channels.reserve_credit, "ch-a", "ghost", rows=1,
                 size_bytes=1, operation_id="op-5", state=state)


# -- X006 publish_segment -------------------------------------------------------

def test_x006():
    store = FakeStore()
    bounds = make_bounds()
    spec = make_spec("ch-d", mode="durable_incremental")
    records = tuple(channels.encode_record(make_record({"i": i}, sequence=i), bounds=bounds)
                    for i in range(3))
    ref = channels.publish_segment(records, spec=spec, store=store, operation_id="pub-1")
    assert store.inspect(ref)
    manifest = json.loads(b"".join(store.read(ref, max_bytes=1 << 20)))
    assert manifest["schema"] == "demiflow.segment.v3"
    assert manifest["sequence_start"] == 0 and manifest["sequence_end"] == 2
    assert manifest["segment_id"] == identity(
        ["dataflow-segment-v3", spec.channel_id, spec.route_epoch,
         list(spec.producer_partitions), 0, 2, manifest["content_digest"]])
    writes = store.physical_writes
    retry = channels.publish_segment(records, spec=spec, store=store, operation_id="pub-1")
    assert retry == ref and store.physical_writes == writes  # lost-response retry: no double charge
    other = channels.publish_segment(records[:2], spec=spec, store=store, operation_id="pub-2")
    assert other.digest != ref.digest
    expect_error("INVALID_CONTRACT", channels.publish_segment, (records[1], records[0]),
                 spec=spec, store=store, operation_id="pub-3")
    expect_error("INVALID_CONTRACT", channels.publish_segment, (), spec=spec, store=store,
                 operation_id="pub-4")
    sealed_spec = make_spec("ch-s", mode="finite_sealed", storage_bytes=64, scope_id="scope-1")
    expect_error("BUDGET_EXCEEDED", channels.publish_segment, records, spec=sealed_spec,
                 store=store, operation_id="pub-5")
    # crash before publish leaves nothing visible: a failed put never registers
    class CrashingStore(FakeStore):
        def put(self, chunks, *, max_bytes, media_type):
            raise RuntimeError("crash before commit")
    crashing = CrashingStore()
    with pytest.raises(RuntimeError):
        channels.publish_segment(records, spec=spec, store=crashing, operation_id="pub-6")
    assert crashing.objects == {}


# -- X007 seal_scope ------------------------------------------------------------

def test_x007():
    store = FakeStore()
    bounds = make_bounds()
    manifest = channels.publish_segment(
        (channels.encode_record(make_record({"i": 0}), bounds=bounds),),
        spec=make_spec(mode="durable_incremental"), store=store, operation_id="m-1")
    empty = channels.publish_segment(
        (channels.encode_record(make_record({"i": 0}), bounds=bounds),),
        spec=make_spec(mode="durable_incremental"), store=store, operation_id="m-2")
    ok = (ProducerCompletion(partition_id="p0", generation=1, status="succeeded",
                             last_sequence=0, manifest_ref=manifest),
          ProducerCompletion(partition_id="p1", generation=1, status="succeeded",
                             last_sequence=-1, manifest_ref=empty))
    ref = channels.seal_scope("scope-1", ok, store=store)
    seal = json.loads(b"".join(store.read(ref, max_bytes=1 << 20)))
    assert seal["status"] == "success" and seal["membership"] == ["p0", "p1"]
    assert channels.seal_scope("scope-1", ok, store=store) == ref  # idempotent
    for status in ("failed", "cancelled", "unknown"):
        bad = (ok[0], ProducerCompletion(partition_id="p1", generation=1, status=status,
                                         last_sequence=0, manifest_ref=empty))
        expect_error("INVALID_CONTRACT", channels.seal_scope, "scope-1", bad, store=store)
    missing = ArtifactRef(digest="f" * 64, size_bytes=1, store_id=store.store_id, key="gone",
                          media_type=bridge.SEGMENT_MEDIA_TYPE)
    expect_error("CORRUPT_ARTIFACT", channels.seal_scope, "scope-1",
                 (ProducerCompletion(partition_id="p0", generation=1, status="succeeded",
                                     last_sequence=0, manifest_ref=missing),), store=store)
    expect_error("INVALID_CONTRACT", channels.seal_scope, "scope-1",
                 (ok[0], ProducerCompletion(partition_id="p1", generation=2, status="succeeded",
                                            last_sequence=0, manifest_ref=empty)), store=store)


# -- X008 snapshot_channels -----------------------------------------------------

def test_x008():
    store = FakeStore()
    bounds = make_bounds()
    services = make_services(store=store, bounds=bounds)
    spec = make_spec("ch-a")
    state = channels.open_channels((spec,), services=services)
    for i in range(3):
        encoded = channels.encode_record(make_record({"i": i}, sequence=i), bounds=bounds)
        state.enqueue("ch-a", state.issue_sequence("ch-a"), encoded)
    lease = state.reserve("ch-a", "r0", rows=1, size_bytes=10, operation_id="lease-1")
    cut = make_cut(sequence=10)
    refs = channels.snapshot_channels(state, cut=cut, store=store)
    assert set(refs) == {"ch-a"}
    doc = json.loads(b"".join(store.read(refs["ch-a"], max_bytes=1 << 20)))
    assert doc["sequence_next"] >= 3  # cut never overtakes issued data
    assert doc["buffer_segment"] is not None  # memory mode rows are durable at cut
    segment = json.loads(b"".join(store.read(
        channels._ref_from_doc(doc["buffer_segment"]), max_bytes=1 << 20)))
    assert segment["sequence_end"] == 2 < cut.sequence
    assert doc["leases"][0]["operation_id"] == "lease-1"
    assert state.buffer_items("ch-a")  # live buffer stays intact for the running plane
    # final cut over a fully succeeded sealed scope produces the seal
    sspec = make_spec("ch-s", mode="finite_sealed", storage_bytes=1 << 20, scope_id="scope-1")
    sstate = channels.open_channels((sspec,), services=services)
    encoded = channels.encode_record(make_record({"i": 0}), bounds=bounds)
    sstate.enqueue("ch-s", sstate.issue_sequence("ch-s"), encoded)
    manifest = channels.publish_segment((encoded,), spec=sspec, store=store, operation_id="m-s")
    sstate.register_completion("ch-s", ProducerCompletion(partition_id="p0", generation=1,
                                                          status="succeeded", last_sequence=0,
                                                          manifest_ref=manifest))
    sealed_refs = channels.snapshot_channels(sstate, cut=make_cut("cut-f", final=True), store=store)
    fdoc = json.loads(b"".join(store.read(sealed_refs["ch-s"], max_bytes=1 << 20)))
    assert fdoc["seal_ref"] is not None
    # a failed member never seals
    fstate = channels.open_channels((sspec,), services=services)
    fstate.register_completion("ch-s", ProducerCompletion(partition_id="p0", generation=1,
                                                          status="failed", last_sequence=0,
                                                          manifest_ref=manifest))
    bad = channels.snapshot_channels(fstate, cut=make_cut("cut-b", final=True), store=store)
    bdoc = json.loads(b"".join(store.read(bad["ch-s"], max_bytes=1 << 20)))
    assert bdoc["seal_ref"] is None


# -- X009 restore_channels ------------------------------------------------------

def test_x009():
    store = FakeStore()
    bounds = make_bounds()
    services = make_services(store=store, bounds=bounds)
    spec = make_spec("ch-r", routing="rebalance", consumers=("r0", "r1"))
    state = channels.open_channels((spec,), services=services)
    first = []
    for i in range(10):
        record = make_record({"i": i}, sequence=i)
        first.append(channels.route_record(record, spec=spec, state=state.routing)[0].reader_id)
        state.enqueue("ch-r", state.issue_sequence("ch-r"),
                      channels.encode_record(record, bounds=bounds))
    lease = state.reserve("ch-r", "r0", rows=1, size_bytes=10, operation_id="lease-r")
    refs = channels.snapshot_channels(state, cut=make_cut(sequence=20), store=store)
    restored = channels.restore_channels((spec,), refs, services=services)
    assert restored.routing.values()["ch-r"] == 10
    assert restored.next_sequence("ch-r") == 10
    assert restored.reader_credit("ch-r", "r0") == state.reader_credit("ch-r", "r0")
    assert len(restored.buffer_items("ch-r")) == 10  # unconfirmed data rebuilt
    suffix = [channels.route_record(make_record({"i": i}, sequence=i), spec=spec,
                                    state=restored.routing)[0].reader_id for i in range(10, 20)]
    reference = channels.open_channels((spec,), services=make_services(store=FakeStore(), bounds=bounds))
    expected = [channels.route_record(make_record({"i": i}, sequence=i), spec=spec,
                                      state=reference.routing)[0].reader_id for i in range(20)]
    assert first + suffix == expected  # same physical channel suffix as the first run
    # wrong epoch / plan are fenced
    stale_epoch = make_spec("ch-r", routing="rebalance", consumers=("r0", "r1"), route_epoch=1)
    expect_error("FENCED", channels.restore_channels, (stale_epoch,), refs, services=services)
    expect_error("FENCED", channels.restore_channels, (spec,), refs,
                 services=make_services(store=store, bounds=bounds, plan_revision=2))
    expect_error("INVALID_CONTRACT", channels.restore_channels, (spec,), {}, services=services)


# -- X010 reclaim_segments ------------------------------------------------------

def test_x010():
    store = FakeStore()
    bounds = make_bounds()
    services = make_services(store=store, bounds=bounds)
    spec = make_spec("ch-a", consumers=("r0", "r1"), routing="broadcast")
    state = channels.open_channels((spec,), services=services)
    refs = []
    for batch in range(3):
        records = tuple(channels.encode_record(make_record({"i": batch * 2 + j},
                                                           sequence=batch * 2 + j), bounds=bounds)
                        for j in range(2))
        ref = channels.publish_segment(records, spec=spec, store=store,
                                       operation_id="seg-%d" % batch)
        state.register_segment("ch-a", ref, seq_end=batch * 2 + 1)
        refs.append(ref)
    cut = make_cut(sequence=50)
    def image(cursors, confirmed=()):
        contribution = CutContribution(cut_id="cut-1", participant_id="w0", boot_id="b0",
                                       sources=(), operator_states={},
                                       channels={"ch-a": {r: {"cursor": c} for r, c in cursors.items()}},
                                       buffers=(), effects=(), sinks={}, final=False)
        return RecoveryImage(commit_id="c1", cut=cut, contributions=(contribution,),
                             confirmed_sinks={str(i): r for i, r in enumerate(confirmed)},
                             source_vector=())
    # unconfirmed cancel: r1 tombstoned live but absent from the committed image
    state.cancel_reader("ch-a", "r1", "user-cancel")
    released = channels.reclaim_segments(state, committed=image({"r0": 5}), store=store)
    assert released == ()  # r1 holds the frontier at its last cursor (-1)
    assert store.inspect(refs[0])
    # once r1's final cursor is committed the frontier advances
    released = channels.reclaim_segments(state, committed=image({"r0": 5, "r1": 3}), store=store)
    assert [r.digest for r in released] == [refs[0].digest, refs[1].digest]
    assert not store.inspect(refs[0]) and store.inspect(refs[2])
    assert channels.reclaim_segments(state, committed=image({"r0": 5, "r1": 3}), store=store) == ()
    # pinned effect/sink refs are retained below the frontier
    state.register_segment("ch-a", refs[2], seq_end=5)
    pinned = channels.reclaim_segments(state, committed=image({"r0": 9, "r1": 9},
                                                              confirmed=(refs[2],)), store=store)
    assert pinned == () and store.inspect(refs[2])
    # repeated crash cycles never accumulate orphans
    for cycle in range(3):
        records = (channels.encode_record(make_record({"c": cycle}, sequence=20 + cycle),
                                          bounds=bounds),)
        ref = channels.publish_segment(records, spec=spec, store=store,
                                       operation_id="orphan-%d" % cycle)
        state.register_segment("ch-a", ref, seq_end=20 + cycle)
        channels.reclaim_segments(state, committed=image({"r0": 30, "r1": 30},
                                                         confirmed=(refs[2],)), store=store)
    assert len(state.published_segments("ch-a")) == 1  # only the pinned segment remains


# -- X011 close_channels --------------------------------------------------------

def test_x011(monkeypatch):
    services = make_services()
    specs = tuple(make_spec("ch-%03d" % i) for i in range(8))
    state = channels.open_channels(specs, services=services)
    bounds = make_bounds()
    for spec in specs:
        state.enqueue(spec.channel_id, state.issue_sequence(spec.channel_id),
                      channels.encode_record(make_record({"x": 1}), bounds=bounds))
        state.reserve(spec.channel_id, "r0", rows=1, size_bytes=10,
                      operation_id="op-" + spec.channel_id)
    clock = {"now": 0.0}
    monkeypatch.setattr(channels.time, "monotonic", lambda: clock["now"])
    original_drain = ChannelSetState.drain_buffer
    def slow_drain(self, channel_id):
        clock["now"] += 1.0  # each drain burns the whole 1ms deadline
        return original_drain(self, channel_id)
    monkeypatch.setattr(ChannelSetState, "drain_buffer", slow_drain)
    channels.close_channels(state, reason="terminate", deadline_ms=1)
    assert state.closed
    assert state.reader_tombstone("ch-000", "r0") == "terminate"
    expect_error("FENCED", state.issue_sequence, "ch-007")  # late data cannot revive
    assert state.outstanding_leases()  # undrained channels keep unknown ownership
    channels.close_channels(state, reason="again", deadline_ms=1)  # idempotent
    expect_error("INVALID_CONTRACT", channels.close_channels, state, reason="x", deadline_ms=0)
    # a clean close within the deadline returns confirmed leases
    clean = channels.open_channels((make_spec("ch-clean"),), services=services)
    channels.close_channels(clean, reason="done", deadline_ms=1000)
    assert clean.closed and clean.outstanding_leases() == ()


# -- X012 to_channel_plan_v1 ----------------------------------------------------

def test_x012():
    spec = make_spec("ch-a", mode="finite_sealed", storage_bytes=4096, scope_id="scope-1",
                     consumers=("r0", "r1"), producers=("p0", "p1"), routing="broadcast")
    plan = bridge.to_channel_plan_v1(spec)
    assert set(plan) == {"channel_id", "edge_id", "exchange_mode", "routing", "keys",
                         "producer_partitions", "consumer_partitions", "route_epoch",
                         "credit_budget", "storage_bytes", "read_domain", "scope_id", "reason"}
    assert plan["consumer_partitions"] == ["r0", "r1"]  # reader set preserved
    assert plan["producer_partitions"] == ["p0", "p1"]  # scope membership preserved
    assert plan["exchange_mode"] == "finite_sealed" and plan["scope_id"] == "scope-1"
    assert plan["credit_budget"] == {"rows": 64, "bytes": 65536}
    document(plan)  # strict Json, no extra fields for the v1 validators
    expect_error("INVALID_CONTRACT", bridge.to_channel_plan_v1,
                 make_spec(mode="finite_sealed", storage_bytes=0, scope_id="scope-1"))
    expect_error("INVALID_CONTRACT", bridge.to_channel_plan_v1,
                 make_spec(mode="finite_sealed", storage_bytes=4096, scope_id=None))
    expect_error("INVALID_CONTRACT", bridge.to_channel_plan_v1, make_spec(credit_bytes=0))
    expect_error("INVALID_CONTRACT", bridge.to_channel_plan_v1, make_spec(consumers=()))
    expect_error("INVALID_CONTRACT", bridge.to_channel_plan_v1,
                 make_spec(routing="hash", keys=()))


# -- X013 artifact_to_data_ref --------------------------------------------------

def test_x013():
    store = FakeStore()
    ref = store.put(iter((b"payload",)), max_bytes=7, media_type=bridge.RECORD_MEDIA_TYPE)
    value = bridge.artifact_to_data_ref(ref, reader_domain="dom-x", owner="ch-a")
    validate_data_ref(value)
    assert value["kind"] == "object_store" and value["durability"] == "durable"
    assert value["location"] == ref.key and value["sha256"] == ref.digest
    assert value["bytes"] == ref.size_bytes and value["scope_ref"] == "ch-a"
    check_read_domain(value, "dom-x")
    with pytest.raises(ProtocolError):
        check_read_domain(value, "dom-other")  # cross-domain rejected
    # an ArtifactRef is not a v1 DataRef: missing frozen keys
    assert not {"schema_version", "ref_id", "read_domain"} <= set(ref.__dataclass_fields__)
    expect_error("INVALID_CONTRACT", bridge.artifact_to_data_ref,
                 dataclasses.replace(ref, size_bytes=-1), reader_domain="dom-x", owner="ch-a")
    for key in ("/abs/path", "a/../b", "k?query=1", "https://host/k", "user@host:k"):
        expect_error("INVALID_CONTRACT", bridge.artifact_to_data_ref,
                     dataclasses.replace(ref, key=key), reader_domain="dom-x", owner="ch-a")
    expect_error("INVALID_CONTRACT", bridge.artifact_to_data_ref,
                 dataclasses.replace(ref, media_type="application/x-python-pickle"),
                 reader_domain="dom-x", owner="ch-a")


# -- X014 record_to_envelope ----------------------------------------------------

def test_x014():
    from demiflow.execution.contracts.exchange import validate_data_envelope
    record = make_record({"v": 1}, sequence=0)
    envelope = bridge.record_to_envelope(record, channel_id="ch-a", route_epoch=0, sequence=0)
    validate_data_envelope(envelope)
    assert envelope["bounds"] == {"rows": 1, "bytes": record.encoded_bytes}
    assert envelope["records"][0]["size_bytes"] == record.encoded_bytes
    payload = envelope["records"][0]["payload"]
    assert base64.b64decode(payload["data"]) == bridge._encode_payload(record.inline, max_bytes=1 << 20)
    # data and control share one sequence space: the next issue is the barrier slot
    state = channels.open_channels((make_spec("ch-a"),), services=make_services())
    seq_data = state.issue_sequence("ch-a")
    seq_barrier = state.issue_sequence("ch-a")
    assert seq_barrier == seq_data + 1
    bridge.record_to_envelope(record, channel_id="ch-a", route_epoch=0, sequence=seq_data)
    barrier = bridge.cut_to_barrier(make_cut(sequence=seq_barrier), channel_id="ch-a",
                                    sequence=seq_barrier)
    assert barrier["sequence"] == seq_barrier
    # size underreport cannot pass the framing gate
    lying = Record(record_id=record.record_id, source_position={}, payload_ref=None,
                   inline={"v": 1}, encoded_bytes=1, sequence=0)
    expect_error("INVALID_CONTRACT", bridge.record_to_envelope, lying, channel_id="ch-a",
                 route_epoch=0, sequence=0)
    store = FakeStore()
    ref_record = make_ref_record({"big": "z" * 64}, store)
    ref_env = bridge.record_to_envelope(ref_record, channel_id="ch-a", route_epoch=0, sequence=1)
    validate_data_envelope(ref_env)
    assert ref_env["records"][0]["payload"]["ref"]["digest"] == ref_record.payload_ref.digest


# -- X015 cut_to_barrier --------------------------------------------------------

def test_x015():
    request = make_cut("cut-9", sequence=5, plan_revision=3, route_epoch=2)
    barrier = bridge.cut_to_barrier(request, channel_id="ch-a", sequence=5)
    assert set(barrier) == {"channel_id", "sequence", "checkpoint_id", "epoch", "cut",
                            "topology_revision"}  # no final/participants: D owns the cut
    assert barrier["checkpoint_id"] == "cut-9" and barrier["epoch"] == 2
    assert barrier["cut"]["producer_sequence_bound"] == 5
    assert barrier["topology_revision"] == 3
    gate = InputGate(["ch-a", "ch-b"])
    gate.push("ch-a", {"kind": "data", "sequence": 0})
    gate.push("ch-a", {"kind": "barrier", **barrier})
    gate.push("ch-a", {"kind": "data", "sequence": 6})  # post-cut held behind the barrier
    gate.push("ch-b", {"kind": "barrier", **bridge.cut_to_barrier(request, channel_id="ch-b",
                                                                  sequence=5)})
    with pytest.raises(ProtocolError):
        gate.barrier_effect("ch-a")  # pre-cut datum not consumed: BARRIER_OVERTAKE
    assert not gate.aligned("cut-9")
    gate.consume("ch-a")
    assert gate.aligned("cut-9")  # empty ch-b aligns on the same cut
    gate.release("cut-9")
    assert gate.consume("ch-a")["sequence"] == 6  # post-cut released only after confirmation
    # post-cut data arriving before its barrier is a sequence violation
    gate2 = InputGate(["ch-a"])
    gate2.push("ch-a", {"kind": "data", "sequence": 6})
    with pytest.raises(ProtocolError):
        gate2.push("ch-a", {"kind": "barrier", **barrier})


# -- X016 ChannelSetState -------------------------------------------------------

def test_x016():
    bounds = make_bounds(max_edges=2, max_partitions=2)
    expect_error("BUDGET_EXCEEDED", ChannelSetState,
                 (make_spec("ch-a"), make_spec("ch-b"), make_spec("ch-c")), bounds=bounds)
    expect_error("BUDGET_EXCEEDED", ChannelSetState,
                 (make_spec("ch-a", consumers=("r0", "r1", "r2")),), bounds=bounds)
    expect_error("INVALID_CONTRACT", ChannelSetState,
                 (make_spec("ch-a"), make_spec("ch-a")), bounds=bounds)
    state = ChannelSetState((make_spec("ch-a"),), bounds=make_bounds())
    doc = state.snapshot_document("ch-a")
    assert doc["schema"] == "demiflow.channel_state.v3"
    document(doc)  # plain Json snapshot, no live carriers
    # live index stays bounded: dropped entries leave immediately
    store = FakeStore()
    ref = store.put(iter((b"x",)), max_bytes=1, media_type=bridge.SEGMENT_MEDIA_TYPE)
    state.register_segment("ch-a", ref, seq_end=0)
    assert len(state.published_segments("ch-a")) == 1
    state.drop_segment("ch-a", ref.digest)
    assert state.published_segments("ch-a") == {}


# -- X017 close -----------------------------------------------------------------

def test_x017():
    state = ChannelSetState((make_spec("ch-a"),), bounds=make_bounds())
    closed = []
    class Carrier:
        def __init__(self, name, fail=False):
            self.name = name
            self.fail = fail
        def close(self):
            closed.append(self.name)
            if self.fail:
                raise RuntimeError("carrier " + self.name + " failed")
    state.add_carrier(Carrier("a", fail=True))
    state.add_carrier(Carrier("b"))
    state.add_carrier(Carrier("c"))
    with pytest.raises(RuntimeError):
        state.close()
    assert closed == ["a", "b", "c"]  # one failure never skips the others
    state.close()  # idempotent
    assert closed == ["a", "b", "c"]
    expect_error("FENCED", state.issue_sequence, "ch-a")
    # drained leases are returned; undrained data keeps unknown ownership
    drained = ChannelSetState((make_spec("ch-a"),), bounds=make_bounds())
    lease = drained.reserve("ch-a", "r0", rows=1, size_bytes=10, operation_id="op-d")
    drained.close()
    assert drained.outstanding_leases() == ()
    assert drained.reader_credit("ch-a", "r0")["bytes_left"] == drained.reader_credit("ch-a", "r0")["bytes_total"]
    held = ChannelSetState((make_spec("ch-a"),), bounds=make_bounds())
    held.reserve("ch-a", "r0", rows=1, size_bytes=10, operation_id="op-h")
    held.enqueue("ch-a", held.issue_sequence("ch-a"),
                 channels.encode_record(make_record({"x": 1}), bounds=make_bounds()))
    held.close()
    assert len(held.outstanding_leases()) == 1  # not fake-released


# -- X018 RoutingCounters -------------------------------------------------------

def test_x018():
    values = {"ch-a": 5}
    counters = RoutingCounters(values=values, route_epoch=2)
    values["ch-a"] = 999  # caller mutation cannot touch the ledger
    assert counters.values() == {"ch-a": 5}
    assert counters.route_epoch == 2
    assert counters.next_ordinal("ch-a") == 5
    assert counters.next_ordinal("ch-a") == 6
    assert counters.next_ordinal("ch-b") == 0
    expect_error("INVALID_CONTRACT", RoutingCounters, values={}, route_epoch=-1)
    expect_error("INVALID_CONTRACT", RoutingCounters, values={"ch-a": -1}, route_epoch=0)
    state = ChannelSetState((make_spec("ch-a", route_epoch=1),), bounds=make_bounds())
    expect_error("FENCED", state.bind_routing, RoutingCounters(values={}, route_epoch=2))
    state.bind_routing(RoutingCounters(values={}, route_epoch=1))
    expect_error("INVALID_CONTRACT", state.bind_routing, RoutingCounters(values={}, route_epoch=1))


# -- X019 next_ordinal ----------------------------------------------------------

def test_x019():
    counters = RoutingCounters(values={}, route_epoch=0)
    results = []
    lock = threading.Lock()
    def worker():
        local = [counters.next_ordinal("ch-a") for _ in range(200)]
        with lock:
            results.extend(local)
    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(results) == list(range(1600))  # concurrent sends never duplicate
    resumed = RoutingCounters(values={"ch-a": 1600}, route_epoch=0)
    assert resumed.next_ordinal("ch-a") == 1600  # resume never resets
    assert resumed.next_ordinal("ch-a") == 1601
    state = ChannelSetState((make_spec("ch-a"),), bounds=make_bounds())
    state.bind_routing(resumed)
    assert state.snapshot_document("ch-a")["routing_ordinal"] == 1602