"""Lane K unit-contract gate G-K: tests/test_runtime_v3_k.py (K001-K022).

The frozen C contracts (demiflow.execution.contracts.dataflow / dataflow_errors)
are owned by lane C and may not be landed in this worktree yet. Per the dispatch
rule, ONLY the test domain installs spec-bound fakes for those missing modules;
production kernel code imports the real paths with no fallback. The fake module
objects are given this tree-resident test file as their __file__ so the IL012
in-process tree-binding assertion still holds for every loaded demiflow module.
"""
from __future__ import annotations

import hashlib
import math
import pickle
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from demiflow.operators import ordering as algorithm_ordering

TREE = Path(__file__).resolve().parents[1]
_THIS_FILE = str(Path(__file__).resolve())


# ---------------------------------------------------------------------------
# Spec-bound frozen contract fakes (test domain only; interfaces.pyi parity).
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ArtifactRef:
    digest: str
    size_bytes: int
    store_id: str
    key: str
    media_type: str


@dataclass(frozen=True)
class Bounds:
    max_nodes: int = 4096
    max_edges: int = 16384
    max_partitions: int = 4096
    max_depth: int = 64
    max_control_bytes: int = 1 << 20
    max_row_bytes: int = 1 << 20
    max_batch_bytes: int = 4 << 20
    max_resident_bytes: int = 8 << 20
    max_spill_bytes: int = 64 << 20
    max_state_bytes: int = 8 << 20
    max_pending_events: int = 1024
    max_open_files: int = 64
    max_output_rows: int = 10 ** 9
    timeout_ms: int = 600000


@dataclass(frozen=True)
class InputPort:
    name: str
    requirement: str
    ordering: str = "none"
    schema_ref: Any = None


@dataclass(frozen=True)
class ResourceProfile:
    requests: dict = field(default_factory=lambda: {"cpu_millis": 1000})
    limits: Any = None
    requirements: Any = None


@dataclass(frozen=True)
class KernelSpec:
    kind: str
    implementation_ref: Any = None
    configuration: dict = field(default_factory=dict)
    inputs: tuple = ()
    state: str = "none"
    effect: str = "pure"
    resource_role: str = "cpu"
    profile: ResourceProfile = field(default_factory=ResourceProfile)
    output_bound_bytes: int = 10 ** 9
    mergeable: bool = True


@dataclass(frozen=True)
class Record:
    record_id: str
    source_position: dict
    payload_ref: Any
    inline: Any
    encoded_bytes: int
    sequence: int


@dataclass(frozen=True)
class ExchangeSpec:
    channel_id: str
    edge_id: str
    producer_partitions: tuple
    consumer_partitions: tuple
    mode: str = "memory_incremental"
    routing: str = "forward"
    keys: tuple = ()
    scope_id: Any = None
    route_epoch: int = 0
    credit_rows: int = 1024
    credit_bytes: int = 1 << 20
    storage_bytes: int = 1 << 20
    read_domain: str = "local"
    reason: str = ""


@dataclass(frozen=True)
class RoutedRecord:
    reader_id: str
    delivery_id: str
    record: Record


@dataclass(frozen=True)
class AggregateState:
    kind: str
    schema_version: str
    state_ref: ArtifactRef
    count: int


@dataclass(frozen=True)
class TerminalResult:
    kind: str
    rows: Any
    scalar: Any
    result_ref: Any
    write_result: Any
    complete: bool
    completion: str


@dataclass(frozen=True)
class LoadedProgram:
    kind: str
    value: Any
    digest: str
    load_private: Any = None


class DataflowError(Exception):
    def __init__(self, code, message, *, retryable=False, context=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.context = dict(context or {})


# Declared test doubles: lane C owns the real contracts (delivered separately).
# These spec-bound fakes stand in ONLY because lane C is not landed in this lane's
# worktree. They are explicitly flagged so the evidence report and the IL012
# tree-binding assertion can distinguish them from real tree-resident modules;
# their synthetic __file__ is never reported as a real file binding.
TEST_DOUBLES = frozenset({
    "demiflow.execution.contracts.dataflow",
    "demiflow.execution.contracts.dataflow_errors",
})


def _real_module_available(name):
    import importlib
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def _install_fake_contracts() -> None:
    if all(_real_module_available(n) for n in TEST_DOUBLES):
        return
    dataflow = types.ModuleType("demiflow.execution.contracts.dataflow")
    dataflow.__file__ = _THIS_FILE
    dataflow.__test_double__ = True
    for name in (
        "ArtifactRef", "Bounds", "InputPort", "ResourceProfile", "KernelSpec",
        "Record", "ExchangeSpec", "RoutedRecord", "AggregateState", "TerminalResult",
        "LoadedProgram",
    ):
        setattr(dataflow, name, globals()[name])
    dataflow.CallableInstance = object
    dataflow.Kernel = object
    dataflow.RuntimeServices = object
    errors = types.ModuleType("demiflow.execution.contracts.dataflow_errors")
    errors.__file__ = _THIS_FILE
    errors.__test_double__ = True
    errors.DataflowError = DataflowError
    sys.modules["demiflow.execution.contracts.dataflow"] = dataflow
    sys.modules["demiflow.execution.contracts.dataflow_errors"] = errors


_install_fake_contracts()

# Integration runs use the actual immutable wire values; helpers below only
# supply test defaults, never alternate DTO implementations.
_BoundsDefaults, _ExchangeDefaults = Bounds, ExchangeSpec
from demiflow.execution.contracts import dataflow as real_dto
from demiflow.resources.contracts import ResourceProfile
ArtifactRef = real_dto.ArtifactRef
Record = real_dto.Record
LoadedProgram = real_dto.LoadedProgram
RoutedRecord = real_dto.RoutedRecord
AggregateState = real_dto.AggregateState
TerminalResult = real_dto.TerminalResult

def Bounds(**overrides):
    from dataclasses import asdict
    return real_dto.Bounds(**asdict(_BoundsDefaults(**overrides)))

def InputPort(name, requirement, ordering="none", schema_ref=None):
    return real_dto.InputPort(name, requirement, ordering, schema_ref)

def ExchangeSpec(*args, **kwargs):
    from dataclasses import asdict
    values = asdict(_ExchangeDefaults(*args, **kwargs))
    values['edge_id'] = hashlib.sha256(values['edge_id'].encode()).hexdigest()
    values['reason'] = 'unit routing proof'
    return real_dto.ExchangeSpec(**values)

from demiflow.execution.kernels import dataflow as kdf  # noqa: E402
from demiflow.execution.kernels import ordering as ko  # noqa: E402
from demiflow.execution.kernels import relational as kr  # noqa: E402
from demiflow.execution.kernels import aggregate as ka  # noqa: E402
from demiflow.execution.kernels.operator import OperatorKernel  # noqa: E402
from demiflow.data.aggregate import AggregateFnV2  # noqa: E402


# ---------------------------------------------------------------------------
# In-process tree binding (IL012): real demiflow code must come from this tree.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _bind_tree():
    import demiflow

    assert Path(demiflow.__file__).resolve().is_relative_to(TREE), demiflow.__file__
    for name, module in list(sys.modules.items()):
        if name != "demiflow" and not name.startswith("demiflow."):
            continue
        if getattr(module, "__test_double__", False):
            # Declared substitute: must be in the declared set; its synthetic
            # __file__ is never asserted as a real tree-resident file binding.
            assert name in TEST_DOUBLES, ("undeclared test double", name)
            continue
        path = getattr(module, "__file__", None)
        if path is None:
            continue
        assert Path(path).resolve().is_relative_to(TREE), (name, path)
    yield


# ---------------------------------------------------------------------------
# Fake store / services / record helpers (test domain).
# ---------------------------------------------------------------------------
class FakeStore:
    def __init__(self):
        self.objects = {}
        self.released = []
        self.retained = []
        self._n = 0

    def scratch(self, *, max_bytes, max_io_bytes):
        # Kernel-unit scratch adapter; real RunJournal ownership/recovery is
        # exercised separately by test_runtime_v3_join_spool.
        import io
        class Scratch:
            def __init__(self):
                self.file = io.BytesIO()
                self.size = 0
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.file.close()
            def write_at(self, offset, raw):
                assert offset <= self.size and len(raw) <= max_io_bytes
                if offset + len(raw) > max_bytes:
                    raise DataflowError('BUDGET_EXCEEDED', 'scratch budget')
                self.file.seek(offset)
                self.file.write(raw)
                self.size = max(self.size, offset + len(raw))
            def append(self, raw):
                offset = self.size
                self.write_at(offset, raw)
                return offset
            def read_at(self, offset, count):
                assert count <= max_io_bytes and offset + count <= self.size
                self.file.seek(offset)
                return self.file.read(count)
        return Scratch()

    def put(self, chunks, *, max_bytes, media_type):
        buf = bytearray()
        for chunk in chunks:
            buf.extend(chunk)
            if len(buf) > max_bytes:
                raise DataflowError("BUDGET_EXCEEDED", "store put exceeds max_bytes")
        data = bytes(buf)
        digest = hashlib.sha256(data).hexdigest()
        self._n += 1
        key = f"obj-{self._n}-{digest[:12]}"
        self.objects[key] = data
        return ArtifactRef(digest, len(data), "fake", key, media_type)

    def read(self, ref, *, max_bytes):
        data = self.objects[ref.key]
        if hashlib.sha256(data).hexdigest() != ref.digest:
            raise DataflowError("CORRUPT_ARTIFACT", "digest mismatch")
        if len(data) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "read exceeds max_bytes")
        yield data

    def read_into(self, ref, destination):
        destination[:] = next(self.read(ref, max_bytes=len(destination)))

    def inspect(self, ref):
        return ref.key in self.objects

    def retain(self, ref, *, owner):
        self.retained.append((ref.key, owner))

    def release(self, ref, *, owner):
        self.released.append((ref.key, owner))
        self.objects.pop(ref.key, None)


class FakeServices:
    def __init__(self, store=None, bounds=None, programs=None, constants=None):
        self.store = store if store is not None else FakeStore()
        self.bounds = bounds if bounds is not None else Bounds()
        self.programs = dict(programs or {})
        self.constants = dict(constants or {"random_seed": 12345})
        self.journal = object()
        # These unit kernels have no compiler-selected SQL stage. The live
        # runtime supplies this same stages interface through WorkerAssignment.
        self.assignment = types.SimpleNamespace(stages=())
        self._clock = [0]
        self.stopped = False

    def clock_ms(self):
        self._clock[0] += 1
        return self._clock[0]

    def check_stop(self):
        if self.stopped:
            raise DataflowError("CANCELLED", "stop requested")


def services(**kw):
    return FakeServices(**kw)


def spec(kind, configuration=None, inputs=None, **kw):
    cfg = {"occurrence_id": "occ-" + kind}
    cfg.update(configuration or {})
    values = dict(implementation_ref=None, state="none", effect="pure", resource_role="cpu",
                  profile=ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": 8 << 20}),
                  output_bound_bytes=10 ** 9, mergeable=True)
    values.update(kw)
    return real_dto.KernelSpec(kind=kind, configuration=cfg, inputs=tuple(inputs or ()), **values)


def rid(label):
    # Real C contracts require Record.record_id to be sha256 hex; map a stable
    # human label to a valid id so these tests stay valid under the real DTOs.
    return hashlib.sha256(str(label).encode()).hexdigest()


def rec(row, record_id=None, sequence=0, position=None):
    from demiflow.execution.contracts.record_codec import _encode_payload
    data = _encode_payload(dict(row), max_bytes=1 << 24)
    return Record(
        record_id=record_id or rid(repr((row, sequence))),
        source_position=dict(position or {"split_id": "s0", "index": sequence}),
        payload_ref=None,
        inline=dict(row),
        encoded_bytes=len(data),
        sequence=sequence,
    )


def rows_of(records):
    return [real_dto.thaw_record_value(r.inline) for r in records]


def program(value, kind="callable"):
    import cloudpickle
    return LoadedProgram(kind, value, hashlib.sha256(repr(value).encode()).hexdigest(),
                         load_private=lambda: cloudpickle.loads(cloudpickle.dumps(value)))


# ---------------------------------------------------------------------------
# K001 make_kernel
# ---------------------------------------------------------------------------
def test_k001():
    svc = services()
    for kind in ("MapOp", "join", "reduce", "groups", "union", "SortOp", "aggregate", "RepartitionOp"):
        kernel = kdf.make_kernel(spec(kind), services=svc, partition_id="p0")
        assert isinstance(kernel, OperatorKernel), kind
        assert kernel._instance is None  # construction performs no execution
    with pytest.raises(DataflowError) as exc:
        kdf.make_kernel(spec("NoSuchOp"), services=svc, partition_id="p0")
    assert exc.value.code == "INVALID_CONTRACT"
    # A callable kind with no staged program cannot masquerade as an implementation.
    kernel = kdf.make_kernel(spec("MapOp"), services=svc, partition_id="p0")
    with pytest.raises(DataflowError) as exc:
        kernel.start()
    assert exc.value.code == "INVALID_CONTRACT"


# ---------------------------------------------------------------------------
# K002 invoke_unary
# ---------------------------------------------------------------------------
def test_k002():
    svc = services()
    s = spec("MapOp")
    out = list(kdf.invoke_unary(s, rec({"v": 1}), instance=lambda r: {"v": r["v"] + 1}, services=svc))
    assert rows_of(out) == [{"v": 2}]

    s = spec("FilterOp")
    assert list(kdf.invoke_unary(s, rec({"v": 1}), instance=lambda r: r["v"] > 0, services=svc))
    assert not list(kdf.invoke_unary(s, rec({"v": 0}), instance=lambda r: r["v"] > 0, services=svc))

    s = spec("FlatMapOp")
    out = list(kdf.invoke_unary(s, rec({"v": 2}, record_id=rid("parent")),
                                instance=lambda r: [{"i": i} for i in range(r["v"])], services=svc))
    assert rows_of(out) == [{"i": 0}, {"i": 1}]
    assert out[0].record_id == kdf._hash_id([kdf.RECORD_DOMAIN, rid("parent"), "occ-FlatMapOp", 0])

    # flat_map amplification fails on the output budget, never truncates.
    s = spec("FlatMapOp", output_bound_bytes=10 ** 9)
    svc_small = services(bounds=Bounds(max_output_rows=2))
    with pytest.raises(DataflowError) as exc:
        list(kdf.invoke_unary(s, rec({"v": 5}), instance=lambda r: [{"i": i} for i in range(r["v"])], services=svc_small))
    assert exc.value.code == "BUDGET_EXCEEDED"

    s = spec("DropColumnsOp", {"columns": ["missing"]})
    with pytest.raises(DataflowError) as exc:
        list(kdf.invoke_unary(s, rec({"v": 1}), instance=None, services=svc))
    assert exc.value.code == "INVALID_CONTRACT"


# ---------------------------------------------------------------------------
# K003 invoke_batch
# ---------------------------------------------------------------------------
def test_k003():
    svc = services()
    records = (rec({"v": 1}, sequence=0), rec({"v": 2}, sequence=1))
    s = spec("MapBatchesOp", {"batch_format": "default"})
    out = list(kdf.invoke_batch(s, records, instance=lambda b: [{"v": v * 10} for v in b["v"]], services=svc))
    assert rows_of(out) == [{"v": 10}, {"v": 20}]
    assert out[0].record_id == records[0].record_id  # positional provenance

    # Arrow/pandas cast keeps source rows alive alongside converted buffers.
    s = spec("MapBatchesOp", {"batch_format": "pyarrow"})
    svc_tight = services(bounds=Bounds(max_batch_bytes=8))
    with pytest.raises(DataflowError) as exc:
        list(kdf.invoke_batch(s, records, instance=lambda b: b, services=svc_tight))
    assert exc.value.code == "BUDGET_EXCEEDED"

    s = spec("BatchMapOp")
    out = list(kdf.invoke_batch(s, records, instance=lambda b: [{"sum": sum(r["v"] for r in b)}], services=svc))
    assert rows_of(out) == [{"sum": 3}]


# ---------------------------------------------------------------------------
# K004 finish_kernel
# ---------------------------------------------------------------------------
def test_k004():
    svc = services()

    class Dummy:
        def finish(self, port):
            yield rec({"port": port})

    out = list(kdf.finish_kernel(Dummy(), port="left", services=svc))
    assert rows_of(out) == [{"port": "left"}]
    svc.stopped = True
    with pytest.raises(DataflowError) as exc:
        list(kdf.finish_kernel(Dummy(), port="left", services=svc))
    assert exc.value.code == "CANCELLED"


# ---------------------------------------------------------------------------
# K005 join_partitions
# ---------------------------------------------------------------------------
def test_k005():
    svc = services()
    left = [rec({"k": "a", "lv": 1}, sequence=0), rec({"k": None, "lv": 9}, sequence=1),
            rec({"k": "a", "lv": 2}, sequence=2)]
    right = [rec({"k": "a", "rv": 10}, sequence=0), rec({"k": "a", "rv": 20}, sequence=1),
             rec({"k": None, "rv": 99}, sequence=2)]

    def join(how, left=left, right=right, **cfgkw):
        s = spec("join", {"on": ["k"], "how": how, **cfgkw})
        return rows_of(kr.join_partitions(iter(left), iter(right), spec=s, services=svc))

    inner = join("inner")
    assert len(inner) == 4  # duplicate-key cartesian multiplicity, no dedup
    assert all(r["k"] == "a" for r in inner)
    left_join = join("left")
    assert any(r["lv"] == 9 and "rv" not in r for r in left_join)  # null key unmatched
    assert len(join("semi")) == 2
    assert rows_of(kr.join_partitions(iter(left), iter(right), spec=spec("join", {"on": ["k"], "how": "anti"}), services=svc)) == [{"k": None, "lv": 9}]

    # Exact typed equality: int 1 does not match float 1.0.
    typed = join("inner", left=[rec({"k": 1})], right=[rec({"k": 1.0})])
    assert typed == []

    # Empty side is valid: inner yields nothing, left keeps unmatched left rows.
    assert join("inner", left=[rec({"k": "a"})], right=[]) == []
    assert join("left", left=[rec({"k": "a", "lv": 1})], right=[]) == [{"k": "a", "lv": 1}]

    # Skew cartesian amplification fails on the output budget.
    big_left = [rec({"k": "hot", "i": i}, sequence=i) for i in range(20)]
    big_right = [rec({"k": "hot", "j": j}, sequence=j) for j in range(20)]
    s = spec("join", {"on": ["k"], "how": "inner"})
    svc_small = services(bounds=Bounds(max_output_rows=10))
    with pytest.raises(DataflowError) as exc:
        list(kr.join_partitions(iter(big_left), iter(big_right), spec=s, services=svc_small))
    assert exc.value.code == "BUDGET_EXCEEDED"


# ---------------------------------------------------------------------------
# K006 reduce_partition
# ---------------------------------------------------------------------------
def test_k006():
    # Non-associative subtraction folds in provenance order: ((0-1)-2)-3 = -6.
    reducer = lambda state, row: {"acc": (state["acc"] if state else 0) - row["v"]}
    svc = services(programs={"sub": program(reducer)})
    s = spec("reduce", {"on": ["g"], "reducer_program": "sub"})
    records = [rec({"g": "x", "v": 1}, sequence=0), rec({"g": "x", "v": 2}, sequence=1),
               rec({"g": "x", "v": 3}, sequence=2)]
    out = rows_of(kr.reduce_partition(iter(records), spec=s, services=svc))
    assert out == [{"acc": -6}]
    # A single-stream fold of the same rows gives the identical result.
    state = None
    for row in rows_of(records):
        state = reducer(state, row)
    assert state == {"acc": -6}


# ---------------------------------------------------------------------------
# K007 union_inputs
# ---------------------------------------------------------------------------
def test_k007():
    svc = services()
    shared = [rec({"v": 1}, record_id=rid("r1")), rec({"v": 2}, record_id=rid("r2"))]
    s = spec("union")
    # The same upstream fed to two ports: source executed once, output twice.
    out = list(kr.union_inputs((iter(shared), iter(shared)), spec=s, services=svc))
    assert len(out) == 4
    assert [r.record_id for r in out] == [rid("r1"), rid("r2"), rid("r1"), rid("r2")]


# ---------------------------------------------------------------------------
# K008 emit_groups
# ---------------------------------------------------------------------------
def test_k008():
    svc = services()
    s = spec("groups", {"on": ["k"], "max_rows": 2, "output": "items"})
    records = [rec({"k": -0.0, "v": 1}, sequence=0), rec({"k": 0.0, "v": 2}, sequence=1),
               rec({"k": b"x", "v": 3}, sequence=2), rec({"k": -0.0, "v": 4}, sequence=3)]
    out = rows_of(kr.emit_groups(iter(records), spec=s, services=svc))
    # -0.0 and 0.0 are distinct keys; bytes key not folded to str.
    keys = [g["k"] for g in out]
    assert any(isinstance(k, float) and math.copysign(1, k) < 0 for k in keys)
    assert any(k == b"x" for k in keys)
    neg_zero = [g for g in out if isinstance(g["k"], float) and math.copysign(1, g["k"]) < 0]
    assert neg_zero[0]["items"] == [{"k": -0.0, "v": 1}, {"k": -0.0, "v": 4}]
    assert neg_zero[0]["group_last"] is True


# ---------------------------------------------------------------------------
# K009 build_sort_runs
# ---------------------------------------------------------------------------
def test_k009():
    svc = services(bounds=Bounds(max_resident_bytes=1024))
    s = spec("SortOp", {"keys": ["k"], "descending": False})
    records = [rec({"k": 3}, sequence=0), rec({"k": 1}, sequence=1), rec({"k": 2}, sequence=2),
               rec({"k": None}, sequence=3)]
    runs = ko.build_sort_runs(iter(records), spec=s, services=svc)
    assert len(runs) >= 2  # bounded budget forces multiple runs
    merged = rows_of(ko.merge_sort_runs(runs, spec=s, services=svc))
    assert [r["k"] for r in merged] == [1, 2, 3, None]  # nulls last

    # Single row over max_row_bytes is rejected, not truncated.
    svc_tight = services(bounds=Bounds(max_row_bytes=4))
    with pytest.raises(DataflowError) as exc:
        ko.build_sort_runs(iter([rec({"k": "wide-row"})]), spec=s, services=svc_tight)
    assert exc.value.code == "BUDGET_EXCEEDED"


# ---------------------------------------------------------------------------
# K010 merge_sort_runs
# ---------------------------------------------------------------------------
def test_k010():
    store = FakeStore()
    svc = services(store=store, bounds=Bounds(max_resident_bytes=1024, max_open_files=2))
    s = spec("SortOp", {"keys": ["k"]})
    records = [rec({"k": i}, sequence=i) for i in range(24, -1, -1)]
    runs = ko.build_sort_runs(iter(records), spec=s, services=svc)
    assert len(runs) > 2  # forces multi-pass with fan_in=2
    merged = rows_of(ko.merge_sort_runs(runs, spec=s, services=svc))
    assert [r["k"] for r in merged] == list(range(25))
    # Caller-owned input runs survive; owned intermediates are released.
    assert all(store.inspect(r) for r in runs)
    assert store.released
    assert ko.merge_sort_runs((), spec=s, services=svc) is not None
    assert list(ko.merge_sort_runs((), spec=s, services=svc)) == []


# ---------------------------------------------------------------------------
# K011 shuffle_key
# ---------------------------------------------------------------------------
def test_k011():
    a = algorithm_ordering.shuffle_key("rec-1", seed=7, occurrence_id="occ")
    b = algorithm_ordering.shuffle_key("rec-1", seed=7, occurrence_id="occ")
    assert a == b and isinstance(a, bytes) and len(a) == 32
    assert algorithm_ordering.shuffle_key("rec-1", seed=8, occurrence_id="occ") != a
    assert algorithm_ordering.shuffle_key("rec-1", seed=7, occurrence_id="other") != a
    assert algorithm_ordering.shuffle_key("rec-2", seed=7, occurrence_id="occ") != a
    with pytest.raises(DataflowError):
        algorithm_ordering.shuffle_key("", seed=7, occurrence_id="occ")


# ---------------------------------------------------------------------------
# K012 repartition_records
# ---------------------------------------------------------------------------
def test_k012():
    svc = services()
    records = [rec({"k": "a", "i": i}, record_id=rid(f"r{i}"), sequence=i) for i in range(6)]

    def exchange(routing, **kw):
        return ExchangeSpec("c", "e", ("p0",), ("c0", "c1", "c2"), routing=routing, **kw)

    hashed = list(ko.repartition_records(iter(records), exchange=exchange("hash", keys=("k",)), services=svc))
    again = list(ko.repartition_records(iter(records), exchange=exchange("hash", keys=("k",)), services=svc))
    assert [r.reader_id for r in hashed] == [r.reader_id for r in again]  # stable hash

    reb = ko.repartition_records(iter(records), exchange=exchange("rebalance"), services=svc)
    first = list(reb)
    assert [r.reader_id for r in first] == ["c0", "c1", "c2", "c0", "c1", "c2"]
    assert reb.ordinal == 6
    reb.restore_ordinal(1)
    nxt = next(ko.repartition_records(iter([rec({"k": "a"}, record_id=rid("x"))]), exchange=exchange("rebalance"), services=svc))
    assert nxt.reader_id in ("c0", "c1", "c2")

    bc = list(ko.repartition_records(iter(records[:1]), exchange=exchange("broadcast"), services=svc))
    assert len(bc) == 3
    assert len({r.delivery_id for r in bc}) == 3  # per-reader delivery identity

    tgt = list(ko.repartition_records(iter([rec({"t": "c1"}, record_id=rid("t0"))]),
                                    exchange=exchange("target", keys=("t",)), services=svc))
    assert tgt[0].reader_id == "c1"


# ---------------------------------------------------------------------------
# K013 accumulate_partial
# ---------------------------------------------------------------------------
def test_k013():
    svc = services()
    s = spec("aggregate", {"aggregates": [{"builtin": "count"}]})
    state = ka.accumulate_partial(iter([]), spec=s, services=svc)
    assert state.count == 0  # empty input count 0

    s = spec("aggregate", {"aggregates": [{"builtin": "sum", "on": "v"}]})
    state = ka.accumulate_partial(iter([rec({"v": 2}), rec({"v": 3})]), spec=s, services=svc)
    assert state.count == 2

    # Oversized user state is rejected before combine copies it.
    class Huge(AggregateFnV2):
        def __init__(self):
            super().__init__("huge", lambda: 0, on="v", ignore_nulls=True)

        def aggregate_block(self, block):
            return "x" * 100000

        def combine(self, a, b):
            return a + b

    svc_tight = services(bounds=Bounds(max_state_bytes=1000), programs={"huge": program(Huge(), "aggregate")})
    s = spec("aggregate", {"aggregates": [{"program": "huge"}]})
    with pytest.raises(DataflowError) as exc:
        ka.accumulate_partial(iter([rec({"v": 1})]), spec=s, services=svc_tight)
    assert exc.value.code == "BUDGET_EXCEEDED"


# ---------------------------------------------------------------------------
# K014 merge_partials
# ---------------------------------------------------------------------------
def test_k014():
    svc = services()
    s = spec("aggregate", {"aggregates": [{"builtin": "mean", "on": "v"}, {"builtin": "std", "on": "v"}]})
    data = [rec({"v": v}, sequence=i) for i, v in enumerate([1, 2, 3, 4, 5, 6])]
    p1 = ka.accumulate_partial(iter(data[:3]), spec=s, services=svc)
    p2 = ka.accumulate_partial(iter(data[3:]), spec=s, services=svc)
    merged = ka.merge_partials(iter([p2, p1]), spec=s, services=svc)  # deterministic reorder
    assert merged.count == 6
    with ka.bind_finalize(svc):
        result = ka.finalize_aggregate(merged, spec=s)
    direct = ka.accumulate_partial(iter(data), spec=s, services=svc)
    with ka.bind_finalize(svc):
        direct_result = ka.finalize_aggregate(direct, spec=s)
    assert result.scalar["mean(v)"] == pytest.approx(direct_result.scalar["mean(v)"])
    assert result.scalar["std(v)"] == pytest.approx(direct_result.scalar["std(v)"], rel=1e-9)

    nonmerge = spec("aggregate", {"aggregates": [{"builtin": "count"}]}, mergeable=False)
    with pytest.raises(DataflowError) as exc:
        ka.merge_partials(iter([p1]), spec=nonmerge, services=svc)
    assert exc.value.code == "UNSUPPORTED_CAPABILITY"


# ---------------------------------------------------------------------------
# K015 finalize_aggregate
# ---------------------------------------------------------------------------
def test_k015():
    svc = services()
    dup = spec("aggregate", {"aggregates": [{"builtin": "count"}, {"builtin": "count"}]})
    with pytest.raises(DataflowError) as exc:
        ka.accumulate_partial(iter([]), spec=dup, services=svc)
    assert exc.value.code == "INVALID_CONTRACT"  # duplicate names rejected first

    s = spec("aggregate", {"aggregates": [{"builtin": "sum", "on": "v"}, {"builtin": "mean", "on": "v"}]})
    empty = ka.accumulate_partial(iter([]), spec=s, services=svc)
    with ka.bind_finalize(svc):
        result = ka.finalize_aggregate(empty, spec=s)
    assert result.scalar is None and result.rows == 0  # empty -> None, not fabricated zero

    allnull = ka.accumulate_partial(iter([rec({"v": None}), rec({"v": None})]), spec=s, services=svc)
    with ka.bind_finalize(svc):
        result = ka.finalize_aggregate(allnull, spec=s)
    assert result.scalar["sum(v)"] is None  # all-null not faked to zero
    assert result.scalar["mean(v)"] is None

    s = spec("aggregate", {"aggregates": [{"builtin": "sum", "on": "v"}, {"builtin": "min", "on": "v"},
                                          {"builtin": "max", "on": "v"}, {"builtin": "std", "on": "v"}]})
    state = ka.accumulate_partial(iter([rec({"v": v}) for v in [2, 4, 6]]), spec=s, services=svc)
    with ka.bind_finalize(svc):
        result = ka.finalize_aggregate(state, spec=s)
    assert result.scalar["sum(v)"] == 12
    assert result.scalar["min(v)"] == 2
    assert result.scalar["max(v)"] == 6
    assert result.scalar["std(v)"] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# K016 OperatorKernel.__init__
# ---------------------------------------------------------------------------
def test_k016():
    svc = services()
    kernel = OperatorKernel(spec("MapOp"), services=svc, partition_id="p0")
    assert kernel._instance is None and not kernel._started
    assert kernel._spec.kind == "MapOp"
    with pytest.raises(DataflowError) as exc:
        OperatorKernel(spec("Bogus"), services=svc, partition_id="p0")
    assert exc.value.code == "INVALID_CONTRACT"


# ---------------------------------------------------------------------------
# K017 start
# ---------------------------------------------------------------------------
def test_k017():
    calls = []

    class Fn:
        def __init__(self):
            self.state = {"calls": 0}

        def __call__(self, row):
            self.state["calls"] += 1
            return {"v": row["v"] + 1}

        def __demiflow_state__(self):
            return self.state

        def __demiflow_restore__(self, value):
            self.state = value

    svc = services(programs={"fn": program(Fn())})
    s = spec("MapOp", {"program": "fn"}, inputs=[InputPort("input", "incremental")], state="auto_json")
    kernel = OperatorKernel(s, services=svc, partition_id="p0")
    kernel.start()
    assert kernel._started and kernel._instance is not None
    list(kernel.push("input", rec({"v": 1})))
    assert kernel._instance.state["calls"] == 1

    # gpu role without a gpu request is rejected at admission.
    gpu_spec = spec("MapOp", {"program": "fn"}, resource_role="gpu")
    with pytest.raises(DataflowError) as exc:
        OperatorKernel(gpu_spec, services=svc, partition_id="p0").start()
    assert exc.value.code == "RESOURCE_INFEASIBLE"


# ---------------------------------------------------------------------------
# K018 push
# ---------------------------------------------------------------------------
def test_k018():
    svc = services(programs={"fn": program(lambda r: {"v": r["v"] + 1})})
    s = spec("MapOp", {"program": "fn"}, inputs=[InputPort("input", "incremental")])
    kernel = OperatorKernel(s, services=svc, partition_id="p0")
    kernel.start()
    assert rows_of(kernel.push("input", rec({"v": 1}))) == [{"v": 2}]

    # finite_complete join: right not complete must not produce a join.
    svc2 = services()
    js = spec("join", {"on": ["k"], "how": "inner"},
              inputs=[InputPort("left", "finite_complete"), InputPort("right", "finite_complete")])
    jk = OperatorKernel(js, services=svc2, partition_id="p0")
    jk.start()
    assert list(jk.push("left", rec({"k": "a", "lv": 1}))) == []
    assert list(jk.finish("left")) == []  # right still open: no join yet
    list(jk.push("right", rec({"k": "a", "rv": 2})))
    out = rows_of(jk.finish("right"))
    assert out == [{"k": "a", "lv": 1, "rv": 2}]


# ---------------------------------------------------------------------------
# K019 finish
# ---------------------------------------------------------------------------
def test_k019():
    svc = services()
    s = spec("aggregate", {"aggregates": [{"builtin": "count"}]},
             inputs=[InputPort("input", "finite_complete")])
    kernel = OperatorKernel(s, services=svc, partition_id="p0")
    kernel.start()
    assert list(kernel.finish("input")) == []
    assert kernel.aggregate_state.count == 0  # empty input count 0

    # One side EOI does not end all ports.
    js = spec("join", {"on": ["k"], "how": "inner"},
              inputs=[InputPort("left", "finite_complete"), InputPort("right", "finite_complete")])
    jk = OperatorKernel(js, services=svc, partition_id="p0")
    jk.start()
    list(jk.push("left", rec({"k": "a"})))
    assert list(jk.finish("left")) == []
    assert not jk._ports["right"].eoi


# ---------------------------------------------------------------------------
# K020 snapshot
# ---------------------------------------------------------------------------
def test_k020():
    svc = services(programs={"fn": program(lambda b: [{"v": r["v"]} for r in b])})
    s = spec("BatchMapOp", {"program": "fn", "max_batch": 4}, inputs=[InputPort("input", "incremental")])
    kernel = OperatorKernel(s, services=svc, partition_id="p0")
    kernel.start()
    list(kernel.push("input", rec({"v": 1})))
    list(kernel.push("input", rec({"v": 2})))  # partial batch (max_batch=4 not reached)
    ref = kernel.snapshot()
    assert ref is not None
    from demiflow.execution.contracts import state_codec
    image = state_codec.loads(b"".join(svc.store.read(ref, max_bytes=ref.size_bytes)), max_bytes=ref.size_bytes)
    assert len(image["batch"]) == 2  # crash must not lose the partial batch
    assert image["output_cursor"] == 0
    # The recovery image carries no live instance / DataFusion session object.
    assert kernel._instance not in image.values()


# ---------------------------------------------------------------------------
# K021 restore
# ---------------------------------------------------------------------------
def test_k021():
    svc = services(programs={"fn": program(lambda r: {"v": r["v"] + 1})})
    s = spec("MapOp", {"program": "fn"}, inputs=[InputPort("input", "incremental")], state="auto_json")
    kernel = OperatorKernel(s, services=svc, partition_id="p0")
    kernel.start()
    list(kernel.push("input", rec({"v": 1})))
    ref = kernel.snapshot()

    restored = OperatorKernel(s, services=svc, partition_id="p0")
    restored.restore(ref)
    restored.start()
    assert restored._emitted == 1  # output cursor restored

    # Stateful kernel cannot restore from an empty ref as if new.
    with pytest.raises(DataflowError) as exc:
        OperatorKernel(s, services=svc, partition_id="p0").restore(None)
    assert exc.value.code == "INCOMPATIBLE_HISTORY"

    # Route / partition change is rejected.
    other = OperatorKernel(s, services=svc, partition_id="p1")
    with pytest.raises(DataflowError) as exc:
        other.restore(ref)
    assert exc.value.code == "INCOMPATIBLE_HISTORY"


# ---------------------------------------------------------------------------
# K022 close
# ---------------------------------------------------------------------------
def test_k022():
    closed = []

    class Fn:
        def __call__(self, row):
            return row

        def close(self):
            closed.append(True)
            raise RuntimeError("business close error")

    svc = services(programs={"fn": program(Fn())})
    s = spec("MapOp", {"program": "fn"}, inputs=[InputPort("input", "incremental")])
    kernel = OperatorKernel(s, services=svc, partition_id="p0")
    kernel.start()
    with pytest.raises(RuntimeError):
        kernel.close()  # business exception preserved
    assert closed == [True]
    kernel.close()  # idempotent: no second close, no raise
    assert closed == [True]

    # finite-complete spool ownership is released on close.
    svc2 = services()
    js = spec("reduce", {"on": ["g"], "reducer_program": "sub"},
              inputs=[InputPort("input", "finite_complete")])
    svc2.programs["sub"] = program(lambda state, row: (state or 0) + row["v"])
    jk = OperatorKernel(js, services=svc2, partition_id="p0")
    jk.start()
    for i in range(50):
        list(jk.push("input", rec({"g": "x", "v": i}, sequence=i)))
    jk._ports["input"]._flush()
    refs = list(jk._ports["input"]._refs)
    jk.close()
    assert all(not svc2.store.inspect(r) for r in refs)
# Integrated lanes use the one production error vocabulary.
from demiflow.execution.contracts.dataflow_errors import DataflowError
