"""Lane S regression gate against the integrated v3 contracts.

DTO construction uses the delivered closed contracts. Adapter tests patch only
explicit cross-module call boundaries for their unit assertions. Source and
native queue tests retain their real readers, journal/HTTP/SQLite paths where
originally exercised. Carrier doubles are unit evidence, not Local/Ray or
end-to-end acceptance evidence.
"""
from __future__ import annotations

import hashlib
import pickle
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

WORKTREE = Path(__file__).resolve().parents[1]
if str(WORKTREE) not in sys.path:
    sys.path.insert(0, str(WORKTREE))

import demiflow  # noqa: E402

assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE), demiflow.__file__


# --- frozen-contract test-domain fakes (interfaces.pyi) ---------------------
@dataclass(frozen=True)
class ArtifactRef:
    digest: str
    size_bytes: int
    store_id: str
    key: str
    media_type: str


@dataclass(frozen=True)
class Bounds:
    max_nodes: int
    max_edges: int
    max_partitions: int
    max_depth: int
    max_control_bytes: int
    max_row_bytes: int
    max_batch_bytes: int
    max_resident_bytes: int
    max_spill_bytes: int
    max_state_bytes: int
    max_pending_events: int
    max_open_files: int
    max_output_rows: int
    timeout_ms: int


@dataclass(frozen=True)
class SourceSnapshot:
    source_id: str
    kind: str
    immutable_identity: Mapping[str, Any]
    finite: bool
    replay: str
    manifest_ref: ArtifactRef | None


@dataclass(frozen=True)
class SourceSplit:
    split_id: str
    source_id: str
    locator: Mapping[str, Any]
    start_cursor: Mapping[str, Any]
    estimated_bytes: int | None
    required_domain: str


@dataclass(frozen=True)
class SourceCursor:
    split_id: str
    offset: Mapping[str, Any]
    next_sequence: int
    end_of_input: bool
    lease_ref: ArtifactRef | None
    pending_refs: tuple[ArtifactRef, ...]


@dataclass(frozen=True)
class Record:
    record_id: str
    source_position: Mapping[str, Any]
    payload_ref: ArtifactRef | None
    inline: Any
    encoded_bytes: int
    sequence: int


@dataclass(frozen=True)
class SourcePoll:
    status: str
    record: Record | None
    retry_after_ms: int


@dataclass(frozen=True)
class QueueBinding:
    action_id: str
    source_id: str | None
    mode: str
    contract_digest: str
    generation: str | None
    pool: str | None
    source_occurrence: str | None
    fetch_occurrence: str
    final_occurrence: str | None


@dataclass(frozen=True)
class QueueCommand:
    operation_id: str
    operation: str
    binding: QueueBinding
    record_id: str | None
    delivery_ref: ArtifactRef | None
    row_ref: ArtifactRef | None


@dataclass(frozen=True)
class QueueReply:
    operation_id: str
    status: str
    delivery_ref: ArtifactRef | None
    row_ref: ArtifactRef | None
    native_fact_ref: ArtifactRef | None
    retry_after_ms: int


@dataclass(frozen=True)
class LoadedProgram:
    kind: str
    value: object
    digest: str


@dataclass(frozen=True)
class SourceDefinition:
    kind: str
    reader_symbol: str
    cursor_fields: tuple[str, ...]


@dataclass(frozen=True)
class WorkerAssignment:
    schema: str
    run_id: str
    action_id: str
    attempt: int
    fence: str
    plan_id: str
    plan_revision: int
    lease: Any
    sealed: Any
    stages: tuple
    inputs: tuple
    outputs: tuple
    source_splits: tuple
    restore_refs: tuple
    bounds: Bounds


@dataclass(frozen=True)
class RuntimeServices:
    store: Any
    journal: Any
    bounds: Bounds
    assignment: WorkerAssignment
    clock_ms: Any
    check_stop: Any
    constants: Mapping[str, Any]
    programs: Mapping[str, LoadedProgram]


class SourceReader:  # protocol stand-in
    pass


class DataflowError(Exception):
    def __init__(self, code, message, *, retryable=False, context=None):
        self.code = code
        self.message = message
        self.retryable = retryable
        self.context = dict(context or {})
        super().__init__(f"{code}: {message}")


def _real_module_available(name):
    import importlib
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def _install(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    if not _real_module_available(name):
        sys.modules[name] = module
        parent, _, leaf = name.rpartition(".")
        if parent in sys.modules:
            setattr(sys.modules[parent], leaf, module)
    return module


_install(
    "demiflow.execution.contracts.dataflow",
    ArtifactRef=ArtifactRef, Bounds=Bounds, SourceSnapshot=SourceSnapshot,
    SourceSplit=SourceSplit, SourceCursor=SourceCursor, Record=Record,
    SourcePoll=SourcePoll, QueueBinding=QueueBinding, QueueCommand=QueueCommand,
    QueueReply=QueueReply, LoadedProgram=LoadedProgram, SourceDefinition=SourceDefinition,
    WorkerAssignment=WorkerAssignment, RuntimeServices=RuntimeServices, SourceReader=SourceReader,
)
_install("demiflow.execution.contracts.dataflow_errors", DataflowError=DataflowError)

_REGISTRY = {
    "ItemsSource": SourceDefinition("ItemsSource", "demiflow.execution.source_readers.ItemsReader", ("index",)),
    "MaterializedSource": SourceDefinition("MaterializedSource", "demiflow.execution.source_readers.ItemsReader", ("index",)),
    "RangeSource": SourceDefinition("RangeSource", "demiflow.execution.source_readers.RangeReader", ("next",)),
    "FileSource": SourceDefinition("FileSource", "demiflow.execution.source_readers.FileReader", ("file_index", "record_index")),
    "LanceSource": SourceDefinition("LanceSource", "demiflow.execution.source_readers.LanceReader", ("version", "fragment_id", "row_offset")),
    "SqlSource": SourceDefinition("SqlSource", "demiflow.execution.source_readers.SqlReader", ("snapshot_id", "partition", "last_key")),
    "task": SourceDefinition("task", "demiflow.execution.source_readers.TaskReader", ("index",)),
    "history": SourceDefinition("history", "demiflow.execution.source_readers.HistoryReader", ("index",)),
    "IterableSource": SourceDefinition("IterableSource", "demiflow.execution.source_readers.IteratorReader", ("index",)),
    "queue": SourceDefinition("queue", "demiflow.execution.source_readers.QueueReader", ("claim_ref", "record_index")),
}
_install("demiflow.planning.dataflow_registry", source_registry=lambda: dict(_REGISTRY))


def _queue_service_request(command, *, services):
    return services.journal.queue(command)


_install("demiflow.execution.dataflow_queue", queue_service_request=_queue_service_request)

from demiflow.execution import dataflow_sources as ds  # noqa: E402
from demiflow.execution import source_readers as sr  # noqa: E402
from demiflow.execution.recovery_contract import identity  # noqa: E402
from demiflow.data.datasource import Datasource, ReadTask, BlockMetadata  # noqa: E402
from demiflow.data.records import FileRecords  # noqa: E402
from demiflow.data.sources import (  # noqa: E402
    DatasourceSource, FileSource, ItemsSource, IterableSource, LanceSource,
    MaterializedSource, RangeSource, SqlSource,
)
from demiflow.execution.queue_source import QueueDatasource  # noqa: E402
from demiflow.lance.model import LanceScanSpec, LanceVectorSearchSpec  # noqa: E402

assert Path(ds.__file__).resolve().is_relative_to(WORKTREE)
assert Path(sr.__file__).resolve().is_relative_to(WORKTREE)


from demiflow.execution.contracts.dataflow import (
    ArtifactRef, Bounds, LoadedProgram, RuntimeServices, WorkerAssignment,
    WorkerLease, SealedExecution, QueueReply,
)

# --- helpers ----------------------------------------------------------------
def assert_worktree():
    for name, module in list(sys.modules.items()):
        if name == "demiflow" or name.startswith("demiflow."):
            path = getattr(module, "__file__", None)
            if path is not None:
                assert Path(path).resolve().is_relative_to(WORKTREE), (name, path)


def make_bounds(**overrides):
    values = dict(
        max_nodes=1000, max_edges=1000, max_partitions=64, max_depth=32,
        max_control_bytes=1 << 20, max_row_bytes=1 << 20, max_batch_bytes=4 << 20,
        max_resident_bytes=64 << 20, max_spill_bytes=64 << 20, max_state_bytes=16 << 20,
        max_pending_events=1024, max_open_files=64, max_output_rows=1_000_000, timeout_ms=60000,
    )
    values.update(overrides)
    return Bounds(**values)


class FakeStore:
    def __init__(self):
        self.blobs = {}
        self.charged = 0
        self.puts = 0
        self.retained = {}
        self.released = []

    def put(self, chunks, *, max_bytes, media_type):
        data = b"".join(chunks)
        if len(data) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "put exceeds max_bytes")
        digest = hashlib.sha256(data).hexdigest()
        self.puts += 1
        if digest not in self.blobs:
            self.blobs[digest] = data
            self.charged += len(data)
        return ArtifactRef(digest, len(data), "test-store", digest, media_type)

    def read(self, ref, *, max_bytes):
        data = self.blobs[ref.key]
        if len(data) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "read exceeds max_bytes")
        yield data

    def inspect(self, ref):
        return ref.key in self.blobs

    def retain(self, ref, *, owner):
        self.retained.setdefault(ref.key, set()).add(owner)

    def release(self, ref, *, owner):
        self.released.append((ref.key, owner))


class FakeJournal:
    def __init__(self, replies=()):
        self.replies = list(replies)
        self.commands = []

    def read_configuration(self):
        return {"queue_binding": self.binding}

    def queue(self, command):
        self.commands.append(command)
        if not self.replies:
            return QueueReply(command.operation_id, "pending", None, None, None, 5)
        status, delivery_ref, row_ref, retry = self.replies.pop(0)
        return QueueReply(command.operation_id, status, delivery_ref, row_ref, None, retry)


def make_assignment(action_id, bounds):
    ref = ArtifactRef("a" * 64, 1, "test-store", "a" * 64, "application/json")
    lease = WorkerLease("allocation", "boot", "stage", ("p0",), "local", "endpoint", ref, 10 ** 13)
    sealed = SealedExecution(ref, ref, (), ref, ref, (), ref)
    return WorkerAssignment(
        schema="demiflow.dataflow_assignment.v3", run_id="run", action_id=action_id,
        attempt=1, fence="fence", plan_id="a" * 64, plan_revision=1, lease=lease,
        sealed=sealed, stages=(), inputs=(), outputs=(), source_splits=(),
        restore_refs=(), bounds=bounds,
    )


def make_services(store, plan, source_id, *, journal=None, bounds=None, action_id="act"):
    bounds = bounds or make_bounds()
    if journal is not None:
        journal.binding = dict(action_id=action_id, source_id=source_id, mode="explicit",
            contract_digest="a" * 64, generation=None, pool=None, source_occurrence=source_id,
            fetch_occurrence=source_id, final_occurrence=None)
    programs = {source_id: LoadedProgram("source", plan, identity(["program", source_id]))}
    return RuntimeServices(
        store=store, journal=journal if journal is not None else FakeJournal(),
        bounds=bounds, assignment=make_assignment(action_id, bounds),
        clock_ms=lambda: 0, check_stop=lambda: None, constants={}, programs=programs,
    )


def make_snapshot(plan, store, *, action_id="act", bounds=None):
    return ds.snapshot_source(plan, action=SimpleNamespace(action_id=action_id),
                              store=store, bounds=bounds or make_bounds())


def first_split(snapshot, plan, *, target_partitions=1, bounds=None):
    return ds.plan_source_splits(snapshot, plan, target_partitions=target_partitions,
                                 bounds=bounds or make_bounds())[0]


def open_reader(snapshot, split, services, cursor=None):
    return ds.open_source_reader(snapshot, split, cursor=cursor, services=services)


def drain(reader):
    rows = []
    while True:
        poll = reader.next_record()
        if poll.status == "eoi":
            return rows
        assert poll.status == "record"
        rows.append(poll.record)


def load_inline(record, store):
    if record.inline is not None:
        return record.inline
    from demiflow.execution.arrow_payload import is_arrow_record, batch_cache
    if is_arrow_record(record):
        return batch_cache(store, make_bounds()).row(record)
    return pickle.loads(b"".join(store.read(record.payload_ref, max_bytes=1 << 20)))


def write_jsonl(path, lines):
    path.write_text("".join(line + "\n" for line in lines), encoding="utf-8")
    return str(path)


class ReadOnlyDS(Datasource):
    """Read-only test-domain history/task datasource; no claim/fetch/ack API."""

    def __init__(self, rows, identity_facts=None):
        self._rows = list(rows)
        self._facts = identity_facts
        self.calls = []

    def snapshot_identity(self):
        return self._facts

    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        self.calls.append("get_read_tasks")
        rows = self._rows

        def read_fn():
            self.calls.append("read_fn")
            yield list(rows)

        return [ReadTask(read_fn, BlockMetadata())]


def make_lance(uri, rows, *, max_rows_per_file=None):
    import lance
    import pyarrow as pa

    table = pa.table({
        "id": pa.array([r["id"] for r in rows], type=pa.int64()),
        "val": pa.array([r["val"] for r in rows], type=pa.string()),
    })
    kwargs = {"mode": "create"}
    if max_rows_per_file is not None:
        kwargs["max_rows_per_file"] = max_rows_per_file
    lance.write_dataset(table, uri, **kwargs)
    return uri


# --- S001 snapshot_source ---------------------------------------------------
def test_s001(tmp_path, monkeypatch):
    assert_worktree()
    store = FakeStore()
    items = make_snapshot(ItemsSource((1, 2, 3)), store)
    assert items.kind == "items" and items.replay == "seek" and items.finite is True
    rng = make_snapshot(RangeSource(5), store)
    assert rng.replay == "seek" and rng.immutable_identity["count"] == 5
    it = make_snapshot(IterableSource(lambda: iter([1])), store)
    assert it.replay == "attempt_only" and it.finite is False
    path = write_jsonl(tmp_path / "a.jsonl", ['{"x": 1}'])
    fs = make_snapshot(FileSource("jsonl", (path,), {}), store)
    assert fs.manifest_ref is not None and fs.replay == "seek"

    # mutable Lance head binds a fixed version once, without reading fragments
    import demiflow.lance.read as lread
    uri = make_lance(str(tmp_path / "ds.lance"), [{"id": i, "val": str(i)} for i in range(6)],
                     max_rows_per_file=2)
    calls = []
    monkeypatch.setattr(lread, "_iter_scan", lambda *a, **k: calls.append(1) or iter(()))
    lance_snap = make_snapshot(LanceSource(LanceScanSpec(uri=uri)), store)
    assert calls == []  # no fragment row scan before first record
    assert isinstance(lance_snap.immutable_identity["version"], int)
    again = make_snapshot(LanceSource(LanceScanSpec(uri=uri)), store)
    assert again.immutable_identity["version"] == lance_snap.immutable_identity["version"]
    assert again.source_id == lance_snap.source_id


# --- S002 plan_source_splits ------------------------------------------------
def test_s002(tmp_path):
    assert_worktree()
    store = FakeStore()
    snap = make_snapshot(ItemsSource(tuple(range(10))), store)
    plan = ItemsSource(tuple(range(10)))
    splits_a = ds.plan_source_splits(snap, plan, target_partitions=3, bounds=make_bounds())
    splits_b = ds.plan_source_splits(snap, plan, target_partitions=3, bounds=make_bounds())
    assert [s.split_id for s in splits_a] == [s.split_id for s in splits_b]  # stable recovery
    assert len(splits_a) == 3
    assert splits_a[0].start_cursor == {"index": 0}
    covered = [i for s in splits_a for i in range(s.locator["start"], s.locator["end"])]
    assert covered == list(range(10))
    with pytest.raises(DataflowError) as exc:
        ds.plan_source_splits(snap, plan, target_partitions=0, bounds=make_bounds())
    assert exc.value.code == "INVALID_CONTRACT"
    with pytest.raises(DataflowError) as exc:
        ds.plan_source_splits(snap, plan, target_partitions=999, bounds=make_bounds(max_partitions=4))
    assert exc.value.code == "BUDGET_EXCEEDED"
    # manifest is bounded; an oversized file list is rejected, not stuffed whole
    paths = tuple(write_jsonl(tmp_path / f"f{i}.jsonl", ['{"x": 1}']) for i in range(40))
    with pytest.raises(DataflowError) as exc:
        make_snapshot(FileSource("jsonl", paths, {}), store, bounds=make_bounds(max_control_bytes=256))
    assert exc.value.code == "BUDGET_EXCEEDED"


# --- S003 open_source_reader ------------------------------------------------
def test_s003(tmp_path):
    assert_worktree()
    store = FakeStore()
    path = write_jsonl(tmp_path / "a.jsonl", ['{"x": 1}', '{"x": 2}'])
    plan = FileSource("jsonl", (path,), {})
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    services = make_services(store, plan, snap.source_id)
    # source changed before any business effect
    Path(path).write_text('{"x": 99}\n', encoding="utf-8")
    with pytest.raises(DataflowError) as exc:
        open_reader(snap, split, services)
    assert exc.value.code == "SOURCE_CHANGED"
    assert store.puts == 1  # only the manifest; no record spooled before failure
    # unknown kind is rejected by the single registry
    bad = SourceSnapshot(snap.source_id, "NoSuchKind", {}, True, "seek", None)
    with pytest.raises(DataflowError) as exc:
        open_reader(bad, split, services)
    assert exc.value.code == "INVALID_CONTRACT"


# --- S004 verify_source_snapshot --------------------------------------------
def test_s004(tmp_path):
    assert_worktree()
    store = FakeStore()
    path = write_jsonl(tmp_path / "a.jsonl", ['{"x": 1}'])
    plan = FileSource("jsonl", (path,), {})
    snap = make_snapshot(plan, store)
    ds.verify_source_snapshot(snap, store=store, bounds=make_bounds())
    Path(path).write_text('{"x": 2}\n', encoding="utf-8")  # same path, new content
    with pytest.raises(DataflowError) as exc:
        ds.verify_source_snapshot(snap, store=store, bounds=make_bounds())
    assert exc.value.code == "SOURCE_CHANGED"
    # empty file set is still a legal, complete source
    empty = make_snapshot(FileSource("jsonl", (), {}), store)
    ds.verify_source_snapshot(empty, store=store, bounds=make_bounds())
    esplit = first_split(empty, FileSource("jsonl", (), {}))
    reader = open_reader(empty, esplit, make_services(store, FileSource("jsonl", (), {}), empty.source_id))
    assert reader.next_record().status == "eoi"
    reader.close()


# --- S005 spool_record ------------------------------------------------------
def test_s005():
    assert_worktree()
    store = FakeStore()
    bounds = make_bounds()
    record = Record("r" * 64, {"index": 0}, None, {"x": 1}, 10, 0)
    ref = ds.spool_record(record, split_id="s" * 64, sequence=0, store=store, bounds=bounds)
    assert store.inspect(ref)
    charged_after_first = store.charged
    # crash after write, before publish: the artifact is recoverable/releasable
    store.retain(ref, owner="split")
    store.release(ref, owner="split")
    assert (ref.key, "split") in store.released
    # repeated publication is content-addressed idempotent: no double quota
    ref2 = ds.spool_record(record, split_id="s" * 64, sequence=0, store=store, bounds=bounds)
    assert ref2.digest == ref.digest
    assert store.charged == charged_after_first
    assert store.puts == 2 and len(store.blobs) == 1
    # passthrough when a payload ref already exists
    assert ds.spool_record(Record("r" * 64, {}, ref, None, 10, 0), split_id="s" * 64,
                           sequence=0, store=store, bounds=bounds) is ref
    with pytest.raises(DataflowError) as exc:
        ds.spool_record(Record("r" * 64, {}, None, {"big": "x" * 100}, 100, 0),
                        split_id="s" * 64, sequence=0, store=store, bounds=make_bounds(max_row_bytes=8))
    assert exc.value.code == "BUDGET_EXCEEDED"


# --- S006-S010 ItemsReader --------------------------------------------------
def _items_setup(tmp_path, items):
    store = FakeStore()
    plan = ItemsSource(tuple({"value": item} for item in items))
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    services = make_services(store, plan, snap.source_id)
    return store, plan, snap, split, services


def test_s006(tmp_path):
    assert_worktree()
    store, plan, snap, split, services = _items_setup(tmp_path, [(1, 2), b"bytes", -0.0])
    reader = sr.ItemsReader(snap, services=services)
    assert reader._opened is False  # construction does not read the source
    assert store.puts == 0  # items carry no manifest; nothing read or spooled yet
    reader.open(split, None)
    assert reader._opened is True
    reader.close()


def test_s007(tmp_path):
    assert_worktree()
    store, plan, snap, split, services = _items_setup(tmp_path, [1, 2, 3])
    reader = sr.ItemsReader(snap, services=services)
    wrong = SourceSplit(split.split_id, "other-source", split.locator, split.start_cursor, None, "local")
    with pytest.raises(DataflowError) as exc:
        reader.open(wrong, None)
    assert exc.value.code == "INVALID_CONTRACT"
    bad_id = SourceSplit("0" * 64, snap.source_id, split.locator, split.start_cursor, None, "local")
    reader2 = sr.ItemsReader(snap, services=services)
    with pytest.raises(DataflowError):
        reader2.open(bad_id, None)


def test_s008(tmp_path):
    assert_worktree()
    items = [(1, 2), b"bytes", -0.0, "s"]
    store, plan, snap, split, services = _items_setup(tmp_path, items)
    reader = open_reader(snap, split, services)
    records = drain(reader)
    assert len(records) == len(items)
    for record, original in zip(records, items):
        value = load_inline(record, store)["value"]
        assert value == original and type(value) is type(original)
    import math
    assert math.copysign(1.0, load_inline(records[2], store)["value"]) == -1.0
    reader.close()
    # empty array does not drift: immediate eoi
    store2, plan2, snap2, split2, services2 = _items_setup(tmp_path, [])
    reader2 = open_reader(snap2, split2, services2)
    assert reader2.next_record().status == "eoi"
    reader2.close()


def test_s009(tmp_path):
    assert_worktree()
    store, plan, snap, split, services = _items_setup(tmp_path, [10, 20, 30])
    reader = open_reader(snap, split, services)
    empty_cursor = reader.snapshot()
    assert empty_cursor.offset == {"index": 0} and empty_cursor.next_sequence == 0
    assert empty_cursor.pending_refs == ()
    first = reader.next_record()
    cursor = reader.snapshot()
    # read-but-unconfirmed frontier holds exactly the last sent record
    assert len(cursor.pending_refs) == 1
    assert cursor.pending_refs[0].media_type == "application/vnd.demiflow.source-pending.v3"
    restored = open_reader(snap, split, services, cursor=cursor)
    assert restored.next_record().record == first.record
    restored.close()
    assert cursor.offset == {"index": 1} and cursor.next_sequence == 1
    reader.next_record()  # pulling again confirms the previous record
    assert reader.snapshot().pending_refs != cursor.pending_refs
    reader.close()


def test_s010(tmp_path):
    assert_worktree()
    store, plan, snap, split, services = _items_setup(tmp_path, [1, 2])
    reader = open_reader(snap, split, services)
    reader.next_record()
    reader.close()
    reader.close()  # idempotent
    with pytest.raises(DataflowError) as exc:
        reader.snapshot()
    assert exc.value.code == "CANCELLED"


# --- S011-S015 RangeReader --------------------------------------------------
def _range_setup(count, **kw):
    store = FakeStore()
    plan = RangeSource(count)
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan, **kw)
    services = make_services(store, plan, snap.source_id)
    return store, plan, snap, split, services


def test_s011():
    assert_worktree()
    store, plan, snap, split, services = _range_setup(0)
    reader = sr.RangeReader(snap, services=services)
    assert reader._opened is False
    reader.open(split, None)
    assert reader.next_record().status == "eoi"  # take0 generates nothing
    reader.close()


def test_s012():
    assert_worktree()
    store, plan, snap, split, services = _range_setup(5)
    reader = sr.RangeReader(snap, services=services)
    wrong = SourceSplit(split.split_id, "other", split.locator, split.start_cursor, None, "local")
    with pytest.raises(DataflowError) as exc:
        reader.open(wrong, None)
    assert exc.value.code == "INVALID_CONTRACT"


def test_s013():
    assert_worktree()
    store, plan, snap, split, services = _range_setup(5)
    reader = open_reader(snap, split, services)
    records = drain(reader)
    assert [load_inline(r, store)["id"] for r in records] == [0, 1, 2, 3, 4]
    reader.close()
    # recovery resumes at the boundary, never repeating it
    cursor = SourceCursor(split.split_id, {"next": 3}, 3, False, None, ())
    reader2 = open_reader(snap, split, services, cursor=cursor)
    resumed = drain(reader2)
    assert [load_inline(r, store)["id"] for r in resumed] == [3, 4]
    reader2.close()


def test_s014():
    assert_worktree()
    store, plan, snap, split, services = _range_setup(5)
    reader = open_reader(snap, split, services)
    reader.next_record()
    cursor = reader.snapshot()
    assert cursor.offset == {"next": 1} and cursor.next_sequence == 1
    reader.close()


def test_s015():
    assert_worktree()
    store, plan, snap, split, services = _range_setup(5)
    reader = open_reader(snap, split, services)
    reader.close()
    reader.close()
    with pytest.raises(DataflowError):
        reader.next_record()


# --- S016-S020 FileReader ---------------------------------------------------
def test_s016(tmp_path):
    assert_worktree()
    store = FakeStore()
    path = write_jsonl(tmp_path / "a.jsonl", ['{"x": 1}', 'not-json', '{"x": 2}'])
    plan = FileSource("records", (path,), {"format": "jsonl"})
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    reader = sr.FileReader(snap, services=make_services(store, plan, snap.source_id))
    assert reader._opened is False  # construction does not read
    reader.open(split, None)
    rows = [load_inline(r, store) for r in drain(reader)]
    assert rows[1]["error"] is not None and rows[1]["raw"].rstrip("\r\n") == "not-json"  # corrupt row kept
    reader.close()


def test_s017(tmp_path):
    assert_worktree()
    import gzip
    store = FakeStore()
    gz = tmp_path / "a.jsonl.gz"
    with gzip.open(gz, "wt", encoding="utf-8") as stream:
        stream.write('{"x": 0}\n{"x": 1}\n{"x": 2}\n')
    plan = FileSource("jsonl", (str(gz),), {})
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    services = make_services(store, plan, snap.source_id)
    reader = sr.FileReader(snap, services=services)
    wrong = SourceSplit(split.split_id, "other", split.locator, split.start_cursor, None, "local")
    with pytest.raises(DataflowError):
        reader.open(wrong, None)
    # compressed source is non-seekable: recovery re-reads deterministically
    cursor = SourceCursor(split.split_id, {"file_index": 0, "record_index": 2}, 2, False, None, ())
    reader2 = open_reader(snap, split, services, cursor=cursor)
    rows = [load_inline(r, store) for r in drain(reader2)]
    assert [r["x"] for r in rows] == [2]
    reader2.close()


def test_s018(tmp_path):
    assert_worktree()
    store = FakeStore()
    path = write_jsonl(tmp_path / "a.jsonl", ['{"x": 1}', 'broken', '{"x": 3}'])
    plan = FileSource("records", (path,), {"format": "jsonl"})
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    reader = open_reader(snap, split, make_services(store, plan, snap.source_id))
    records = drain(reader)
    middle = load_inline(records[1], store)
    assert middle["value"] is None and middle["error"]["type"] and middle["raw"].rstrip("\r\n") == "broken"
    assert records[0].source_position == {"file_index": 0, "record_index": 0}
    reader.close()


def test_s019(tmp_path):
    assert_worktree()
    store = FakeStore()
    p0 = write_jsonl(tmp_path / "a.jsonl", ['{"x": 1}'])
    p1 = write_jsonl(tmp_path / "b.jsonl", ['{"x": 2}'])
    plan = FileSource("jsonl", (p0, p1), {})
    snap = make_snapshot(plan, store)
    splits = ds.plan_source_splits(snap, plan, target_partitions=2, bounds=make_bounds())
    services = make_services(store, plan, snap.source_id)
    reader = open_reader(snap, splits[0], services)
    reader.next_record()
    cursor = reader.snapshot()
    assert cursor.offset == {"file_index": 0, "record_index": 1}
    reader.close()


def test_s020(tmp_path):
    assert_worktree()
    store = FakeStore()
    path = write_jsonl(tmp_path / "a.jsonl", ['{"x": 1}', '{"x": 2}'])
    plan = FileSource("jsonl", (path,), {})
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    reader = open_reader(snap, split, make_services(store, plan, snap.source_id))
    reader.next_record()
    reader.close()
    reader.close()
    assert reader._iter is None and reader._stream is None  # no FD leak


# --- S021-S025 LanceReader --------------------------------------------------
def test_s021(tmp_path):
    assert_worktree()
    store = FakeStore()
    uri = make_lance(str(tmp_path / "ds.lance"), [{"id": i, "val": str(i)} for i in range(4)])
    plan = LanceSource(LanceScanSpec(uri=uri))
    snap = make_snapshot(plan, store)
    fixed_version = snap.immutable_identity["version"]
    split = first_split(snap, plan)
    services = make_services(store, plan, snap.source_id)
    reader = sr.LanceReader(snap, services=services)
    assert reader._opened is False
    reader.open(split, None)
    # advance the head; the fixed-version reader must not follow it
    import lance
    import pyarrow as pa
    lance.write_dataset(pa.table({"id": pa.array([99], type=pa.int64()),
                                  "val": pa.array(["new"])}), uri, mode="append")
    rows = [load_inline(r, store) for r in drain(reader)]
    assert sorted(r["id"] for r in rows) == [0, 1, 2, 3]
    assert reader._version == fixed_version
    reader.close()


def test_s022(tmp_path):
    assert_worktree()
    store = FakeStore()
    uri = make_lance(str(tmp_path / "ds.lance"), [{"id": i, "val": str(i)} for i in range(4)])
    plan = LanceSource(LanceScanSpec(uri=uri))
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    services = make_services(store, plan, snap.source_id)
    reader = sr.LanceReader(snap, services=services)
    wrong = SourceSplit(split.split_id, "other", split.locator, split.start_cursor, None, "local")
    with pytest.raises(DataflowError):
        reader.open(wrong, None)


def test_s023(tmp_path):
    assert_worktree()
    import lance
    import pyarrow as pa
    store = FakeStore()
    uri = str(tmp_path / "vec.lance")
    table = pa.table({
        "id": pa.array(list(range(8)), type=pa.int64()),
        "vec": pa.FixedSizeListArray.from_arrays(pa.array([float(i) for i in range(32)], type=pa.float32()), 4),
    })
    lance.write_dataset(table, uri, mode="create", max_rows_per_file=2)
    plan = LanceSource(LanceVectorSearchSpec(uri=uri, vector=(0.0, 0.0, 0.0, 0.0),
                                             vector_column="vec", top_k=3))
    snap = make_snapshot(plan, store)
    splits = ds.plan_source_splits(snap, plan, target_partitions=4, bounds=make_bounds())
    assert len(splits) == 1  # global query stays a single global split
    services = make_services(store, plan, snap.source_id)
    reader = open_reader(snap, splits[0], services)
    rows = [load_inline(r, store) for r in drain(reader)]
    assert len(rows) == 3  # global top_k, not per-fragment top_k
    reader.close()


def test_s024(tmp_path):
    assert_worktree()
    store = FakeStore()
    uri = make_lance(str(tmp_path / "ds.lance"), [{"id": i, "val": str(i)} for i in range(4)],
                     max_rows_per_file=2)
    plan = LanceSource(LanceScanSpec(uri=uri))
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    reader = open_reader(snap, split, make_services(store, plan, snap.source_id))
    reader.next_record()
    cursor = reader.snapshot()
    assert cursor.offset["version"] == snap.immutable_identity["version"]
    assert "fragment_id" in cursor.offset and "row_offset" in cursor.offset
    reader.close()


def test_s025(tmp_path):
    assert_worktree()
    store = FakeStore()
    uri = make_lance(str(tmp_path / "ds.lance"), [{"id": i, "val": str(i)} for i in range(2)])
    plan = LanceSource(LanceScanSpec(uri=uri))
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    reader = open_reader(snap, split, make_services(store, plan, snap.source_id))
    reader.close()
    reader.close()
    with pytest.raises(DataflowError):
        reader.next_record()


# --- S026-S030 SqlReader ----------------------------------------------------
def _sqlite_factory(path, counter):
    import sqlite3

    def factory():
        counter.append(1)
        connection = sqlite3.connect(path)
        return connection

    return factory


def _sql_setup(tmp_path, *, keyset):
    import sqlite3
    path = str(tmp_path / "db.sqlite")
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
    connection.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"v{i}") for i in range(5)])
    connection.commit()
    connection.close()
    counter = []
    options = {"snapshot_id": "snap-1"}
    if keyset:
        options["keyset"] = ["k"]
    plan = SqlSource("SELECT k, v FROM t ORDER BY k", _sqlite_factory(path, counter), options)
    store = FakeStore()
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    services = make_services(store, plan, snap.source_id)
    return store, plan, snap, split, services, counter


def test_s026(tmp_path):
    assert_worktree()
    store, plan, snap, split, services, counter = _sql_setup(tmp_path, keyset=False)
    assert snap.replay == "durable_spool"  # no keyset guarantee -> spool declared
    reader = sr.SqlReader(snap, services=services)
    assert reader._opened is False
    reader.open(split, None)
    assert len(counter) == 1  # captured once
    reader.close()


def test_s027(tmp_path):
    assert_worktree()
    store, plan, snap, split, services, counter = _sql_setup(tmp_path, keyset=True)
    reader = sr.SqlReader(snap, services=services)
    wrong = SourceSplit(split.split_id, "other", split.locator, split.start_cursor, None, "local")
    with pytest.raises(DataflowError):
        reader.open(wrong, None)


def test_s028(tmp_path):
    assert_worktree()
    store, plan, snap, split, services, counter = _sql_setup(tmp_path, keyset=False)
    reader = open_reader(snap, split, services)
    first = [load_inline(reader.next_record().record, store) for _ in range(3)]
    assert [r["k"] for r in first] == [0, 1, 2]
    reader.acknowledge_output()  # consumer has durably accepted the final pulled row
    cursor = reader.snapshot()  # carries the durable spool reference and last_key=3
    reader.close()
    # reconnect on recovery replays the spool; it must not splice a fresh snapshot
    reader2 = open_reader(snap, split, services, cursor=cursor)
    resumed = [load_inline(r, store) for r in drain(reader2)]
    assert [r["k"] for r in resumed] == [3, 4]
    assert len(counter) == 1  # durable spool reused; no second query
    reader2.close()


def test_s029(tmp_path):
    assert_worktree()
    store, plan, snap, split, services, counter = _sql_setup(tmp_path, keyset=True)
    assert snap.replay == "seek"
    reader = open_reader(snap, split, services)
    reader.next_record()
    cursor = reader.snapshot()
    assert cursor.offset["snapshot_id"] == "snap-1"
    assert cursor.offset["last_key"] == 0
    reader.close()


def test_s030(tmp_path):
    assert_worktree()
    store, plan, snap, split, services, counter = _sql_setup(tmp_path, keyset=True)
    reader = open_reader(snap, split, services)
    reader.next_record()
    reader.close()
    reader.close()
    assert reader._connection is None and reader._cursor is None


# --- S031-S035 TaskReader ---------------------------------------------------
def _task_setup(tmp_path):
    store = FakeStore()
    path = write_jsonl(tmp_path / "data.jsonl", ['{"x": 1}', '{"x": 2}', '{"x": 3}'])
    plan = DatasourceSource(FileRecords([path], format="jsonl"))
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    services = make_services(store, plan, snap.source_id)
    return store, plan, snap, split, services


def test_s031(tmp_path):
    assert_worktree()
    store, plan, snap, split, services = _task_setup(tmp_path)
    assert snap.kind == "datasource"  # new seals use the canonical source kind
    assert ds._canonical_kind("task") == "datasource"  # old seals remain readable
    assert snap.replay == "attempt_only" and snap.finite is False  # None identity not faked recoverable
    reader = sr.TaskReader(snap, services=services)
    assert reader._opened is False
    reader.open(split, None)
    reader.close()


def test_s032(tmp_path):
    assert_worktree()
    store, plan, snap, split, services = _task_setup(tmp_path)
    reader = sr.TaskReader(snap, services=services)
    cursor = SourceCursor(split.split_id, {"index": 1}, 1, False, None, ())
    with pytest.raises(DataflowError) as exc:
        reader.open(split, cursor)
    assert exc.value.code == "INVALID_CONTRACT"  # attempt_only cannot resume


def test_s033(tmp_path):
    assert_worktree()
    store, plan, snap, split, services = _task_setup(tmp_path)
    reader = open_reader(snap, split, services)
    records = drain(reader)
    assert len(records) == 3
    assert all(r.inline is not None and r.payload_ref is None for r in records)  # attempt_only inline
    assert records[0].inline["value"] == {"x": 1}
    reader.close()


def test_s034(tmp_path):
    assert_worktree()
    store, plan, snap, split, services = _task_setup(tmp_path)
    reader = open_reader(snap, split, services)
    reader.next_record()
    cursor = reader.snapshot()
    assert cursor.offset == {"index": 1} and cursor.next_sequence == 1
    reader.close()


def test_s035(tmp_path):
    assert_worktree()
    store, plan, snap, split, services = _task_setup(tmp_path)
    reader = open_reader(snap, split, services)
    reader.close()
    reader.close()
    with pytest.raises(DataflowError):
        reader.next_record()


# --- S036-S040 IteratorReader -----------------------------------------------
def _iter_setup(factory):
    store = FakeStore()
    plan = IterableSource(factory)
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    services = make_services(store, plan, snap.source_id)
    return store, plan, snap, split, services


def test_s036():
    assert_worktree()
    constructed = []

    def factory():
        constructed.append(1)
        return iter([1, 2])

    store, plan, snap, split, services = _iter_setup(factory)
    reader = sr.IteratorReader(snap, services=services)
    assert constructed == []  # factory not called at construction
    # a closure capturing a live executor is rejected by precheck
    holder = SimpleNamespace(_executor=True)

    def bad_factory():
        yield holder

    store2, plan2, snap2, split2, services2 = _iter_setup(bad_factory)
    bad = sr.IteratorReader(snap2, services=services2)
    with pytest.raises(DataflowError) as exc:
        bad.open(split2, None)
    assert exc.value.code == "INVALID_CONTRACT"


def test_s037():
    assert_worktree()
    constructed = []

    def factory():
        constructed.append(1)
        return iter([1])

    store, plan, snap, split, services = _iter_setup(factory)
    reader = sr.IteratorReader(snap, services=services)
    wrong = SourceSplit(split.split_id, "other", split.locator, split.start_cursor, None, "local")
    with pytest.raises(DataflowError):
        reader.open(wrong, None)
    reader2 = sr.IteratorReader(snap, services=services)
    reader2.open(split, None)
    assert constructed == [1]  # invoked on the worker at open
    reader2.close()


def test_s038():
    assert_worktree()
    store, plan, snap, split, services = _iter_setup(lambda: iter([{"value": i} for i in [10, 20, 30]]))
    reader = open_reader(snap, split, services)
    records = drain(reader)
    assert [r.inline["value"] for r in records] == [10, 20, 30]
    assert all(r.payload_ref is None for r in records)  # attempt_only
    reader.close()


def test_s039():
    assert_worktree()
    store, plan, snap, split, services = _iter_setup(lambda: iter([{"value": 1}, {"value": 2}]))
    reader = open_reader(snap, split, services)
    reader.next_record()
    cursor = reader.snapshot()
    assert cursor.offset == {"index": 1} and cursor.next_sequence == 1
    reader.close()


def test_s040():
    assert_worktree()
    closed = []

    def factory():
        try:
            yield {"value": 1}
            yield {"value": 2}
        finally:
            closed.append(1)

    store, plan, snap, split, services = _iter_setup(factory)
    reader = open_reader(snap, split, services)
    reader.next_record()
    reader.close()
    reader.close()  # idempotent: generator closed exactly once
    assert closed == [1]


# --- S041-S045 QueueReader --------------------------------------------------
def _queue_setup(tmp_path, replies):
    store = FakeStore()
    qds = QueueDatasource({"backend": "sqlite", "queue_id": "q1", "path": str(tmp_path / "q.sqlite")})
    plan = DatasourceSource(qds)
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    journal = FakeJournal(replies)
    services = make_services(store, plan, snap.source_id, journal=journal)
    return store, plan, snap, split, services, journal


def _queue_row_ref(store, row):
    import json
    raw = json.dumps(row).encode()
    return store.put(iter((raw,)), max_bytes=len(raw), media_type="application/json")


def _delivery_ref(key):
    return ArtifactRef("d" * 64, 16, "test-store", key, "application/vnd.demiflow.delivery.v3")


def test_s041(tmp_path):
    assert_worktree()
    store, plan, snap, split, services, journal = _queue_setup(tmp_path, [])
    assert snap.kind == "queue" and snap.replay == "lease_queue" and snap.finite is False
    reader = sr.QueueReader(snap, services=services)
    assert reader._opened is False
    reader.open(split, None)
    poll = reader.next_record()  # empty open pool -> pending, never EOI
    assert poll.status == "pending" and poll.retry_after_ms > 0
    assert journal.commands[0].operation == "poll"
    reader.close()


def test_s042(tmp_path):
    assert_worktree()
    store, plan, snap, split, services, journal = _queue_setup(tmp_path, [])
    reader = sr.QueueReader(snap, services=services)
    wrong = SourceSplit(split.split_id, "other", split.locator, split.start_cursor, None, "driver")
    with pytest.raises(DataflowError):
        reader.open(wrong, None)


def test_s043(tmp_path):
    assert_worktree()
    store = FakeStore()
    row_ref = _queue_row_ref(store, {"x": 7})
    delivery = _delivery_ref("claim-1")
    replies = [("pending", None, None, 9), ("record", delivery, row_ref, 0), ("eoi", None, None, 0)]
    qds = QueueDatasource({"backend": "sqlite", "queue_id": "q1", "path": str(tmp_path / "q.sqlite")})
    plan = DatasourceSource(qds)
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    journal = FakeJournal(replies)
    services = make_services(store, plan, snap.source_id, journal=journal)
    reader = open_reader(snap, split, services)
    assert reader.next_record().status == "pending"
    poll = reader.next_record()
    assert poll.status == "record" and load_inline(poll.record, store) == {"x": 7}
    assert reader.next_record().status == "eoi"
    reader.close()
    # a lost lease (unknown) must not confirm the old task
    store2 = FakeStore()
    qds2 = QueueDatasource({"backend": "sqlite", "queue_id": "q2", "path": str(tmp_path / "q2.sqlite")})
    plan2 = DatasourceSource(qds2)
    snap2 = make_snapshot(plan2, store2)
    split2 = first_split(snap2, plan2)
    journal2 = FakeJournal([("unknown", None, None, 0)])
    services2 = make_services(store2, plan2, snap2.source_id, journal=journal2)
    reader2 = open_reader(snap2, split2, services2)
    with pytest.raises(DataflowError):
        reader2.next_record()
    assert reader2.snapshot().next_sequence == 0  # nothing confirmed
    reader2.close()


def test_s044(tmp_path):
    assert_worktree()
    store = FakeStore()
    row_ref = _queue_row_ref(store, {"x": 1})
    delivery = _delivery_ref("claim-9")
    qds = QueueDatasource({"backend": "sqlite", "queue_id": "q1", "path": str(tmp_path / "q.sqlite")})
    plan = DatasourceSource(qds)
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    journal = FakeJournal([("record", delivery, row_ref, 0)])
    services = make_services(store, plan, snap.source_id, journal=journal)
    reader = open_reader(snap, split, services)
    reader.next_record()
    cursor = reader.snapshot()
    assert cursor.offset["record_index"] == 1
    assert cursor.lease_ref == delivery
    assert len(cursor.pending_refs) == 1
    assert cursor.pending_refs[0].media_type == "application/vnd.demiflow.source-pending.v3"
    import json
    pending = json.loads(b"".join(store.read(cursor.pending_refs[0], max_bytes=1 << 20)))
    assert pending["sequence"] == 0
    assert "queue_delivery_ref" in pending["document"]
    reader.close()
    restored = open_reader(snap, split, services, cursor=cursor)
    replay = restored.next_record()
    assert replay.status == "record" and replay.record.sequence == 0
    assert load_inline(replay.record, store) == {"x": 1}
    assert restored._delivery_ref == delivery
    assert restored.snapshot() == cursor
    assert len(journal.commands) == 1  # captured row is not a new native claim
    restored.close()


def test_s045(tmp_path):
    assert_worktree()
    store = FakeStore()
    row_ref = _queue_row_ref(store, {"x": 1})
    delivery = _delivery_ref("claim-1")
    qds = QueueDatasource({"backend": "sqlite", "queue_id": "q1", "path": str(tmp_path / "q.sqlite")})
    plan = DatasourceSource(qds)
    snap = make_snapshot(plan, store)
    split = first_split(snap, plan)
    journal = FakeJournal([("record", delivery, row_ref, 0)])
    services = make_services(store, plan, snap.source_id, journal=journal)
    reader = open_reader(snap, split, services)
    reader.next_record()
    reader.close()
    reader.close()
    # close never ACKs: only poll commands were issued, no finalize
    assert all(c.operation == "poll" for c in journal.commands)


# --- S046-S050 HistoryReader ------------------------------------------------
def _history_setup(rows):
    store = FakeStore()
    history_ds = ReadOnlyDS(rows, identity_facts={"contract": "history.test", "rows": len(rows)})
    plan = DatasourceSource(history_ds)
    source_id = identity(["dataflow-occurrence-v3", "act", 0])
    snap = SourceSnapshot(source_id, "history",
                          {"kind": "history", "declaration": history_ds.snapshot_identity()},
                          True, "seek", None)
    split = first_split(snap, plan)
    services = make_services(store, plan, source_id)
    return store, plan, snap, split, services, history_ds


def test_s046():
    assert_worktree()
    store, plan, snap, split, services, history_ds = _history_setup([{"x": 1}, {"x": 2}])
    reader = sr.HistoryReader(snap, services=services)
    assert reader._opened is False
    reader.open(split, None)
    assert set(history_ds.calls) <= {"get_read_tasks", "read_fn"}  # read-only, no claim/fetch
    assert "read_fn" not in history_ds.calls  # rows not pulled until next_record
    reader.close()


def test_s047():
    assert_worktree()
    store, plan, snap, split, services, history_ds = _history_setup([{"x": 1}])
    reader = sr.HistoryReader(snap, services=services)
    wrong = SourceSplit(split.split_id, "other", split.locator, split.start_cursor, None, "local")
    with pytest.raises(DataflowError):
        reader.open(wrong, None)


def test_s048():
    assert_worktree()
    store, plan, snap, split, services, history_ds = _history_setup([{"x": 1}, {"x": 2}])
    reader = open_reader(snap, split, services)
    records = drain(reader)
    assert [load_inline(r, store)["x"] for r in records] == [1, 2]
    assert set(history_ds.calls) <= {"get_read_tasks", "read_fn"}  # no claim/fetch/ack
    reader.close()


def test_s049():
    assert_worktree()
    store, plan, snap, split, services, history_ds = _history_setup([{"x": 1}, {"x": 2}])
    reader = open_reader(snap, split, services)
    reader.next_record()
    cursor = reader.snapshot()
    assert cursor.offset == {"index": 1} and cursor.next_sequence == 1
    reader.close()


def test_s050():
    assert_worktree()
    store, plan, snap, split, services, history_ds = _history_setup([{"x": 1}])
    reader = open_reader(snap, split, services)
    reader.close()
    reader.close()
    assert set(history_ds.calls) <= {"get_read_tasks", "read_fn"}
    with pytest.raises(DataflowError):
        reader.next_record()
# Integrated lanes use the one production error vocabulary.
from demiflow.execution.contracts.dataflow_errors import DataflowError

from demiflow.execution.contracts.dataflow import ArtifactRef


def test_producer_pending_replay_preserves_identity_and_cursor(tmp_path):
    """Pending replay preserves data; it does not grant a new native lease."""
    import json
    from dataclasses import replace
    store, plan, snap, split, services, journal = _queue_setup(tmp_path, [])
    from demiflow.execution.contracts.dataflow import thaw_record_value
    snap = replace(snap, kind="producer", immutable_identity=thaw_record_value(snap.immutable_identity))
    descriptor = {"position": 7, "next_cursor": {"offset": 8}, "occurrence": "b" * 64}
    delivery = _queue_row_ref(store, descriptor)
    row_ref = _queue_row_ref(store, {"x": 9})
    journal.replies.append(("record", delivery, row_ref, 0))
    first = sr.ProducerReader(snap, services=services)
    first.open(split, None)
    record = first.next_record().record
    cursor = first.snapshot()
    assert record.record_id == descriptor["occurrence"]
    assert cursor.offset["producer_position"] == 8
    first.close()
    # The pending artifact contains the complete source identity. Replaying it
    # must neither re-read the producer descriptor nor request a fresh claim.
    del store.blobs[delivery.key]
    resumed = sr.ProducerReader(snap, services=services)
    resumed.open(split, cursor)
    assert resumed.next_record().record == record
    assert resumed.snapshot() == cursor
    assert resumed._delivery_ref == delivery
    assert len(journal.commands) == 1
    resumed.close()


@pytest.mark.parametrize("kind", ["queue", "producer"])
@pytest.mark.parametrize("following", ["pending", "eoi"])
def test_queue_next_pull_clears_published_record_but_keeps_claim(tmp_path, kind, following):
    """A later pull confirms publication, without granting or releasing a lease."""
    from dataclasses import replace
    from demiflow.execution.contracts.dataflow import thaw_record_value
    store, plan, snap, split, services, journal = _queue_setup(tmp_path, [])
    reader_class = sr.QueueReader
    delivery = _delivery_ref("claim-1")
    if kind == "producer":
        snap = replace(snap, kind=kind, immutable_identity=thaw_record_value(snap.immutable_identity))
        reader_class = sr.ProducerReader
        delivery = _queue_row_ref(store, {
            "position": 7, "next_cursor": {"offset": 8}, "occurrence": "b" * 64})
    row_ref = _queue_row_ref(store, {"x": 9})
    journal.replies.extend([("record", delivery, row_ref, 0), (following, None, None, 5 if following == "pending" else 0)])
    reader = reader_class(snap, services=services)
    reader.open(split, None)
    assert reader.next_record().status == "record"
    # Deliberately no snapshot between pulls: this exercises the in-memory
    # pending Record as well as the already-serialized pending_refs path.
    assert reader.next_record().status == following
    cursor = reader.snapshot()
    assert cursor.pending_refs == ()
    assert cursor.lease_ref == delivery
    assert cursor.next_sequence == 1 and cursor.offset["record_index"] == 1
    if kind == "producer":
        assert cursor.offset["producer_position"] == 8
        assert cursor.offset["producer_cursor"] == {"offset": 8}
    reader.close()
    resumed = reader_class(snap, services=services)
    resumed.open(split, cursor)
    assert resumed.next_record().status == following
    assert len(journal.commands) == (3 if following == "pending" else 2)
    assert all(command.operation == "poll" for command in journal.commands)
    assert resumed.snapshot() == cursor
    resumed.close()
