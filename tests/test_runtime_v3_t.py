"""Lane T unit acceptance (gate G-T): terminals, iterators, materialized handles.

Each test calls the real lane-T symbol from this worktree. The C/G/E lanes are
peer test domains whose frozen contracts are not yet landed, so strict-contract
spec fakes stand in for them (allowed for consumer tests); the fakes are defined
here and registered under their frozen module names with a tree-bound __file__.
No production fallback is used: the implementation imports the frozen paths
directly and these fakes satisfy them in-process.
"""
from __future__ import annotations

import hashlib
import json
import pickle
import sys
import types
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import pytest

WORKTREE = Path(__file__).resolve().parent.parent
TEST_FILE = Path(__file__).resolve()

# --- IL012 in-process tree binding -----------------------------------------
import demiflow  # noqa: E402

assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE), "demiflow not tree-bound"

from demiflow.execution import dataflow_handles as handles  # noqa: E402
from demiflow.execution import dataflow_terminals as terminals  # noqa: E402
from demiflow.data.datasink import WriteResult  # noqa: E402


# --- strict-contract spec fakes for the frozen C-lane DTOs ------------------
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
class Record:
    record_id: str
    source_position: dict
    payload_ref: object
    inline: object
    encoded_bytes: int
    sequence: int


@dataclass(frozen=True)
class TerminalResult:
    kind: str
    rows: object
    scalar: object
    result_ref: object
    write_result: object
    complete: bool
    completion: str


@dataclass(frozen=True)
class ExecutionStatus:
    state: str
    pending_events: int = 0
    confirmed_rows: int = 0


@dataclass(frozen=True)
class ActionRequest:
    kind: str
    source: object = None
    plan: object = None
    arguments: dict = field(default_factory=dict)
    sink: object = None
    programs: tuple = ()
    provider: str = "local"
    action: object = None


@dataclass(frozen=True)
class TerminalDefinition:
    kind: str
    adapter_symbol: str
    result_kind: str


class DataflowError(Exception):
    def __init__(self, code, message, *, retryable=False, context=None):
        self.code = code
        self.message = message
        self.retryable = retryable
        self.context = context or {}
        super().__init__(f"{code}: {message}")


class RunAction:
    def __init__(self, action_id="act-1"):
        self.action_id = action_id


class DriverServices:
    def __init__(self, store, bounds, action=None):
        self.store = store
        self.bounds = bounds
        self.action = action or RunAction()


# Mirrors the delivered G009 terminal table (runtime_v3_g_20261114 @ 5146375)
# kind-for-kind so the spec fake is representative; fields match the C007+ DTO.
_TERMINAL_PREFIX = "demiflow.execution.dataflow_terminals."
_TERMINALS = {
    "take": TerminalDefinition("take", _TERMINAL_PREFIX + "collect_rows", "rows"),
    "take_all": TerminalDefinition("take_all", _TERMINAL_PREFIX + "collect_rows", "rows"),
    "take_batch": TerminalDefinition("take_batch", _TERMINAL_PREFIX + "collect_rows", "batches"),
    "iter_rows": TerminalDefinition("iter_rows", _TERMINAL_PREFIX + "iterate_rows", "row_iterator"),
    "iter_batches": TerminalDefinition("iter_batches", _TERMINAL_PREFIX + "iterate_batches", "batch_iterator"),
    "count": TerminalDefinition("count", _TERMINAL_PREFIX + "collect_rows", "scalar"),
    "aggregate": TerminalDefinition("aggregate", _TERMINAL_PREFIX + "collect_rows", "scalar"),
    "schema": TerminalDefinition("schema", _TERMINAL_PREFIX + "inspect_result", "schema"),
    "columns": TerminalDefinition("columns", _TERMINAL_PREFIX + "inspect_result", "columns"),
    "size_bytes": TerminalDefinition("size_bytes", _TERMINAL_PREFIX + "inspect_result", "scalar"),
    "materialize": TerminalDefinition("materialize", _TERMINAL_PREFIX + "materialized_handle", "materialized"),
    "write_datasink": TerminalDefinition("write_datasink", _TERMINAL_PREFIX + "write_result", "write_result"),
    "write_lance": TerminalDefinition("write_lance", _TERMINAL_PREFIX + "write_result", "write_result"),
    "write_file": TerminalDefinition("write_file", _TERMINAL_PREFIX + "write_result", "write_result"),
    "run_stream": TerminalDefinition("run_stream", _TERMINAL_PREFIX + "iterate_rows", "row_iterator"),
    "collect_summary": TerminalDefinition("collect_summary", _TERMINAL_PREFIX + "collect_rows", "rows"),
}


def terminal_registry():
    return dict(_TERMINALS)


def _real_module_available(name):
    import importlib
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def _install_fake_module(name, **attrs):
    mod = types.ModuleType(name)
    mod.__file__ = str(TEST_FILE)  # fakes live in this tree-bound test file
    for key, value in attrs.items():
        setattr(mod, key, value)
    if not _real_module_available(name):
        sys.modules[name] = mod
    return mod


_install_fake_module(
    "demiflow.execution.contracts.dataflow",
    ArtifactRef=ArtifactRef, Bounds=Bounds, Record=Record, TerminalResult=TerminalResult,
    ExecutionStatus=ExecutionStatus, ActionRequest=ActionRequest, TerminalDefinition=TerminalDefinition,
    DriverServices=DriverServices, RunAction=RunAction,
)
_install_fake_module("demiflow.execution.contracts.dataflow_errors", DataflowError=DataflowError)
_install_fake_module("demiflow.planning.dataflow_registry", terminal_registry=terminal_registry)


# --- E-lane test-domain execution / subscription ---------------------------
class FakeSubscription:
    def __init__(self, execution):
        self._execution = execution
        self.closed = False
        self.read_calls = 0

    def read(self, *, max_records, deadline_ms):
        self.read_calls += 1
        ex = self._execution
        if ex._infinite:
            batch = tuple(make_record(ex._generated + j) for j in range(max_records))
            ex._generated += max_records
            return batch
        start = ex._index
        end = min(start + max_records, len(ex._records))
        batch = tuple(ex._records[start:end])
        ex._index = end
        return batch

    def close(self):
        self.closed = True


class FakeExecution:
    def __init__(self, records=(), terminal_state="succeeded", result=None, failure=None,
                 services=None, request=None, infinite=False):
        self._records = list(records)
        self._index = 0
        self._infinite = infinite
        self._generated = 0
        self._terminal_state = terminal_state
        self._result = result
        self._failure = failure
        self.services = services
        self.request = request
        self._cancelled = False
        self.cancel_calls = []
        self.satisfy_calls = 0
        self.finish_calls = 0
        self._satisfied = False
        self.close_calls = 0
        self.pump_calls = 0
        self.subscriptions = []

    def subscribe_terminal(self):
        sub = FakeSubscription(self)
        self.subscriptions.append(sub)
        return sub

    def pump(self, *, deadline_ms):
        self.pump_calls += 1
        if self._cancelled:
            state = "cancelled"
        elif self._satisfied:
            state = 'succeeded'
        elif self.request.arguments.get("limit") == 0:
            state = self._terminal_state
        elif self._infinite:
            state = "running"
        elif self._index < len(self._records):
            state = "running"
        else:
            state = self._terminal_state
        return ExecutionStatus(state=state, pending_events=0, confirmed_rows=self._index)

    def satisfy_terminal(self):
        self.satisfy_calls += 1
        self._satisfied = True

    def finish(self):
        self.finish_calls += 1
        if self._failure is not None:
            raise self._failure
        if self._result is not None:
            return self._result
        kind = self.request.kind if self.request is not None else "take"
        return TerminalResult(kind=kind, rows=len(self._records), scalar=None, result_ref=None,
                              write_result=None, complete=True, completion="source_exhausted")

    def cancel(self, reason):
        self.cancel_calls.append(reason)
        self._cancelled = True

    def close(self):
        self.close_calls += 1


# --- D-lane test-domain artifact store -------------------------------------
class FakeStore:
    def __init__(self):
        self._data = {}
        self._pins = {}
        self.reads = 0
        self.retains = []
        self.releases = []

    def put(self, chunks, *, max_bytes, media_type):
        raw = b"".join(chunks)
        if len(raw) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "artifact exceeds put budget")
        key = uuid.uuid4().hex
        self._data[key] = raw
        return ArtifactRef(digest=hashlib.sha256(raw).hexdigest(), size_bytes=len(raw),
                           store_id="test", key=key, media_type=media_type)

    def read(self, ref, *, max_bytes):
        self.reads += 1
        raw = self._data[ref.key]
        if len(raw) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "artifact exceeds read budget")
        for i in range(0, len(raw), 4096):
            yield raw[i:i + 4096]

    def inspect(self, ref):
        return ref.key in self._data

    def retain(self, ref, *, owner):
        self.retains.append((ref.key, owner))
        self._pins.setdefault(ref.key, set()).add(owner)

    def release(self, ref, *, owner):
        self.releases.append((ref.key, owner))
        owners = self._pins.get(ref.key, set())
        owners.discard(owner)
        if not owners:
            self._pins.pop(ref.key, None)
            self._data.pop(ref.key, None)

    def is_pinned(self, ref, owner):
        return owner in self._pins.get(ref.key, set())

    def has_data(self, ref):
        return ref.key in self._data


# --- helpers ---------------------------------------------------------------
def make_bounds(**over):
    defaults = dict(max_nodes=1000, max_edges=1000, max_partitions=64, max_depth=64,
                    max_control_bytes=1 << 20, max_row_bytes=1 << 20, max_batch_bytes=4 << 20,
                    max_resident_bytes=8 << 20, max_spill_bytes=1 << 30, max_state_bytes=1 << 20,
                    max_pending_events=1024, max_open_files=64, max_output_rows=100000,
                    timeout_ms=30000)
    defaults.update(over)
    return Bounds(**defaults)


def make_record(i, row=None, payload_ref=None):
    if payload_ref is not None:
        return Record(record_id=f"r{i}", source_position={}, payload_ref=payload_ref,
                      inline=None, encoded_bytes=payload_ref.size_bytes, sequence=i)
    if row is None:
        row = {"id": i, "value": i * 2}
    encoded = pickle.dumps(row, protocol=4)
    return Record(record_id=f"r{i}", source_position={}, payload_ref=None, inline=row,
                  encoded_bytes=len(encoded), sequence=i)


def make_execution(records=(), terminal_state="succeeded", result=None, failure=None,
                   store=None, kind="take", arguments=None, bounds=None, infinite=False):
    bounds = bounds or make_bounds()
    services = DriverServices(store or FakeStore(), bounds)
    request = ActionRequest(kind=kind, arguments=arguments or {})
    return FakeExecution(records=records, terminal_state=terminal_state, result=result,
                         failure=failure, services=services, request=request, infinite=infinite)


def open_handle(execution):
    return terminals.open_terminal(execution.request, execution=execution)


def build_materialized(store, rows, *, columns=None, num_blocks=1, owner_rows=None):
    groups = [rows[i::num_blocks] for i in range(num_blocks)] if num_blocks > 1 else [rows]
    block_refs = []
    for group in groups:
        payload = b"".join(pickle.dumps(row, protocol=4) for row in group)
        block_refs.append(store.put(iter([payload]), max_bytes=len(payload) + 1,
                                    media_type="application/x-pickle-row-stream"))
    manifest = {"schema": handles.MANIFEST_SCHEMA,
                "blocks": [dict(digest=r.digest, size_bytes=r.size_bytes, store_id=r.store_id,
                                key=r.key, media_type=r.media_type) for r in block_refs],
                "rows": len(rows)}
    if columns is not None:
        manifest["columns"] = columns
    raw = json.dumps(manifest).encode()
    manifest_ref = store.put(iter([raw]), max_bytes=len(raw) + 1, media_type="application/json")
    result = TerminalResult(kind="materialize", rows=len(rows), scalar=None,
                            result_ref=manifest_ref, write_result=None, complete=True,
                            completion="source_exhausted")
    return result, manifest_ref, block_refs


@pytest.fixture(autouse=True)
def _tree_binding():
    assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE)
    for name in ("demiflow.execution.dataflow_handles", "demiflow.execution.dataflow_terminals"):
        assert Path(sys.modules[name].__file__).resolve().is_relative_to(WORKTREE)
    yield


# --- T001 open_terminal ----------------------------------------------------
def test_t001():
    for kind in ("take", "iter_rows", "count", "write_datasink"):
        ex = make_execution(kind=kind)
        handle = terminals.open_terminal(ex.request, execution=ex)
        assert isinstance(handle, handles.RuntimeActionHandle)
        assert handle.request is ex.request
        handle.close()
    ex = make_execution(kind="take")
    with pytest.raises(DataflowError) as ei:
        terminals.open_terminal(ActionRequest(kind="bogus"), execution=ex)
    assert ei.value.code == "INVALID_CONTRACT"


# --- T002 collect_rows -----------------------------------------------------
def test_t002():
    rows = [{"id": i} for i in range(5)]
    ex = make_execution(records=[make_record(i, row=r) for i, r in enumerate(rows)])
    handle = open_handle(ex)
    assert terminals.collect_rows(handle, limit=None, bounds=make_bounds()) == rows

    ex0 = make_execution(records=[make_record(i) for i in range(3)], arguments={"limit": 0})
    h0 = open_handle(ex0)
    assert terminals.collect_rows(h0, limit=0, bounds=make_bounds()) == []
    assert ex0.subscriptions[0].read_calls == 0  # zero-read

    ex2 = make_execution(records=[make_record(i) for i in range(5)])
    h2 = open_handle(ex2)
    got = terminals.collect_rows(h2, limit=2, bounds=make_bounds())
    assert len(got) == 2
    assert ex2.satisfy_calls == 1 and ex2.finish_calls == 1
    assert not ex2.cancel_calls

    # take_all over the resident byte budget fails outright (no truncated success)
    big = [{"id": i, "blob": "x" * 200} for i in range(4)]
    exb = make_execution(records=[make_record(i, row=r) for i, r in enumerate(big)])
    hb = open_handle(exb)
    with pytest.raises(DataflowError) as ei:
        terminals.collect_rows(hb, limit=None, bounds=make_bounds(max_resident_bytes=50))
    assert ei.value.code == "BUDGET_EXCEEDED"
    assert exb.cancel_calls and exb.satisfy_calls == 0 and exb.finish_calls == 0

    exl = make_execution(records=[make_record(i) for i in range(3)])
    hl = open_handle(exl)
    with pytest.raises(DataflowError) as ei:
        terminals.collect_rows(hl, limit=10, bounds=make_bounds(max_output_rows=5))
    assert ei.value.code == "BUDGET_EXCEEDED"

    # bounded take on an infinite queue source terminates and cancels upstream
    exinf = make_execution(infinite=True)
    hinf = open_handle(exinf)
    got_inf = terminals.collect_rows(hinf, limit=3, bounds=make_bounds())
    assert len(got_inf) == 3
    assert exinf.satisfy_calls == 1 and exinf.finish_calls == 1
    assert not exinf.cancel_calls
    assert exinf.subscriptions[0].closed


# --- T003 iterate_rows -----------------------------------------------------
def test_t003():
    store = FakeStore()
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    payload = _encode_payload({"id": 9, "tag": "ref"}, max_bytes=1 << 20)
    ref = store.put(iter([payload]), max_bytes=len(payload) + 1, media_type="application/vnd.demiflow.record.v3")
    records = [make_record(0, row={"id": 0}), make_record(1, payload_ref=ref)]
    ex = make_execution(records=records, store=store)
    handle = open_handle(ex)
    assert list(terminals.iterate_rows(handle)) == [{"id": 0}, {"id": 9, "tag": "ref"}]

    # Normal consumer close stops upstream and seals its bounded prefix before cleanup.
    ex2 = make_execution(records=[make_record(i) for i in range(5)])
    h2 = open_handle(ex2)
    it = terminals.iterate_rows(h2)
    next(it)
    it.close()
    assert ex2.satisfy_calls == 1 and ex2.finish_calls == 1
    assert not ex2.cancel_calls
    assert ex2.subscriptions[0].closed
    assert ex2.close_calls >= 1


# --- T004 iterate_batches --------------------------------------------------
def test_t004():
    rows = [{"a": i, "b": float(i)} for i in range(5)]
    ex = make_execution(records=[make_record(i, row=r) for i, r in enumerate(rows)])
    handle = open_handle(ex)
    batches = list(terminals.iterate_batches(handle, batch_size=2, batch_format="list", drop_last=False))
    assert [len(b) for b in batches] == [2, 2, 1]

    exd = make_execution(records=[make_record(i, row=r) for i, r in enumerate(rows)])
    hd = open_handle(exd)
    dropped = list(terminals.iterate_batches(hd, batch_size=2, batch_format="list", drop_last=True))
    assert [len(b) for b in dropped] == [2, 2]  # partial tail dropped

    import pyarrow as pa
    exp = make_execution(records=[make_record(i, row=r) for i, r in enumerate(rows)])
    hp = open_handle(exp)
    arrow = list(terminals.iterate_batches(hp, batch_size=5, batch_format="pyarrow", drop_last=False))
    assert isinstance(arrow[0], pa.Table) and arrow[0].num_rows == 5

    import pandas as pd
    expd = make_execution(records=[make_record(i, row=r) for i, r in enumerate(rows)])
    hpd = open_handle(expd)
    frame = list(terminals.iterate_batches(hpd, batch_size=5, batch_format="pandas", drop_last=False))
    assert isinstance(frame[0], pd.DataFrame)

    import numpy as np
    exn = make_execution(records=[make_record(i, row=r) for i, r in enumerate(rows)])
    hn = open_handle(exn)
    nd = list(terminals.iterate_batches(hn, batch_size=5, batch_format="numpy", drop_last=False))
    assert isinstance(nd[0]["a"], np.ndarray)

    # conversion peak over the batch byte budget is rejected, not silently copied
    wide = [{"a": "x" * 300} for _ in range(3)]
    exw = make_execution(records=[make_record(i, row=r) for i, r in enumerate(wide)],
                         bounds=make_bounds(max_batch_bytes=100))
    hw = open_handle(exw)
    with pytest.raises(DataflowError) as ei:
        list(terminals.iterate_batches(hw, batch_size=3, batch_format="list", drop_last=False))
    assert ei.value.code == "BUDGET_EXCEEDED"


# --- T005 materialized_handle ----------------------------------------------
def test_t005():
    store = FakeStore()
    rows = [{"id": i} for i in range(4)]
    result, manifest_ref, block_refs = build_materialized(store, rows)
    ex = make_execution(store=store, kind="materialize")
    handle = terminals.materialized_handle(result, execution=ex)
    assert isinstance(handle, handles.StoredMaterializedHandle)
    reads_before = store.reads
    assert list(handle.rows()) == rows
    assert list(handle.rows()) == rows  # second read does not re-run the source/model
    assert store.reads > reads_before
    handle.release()


# --- T006 write_result -----------------------------------------------------
def test_t006():
    wr = WriteResult(written_rows=3, failed_rows=0, blocks_written=1, target="t")
    ok = TerminalResult(kind="write_datasink", rows=3, scalar=None, result_ref=None, write_result=wr,
                        complete=True, completion="source_exhausted")
    assert terminals.write_result(ok, kind="write_datasink") is wr

    # commit failure with non-zero written_rows is never a success
    bad = TerminalResult(kind="write_datasink", rows=3, scalar=None, result_ref=None, write_result=wr,
                         complete=False, completion="incomplete")
    with pytest.raises(DataflowError) as ei:
        terminals.write_result(bad, kind="write_datasink")
    assert ei.value.code == "UNKNOWN_EFFECT" and ei.value.retryable is False

    with pytest.raises(DataflowError) as ei:
        terminals.write_result(ok, kind="take")
    assert ei.value.code == "INVALID_CONTRACT"


# --- T007 inspect_result ---------------------------------------------------
def test_t007():
    schema = {"columns": [{"name": "a"}, {"name": "b"}]}
    res = TerminalResult(kind="schema", rows=None, scalar=schema, result_ref=None,
                         write_result=None, complete=True, completion="source_exhausted")
    ex = make_execution(result=res, kind="schema", arguments={"fetch_if_missing": False})
    handle = open_handle(ex)
    assert terminals.inspect_result(handle, kind="schema") == schema
    assert terminals.inspect_result(handle, kind="columns") == ["a", "b"]
    handle.close()

    # unknown schema with fetch_if_missing=False is not guessed as an empty table
    unknown = TerminalResult(kind="schema", rows=None, scalar=None, result_ref=None,
                             write_result=None, complete=True, completion="source_exhausted")
    exu = make_execution(result=unknown, kind="schema", arguments={"fetch_if_missing": False})
    hu = open_handle(exu)
    assert terminals.inspect_result(hu, kind="schema") is None
    hu.close()

    # unexecuted stats are explicit, not a silent empty success (stats is an
    # inspect kind; the producing terminal is aggregate)
    exs = make_execution(result=TerminalResult(kind="aggregate", rows=None, scalar=None,
                         result_ref=None, write_result=None, complete=True,
                         completion="source_exhausted"), kind="aggregate")
    hs = open_handle(exs)
    with pytest.raises(DataflowError) as ei:
        terminals.inspect_result(hs, kind="stats")
    assert ei.value.code == "INVALID_CONTRACT"
    hs.close()

    # collect_summary consumes the bounded execution result, not a guessed value
    summary = {"count": 4, "min": 0, "max": 3}
    exc = make_execution(result=TerminalResult(kind="collect_summary", rows=None, scalar=summary,
                         result_ref=None, write_result=None, complete=True,
                         completion="source_exhausted"), kind="collect_summary")
    hc = open_handle(exc)
    assert terminals.inspect_result(hc, kind="collect_summary") == summary
    hc.close()


# --- T008 RuntimeActionHandle.__init__ -------------------------------------
def test_t008():
    ex = make_execution()
    bounds = make_bounds()
    handle = handles.RuntimeActionHandle(ex, bounds=bounds)
    assert handle.request is ex.request
    assert handle.services is ex.services
    assert handle.bounds is bounds
    assert len(ex.subscriptions) == 1
    handle.close()


# --- T009 RuntimeActionHandle.rows -----------------------------------------
def test_t009():
    ex = make_execution(records=[make_record(i) for i in range(3)])
    handle = handles.RuntimeActionHandle(ex, bounds=make_bounds())
    records = list(handle.rows())
    assert [r.sequence for r in records] == [0, 1, 2]
    handle.close()

    ex2 = make_execution(records=[make_record(i) for i in range(3)])
    h2 = handles.RuntimeActionHandle(ex2, bounds=make_bounds())
    h2.rows()
    with pytest.raises(DataflowError) as ei:
        h2.rows()
    assert ei.value.code == "INVALID_CONTRACT"
    h2.close()


# --- T010 RuntimeActionHandle.result ---------------------------------------
def test_t010():
    res = TerminalResult(kind="count", rows=None, scalar=7, result_ref=None, write_result=None,
                         complete=True, completion="source_exhausted")
    ex = make_execution(result=res, kind="count")
    handle = handles.RuntimeActionHandle(ex, bounds=make_bounds())
    assert handle.result() is res
    assert handle.result() is res  # cached
    handle.close()

    failure = DataflowError("UNKNOWN_EFFECT", "boom")
    exf = make_execution(terminal_state="failed", failure=failure, kind="count")
    hf = handles.RuntimeActionHandle(exf, bounds=make_bounds())
    with pytest.raises(DataflowError) as ei:
        hf.result()
    assert ei.value is failure
    with pytest.raises(DataflowError):
        hf.close()  # close does not swallow the business failure

    exc = make_execution(terminal_state="cancelled", kind="count")
    hc = handles.RuntimeActionHandle(exc, bounds=make_bounds())
    with pytest.raises(DataflowError) as ei:
        hc.result()
    assert ei.value.code == "CANCELLED"
    with pytest.raises(DataflowError):
        hc.close()

    incomplete = TerminalResult(kind="count", rows=None, scalar=None, result_ref=None,
                                write_result=None, complete=False, completion="incomplete")
    exi = make_execution(result=incomplete, kind="count")
    hi = handles.RuntimeActionHandle(exi, bounds=make_bounds())
    with pytest.raises(DataflowError) as ei:
        hi.result()
    assert ei.value.code == "UNKNOWN_EFFECT"
    with pytest.raises(DataflowError):
        hi.close()


# --- T011 RuntimeActionHandle.close ----------------------------------------
def test_t011():
    ex = make_execution(records=[make_record(i) for i in range(3)])
    handle = handles.RuntimeActionHandle(ex, bounds=make_bounds())
    handle.close()
    handle.close()  # idempotent
    assert ex.cancel_calls  # not finished -> cancel
    assert ex.close_calls == 1

    exf = make_execution(records=[make_record(i) for i in range(3)])
    hf = handles.RuntimeActionHandle(exf, bounds=make_bounds())
    list(hf.rows())  # finish -> succeeded
    hf.close()
    assert not exf.cancel_calls  # finished -> release only, no cancel
    assert exf.subscriptions[0].closed


# --- T012 StoredMaterializedHandle.__init__ --------------------------------
def test_t012():
    store = FakeStore()
    rows = [{"id": i} for i in range(3)]
    result, manifest_ref, block_refs = build_materialized(store, rows)
    handle = handles.StoredMaterializedHandle(result, store=store, owner="owner-a")
    assert handle.num_blocks() == 1
    assert store.is_pinned(manifest_ref, "owner-a")
    assert all(store.is_pinned(r, "owner-a") for r in block_refs)
    handle.release()

    # corrupt manifest releases the pins it took and fails
    store2 = FakeStore()
    bad_raw = b"not json"
    bad_ref = store2.put(iter([bad_raw]), max_bytes=len(bad_raw) + 1, media_type="application/json")
    bad_result = TerminalResult(kind="materialize", rows=0, scalar=None, result_ref=bad_ref,
                                write_result=None, complete=True, completion="source_exhausted")
    with pytest.raises(DataflowError) as ei:
        handles.StoredMaterializedHandle(bad_result, store=store2, owner="owner-b")
    assert ei.value.code == "CORRUPT_ARTIFACT"
    assert not store2.is_pinned(bad_ref, "owner-b")


# --- T013 StoredMaterializedHandle.rows ------------------------------------
def test_t013():
    store = FakeStore()
    rows = [{"t": (1, 2), "b": b"\x00\xff", "n": None, "f": -0.0, "l": [1, 2, 3]},
            {"t": (3,), "b": b"", "n": None, "f": 3.14159, "l": []}]
    result, _, _ = build_materialized(store, rows, num_blocks=2)
    handle = handles.StoredMaterializedHandle(result, store=store, owner="o")
    got = list(handle.rows())
    assert got == rows
    assert got[0]["t"] == (1, 2) and isinstance(got[0]["b"], bytes)
    assert got[0]["l"] == [1, 2, 3] and got[1]["l"] == []
    assert list(handle.rows()) == rows  # re-read, no re-run
    handle.release()


# --- T014 StoredMaterializedHandle.release ---------------------------------
def test_t014():
    store = FakeStore()
    rows = [{"id": i} for i in range(2)]
    result, manifest_ref, block_refs = build_materialized(store, rows)
    a = handles.StoredMaterializedHandle(result, store=store, owner="owner-a")
    b = handles.StoredMaterializedHandle(result, store=store, owner="owner-b")
    a.release()
    a.release()  # idempotent
    # owner-b still holds its pins and the data survives
    assert store.is_pinned(manifest_ref, "owner-b")
    assert store.has_data(manifest_ref)
    assert list(b.rows()) == rows
    with pytest.raises(RuntimeError):
        list(a.rows())
    b.release()
    assert not store.has_data(manifest_ref)


# --- T015 StoredMaterializedHandle.num_blocks ------------------------------
def test_t015():
    store = FakeStore()
    rows = [{"id": i} for i in range(6)]
    result, _, block_refs = build_materialized(store, rows, num_blocks=3)
    handle = handles.StoredMaterializedHandle(result, store=store, owner="o")
    assert handle.num_blocks() == len(block_refs) == 3
    handle.release()
    with pytest.raises(RuntimeError):
        handle.num_blocks()
# Integrated lanes use the one production error vocabulary.
from demiflow.execution.contracts.dataflow_errors import DataflowError
