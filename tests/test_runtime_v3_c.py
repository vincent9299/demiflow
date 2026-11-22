"""Lane C acceptance: shared types, codec, identity, errors (cards C001-C088).

Every test calls the real symbol under test. An autouse fixture asserts in
process that ``demiflow`` and every loaded submodule resolve to this worktree,
so an editable install pointing at the main repository cannot produce a pass.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import pickle
import sys
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import pytest

import demiflow
import demiflow.execution.contracts.dataflow as d
import demiflow.execution.contracts.dataflow_codec as c
from demiflow.execution.contracts.dataflow_errors import (
    DATAFLOW_ERROR_CODES,
    DataflowError,
)
from demiflow.resources.contracts import ResourceProfile

WORKTREE = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _bind_worktree():
    assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE)
    for name, module in list(sys.modules.items()):
        if name == "demiflow" or name.startswith("demiflow."):
            path = getattr(module, "__file__", None)
            if path:
                assert Path(path).resolve().is_relative_to(WORKTREE), name


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def bounds(**overrides) -> d.Bounds:
    base = dict(
        max_nodes=100000, max_edges=100000, max_partitions=10000, max_depth=64,
        max_control_bytes=10_000_000, max_row_bytes=1_000_000, max_batch_bytes=1_000_000,
        max_resident_bytes=10_000_000, max_spill_bytes=10_000_000, max_state_bytes=10_000_000,
        max_pending_events=10000, max_open_files=1000, max_output_rows=1_000_000, timeout_ms=60000,
    )
    base.update(overrides)
    return d.Bounds(**base)


PROFILE = ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": 1024})


def aref(key: str | None = None) -> d.ArtifactRef:
    digest = sha("artifact")
    return d.ArtifactRef(
        digest=digest, size_bytes=1, store_id="store",
        key=key or "objects/" + digest, media_type="application/octet-stream",
    )


_WIRE_OVERRIDE = {
    "Record": {"inline": {}},
    "ValueEdge": {"kind": "fixed_dataset", "output_port": "dataset", "input_port": "train"},
    "WorkerEvent": {"payload": {}},
    "ControlReply": {"status": "ok", "error_code": None},
    "QueueCommand": {"operation": "poll"},
    "ScheduleChoice": {"retry_after_ms": 1},
    "SourceCursor": {"offset": {"next": 0}},
}


def _min(spec):
    kind = spec[0]
    if kind == "dto":
        return make(spec[1])
    if kind == "sha256":
        return sha("field")
    if kind == "ident":
        return "id0"
    if kind == "text":
        return "x"
    if kind == "int":
        return spec[1]
    if kind == "bool":
        return False
    if kind == "enum":
        return sorted(spec[1])[0]
    if kind == "lit":
        return spec[1]
    if kind == "decimal":
        return "0"
    if kind == "key":
        return "objects/" + sha("key")
    if kind in ("json", "jmap"):
        return {}
    if kind == "ref":
        return aref()
    if kind == "profile":
        return PROFILE
    if kind in ("opt_sha256", "opt_ident", "opt_text", "opt_int", "opt_bool",
                "opt_jmap", "opt_errorcode", "opt_ref", "opt_dto",
                "opt_writeresult", "opt_row", "opt_reservation"):
        return None
    if kind in ("refs", "dtos", "strs", "strs_nested", "texts", "profiles"):
        return ()
    if kind in ("imap", "fmap", "refmap", "jmapmap"):
        return {}
    raise AssertionError("unhandled spec kind " + kind)


def _nonwire(name):
    if name == "ActionProgram":
        return dict(name="p0", kind="sink", value=object())
    if name == "LoadedProgram":
        return dict(kind="callable", value=object(), digest=sha("loaded"))
    if name == "EncodedRecord":
        return dict(record_id=sha("encoded"), encoding="json", data=b"x", ref=None,
                    encoded_bytes=1, decoded_upper_bytes=1, sequence=0)
    if name == "NativeOperation":
        return dict(kind="sql", configuration={}, bindings={}, factory=object())
    if name == "NativeRunView":
        return dict(limits={}, fence="f0", check_stop=lambda: None)
    if name == "NativeActionView":
        return dict(action_id="a0", context=make("NativeRunView"), object_sinks={},
                    native_image_effects=False)
    if name == "PlanInputBinding":
        return dict(port="in", source=object(), plan=object(), path=())
    if name == "ProgramDeclaration":
        return dict(occurrence_id=sha("decl"), callable_spec=None, source_spec=None,
                    action_program=make("ActionProgram"), code_identity={})
    if name == "CompiledDataflow":
        return dict(graph=make("DataflowGraph"), declarations=(make("ProgramDeclaration"),))
    if name == "ActionRequest":
        return dict(kind=sorted(d._ACTION_KINDS)[0], source=object(), plan=object(),
                    arguments={}, sink=None, programs=(), provider="local", action=object())
    if name == "RuntimeServices":
        return dict(store=object(), journal=object(), bounds=make("Bounds"),
                    assignment=make("WorkerAssignment"), clock_ms=lambda: 0,
                    check_stop=lambda: None, constants={}, programs={})
    if name == "DriverServices":
        return dict(action=object(), journal_service=object(), store=object(),
                    resources=object(), policy=make("RuntimePolicy"), bounds=make("Bounds"),
                    hosts=(), clock_ms=lambda: 0)
    raise AssertionError("no factory for " + name)


def make(type_name, **overrides):
    if type_name in c._WIRE_TYPES:
        cls, fields = c._WIRE_TYPES[type_name]
        kwargs = {field: _min(spec) for field, spec in fields}
        kwargs.update(_WIRE_OVERRIDE.get(type_name, {}))
    else:
        kwargs = _nonwire(type_name)
    kwargs.update(overrides)
    return getattr(d, type_name)(**kwargs)


def _check_dto(name, **overrides):
    cls = getattr(d, name)
    obj = make(name, **overrides)
    assert isinstance(obj, cls)
    first = dataclasses.fields(obj)[0].name
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(obj, first, None)
    if name in c._WIRE_TYPES:
        for field in dataclasses.fields(obj):
            value = getattr(obj, field.name)
            if isinstance(value, Mapping):
                assert isinstance(value, MappingProxyType), field.name
            assert type(value) is not list, field.name
        budget = bounds()
        raw = c.encode_wire(obj, type_name=name, bounds=budget)
        back = c.decode_wire(raw, type_name=name, bounds=budget)
        assert type(back) is cls
        assert c.encode_wire(back, type_name=name, bounds=budget) == raw
    return obj


def _corrupt(spec):
    kind = spec[0]
    if kind == "sha256":
        return "not-hex"
    if kind == "ident":
        return "bad id!"
    if kind == "enum":
        return "__not_an_enum__"
    if kind == "int":
        return -1
    if kind == "bool":
        return "x"
    if kind == "text":
        return ""
    return None


def _negative(name):
    cls, fields = c._WIRE_TYPES[name]
    for field, spec in fields:
        if spec[0] in ("sha256", "ident", "enum", "int", "bool", "text"):
            with pytest.raises(DataflowError):
                make(name, **{field: _corrupt(spec)})
            return
    for field, spec in fields:
        if spec[0] in ("ref", "opt_ref", "dto", "opt_dto"):
            with pytest.raises(DataflowError):
                make(name, **{field: object()})
            return
    raise AssertionError("no corruptible field for " + name)


def _proto_fake(name):
    proto = getattr(d, name)
    namespace = {}
    for member in dir(proto):
        if member.startswith("_"):
            continue
        attr = getattr(proto, member)
        if callable(attr):
            namespace[member] = lambda self, *a, **k: None
        else:
            namespace[member] = None
    if name == "CallableInstance":
        namespace["__call__"] = lambda self, row: None
    return type("Fake" + name, (), namespace)()


def _check_protocol(name):
    proto = getattr(d, name)
    assert getattr(proto, "_is_protocol", False)
    fake = _proto_fake(name)
    assert isinstance(fake, proto)
    assert not isinstance(object(), proto)
    return fake


def _graph(action_id="act-1", swap=False):
    def node(i, kind):
        return d.GraphNode(
            occurrence_id=sha("occ%d" % i), semantic_id=sha("sem%d" % i), kind=kind,
            kernel=make("KernelSpec"), program_ref=None, shared_token=str(i),
        )
    nodes = (node(0, "source"), node(1, "transform"), node(2, "terminal"))
    if swap:
        first = d.GraphEdge(edge_id=sha("e0"), producer=sha("occ1"), output_port="out",
                            consumer=sha("occ0"), input_port="in")
    else:
        first = d.GraphEdge(edge_id=sha("e0"), producer=sha("occ0"), output_port="out",
                            consumer=sha("occ1"), input_port="in")
    second = d.GraphEdge(edge_id=sha("e1"), producer=sha("occ1"), output_port="out",
                         consumer=sha("occ2"), input_port="in")
    return d.DataflowGraph(
        schema="demiflow.dataflow_graph.v3", action_id=action_id, dag_id=sha("placeholder"),
        nodes=nodes, edges=(first, second), terminals=(sha("occ2"),), sources=(sha("occ0"),),
    )


def _physical(partitions=("p0",), constants_key=None, plan_id=None):
    stage = d.StageSpec(
        stage_id="st0", occurrences=(sha("occ1"),), partitions=partitions, profile=PROFILE,
        replica_min=1, replica_target=1, replica_max=2, resident_bytes=1024,
        output_bytes=1024, state_bytes=0, kernel_refs=(),
    )
    return d.PhysicalDataflow(
        schema="demiflow.physical_dataflow.v3", dag_id=sha("dag"), plan_id=plan_id or sha("plan"),
        plan_revision=0, stages=(stage,), exchanges=(),
        residency=d.ResidencyPlan(simultaneous_groups=(), control_reserved_bytes=1024,
                                  consumer_reserved_slots=1, required_spill_bytes=0),
        scopes=(), sources=(), splits=(),
        policy=make("RuntimePolicy"), bounds=bounds(), recovery="attempt_only",
        limitations=(), constants_ref=aref(constants_key),
    )


# --- C001 encode_wire -------------------------------------------------------
def test_c001():
    record = make("Record", inline={"a": 1})
    raw = c.encode_wire(record, type_name="Record", bounds=bounds())
    assert type(raw) is bytes
    frame = json.loads(raw)
    assert frame["schema"] == "demiflow.runtime_wire.v3"
    assert frame["type"] == "Record"
    assert set(frame) == {"schema", "type", "payload"}

    class Evil:
        def __reduce__(self):
            return (eval, ("1",))

    with pytest.raises(DataflowError):
        c.encode_wire(Evil(), type_name="Record", bounds=bounds())
    with pytest.raises(DataflowError):
        c.encode_wire(record, type_name="NotAType", bounds=bounds())


# --- C002 decode_wire -------------------------------------------------------
def test_c002():
    record = make("Record", inline={"a": 1})
    budget = bounds()
    raw = c.encode_wire(record, type_name="Record", bounds=budget)
    back = c.decode_wire(raw, type_name="Record", bounds=budget)
    assert isinstance(back, d.Record)
    frame = json.loads(raw)
    payload = frame["payload"]

    def expect_bad(mutate, label):
        bad = dict(frame)
        mutate(bad)
        with pytest.raises(DataflowError):
            c.decode_wire(json.dumps(bad).encode("utf-8"), type_name="Record", bounds=budget)

    expect_bad(lambda f: f.update(schema="demiflow.runtime_wire.v9"), "version")
    expect_bad(lambda f: f["payload"].update(evil=1), "extra")
    expect_bad(lambda f: f["payload"].pop("sequence"), "missing")
    expect_bad(lambda f: f["payload"].update(payload_ref=None, inline=None), "double")
    compact = json.dumps(frame, separators=(",", ":")).encode()
    with pytest.raises(DataflowError):
        c.decode_wire(compact.replace(b'"sequence":0', b'"sequence":NaN'),
                      type_name="Record", bounds=budget)
    with pytest.raises(DataflowError):
        c.decode_wire(compact.replace(b'"sequence":0', b'"sequence":true'),
                      type_name="Record", bounds=budget)
    with pytest.raises(DataflowError):
        c.decode_wire(b'{"schema":"x","schema":"y","type":"Record","payload":{}}',
                      type_name="Record", bounds=budget)


# --- C003 validate_dataflow_record -----------------------------------------
def test_c003():
    c.validate_dataflow_record(make("Record", inline={}), type_name="Record", bounds=bounds())
    with pytest.raises(DataflowError):
        d.Bounds(**{f.name: 0 for f in dataclasses.fields(d.Bounds)})
    cursor = make("SourceCursor")
    with pytest.raises(DataflowError):
        c.validate_dataflow_record(make("SourceCursor", next_sequence=-1),
                                   type_name="SourceCursor", bounds=bounds())
    with pytest.raises(DataflowError):
        c.validate_dataflow_record(cursor, type_name="SourceCursor",
                                   bounds=bounds(max_control_bytes=1))
    with pytest.raises(DataflowError):
        c.validate_dataflow_record(object(), type_name="SourceCursor", bounds=bounds())


# --- C004 compute_graph_digest ---------------------------------------------
def test_c004():
    graph = _graph()
    digest = c.compute_graph_digest(graph)
    assert len(digest) == 64 and digest == digest.lower()
    budget = bounds()
    rebuilt = c.decode_wire(c.encode_wire(graph, type_name="DataflowGraph", bounds=budget),
                            type_name="DataflowGraph", bounds=budget)
    assert c.compute_graph_digest(rebuilt) == digest
    assert c.compute_graph_digest(_graph(swap=True)) != digest
    other = d.DataflowGraph(schema="demiflow.dataflow_graph.v3", action_id="act-1",
                            dag_id=sha("different"), nodes=graph.nodes, edges=graph.edges,
                            terminals=graph.terminals, sources=graph.sources)
    assert c.compute_graph_digest(other) == digest
    assert c.compute_graph_digest(_graph(action_id="act-2")) != digest


# --- C005 compute_physical_digest ------------------------------------------
def test_c005():
    physical = _physical()
    digest = c.compute_physical_digest(physical)
    assert len(digest) == 64
    assert c.compute_physical_digest(_physical(partitions=("p0", "p1"))) != digest
    assert c.compute_physical_digest(_physical(constants_key="objects/elsewhere")) == digest
    assert c.compute_physical_digest(_physical(plan_id=sha("other"))) == digest
    budget = bounds()
    rebuilt = c.decode_wire(c.encode_wire(physical, type_name="PhysicalDataflow", bounds=budget),
                            type_name="PhysicalDataflow", bounds=budget)
    assert c.compute_physical_digest(rebuilt) == digest


# --- C006 DataflowError.__init__ -------------------------------------------
def test_c006():
    err = DataflowError("FENCED", "fenced out", retryable=True, context={"scope_id": "s0"})
    assert err.code == "FENCED" and err.retryable is True
    assert isinstance(err.context, MappingProxyType)
    with pytest.raises(ValueError):
        DataflowError("NOT_A_CODE", "x")
    with pytest.raises(ValueError):
        DataflowError("UNKNOWN_EFFECT", "x", retryable=True)
    with pytest.raises(ValueError):
        DataflowError("BUDGET_EXCEEDED", "x", retryable=True)
    assert DataflowError("CAPACITY_PENDING", "temporary capacity", retryable=True).retryable
    with pytest.raises(ValueError):
        DataflowError("FENCED", "x", context={"password": "hunter2"})
    with pytest.raises(ValueError):
        DataflowError("FENCED", "")
    detached = {"k": "v"}
    err2 = DataflowError("CANCELLED", "stop", context=detached)
    detached["k"] = "changed"
    assert err2.context["k"] == "v"


# --- C007..C036 frozen DTOs -------------------------------------------------
def test_c007():
    obj = _check_dto("ArtifactRef")
    with pytest.raises(DataflowError):
        d.ArtifactRef(digest=sha("a"), size_bytes=1, store_id="s", key="../escape",
                      media_type="application/octet-stream")


def test_c008():
    _check_dto("Bounds")
    with pytest.raises(DataflowError):
        bounds(max_depth=0)


def test_c009():
    obj = _check_dto("ActionRequest")
    assert isinstance(obj.arguments, MappingProxyType)
    with pytest.raises(DataflowError):
        make("ActionRequest", kind="__bad__")


def test_c010():
    _check_dto("InputPort")
    _negative("InputPort")


def test_c011():
    obj = _check_dto("KernelSpec")
    assert isinstance(obj.configuration, MappingProxyType)
    _negative("KernelSpec")


def test_c012():
    _check_dto("GraphNode")
    with pytest.raises(DataflowError):
        make("GraphNode", shared_token="not-decimal")


def test_c013():
    _check_dto("GraphEdge")
    _negative("GraphEdge")


def test_c014():
    obj = _check_dto("DataflowGraph")
    assert obj.schema == "demiflow.dataflow_graph.v3"


def test_c015():
    _check_dto("SourceSnapshot")
    _negative("SourceSnapshot")


def test_c016():
    _check_dto("SourceSplit")
    _negative("SourceSplit")


def test_c017():
    _check_dto("SourceCursor")
    with pytest.raises(DataflowError):
        make("SourceCursor", next_sequence=-1)


def test_c018():
    _check_dto("Record", inline={"a": 1})
    with pytest.raises(DataflowError):
        make("Record", inline=None, payload_ref=None)


def test_c019():
    _check_dto("StageSpec")
    with pytest.raises(DataflowError):
        make("StageSpec", replica_min=3, replica_target=1, replica_max=2)


def test_c020():
    _check_dto("ExchangeSpec")
    _negative("ExchangeSpec")


def test_c021():
    _check_dto("RuntimePolicy")
    _negative("RuntimePolicy")


def test_c022():
    obj = _check_dto("PhysicalDataflow")
    assert obj.schema == "demiflow.physical_dataflow.v3"


def test_c023():
    _check_dto("SealedExecution")
    _negative("SealedExecution")


def test_c024():
    _check_dto("ReadyWork")
    _negative("ReadyWork")


def test_c025():
    _check_dto("WorkerLease")
    _negative("WorkerLease")


def test_c026():
    _check_dto("WorkerAssignment")
    _negative("WorkerAssignment")


def test_c027():
    _check_dto("CutRequest")
    with pytest.raises(DataflowError):
        make("CutRequest", participants=("a", "a"))


def test_c028():
    _check_dto("CutContribution")
    _negative("CutContribution")


def test_c029():
    _check_dto("RecoveryImage")
    _negative("RecoveryImage")


def test_c030():
    _check_dto("EffectCommand")
    _negative("EffectCommand")


def test_c031():
    _check_dto("EffectReply")
    _negative("EffectReply")


def test_c032():
    _check_dto("WorkerEvent", payload={"operation_id": sha("op")})
    with pytest.raises(DataflowError):
        make("WorkerEvent", payload=None, payload_ref=None)


def test_c033():
    _check_dto("TerminalResult")
    _negative("TerminalResult")


def test_c034():
    _check_dto("CapabilityReport")
    _negative("CapabilityReport")


def test_c035():
    _check_dto("HostDescriptor")
    _negative("HostDescriptor")


def test_c036():
    _check_dto("HistoricalBinding")
    _negative("HistoricalBinding")


# --- C037..C041 protocols ---------------------------------------------------
def test_c037():
    _check_protocol("ArtifactStore")


def test_c038():
    _check_protocol("SourceReader")


def test_c039():
    _check_protocol("Kernel")


def test_c040():
    _check_protocol("WorkerJournalPort")


def test_c041():
    _check_protocol("HostEndpoint")


# --- C042 PlanInputBinding --------------------------------------------------
def test_c042():
    obj = _check_dto("PlanInputBinding")
    assert type(obj.path) is tuple
    with pytest.raises(DataflowError):
        make("PlanInputBinding", port="bad port!")


# --- C043..C056 DTOs --------------------------------------------------------
def test_c043():
    _check_dto("OperatorDefinition")
    _negative("OperatorDefinition")


def test_c044():
    _check_dto("SourceDefinition")
    _negative("SourceDefinition")


def test_c045():
    _check_dto("TerminalDefinition")
    _negative("TerminalDefinition")


def test_c046():
    _check_dto("ResidencyPlan")
    _negative("ResidencyPlan")


def test_c047():
    obj = _check_dto("EncodedRecord")
    assert obj.data == b"x"
    with pytest.raises(DataflowError):
        make("EncodedRecord", data=None, ref=None)


def test_c048():
    _check_dto("RoutedRecord")
    _negative("RoutedRecord")


def test_c049():
    _check_dto("ProducerCompletion")
    _negative("ProducerCompletion")


def test_c050():
    _check_dto("CreditLease")
    _negative("CreditLease")


def test_c051():
    _check_dto("AggregateState")
    _negative("AggregateState")


def test_c052():
    _check_dto("DomainProof")
    _negative("DomainProof")


def test_c053():
    _check_dto("DataLocation")
    _negative("DataLocation")


def test_c054():
    _check_dto("WorkerStep")
    _negative("WorkerStep")


def test_c055():
    _check_dto("ExecutionStatus")
    _negative("ExecutionStatus")


def test_c056():
    _check_dto("NodeObservation")
    _negative("NodeObservation")


# --- C057/C058 service bundles ---------------------------------------------
def test_c057():
    obj = _check_dto("RuntimeServices")
    assert isinstance(obj.constants, MappingProxyType)
    assert isinstance(obj.programs, MappingProxyType)
    with pytest.raises(DataflowError):
        make("RuntimeServices", bounds=object())


def test_c058():
    obj = _check_dto("DriverServices")
    assert type(obj.hosts) is tuple
    with pytest.raises(DataflowError):
        make("DriverServices", policy=object())


# --- C059..C065 protocols ---------------------------------------------------
def test_c059():
    _check_protocol("ControlTransport")


def test_c060():
    _check_protocol("ActionHandle")


def test_c061():
    _check_protocol("CallableInstance")


def test_c062():
    _check_protocol("NativeEffectTransport")


def test_c063():
    _check_protocol("MaterializedHandle")


def test_c064():
    _check_protocol("ChannelSet")


def test_c065():
    _check_protocol("RoutingState")


# --- C066..C074 DTOs --------------------------------------------------------
def test_c066():
    _check_dto("SourcePoll", status="eoi", record=None, retry_after_ms=0)
    with pytest.raises(DataflowError):
        make("SourcePoll", status="pending", record=None, retry_after_ms=0)


def test_c067():
    _check_dto("ScheduleChoice", retry_after_ms=1)
    with pytest.raises(DataflowError):
        make("ScheduleChoice", work=None, retry_after_ms=0)


def test_c068():
    _check_dto("ArtifactCommand")
    _negative("ArtifactCommand")


def test_c069():
    _check_dto("ArtifactReply")
    _negative("ArtifactReply")


def test_c070():
    _check_dto("ControlReply", status="ok", error_code=None)
    with pytest.raises(DataflowError):
        make("ControlReply", status="error", error_code=None)


def test_c071():
    _check_protocol("SocketConnection")


def test_c072():
    _check_dto("FiniteScope")
    with pytest.raises(DataflowError):
        make("FiniteScope", membership=("a", "a"))


def test_c073():
    obj = _check_dto("ProgramDeclaration")
    assert isinstance(obj.code_identity, MappingProxyType)
    with pytest.raises(DataflowError):
        make("ProgramDeclaration", callable_spec=None, source_spec=None, action_program=None)


def test_c074():
    obj = _check_dto("CompiledDataflow")
    assert isinstance(obj.graph, d.DataflowGraph)
    with pytest.raises(DataflowError):
        make("CompiledDataflow", graph=object())


# --- C075 DataflowError class ----------------------------------------------
def test_c075():
    assert issubclass(DataflowError, Exception)
    assert len(DATAFLOW_ERROR_CODES) == 14
    assert "CAPACITY_PENDING" in DATAFLOW_ERROR_CODES
    err = DataflowError("SOURCE_CHANGED", "changed", context={"source_id": "s0"})
    restored = pickle.loads(pickle.dumps(err))
    assert restored.code == err.code
    assert dict(restored.context) == dict(err.context)
    assert "SOURCE_CHANGED" in str(err)
    with pytest.raises(ValueError):
        DataflowError("FENCED", "x", retryable="yes")


# --- C076..C086 DTOs --------------------------------------------------------
def test_c076():
    _check_dto("QueueBinding")
    _negative("QueueBinding")


def test_c077():
    _check_dto("QueueCommand", operation="poll")
    with pytest.raises(DataflowError):
        make("QueueCommand", operation="acquire", row_ref=None)


def test_c078():
    _check_dto("QueueReply")
    _negative("QueueReply")


def test_c079():
    _check_dto("CompletedControl")
    _negative("CompletedControl")


def test_c080():
    _check_protocol("WorkerControlMailbox")


def test_c081():
    _check_dto("NativeJournalCommand")
    _negative("NativeJournalCommand")


def test_c082():
    _check_dto("NativeJournalReply")
    with pytest.raises(DataflowError):
        make("NativeJournalReply", value={"a": 1}, value_ref=aref())


def test_c083():
    obj = _check_dto("NativeRunView")
    assert isinstance(obj.limits, MappingProxyType)
    with pytest.raises(DataflowError):
        make("NativeRunView", fence="bad fence!")


def test_c084():
    obj = _check_dto("NativeActionView")
    assert isinstance(obj.object_sinks, MappingProxyType)
    with pytest.raises(DataflowError):
        make("NativeActionView", context=object())


def test_c085():
    obj = _check_dto("LoadedProgram")
    with pytest.raises(DataflowError):
        make("LoadedProgram", digest="not-hex")


def test_c086():
    obj = _check_dto("ActionProgram")
    with pytest.raises(DataflowError):
        make("ActionProgram", kind="__bad__")


# --- C087/C088 protocols ----------------------------------------------------
def test_c087():
    _check_protocol("ArtifactAuthorityPort")


def test_c088():
    _check_protocol("DriverAuthority")