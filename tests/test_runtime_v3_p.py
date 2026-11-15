"""G-P unit contract tests for lane P (physical placement and resource policy).

Each test calls the real production symbol in this worktree. Lane C owns the
shared DTO/codec/error modules; when they are not yet merged into this isolated
tree, a strict spec-bound test-domain fake (mirroring interfaces.pyi) stands in
for them. No production fallback exists. An in-process assertion binds every
loaded demiflow module to this worktree (IL012).
"""
from __future__ import annotations

import hashlib
import importlib
import json
import sys
import types
from dataclasses import asdict, dataclass, fields, is_dataclass
from pathlib import Path

import pytest

import demiflow

ROOT = Path(__file__).resolve().parents[1]


def _assert_worktree() -> None:
    assert Path(demiflow.__file__).resolve().is_relative_to(ROOT)
    for name, module in list(sys.modules.items()):
        if not name.startswith("demiflow"):
            continue
        path = getattr(module, "__file__", None)
        if path is None:
            continue
        assert Path(path).resolve().is_relative_to(ROOT), f"module outside worktree: {name} -> {path}"


# --- Spec-bound test-domain fakes for lane C (only installed when absent). ---

_ERROR_CODES = frozenset({
    "INVALID_CONTRACT", "UNSUPPORTED_CAPABILITY", "RESOURCE_INFEASIBLE", "BUDGET_EXCEEDED",
    "SOURCE_CHANGED", "CODE_MISMATCH", "FENCED", "CANCELLED", "DEADLINE_EXCEEDED",
    "CORRUPT_ARTIFACT", "UNKNOWN_EFFECT", "INCOMPATIBLE_HISTORY", "INTERNAL_PROTOCOL",
})


def _build_dataflow_module() -> types.ModuleType:
    mod = types.ModuleType("demiflow.execution.contracts.dataflow")
    mod.__file__ = str(ROOT / "demiflow/execution/contracts/dataflow.py")

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
    class InputPort:
        name: str
        requirement: str
        ordering: str
        schema_ref: object = None

    @dataclass(frozen=True)
    class KernelSpec:
        kind: str
        implementation_ref: object
        configuration: dict
        inputs: tuple
        state: str
        effect: str
        resource_role: str
        profile: object
        output_bound_bytes: int
        mergeable: bool

    @dataclass(frozen=True)
    class GraphNode:
        occurrence_id: str
        semantic_id: str
        kind: str
        kernel: object
        program_ref: object
        shared_token: str

    @dataclass(frozen=True)
    class GraphEdge:
        edge_id: str
        producer: str
        output_port: str
        consumer: str
        input_port: str

    @dataclass(frozen=True)
    class DataflowGraph:
        schema: str
        action_id: str
        dag_id: str
        nodes: tuple
        edges: tuple
        terminals: tuple
        sources: tuple

    @dataclass(frozen=True)
    class SourceSnapshot:
        source_id: str
        kind: str
        immutable_identity: dict
        finite: bool
        replay: str
        manifest_ref: object = None

    @dataclass(frozen=True)
    class SourceSplit:
        split_id: str
        source_id: str
        locator: dict
        start_cursor: dict
        estimated_bytes: object
        required_domain: str

    @dataclass(frozen=True)
    class StageSpec:
        stage_id: str
        occurrences: tuple
        partitions: tuple
        profile: object
        replica_min: int
        replica_target: int
        replica_max: int
        resident_bytes: int
        output_bytes: int
        state_bytes: int
        kernel_refs: tuple

    @dataclass(frozen=True)
    class ExchangeSpec:
        channel_id: str
        edge_id: str
        producer_partitions: tuple
        consumer_partitions: tuple
        mode: str
        routing: str
        keys: tuple
        scope_id: object
        route_epoch: int
        credit_rows: int
        credit_bytes: int
        storage_bytes: int
        read_domain: str
        reason: str

    @dataclass(frozen=True)
    class RuntimePolicy:
        provider: str
        durability: str
        default_partitions: int
        max_replicas: int
        fusion: bool
        checkpoint_rows: int
        checkpoint_interval_ms: int
        fairness_quantum_bytes: int
        control_timeout_ms: int
        store_id: str
        required_recovery: str

    @dataclass(frozen=True)
    class ResidencyPlan:
        simultaneous_groups: tuple
        control_reserved_bytes: int
        consumer_reserved_slots: int
        required_spill_bytes: int

    @dataclass(frozen=True)
    class FiniteScope:
        scope_id: str
        generation: int
        membership: tuple
        completion_condition: str

    @dataclass(frozen=True)
    class PhysicalDataflow:
        schema: str
        dag_id: str
        plan_id: str
        plan_revision: int
        stages: tuple
        exchanges: tuple
        residency: object
        scopes: tuple
        sources: tuple
        splits: tuple
        policy: object
        bounds: object
        recovery: str
        limitations: tuple
        constants_ref: object

    @dataclass(frozen=True)
    class CapabilityReport:
        supported: bool
        reasons: tuple
        recovery: str
        required_domains: tuple
        required_profiles: tuple

    @dataclass(frozen=True)
    class HostDescriptor:
        host_id: str
        provider: str
        runtime_revision: str
        store_domains: tuple
        logical_resources: dict
        isolation_capabilities: tuple

    for cls in (
        ArtifactRef, Bounds, InputPort, KernelSpec, GraphNode, GraphEdge, DataflowGraph,
        SourceSnapshot, SourceSplit, StageSpec, ExchangeSpec, RuntimePolicy, ResidencyPlan,
        FiniteScope, PhysicalDataflow, CapabilityReport, HostDescriptor,
    ):
        setattr(mod, cls.__name__, cls)
    return mod


def _build_errors_module() -> types.ModuleType:
    mod = types.ModuleType("demiflow.execution.contracts.dataflow_errors")
    mod.__file__ = str(ROOT / "demiflow/execution/contracts/dataflow_errors.py")

    class DataflowError(Exception):
        def __init__(self, code, message, *, retryable=False, context=None):
            if code not in _ERROR_CODES:
                raise ValueError(f"unknown error code {code}")
            if code == "UNKNOWN_EFFECT" and retryable:
                raise ValueError("UNKNOWN_EFFECT must not be retryable")
            super().__init__(message)
            self.code = code
            self.message = message
            self.retryable = bool(retryable)
            self.context = dict(context or {})

    mod.DataflowError = DataflowError
    return mod


def _build_codec_module() -> types.ModuleType:
    mod = types.ModuleType("demiflow.execution.contracts.dataflow_codec")
    mod.__file__ = str(ROOT / "demiflow/execution/contracts/dataflow_codec.py")

    def project(value):
        # ArtifactRef logical projection drops physical store key/location.
        if is_dataclass(value):
            if type(value).__name__ == "ArtifactRef":
                return {"digest": value.digest, "size_bytes": value.size_bytes, "media_type": value.media_type}
            return {f.name: project(getattr(value, f.name)) for f in fields(value) if f.name != "plan_id"}
        if isinstance(value, dict):
            return {str(k): project(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [project(v) for v in value]
        return value

    def compute_physical_digest(physical):
        canonical = json.dumps(project(physical), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    mod.compute_physical_digest = compute_physical_digest
    return mod


def _ensure_lane_c() -> None:
    for name, builder in (
        ("demiflow.execution.contracts.dataflow", _build_dataflow_module),
        ("demiflow.execution.contracts.dataflow_errors", _build_errors_module),
        ("demiflow.execution.contracts.dataflow_codec", _build_codec_module),
    ):
        try:
            importlib.import_module(name)
        except ImportError:
            module = builder()
            sys.modules[name] = module
            parent_name, attr = name.rsplit(".", 1)
            setattr(sys.modules[parent_name], attr, module)


_ensure_lane_c()

from demiflow.execution.contracts.dataflow import (  # noqa: E402
    ArtifactRef, Bounds, CapabilityReport, DataflowGraph, ExchangeSpec, FiniteScope,
    GraphEdge, GraphNode, HostDescriptor, InputPort, KernelSpec, PhysicalDataflow,
    ResidencyPlan, RuntimePolicy, SourceSnapshot, SourceSplit, StageSpec,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError  # noqa: E402
from demiflow.planning import dataflow_place as place  # noqa: E402
from demiflow.planning import dataflow_policy as policy_mod  # noqa: E402
from demiflow.resources.contracts import ResourceProfile  # noqa: E402


@pytest.fixture(autouse=True)
def _worktree_binding():
    _assert_worktree()
    yield
    _assert_worktree()


# --- Construction helpers (test domain). ---

MiB = 1024 * 1024


def _profile(cpu=1000, memory=8 * MiB, gpu=0, isolation=None):
    requests = {"cpu_millis": cpu, "memory_bytes": memory, "scratch_bytes": MiB}
    if gpu:
        requests["accelerators"] = [{"type": "gpu", "count": gpu}]
    requirements = {"isolation": isolation} if isolation else None
    return ResourceProfile(requests, None, requirements)


def _bounds(**overrides):
    base = dict(
        max_nodes=4096, max_edges=16384, max_partitions=16, max_depth=64,
        max_control_bytes=MiB, max_row_bytes=MiB, max_batch_bytes=MiB,
        max_resident_bytes=64 * MiB, max_spill_bytes=16 * MiB, max_state_bytes=MiB,
        max_pending_events=1024, max_open_files=64, max_output_rows=100000, timeout_ms=600000,
    )
    base.update(overrides)
    return Bounds(**base)


def _policy(**overrides):
    base = dict(
        provider="local", durability="ephemeral", default_partitions=1, max_replicas=8,
        fusion=True, checkpoint_rows=512, checkpoint_interval_ms=1000,
        fairness_quantum_bytes=MiB, control_timeout_ms=30000, store_id="local",
        required_recovery="attempt_only",
    )
    base.update(overrides)
    return RuntimePolicy(**base)


def _kernel(kind, role="cpu", profile=None, *, state="none", effect="pure", inputs=None,
            output_bound_bytes=MiB, mergeable=True, configuration=None):
    return KernelSpec(
        kind=kind, implementation_ref=None, configuration=configuration or {},
        inputs=tuple(inputs if inputs is not None else (InputPort("in", "incremental", "none"),)),
        state=state, effect=effect, resource_role=role,
        profile=profile if profile is not None else _profile(),
        output_bound_bytes=output_bound_bytes, mergeable=mergeable,
    )


def _node(occ, kind, kernel, *, node_kind="transform"):
    return GraphNode(occ, "sem-" + occ, node_kind, kernel, None, "0")


def _edge(producer, consumer, *, input_port="in", output_port="out"):
    return GraphEdge(f"edge-{producer}-{consumer}-{input_port}", producer, output_port, consumer, input_port)


def _graph(nodes, edges, terminals, sources):
    return DataflowGraph("demiflow.dataflow_graph.v3", "action-1", "dag-1",
                         tuple(nodes), tuple(edges), tuple(terminals), tuple(sources))


def _host(host_id="host-1", *, provider="local", cpu=64.0, memory=4096 * MiB, gpu=0.0, isolation=()):
    return HostDescriptor(host_id, provider, "rev-1", ("local",),
                          {"cpu": cpu, "memory_bytes": memory, "gpu": gpu}, tuple(isolation))


def _snapshot(source_id, *, replay="seek", finite=True):
    return SourceSnapshot(source_id, "items", {}, finite, replay, None)


def _split(split_id, source_id, *, domain="local"):
    return SourceSplit(split_id, source_id, {}, {}, None, domain)


def _constants():
    return ArtifactRef("c" * 64, 16, "local", "constants", "application/json")


def _cpu_gpu_io_graph():
    cpu = _node("src", "source", _kernel("items_source", "cpu", inputs=()), node_kind="source")
    gpu = _node("model", "model_map", _kernel("model_map", "gpu", _profile(gpu=1), state="adapter", effect="journaled"))
    io = _node("sink", "write", _kernel("write_file", "io", inputs=(InputPort("in", "incremental", "none"),)), node_kind="terminal")
    edges = [_edge("src", "model"), _edge("model", "sink")]
    return _graph([cpu, gpu, io], edges, ("sink",), ("src",))


# --- P001 ---

def test_p001():
    graph = _cpu_gpu_io_graph()
    snapshots = (_snapshot("src"),)
    splits = (_split("split-0", "src"),)
    hosts = (_host(gpu=1.0, isolation=("gpu",)),)
    physical = place.compile_physical_dataflow(
        graph, snapshots, splits, policy=_policy(), hosts=hosts, bounds=_bounds(), constants_ref=_constants())
    assert isinstance(physical, PhysicalDataflow)
    assert physical.dag_id == graph.dag_id
    assert len(physical.plan_id) == 64
    roles = [stage.profile.requests.get("accelerators") for stage in physical.stages]
    assert any(roles)  # GPU stage preserved, not collapsed to CPU
    place.validate_physical_dataflow(graph, physical)

    # GPU requirement must not silently degrade to a CPU success.
    with pytest.raises(DataflowError) as exc:
        place.compile_physical_dataflow(
            graph, snapshots, splits, policy=_policy(), hosts=(_host(gpu=0.0),),
            bounds=_bounds(), constants_ref=_constants())
    assert exc.value.code == "RESOURCE_INFEASIBLE"

    # Local and Ray keep the same logical coverage (layout may differ).
    ray = place.compile_physical_dataflow(
        graph, snapshots, splits, policy=_policy(provider="ray_core"),
        hosts=(_host(provider="ray_core", gpu=1.0, isolation=("gpu",)),),
        bounds=_bounds(), constants_ref=_constants())
    assert {occ for s in ray.stages for occ in s.occurrences} == {occ for s in physical.stages for occ in s.occurrences}
    assert ray.dag_id == physical.dag_id


# --- P002 ---

def test_p002():
    graph = _cpu_gpu_io_graph()
    stages = place.partition_stages(graph, policy=_policy(), bounds=_bounds())
    assert len(stages) == 3  # CPU/GPU/IO are independent, never one chain-group
    roles = {stage.occurrences[0]: next(n.kernel.resource_role for n in graph.nodes if n.occurrence_id == stage.occurrences[0]) for stage in stages}
    assert set(roles.values()) == {"cpu", "gpu", "io"}

    stateful = _node("st", "stateful_map", _kernel("stateful_map", "cpu", state="auto_json"))
    src = _node("s", "source", _kernel("items_source", "cpu", inputs=()), node_kind="source")
    g2 = _graph([src, stateful], [_edge("s", "st")], ("st",), ("s",))
    stages2 = place.partition_stages(g2, policy=_policy(default_partitions=4), bounds=_bounds())
    stateful_stage = next(s for s in stages2 if "st" in s.occurrences)
    assert stateful_stage.replica_max == 1  # stateful map not auto-scaled into shared instances


# --- P003 ---

def test_p003():
    profile = _profile(memory=8 * MiB)
    a = _node("a", "map", _kernel("map", "cpu", profile))
    b = _node("b", "map", _kernel("map", "cpu", profile))
    src = _node("s", "source", _kernel("items_source", "cpu", profile, inputs=()), node_kind="source")
    graph = _graph([src, a, b], [_edge("s", "a"), _edge("a", "b")], ("b",), ("s",))
    stages = place.partition_stages(graph, policy=_policy(), bounds=_bounds())
    fused = place.select_fusion_groups(graph, stages, policy=_policy())
    ab = next(s for s in fused if "a" in s.occurrences)
    assert "b" in ab.occurrences  # pure same-resource linear chain fused, occurrences kept
    assert ab.resident_bytes == sum(s.resident_bytes for s in stages if s.occurrences[0] in {"a", "b"})

    # Two stages each filling the per-replica budget must not fuse into a double allowance.
    full = _bounds(max_batch_bytes=8 * MiB)
    stages_full = place.partition_stages(graph, policy=_policy(), bounds=full)
    full_stages = tuple(
        StageSpec(s.stage_id, s.occurrences, s.partitions, s.profile, s.replica_min, s.replica_target,
                  s.replica_max, 8 * MiB, s.output_bytes, s.state_bytes, s.kernel_refs)
        if s.occurrences[0] in {"a", "b"} else s
        for s in stages_full
    )
    not_fused = place.select_fusion_groups(graph, full_stages, policy=_policy())
    assert sum(1 for s in not_fused if s.occurrences[0] in {"a", "b"}) == 2

    # Model initialization (stateful GPU) is never fused/replicated.
    model = _node("m", "model_map", _kernel("model_map", "gpu", _profile(gpu=1), state="adapter", effect="journaled"))
    g3 = _graph([src, model], [_edge("s", "m")], ("m",), ("s",))
    fused3 = place.select_fusion_groups(g3, place.partition_stages(g3, policy=_policy(), bounds=_bounds()), policy=_policy())
    assert all(len(s.occurrences) == 1 for s in fused3)


# --- P004 ---

def test_p004():
    src = _node("s", "source", _kernel("items_source", "cpu", inputs=()), node_kind="source")
    mapping = _node("m", "map", _kernel("map", "cpu"))
    graph = _graph([src, mapping], [_edge("s", "m")], ("m",), ("s",))
    stages = place.partition_stages(graph, policy=_policy(), bounds=_bounds())
    exchanges = place.plan_exchanges(graph, stages, policy=_policy(), bounds=_bounds())
    assert len(exchanges) == 1
    assert exchanges[0].mode == "memory_incremental"  # incremental map gets no finite seal
    assert exchanges[0].read_domain == "local"

    sort = _node("sort", "sort", _kernel("sort", "cpu", inputs=(InputPort("in", "finite_complete", "global"),)))
    g2 = _graph([src, sort], [_edge("s", "sort")], ("sort",), ("s",))
    ex2 = place.plan_exchanges(g2, place.partition_stages(g2, policy=_policy(), bounds=_bounds()), policy=_policy(), bounds=_bounds())
    assert ex2[0].mode == "finite_sealed"  # global sort cannot fake completion with memory incremental
    assert ex2[0].scope_id is not None

    ex3 = place.plan_exchanges(graph, stages, policy=_policy(durability="durable"), bounds=_bounds())
    assert ex3[0].mode == "durable_incremental"


# --- P005 ---

def test_p005():
    profile = _profile(memory=8 * MiB)
    src = _node("s", "source", _kernel("items_source", "cpu", profile, inputs=()), node_kind="source")
    prod = _node("p", "map", _kernel("map", "cpu", profile))
    cons = _node("c", "write", _kernel("write_file", "io", profile), node_kind="terminal")
    graph = _graph([src, prod, cons], [_edge("s", "p"), _edge("p", "c")], ("c",), ("s",))
    bounds = _bounds(max_resident_bytes=10 * MiB, max_control_bytes=MiB)
    stages = place.partition_stages(graph, policy=_policy(), bounds=bounds)
    # Force each stage to fill the resident window so producers crowd out the consumer.
    stages = tuple(
        StageSpec(s.stage_id, s.occurrences, s.partitions, s.profile, 1, 1, 1, 9 * MiB, s.output_bytes, s.state_bytes, s.kernel_refs)
        for s in stages
    )
    memory_ex = place.plan_exchanges(graph, stages, policy=_policy(), bounds=bounds)
    with pytest.raises(DataflowError) as exc:
        place.solve_residency(stages, memory_ex, hosts=(_host(),), bounds=bounds)
    assert exc.value.code == "RESOURCE_INFEASIBLE"

    # A durable boundary allows legal staged drain instead of admission failure.
    durable_ex = tuple(
        ExchangeSpec(e.channel_id, e.edge_id, e.producer_partitions, e.consumer_partitions,
                     "durable_incremental", e.routing, e.keys, e.scope_id, e.route_epoch,
                     e.credit_rows, e.credit_bytes, 4 * MiB, e.read_domain, e.reason)
        for e in memory_ex
    )
    residency = place.solve_residency(stages, durable_ex, hosts=(_host(),), bounds=bounds)
    assert len(residency.simultaneous_groups) >= 2
    assert residency.control_reserved_bytes > 0
    assert residency.consumer_reserved_slots >= 1


# --- P006 ---

def test_p006():
    graph = _cpu_gpu_io_graph()
    physical = place.compile_physical_dataflow(
        graph, (_snapshot("src"),), (_split("split-0", "src"),),
        policy=_policy(), hosts=(_host(gpu=1.0, isolation=("gpu",)),), bounds=_bounds(), constants_ref=_constants())
    place.validate_physical_dataflow(graph, physical)

    from dataclasses import replace as dc_replace
    # Dangling stage with an unknown occurrence.
    bad_stage = StageSpec("stage-x", ("ghost",), ("stage-x:p0",), _profile(), 1, 1, 1, MiB, MiB, 0, ())
    with pytest.raises(DataflowError):
        place.validate_physical_dataflow(graph, dc_replace(physical, stages=physical.stages + (bad_stage,)))

    # Two owners for one occurrence.
    dup = dc_replace(physical.stages[0], stage_id="stage-dup")
    with pytest.raises(DataflowError):
        place.validate_physical_dataflow(graph, dc_replace(physical, stages=physical.stages + (dup,)))

    # Unknown read_domain.
    bad_ex = dc_replace(physical.exchanges[0], read_domain="unknown-domain")
    with pytest.raises(DataflowError):
        place.validate_physical_dataflow(graph, dc_replace(physical, exchanges=(bad_ex,) + physical.exchanges[1:]))

    # Source snapshot drift.
    with pytest.raises(DataflowError):
        place.validate_physical_dataflow(graph, dc_replace(physical, sources=(_snapshot("other"),)))


# --- P007 ---

def test_p007():
    src = _node("s", "source", _kernel("items_source", "cpu", inputs=()), node_kind="source")
    actor = _node("a", "actor_map", _kernel("actor_map", "cpu", state="nonrecoverable"))
    graph = _graph([src, actor], [_edge("s", "a")], ("a",), ("s",))
    report = place.infer_recovery(graph, (_snapshot("s"),), policy=_policy(required_recovery="checkpoint"))
    assert report.supported is False
    assert report.recovery == "attempt_only"

    paid = _node("p", "prompt", _kernel("prompt", "remote_api", effect="unknown_replay_forbidden"))
    g2 = _graph([src, paid], [_edge("s", "p")], ("p",), ("s",))
    report2 = place.infer_recovery(g2, (_snapshot("s"),), policy=_policy(required_recovery="checkpoint"))
    assert report2.recovery != "checkpoint"  # unknown paid result never judged retryable
    assert report2.supported is False

    pure = _node("m", "map", _kernel("map", "cpu"))
    g3 = _graph([src, pure], [_edge("s", "m")], ("m",), ("s",))
    report3 = place.infer_recovery(g3, (_snapshot("s", replay="seek"),), policy=_policy(required_recovery="checkpoint"))
    assert report3.supported is True
    assert report3.recovery == "checkpoint"


# --- P008 ---

def test_p008():
    pol, bounds = policy_mod.resolve_runtime_policy({}, {}, provider="local")
    assert isinstance(pol, RuntimePolicy) and isinstance(bounds, Bounds)
    assert bounds.timeout_ms == 600 * 1000
    assert bounds.max_resident_bytes == 256 * MiB
    # Consumers cannot inject divergent defaults.
    pol2, bounds2 = policy_mod.resolve_runtime_policy({}, {}, provider="local")
    assert pol == pol2 and bounds == bounds2

    with pytest.raises(DataflowError):
        policy_mod.resolve_runtime_policy({}, {"memory_bytes": True}, provider="local")  # bool not int
    with pytest.raises(DataflowError):
        policy_mod.resolve_runtime_policy({}, {"max_output_rows": 0}, provider="local")  # 0 not unlimited
    with pytest.raises(DataflowError):
        policy_mod.resolve_runtime_policy({"bogus": 1}, {}, provider="local")  # unknown option
    with pytest.raises(DataflowError):
        policy_mod.resolve_runtime_policy({}, {"bogus": 1}, provider="local")  # unknown limit

    tightened, tb = policy_mod.resolve_runtime_policy({"max_nodes": 10}, {}, provider="local")
    assert tb.max_nodes == 10  # explicit input tightens the versioned ceiling
    assert tb.max_nodes <= bounds.max_nodes


# --- P009 ---

def test_p009():
    out = policy_mod.normalize_native_options(
        {"num_cpus": 2, "num_gpus": 1, "memory": 4 * MiB, "concurrency": 3, "partitions": 4},
        operation_kind="row_transform")
    assert out["profile"]["cpu_millis"] == 2000
    assert out["profile"]["memory_bytes"] == 4 * MiB
    assert out["profile"]["accelerators"] == [{"type": "gpu", "count": 1}]
    assert out["replicas"]["target"] == 3
    assert out["partitions"] == 4
    assert policy_mod.normalize_native_options(None, operation_kind="map") == {}

    with pytest.raises(DataflowError) as exc:
        policy_mod.normalize_native_options({"mystery knob": 1}, operation_kind="map")
    assert exc.value.code == "UNSUPPORTED_CAPABILITY"  # unknown never silently dropped
    with pytest.raises(DataflowError) as exc2:
        policy_mod.normalize_native_options({"ray_remote_args": {"num_cpus": 1}}, operation_kind="map")
    assert exc2.value.code == "UNSUPPORTED_CAPABILITY"  # Ray Data tuning explicitly reported