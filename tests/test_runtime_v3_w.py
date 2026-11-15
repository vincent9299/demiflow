"""Lane W regression gate against the integrated v3 contracts.

DTO construction uses the delivered closed contracts. Adapter tests patch only
explicit cross-module call boundaries for their unit assertions. Source and
native queue tests retain their real readers, journal/HTTP/SQLite paths where
originally exercised. Carrier doubles are unit evidence, not Local/Ray or
end-to-end acceptance evidence.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import types
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path

import pytest

import demiflow

ROOT = Path(__file__).resolve().parents[1]

# Modules that may be stood in by a spec-bound test-domain fake when absent.
TEST_DOUBLES: set[str] = set()


def _assert_worktree() -> None:
    assert Path(demiflow.__file__).resolve().is_relative_to(ROOT)
    for name, module in list(sys.modules.items()):
        if not name.startswith("demiflow"):
            continue
        path = getattr(module, "__file__", None)
        if path is None:
            continue
        assert Path(path).resolve().is_relative_to(ROOT), f"module outside worktree: {name} -> {path}"


# --- Spec-bound test-domain fakes for lane C / D (installed only when absent). ---

_ERROR_CODES = frozenset({
    "INVALID_CONTRACT", "UNSUPPORTED_CAPABILITY", "RESOURCE_INFEASIBLE", "BUDGET_EXCEEDED",
    "SOURCE_CHANGED", "CODE_MISMATCH", "FENCED", "CANCELLED", "DEADLINE_EXCEEDED",
    "CORRUPT_ARTIFACT", "UNKNOWN_EFFECT", "INCOMPATIBLE_HISTORY", "INTERNAL_PROTOCOL",
})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENT = re.compile(r"[A-Za-z0-9_.:\-]{1,128}\Z")


def _build_errors_module() -> types.ModuleType:
    mod = types.ModuleType("demiflow.execution.contracts.dataflow_errors")

    class DataflowError(Exception):
        def __init__(self, code, message, *, retryable=False, context=None):
            if type(code) is not str or code not in _ERROR_CODES:
                raise ValueError("unknown dataflow error code: " + repr(code))
            if type(message) is not str or not 1 <= len(message) <= 2048:
                raise ValueError("error message must be a bounded nonempty string")
            if type(retryable) is not bool:
                raise ValueError("retryable must be a bool")
            if code == "UNKNOWN_EFFECT" and retryable:
                raise ValueError("UNKNOWN_EFFECT must not be retryable")
            self.code = code
            self.retryable = retryable
            self.context = dict(context or {})
            self._message = message
            super().__init__(code + ": " + message)

    mod.DataflowError = DataflowError
    mod.DATAFLOW_ERROR_CODES = _ERROR_CODES
    return mod


def _build_dataflow_module() -> types.ModuleType:
    mod = types.ModuleType("demiflow.execution.contracts.dataflow")
    errors = sys.modules["demiflow.execution.contracts.dataflow_errors"]
    DataflowError = errors.DataflowError

    def _err(message):
        return DataflowError("INVALID_CONTRACT", message)

    def _sha256(value, label):
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise _err(label + " must be lowercase sha256 hex")
        return value

    def _ident(value, label):
        if type(value) is not str or _IDENT.fullmatch(value) is None:
            raise _err(label + " must be a bounded identifier")
        return value

    def _key(value, label):
        if type(value) is not str or not 1 <= len(value) <= 1024:
            raise _err(label + " must be a bounded key")
        if value.startswith("/") or ".." in value.split("/") or "\\" in value or "?" in value \
                or "#" in value or "@" in value or "://" in value:
            raise _err(label + " must be a safe relative POSIX key")
        return value

    @dataclass(frozen=True)
    class ArtifactRef:
        digest: str
        size_bytes: int
        store_id: str
        key: str
        media_type: str

        def __post_init__(self):
            _sha256(self.digest, "ArtifactRef.digest")
            if type(self.size_bytes) is not int or self.size_bytes < 0:
                raise _err("ArtifactRef.size_bytes must be a nonnegative int")
            _ident(self.store_id, "ArtifactRef.store_id")
            _key(self.key, "ArtifactRef.key")
            if type(self.media_type) is not str or not 1 <= len(self.media_type) <= 256:
                raise _err("ArtifactRef.media_type must be bounded")

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
        kernel: KernelSpec
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
        manifest_ref: object

    @dataclass(frozen=True)
    class SourceSplit:
        split_id: str
        source_id: str
        locator: dict
        start_cursor: dict
        estimated_bytes: object
        required_domain: str

    @dataclass(frozen=True)
    class SourceCursor:
        split_id: str
        offset: dict
        next_sequence: int
        end_of_input: bool
        lease_ref: object
        pending_refs: tuple

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
        residency: ResidencyPlan
        scopes: tuple
        sources: tuple
        splits: tuple
        policy: RuntimePolicy
        bounds: Bounds
        recovery: str
        limitations: tuple
        constants_ref: ArtifactRef

    @dataclass(frozen=True)
    class SealedExecution:
        logical_ref: ArtifactRef
        physical_ref: ArtifactRef
        programs: tuple
        code_pin_ref: ArtifactRef
        identity_bridge_ref: ArtifactRef
        source_refs: tuple
        constants_ref: ArtifactRef

    @dataclass(frozen=True)
    class CutRequest:
        cut_id: str
        sequence: int
        plan_revision: int
        route_epoch: int
        participants: tuple
        final: bool

    @dataclass(frozen=True)
    class CutContribution:
        cut_id: str
        participant_id: str
        boot_id: str
        sources: tuple
        operator_states: dict
        channels: dict
        buffers: tuple
        effects: tuple
        sinks: dict
        final: bool

    @dataclass(frozen=True)
    class RecoveryImage:
        commit_id: str
        cut: CutRequest
        contributions: tuple
        confirmed_sinks: dict
        source_vector: tuple

    @dataclass(frozen=True)
    class CapabilityReport:
        supported: bool
        reasons: tuple
        recovery: str
        required_domains: tuple
        required_profiles: tuple

    @dataclass(frozen=True)
    class HistoricalBinding:
        schema_version: str
        original_plan_ref: ArtifactRef
        original_code_pin_ref: ArtifactRef
        compatible: bool
        reason: object
        restore_image: object

    @dataclass(frozen=True)
    class NodeObservation:
        occurrence_id: str
        attempt: int
        plan_id: str
        observed: bool
        metrics: dict

    for name, value in list(locals().items()):
        if isinstance(value, type) and value.__module__ == mod.__name__ or name in (
                "ArtifactRef", "Bounds", "InputPort", "KernelSpec", "GraphNode", "GraphEdge",
                "DataflowGraph", "SourceSnapshot", "SourceSplit", "SourceCursor", "StageSpec",
                "ExchangeSpec", "RuntimePolicy", "ResidencyPlan", "FiniteScope", "PhysicalDataflow",
                "SealedExecution", "CutRequest", "CutContribution", "RecoveryImage",
                "CapabilityReport", "HistoricalBinding", "NodeObservation"):
            setattr(mod, name, value)
    return mod


def _build_store_module() -> types.ModuleType:
    mod = types.ModuleType("demiflow.execution.dataflow_store")
    errors = sys.modules["demiflow.execution.contracts.dataflow_errors"]
    DataflowError = errors.DataflowError

    class RunArtifactStore:
        """Spec-bound run-rooted store fake: root/store_id/objects/<digest>."""

        def __init__(self, *, root, store_id, journal, bounds):
            self.root = Path(root)
            self.store_id = store_id
            self.journal = journal
            self.bounds = bounds

        def _path(self, ref):
            return self.root / self.store_id / "objects" / ref.digest

        def put(self, chunks, *, max_bytes, media_type):
            raise NotImplementedError("test store fake is read-only")

        def read(self, ref, *, max_bytes):
            path = self._path(ref)
            if not path.is_file():
                raise DataflowError("CORRUPT_ARTIFACT", "artifact is missing")
            raw = path.read_bytes()
            if len(raw) > max_bytes:
                raise DataflowError("BUDGET_EXCEEDED", "artifact exceeds the read budget")
            if hashlib.sha256(raw).hexdigest() != ref.digest:
                raise DataflowError("CORRUPT_ARTIFACT", "artifact digest differs")
            yield raw

        def inspect(self, ref):
            path = self._path(ref)
            return path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == ref.digest

        def retain(self, ref, *, owner):
            return None

        def release(self, ref, *, owner):
            return None

    mod.RunArtifactStore = RunArtifactStore
    return mod


def _build_seal_module() -> types.ModuleType:
    mod = types.ModuleType("demiflow.execution.dataflow_seal")
    # Test-domain registry: (directory, action_id) -> (graph, physical, sealed).
    mod._registry = {}

    def load_dataflow(directory, action_id, *, store, bounds):
        key = (str(Path(directory).resolve()), action_id)
        if key not in mod._registry:
            raise KeyError("no sealed dataflow registered for " + repr(key))
        return mod._registry[key]

    mod.load_dataflow = load_dataflow
    return mod


def _ensure_cross_lane() -> None:
    builders = (
        ("demiflow.execution.contracts.dataflow_errors", _build_errors_module),
        ("demiflow.execution.contracts.dataflow", _build_dataflow_module),
        ("demiflow.execution.dataflow_store", _build_store_module),
        ("demiflow.execution.dataflow_seal", _build_seal_module),
    )
    for name, builder in builders:
        try:
            importlib.import_module(name)
        except ImportError:
            module = builder()
            sys.modules[name] = module
            parent_name, attr = name.rsplit(".", 1)
            setattr(sys.modules[parent_name], attr, module)
            TEST_DOUBLES.add(name)


_ensure_cross_lane()

from demiflow.execution.contracts.dataflow import (  # noqa: E402
    ArtifactRef, Bounds, CapabilityReport, CutContribution, CutRequest, DataflowGraph,
    ExchangeSpec, FiniteScope, GraphEdge, GraphNode, HistoricalBinding, InputPort,
    KernelSpec, NodeObservation, PhysicalDataflow, RecoveryImage, ResidencyPlan,
    RuntimePolicy, SealedExecution, SourceCursor, SourceSnapshot, SourceSplit, StageSpec,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError  # noqa: E402
from demiflow.execution import dataflow_history as hist  # noqa: E402
from demiflow.execution import sealed_plans  # noqa: E402
from demiflow.execution.recovery_contract import RecoveryContractError  # noqa: E402
from demiflow.execution.recovery_identity import platform_identity  # noqa: E402
from demiflow.execution.run_journal import RunJournal  # noqa: E402
from demiflow.resources.contracts import ResourceProfile  # noqa: E402


@pytest.fixture(autouse=True)
def _worktree_binding():
    _assert_worktree()
    yield
    _assert_worktree()


# --- Construction helpers (test domain). ---

def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def aref(label: str = "artifact", *, store_id="run", media_type="application/json") -> ArtifactRef:
    digest = sha(label)
    return ArtifactRef(digest=digest, size_bytes=1, store_id=store_id,
                       key=digest, media_type=media_type)


def ref_json(ref: ArtifactRef) -> dict:
    return {"digest": ref.digest, "size_bytes": ref.size_bytes, "store_id": ref.store_id,
            "key": ref.key, "media_type": ref.media_type}


def json_ref(value, *, store_id="run"):
    raw = json.dumps(value).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return ArtifactRef(digest=digest, size_bytes=len(raw), store_id=store_id,
                       key=digest, media_type="application/json"), raw


def profile(cpu=1000, memory=8 * 1024 * 1024) -> ResourceProfile:
    return ResourceProfile(requests={"cpu_millis": cpu, "memory_bytes": memory})


def bounds(**overrides) -> Bounds:
    base = dict(max_nodes=4096, max_edges=8192, max_partitions=4096, max_depth=128,
                max_control_bytes=4 * 1024 ** 2, max_row_bytes=1024 * 1024,
                max_batch_bytes=4 * 1024 ** 2, max_resident_bytes=4 * 1024 ** 2,
                max_spill_bytes=4 * 1024 ** 2, max_state_bytes=4 * 1024 ** 2,
                max_pending_events=4096, max_open_files=256, max_output_rows=1024 * 1024,
                timeout_ms=60000)
    base.update(overrides)
    return Bounds(**base)


def kernel(kind="map", *, role="cpu", state="none", effect="pure", inputs=None) -> KernelSpec:
    return KernelSpec(kind=kind, implementation_ref=None, configuration={},
                      inputs=tuple(inputs if inputs is not None else (InputPort("in", "incremental", "none", None),)),
                      state=state, effect=effect, resource_role=role, profile=profile(),
                      output_bound_bytes=1024 * 1024, mergeable=True)


def make_graph(action_id="a0", *, occurrences=1):
    """A source -> transform chain; ``occurrences`` repeats the transform occurrence."""
    dag_id = sha("dag-" + action_id)
    src_occ = sha("occ-src-" + action_id)
    nodes = [GraphNode(occurrence_id=src_occ, semantic_id=sha("src"), kind="source",
                       kernel=kernel("read_table", inputs=()), program_ref=None, shared_token="0")]
    edges = []
    transform_occs = []
    for index in range(occurrences):
        occ = sha(f"occ-map-{action_id}-{index}")
        transform_occs.append(occ)
        nodes.append(GraphNode(occurrence_id=occ, semantic_id=sha("map"), kind="transform",
                               kernel=kernel("map"), program_ref=aref("prog-" + str(index)),
                               shared_token=str(index + 1)))
        edges.append(GraphEdge(edge_id=sha(f"edge-{index}"), producer=src_occ, output_port="out",
                               consumer=occ, input_port="in"))
    graph = DataflowGraph(schema="demiflow.dataflow_graph.v3", action_id=action_id, dag_id=dag_id,
                          nodes=tuple(nodes), edges=tuple(edges), terminals=(transform_occs[-1],),
                          sources=(src_occ,))
    return graph, transform_occs


def make_physical(graph, *, recovery="checkpoint", limitations=()):
    plan_id = sha("plan-" + graph.action_id)
    stages = []
    for node in graph.nodes:
        stages.append(StageSpec(stage_id=sha("stage-" + node.occurrence_id),
                                occurrences=(node.occurrence_id,), partitions=("p0", "p1"),
                                profile=profile(), replica_min=1, replica_target=2, replica_max=4,
                                resident_bytes=1024, output_bytes=2048, state_bytes=512,
                                kernel_refs=()))
    exchanges = []
    for edge in graph.edges:
        exchanges.append(ExchangeSpec(channel_id=sha("chan-" + edge.edge_id), edge_id=edge.edge_id,
                                      producer_partitions=("p0",), consumer_partitions=("p0", "p1"),
                                      mode="memory_incremental", routing="forward", keys=(),
                                      scope_id=None, route_epoch=0, credit_rows=64, credit_bytes=4096,
                                      storage_bytes=0, read_domain="local", reason="forward chain"))
    policy = RuntimePolicy(provider="local", durability="ephemeral", default_partitions=2,
                           max_replicas=4, fusion=True, checkpoint_rows=512, checkpoint_interval_ms=1000,
                           fairness_quantum_bytes=1024, control_timeout_ms=30000, store_id="run",
                           required_recovery=recovery)
    residency = ResidencyPlan(simultaneous_groups=(("g0",),), control_reserved_bytes=1024,
                              consumer_reserved_slots=2, required_spill_bytes=0)
    physical = PhysicalDataflow(schema="demiflow.physical_dataflow.v3", dag_id=graph.dag_id,
                                plan_id=plan_id, plan_revision=1, stages=tuple(stages),
                                exchanges=tuple(exchanges), residency=residency, scopes=(),
                                sources=(), splits=(), policy=policy, bounds=bounds(),
                                recovery=recovery, limitations=tuple(limitations),
                                constants_ref=aref("constants"))
    return physical


def make_sealed() -> SealedExecution:
    return SealedExecution(logical_ref=aref("logical"), physical_ref=aref("physical"),
                           programs=(aref("program"),), code_pin_ref=aref("code-pin"),
                           identity_bridge_ref=aref("bridge"), source_refs=(aref("source"),),
                           constants_ref=aref("constants"))


class MemoryStore:
    """Spec-bound in-memory ArtifactStore double for W002/W003."""

    def __init__(self):
        self.objects = {}
        self.unsealed = set()

    def add(self, ref, payload: bytes):
        self.objects[ref.digest] = payload
        return ref

    def add_json(self, ref, value):
        return self.add(ref, json.dumps(value).encode("utf-8"))

    def read(self, ref, *, max_bytes):
        if ref.digest not in self.objects:
            raise DataflowError("CORRUPT_ARTIFACT", "missing")
        raw = self.objects[ref.digest]
        if len(raw) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "too large")
        yield raw

    def inspect(self, ref):
        return ref.digest in self.objects and ref.digest not in self.unsealed

    def retain(self, ref, *, owner):
        return None

    def release(self, ref, *, owner):
        return None


def make_journal(tmp_path, *, run_id=None, platform_revision=None):
    run_id = run_id or ("r" + "1" * 24)
    target = tmp_path / run_id
    target.mkdir(parents=True, exist_ok=True)
    journal = RunJournal(target)
    journal.initialize(dict(run_id=run_id, object_locations=[]), attempts=True)
    configuration = dict(run_id=run_id,
                         platform_revision=platform_revision if platform_revision is not None else platform_identity(),
                         limits=dict(max_control_bytes=8 * 1024 ** 2, max_commits=1024,
                                     admission_timeout_s=30))
    fence = "f" + "1" * 32
    journal.claim_execution(configuration, fence)
    return target, journal, fence, run_id


def v3_marker(run_id, action_id="a0", *, dag_id=None, plan_id=None):
    dag_id = dag_id or sha("dag-" + action_id)
    plan_id = plan_id or sha("plan-" + action_id)
    return {"schema": hist.DATAFLOW_SEALED_SCHEMA, "run_id": run_id, "action_id": action_id,
            "dag_id": dag_id, "plan_id": plan_id, "logical_ref": ref_json(aref("logical")),
            "physical_ref": ref_json(aref("physical")), "programs": [ref_json(aref("program"))],
            "code_pin_ref": ref_json(aref("code-pin")), "identity_bridge_ref": ref_json(aref("bridge")),
            "source_refs": [ref_json(aref("source"))], "constants_ref": ref_json(aref("constants")),
            "plan_revision": 0, "graph_digest": sha("graph"), "physical_digest": sha("physical")}


# --- W001 load_execution_version -------------------------------------------

def test_w001(tmp_path):
    from demiflow import data
    from test_runtime_v3_real_seams import prepared
    with prepared(tmp_path, data.from_items([{"x": 1}])) as (ex, services):
        target = services.action.context.directory
        journal, fence = services.action.context.journal, services.action.context.fence
        run_id, action_id = services.action.context.ref.run_id, services.action.action_id
        # v3 happy path: journal-pointed marker binds by its original version.
        binding = hist.load_execution_version(target, action_id, max_bytes=1024 * 1024)
        assert isinstance(binding, HistoricalBinding)
        assert binding.schema_version == hist.DATAFLOW_SEALED_SCHEMA
        assert binding.compatible is True and binding.reason is None
        assert binding.original_plan_ref.digest == ex._sealed.physical_ref.digest
        assert binding.original_code_pin_ref.digest == ex._sealed.code_pin_ref.digest
        assert binding.restore_image is None
        original_marker = journal.source_binding(action_id + "-dataflow-sealed")
        assert type(original_marker['sealed_at']) is int
        # Historical markers without optional UI metadata retain original ids;
        # current markers accept only the producer's two bounded metadata fields.
        historical = {key: value for key, value in original_marker.items()
                      if key not in {'sealed_at', 'observation_labels'}}
        old_binding = hist._load_v3(journal, target, action_id, historical)
        assert old_binding.original_plan_ref == binding.original_plan_ref
        occurrences = [node.occurrence_id for node in ex._graph.nodes]
        valid = dict(original_marker, observation_labels={occurrences[0]: 'source'})
        assert hist._load_v3(journal, target, action_id, valid).original_plan_ref == binding.original_plan_ref
        invalid = [dict(original_marker, sealed_at=value) for value in (True, -1, 'now')]
        invalid += [dict(original_marker, observation_labels=labels) for labels in
                    ([], {'outside-original-graph': 'x'}, {occurrences[0]: ''},
                     {occurrences[0]: True}, {key: 'duplicate' for key in occurrences})]
        invalid.append(dict(original_marker, future_metadata={}))
        for marker in invalid:
            with pytest.raises(DataflowError) as exc:
                hist._load_v3(journal, target, action_id, marker)
            assert exc.value.code == 'INVALID_CONTRACT'
        assert journal.source_binding(action_id + "-dataflow-sealed") == original_marker
        # Neither v3 nor v1 facts -> incompatible history, never a fabricated projection.
        with pytest.raises(DataflowError) as exc:
            hist.load_execution_version(target, "missing", max_bytes=1024 * 1024)
        assert exc.value.code == "INCOMPATIBLE_HISTORY"
        # Both v3 and v1 facts bound -> invalid contract.
        journal.bind_source(fence, "both-dataflow-sealed", v3_marker(run_id, "both"))
        journal.bind_source(fence, "both-sealed-plan", {"schema": hist.SEALED_PLAN_FACTS_SCHEMA})
        with pytest.raises(DataflowError) as exc:
            hist.load_execution_version(target, "both", max_bytes=1024 * 1024)
        assert exc.value.code == "INVALID_CONTRACT"
        # Future/unknown v3 schema -> incompatible history.
        future = v3_marker(run_id, "future")
        future["schema"] = "demiflow.dataflow_sealed.v99"
        journal.bind_source(fence, "future-dataflow-sealed", future)
        with pytest.raises(DataflowError) as exc:
            hist.load_execution_version(target, "future", max_bytes=1024 * 1024)
        assert exc.value.code == "INCOMPATIBLE_HISTORY"
        # Wrong action_id inside the marker -> invalid contract.
        wrong = v3_marker(run_id, "wrong")
        wrong["action_id"] = "other"
        journal.bind_source(fence, "wrong-dataflow-sealed", wrong)
        with pytest.raises(DataflowError) as exc:
            hist.load_execution_version(target, "wrong", max_bytes=1024 * 1024)
        assert exc.value.code == "INVALID_CONTRACT"
        # Bad digest in a sealed ref -> invalid contract (closed artifact reference).
        bad = v3_marker(run_id, "bad")
        bad["code_pin_ref"]["digest"] = "not-a-digest"
        journal.bind_source(fence, "bad-dataflow-sealed", bad)
        with pytest.raises(DataflowError) as exc:
            hist.load_execution_version(target, "bad", max_bytes=1024 * 1024)
        assert exc.value.code == "INVALID_CONTRACT"
        # Invalid action_id and max_bytes guards.
        with pytest.raises(DataflowError) as exc:
            hist.load_execution_version(target, "bad id!", max_bytes=1024)
        assert exc.value.code == "INVALID_CONTRACT"
        with pytest.raises(DataflowError) as exc:
            hist.load_execution_version(target, action_id, max_bytes=-1)
        assert exc.value.code == "INVALID_CONTRACT"
        # Absent journal -> incompatible history.
        with pytest.raises(DataflowError) as exc:
            hist.load_execution_version(tmp_path / "absent", action_id, max_bytes=1024)
        assert exc.value.code == "INCOMPATIBLE_HISTORY"
        # v1/v2 unknown schema -> incompatible history (original version validation).
        journal.bind_source(fence, "old-sealed-plan", {"schema": "demiflow.sealed_plan_facts.v99"})
        with pytest.raises(DataflowError) as exc:
            hist.load_execution_version(target, "old", max_bytes=1024 * 1024)
        assert exc.value.code == "INCOMPATIBLE_HISTORY"


# --- W002 validate_resume_compatibility ------------------------------------

def _v3_binding(plan_recovery="checkpoint"):
    return HistoricalBinding(schema_version=hist.DATAFLOW_SEALED_SCHEMA,
                             original_plan_ref=aref("physical"), original_code_pin_ref=aref("code-pin"),
                             compatible=True, reason=None, restore_image=None)


def test_w002():
    from dataclasses import replace
    from demiflow.execution.contracts.dataflow_codec import encode_wire, compute_physical_digest
    store = MemoryStore()
    code_ref, code_raw = json_ref({"platform_revision": "engine-x"})
    store.add(code_ref, code_raw)

    def bind(plan_value, *, image=True):
        policy = RuntimePolicy(provider="local", durability="durable", default_partitions=1,
            max_replicas=1, fusion=False, checkpoint_rows=10, checkpoint_interval_ms=1000,
            fairness_quantum_bytes=1024, control_timeout_ms=1000, store_id="run",
            required_recovery=plan_value["recovery"])
        splits = tuple(SourceSplit(source_id=sha("source"), split_id=sha(str(i)), locator={},
            start_cursor={}, estimated_bytes=None, required_domain=s["required_domain"])
            for i, s in enumerate(plan_value["splits"]))
        physical = PhysicalDataflow(schema="demiflow.physical_dataflow.v3", dag_id=sha("dag"),
            plan_id="0" * 64, plan_revision=0, stages=(), exchanges=(),
            residency=ResidencyPlan(simultaneous_groups=(), control_reserved_bytes=1,
                consumer_reserved_slots=1, required_spill_bytes=0), scopes=(), sources=(), splits=splits,
            policy=policy, bounds=bounds(), recovery=plan_value["recovery"], limitations=(),
            constants_ref=aref("constants"))
        physical = replace(physical, plan_id=compute_physical_digest(physical))
        raw = encode_wire(physical, type_name="PhysicalDataflow", bounds=bounds())
        digest = hashlib.sha256(raw).hexdigest()
        plan_ref = ArtifactRef(digest, len(raw), "run", digest, "application/json")
        store.add(plan_ref, raw)
        cut = CutRequest(sha("cut"), 1, 0, 0, (), True)
        restore = RecoveryImage(sha("commit"), cut, (), {}, ()) if image else None
        return HistoricalBinding(hist.DATAFLOW_SEALED_SCHEMA, plan_ref, code_ref, True, None, restore)

    binding = bind({"recovery": "checkpoint", "splits": [{"required_domain": "local"}, {"required_domain": "durable"}]})
    report = hist.validate_resume_compatibility(binding, current_runtime="engine-x", store=store)
    assert isinstance(report, CapabilityReport) and report.supported and report.reasons == ()
    assert report.recovery == "checkpoint" and report.required_domains == ("durable", "local")
    absent = bind({"recovery": "checkpoint", "splits": []}, image=False)
    report = hist.validate_resume_compatibility(absent, current_runtime="engine-x", store=store)
    assert not report.supported and any("journal-proven" in r for r in report.reasons)
    drift = hist.validate_resume_compatibility(binding, current_runtime="other-engine", store=store)
    assert not drift.supported and any("platform code differs" in r for r in drift.reasons)
    for recovery in ("attempt_only", "delivery_only"):
        report = hist.validate_resume_compatibility(bind({"recovery": recovery, "splits": []}),
                                                    current_runtime="engine-x", store=store)
        assert not report.supported and report.recovery == recovery
        assert any(recovery in r for r in report.reasons)
    store.objects.pop(code_ref.digest)
    binding = bind({"recovery": "checkpoint", "splits": []})
    report = hist.validate_resume_compatibility(binding, current_runtime="engine-x", store=store)
    assert not report.supported and any("code pin" in r for r in report.reasons)
    assert binding.original_code_pin_ref == code_ref
    store.add(code_ref, code_raw)
    # Incompatible binding -> unsupported carrying its reason.
    plan_ref, raw = json_ref({"recovery": "checkpoint", "splits": []})
    store.add(plan_ref, raw)
    incompat = HistoricalBinding(schema_version=hist.DATAFLOW_SEALED_SCHEMA,
                                 original_plan_ref=plan_ref, original_code_pin_ref=code_ref,
                                 compatible=False, reason="source changed", restore_image=None)
    report = hist.validate_resume_compatibility(incompat, current_runtime="engine-x", store=store)
    assert report.supported is False and report.reasons == ("source changed",)
    # Unknown schema -> unsupported.
    unknown = HistoricalBinding(schema_version="demiflow.unknown.v9", original_plan_ref=plan_ref,
                                original_code_pin_ref=code_ref, compatible=True, reason=None,
                                restore_image=None)
    report = hist.validate_resume_compatibility(unknown, current_runtime="engine-x", store=store)
    assert report.supported is False and any("unsupported historical schema" in r for r in report.reasons)
    # Guard: current_runtime must be a bounded nonempty string.
    with pytest.raises(DataflowError) as exc:
        hist.validate_resume_compatibility(binding, current_runtime="", store=store)
    assert exc.value.code == "INVALID_CONTRACT"


# --- W003 translate_native_checkpoint --------------------------------------

def _recovery_image(*, buffers=(), operator_states=None, splits=1):
    cursors = tuple(SourceCursor(split_id=sha("split-" + str(i)), offset={"next_ordinal": i},
                                 next_sequence=i, end_of_input=True, lease_ref=None, pending_refs=())
                    for i in range(splits))
    contribution = CutContribution(cut_id=sha("cut"), participant_id="a0", boot_id=sha("boot"),
                                   sources=cursors, operator_states=dict(operator_states or {}),
                                   channels={}, buffers=tuple(buffers), effects=(), sinks={}, final=True)
    cut = CutRequest(cut_id=sha("cut"), sequence=1, plan_revision=1, route_epoch=0,
                     participants=("a0",), final=True)
    return RecoveryImage(commit_id=sha("commit"), cut=cut, contributions=(contribution,),
                         confirmed_sinks={}, source_vector=cursors)


def _v1_binding(image):
    return HistoricalBinding(schema_version=hist.SEALED_PLAN_FACTS_SCHEMA,
                             original_plan_ref=aref("physical"), original_code_pin_ref=aref("code-pin"),
                             compatible=True, reason=None, restore_image=image)


def test_w003():
    store = MemoryStore()
    state_ref = aref("state")
    store.add(state_ref, b"state")
    image = _recovery_image(operator_states={"occ0": state_ref})
    binding = _v1_binding(image)
    result = hist.translate_native_checkpoint(binding, store=store, bounds=bounds())
    assert result is image  # original identity preserved, never rewritten
    assert result.commit_id == sha("commit")
    assert len(result.source_vector) == 1
    # Unsealed operator state -> incompatible history.
    store.unsealed.add(state_ref.digest)
    with pytest.raises(DataflowError) as exc:
        hist.translate_native_checkpoint(binding, store=store, bounds=bounds())
    assert exc.value.code == "INCOMPATIBLE_HISTORY"
    store.unsealed.clear()
    # Unsealed buffer -> incompatible history.
    buf_ref = aref("buffer")
    store.add(buf_ref, b"buf")
    store.unsealed.add(buf_ref.digest)
    buf_image = _recovery_image(buffers=(buf_ref,))
    with pytest.raises(DataflowError) as exc:
        hist.translate_native_checkpoint(_v1_binding(buf_image), store=store, bounds=bounds())
    assert exc.value.code == "INCOMPATIBLE_HISTORY"
    store.unsealed.clear()
    # Duplicate splits -> invalid contract.
    dup = _recovery_image(splits=2)
    object.__setattr__(dup, "source_vector", (dup.source_vector[0], dup.source_vector[0]))
    with pytest.raises(DataflowError) as exc:
        hist.translate_native_checkpoint(_v1_binding(dup), store=store, bounds=bounds())
    assert exc.value.code == "INVALID_CONTRACT"
    # No committed checkpoint -> incompatible history (files alone never authority).
    with pytest.raises(DataflowError) as exc:
        hist.translate_native_checkpoint(_v1_binding(None), store=store, bounds=bounds())
    assert exc.value.code == "INCOMPATIBLE_HISTORY"
    # Unknown schema -> no listed adapter.
    other = HistoricalBinding(schema_version="demiflow.unknown.v9", original_plan_ref=aref("physical"),
                              original_code_pin_ref=aref("code-pin"), compatible=True, reason=None,
                              restore_image=image)
    with pytest.raises(DataflowError) as exc:
        hist.translate_native_checkpoint(other, store=store, bounds=bounds())
    assert exc.value.code == "INCOMPATIBLE_HISTORY"
    # _translate_manifest: one cursor per source, never a guessed multi-source vector.
    manifest = _checkpoint_manifest()
    fact = {"sequence": 3, "commit_id": sha("commit")}
    translated = hist._translate_manifest(manifest, fact)
    assert len(translated.source_vector) == 1
    assert translated.source_vector[0].offset == {"next_ordinal": 7}
    assert translated.commit_id == sha("commit")
    assert translated.cut.sequence == 3
    # Unknown top-level field -> rejected (future format cannot be mis-read).
    bad = dict(manifest)
    bad["future_field"] = 1
    with pytest.raises(DataflowError) as exc:
        hist._translate_manifest(bad, fact)
    assert exc.value.code == "INCOMPATIBLE_HISTORY"
    # Unsealed manifest -> rejected.
    unsealed = dict(manifest)
    unsealed["seal"] = {"sealed": False}
    with pytest.raises(DataflowError) as exc:
        hist._translate_manifest(unsealed, fact)
    assert exc.value.code == "INCOMPATIBLE_HISTORY"
    # Identifier-only inflight effect -> rejected, never fabricated as an artifact.
    ident_effect = dict(manifest)
    ident_effect["inflight"] = [{"request_key": "k", "status": "known_completed", "effect_ref": "effect-id"}]
    with pytest.raises(DataflowError) as exc:
        hist._translate_manifest(ident_effect, fact)
    assert exc.value.code == "INCOMPATIBLE_HISTORY"


def _checkpoint_manifest():
    return {
        "schema_version": hist.CHECKPOINT_MANIFEST_SCHEMA,
        "checkpoint_id": sha("checkpoint"), "run_id": "r" + "1" * 24, "action_id": "a0",
        "epoch": 1, "topology_revision": 1, "route_epoch": 0, "plan_revision": 1,
        "artifact_manifest_ref": {"manifest_id": "m0", "sha256": sha("manifest")},
        "participants": {"sources": [{"source_id": "s0", "partition_id": "p0"}]},
        "source_progress": [{"source_id": "s0", "partition_id": "p0", "position": {"next_ordinal": 7}}],
        "channel_cuts": [], "operator_states": [], "inflight": [], "sink_receipts": [],
        "seal": {"sealed": True},
    }


# --- W004 build_plan_view ---------------------------------------------------

def test_w004():
    graph, occs = make_graph("a0", occurrences=2)
    physical = make_physical(graph, recovery="checkpoint", limitations=("no_remote_actor",))
    sealed = make_sealed()
    obs = (NodeObservation(occurrence_id=occs[0], attempt=1, plan_id=physical.plan_id,
                           observed=True, metrics={"rows": 3}),)
    view = hist.build_plan_view(graph, sealed, physical, observations=obs)
    assert view["schema"] == hist.PLAN_VIEW_SCHEMA
    assert view["binding_level"] == "sealed" and view["status"] == "sealed"
    # Same function at two call sites stays two nodes, never merged.
    map_nodes = [n for n in view["dag"]["nodes"] if n["logical_node_id"] == sha("map")]
    assert len(map_nodes) == 2
    assert {n["occurrence_id"] for n in map_nodes} == set(occs)
    # Observed occurrence is observed; the other is explicitly not_observed (no inference).
    by_occ = {o["occurrence_id"]: o for o in view["node_observations"]}
    assert by_occ[occs[0]]["observed"] is True and by_occ[occs[0]]["availability"] == "observed"
    assert by_occ[occs[1]]["observed"] is False and by_occ[occs[1]]["availability"] == "not_observed"
    assert by_occ[occs[1]]["metrics"] is None
    # Physical stage keyed by occurrence carries profile/replicas; exchanges + limits visible.
    stage = view["physical"]["stages"][occs[0]]
    assert stage["partition_count"] == 2 and stage["replica_target"] == 2
    assert "requests" in stage["profile"]
    assert view["physical"]["recovery"] == "checkpoint"
    assert view["physical"]["limitations"] == ["no_remote_actor"]
    assert view["physical"]["exchanges"][0]["mode"] == "memory_incremental"
    assert view["code_pin_ref"]["digest"] == sha("code-pin")
    # Pure: no observation for a node never implies success.
    empty = hist.build_plan_view(graph, sealed, physical, observations=())
    assert all(o["observed"] is False for o in empty["node_observations"])
    # dag_id mismatch -> invalid contract.
    other_graph, _ = make_graph("other")
    with pytest.raises(DataflowError) as exc:
        hist.build_plan_view(other_graph, sealed, physical, observations=())
    assert exc.value.code == "INVALID_CONTRACT"
    # Observation plan_id mismatch -> invalid contract.
    bad_obs = (NodeObservation(occurrence_id=occs[0], attempt=1, plan_id=sha("other-plan"),
                               observed=True, metrics={}),)
    with pytest.raises(DataflowError) as exc:
        hist.build_plan_view(graph, sealed, physical, observations=bad_obs)
    assert exc.value.code == "INVALID_CONTRACT"
    # observations must be a tuple.
    with pytest.raises(DataflowError) as exc:
        hist.build_plan_view(graph, sealed, physical, observations=[])
    assert exc.value.code == "INVALID_CONTRACT"

# --- W005 CheckedExecution.execution_bindings ------------------------------

def _checked(tmp_path, **overrides):
    from demiflow.execution.checked import CheckedExecution
    kwargs = dict(nodes=[{"id": "a0", "kind": "take_all"}], edges=[], actions=["a0"],
                  operators=[], source_root=tmp_path, limits={}, record=lambda *a: None,
                  check_stop=lambda: None)
    kwargs.update(overrides)
    return CheckedExecution(**kwargs)


def test_w005(tmp_path):
    ce = _checked(tmp_path)
    src, op = object(), object()
    ce.source_bindings = {id(src): ("a0", "src_node", 0)}
    ce.operation_bindings = {id(op): ("a0", "op_node", 0)}
    ce.observations = {("a0", "src_node", 0): {"state": "execute"},
                       ("a0", "op_node", 0): {"state": "reused"},
                       ("a0", "a0", 0): {"state": "execute"}}
    plan = types.SimpleNamespace(operations=[op])
    result = ce.execution_bindings(src, plan, action_id="a0")
    assert isinstance(result, tuple) and len(result) == 3
    source, operation, terminal = result
    # Real port structure + per-item source token, all legacy fields preserved.
    assert source == {"role": "source", "program_index": None, "report_node_id": "src_node",
                      "report_occurrence": 0, "mode": "execute", "input_ports": [],
                      "output_ports": ["out"], "source_token": "src_node"}
    assert operation == {"role": "operation", "program_index": 0, "report_node_id": "op_node",
                         "report_occurrence": 0, "mode": "reuse", "input_ports": ["in"],
                         "output_ports": ["out"], "source_token": None}
    assert terminal == {"role": "terminal", "program_index": None, "report_node_id": "a0",
                        "report_occurrence": 0, "mode": "execute", "input_ports": ["in"],
                        "output_ports": [], "source_token": None}
    # No bindings -> no fabricated items.
    ce2 = _checked(tmp_path)
    assert ce2.execution_bindings(object(), types.SimpleNamespace(operations=[]), action_id="a0") == ()


# --- W006 CheckedExecution.action driver_route -----------------------------

def test_w006(tmp_path, monkeypatch):
    import demiflow.execution.checked as checked
    events = []
    ce = _checked(tmp_path, edges=[{"from": "p0", "to": "a0", "port": 0}],
                  record=lambda identity, kind, value: events.append((identity, kind, value)))
    controlled = types.SimpleNamespace(operations=[])
    ce.plan = lambda source, plan, identity, depth=0: (source, controlled)
    # Positive driver route is added to the intent (dependency double; real action symbol).
    monkeypatch.setattr(checked, "check_composition", lambda ops: {"driver": "route-x"})
    with ce.action("take_all", None, None, None) as action:
        action.completed = True
    intent = [v for (i, k, v) in events if k == "intent"][0]
    assert intent["kind"] == "take_all" and intent["driver_route"] == "route-x"
    # No driver route -> intent carries no fabricated driver_route field.
    events.clear()
    ce2 = _checked(tmp_path, edges=[{"from": "p0", "to": "a0", "port": 0}],
                   record=lambda identity, kind, value: events.append((identity, kind, value)))
    ce2.plan = lambda source, plan, identity, depth=0: (source, controlled)
    monkeypatch.setattr(checked, "check_composition", lambda ops: {"driver": None})
    with ce2.action("take_all", None, None, None) as action:
        action.completed = True
    intent = [v for (i, k, v) in events if k == "intent"][0]
    assert intent == {"kind": "take_all"}


# --- W007 load_sealed_plan v3 ----------------------------------------------

def _register_seal(target, action_id, graph, physical, sealed, monkeypatch):
    """Unit boundary only; production sealed-byte integration is tested separately."""
    import demiflow.execution.dataflow_seal as seal_mod
    (Path(target) / ".dataflow-store").mkdir(exist_ok=True)
    def load(directory, wanted, *, store, bounds):
        assert Path(directory).resolve() == Path(target).resolve() and wanted == action_id
        return graph, physical, sealed
    monkeypatch.setattr(seal_mod, "load_dataflow", load)


def test_w007(tmp_path, monkeypatch):
    from demiflow.execution.plan_interpreter import PlanContractError
    target, journal, fence, run_id = make_journal(tmp_path)
    graph, _ = make_graph("a0")
    physical = make_physical(graph)
    sealed = make_sealed()
    marker = v3_marker(run_id, "a0", dag_id=graph.dag_id, plan_id=physical.plan_id)
    journal.bind_source(fence, "a0-dataflow-sealed", marker)
    _register_seal(target, "a0", graph, physical, sealed, monkeypatch)
    bundle = sealed_plans.load_sealed_plan(target, "a0")
    assert bundle["version"] == "v3"
    assert bundle["graph"] is graph and bundle["physical"] is physical and bundle["sealed"] is sealed
    assert bundle["facts"] == marker
    # Absent facts -> missing_plan (never a fabricated plan).
    with pytest.raises(PlanContractError) as exc:
        sealed_plans.load_sealed_plan(target, "missing")
    assert exc.value.reason == "missing_plan"
    # Absent journal -> missing_plan.
    with pytest.raises(PlanContractError) as exc:
        sealed_plans.load_sealed_plan(tmp_path / "absent", "a0")
    assert exc.value.reason == "missing_plan"


# --- W008 load_execution_bindings v3 ---------------------------------------

def write_store_json(target, value):
    ref, raw = json_ref(value)
    obj = Path(target) / ".dataflow-store" / "run" / "objects" / ref.digest
    obj.parent.mkdir(parents=True, exist_ok=True)
    obj.write_bytes(raw)
    return ref


def _v3_bundle(target, graph, physical, bridge_doc):
    from dataclasses import replace
    bridge_ref = write_store_json(target, bridge_doc)
    sealed = replace(make_sealed(), identity_bridge_ref=bridge_ref)
    return {"version": "v3", "facts": {}, "graph": graph, "physical": physical, "sealed": sealed}


def test_w008(tmp_path):
    from demiflow.execution.plan_interpreter import PlanContractError
    target, journal, fence, run_id = make_journal(tmp_path)
    graph, _ = make_graph("a0")
    physical = make_physical(graph)
    good = {"action_id": "a0", "dag_id": graph.dag_id, "plan_id": physical.plan_id, "entries": []}
    bundle = _v3_bundle(target, graph, physical, good)
    result = sealed_plans.load_execution_bindings(target, "a0", sealed_plan=bundle)
    assert result["version"] == "v3" and result["document"] == good
    assert result["ref"]["digest"] == bundle["sealed"].identity_bridge_ref.digest
    # Wrong action_id -> identity_mismatch.
    bad = _v3_bundle(target, graph, physical, {**good, "action_id": "other"})
    with pytest.raises(PlanContractError) as exc:
        sealed_plans.load_execution_bindings(target, "a0", sealed_plan=bad)
    assert exc.value.reason == "identity_mismatch"
    # Wrong dag_id -> identity_mismatch.
    bad = _v3_bundle(target, graph, physical, {**good, "dag_id": sha("other-dag")})
    with pytest.raises(PlanContractError) as exc:
        sealed_plans.load_execution_bindings(target, "a0", sealed_plan=bad)
    assert exc.value.reason == "identity_mismatch"
    # v2 driver/worker placement in a v3 bridge -> unsupported_layout, never misread.
    bad = _v3_bundle(target, graph, physical, {**good, "worker_groups": []})
    with pytest.raises(PlanContractError) as exc:
        sealed_plans.load_execution_bindings(target, "a0", sealed_plan=bad)
    assert exc.value.reason == "unsupported_layout"


# --- W009 facts.read_sealed_plan v3 ----------------------------------------

def test_w009(tmp_path, monkeypatch):
    from demiflow.workbench.runs import facts
    target, journal, fence, run_id = make_journal(tmp_path)
    graph, _ = make_graph("a0")
    physical = make_physical(graph)
    sealed = make_sealed()
    marker = v3_marker(run_id, "a0", dag_id=graph.dag_id, plan_id=physical.plan_id)
    journal.bind_source(fence, "a0-dataflow-sealed", marker)
    _register_seal(target, "a0", graph, physical, sealed, monkeypatch)
    result = facts.read_sealed_plan(target, "a0")
    assert result["available"] is True and result["version"] == "v3"
    assert result["dag_id"] == graph.dag_id and result["plan_id"] == physical.plan_id
    view = result["plan_view"]
    assert view["schema"] == hist.PLAN_VIEW_SCHEMA
    assert len(view["dag"]["nodes"]) == len(graph.nodes)
    assert all(o["observed"] is False for o in view["node_observations"])
    # Invalid action id -> structured unavailable, never raises.
    bad = facts.read_sealed_plan(target, "")
    assert not bad.get("available") and bad["status"] == "unavailable"
    # No facts -> unavailable.
    miss = facts.read_sealed_plan(target, "missing")
    assert not miss.get("available") and miss["status"] == "unavailable"


# --- W010 facts._physical_summary ------------------------------------------

def test_w010():
    from demiflow.workbench.runs import facts
    graph, occs = make_graph("a0")
    physical = make_physical(graph, limitations=("limit",))
    view = hist.build_plan_view(graph, make_sealed(), physical, observations=())
    # v3 view passes through unchanged (already complete; nothing re-derived).
    assert facts._physical_summary(view["physical"]) is view["physical"]
    # v1/v2 layout is summarized into the historical shape.
    v1 = {"plan_id": "p", "plan_revision": 1, "recovery": {"terminals": []},
          "worker_groups": [{"group_id": "g", "role": "cpu", "stage_refs": ["s"],
                             "min_workers": 1, "initial_workers": 1, "max_workers": 2}],
          "stages": [{"stage_id": "s", "logical_node_id": "n", "partitions": ["p0", "p1"],
                      "budgets": {"cpu_millis": 1}}],
          "channels": [{"channel_id": "c", "exchange_mode": "memory_incremental", "routing": "forward",
                        "durability": {"declared": "ephemeral"}, "producer_partitions": ["p0"],
                        "consumer_partitions": ["p0"]}]}
    summary = facts._physical_summary(v1)
    assert summary["stages"]["n"]["partition_count"] == 2
    assert summary["stages"]["n"]["role"] == "cpu"
    assert summary["channels"][0]["producer_count"] == 1
    assert summary["worker_groups"][0]["max_workers"] == 2


# --- W011 facts._sealed_entry ----------------------------------------------

def test_w011():
    from demiflow.workbench.runs import facts
    graph, occs = make_graph("a0")
    physical = make_physical(graph)
    view = hist.build_plan_view(graph, make_sealed(), physical, observations=())
    v3_result = {"version": "v3", "action_id": "a0", "plan_id": physical.plan_id,
                 "sealed_at": 1700000000, "plan_view": view, "recovery_restriction": None}
    entry = facts._sealed_entry(v3_result)
    assert entry["version"] == "v3" and entry["status"] == "sealed"
    assert entry["binding_level"] == "sealed"
    assert entry["dag"] is view["dag"]
    assert entry["physical"] is view["physical"]
    assert entry["node_observations"] is view["node_observations"]
    assert entry["code_pin_ref"] == view["code_pin_ref"]
    assert entry["identity_bridge"]["available"] is False
    # v1/v2 entry keeps the historical shape.
    v1_result = {"action_id": "a0", "plan_id": "p", "sealed_at": 1, "logical_dag": {"nodes": []},
                 "physical_plan": {"plan_id": "p", "plan_revision": 1, "recovery": {},
                                   "worker_groups": [], "stages": [], "channels": []},
                 "artifact_manifest_ref": {"manifest_id": "m", "sha256": sha("m")}}
    v1_entry = facts._sealed_entry(v1_result)
    assert "version" not in v1_entry and v1_entry["dag"] == {"nodes": []}


# --- W012 facts.join_node_observations -------------------------------------

def test_w012():
    from demiflow.workbench.runs import facts
    from demiflow.forge.schema import ForgeError
    bridge = {"action_id": "a0", "entries": [
        {"report_node_id": "n0", "report_occurrence": 0, "logical_node_id": "map", "occurrence_id": "o0", "mode": "execute"},
        {"report_node_id": "n1", "report_occurrence": 0, "logical_node_id": "map", "occurrence_id": "o1", "mode": "execute"}]}
    page = {"format": "demiflow_forge_node_observation_page_v1", "attempt": 1, "action_id": "a0",
            "plan_id": "plan-x", "observations": [
                {"action_id": "a0", "node_id": "n0", "occurrence": 0, "plan_id": "plan-x", "state": "succeeded"},
                {"action_id": "a0", "node_id": "n1", "occurrence": 0, "plan_id": "other-plan", "state": "succeeded"}]}
    joined = facts.join_node_observations(bridge, page, action_id="a0", attempt=1)
    assert joined[0]["availability"] == "observed" and joined[0]["observation"]["state"] == "succeeded"
    # Row from a different plan is not joined (composite key, never function-name merge).
    assert joined[1]["availability"] == "not_in_page" and joined[1]["observation"] is None
    # Old page without plan_id keeps occurrence-only behaviour.
    old_page = {**page, "plan_id": None, "observations": [
        {"action_id": "a0", "node_id": "n1", "occurrence": 0, "state": "succeeded"}]}
    joined = facts.join_node_observations(bridge, old_page, action_id="a0", attempt=1)
    assert joined[1]["availability"] == "observed"
    # Scope mismatch raises rather than guesses.
    with pytest.raises(ForgeError):
        facts.join_node_observations(bridge, {**page, "attempt": 2}, action_id="a0", attempt=1)
    with pytest.raises(ForgeError):
        facts.join_node_observations({**bridge, "action_id": "other"}, page, action_id="a0", attempt=1)


# --- W013 RunContext.resume admission --------------------------------------

def test_w013(tmp_path, monkeypatch):
    from demiflow.execution.run_context import RunContext, RunRef
    target, journal, fence, run_id = make_journal(tmp_path)
    reference = RunRef(str(target), run_id)
    # Wrong reference type.
    with pytest.raises(TypeError):
        RunContext.resume(object())
    # No unified recovery contract.
    bare = tmp_path / "bare"
    bare.mkdir()
    RunJournal(bare).initialize(dict(run_id="r" + "2" * 24, object_locations=[]), attempts=True)
    with pytest.raises(RecoveryContractError, match="no unified recovery contract"):
        RunContext.resume(RunRef(str(bare), "r" + "2" * 24))
    # Platform implementation changed.
    other_target, _, _, other_run = make_journal(tmp_path / "other", platform_revision="other-engine")
    with pytest.raises(RecoveryContractError, match="Platform implementation changed"):
        RunContext.resume(RunRef(str(other_target), other_run))
    # RunRef identity does not match its directory.
    with pytest.raises(RecoveryContractError, match="RunRef identity does not match"):
        RunContext.resume(RunRef(str(target), "r" + "3" * 24))
    # Matching identity passes the gate and preserves the original RunRef.
    monkeypatch.setattr(RunContext, "__init__", lambda self, *a, **k: None)
    context = RunContext.resume(reference)
    assert context.expected_run_id == run_id


# --- W014 manager.resume admission -----------------------------------------

def test_w014(tmp_path, monkeypatch):
    from demiflow.workbench.runs import manager
    from demiflow.forge.schema import ForgeError
    run_id = "r" + "1" * 24
    target = manager.directory(tmp_path, run_id)
    target.mkdir(parents=True, exist_ok=True)
    journal = RunJournal(target)
    journal.initialize(dict(run_id=run_id, object_locations=[]), attempts=True)
    authority = types.SimpleNamespace(require=lambda *a, **k: None, bootstrap=lambda: None)
    # No unified recovery contract.
    with pytest.raises(ForgeError) as exc:
        manager.resume(tmp_path, run_id, expected="d", expected_attempt=1, authority=authority)
    assert exc.value.code == "conflict"
    # Platform implementation changed.
    configuration = dict(run_id=run_id, platform_revision="other-engine",
                         limits=dict(max_control_bytes=8 * 1024 ** 2, max_commits=1024, admission_timeout_s=30))
    journal.claim_execution(configuration, "f" + "1" * 32)
    with pytest.raises(ForgeError) as exc:
        manager.resume(tmp_path, run_id, expected="d", expected_attempt=1, authority=authority)
    assert exc.value.code == "conflict"
    # Matching identity passes the gate (downstream lifecycle doubled).
    import demiflow.workbench.worker as worker
    import demiflow.workbench.runs.domain as domain
    from contextlib import contextmanager
    @contextmanager
    def fake_lock():
        yield None
    monkeypatch.setattr(domain, "preparation_lock", fake_lock)
    monkeypatch.setattr(worker, "operation", lambda *a, **k: {"attempt": 1})
    monkeypatch.setattr(manager, "start", lambda *a, **k: "started")
    journal2 = RunJournal(target)
    configuration["platform_revision"] = platform_identity()
    # Re-claim with matching revision is immutable-conflict; build a fresh run instead.
    run_id2 = "r" + "4" * 24
    target2 = manager.directory(tmp_path, run_id2)
    target2.mkdir(parents=True, exist_ok=True)
    j2 = RunJournal(target2)
    j2.initialize(dict(run_id=run_id2, object_locations=[]), attempts=True)
    j2.claim_execution(dict(run_id=run_id2, platform_revision=platform_identity(),
                            limits=dict(max_control_bytes=8 * 1024 ** 2, max_commits=1024, admission_timeout_s=30)),
                       "f" + "1" * 32)
    assert manager.resume(tmp_path, run_id2, expected="d", expected_attempt=1, authority=authority) == "started"


# --- W015 model.get sealed_versions ----------------------------------------

def test_w015(tmp_path, monkeypatch):
    from demiflow.workbench.graph import model
    from demiflow.workbench.runs import facts
    from demiflow.workbench.graph import logical
    from demiflow.workbench.graph import parameters
    import demiflow.workbench.projects.workspace as ws
    from demiflow.workbench.runs import manager as runs
    run_id = "r" + "1" * 24
    report = {"digest": "digest-x", "status": "checked", "entry": "entry-x",
              "binding": {"entry": "entry-x", "parameter_path": "params-x", "sources": {}, "parameters": {}},
              "nodes": [{"id": "n0", "api": "x", "source": "s"}], "edges": [], "actions": ["a0"],
              "coverage": {}}
    monkeypatch.setattr(ws, "load_workspace", lambda root: (root, {}, None))
    monkeypatch.setattr(ws, "workspace_id", lambda root, control: "ws")
    monkeypatch.setattr(runs, "show", lambda *a, **k: {"attempt": 1, "report": report,
                                                       "digest": "digest-x", "status": "checked"})
    monkeypatch.setattr(logical, "project", lambda *a, **k: {"available": True, "actions": [], "action_count": 0})
    sealed_view = {"available": True, "action_count": 2, "actions": [
        {"action_id": "a0", "binding_level": "sealed", "version": "v3", "status": "sealed",
         "identity_bridge": {"available": False}},
        {"action_id": "a1", "binding_level": "sealed", "status": "sealed",
         "identity_bridge": {"available": False}},
        {"action_id": "a2", "binding_level": "declared", "status": "projected"}]}
    monkeypatch.setattr(facts, "overlay_sealed", lambda *a, **k: sealed_view)
    monkeypatch.setattr(parameters, "public_model", lambda *a, **k: {"sha256": "x", "fields": []})
    result = model.get(tmp_path, basis_kind="run", run_id=run_id, attempt=1)
    assert result["logical"]["sealed_versions"] == ["v1", "v3"]


# --- W016 model.bound_source sealed_code_pin_ref ---------------------------

def test_w016(tmp_path, monkeypatch):
    from demiflow.workbench.graph import model
    from demiflow.workbench.runs import facts
    from demiflow.workbench.graph import parameters
    import demiflow.workbench.projects.workspace as ws
    from demiflow.workbench.runs import manager as runs
    run_id = "r" + "1" * 24
    report = {"digest": "digest-x", "entry": "entry-x", "actions": ["a0"],
              "binding": {"entry": "entry-x", "parameter_path": "params-x", "sources": {}, "parameters": {}},
              "nodes": [{"id": "n0", "api": "x", "source": "s"}]}
    binding = {"workspace_id": "ws", "basis_kind": "run", "basis_digest": "digest-x",
               "entry": "entry-x", "params": "params-x", "run_id": run_id, "attempt": 1}
    code_pin = ref_json(aref("code-pin"))
    monkeypatch.setattr(ws, "load_workspace", lambda root: (root, {}, None))
    monkeypatch.setattr(ws, "workspace_id", lambda root, control: "ws")
    monkeypatch.setattr(runs, "_load", lambda *a, **k: (tmp_path, {"state": {"attempt": 1},
                                                                  "manifest": {"digest": "digest-x"}}))
    monkeypatch.setattr(runs, "_report", lambda *a, **k: report)
    monkeypatch.setattr(facts, "read_sealed_plan", lambda target, action_id: {
        "available": True, "version": "v3", "plan_view": {"code_pin_ref": code_pin}})
    monkeypatch.setattr(parameters, "public_model", lambda *a, **k: {"sha256": "x", "fields": []})
    result = model.bound_source(tmp_path, binding, "n0")
    assert result["sealed_code_pin_ref"] == code_pin
    assert result["nodes"][0]["id"] == "n0"


# --- W017 model._join_sealed_observations ----------------------------------

def test_w017(tmp_path, monkeypatch):
    from demiflow.workbench.graph import model
    from demiflow.workbench.runs import view as run_view
    graph, occs = make_graph("a0")
    physical = make_physical(graph)
    view = hist.build_plan_view(graph, make_sealed(), physical, observations=())
    bridge = {"available": True, "bridge": {"action_id": "a0", "entries": [
        {"report_node_id": "n0", "report_occurrence": 0, "logical_node_id": "map",
         "occurrence_id": occs[0], "mode": "execute"}]}}
    entry = {"action_id": "a0", "status": "sealed", "version": "v3",
             "node_observations": view["node_observations"], "identity_bridge": bridge}
    logical_view = {"actions": [entry]}
    page = {"format": "demiflow_forge_node_observation_page_v1", "attempt": 1, "action_id": "a0",
            "plan_id": physical.plan_id, "availability": "available",
            "observations": [{"action_id": "a0", "node_id": "n0", "occurrence": 0,
                              "plan_id": physical.plan_id, "state": "succeeded"}]}
    monkeypatch.setattr(run_view, "observations", lambda *a, **k: page)
    model._join_sealed_observations(tmp_path, "r" + "1" * 24, 1, logical_view)
    # The build_plan_view per-occurrence projection is preserved as plan-level authority.
    assert entry["plan_node_observations"] is view["node_observations"]
    # The attempt-scoped bridge join lands in node_observations (compatible shape).
    assert entry["node_observations"]["available"] is True
    assert entry["node_observations"]["entries"][0]["availability"] == "observed"


# --- W018-W021 workbench JS -------------------------------------------------
# The real JS symbol bodies are extracted verbatim from the worktree source and
# executed under node against a marked DOM/host TEST DOUBLE (no browser). This
# verifies the projection/render logic of W018-W021; it is NOT V030/V031 real
# browser verification, which is recorded separately as not_run.

_JS_HARNESS = r'''
'use strict';
const fs = require('fs');
const vm = require('vm');
const path = require('path');
const ROOT = process.env.W_ROOT;
if (!ROOT) throw new Error('W_ROOT not set');
const wbPath = path.join(ROOT, 'demiflow/workbench/web/workbench.js');
const graphPath = path.join(ROOT, 'demiflow/workbench/web/workbench-graph.js');
const wbSrc = fs.readFileSync(wbPath, 'utf8');
const graphSrc = fs.readFileSync(graphPath, 'utf8');

function assert(cond, msg) { if (!cond) throw new Error('ASSERT: ' + msg); }

function makeClassList() {
  const s = new Set();
  return {
    add: c => s.add(c), remove: c => s.delete(c), contains: c => s.has(c),
    toggle: (c, f) => { if (f === undefined) { s.has(c) ? s.delete(c) : s.add(c); } else { f ? s.add(c) : s.delete(c); } return s.has(c); },
  };
}
function makeEl(tag) {
  const el = {
    tagName: tag, children: [], attributes: {}, dataset: {}, className: '',
    style: { setProperty() {} }, classList: makeClassList(),
    _text: '', hidden: false, open: false, type: '', value: '', parentNode: null,
    append(...ns) { for (const n of ns) { this.children.push(n); n.parentNode = this; } },
    appendChild(n) { this.append(n); return n; },
    prepend(...ns) { for (const n of ns) { this.children.unshift(n); n.parentNode = this; } },
    replaceChildren(...ns) { this.children = []; this.append(...ns); },
    setAttribute(k, v) { this.attributes[k] = String(v); },
    getAttribute(k) { return k in this.attributes ? this.attributes[k] : null; },
    contains(n) { return this.children.includes(n); },
    addEventListener() {}, removeEventListener() {},
    get textContent() { return this._text; },
    set textContent(v) { this._text = String(v); this.children = []; },
    get isConnected() { return true; },
  };
  return el;
}
const documentDouble = { createElement: t => makeEl(t), createElementNS: (ns, t) => makeEl(t) };
function textOf(el) { if (!el) return ''; let t = el._text || ''; for (const c of el.children || []) t += ' ' + textOf(c); return t; }
function walk(el, fn) { fn(el); for (const c of el.children || []) walk(c, fn); }
function findByAttr(root, attr, val) { let found = null; walk(root, e => { if (e.attributes && e.attributes[attr] === val) found = e; }); return found; }

function extractFn(src, name) {
  const marker = 'function ' + name + '(';
  const start = src.indexOf(marker);
  if (start < 0) throw new Error('missing function ' + name);
  const i = src.indexOf('{', start);
  let depth = 0, j = i;
  for (; j < src.length; j++) { const c = src[j]; if (c === '{') depth++; else if (c === '}') { depth--; if (depth === 0) { j++; break; } } }
  const body = src.slice(start, j);
  assert(body.startsWith(marker) && body.endsWith('}'), 'extraction bounds for ' + name);
  return body;
}

function makeHost(state) {
  const rec = { raw: [], messages: [], locate: [], choose: [], viewUpdate: [], viewSelect: [], details: [] };
  const host = {
    document: documentDouble, console,
    disposed: false, logicalAction: null, logicalSelected: null,
    logicalBar: makeEl('div'), graphLegend: makeEl('div'), graphTitle: makeEl('div'),
    graphHost: makeEl('div'), inspector: makeEl('div'),
    data: () => state,
    $: (tag, text, cls) => { const n = documentDouble.createElement(tag); if (text !== undefined) n.textContent = text; if (cls) n.className = cls; return n; },
    select: (label, items, value) => { const s = makeEl('select'); s.value = value; s._items = items; return s; },
    field: (name, el, parent) => { parent.append(el); },
    details: (title, h) => { const b = makeEl('details'); b._title = title; rec.details.push(title); return b; },
    raw: (name, value, box) => { rec.raw.push({ name, value }); const p = makeEl('pre'); p.textContent = JSON.stringify(value); box.append(p); },
    button: (label, action, parent, opts) => { const b = makeEl('button'); b._label = label; b._action = action; b.disabled = !!(opts && opts.disabled); parent.append(b); return b; },
    message: (text, tone) => { rec.messages.push(text); },
    locateNode: id => { rec.locate.push(id); return Promise.resolve(); },
    chooseNode: (id, opts) => { rec.choose.push({ id, opts }); },
    logicalView: { update: (...a) => { rec.viewUpdate.push(a); }, select: id => { rec.viewSelect.push(id); } },
  };
  const fns = ['logicalName', 'actionStatusLabel', 'logicalEntry', 'logicalPanel', 'renderLogical', 'bridgeEntryFor', 'observationLabel', 'chooseLogicalNode'];
  const code = fns.map(n => extractFn(wbSrc, n)).join('\n');
  vm.createContext(host);
  vm.runInContext(code, host, { filename: wbPath });
  return { host, rec };
}

function dagNode(logical, occ, extra) {
  return Object.assign({ logical_node_id: logical, occurrence_id: occ, op_kind: 'map', input_ports: ['in'], output_ports: ['out'], shared_token: null, traits: null, operator_spec_ref: null }, extra || {});
}
function v3Dag() {
  return {
    schema_version: 'demiflow.logical_dag.v3', dag_id: 'd'.repeat(64),
    nodes: [dagNode('src', 'occ-src'), dagNode('map', 'occ-map-0'), dagNode('map', 'occ-map-1')],
    edges: [{ from: { logical_node_id: 'src', port: 'out' }, to: { logical_node_id: 'map', port: 'in' } }],
    shared_ancestors: [{ logical_node_id: 'src' }],
  };
}
function v3Physical() {
  return {
    plan_id: 'p'.repeat(64), plan_revision: 3, recovery: 'checkpoint',
    stages: {
      'occ-map-0': { stage_id: 's0', partition_count: 2, replica_min: 1, replica_target: 2, replica_max: 4, resident_bytes: 10, output_bytes: 20, state_bytes: 30, profile: { requests: { cpu: '2', memory: '1Gi' } } },
      'occ-map-1': { stage_id: 's1', partition_count: 3, replica_min: 1, replica_target: 3, replica_max: 6, resident_bytes: 11, output_bytes: 21, state_bytes: 31, profile: { requests: { cpu: '3' } } },
    },
    exchanges: [{ channel_id: 'c0', mode: 'durable_incremental' }, { channel_id: 'c1', mode: 'memory_incremental' }],
    limitations: ['某资源 actor 未声明恢复钩子'], policy: {}, residency: {},
  };
}

function w018() {
  const state = { graph: { logical: { available: true, actions: [] } } };
  const { host } = makeHost(state);
  assert(host.logicalEntry() === null, 'empty actions -> null');
  state.graph.logical.available = false;
  assert(host.logicalEntry() === null, 'unavailable -> null');
  state.graph.logical.available = true;
  const v3noDag = { action_id: 'a0', status: 'sealed', version: 'v3' };
  state.graph.logical.actions = [v3noDag];
  assert(host.logicalEntry() === null, 'sealed v3 without dag -> null (no fabricated graph)');
  const v3dag = { action_id: 'a0', status: 'sealed', version: 'v3', dag: v3Dag() };
  state.graph.logical.actions = [v3dag];
  assert(host.logicalEntry() === v3dag, 'sealed v3 with dag -> entry');
  const v1 = { action_id: 'a1', status: 'sealed', version: 'v1' };
  state.graph.logical.actions = [v1];
  assert(host.logicalEntry() === v1, 'sealed v1 without dag still returned (v1 not guarded)');
  state.graph.logical.actions = [v3dag, { action_id: 'a2', status: 'projected', dag: v3Dag() }];
  host.logicalAction = 'a2';
  assert(host.logicalEntry().action_id === 'a2', 'logicalAction selects entry');
  return { ok: true };
}

function w019() {
  const dag = v3Dag();
  const sealedEntry = {
    action_id: 'a0', status: 'sealed', version: 'v3', binding_level: 'sealed',
    dag, physical: v3Physical(), plan_id: 'p'.repeat(64), sealed_at: 1700000000,
    recovery_restriction: { reason: 'actor_without_hook' }, node_map: {},
    node_observations: [], identity_bridge: { available: false },
  };
  const state = { graph: { logical: { available: true, action_count: 1, actions: [sealedEntry] }, nodes: [{ id: 'n0', api: 'x.y', kind: 'map' }] } };
  const { host, rec } = makeHost(state);
  host.renderLogical(state.graph);
  const bar = textOf(host.logicalBar);
  assert(bar.includes('封存计划结构 · '), 'sealed structure tag present');
  assert(!bar.includes('结构校验通过'), 'no execution-contract-passed claim');
  assert(bar.includes('恢复级别 checkpoint'), 'recovery level shown');
  assert(bar.includes('交换通道 2'), 'exchange count shown');
  assert(bar.includes('durable_incremental') && bar.includes('memory_incremental'), 'exchange modes shown');
  assert(bar.includes('恢复限制 1 项'), 'limitation count shown');
  assert(bar.includes('某资源 actor 未声明恢复钩子'), 'limitation detail shown');
  assert(bar.includes('中断后不能继续恢复'), 'recovery restriction warning shown');
  assert(bar.includes('已绑定本次执行计划'), 'sealed bound tag');
  assert(rec.viewUpdate.length === 1 && rec.viewUpdate[0][4] === 'sealed', 'logicalView basis sealed');
  const projected = { action_id: 'a0', status: 'projected', version: 'v3', binding_level: 'declared', dag, node_map: {}, node_observations: [] };
  state.graph.logical.actions = [projected];
  host.logicalAction = null;
  const h2 = makeHost(state);
  h2.host.renderLogical(state.graph);
  const bar2 = textOf(h2.host.logicalBar);
  assert(bar2.includes('声明投影结构（未执行）'), 'static graph honest label');
  assert(!bar2.includes('结构校验通过'), 'static graph no execution claim');
  assert(!bar2.includes('已绑定本次执行计划'), 'static graph not bound to execution');
  assert(bar2.includes('未绑定本次执行计划'), 'static graph unbound tag');
  assert(h2.rec.viewUpdate[0][4] === 'declared', 'logicalView basis declared');
  return { ok: true };
}

function w020() {
  const dag = v3Dag();
  const phys = v3Physical();
  const obsList = [
    { occurrence_id: 'occ-map-0', observed: true, availability: 'observed', metrics: {} },
    { occurrence_id: 'occ-map-1', observed: false, availability: 'unobserved', metrics: {} },
  ];
  const entry = {
    action_id: 'a0', status: 'sealed', version: 'v3', binding_level: 'sealed',
    dag, physical: phys, node_map: {}, identity_bridge: { available: false },
    node_observations: obsList, plan_node_observations: obsList,
    recovery_restriction: { reason: 'actor_without_hook' },
  };
  const state = { graph: { logical: { available: true, action_count: 1, actions: [entry] }, nodes: [{ id: 'n0', api: 'x.y', kind: 'map' }] } };
  const { host, rec } = makeHost(state);
  host.chooseLogicalNode('occ-map-0', null);
  const names = rec.raw.map(r => r.name);
  assert(names.includes('stage.profile'), 'v3 stage.profile raw block');
  assert(names.includes('stage.replicas'), 'v3 stage.replicas raw block');
  assert(!names.includes('stages_by_occurrence'), 'v3 selection resolves exactly one occurrence');
  assert(names.includes('plan_node_observations'), 'plan_node_observations preserved');
  assert(names.includes('plan_facts'), 'v3 plan_facts raw block');
  const pf = rec.raw.find(r => r.name === 'plan_facts').value;
  assert(Array.isArray(pf.exchanges) && pf.exchanges.length === 2, 'plan_facts carries exchanges');
  assert(!('worker_groups' in pf), 'v3 plan_facts has no worker_groups');
  const sp = rec.raw.find(r => r.name === 'stage.profile').value;
  assert(sp.requests.cpu === '2', 'stage resolved by occurrence_id occ-map-0');

  const boxText = textOf(host.inspector);
  assert(boxText.includes('已有观测'), 'list-shape observation label observed');
  assert(boxText.includes('副本目标'), 'v3 replica target row');
  assert(boxText.includes('资源画像'), 'v3 profile row');
  assert(boxText.includes('中断后不能继续恢复'), 'recovery restriction note');
  const v1entry = {
    action_id: 'a0', status: 'sealed', version: 'v1', binding_level: 'sealed',
    dag, node_map: {}, identity_bridge: { available: false }, node_observations: { available: false, reason: 'x' },
    physical: { plan_id: 'p'.repeat(64), plan_revision: 1, recovery: 'final_commit', stages: { map: { stage_id: 's', partition_count: 1, role: 'worker', budgets: { cpu: 1 } } }, channels: [], worker_groups: [] },
  };
  const state2 = { graph: { logical: { available: true, action_count: 1, actions: [v1entry] }, nodes: [{ id: 'n0', api: 'x.y', kind: 'map' }] } };
  const h2 = makeHost(state2);
  h2.host.chooseLogicalNode('map', null);
  const n2 = h2.rec.raw.map(r => r.name);
  assert(n2.includes('stage.budgets'), 'v1 stage.budgets raw block');
  const pf2 = h2.rec.raw.find(r => r.name === 'plan_facts').value;
  assert('channels' in pf2 && 'worker_groups' in pf2, 'v1 plan_facts channels/worker_groups');
  assert(!('exchanges' in pf2), 'v1 plan_facts has no exchanges');
  assert(!n2.includes('stage.replicas'), 'v1 has no replica raw block');
  return { ok: true };
}

function w021() {
  const ctx = vm.createContext({ document: documentDouble, console });
  vm.runInContext(graphSrc, ctx, { filename: graphPath });
  const mountLogical = ctx.ForgeWorkbenchGraph.mountLogical;
  assert(typeof mountLogical === 'function', 'mountLogical exported');
  const host = makeEl('div'); host.clientWidth = 800; host.clientHeight = 400;
  const view = mountLogical(host, {});
  const dag = v3Dag();
  dag.nodes[1].traits = { resource_role: 'worker', effect: 'pure', state: 'stateless' };
  dag.nodes[1].shared_token = 'tok-123';
  const nodeMap = { map: 'n0' }; const labels = { n0: '映射算子' };
  view.update(dag, nodeMap, labels, null, 'sealed');
  const canvas = host.children[0];
  assert(canvas.getAttribute('aria-label') === '运行封存计划（E2）', 'sealed basis aria-label');
  const mapGroup = findByAttr(canvas, 'data-node', 'map');
  assert(mapGroup, 'map node group rendered');
  const aria = mapGroup.getAttribute('aria-label');
  assert(aria.includes('画像 worker · pure · stateless'), 'trait hint in aria-label');
  const titleEl = mapGroup.children.find(c => c.tagName === 'title');
  const titleText = titleEl ? titleEl._text : '';
  assert(titleText.includes('shared_token tok-123'), 'shared_token in title');
  assert(titleText.includes('资源画像 worker · pure · stateless'), 'resource profile in title');
  assert(titleText.includes('物理 stage/profile/exchange 与恢复限制见节点详情'), 'sealed physical hint in title');
  const stateEl = mapGroup.children.find(c => c.attributes && c.attributes.class === 'wb-node-state');
  assert(stateEl && stateEl._text.includes('×2 次出现'), 'multi-occurrence badge, no execution inference');
  assert(!stateEl._text.includes('成功') && !stateEl._text.includes('执行完成'), 'no fabricated success state');
  view.select('map');
  assert(mapGroup.getAttribute('aria-pressed') === 'true', 'selected before rebind');
  view.update(dag, nodeMap, labels, null, 'declared');
  const canvas2 = host.children[0];
  assert(canvas2.getAttribute('aria-label') === '声明图投影（E2）', 'declared basis aria-label');
  const mapGroup2 = findByAttr(canvas2, 'data-node', 'map');
  assert(mapGroup2.getAttribute('aria-pressed') === 'false', 'selection cleared on new binding');
  const title2 = mapGroup2.children.find(c => c.tagName === 'title')._text;
  assert(!title2.includes('物理 stage/profile/exchange'), 'declared basis omits physical hint');
  view.dispose();
  return { ok: true };
}

const scenarios = { w018, w019, w020, w021 };
const name = process.argv[2];
if (!scenarios[name]) { console.error('unknown scenario ' + name); process.exit(2); }
try {
  const result = scenarios[name]();
  console.log(JSON.stringify(result));
} catch (err) {
  console.error(String(err && err.stack || err));
  process.exit(1);
}
'''


def _node_binary() -> str:
    found = shutil.which("node")
    if found:
        return found
    candidate = "/opt/conda/bin/node"
    if Path(candidate).exists():
        return candidate
    raise RuntimeError("node binary unavailable; W018-W021 cannot run")


def _run_js(scenario: str, tmp_path: Path) -> dict:
    _assert_worktree()
    work = Path("/tmp/demiflow-w-js")
    work.mkdir(parents=True, exist_ok=True)
    harness = work / "harness.pytest.js"
    harness.write_text(_JS_HARNESS, encoding="utf-8")
    env = dict(os.environ, W_ROOT=str(ROOT))
    proc = subprocess.run(
        [_node_binary(), str(harness), scenario],
        cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, f"{scenario} failed:\n{proc.stdout}\n{proc.stderr}"
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    assert result.get("ok") is True
    _assert_worktree()
    return result


def test_w018(tmp_path):
    _run_js("w018", tmp_path)


def test_w019(tmp_path):
    _run_js("w019", tmp_path)


def test_w020(tmp_path):
    _run_js("w020", tmp_path)


def test_w021(tmp_path):
    _run_js("w021", tmp_path)
