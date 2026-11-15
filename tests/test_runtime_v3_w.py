"""Lane W acceptance: workbench real graph & history (cards W001-W021).

Every test calls the real production symbol in this worktree. Lanes C (shared
DTO/codec/error) and D (artifact store / seal) own modules that are not yet merged
into this isolated tree; when absent, a strict spec-bound test-domain fake
(mirroring ``interfaces.pyi``) stands in for them. No production fallback exists.
An in-process assertion binds every loaded demiflow module to this worktree
(IL012). Injected fakes are recorded in ``TEST_DOUBLES`` so real modules and
test-domain doubles stay distinguishable in the evidence report; a synthetic
``__file__`` never impersonates a real file binding.
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
    mod.__file__ = str(ROOT / "demiflow/execution/contracts/dataflow_errors.py")

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
    mod.__file__ = str(ROOT / "demiflow/execution/contracts/dataflow.py")
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
    mod.__file__ = str(ROOT / "demiflow/execution/dataflow_store.py")
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
    mod.__file__ = str(ROOT / "demiflow/execution/dataflow_seal.py")
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
                       key="objects/" + digest, media_type=media_type)


def ref_json(ref: ArtifactRef) -> dict:
    return {"digest": ref.digest, "size_bytes": ref.size_bytes, "store_id": ref.store_id,
            "key": ref.key, "media_type": ref.media_type}


def json_ref(value, *, store_id="run"):
    raw = json.dumps(value).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return ArtifactRef(digest=digest, size_bytes=len(raw), store_id=store_id,
                       key="objects/" + digest, media_type="application/json"), raw


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
                      inputs=tuple(inputs if inputs is not None else (InputPort("in", "incremental", "none"),)),
                      state=state, effect=effect, resource_role=role, profile=profile(),
                      output_bound_bytes=1024 * 1024, mergeable=True)


def make_graph(action_id="a0", *, occurrences=1):
    """A source -> transform chain; ``occurrences`` repeats the transform occurrence."""
    dag_id = sha("dag-" + action_id)
    src_occ = sha("occ-src-" + action_id)
    nodes = [GraphNode(occurrence_id=src_occ, semantic_id="src", kind="source",
                       kernel=kernel("read_table", inputs=()), program_ref=None, shared_token="0")]
    edges = []
    transform_occs = []
    for index in range(occurrences):
        occ = sha(f"occ-map-{action_id}-{index}")
        transform_occs.append(occ)
        nodes.append(GraphNode(occurrence_id=occ, semantic_id="map", kind="transform",
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
            "sealed_at": 1700000000}


# --- W001 load_execution_version -------------------------------------------

def test_w001(tmp_path):
    target, journal, fence, run_id = make_journal(tmp_path)
    action_id = "a0"
    # v3 happy path: journal-pointed marker binds by its original version.
    journal.bind_source(fence, action_id + "-dataflow-sealed", v3_marker(run_id, action_id))
    binding = hist.load_execution_version(target, action_id, max_bytes=1024 * 1024)
    assert isinstance(binding, HistoricalBinding)
    assert binding.schema_version == hist.DATAFLOW_SEALED_SCHEMA
    assert binding.compatible is True and binding.reason is None
    assert binding.original_plan_ref.digest == sha("physical")
    assert binding.original_code_pin_ref.digest == sha("code-pin")
    assert binding.restore_image is None
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
    store = MemoryStore()
    code_ref = aref("code-pin")
    store.add(code_ref, b"code")

    def bind(plan_value):
        plan_ref, raw = json_ref(plan_value)
        store.add(plan_ref, raw)
        return HistoricalBinding(schema_version=hist.DATAFLOW_SEALED_SCHEMA,
                                 original_plan_ref=plan_ref, original_code_pin_ref=code_ref,
                                 compatible=True, reason=None, restore_image=None)

    binding = bind({"recovery": "checkpoint",
                    "splits": [{"required_domain": "local"}, {"required_domain": "durable"}]})
    report = hist.validate_resume_compatibility(binding, current_runtime="engine-x", store=store)
    assert isinstance(report, CapabilityReport)
    assert report.supported is True and report.reasons == ()
    assert report.recovery == "checkpoint"
    assert report.required_domains == ("durable", "local")
    # attempt_only -> unsupported with a concrete reason, never a fabricated graph.
    report = hist.validate_resume_compatibility(bind({"recovery": "attempt_only", "splits": []}),
                                                current_runtime="engine-x", store=store)
    assert report.supported is False and report.recovery == "attempt_only"
    assert any("attempt_only" in r for r in report.reasons)
    # delivery_only -> unsupported.
    report = hist.validate_resume_compatibility(bind({"recovery": "delivery_only", "splits": []}),
                                                current_runtime="engine-x", store=store)
    assert report.supported is False and any("delivery_only" in r for r in report.reasons)
    # Missing original code pin -> unsupported, original reference preserved.
    store.objects.pop(code_ref.digest)
    binding = bind({"recovery": "checkpoint", "splits": []})
    report = hist.validate_resume_compatibility(binding, current_runtime="engine-x", store=store)
    assert report.supported is False and any("code pin" in r for r in report.reasons)
    assert binding.original_code_pin_ref == code_ref
    store.add(code_ref, b"code")
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
    map_nodes = [n for n in view["dag"]["nodes"] if n["logical_node_id"] == "map"]
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

def _register_seal(target, action_id, graph, physical, sealed):
    seal_mod = sys.modules["demiflow.execution.dataflow_seal"]
    seal_mod._registry[(str(Path(target).resolve()), action_id)] = (graph, physical, sealed)


def test_w007(tmp_path):
    from demiflow.execution.plan_interpreter import PlanContractError
    target, journal, fence, run_id = make_journal(tmp_path)
    graph, _ = make_graph("a0")
    physical = make_physical(graph)
    sealed = make_sealed()
    marker = v3_marker(run_id, "a0", dag_id=graph.dag_id, plan_id=physical.plan_id)
    journal.bind_source(fence, "a0-dataflow-sealed", marker)
    _register_seal(target, "a0", graph, physical, sealed)
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
    obj = Path(target) / "run" / "objects" / ref.digest
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

def test_w009(tmp_path):
    from demiflow.workbench.runs import facts
    target, journal, fence, run_id = make_journal(tmp_path)
    graph, _ = make_graph("a0")
    physical = make_physical(graph)
    sealed = make_sealed()
    marker = v3_marker(run_id, "a0", dag_id=graph.dag_id, plan_id=physical.plan_id)
    journal.bind_source(fence, "a0-dataflow-sealed", marker)
    _register_seal(target, "a0", graph, physical, sealed)
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
    report = {"digest": "digest-x", "entry": "entry-x", "actions": ["a0", "a1"],
              "binding": {"entry": "entry-x", "parameter_path": "params-x", "sources": {}, "parameters": {}},
              "nodes": [{"id": "n0", "api": "x", "source": "s"}]}
    binding = {"workspace_id": "ws", "basis_kind": "run", "basis_digest": "digest-x",
               "entry": "entry-x", "params": "params-x", "run_id": run_id, "attempt": 1}
    pins = {"a0": ref_json(aref("code-pin-a0")), "a1": ref_json(aref("code-pin-a1"))}
    monkeypatch.setattr(ws, "load_workspace", lambda root: (root, {}, None))
    monkeypatch.setattr(ws, "workspace_id", lambda root, control: "ws")
    monkeypatch.setattr(runs, "_load", lambda *a, **k: (tmp_path, {"state": {"attempt": 1},
                                                                  "manifest": {"digest": "digest-x"}}))
    monkeypatch.setattr(runs, "_report", lambda *a, **k: report)
    monkeypatch.setattr(facts, "read_sealed_plan", lambda target, action_id: {
        "available": True, "version": "v3", "dag_id": "dag-" + action_id,
        "plan_view": {"code_pin_ref": pins[action_id]}})

    def fake_bridge(target, action_id, *, dag_id, report_digest):
        entries = ([{"report_node_id": "n0", "report_occurrence": 0, "logical_node_id": "map",
                     "occurrence_id": "occ1", "mode": "execute"}] if action_id == "a1" else [])
        return {"available": True, "bridge": {"action_id": action_id, "entries": entries}}

    monkeypatch.setattr(facts, "read_identity_bridge", fake_bridge)
    monkeypatch.setattr(parameters, "public_model", lambda *a, **k: {"sha256": "x", "fields": []})
    result = model.bound_source(tmp_path, binding, "n0", logical_node_id="map", occurrence_id="occ1")
    # U3: the code pin binds to the unique owning action a1, NOT the first v3 action a0.
    assert result["sealed_code_pin_ref"] == pins["a1"]
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
             "dag": view["dag"], "plan_id": physical.plan_id, "physical": view["physical"],
             "node_observations": view["node_observations"], "identity_bridge": bridge}
    logical_view = {"actions": [entry]}
    page = {"format": "demiflow_forge_node_observation_page_v1", "attempt": 1, "action_id": "a0",
            "plan_id": physical.plan_id, "dag_id": graph.dag_id, "plan_revision": physical.plan_revision,
            "availability": "available",
            "observations": [{"action_id": "a0", "node_id": "n0", "occurrence": 0,
                              "plan_id": physical.plan_id, "state": "succeeded"}]}
    monkeypatch.setattr(run_view, "observations", lambda *a, **k: page)
    model._join_sealed_observations(tmp_path, "r" + "1" * 24, 1, logical_view)
    # The build_plan_view per-occurrence projection is preserved as plan-level authority.
    assert entry["plan_node_observations"] is view["node_observations"]
    # The attempt-scoped bridge join lands in node_observations (compatible shape).
    obs = entry["node_observations"]
    assert obs["available"] is True
    assert obs["entries"][0]["availability"] == "observed"
    # U2: this-run scope credentials are attached after server-side verification.
    assert obs["action_id"] == "a0" and obs["dag_id"] == graph.dag_id
    assert obs["plan_id"] == physical.plan_id and obs["plan_revision"] == physical.plan_revision


# --- U1 edge/occurrence identity (W004/W011/W015 consumption contract) ------

def test_u1_edge_identity():
    # src -> map0 -> map1: the two transforms share semantic_id "map" but are
    # distinct occurrences in series (the one-to-many case the contract requires).
    dag_id = sha("dag-u1")
    src, m0, m1 = sha("occ-src-u1"), sha("occ-map-u1-0"), sha("occ-map-u1-1")
    nodes = (
        GraphNode(occurrence_id=src, semantic_id="src", kind="source",
                  kernel=kernel("read_table", inputs=()), program_ref=None, shared_token="0"),
        GraphNode(occurrence_id=m0, semantic_id="map", kind="transform",
                  kernel=kernel("map"), program_ref=None, shared_token="1"),
        GraphNode(occurrence_id=m1, semantic_id="map", kind="transform",
                  kernel=kernel("map"), program_ref=None, shared_token="2"),
    )
    e0 = GraphEdge(edge_id=sha("e0"), producer=src, output_port="out", consumer=m0, input_port="in")
    e1 = GraphEdge(edge_id=sha("e1"), producer=m0, output_port="out", consumer=m1, input_port="in")
    graph = DataflowGraph(schema="demiflow.dataflow_graph.v3", action_id="a0", dag_id=dag_id,
                          nodes=nodes, edges=(e0, e1), terminals=(m1,), sources=(src,))
    physical = make_physical(graph)
    view = hist.build_plan_view(graph, make_sealed(), physical, observations=())
    edges = view["dag"]["edges"]
    assert len(edges) == 2
    # edge_id, occurrence_id and ports are copied verbatim from the original GraphEdge.
    assert edges[0]["edge_id"] == e0.edge_id
    assert edges[0]["from"] == {"logical_node_id": "src", "occurrence_id": src, "port": "out"}
    assert edges[0]["to"] == {"logical_node_id": "map", "occurrence_id": m0, "port": "in"}
    assert edges[1]["edge_id"] == e1.edge_id
    assert edges[1]["from"] == {"logical_node_id": "map", "occurrence_id": m0, "port": "out"}
    assert edges[1]["to"] == {"logical_node_id": "map", "occurrence_id": m1, "port": "in"}
    # Same-semantic series edge is not a self loop: endpoints differ by occurrence.
    assert edges[1]["from"]["logical_node_id"] == edges[1]["to"]["logical_node_id"] == "map"
    assert edges[1]["from"]["occurrence_id"] != edges[1]["to"]["occurrence_id"]
    # One node per occurrence; the two "map" occurrences are never merged.
    occs = [n["occurrence_id"] for n in view["dag"]["nodes"]]
    assert len(occs) == 3 and occs.count(m0) == 1 and occs.count(m1) == 1
    # physical.stages keyed by occurrence; every exchange joins an actual edge.
    assert set(view["physical"]["stages"]) == {src, m0, m1}
    edge_ids = {e["edge_id"] for e in edges}
    assert all(x["edge_id"] in edge_ids for x in view["physical"]["exchanges"])
    # source/terminal occurrences fall in the node set; plan identity preserved.
    assert set(view["dag"]["sources"]) <= set(occs) and set(view["dag"]["terminals"]) <= set(occs)
    assert view["plan_id"] == physical.plan_id and view["plan_revision"] == physical.plan_revision
    assert view["dag"]["schema_version"] == "demiflow.dataflow_graph.v3"


def test_u1_projection_rejects_dangling_and_bad_scope():
    from dataclasses import replace
    graph, occs = make_graph("a0")
    physical = make_physical(graph)
    # Dangling edge endpoint (unknown occurrence) -> INVALID_CONTRACT, never guessed.
    bad = GraphEdge(edge_id=sha("bad"), producer=sha("ghost"), output_port="out",
                    consumer=occs[0], input_port="in")
    g2 = replace(graph, edges=graph.edges + (bad,))
    with pytest.raises(DataflowError) as exc:
        hist.build_plan_view(g2, make_sealed(), make_physical(g2), observations=())
    assert exc.value.code == "INVALID_CONTRACT"
    # Negative plan_revision -> rejected (non-negative safe integer required).
    with pytest.raises(DataflowError) as exc:
        hist.build_plan_view(graph, make_sealed(), replace(physical, plan_revision=-1), observations=())
    assert exc.value.code == "INVALID_CONTRACT"
    # Source occurrence outside the node set -> rejected (no dangling source).
    g3 = replace(graph, sources=(sha("ghost-src"),))
    with pytest.raises(DataflowError) as exc:
        hist.build_plan_view(g3, make_sealed(), make_physical(g3), observations=())
    assert exc.value.code == "INVALID_CONTRACT"


# --- U2 this-run observation scope credentials (W012/W017) ------------------

def test_u2_observation_scope():
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.runs import facts
    graph, occs = make_graph("a0")
    physical = make_physical(graph)
    plan_id = physical.plan_id
    bridge = {"action_id": "a0", "entries": [
        {"report_node_id": "n0", "report_occurrence": 0, "logical_node_id": "map",
         "occurrence_id": occs[0], "mode": "execute"}]}

    def page(rows, **extra):
        return dict({"format": "demiflow_forge_node_observation_page_v1", "attempt": 1,
                     "action_id": "a0", "availability": "available", "observations": rows}, **extra)

    row = {"action_id": "a0", "node_id": "n0", "occurrence": 0, "plan_id": plan_id, "state": "succeeded"}
    # Verified v3 scope: a row carrying the matching plan is observed.
    joined = facts.join_node_observations(bridge, page([row], plan_id=plan_id, dag_id=graph.dag_id,
                                                       plan_revision=physical.plan_revision),
                                          action_id="a0", attempt=1, plan_id=plan_id,
                                          dag_id=graph.dag_id, plan_revision=physical.plan_revision)
    assert joined[0]["availability"] == "observed" and joined[0]["occurrence_id"] == occs[0]
    # A page lacking the plan scope is rejected, not promoted with a stitched identity.
    with pytest.raises(ForgeError) as exc:
        facts.join_node_observations(bridge, page([row]), action_id="a0", attempt=1, plan_id=plan_id)
    assert exc.value.code == "conflict"
    # A plan-less legacy row is never backfilled to the current plan under v3 scope.
    legacy_row = {"action_id": "a0", "node_id": "n0", "occurrence": 0, "state": "succeeded"}
    joined = facts.join_node_observations(bridge, page([legacy_row], plan_id=plan_id),
                                          action_id="a0", attempt=1, plan_id=plan_id)
    assert joined[0]["availability"] == "not_in_page" and joined[0]["observation"] is None
    # A row from a different plan is not joined.
    other = dict(row, plan_id=sha("other-plan"))
    joined = facts.join_node_observations(bridge, page([other], plan_id=plan_id),
                                          action_id="a0", attempt=1, plan_id=plan_id)
    assert joined[0]["availability"] == "not_in_page"
    # Legacy (no scope) preserves occurrence-only behaviour.
    joined = facts.join_node_observations(bridge, page([legacy_row]), action_id="a0", attempt=1)
    assert joined[0]["availability"] == "observed"


def test_u2_v3_no_scope_not_promoted(tmp_path, monkeypatch):
    from demiflow.workbench.graph import model
    from demiflow.workbench.runs import view as run_view
    graph, occs = make_graph("a0")
    physical = make_physical(graph)
    view = hist.build_plan_view(graph, make_sealed(), physical, observations=())
    bridge = {"available": True, "bridge": {"action_id": "a0", "entries": [
        {"report_node_id": "n0", "report_occurrence": 0, "logical_node_id": "map",
         "occurrence_id": occs[0], "mode": "execute"}]}}
    entry = {"action_id": "a0", "status": "sealed", "version": "v3", "dag": view["dag"],
             "plan_id": physical.plan_id, "physical": view["physical"],
             "node_observations": view["node_observations"], "identity_bridge": bridge}
    logical_view = {"actions": [entry]}
    # Real producer currently emits no page-level plan scope -> v3 must not promote.
    page = {"format": "demiflow_forge_node_observation_page_v1", "attempt": 1, "action_id": "a0",
            "availability": "available",
            "observations": [{"action_id": "a0", "node_id": "n0", "occurrence": 0, "state": "succeeded"}]}
    monkeypatch.setattr(run_view, "observations", lambda *a, **k: page)
    model._join_sealed_observations(tmp_path, "r" + "1" * 24, 1, logical_view)
    obs = entry["node_observations"]
    assert obs["available"] is False and obs["reason"] == "observation_plan_scope_unverified"
    assert obs["plan_id"] == physical.plan_id and obs["dag_id"] == graph.dag_id
    # The plan-level authority remains the honest "no this-run observation" projection.
    assert entry["plan_node_observations"] is view["node_observations"]
    assert all(o["observed"] is False for o in entry["plan_node_observations"])


# --- U3 unique-sealed-action source binding (W016) --------------------------

def test_u3_unique_action_resolution(tmp_path, monkeypatch):
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.graph import model
    from demiflow.workbench.runs import facts
    report = {"digest": "digest-x", "actions": ["a0", "a1"]}
    monkeypatch.setattr(facts, "read_sealed_plan", lambda target, action_id: {
        "available": True, "version": "v3", "dag_id": "dag-" + action_id, "plan_view": {}})

    def bridge_with(action_id, entries):
        return {"available": True, "bridge": {"action_id": action_id, "entries": entries}}

    entry = {"report_node_id": "n0", "report_occurrence": 0, "logical_node_id": "map",
             "occurrence_id": "occ1", "mode": "execute"}
    # Ambiguous: both actions bridge the same logical/occurrence -> requires occurrence.
    monkeypatch.setattr(facts, "read_identity_bridge",
                        lambda target, action_id, *, dag_id, report_digest: bridge_with(action_id, [entry]))
    with pytest.raises(ForgeError) as exc:
        model._resolve_bridge_action(tmp_path, report, "map", "occ1")
    assert exc.value.code == "conflict"
    # Unique owner: only a1 bridges the pair -> a1 is returned (not the first action a0).
    monkeypatch.setattr(facts, "read_identity_bridge",
                        lambda target, action_id, *, dag_id, report_digest:
                        bridge_with(action_id, [entry] if action_id == "a1" else []))
    assert model._resolve_bridge_action(tmp_path, report, "map", "occ1") == "a1"
    # Unbridged pair -> not_found, never guessed.
    monkeypatch.setattr(facts, "read_identity_bridge",
                        lambda target, action_id, *, dag_id, report_digest: bridge_with(action_id, []))
    with pytest.raises(ForgeError) as exc:
        model._resolve_bridge_action(tmp_path, report, "map", "occ1")
    assert exc.value.code == "not_found"


# --- U2 real producer (view.observations) + consumer gate integration -------

def _real_obs_run(tmp_path, *, action_id="a0", v3=True, nodes=("n0",)):
    """Build a real run journal (manifest + native observations) and optional v3 seal.

    The real view.observations producer reads this run.sqlite. The sealed bundle
    uses the C14 cross-lane dataflow_seal double only because the real D module is
    absent in this isolated tree; the marker, journal and readers are real.
    """
    from demiflow.workbench.runs import manager
    from demiflow.forge.platform import content_digest
    run_id = "r" + "1" * 24
    root = tmp_path
    (Path(root) / "workspace.json").write_text('{"id": "ws-u2"}')
    target = manager.directory(root, run_id)
    target.mkdir(parents=True, exist_ok=True)
    journal = RunJournal(target)
    journal.initialize(dict(run_id=run_id, root=str(Path(root).absolute()),
                            digest=content_digest(dict(run_id=run_id)), object_locations=[]), attempts=True)
    fence = "f" + "1" * 32
    journal.claim_execution(dict(run_id=run_id, platform_revision=platform_identity(),
                                 limits=dict(max_control_bytes=8 * 1024 ** 2, max_commits=1024,
                                             admission_timeout_s=30)), fence)
    journal.reserve_nodes([(action_id, node, 0) for node in nodes])
    journal.observe_nodes([dict(action_id=action_id, node_id=node, occurrence=0,
                                document=dict(sequence=1, state="succeeded", rows_output=3)) for node in nodes])
    plan = None
    if v3:
        graph, occs = make_graph(action_id)
        physical = make_physical(graph)
        marker = v3_marker(run_id, action_id, dag_id=graph.dag_id, plan_id=physical.plan_id)
        journal.bind_source(fence, action_id + "-dataflow-sealed", marker)
        _register_seal(target, action_id, graph, physical, make_sealed())
        plan = (graph, physical, occs)
    return root, run_id, target, journal, fence, plan


def _u2_entry(graph, physical, bridge):
    view = hist.build_plan_view(graph, make_sealed(), physical, observations=())
    return {"action_id": "a0", "status": "sealed", "version": "v3", "dag": view["dag"],
            "plan_id": physical.plan_id, "physical": view["physical"],
            "node_observations": view["node_observations"],
            "identity_bridge": {"available": True, "bridge": bridge}}, view


def test_u2_producer_page_scope_verified(tmp_path):
    from demiflow.workbench.runs import view as run_view
    from demiflow.workbench.runs import facts
    from demiflow.workbench.graph import model
    root, run_id, target, journal, fence, plan = _real_obs_run(tmp_path, v3=True)
    graph, physical, occs = plan
    # REAL producer: page and rows carry the sealed v3 plan identity verbatim.
    page = run_view.observations(root, run_id, attempt=1, action_id="a0", limit=64)
    assert page["availability"] == "available"
    assert page["plan_id"] == physical.plan_id and page["dag_id"] == graph.dag_id
    assert page["plan_revision"] == physical.plan_revision
    assert len(page["observations"]) == 1
    row = page["observations"][0]
    assert row["plan_id"] == physical.plan_id and row["dag_id"] == graph.dag_id
    assert row["plan_revision"] == physical.plan_revision
    bridge = {"action_id": "a0", "entries": [
        {"report_node_id": "n0", "report_occurrence": 0, "logical_node_id": "map",
         "occurrence_id": occs[0], "mode": "execute"}]}
    # REAL consumer: exact-match scope verification promotes the observation.
    joined = facts.join_node_observations(bridge, page, action_id="a0", attempt=1,
                                          plan_id=physical.plan_id, dag_id=graph.dag_id,
                                          plan_revision=physical.plan_revision)
    assert joined[0]["availability"] == "observed" and joined[0]["occurrence_id"] == occs[0]
    # REAL model wrapper over the REAL producer (no monkeypatch): credentialed promotion.
    entry, view = _u2_entry(graph, physical, bridge)
    model._join_sealed_observations(root, run_id, 1, {"actions": [entry]})
    obs = entry["node_observations"]
    assert obs["available"] is True and obs["entries"][0]["availability"] == "observed"
    assert obs["action_id"] == "a0" and obs["dag_id"] == graph.dag_id
    assert obs["plan_id"] == physical.plan_id and obs["plan_revision"] == physical.plan_revision
    assert entry["plan_node_observations"] is view["node_observations"]


def test_u2_producer_scopeless_degrades(tmp_path):
    from demiflow.workbench.runs import view as run_view
    from demiflow.workbench.runs import facts
    from demiflow.workbench.graph import model
    from demiflow.forge.schema import ForgeError
    # Real run whose action has NO v3 seal -> real producer emits no plan scope.
    root, run_id, target, journal, fence, _ = _real_obs_run(tmp_path, v3=False)
    page = run_view.observations(root, run_id, attempt=1, action_id="a0", limit=64)
    assert page["availability"] == "available"
    assert "plan_id" not in page and "dag_id" not in page and "plan_revision" not in page
    assert all("plan_id" not in r for r in page["observations"])
    graph, occs = make_graph("a0")
    physical = make_physical(graph)
    bridge = {"action_id": "a0", "entries": [
        {"report_node_id": "n0", "report_occurrence": 0, "logical_node_id": "map",
         "occurrence_id": occs[0], "mode": "execute"}]}
    # A v3 scope cannot be verified against a scope-less page -> conflict, never promoted.
    with pytest.raises(ForgeError) as exc:
        facts.join_node_observations(bridge, page, action_id="a0", attempt=1, plan_id=physical.plan_id)
    assert exc.value.code == "conflict"
    entry, view = _u2_entry(graph, physical, bridge)
    model._join_sealed_observations(root, run_id, 1, {"actions": [entry]})
    obs = entry["node_observations"]
    assert obs["available"] is False and obs["reason"] == "observation_plan_scope_unverified"
    assert obs["plan_id"] == physical.plan_id and obs["dag_id"] == graph.dag_id
    assert entry["plan_node_observations"] is view["node_observations"]


def test_u2_producer_missing_row_not_in_page(tmp_path):
    from demiflow.workbench.runs import view as run_view
    from demiflow.workbench.runs import facts
    # Real v3 run observes n0 only; the bridge also references an absent report node.
    root, run_id, target, journal, fence, plan = _real_obs_run(tmp_path, v3=True, nodes=("n0",))
    graph, physical, occs = plan
    page = run_view.observations(root, run_id, attempt=1, action_id="a0", limit=64)
    bridge = {"action_id": "a0", "entries": [
        {"report_node_id": "n0", "report_occurrence": 0, "logical_node_id": "map",
         "occurrence_id": occs[0], "mode": "execute"},
        {"report_node_id": "n1", "report_occurrence": 0, "logical_node_id": "map",
         "occurrence_id": sha("occ-n1"), "mode": "execute"}]}
    joined = facts.join_node_observations(bridge, page, action_id="a0", attempt=1,
                                          plan_id=physical.plan_id, dag_id=graph.dag_id,
                                          plan_revision=physical.plan_revision)
    assert joined[0]["availability"] == "observed"
    # A restricted page missing the item is not_in_page, never "never ran".
    assert joined[1]["availability"] == "not_in_page" and joined[1]["observation"] is None


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
const WEB = path.join(ROOT, 'demiflow/workbench/web');
const presentationPath = path.join(WEB, 'workbench-presentation.js');
const graphPath = path.join(WEB, 'workbench-graph.js');
const presentationSrc = fs.readFileSync(presentationPath, 'utf8');
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
    contains(n) { return this === n || this.children.some(c => c.contains && c.contains(n)); },
    addEventListener() {}, removeEventListener() {},
    remove() { if (this.parentNode) { const s = this.parentNode.children; const i = s.indexOf(this); if (i >= 0) s.splice(i, 1); this.parentNode = null; } },
    getBoundingClientRect() { return {x: 0, y: 0, width: this.clientWidth, height: this.clientHeight, top: 0, left: 0, right: this.clientWidth, bottom: this.clientHeight}; },
    scrollIntoView() {},
    clientWidth: 800, clientHeight: 400,
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

// SVG nodes carry class via setAttribute, HTML nodes via the className property.
function findAllByClass(root, cls) {
  const out = [];
  walk(root, e => {
    const value = (e.attributes && e.attributes.class) || e.className || '';
    if (String(value).split(/\s+/).includes(cls)) out.push(e);
  });
  return out;
}

// The C merge replaced the old workbench.js logical* projection with the pure
// presentation module (C01-C04/C09). Load the real exported surface instead of
// extracting deleted function names; no consumer double, no second authority.
function presentation() {
  const ctx = vm.createContext({console});
  vm.runInContext(presentationSrc, ctx, {filename: presentationPath});
  const api = ctx.ForgeWorkbenchPresentation;
  assert(api, 'ForgeWorkbenchPresentation exported');
  for (const name of ['chooseGraphView', 'buildNodeCard', 'buildGraphModel', 'buildNodeDetails', 'normalizeExecutionPlan'])
    assert(typeof api[name] === 'function', 'presentation export ' + name);
  return api;
}

function graphHost() {
  const host = makeEl('div');
  const ctx = vm.createContext({
    document: documentDouble, console,
    ResizeObserver: class { observe() {} disconnect() {} },
    requestAnimationFrame: fn => { fn(0); return 1; },
    cancelAnimationFrame: () => {},
  });
  vm.runInContext(graphSrc, ctx, {filename: graphPath});
  const api = ctx.ForgeWorkbenchGraph;
  assert(api, 'ForgeWorkbenchGraph exported');
  for (const name of ['mount', 'layoutGraph', 'status', 'cpuChart'])
    assert(typeof api[name] === 'function', 'graph export ' + name);
  assert(!('mountLogical' in api), 'removed mountLogical must not be resurrected');
  return {api, host, ctx};
}

// Contract-valid fixtures for the merged C projection. v3 identities must be
// 64-hex (isSha) and edges must carry edge_id + occurrence endpoints; the real
// schema is demiflow.dataflow_graph.v3 and resource_role must be a known ROLES
// member. The old short-id / logical_dag.v3 / 'worker' fixtures are not admitted
// by normalizeExecutionPlan and would fabricate a false refusal.
const sha = c => c.repeat(64);
const PROJECT_ID = 'w' + 'a1'.repeat(16);
const BINDING = {basis_kind: 'run', workspace_id: PROJECT_ID, basis_digest: sha('b'), run_id: 'run-1', attempt: 1};
const ACTION = 'a0', DAG_ID = sha('8'), PLAN_ID = sha('9');
const L_SRC = sha('1'), L_MAP = sha('2');
const O_SRC = sha('3'), O_MAP0 = sha('4'), O_MAP1 = sha('5');
const E0 = sha('6'), E1 = sha('7');

function v3Dag() {
  return {
    schema_version: 'demiflow.dataflow_graph.v3', dag_id: DAG_ID, action_id: ACTION,
    nodes: [
      {logical_node_id: L_SRC, occurrence_id: O_SRC, op_kind: 'source', input_ports: [], output_ports: ['out'], shared_token: null, traits: null},
      {logical_node_id: L_MAP, occurrence_id: O_MAP0, op_kind: 'map', input_ports: ['in'], output_ports: ['out'], shared_token: null, traits: {resource_role: 'cpu', effect: 'pure', state: 'stateless'}},
      {logical_node_id: L_MAP, occurrence_id: O_MAP1, op_kind: 'map', input_ports: ['in'], output_ports: ['out'], shared_token: 'tok-123', traits: {resource_role: 'cpu', effect: 'pure', state: 'stateless'}},
    ],
    edges: [
      {edge_id: E0, from: {logical_node_id: L_SRC, occurrence_id: O_SRC, port: 'out'}, to: {logical_node_id: L_MAP, occurrence_id: O_MAP0, port: 'in'}},
      {edge_id: E1, from: {logical_node_id: L_SRC, occurrence_id: O_SRC, port: 'out'}, to: {logical_node_id: L_MAP, occurrence_id: O_MAP1, port: 'in'}},
    ],
    sources: [O_SRC], terminals: [O_MAP0, O_MAP1],
  };
}
function v3Physical() {
  return {
    plan_id: PLAN_ID, plan_revision: 3, recovery: 'checkpoint',
    stages: {
      [O_MAP0]: {stage_id: 's0', partition_count: 2, replica_min: 1, replica_target: 2, replica_max: 4, profile: {requests: {cpu: '2', memory: '1Gi'}}},
      [O_MAP1]: {stage_id: 's1', partition_count: 3, replica_min: 1, replica_target: 3, replica_max: 6, profile: {requests: {cpu: '3'}}},
    },
    exchanges: [{channel_id: 'c0', edge_id: E0, mode: 'durable_incremental'}, {channel_id: 'c1', edge_id: E1, mode: 'memory_incremental'}],
    limitations: ['某资源 actor 未声明恢复钩子'],
  };
}
function legacyDag() {
  return {
    schema_version: 'demiflow.logical_dag.v1', dag_id: DAG_ID, action_id: ACTION,
    nodes: [
      {logical_node_id: 'src', occurrence_id: 'occ-src', op_kind: 'source', input_ports: [], output_ports: ['out']},
      {logical_node_id: 'map', occurrence_id: 'occ-map-0', op_kind: 'map', input_ports: ['in'], output_ports: ['out']},
      {logical_node_id: 'map', occurrence_id: 'occ-map-1', op_kind: 'map', input_ports: ['in'], output_ports: ['out']},
    ],
    edges: [{from: {logical_node_id: 'src', port: 'out'}, to: {logical_node_id: 'map', port: 'in'}}],
    sources: ['src'], terminals: ['map'],
  };
}
function legacyPhysical() {
  return {
    plan_id: PLAN_ID, plan_revision: 1, recovery: 'final_commit',
    stages: {map: {stage_id: 's', partition_count: 1, role: 'worker', budgets: {cpu: 1}}},
    channels: [{channel_id: 'c0', exchange_mode: 'memory'}], worker_groups: [], limitations: [],
  };
}
function sealedEntry(over) {
  return Object.assign({
    action_id: ACTION, status: 'sealed', binding_level: 'sealed', version: 'v3',
    dag: v3Dag(), physical: v3Physical(), plan_id: PLAN_ID, sealed_at: 1700000000,
    recovery_restriction: {reason: 'actor_without_hook'},
    node_map: {}, node_observations: {available: false, reason: 'observation_page_unavailable'},
    identity_bridge: {available: false},
  }, over || {});
}
function legacyEntry(over) {
  return sealedEntry(Object.assign({version: 'v1', dag: legacyDag(), physical: legacyPhysical()}, over || {}));
}
function runGraph(entry, extra) {
  return Object.assign({
    binding: BINDING,
    logical: {available: true, action_count: 1, actions: [entry]},
    nodes: [{id: 'n0', api: 'x.y', kind: 'map', fields: {}, source: 'f.py', line: 1}],
    edges: [],
  }, extra || {});
}
// A real identity bridge + observation page, so the consumer half is exercised
// against admitted facts rather than a missing-bridge placeholder.
function bridged(occurrences) {
  return {
    identity_bridge: {available: true, bridge: {
      schema: 'demiflow.identity_bridge.v1', action_id: ACTION, dag_id: DAG_ID, report_digest: BINDING.basis_digest,
      entries: occurrences.map((occ, i) => ({logical_node_id: L_MAP, occurrence_id: occ, report_node_id: 'n' + i, report_occurrence: 0, mode: 'execute'})),
    }},
    node_observations: {available: true, attempt: 1, action_id: ACTION, dag_id: DAG_ID, plan_id: PLAN_ID, plan_revision: 3, has_more: false,
      entries: occurrences.map((occ, i) => ({logical_node_id: L_MAP, occurrence_id: occ, report_node_id: 'n' + i, report_occurrence: 0, availability: 'observed'}))},
    node_map: occurrences.reduce((acc, occ, i) => { acc[L_MAP] = 'n' + i; return acc; }, {}),
  };
}

function w018() {
  const P = presentation();
  // No actions -> no sealed view, never a fabricated graph.
  const empty = runGraph(sealedEntry()); empty.logical.actions = [];
  assert(P.chooseGraphView(empty, {}).kind === 'unavailable', 'empty actions -> unavailable');
  // Sealed v3 WITHOUT dag: the view may be chosen, but the projection must
  // refuse. The old logicalEntry() guard moved into normalizeExecutionPlan and
  // now covers BOTH formats (stricter than the old v1-exempt behaviour).
  const noDag = sealedEntry({dag: undefined});
  const g = runGraph(noDag);
  const choice = P.chooseGraphView(g, {});
  assert(choice.kind === 'sealed' && choice.actionId === ACTION, 'sealed v3 chosen');
  let proj = P.normalizeExecutionPlan(noDag, BINDING);
  assert(proj.available === false && proj.reason === 'unsupported_plan_schema', 'sealed v3 without dag -> refused (no fabricated graph)');
  assert(/缺少 dag/.test(proj.detail), 'refusal names the missing dag');
  const legacyNoDag = legacyEntry({dag: undefined});
  proj = P.normalizeExecutionPlan(legacyNoDag, BINDING);
  assert(proj.available === false, 'sealed legacy without dag also refused (guard covers both formats)');
  // Sealed v3 WITH a contract-valid dag -> real projection.
  const withDag = sealedEntry();
  proj = P.normalizeExecutionPlan(withDag, BINDING);
  assert(proj.available === true && proj.identity.format === 'v3', 'sealed v3 with dag -> v3 projection');
  assert(proj.nodes.length === 3 && proj.edges.length === 2, 'v3 topology projected');
  assert(proj.identity.dagId === DAG_ID && proj.identity.planId === PLAN_ID, 'identity carries dag/plan');
  // Projected (not sealed) -> honest refusal, never a sealed view.
  const projected = sealedEntry({status: 'projected', binding_level: 'declared'});
  const g2 = runGraph(projected);
  assert(P.chooseGraphView(g2, {}).kind === 'unavailable', 'projected action -> unavailable, not sealed');
  proj = P.normalizeExecutionPlan(projected, BINDING);
  assert(proj.available === false && proj.reason === 'not_sealed', 'projected -> not_sealed');
  // Explicit actionId selects the entry; an unknown id is refused, not guessed.
  const g3 = runGraph(withDag);
  assert(P.chooseGraphView(g3, {actionId: ACTION}).actionId === ACTION, 'actionId selects entry');
  assert(P.chooseGraphView(g3, {actionId: 'nope'}).reason === 'action_not_found', 'unknown actionId refused');
  // A v3 dag with an inconsistent version marker must not be silently accepted.
  proj = P.normalizeExecutionPlan(sealedEntry({version: 'v1'}), BINDING);
  assert(proj.available === false && proj.reason === 'identity_mismatch', 'v3 dag with v1 marker refused');
  return { ok: true };
}

function w019() {
  const P = presentation();
  const entry = sealedEntry();
  const g = runGraph(entry);
  const choice = P.chooseGraphView(g, {});
  const model = P.buildGraphModel(g, {choice});
  assert(model.choice.kind === 'sealed' && model.choice.label === '运行封存计划', 'sealed basis label');
  assert(model.execution && model.execution.format === 'v3', 'sealed model bound to this execution identity');
  assert(model.nodes.length === 3, 'one card per v3 occurrence');
  const proj = P.normalizeExecutionPlan(entry, BINDING);
  assert(proj.recovery.declared === 'checkpoint', 'recovery level shown');
  assert(proj.exchanges.length === 2, 'exchange count shown');
  const modes = proj.exchanges.map(item => item.mode);
  assert(modes.includes('durable_incremental') && modes.includes('memory_incremental'), 'exchange modes shown');
  assert(proj.recovery.limitations.length === 1 && proj.recovery.limitations[0] === '某资源 actor 未声明恢复钩子', 'limitation detail shown');
  assert(proj.recovery.restriction === 'actor_without_hook', 'recovery restriction shown');
  const sealedText = JSON.stringify(model);
  assert(!sealedText.includes('结构校验通过'), 'no execution-contract-passed claim');
  assert(!sealedText.includes('执行完成') && !sealedText.includes('成功'), 'no fabricated success without observations');
  assert(model.notices.includes('该 action 尚无节点身份桥；执行节点与报告观测的对应不猜测。'), 'missing bridge is honest, not guessed');
  // Projected / declared -> honest unbound view, draws no plan, claims no execution.
  const projected = sealedEntry({status: 'projected', binding_level: 'declared'});
  const g2 = runGraph(projected);
  const c2 = P.chooseGraphView(g2, {});
  const m2 = P.buildGraphModel(g2, {choice: c2});
  assert(m2.choice.kind === 'unavailable', 'projected honest unavailable label');
  assert(m2.execution === null, 'projected not bound to execution');
  assert(m2.nodes.length === 0, 'projected draws no fabricated plan');
  assert(m2.notices.some(n => n.includes('该 action 没有封存执行计划')), 'projected refusal is explicit');
  assert(!JSON.stringify(m2).includes('结构校验通过'), 'static graph no execution claim');
  // Sealed but unprojectable -> explicit refusal, never a silent old-version plan.
  const g3 = runGraph(sealedEntry({dag: undefined}));
  const m3 = P.buildGraphModel(g3, {choice: P.chooseGraphView(g3, {})});
  assert(m3.choice.kind === 'unavailable' && m3.execution === null, 'unprojectable sealed -> unbound');
  assert(m3.notices.some(n => n.includes('不会按旧版悄悄画完整计划')), 'honest refusal notice, no silent fallback');
  return { ok: true };
}

function w020() {
  const P = presentation();
  // v3: stages resolve per occurrence, profile/replicas/exchanges are v3 facts.
  const entry = sealedEntry(bridged([O_MAP0, O_MAP1]));
  const g = runGraph(entry);
  const proj = P.normalizeExecutionPlan(entry, BINDING);
  assert(proj.available === true, 'bridged v3 entry projects');
  const byOcc = new Map(proj.nodes.map(n => [n.occurrenceId, n]));
  const m0 = byOcc.get(O_MAP0), m1 = byOcc.get(O_MAP1);
  assert(m0 && m1, 'both occurrences projected');
  assert(m0.placement.profile.requests.cpu === '2', 'v3 stage.profile resolved by occurrence O_MAP0');
  assert(m1.placement.profile.requests.cpu === '3', 'v3 stage.profile resolved by occurrence O_MAP1');
  assert(m0.placement.replicaTarget === 2 && m1.placement.replicaTarget === 3, 'v3 replica targets per occurrence');
  assert(m0.placement.replicaMin === 1 && m0.placement.replicaMax === 4, 'v3 replica bounds');
  assert(m0.placement.stageId === 's0' && m1.placement.stageId === 's1', 'stages keyed per occurrence');
  assert(m0.placement.role === 'cpu', 'known resource_role admitted');
  assert(proj.exchanges.length === 2 && proj.exchanges.every(e => e.edgeId), 'v3 exchanges carry edge identity');
  assert(!('worker_groups' in proj), 'v3 projection has no worker_groups');
  assert(proj.recovery.restriction === 'actor_without_hook', 'recovery restriction preserved');
  // Node cards / details come from the real consumer surface.
  const model = P.buildGraphModel(g, {choice: P.chooseGraphView(g, {})});
  const card = model.nodes.find(c => c.selection.occurrenceId === O_MAP0);
  assert(card, 'occurrence card present');
  const summary = JSON.stringify(card.summary);
  assert(summary.includes('s0') && summary.includes('2 分区'), 'placement summary shows stage and partitions');
  assert(card.state.label.includes('已有观测'), 'observed availability label, not inferred success');
  assert(!card.state.label.includes('执行完成') && !card.state.label.includes('成功'), 'no fabricated success state');
  const details = P.buildNodeDetails(g, card.selection);
  assert(details.execution && details.execution.format === 'v3', 'details bound to v3 execution identity');
  assert(details.sourceAvailable === true, 'bridged node exposes source');
  assert(details.recoveryRestriction === 'actor_without_hook', 'details carry recovery restriction');
  assert(details.exchanges.length >= 1, 'details carry adjacent v3 exchanges');
  // Unbridged node must stay honest rather than guess a correspondence.
  const unbridged = P.buildGraphModel(runGraph(sealedEntry()), {choice: {kind: 'sealed', actionId: ACTION, label: '运行封存计划', reason: null}});
  const ucard = unbridged.nodes.find(c => c.selection.occurrenceId === O_MAP0);
  assert(ucard.state.label === '未桥接 · 无观测关联', 'missing bridge is honest');
  const udetails = P.buildNodeDetails(runGraph(sealedEntry()), ucard.selection);
  assert(udetails.sourceAvailable === false && udetails.sourceReason === 'identity_bridge_missing', 'unbridged source refused, not fabricated');
  // legacy v1: budgets instead of profile, channels instead of exchanges, no replicas.
  const lentry = legacyEntry();
  const lproj = P.normalizeExecutionPlan(lentry, BINDING);
  assert(lproj.available === true && lproj.identity.format === 'legacy', 'legacy entry projects as legacy');
  const lmap = lproj.nodes.find(n => n.logicalNodeId === 'map');
  assert(lmap.placement.profile.cpu === 1, 'legacy stage.budgets preserved');
  assert(lmap.placement.replicaTarget === null && lmap.placement.replicaMin === null, 'legacy has no replica facts');
  assert(lproj.exchanges.length === 1 && lproj.exchanges[0].edgeId === null, 'legacy channels projected without edge identity');
  assert(!('exchanges' in lentry.physical), 'legacy physical has no exchanges');
  assert(Array.isArray(lentry.physical.worker_groups), 'legacy physical keeps worker_groups');
  assert(lproj.recovery.declared === 'final_commit', 'legacy recovery preserved');
  // Multi-occurrence legacy group must not infer an execution outcome.
  const lmodel = P.buildGraphModel(runGraph(lentry), {choice: P.chooseGraphView(runGraph(lentry), {})});
  const lcard = lmodel.nodes.find(c => c.selection.logicalNodeId === 'map');
  assert(lcard && lcard.occurrences.length === 2, 'legacy group lists both occurrences');
  assert(lcard.state.label.includes('2 次出现'), 'multi-occurrence badge, no execution inference');
  assert(!lcard.state.label.includes('成功') && !lcard.state.label.includes('执行完成'), 'no fabricated success state');
  return { ok: true };
}

function w021() {
  const P = presentation();
  const {api, host} = graphHost();
  const view = api.mount(host, {});
  for (const name of ['update', 'select', 'fit', 'zoom', 'dispose'])
    assert(typeof view[name] === 'function', 'unified mount controller export ' + name);
  const canvasOf = () => findByAttr(host, 'class', 'wb-graph-canvas');
  const tipOf = g => { const t = g.children.find(c => c.tagName === 'title'); return t ? t._text : ''; };
  const textOfFirst = (g, cls) => { const e = findAllByClass(g, cls)[0]; return e ? e._text : ''; };
  // Cards no longer carry data-node; locate them by the sealed selection identity
  // that paintCard writes into <title>, which is the real consumer contract.
  const groupFor = key => findAllByClass(canvasOf(), 'wb-node').find(g => tipOf(g).includes(key));

  // Sealed v3: the main graph IS the executed plan (the old E2 toggle is gone).
  const g = runGraph(sealedEntry(bridged([O_MAP0, O_MAP1])));
  const model = P.buildGraphModel(g, {choice: P.chooseGraphView(g, {})});
  view.update(model);
  const canvas = canvasOf();
  assert(canvas.getAttribute('aria-label') === '运行封存计划', 'sealed basis aria-label on the merged main graph');
  assert(model.nodes.length === 3 && findAllByClass(canvas, 'wb-node').length === 3, 'one rendered card per v3 occurrence');
  const mapCard = model.nodes.find(c => c.selection.occurrenceId === O_MAP0);
  const group = groupFor(mapCard.key);
  assert(group, 'occurrence card located by execution identity');
  const tip = tipOf(group);
  assert(tip.includes('物理 stage/profile/exchange 与恢复限制见节点详情'), 'sealed physical hint in title');
  assert(tip.includes(mapCard.selection.key), 'title carries the sealed selection identity');
  // The merged C surface keeps traits.resource_role as placement.role; the old
  // 'worker' fixture value is not an admitted ROLES member, so it projected null.
  const aria = group.getAttribute('aria-label');
  assert(aria.includes('CPU'), 'resource role surfaces in the card aria-label');
  assert(aria.includes('s0') && aria.includes('2 分区'), 'sealed stage/partition facts in aria-label');
  assert(!textOfFirst(group, 'wb-node-state').includes('成功')
    && !textOfFirst(group, 'wb-node-state').includes('执行完成'), 'no fabricated success state');

  // Legacy multi-occurrence group: badge only, never an inferred outcome.
  const lg = runGraph(legacyEntry());
  const lmodel = P.buildGraphModel(lg, {choice: P.chooseGraphView(lg, {})});
  view.update(lmodel);
  assert(canvasOf().getAttribute('aria-label') === '运行封存计划', 'legacy sealed plan is also the main graph');
  const lcard = lmodel.nodes.find(c => c.selection.logicalNodeId === 'map');
  assert(lcard && lcard.occurrences.length === 2, 'legacy group keeps both occurrences');
  const lgroup = groupFor(lcard.key);
  assert(lgroup, 'legacy group card rendered');
  assert(textOfFirst(lgroup, 'wb-node-kind').includes('×2'), 'multi-occurrence badge');
  assert(textOfFirst(lgroup, 'wb-node-state').includes('2 次出现'), 'occurrence count, no execution inference');
  assert(!textOfFirst(lgroup, 'wb-node-state').includes('成功')
    && !textOfFirst(lgroup, 'wb-node-state').includes('执行完成'), 'no fabricated success state');
  assert(tipOf(lgroup).includes('成员 2 次出现'), 'title lists member occurrences');
  assert(tipOf(lgroup).includes('物理 stage/profile/exchange 与恢复限制见节点详情'), 'legacy sealed keeps physical hint');
  view.select(lcard.key);
  assert(lgroup.getAttribute('aria-pressed') === 'true', 'selected before rebind');

  // Rebinding the same graph to the non-sealed report projection must clear the
  // selection and drop the sealed physical hint (old 'declared' basis counterpart).
  const rmodel = P.buildGraphModel(lg, {choice: P.chooseGraphView(lg, {preference: 'report'})});
  view.update(rmodel);
  const canvas2 = canvasOf();
  assert(canvas2.getAttribute('aria-label') === '数据算子图', 'report basis aria-label');
  assert(rmodel.selected === null, 'selection cleared on new binding');
  const rgroups = findAllByClass(canvas2, 'wb-node');
  assert(rgroups.length === 1 && rgroups.every(g2 => g2.getAttribute('aria-pressed') === 'false'), 'no stale pressed card after rebind');
  assert(!tipOf(rgroups[0]).includes('物理 stage/profile/exchange'), 'report basis omits the sealed physical hint');

  // An unavailable choice must render an honest empty graph, not a fabricated one.
  const umodel = P.buildGraphModel(lg, {choice: P.chooseGraphView(lg, {actionId: 'nope'})});
  view.update(umodel);
  assert(findAllByClass(canvasOf(), 'wb-node').length === 0, 'unavailable choice renders no nodes');
  assert(textOf(findAllByClass(host, 'wb-graph-notices')[0]).includes('action'), 'unavailable reason is surfaced');
  view.dispose();
  assert(host.children.length === 0, 'dispose removes the mounted view');
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
