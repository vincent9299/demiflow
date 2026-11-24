"""Lane D unit contract tests (C08/C12, gate G-D).

These exercise the REAL lane-D symbols (dataflow_checkpoint / dataflow_sinks /
dataflow_seal) over the REAL native RunJournal / CommitCoordinator / RunContext /
StateArtifacts / chain.plan_code_pin / identity_bridge. Cross-lane dependencies that
have not landed in this worktree (lane C DTOs/codec/errors, lane W history, lane H
store) are provided as strict spec-bound test-domain fakes installed into
sys.modules before the production imports; production code carries no fallback.

IL012: every test process asserts demiflow and the loaded submodules resolve to
this worktree. V018: the D004 crash matrix truly kills a child process at native
fault points and reconciles in a fresh process. No GPU/multi-host/PostgreSQL claims.
"""
from __future__ import annotations
from demiflow.execution.checkpoint.coordinator import ack_source_claims

import hashlib
import json
import os
import subprocess
import sys
import types
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import pytest

# --- IL012 in-process tree binding -----------------------------------------
WORKTREE = Path(__file__).resolve().parent.parent
import demiflow  # noqa: E402

assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE), demiflow.__file__


def _real_module_available(name):
    import importlib
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def _install_fakes():
    """Install spec-bound cross-lane fakes only where the real module is absent."""
    if _real_module_available("demiflow.execution.contracts.dataflow"):
        return
    import demiflow.execution.contracts as contracts_pkg
    from demiflow.execution.contracts.recovery_contract import identity

    dto = types.ModuleType("demiflow.execution.contracts.dataflow")

    def dc(cls):
        return dataclass(frozen=True)(cls)

    @dc
    class ArtifactRef:
        digest: str
        size_bytes: int
        store_id: str
        key: str
        media_type: str

    @dc
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

    @dc
    class InputPort:
        name: str
        requirement: str
        ordering: str
        schema_ref: object

    @dc
    class KernelSpec:
        kind: str
        implementation_ref: object
        configuration: object
        inputs: tuple
        state: str
        effect: str
        resource_role: str
        profile: object
        output_bound_bytes: int
        mergeable: bool

    @dc
    class GraphNode:
        occurrence_id: str
        semantic_id: str
        kind: str
        kernel: object
        program_ref: object
        shared_token: str

    @dc
    class GraphEdge:
        edge_id: str
        producer: str
        output_port: str
        consumer: str
        input_port: str

    @dc
    class DataflowGraph:
        schema: str
        action_id: str
        dag_id: str
        nodes: tuple
        edges: tuple
        terminals: tuple
        sources: tuple

    @dc
    class SourceSnapshot:
        source_id: str
        kind: str
        immutable_identity: object
        finite: bool
        replay: str
        manifest_ref: object

    @dc
    class SourceSplit:
        split_id: str
        source_id: str
        locator: object
        start_cursor: object
        estimated_bytes: object
        required_domain: str

    @dc
    class SourceCursor:
        split_id: str
        offset: object
        next_sequence: int
        end_of_input: bool
        lease_ref: object
        pending_refs: tuple

    @dc
    class Record:
        record_id: str
        source_position: object
        payload_ref: object
        inline: object
        encoded_bytes: int
        sequence: int

    @dc
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

    @dc
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

    @dc
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

    @dc
    class ResidencyPlan:
        simultaneous_groups: tuple
        control_reserved_bytes: int
        consumer_reserved_slots: int
        required_spill_bytes: int

    @dc
    class FiniteScope:
        scope_id: str
        generation: int
        membership: tuple
        completion_condition: str

    @dc
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

    @dc
    class SealedExecution:
        logical_ref: object
        physical_ref: object
        programs: tuple
        code_pin_ref: object
        identity_bridge_ref: object
        source_refs: tuple
        constants_ref: object

    @dc
    class CutRequest:
        cut_id: str
        sequence: int
        plan_revision: int
        route_epoch: int
        participants: tuple
        final: bool

    @dc
    class CutContribution:
        cut_id: str
        participant_id: str
        boot_id: str
        sources: tuple
        operator_states: object
        channels: object
        buffers: tuple
        effects: tuple
        sinks: object
        final: bool

    @dc
    class RecoveryImage:
        commit_id: str
        cut: object
        contributions: tuple
        confirmed_sinks: object
        source_vector: tuple

    @dc
    class ProgramDeclaration:
        occurrence_id: str
        callable_spec: object
        source_spec: object
        action_program: object
        code_identity: object

    @dc
    class CompiledDataflow:
        graph: object
        declarations: tuple

    @dc
    class LoadedProgram:
        kind: str
        value: object
        digest: str

    @dc
    class ActionProgram:
        name: str
        kind: str
        value: object

    @dc
    class WorkerLease:
        allocation_id: str
        boot_id: str
        stage_id: str
        partition_ids: tuple
        host_id: str
        endpoint_ref: str
        grant_ref: object
        expires_at_ms: int

    @dc
    class WorkerAssignment:
        schema: str
        run_id: str
        action_id: str
        attempt: int
        fence: str
        plan_id: str
        plan_revision: int
        lease: object
        sealed: object
        stages: tuple
        inputs: tuple
        outputs: tuple
        source_splits: tuple
        restore_refs: tuple
        bounds: object

    @dc
    class DriverServices:
        action: object
        journal_service: object
        store: object
        resources: object
        policy: object
        bounds: object
        hosts: tuple
        clock_ms: object

    @dc
    class RuntimeServices:
        store: object
        journal: object
        bounds: object
        assignment: object
        clock_ms: object
        check_stop: object
        constants: object
        programs: object

    @dc
    class CapabilityReport:
        supported: bool
        reasons: tuple
        recovery: str
        required_domains: tuple
        required_profiles: tuple

    @dc
    class HistoricalBinding:
        schema_version: str
        original_plan_ref: object
        original_code_pin_ref: object
        compatible: bool
        reason: object
        restore_image: object

    for name, obj in list(locals().items()):
        if isinstance(obj, type) and obj.__module__ == dto.__name__:
            setattr(dto, name, obj)
    # The dataclass decorator rebinds __module__ to this test module; fix it so
    # the registry above also catches them via explicit export below.
    for name in ["ArtifactRef", "Bounds", "InputPort", "KernelSpec", "GraphNode",
                 "GraphEdge", "DataflowGraph", "SourceSnapshot", "SourceSplit",
                 "SourceCursor", "Record", "StageSpec", "ExchangeSpec",
                 "RuntimePolicy", "ResidencyPlan", "FiniteScope", "PhysicalDataflow",
                 "SealedExecution", "CutRequest", "CutContribution", "RecoveryImage",
                 "ProgramDeclaration", "CompiledDataflow", "LoadedProgram",
                 "ActionProgram", "WorkerLease", "WorkerAssignment", "DriverServices",
                 "RuntimeServices", "CapabilityReport", "HistoricalBinding"]:
        setattr(dto, name, locals()[name])
    sys.modules[dto.__name__] = dto
    setattr(contracts_pkg, "dataflow", dto)

    # -- codec (C001/C002/C004/C005) --
    codec = types.ModuleType("demiflow.execution.contracts.dataflow_codec")

    def _ref(x):
        return ArtifactRef(**x) if x is not None else None

    def _logical_proj(obj):
        if isinstance(obj, dict):
            if set(obj) == {"digest", "size_bytes", "store_id", "key", "media_type"}:
                return {"digest": obj["digest"], "size_bytes": obj["size_bytes"],
                        "media_type": obj["media_type"]}
            return {k: _logical_proj(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_logical_proj(v) for v in obj]
        return obj

    def compute_graph_digest(graph):
        return identity(["dataflow-dag-v3", graph.action_id,
                         [[n.occurrence_id, n.semantic_id, n.kind, n.shared_token] for n in graph.nodes],
                         [[e.edge_id, e.producer, e.output_port, e.consumer, e.input_port] for e in graph.edges],
                         list(graph.terminals), list(graph.sources)])

    def compute_physical_digest(physical):
        proj = _logical_proj(asdict(physical))
        proj.pop("plan_id", None)
        return identity(["dataflow-physical-v3", proj])

    def _graph_from_dict(d):
        nodes = []
        for n in d["nodes"]:
            k = n["kernel"]
            kernel = KernelSpec(kind=k["kind"], implementation_ref=_ref(k["implementation_ref"]),
                                configuration=k["configuration"],
                                inputs=tuple(InputPort(name=i["name"], requirement=i["requirement"],
                                                       ordering=i["ordering"], schema_ref=_ref(i["schema_ref"]))
                                             for i in k["inputs"]),
                                state=k["state"], effect=k["effect"], resource_role=k["resource_role"],
                                profile=k["profile"], output_bound_bytes=k["output_bound_bytes"],
                                mergeable=k["mergeable"])
            nodes.append(GraphNode(occurrence_id=n["occurrence_id"], semantic_id=n["semantic_id"],
                                   kind=n["kind"], kernel=kernel, program_ref=_ref(n["program_ref"]),
                                   shared_token=n["shared_token"]))
        return DataflowGraph(schema=d["schema"], action_id=d["action_id"], dag_id=d["dag_id"],
                             nodes=tuple(nodes), edges=tuple(GraphEdge(**e) for e in d["edges"]),
                             terminals=tuple(d["terminals"]), sources=tuple(d["sources"]))

    def _physical_from_dict(d):
        stages = tuple(StageSpec(stage_id=s["stage_id"], occurrences=tuple(s["occurrences"]),
                                 partitions=tuple(s["partitions"]), profile=s["profile"],
                                 replica_min=s["replica_min"], replica_target=s["replica_target"],
                                 replica_max=s["replica_max"], resident_bytes=s["resident_bytes"],
                                 output_bytes=s["output_bytes"], state_bytes=s["state_bytes"],
                                 kernel_refs=tuple(_ref(r) for r in s["kernel_refs"])) for s in d["stages"])
        exchanges = tuple(ExchangeSpec(**e) for e in d["exchanges"])
        sources = tuple(SourceSnapshot(source_id=s["source_id"], kind=s["kind"],
                                       immutable_identity=s["immutable_identity"], finite=s["finite"],
                                       replay=s["replay"], manifest_ref=_ref(s["manifest_ref"]))
                        for s in d["sources"])
        splits = tuple(SourceSplit(**s) for s in d["splits"])
        policy = RuntimePolicy(**d["policy"])
        bounds = Bounds(**d["bounds"])
        residency = ResidencyPlan(simultaneous_groups=tuple(tuple(g) for g in d["residency"]["simultaneous_groups"]),
                                  control_reserved_bytes=d["residency"]["control_reserved_bytes"],
                                  consumer_reserved_slots=d["residency"]["consumer_reserved_slots"],
                                  required_spill_bytes=d["residency"]["required_spill_bytes"])
        scopes = tuple(FiniteScope(scope_id=s["scope_id"], generation=s["generation"],
                                   membership=tuple(s["membership"]), completion_condition=s["completion_condition"])
                       for s in d["scopes"])
        return PhysicalDataflow(schema=d["schema"], dag_id=d["dag_id"], plan_id=d["plan_id"],
                                plan_revision=d["plan_revision"], stages=stages, exchanges=exchanges,
                                residency=residency, scopes=scopes, sources=sources, splits=splits,
                                policy=policy, bounds=bounds, recovery=d["recovery"],
                                limitations=tuple(d["limitations"]), constants_ref=_ref(d["constants_ref"]))

    def encode_wire(value, *, type_name, bounds):
        if type_name == "DataflowGraph":
            assert isinstance(value, DataflowGraph)
            proj = asdict(value)
        elif type_name == "PhysicalDataflow":
            assert isinstance(value, PhysicalDataflow)
            proj = asdict(value)
        else:
            raise AssertionError("unsupported wire type " + type_name)
        raw = json.dumps(proj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(raw) > bounds.max_control_bytes:
            raise AssertionError("wire exceeds budget")
        return raw

    def decode_wire(raw, *, type_name, bounds):
        if len(raw) > bounds.max_control_bytes:
            raise AssertionError("wire exceeds budget")
        d = json.loads(raw)
        if type_name == "DataflowGraph":
            return _graph_from_dict(d)
        if type_name == "PhysicalDataflow":
            return _physical_from_dict(d)
        raise AssertionError("unsupported wire type " + type_name)

    codec.encode_wire = encode_wire
    codec.decode_wire = decode_wire
    codec.compute_graph_digest = compute_graph_digest
    codec.compute_physical_digest = compute_physical_digest
    sys.modules[codec.__name__] = codec
    setattr(contracts_pkg, "dataflow_codec", codec)

    # -- errors (C006) --
    errors = types.ModuleType("demiflow.execution.contracts.dataflow_errors")

    class DataflowError(Exception):
        def __init__(self, code, message, *, retryable=False, context=None):
            self.code = code
            self.message = message
            self.retryable = retryable
            self.context = dict(context or {})
            super().__init__(message)

    errors.DataflowError = DataflowError
    sys.modules[errors.__name__] = errors
    setattr(contracts_pkg, "dataflow_errors", errors)

    # -- history (W001/W002/W003) --
    history = types.ModuleType("demiflow.execution.journal.dataflow_history")
    history.state = {"binding": None, "supported": True, "image": None}

    def load_execution_version(directory, action_id, *, max_bytes):
        b = history.state["binding"]
        if b is None:
            b = HistoricalBinding(schema_version="v2", original_plan_ref=None,
                                  original_code_pin_ref=None, compatible=False,
                                  reason="legacy", restore_image=None)
        return b

    def validate_resume_compatibility(binding, *, current_runtime, store):
        return CapabilityReport(supported=history.state["supported"],
                                reasons=() if history.state["supported"] else ("legacy incompatible",),
                                recovery="checkpoint", required_domains=(), required_profiles=())

    def translate_native_checkpoint(binding, *, store, bounds):
        if history.state["image"] is None:
            raise DataflowError("INCOMPATIBLE_HISTORY", "no historical image")
        return history.state["image"]

    history.load_execution_version = load_execution_version
    history.validate_resume_compatibility = validate_resume_compatibility
    history.translate_native_checkpoint = translate_native_checkpoint
    sys.modules[history.__name__] = history
    recovery_history = types.ModuleType("demiflow.execution.checkpoint.history")
    recovery_history.validate_resume_compatibility = validate_resume_compatibility
    recovery_history.translate_native_checkpoint = translate_native_checkpoint
    sys.modules[recovery_history.__name__] = recovery_history


_install_fakes()

from demiflow.execution.contracts.dataflow import (  # noqa: E402
    ArtifactRef, Bounds, CompiledDataflow, CutContribution, DataflowGraph, DriverServices,
    ExchangeSpec, GraphEdge, GraphNode, KernelSpec, LoadedProgram, PhysicalDataflow,
    ProgramDeclaration, Record, RecoveryImage, ResidencyPlan, RuntimePolicy, RuntimeServices,
    SourceCursor, SourceSnapshot, SourceSplit, StageSpec, WorkerAssignment, WorkerLease,
)
from demiflow.execution.contracts.dataflow_codec import (  # noqa: E402
    compute_graph_digest, compute_physical_digest,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError  # noqa: E402
from demiflow.execution.checkpoint import coordinator as ckpt  # noqa: E402
from demiflow.execution.outputs import sinks  # noqa: E402
from demiflow.execution.seal import plan as seal  # noqa: E402
from demiflow.execution import commit as commit_module  # noqa: E402
from demiflow.execution.contracts.recovery_contract import RunRef, SourcePosition, document, identifier, identity
from demiflow.execution.pipeline.run_context import RunAction, RunContext  # noqa: E402
from demiflow.execution.journal.run_journal import RunJournal  # noqa: E402
from demiflow.data.datasink import Datasink, WriteContext  # noqa: E402
from demiflow.data.plan import CallableSpec  # noqa: E402

ACTION = "action-d"
SINK_ID = "out"


# --- spec-bound fake store (H031/H032): disk-backed, content-addressed ------
class FakeStore:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key):
        return self.root / key

    def put(self, chunks, *, max_bytes, media_type):
        raw = b"".join(chunks)
        if len(raw) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "store put exceeds budget")
        digest = hashlib.sha256(raw).hexdigest()
        path = self._path(digest)
        if not path.exists():
            path.write_bytes(raw)
        return ArtifactRef(digest=digest, size_bytes=len(raw), store_id="test",
                           key=digest, media_type=media_type)

    def read(self, ref, *, max_bytes):
        path = self._path(ref.key)
        if not path.is_file():
            raise DataflowError("CORRUPT_ARTIFACT", "store artifact missing")
        raw = path.read_bytes()
        if len(raw) > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "store read exceeds budget")
        yield raw

    def read_into(self, ref, destination):
        with self._path(ref.key).open("rb") as stream:
            assert stream.readinto(destination) == ref.size_bytes

    def inspect(self, ref):
        path = self._path(ref.key)
        if not path.is_file():
            return False
        raw = path.read_bytes()
        if len(raw) != ref.size_bytes or hashlib.sha256(raw).hexdigest() != ref.digest:
            raise DataflowError("INVALID_CONTRACT", "artifact bytes do not match its reference")
        return True

    def object_signature(self, ref):
        # This crash-test store has no reusable checksum proofs. Publication
        # must authenticate its leaves through inspect on every attempt.
        return None

    def reusable_signature(self, ref, *, witness):
        return None

    def _owners_path(self, key):
        return self.root / (key + ".owners.json")

    def retain(self, ref, *, owner):
        p = self._owners_path(ref.key)
        owners = set(json.loads(p.read_text())) if p.exists() else set()
        owners.add(owner)
        p.write_text(json.dumps(sorted(owners)))

    def release(self, ref, *, owner):
        p = self._owners_path(ref.key)
        if not p.exists():
            return
        owners = set(json.loads(p.read_text()))
        owners.discard(owner)
        if owners:
            p.write_text(json.dumps(sorted(owners)))
        else:
            p.unlink()
            data = self._path(ref.key)
            if data.exists():
                data.unlink()

    def tamper(self, ref, raw):
        """Test-only: overwrite bytes without changing the ref (digest drift)."""
        self._path(ref.key).write_bytes(raw)


# --- builders ---------------------------------------------------------------
def make_bounds():
    return Bounds(max_nodes=64, max_edges=64, max_partitions=16, max_depth=8,
                  max_control_bytes=1 << 20, max_row_bytes=1 << 16, max_batch_bytes=1 << 20,
                  max_resident_bytes=1 << 24, max_spill_bytes=1 << 24, max_state_bytes=1 << 20,
                  max_pending_events=1024, max_open_files=64, max_output_rows=10000,
                  timeout_ms=60000)


def make_kernel(*, kind="terminal", configuration=None, implementation_ref=None):
    return KernelSpec(kind=kind, implementation_ref=implementation_ref,
                      configuration=configuration if configuration is not None else {"sink_id": SINK_ID},
                      inputs=(), state="none", effect="journaled", resource_role="cpu",
                      profile=ResourceProfile(requests={"cpu_millis": 100, "memory_bytes": 1024}), output_bound_bytes=1 << 20, mergeable=False)


def make_graph(action_id=ACTION, *, kinds=("source", "transform", "terminal"), same_semantic=False):
    nodes = []
    for index, kind in enumerate(kinds):
        occ = identity(["dataflow-occurrence-v3", action_id, index])
        sem = identity(["dataflow-semantic-v3", "shared" if same_semantic else index])
        nodes.append(GraphNode(occurrence_id=occ, semantic_id=sem, kind=kind,
                               kernel=make_kernel(kind=kind), program_ref=None,
                               shared_token=str(index)))
    edges = tuple(GraphEdge(edge_id=identity(["dataflow-edge-v3", i]),
                            producer=nodes[i].occurrence_id, output_port="out",
                            consumer=nodes[i + 1].occurrence_id, input_port="in")
                  for i in range(len(nodes) - 1))
    graph = DataflowGraph(schema="demiflow.dataflow_graph.v3", action_id=action_id,
                          dag_id="0" * 64, nodes=tuple(nodes), edges=edges,
                          terminals=(nodes[-1].occurrence_id,), sources=(nodes[0].occurrence_id,))
    return replace(graph, dag_id=compute_graph_digest(graph))


def make_physical(action_id, graph, store, *, partitions=("p0", "p1"), with_sources=False,
                  plan_revision=1, route_epoch=0):
    bounds = make_bounds()
    constants_raw = document({"schema": "demiflow.dataflow_constants.v3", "constants": {}})
    constants_ref = store.put(iter([constants_raw]), max_bytes=bounds.max_state_bytes,
                              media_type=seal.CONSTANTS_MEDIA)
    # This plan has one stage, so the stage is granted the plan's whole resident budget.
    # The record codec checks its workspace against the stage grant: with the former 1 KiB
    # (below one batch plus one row, a grant the compiler refuses) every record failed in the
    # codec, before any of the commit, recovery and budget behaviour these tests are about.
    # A grant of only one batch plus one row would still stop D009's 10,001 rows in the codec,
    # not at the sink partition budget that stopped them before the codec check existed.
    stage = StageSpec(stage_id="stage-0", occurrences=tuple(n.occurrence_id for n in graph.nodes),
                      partitions=tuple(partitions), profile=ResourceProfile(requests={"cpu_millis": 100, "memory_bytes": bounds.max_resident_bytes}), replica_min=1, replica_target=1,
                      replica_max=1, resident_bytes=0, output_bytes=0, state_bytes=0, kernel_refs=())
    exchanges = ()
    if route_epoch:
        exchanges = (ExchangeSpec(channel_id=identity(["chan"]), edge_id=identity("e"),
                                  producer_partitions=tuple(partitions),
                                  consumer_partitions=tuple(partitions), mode="finite_sealed",
                                  routing="forward", keys=(), scope_id=None, route_epoch=route_epoch,
                                  credit_rows=10, credit_bytes=1024, storage_bytes=1024,
                                  read_domain="host", reason="test"),)
    sources, splits = (), ()
    if with_sources:
        manifest_ref = store.put(iter([b"{}"]), max_bytes=bounds.max_control_bytes,
                                 media_type=seal.GRAPH_MEDIA)
        sources = (SourceSnapshot(source_id=identity("src-0"), kind="items", immutable_identity={},
                                  finite=True, replay="seek", manifest_ref=manifest_ref),)
        splits = tuple(SourceSplit(split_id=identity("split-" + p), source_id=identity("src-0"), locator={},
                                   start_cursor={}, estimated_bytes=None, required_domain="host")
                       for p in partitions)
    physical = PhysicalDataflow(schema="demiflow.physical_dataflow.v3", dag_id=graph.dag_id,
                                plan_id="0" * 64, plan_revision=plan_revision, stages=(stage,),
                                exchanges=exchanges, residency=ResidencyPlan((), 0, 0, 0), scopes=(),
                                sources=sources, splits=splits,
                                policy=RuntimePolicy(provider="local", durability="durable",
                                                     default_partitions=len(partitions), max_replicas=1,
                                                     fusion=False, checkpoint_rows=100,
                                                     checkpoint_interval_ms=1000, fairness_quantum_bytes=1024,
                                                     control_timeout_ms=1000, store_id="test",
                                                     required_recovery="checkpoint"),
                                bounds=bounds, recovery="checkpoint", limitations=(),
                                constants_ref=constants_ref)
    return replace(physical, plan_id=compute_physical_digest(physical))


def make_record(index, row, bounds):
    body = {"record_id": identity(["rec", index]), "source_position": {"split": "split-p0", "row": index},
            "sequence": index, "inline": row, "payload_ref": None}
    from demiflow.execution.contracts.record_codec import _encode_payload
    raw = _encode_payload(row, max_bytes=bounds.max_row_bytes)
    return Record(record_id=body["record_id"], source_position=body["source_position"],
                  payload_ref=None, inline=row, encoded_bytes=len(raw), sequence=index)


def driver_services(action, physical, store):
    return DriverServices(action=action, journal_service=None, store=store, resources=None,
                          policy=physical.policy, bounds=physical.bounds, hosts=(), clock_ms=lambda: 0)


def runtime_services(action, physical, store, *, partition_ids=("p0", "p1")):
    lease = WorkerLease(allocation_id="alloc", boot_id="boot-0", stage_id="stage-0",
                        partition_ids=tuple(partition_ids), host_id="host-0", endpoint_ref="ep",
                        grant_ref=store.put(iter([b"{}"]), max_bytes=1024, media_type="application/json"), expires_at_ms=9999999999999)
    ref = lease.grant_ref
    from demiflow.execution.contracts.dataflow import SealedExecution
    sealed = SealedExecution(ref, ref, (), ref, ref, (), ref)
    assignment = WorkerAssignment(schema="demiflow.dataflow_assignment.v3", run_id=action.context.ref.run_id,
                                  action_id=action.action_id, attempt=action.context.attempt,
                                  fence=action.context.fence, plan_id=physical.plan_id,
                                  plan_revision=physical.plan_revision, lease=lease, sealed=sealed,
                                  stages=physical.stages, inputs=(), outputs=(), source_splits=physical.splits,
                                  restore_refs=(), bounds=physical.bounds)
    return RuntimeServices(store=store, journal=None, bounds=physical.bounds, assignment=assignment,
                           clock_ms=lambda: 0, check_stop=lambda: None, constants={}, programs={})


def make_adapter(action, physical, store):
    spec = make_kernel(configuration={"sink_id": SINK_ID})
    return sinks.make_sink_adapter(spec, services=driver_services(action, physical, store))


def prepare_partition(action, physical, store, partition_id, rows):
    spec = make_kernel(configuration={"sink_id": SINK_ID, "partition_id": partition_id})
    services = runtime_services(action, physical, store)
    records = [make_record(i, row, physical.bounds) for i, row in enumerate(rows)]
    return sinks.prepare_sink_partition(iter(records), spec=spec, services=services)


def contribution(cut, participant_id, *, sink_ref=None, sources=(), boot_id="boot-0", buffers=()):
    return CutContribution(cut_id=cut.cut_id, participant_id=participant_id, boot_id=boot_id,
                           sources=tuple(sources), operator_states={}, channels={},
                           buffers=tuple(buffers), effects=(),
                           sinks={SINK_ID: sink_ref} if sink_ref is not None else {}, final=cut.final)


@pytest.fixture
def env(tmp_path):
    run_dir = tmp_path / "run"
    with RunContext.start(run_dir) as run:
        declaration = {"kind": "dataflow", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, ACTION, declaration)
        action = RunAction(run, ACTION, declaration)
        store = FakeStore(run_dir / "fake-store")
        try:
            yield types.SimpleNamespace(run=run, action=action, store=store, run_dir=run_dir)
        finally:
            with run.journal._connection() as db:
                pending = [r[0] for r in db.execute(
                    "SELECT action_id FROM runtime_actions WHERE result IS NULL").fetchall()]
            for action_id in pending:
                run.journal.complete_execution_action(run.fence, action_id, {"kind": "test-complete"})


def _code_error(fn, code):
    with pytest.raises(DataflowError) as exc:
        fn()
    assert exc.value.code == code, (exc.value.code, str(exc.value))


# --- D001 -------------------------------------------------------------------
def test_d001(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    coord = ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store)
    try:
        assert coord._participants == (compute_participant_id("stage-0", "p0"), compute_participant_id("stage-0", "p1"))
        # one live driver owner per action
        _code_error(lambda: ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store),
                    "INVALID_CONTRACT")
    finally:
        coord.close()
    # a worker (no driver RunAction) cannot construct it
    _code_error(lambda: ckpt.DataflowCheckpointCoordinator(object(), physical, store=env.store),
                "INVALID_CONTRACT")
    # after close a new owner may take over
    coord2 = ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store)
    coord2.close()


# --- D002 -------------------------------------------------------------------
def test_d002(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store, with_sources=True)
    coord = ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store)
    try:
        # final without EOI evidence is refused
        _code_error(lambda: coord.begin(final=True), "INVALID_CONTRACT")
        cut1 = coord.begin(final=False)
        assert cut1.sequence == 1 and cut1.participants == (compute_participant_id("stage-0", "p0"), compute_participant_id("stage-0", "p1"))
        assert cut1.cut_id == identity(["dataflow-cut-v3", ACTION, physical.plan_revision, 0, 1])
        # concurrent begin never reuses a sequence with different members
        cut2 = coord.begin(final=False)
        assert cut2.sequence == 2 and cut2.cut_id != cut1.cut_id
        assert cut2.participants == cut1.participants
        # record EOI through a non-final commit, then final is admitted
        ref0 = prepare_partition(env.action, physical, env.store, "p0", [{"x": 1}])
        ref1 = prepare_partition(env.action, physical, env.store, "p1", [{"x": 2}])
        eoi = tuple(SourceCursor(split_id=s.split_id, offset={"row": 1}, next_sequence=1,
                                 end_of_input=True, lease_ref=None, pending_refs=())
                    for s in physical.splits)
        coord.contribute(contribution(cut1, compute_participant_id("stage-0", "p0"), sink_ref=ref0, sources=eoi))
        coord.contribute(contribution(cut1, compute_participant_id("stage-0", "p1"), sink_ref=ref1, sources=eoi))
        coord.commit(cut1.cut_id, sinks={SINK_ID: make_adapter(env.action, physical, env.store)})
        final = coord.begin(final=True)
        # begin sequences are monotonic per call (cut_id uniqueness); this is the
        # third begin. Native commit sequences stay contiguous in begin order.
        assert final.final and final.sequence == 3
    finally:
        coord.close()


# --- D003 -------------------------------------------------------------------
def test_d003(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    coord = ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store)
    try:
        cut = coord.begin(final=False)
        ref0 = prepare_partition(env.action, physical, env.store, "p0", [{"x": 1}])
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p0"), sink_ref=ref0))
        # first worker ready does NOT complete the cut: second still missing
        _code_error(lambda: coord.commit(cut.cut_id, sinks={SINK_ID: make_adapter(env.action, physical, env.store)}),
                    "INVALID_CONTRACT")
        # same content idempotent; conflicting content rejected
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p0"), sink_ref=ref0))
        ref_other = prepare_partition(env.action, physical, env.store, "p0", [{"x": 999}])
        _code_error(lambda: coord.contribute(contribution(cut, compute_participant_id("stage-0", "p0"), sink_ref=ref_other)),
                    "INVALID_CONTRACT")
        # non-participant refused
        _code_error(lambda: coord.contribute(contribution(cut, "stage-9:p9", sink_ref=ref0)),
                    "INVALID_CONTRACT")
        ref1 = prepare_partition(env.action, physical, env.store, "p1", [{"x": 2}])
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p1"), sink_ref=ref1))
        image = coord.commit(cut.cut_id, sinks={SINK_ID: make_adapter(env.action, physical, env.store)})
        assert set(image.confirmed_sinks) == {SINK_ID}
    finally:
        coord.close()


# --- D004 (incl. V018 real process-kill crash matrix) ------------------------
def _scenario_commit(run_dir, action_id):
    """Deterministic commit scenario shared by the crash child and recover child."""
    store = FakeStore(Path(run_dir) / "fake-store")
    graph = make_graph(action_id)
    physical = make_physical(action_id, graph, store)
    return store, graph, physical


def _commit_child(run_dir, action_id, fault):
    store, graph, physical = _scenario_commit(run_dir, action_id)
    with RunContext.start(Path(run_dir)) as run:
        declaration = {"kind": "dataflow", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, action_id, declaration)
        action = RunAction(run, action_id, declaration)
        coord = ckpt.DataflowCheckpointCoordinator(action, physical, store=store)
        cut = coord.begin(final=True)
        ref0 = prepare_partition(action, physical, store, "p0", [{"x": 1}, {"x": 2}])
        ref1 = prepare_partition(action, physical, store, "p1", [{"x": 3}])
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p0"), sink_ref=ref0))
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p1"), sink_ref=ref1))
        adapter = make_adapter(action, physical, store)

        def crash(name, commit_id):
            if name == fault:
                sys.stdout.flush()
                os._exit(137)
        commit_module.fault_point = crash
        coord.commit(cut.cut_id, sinks={SINK_ID: adapter})
        print(json.dumps({"completed": True}))


def _recover_child(run_dir, action_id):
    store, graph, physical = _scenario_commit(run_dir, action_id)
    journal = RunJournal(Path(run_dir))
    run_id = journal.recovery_identity()["run_id"]
    with RunContext.resume(RunRef(str(run_dir), run_id)) as run:
        action = RunAction(run, action_id, {"kind": "dataflow", "source": {"source_id": "fixed"}})
        coord = ckpt.DataflowCheckpointCoordinator(action, physical, store=store)
        adapter = make_adapter(action, physical, store)
        image = coord.recover(sinks={SINK_ID: adapter})
        coord.close()
        run.journal.complete_execution_action(run.fence, action_id, {"kind": "test-complete"})
        facts = run.journal.execution_commits(action_id)
        marker = adapter._marker(types.SimpleNamespace(commit_id=image.commit_id))
        print(json.dumps({"commit_id": image.commit_id,
                          "statuses": [f["status"] for f in facts],
                          "n_facts": len(facts),
                          "rows": image.confirmed_sinks and adapter.inspect(
                              facts[0]["prepared"]["sinks"][SINK_ID],
                              commit_module.CommitCoordinator(action)._context(image.commit_id))["data"]["rows"],
                          "marker_exists": marker.is_file()}))


CHILD_SRC = """
import sys, json
sys.path.insert(0, {worktree!r})
sys.path.insert(0, {tests!r})
import test_runtime_v3_d as T
mode, run_dir, action_id = sys.argv[1:4]
if mode == "commit":
    T._commit_child(run_dir, action_id, sys.argv[4])
else:
    T._recover_child(run_dir, action_id)
"""


def test_d004(env, tmp_path):
    # in-process happy path: ACK follows every sink receipt; image is complete
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    coord = ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store)
    try:
        cut = coord.begin(final=True)
        ref0 = prepare_partition(env.action, physical, env.store, "p0", [{"x": 1}])
        ref1 = prepare_partition(env.action, physical, env.store, "p1", [{"x": 2}])
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p0"), sink_ref=ref0))
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p1"), sink_ref=ref1))
        image = coord.commit(cut.cut_id, sinks={SINK_ID: make_adapter(env.action, physical, env.store)})
        fact = env.run.journal.execution_commit(image.commit_id)
        assert fact["status"] == "acked"
        assert SINK_ID in fact["receipts"]
        assert image.commit_id and image.cut.sequence == 1
    finally:
        coord.close()

    # V018: truly kill a child at native fault points, reconcile in a fresh process
    child = tmp_path / "crash_child.py"
    child.write_text(CHILD_SRC.format(worktree=str(WORKTREE), tests=str(WORKTREE / "tests")))
    python = sys.executable
    for fault in ("after_prepared", "after_sink_commit_before_receipt"):
        run_dir = tmp_path / ("crash-" + fault)
        run_dir.mkdir()
        env_vars = dict(os.environ, PYTHONPATH=str(WORKTREE))
        crashed = subprocess.run([python, "-B", str(child), "commit", str(run_dir), ACTION, fault],
                                 capture_output=True, text=True, env=env_vars, timeout=120)
        assert crashed.returncode == 137, (fault, crashed.returncode, crashed.stderr[-2000:])
        # journal proof exists; publication state is durable
        facts = RunJournal(run_dir).execution_commits(ACTION)
        assert len(facts) == 1 and facts[0]["status"] == "prepared"
        recovered = subprocess.run([python, "-B", str(child), "recover", str(run_dir), ACTION],
                                   capture_output=True, text=True, env=env_vars, timeout=120)
        assert recovered.returncode == 0, recovered.stderr[-2000:]
        result = json.loads(recovered.stdout.strip().splitlines()[-1])
        assert result["n_facts"] == 1, result          # no duplicate billing
        assert result["statuses"] == ["acked"], result  # reconciled, not skipped
        assert result["marker_exists"] is True, result
        assert result["rows"] == 3, result              # p0(2)+p1(1), prepared once


# --- D005 -------------------------------------------------------------------
def test_d005(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    # seal so recover takes the v3 path
    staged = _stage_graph(env.action, graph, env.store)
    seal.seal_dataflow(env.action, staged, physical, store=env.store)
    coord = ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store)
    try:
        # files without journal proof are not a recovery authority
        _code_error(lambda: coord.recover(sinks={SINK_ID: make_adapter(env.action, physical, env.store)}),
                    "INCOMPATIBLE_HISTORY")
        cut = coord.begin(final=True)
        ref0 = prepare_partition(env.action, physical, env.store, "p0", [{"x": 1}])
        ref1 = prepare_partition(env.action, physical, env.store, "p1", [{"x": 2}])
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p0"), sink_ref=ref0))
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p1"), sink_ref=ref1))
        image = coord.commit(cut.cut_id, sinks={SINK_ID: make_adapter(env.action, physical, env.store)})
        # recover returns the complete image, cross-checking the source vector
        again = coord.recover(sinks={SINK_ID: make_adapter(env.action, physical, env.store)})
        assert again.commit_id == image.commit_id
        assert again.cut.cut_id == cut.cut_id
        assert again.source_vector == image.source_vector
    finally:
        coord.close()
    # unknown sink (no native receipt) must not be ACKed
    bad = RecoveryImage(commit_id=image.commit_id, cut=image.cut, contributions=image.contributions,
                        confirmed_sinks={"ghost": image.confirmed_sinks[SINK_ID]},
                        source_vector=image.source_vector)
    _code_error(lambda: ack_source_claims(bad, action=env.action), "UNKNOWN_EFFECT")


# --- D006 -------------------------------------------------------------------
def test_d006(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    coord = ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store)
    try:
        cut = coord.begin(final=False)
        ref0 = prepare_partition(env.action, physical, env.store, "p0", [{"x": 1}])
        # a published output artifact retained under the commit owner
        published = env.store.put(iter([b"published-output"]), max_bytes=1024, media_type="application/x-test")
        env.store.retain(published, owner="commit-owner")
        # unpublished staging retained under this cut
        staging = env.store.put(iter([b"staging"]), max_bytes=1024, media_type="application/x-test")
        env.store.retain(staging, owner=cut.cut_id)
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p0"), sink_ref=ref0, buffers=(staging,)))
        coord.abort(cut.cut_id, reason="cancel")
        assert not env.store.inspect(staging)      # own staging released
        assert env.store.inspect(published)        # published output preserved
        # aborted cut cannot be committed; stale cut not owned is refused
        _code_error(lambda: coord.commit(cut.cut_id, sinks={SINK_ID: make_adapter(env.action, physical, env.store)}),
                    "INVALID_CONTRACT")
        _code_error(lambda: coord.abort("not-owned-cut", reason="x"), "INVALID_CONTRACT")
    finally:
        coord.close()


# --- D007 -------------------------------------------------------------------
def test_d007(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    cut = ckpt.CutRequest(cut_id=identity(["cut"]), sequence=3, plan_revision=1, route_epoch=0,
                          participants=(compute_participant_id("stage-0", "p0"),), final=False)
    cursors = (SourceCursor(split_id=identity("split-p0"), offset={"row": 500}, next_sequence=500,
                            end_of_input=False, lease_ref=None, pending_refs=()),)
    image = RecoveryImage(commit_id=identity("c"), cut=cut, contributions=(), confirmed_sinks={}, source_vector=cursors)
    position = ckpt.source_vector_position(image, action_id=ACTION)
    assert isinstance(position, SourcePosition)
    # ordinal is the cut sequence, NOT the business row offset (no skip-read)
    assert position.next_ordinal == 3
    assert position.source_id == identity(["dataflow-vector", ACTION])
    assert image.source_vector[0].offset == {"row": 500}
    # duplicate split in the vector is refused
    dup = RecoveryImage(commit_id=identity("c"), cut=cut, contributions=(), confirmed_sinks={},
                        source_vector=cursors + cursors)
    _code_error(lambda: ckpt.source_vector_position(dup, action_id=ACTION), "SOURCE_CHANGED")


# --- D008 -------------------------------------------------------------------
class _DurableSink(Datasink):
    def recovery_identity(self):
        return {"format": "test"}

    def prepare(self, blocks, context):
        return {"ok": True}

    def commit(self, prepared, context):
        return {"commit_id": context.commit_id, "sink_id": "x", "status": "committed", "data": {}}

    def inspect(self, prepared, context):
        return None

    def abort(self, prepared, context):
        return None


class _LegacyWriteSink(Datasink):
    def write(self, blocks, context):
        return None


def test_d008(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    services = driver_services(env.action, physical, env.store)
    # pure declaration assembles a durable four-method artifact sink
    adapter = sinks.make_sink_adapter(make_kernel(configuration={"sink_id": SINK_ID}), services=services)
    assert isinstance(adapter, Datasink)
    for method in ("prepare", "commit", "inspect", "abort"):
        assert getattr(type(adapter), method) is not getattr(Datasink, method)
    # a real durable custom sink is preserved as-is
    durable = _DurableSink()
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.data.plan import LogicalPlan
    from demiflow.data.sources import ItemsSource
    request = ActionRequest('write_datasink', ItemsSource(()), LogicalPlan(), {},
                            durable, (), 'local', env.action)
    spec = make_kernel(kind='write_datasink')
    assert sinks.make_native_terminal_sink(spec, request=request, services=services).native is durable
    # The same native entry rejects a legacy write-only sink before prepare.
    _code_error(lambda: sinks.make_native_terminal_sink(spec,
        request=replace(request, sink=_LegacyWriteSink()), services=services), 'INVALID_CONTRACT')
    # legacy write callback cannot impersonate a recoverable sink
    _code_error(lambda: sinks.make_sink_adapter(make_kernel(configuration={"sink": _LegacyWriteSink()}),
                                                services=services), "INVALID_CONTRACT")
    _code_error(lambda: sinks.make_sink_adapter(make_kernel(configuration={"sink": object()}),
                                                services=services), "INVALID_CONTRACT")
    # live handle in pure config is refused
    _code_error(lambda: sinks.make_sink_adapter(make_kernel(configuration={"sink_id": SINK_ID, "handle": object()}),
                                                services=services), "INVALID_CONTRACT")


# --- D009 -------------------------------------------------------------------
def test_d009(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    bounds = physical.bounds
    ref = prepare_partition(env.action, physical, env.store, "p0", [{"x": 1}, {"x": 2}])
    manifest = json.loads(b"".join(env.store.read(ref, max_bytes=bounds.max_control_bytes)))
    assert manifest["rows"] == 2 and manifest["partition_id"] == "p0"
    assert manifest["columns"] == ["x"]
    assert manifest["bytes"] > 0
    # staged data is a reconcilable owned artifact
    data_ref = sinks._artifact_from_json(manifest["data_ref"])
    assert env.store.inspect(data_ref)
    # row count drift is refused
    bad = make_record(0, {"x": 1}, bounds)
    bad = replace(bad, encoded_bytes=bad.encoded_bytes + 1)
    spec = make_kernel(configuration={"sink_id": SINK_ID, "partition_id": "p0"})
    services = runtime_services(env.action, physical, env.store)
    _code_error(lambda: sinks.prepare_sink_partition(iter([bad]), spec=spec, services=services),
                "INVALID_CONTRACT")
    # output row budget enforced
    many = [make_record(i, {"x": i}, bounds) for i in range(bounds.max_output_rows + 1)]
    _code_error(lambda: sinks.prepare_sink_partition(iter(many), spec=spec, services=services),
                "BUDGET_EXCEEDED")


# --- D010 -------------------------------------------------------------------
def test_d010(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    services = driver_services(env.action, physical, env.store)
    ref0 = prepare_partition(env.action, physical, env.store, "p0", [{"x": 1}])
    ref1 = prepare_partition(env.action, physical, env.store, "p1", [{"x": 2}])
    combined = sinks.combine_prepared_sinks((ref0, ref1), sink_id=SINK_ID, store=services.store, bounds=services.bounds)
    assert combined["total_rows"] == 2
    assert [p["partition_id"] for p in combined["partitions"]] == ["p0", "p1"]
    # two workers submitting one partition is refused
    dup = prepare_partition(env.action, physical, env.store, "p0", [{"x": 3}])
    _code_error(lambda: sinks.combine_prepared_sinks((ref0, dup), sink_id=SINK_ID, store=services.store, bounds=services.bounds),
                "INVALID_CONTRACT")
    # schema drift refused
    drift = prepare_partition(env.action, physical, env.store, "p1", [{"y": 2}])
    _code_error(lambda: sinks.combine_prepared_sinks((ref0, drift), sink_id=SINK_ID, store=services.store, bounds=services.bounds),
                "SOURCE_CHANGED")
    # empty partition still records completion
    empty = prepare_partition(env.action, physical, env.store, "p1", [])
    combined2 = sinks.combine_prepared_sinks((ref0, empty), sink_id=SINK_ID, store=services.store, bounds=services.bounds)
    assert combined2["total_rows"] == 1
    assert any(p["partition_id"] == "p1" and p["rows"] == 0 for p in combined2["partitions"])


# --- D011 -------------------------------------------------------------------
def test_d011(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    coord = ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store)
    try:
        cut = coord.begin(final=True)
        ref0 = prepare_partition(env.action, physical, env.store, "p0", [{"x": 1}])
        ref1 = prepare_partition(env.action, physical, env.store, "p1", [{"x": 2}])
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p0"), sink_ref=ref0))
        coord.contribute(contribution(cut, compute_participant_id("stage-0", "p1"), sink_ref=ref1))
        image = coord.commit(cut.cut_id, sinks={SINK_ID: make_adapter(env.action, physical, env.store)})
        # response-lost retry is idempotent (verify-only, no second ACK)
        ack_source_claims(image, action=env.action)
        ack_source_claims(image, action=env.action)
        fact = env.run.journal.execution_commit(image.commit_id)
        assert fact["status"] == "acked"
        # an image whose commit is not acked cannot advance the frontier
        pending = RecoveryImage(commit_id=identity(["no-commit"]), cut=cut, contributions=(),
                                confirmed_sinks={}, source_vector=image.source_vector)
        _code_error(lambda: ack_source_claims(pending, action=env.action), "INVALID_CONTRACT")
    finally:
        coord.close()


# --- program staging helpers (D012/D015/D016) -------------------------------
def _double(row):
    return {"x": row["x"] * 2}


def _stage_graph(action, graph, store):
    declarations = tuple(ProgramDeclaration(occurrence_id=n.occurrence_id,
                                            callable_spec=CallableSpec.create(_double),
                                            source_spec=None, action_program=None, code_identity={})
                         for n in graph.nodes)
    compiled = CompiledDataflow(graph=graph, declarations=declarations)
    return seal.stage_dataflow_programs(compiled, action=action, store=store, bounds=make_bounds())


def _program_manifest(store, ref, bounds):
    return json.loads(b"".join(store.read(ref, max_bytes=bounds.max_control_bytes)))


# --- D012 -------------------------------------------------------------------
def test_d012(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    staged = _stage_graph(env.action, graph, env.store)
    sealed = seal.seal_dataflow(env.action, staged, physical, store=env.store)
    assert isinstance(sealed, seal.SealedExecution)
    binding = env.run.journal.source_binding(ACTION + "-dataflow-sealed")
    assert binding is not None and binding["schema"] == seal.SEAL_SCHEMA
    assert binding["graph_digest"] == compute_graph_digest(staged)
    # same action, different plan is refused (write-once fact)
    other_physical = make_physical(ACTION, graph, env.store, plan_revision=2)
    _code_error(lambda: seal.seal_dataflow(env.action, staged, other_physical, store=env.store),
                "INVALID_CONTRACT")
    # a failed seal (missing program artifact) never writes the plan fact
    env.run.journal.bind_execution_action(env.run.fence, "action-fail", {"kind": "dataflow"})
    action2 = RunAction(env.run, "action-fail", {"kind": "dataflow"})
    graph2 = make_graph("action-fail")
    physical2 = make_physical("action-fail", graph2, env.store)
    staged2 = _stage_graph(action2, graph2, env.store)
    missing_ref = ArtifactRef(digest="0" * 64, size_bytes=1, store_id="test", key="0" * 64,
                              media_type=seal.PROGRAM_MEDIA)
    bad_graph = replace(staged2, nodes=(replace(staged2.nodes[0], program_ref=missing_ref),)
                        + staged2.nodes[1:])
    _code_error(lambda: seal.seal_dataflow(action2, bad_graph, physical2, store=env.store),
                "CORRUPT_ARTIFACT")
    assert env.run.journal.source_binding("action-fail-dataflow-sealed") is None


# --- D013 -------------------------------------------------------------------
def test_d013(env):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    staged = _stage_graph(env.action, graph, env.store)
    sealed = seal.seal_dataflow(env.action, staged, physical, store=env.store)
    bounds = make_bounds()
    loaded_graph, loaded_physical, loaded_sealed = seal.load_dataflow(
        env.run_dir, ACTION, store=env.store, bounds=bounds)
    assert loaded_graph.dag_id == staged.dag_id
    assert loaded_physical.plan_id == physical.plan_id
    assert loaded_sealed == sealed
    # wrong action refused
    _code_error(lambda: seal.load_dataflow(env.run_dir, "action-other", store=env.store, bounds=bounds),
                "INCOMPATIBLE_HISTORY")
    # bad digest (tampered graph bytes) refused
    env.store.tamper(sealed.logical_ref, b'{"tampered": true}')
    _code_error(lambda: seal.load_dataflow(env.run_dir, ACTION, store=env.store, bounds=bounds),
                "CORRUPT_ARTIFACT")
    # partial files (a program artifact removed) refused — fresh action, clean seal
    graph3 = make_graph("action-partial")
    physical3 = make_physical("action-partial", graph3, env.store)
    staged3 = _stage_graph(env.action, graph3, env.store)
    env.run.journal.bind_execution_action(env.run.fence, "action-partial", {"kind": "dataflow"})
    action3 = RunAction(env.run, "action-partial", {"kind": "dataflow"})
    sealed3 = seal.seal_dataflow(action3, staged3, physical3, store=env.store)
    env.store._path(sealed3.programs[0].key).unlink()
    _code_error(lambda: seal.load_dataflow(env.run_dir, "action-partial", store=env.store, bounds=bounds),
                "CORRUPT_ARTIFACT")
    # future version refused (fresh action; the v3 seal binding is write-once)
    fact = env.run.journal.source_binding(ACTION + "-dataflow-sealed")
    env.run.journal.bind_execution_action(env.run.fence, "action-future", {"kind": "dataflow"})
    env.run.journal.bind_source(env.run.fence, "action-future-dataflow-sealed",
                                dict(fact, schema="demiflow.dataflow_seal.v99", action_id="action-future"))
    _code_error(lambda: seal.load_dataflow(env.run_dir, "action-future", store=env.store, bounds=bounds),
                "INCOMPATIBLE_HISTORY")


# --- D014 -------------------------------------------------------------------
def test_d014(env):
    # same function used twice: distinct occurrences, both must appear in the bridge
    graph = make_graph(kinds=("transform", "transform"), same_semantic=True)
    bindings = {n.occurrence_id: {"occurrence_id": n.occurrence_id, "mode": "execute"}
                for n in graph.nodes}
    bridge = seal.build_dataflow_identity_bridge(graph, report_digest=compute_graph_digest(graph),
                                                 bindings=bindings)
    assert bridge["schema"] == "demiflow.identity_bridge.v1"
    assert len(bridge["entries"]) == 2
    assert {e["occurrence_id"] for e in bridge["entries"]} == {n.occurrence_id for n in graph.nodes}
    # missing occurrence (no guessing) refused
    partial = {graph.nodes[0].occurrence_id: bindings[graph.nodes[0].occurrence_id]}
    _code_error(lambda: seal.build_dataflow_identity_bridge(graph, report_digest="d", bindings=partial),
                "INVALID_CONTRACT")
    # unknown occurrence refused
    bad = dict(bindings, **{"ghost": {"occurrence_id": "ghost", "mode": "execute"}})
    _code_error(lambda: seal.build_dataflow_identity_bridge(graph, report_digest="d", bindings=bad),
                "INVALID_CONTRACT")


# --- D015 -------------------------------------------------------------------
def test_d015(env):
    graph = make_graph()
    bounds = make_bounds()
    staged = _stage_graph(env.action, graph, env.store)
    for node in staged.nodes:
        assert node.program_ref is not None and env.store.inspect(node.program_ref)
    # semantic identity is unchanged by staging (store path never enters identity)
    assert [n.semantic_id for n in staged.nodes] == [n.semantic_id for n in graph.nodes]
    assert staged.dag_id == graph.dag_id
    # a declared program_ref that does not exist cannot be fabricated
    fake_ref = ArtifactRef(digest="1" * 64, size_bytes=1, store_id="test", key="1" * 64,
                           media_type=seal.PROGRAM_MEDIA)
    bad_graph = replace(graph, nodes=(replace(graph.nodes[0], program_ref=fake_ref),) + graph.nodes[1:])
    compiled = CompiledDataflow(graph=bad_graph, declarations=())
    _code_error(lambda: seal.stage_dataflow_programs(compiled, action=env.action, store=env.store, bounds=bounds),
                "CORRUPT_ARTIFACT")
    # an executable node with no declaration and no implementation_ref is refused
    compiled2 = CompiledDataflow(graph=graph, declarations=())
    _code_error(lambda: seal.stage_dataflow_programs(compiled2, action=env.action, store=env.store, bounds=bounds),
                "INVALID_CONTRACT")


@pytest.mark.parametrize("shared", [False, True])
def test_program_size_is_independent_of_checkpoint_state(env, shared):
    bounds = replace(make_bounds(), max_state_bytes=9661)
    graph = make_graph(kinds=("transform", "transform"))
    members = [node.occurrence_id for node in graph.nodes]
    if shared:
        binding = {"owner": members[0], "members": members, "state_owner": members[-1]}
        graph = replace(graph, nodes=tuple(replace(node, kernel=replace(node.kernel,
            configuration={"shared_actor": binding})) for node in graph.nodes))

    lookup = b"x" * (128 * 1024)
    def transform(row):
        return {"value": lookup[row["index"]]}

    spec = CallableSpec.create(transform)
    compiled = CompiledDataflow(graph=graph, declarations=tuple(
        ProgramDeclaration(occurrence_id=member, callable_spec=spec, source_spec=None,
                           action_program=None, code_identity={}) for member in members))
    staged = seal.stage_dataflow_programs(compiled, action=env.action, store=env.store, bounds=bounds)
    cache = {}
    for node in staged.nodes:
        manifest = _program_manifest(env.store, node.program_ref, bounds)
        assert manifest["payload_ref"]["size_bytes"] > bounds.max_state_bytes
        program = seal.verify_dataflow_program(node.program_ref, expected_pin=manifest["code_identity"],
                                               store=env.store, bounds=bounds, shared_cache=cache)
        assert program.value.instantiate()({"index": 100_000}) == {"value": ord("x")}
    assert len(cache) == int(shared)


@pytest.mark.parametrize("opaque", [False, True], ids=["function-slots", "sealed-private-class"])
def test_private_task_loads_large_program_with_small_checkpoint(env, opaque):
    from test_runtime_v3_k import services, spec, InputPort, rec, rows_of
    from demiflow.execution.kernels.operator import OperatorKernel

    bounds = replace(make_bounds(), max_state_bytes=9661)
    graph = make_graph(kinds=("transform",))
    original = graph.nodes[0]
    key = original.occurrence_id
    policy = spec("MapOp", {"program": key, "task_batch_rows": 2},
                  state="adapter", inputs=[InputPort("input", "incremental")])
    graph = replace(graph, nodes=(replace(original, kernel=policy),))
    lookup = "x" * (128 * 1024)
    count = [0]

    def lookup_row(row):
        count[0] += 1
        return {"count": count[0], "size": len(lookup)}

    class Lookup:
        def __init__(self, lookup):
            self.size = len(lookup)
            self.count = 0

        def __call__(self, row):
            self.count += 1
            return {"count": self.count, "size": self.size}

    declaration = CallableSpec.create(Lookup, fn_constructor_args=[lookup]) if opaque else CallableSpec.create(lookup_row)
    compiled = CompiledDataflow(graph=graph, declarations=(ProgramDeclaration(
        occurrence_id=key, callable_spec=declaration, source_spec=None,
        action_program=None, code_identity={}),))
    node, = seal.stage_dataflow_programs(compiled, action=env.action, store=env.store, bounds=bounds).nodes
    manifest = _program_manifest(env.store, node.program_ref, bounds)
    payload = seal._artifact_from_json(manifest["payload_ref"])
    assert payload.size_bytes > bounds.max_state_bytes
    assert node.kernel.configuration.get("task_load_bytes") == (payload.size_bytes if opaque else None)
    program = seal.verify_dataflow_program(node.program_ref, expected_pin=manifest["code_identity"],
                                           store=env.store, bounds=bounds)
    kernel = OperatorKernel(node.kernel, services=services(programs={key: program}, bounds=bounds),
                            partition_id="p0")
    observed = []
    try:
        kernel.start()
        if opaque:
            assert kernel._instance is None
        for i in range(3):
            observed.extend(rows_of(kernel.push("input", rec({"i": i}, sequence=i))))
            assert kernel.snapshot().size_bytes <= bounds.max_state_bytes
    finally:
        kernel.close()
    assert observed == [{"count": n, "size": len(lookup)} for n in (1, 2, 1)]
    # Even a same-length modification is rejected on the next opaque task load.
    if opaque:
        tighter = replace(bounds, max_resident_bytes=payload.size_bytes - 1)
        denied = seal.verify_dataflow_program(node.program_ref, expected_pin=manifest["code_identity"],
                                              store=env.store, bounds=tighter)
        _code_error(denied.load_private, "RESOURCE_INFEASIBLE")
        raw = env.store._path(payload.key).read_bytes()
        env.store.tamper(payload, raw[:-1] + bytes([raw[-1] ^ 1]))
        with pytest.raises(DataflowError, match=payload.digest) as corrupted:
            program.load_private()
        assert corrupted.value.code == 'CORRUPT_ARTIFACT'
        assert corrupted.value.context['occurrence_id'] == key
        assert corrupted.value.__cause__ is not None


def test_private_payload_reuses_sealed_buffer_at_tighter_live_windows(tmp_path):
    from demiflow.execution.storage.objects import RunArtifactStore
    from types import SimpleNamespace
    import hashlib

    # Verify the reader that both local and Ray workers use. Shrinking the
    # window after sealing must only change each individual readinto extent.
    data = b'x' * ((1 << 20) + 7)
    for size in (64 << 10, (1 << 20) + 7):
        raw = data[:size]
        digest = hashlib.sha256(raw).hexdigest()
        (tmp_path / digest).write_bytes(raw)
        ref = ArtifactRef(digest, size, "test", digest, seal.PAYLOAD_MEDIA)
        for window in (2 << 20, 64 << 10, 32 << 10):
            bounds = replace(make_bounds(), max_row_bytes=window)
            store = SimpleNamespace(_bounds=bounds, _object_path=lambda ref: tmp_path / ref.digest)
            destination = bytearray(size)
            total = 0
            for chunk in RunArtifactStore._read(store, ref, max_bytes=size, destination=destination):
                assert chunk.obj is destination and len(chunk) <= window
                total += len(chunk)
            assert total == size and destination == raw


# --- D016 -------------------------------------------------------------------
def test_d016(env):
    graph = make_graph(kinds=("transform",))
    bounds = make_bounds()
    staged = _stage_graph(env.action, graph, env.store)
    ref = staged.nodes[0].program_ref
    manifest = _program_manifest(env.store, ref, bounds)
    pin = manifest["code_identity"]
    loaded = seal.verify_dataflow_program(ref, expected_pin=pin, store=env.store, bounds=bounds)
    assert isinstance(loaded, LoadedProgram) and loaded.kind == "callable"
    assert loaded.value.instantiate()({"x": 2}) == {"x": 4}
    # a different expected pin is refused
    _code_error(lambda: seal.verify_dataflow_program(ref, expected_pin={"operations": []},
                                                     store=env.store, bounds=bounds), "INVALID_CONTRACT")
    # payload bytes changed but ref digest unchanged -> digest gate refuses before load
    payload_ref = sinks._artifact_from_json(manifest["payload_ref"])
    env.store.tamper(payload_ref, b"tampered-pickle-bytes")
    _code_error(lambda: seal.verify_dataflow_program(ref, expected_pin=pin, store=env.store, bounds=bounds),
                "CORRUPT_ARTIFACT")
    # wrong code impersonating the same pin (models importing a different module):
    # rebuild a manifest whose payload is a DIFFERENT function but keep the old pin
    def _other(row):
        return {"x": row["x"] + 100}
    import cloudpickle
    other_payload = cloudpickle.dumps(CallableSpec.create(_other))
    other_ref = env.store.put(iter([other_payload]), max_bytes=bounds.max_state_bytes,
                              media_type=seal.PAYLOAD_MEDIA)
    forged = dict(manifest, payload_ref=sinks._artifact_json(other_ref))
    forged_raw = document(forged, bounds.max_control_bytes)
    forged_ref = env.store.put(iter([forged_raw]), max_bytes=bounds.max_control_bytes,
                               media_type=seal.PROGRAM_MEDIA)
    _code_error(lambda: seal.verify_dataflow_program(forged_ref, expected_pin=pin, store=env.store, bounds=bounds),
                "INVALID_CONTRACT")


# --- D017 -------------------------------------------------------------------
def test_d017(env):
    bounds = make_bounds()
    config = {"sink_id": SINK_ID, "seed": None, "shuffle_seed": None, "sample_seed": 42}
    node = GraphNode(occurrence_id=identity(["occ-const"]), semantic_id=identity(["sem-const"]),
                     kind="transform", kernel=make_kernel(configuration=config), program_ref=None,
                     shared_token="0")
    graph = DataflowGraph(schema="demiflow.dataflow_graph.v3", action_id=ACTION, dag_id=identity("d"),
                          nodes=(node,), edges=(), terminals=(), sources=(node.occurrence_id,))
    ref1 = seal.resolve_execution_constants(graph, action=env.action, store=env.store)
    constants1 = json.loads(b"".join(env.store.read(ref1, max_bytes=bounds.max_state_bytes)))["constants"]
    generated = constants1[node.occurrence_id]
    assert isinstance(generated["seed"], int) and isinstance(generated["shuffle_seed"], int)
    assert "sample_seed" not in generated  # explicit seed preserved, not overwritten
    # a second call (e.g. after crash recovery) reads the ORIGINAL constants
    ref2 = seal.resolve_execution_constants(graph, action=env.action, store=env.store)
    assert ref2 == ref1
    constants2 = json.loads(b"".join(env.store.read(ref2, max_bytes=bounds.max_state_bytes)))["constants"]
    assert constants2 == constants1
    # explicit-only config generates nothing
    explicit = replace(node, kernel=make_kernel(configuration={"sink_id": SINK_ID, "seed": 7}))
    graph2 = replace(graph, action_id="action-explicit", nodes=(explicit,))
    env.run.journal.bind_execution_action(env.run.fence, "action-explicit", {"kind": "dataflow"})
    action2 = RunAction(env.run, "action-explicit", {"kind": "dataflow"})
    ref3 = seal.resolve_execution_constants(graph2, action=action2, store=env.store)
    constants3 = json.loads(b"".join(env.store.read(ref3, max_bytes=bounds.max_state_bytes)))["constants"]
    assert constants3 == {}
from demiflow.resources.contracts import ResourceProfile

from demiflow.execution.contracts.dataflow_codec import compute_participant_id


def test_only_owner_can_replace_or_abort_parked_cut(env):
    physical = make_physical(ACTION, make_graph(), env.store, with_sources=True)
    coordinator = ckpt.DataflowCheckpointCoordinator(env.action, physical, store=env.store)
    try:
        coordinator.observe_input_satisfied([split.split_id for split in physical.splits])
        assert coordinator.inputs_complete() and not coordinator.sources_exhausted()
        assert coordinator.satisfied_splits() == tuple(sorted(split.split_id for split in physical.splits))
        cut = coordinator.begin(final=False, residency={'deliveries': {}})
        same_fields = replace(cut)
        assert same_fields == cut and same_fields is not cut
        coordinator.abort_pending(same_fields, reason='stale owner')
        _code_error(lambda: coordinator.update_residency(same_fields, {}), 'FENCED')
        snapshot = {'deliveries': {'record': {'consumed': True}}}
        coordinator.update_residency(cut, snapshot)
        snapshot['deliveries'].clear()
        assert coordinator._pending[cut.cut_id].residency['deliveries'] == {'record': {'consumed': True}}
        coordinator.abort_pending(cut, reason='cancel')
        coordinator.abort_pending(cut, reason='already cancelled')
        _code_error(lambda: coordinator.update_residency(cut, {}), 'FENCED')
        _code_error(lambda: coordinator.abort(cut.cut_id, reason='not pending'), 'INVALID_CONTRACT')
    finally:
        coordinator.close()
