"""Program sealing, cut-free recovery loading and run constants for runtime v3 (D).

Sealing forms the in-memory declaration first (``CompiledDataflow``), then stages
real program artifacts and only then writes the single immutable journal seal fact.
A program ref is content-addressed from code/config semantics, so it never changes
the fixed logical identity. Recovery reads the original constants/source/code by
journal pointer and fully verifies every artifact; it never re-runs seed=None or
head-version resolution, and never accepts a file merely because it exists.

The historical code-pin (``chain.plan_code_pin``) and identity-bridge
(``identity_bridge.build_identity_bridge``) algorithms are reused unchanged.
"""
from __future__ import annotations

import hashlib
import secrets
from dataclasses import replace
from pathlib import Path
from collections.abc import Mapping

from ..data.datasink import Datasink
from ..data.plan import CallableSpec, LogicalPlan, MapOp
from .chain import _stable_code_identity, plan_code_pin
from .contracts.dataflow import (
    ArtifactRef, Bounds, CompiledDataflow, DataflowGraph, LoadedProgram,
    PhysicalDataflow, ProgramDeclaration, SealedExecution,
)
from .contracts.dataflow_codec import (
    compute_graph_digest, compute_physical_digest, decode_wire, encode_wire,
)
from .contracts.dataflow_errors import DataflowError
from .identity_bridge import build_identity_bridge
from .recovery_contract import RecoveryContractError, document, identifier, identity
from .recovery_identity import describe
from .run_context import RunAction
from .run_journal import RunJournal

SEAL_SCHEMA = "demiflow.dataflow_seal.v3"
PROGRAM_SCHEMA = "demiflow.dataflow_program.v3"
CODE_PIN_SCHEMA = "demiflow.dataflow_code_pin.v3"
CONSTANTS_SCHEMA = "demiflow.dataflow_constants.v3"
GRAPH_MEDIA = "application/x-demiflow-graph-v3"
PHYSICAL_MEDIA = "application/x-demiflow-physical-v3"
PROGRAM_MEDIA = "application/x-demiflow-program-v3"
PAYLOAD_MEDIA = "application/x-demiflow-program-payload-v3"
CODE_PIN_MEDIA = "application/x-demiflow-code-pin-v3"
BRIDGE_MEDIA = "application/x-demiflow-bridge-v3"
CONSTANTS_MEDIA = "application/x-demiflow-constants-v3"
_SEAL_KEY = "-dataflow-sealed"
_CONSTANTS_KEY = "-dataflow-constants"
_SEED_KEYS = ("seed", "shuffle_seed", "sample_seed", "block_order_seed")


def _builtin_node(node):
    """Only registered platform implementations may omit a user program."""
    from ..planning.dataflow_registry import operator_registry, source_registry, terminal_registry
    table = terminal_registry() if node.kind == "terminal" else (
        source_registry() if node.kind == "relational" else operator_registry())
    return node.kind != "source" and node.kernel.kind in table


def _artifact_json(ref: ArtifactRef) -> dict:
    return {"digest": ref.digest, "size_bytes": ref.size_bytes,
            "store_id": ref.store_id, "key": ref.key, "media_type": ref.media_type}


def _artifact_from_json(value) -> ArtifactRef:
    if type(value) is not dict or set(value) != {
            "digest", "size_bytes", "store_id", "key", "media_type"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid artifact reference in sealed execution")
    return ArtifactRef(digest=value["digest"], size_bytes=value["size_bytes"],
                       store_id=value["store_id"], key=value["key"],
                       media_type=value["media_type"])


def _read_bytes(store, ref: ArtifactRef, *, max_bytes: int, media_type: str) -> bytes:
    if ref.media_type != media_type:
        raise DataflowError("INVALID_CONTRACT", "Sealed artifact media type differs")
    if ref.size_bytes > max_bytes:
        raise DataflowError("BUDGET_EXCEEDED", "Sealed artifact exceeds read budget")
    raw = b"".join(store.read(ref, max_bytes=max_bytes))
    if len(raw) != ref.size_bytes:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed artifact length differs")
    # Content-address gate before any bytes are consumed (notably before a program
    # payload is deserialized): only bytes matching the sealed sha256 may load.
    if hashlib.sha256(raw).hexdigest() != ref.digest:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed artifact digest differs")
    return raw


def _read_json(store, ref: ArtifactRef, *, max_bytes: int, media_type: str):
    import json
    raw = _read_bytes(store, ref, max_bytes=max_bytes, media_type=media_type)
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed artifact is not valid JSON") from exc


# -- program code identity (single authority shared by D015 seal / D016 verify) --
def _program_kind(declaration: ProgramDeclaration) -> str:
    if declaration.callable_spec is not None:
        return "callable"
    if declaration.source_spec is not None:
        return "source"
    if declaration.action_program is not None:
        return declaration.action_program.kind
    raise DataflowError("INVALID_CONTRACT", "ProgramDeclaration has no program body")


def _program_value(declaration: ProgramDeclaration):
    if declaration.callable_spec is not None:
        return declaration.callable_spec
    if declaration.source_spec is not None:
        return declaration.source_spec
    if declaration.action_program is not None:
        return declaration.action_program.value
    raise DataflowError("INVALID_CONTRACT", "ProgramDeclaration has no program body")


def _code_identity_from_value(kind: str, value):
    """Recompute the historical code-pin from an actually-loaded program value."""
    try:
        if kind == "callable":
            return plan_code_pin(LogicalPlan((MapOp(callable=value),)))
        if kind == "source":
            return plan_code_pin(LogicalPlan(()), source=value)
        if kind == "aggregate":
            from ..data.aggregate import AggregateFnV2
            aggregates = value if isinstance(value, tuple) else (value,)
            if not aggregates or any(not isinstance(a, AggregateFnV2) for a in aggregates):
                raise DataflowError("INVALID_CONTRACT", "aggregate program must declare AggregateFnV2")
            return {"aggregates": [
                {"configuration": describe(vars(a)), "methods": [plan_code_pin(LogicalPlan((MapOp(
                    callable=CallableSpec.create(getattr(type(a), method))),)))
                    for method in ("aggregate_partial", "aggregate_block", "merge_states", "combine", "finalize_state", "finalize")]}
                for a in aggregates]}
        if kind == "sink":
            from ..lance.model import LanceWriteSpec
            if isinstance(value, LanceWriteSpec):
                return {"sink": "demiflow.lance.spec", "declaration": value.to_dict()}
            if not isinstance(value, Datasink):
                raise DataflowError("INVALID_CONTRACT", "Terminal sink program is not a Datasink")
            try:
                declaration = value.recovery_identity()
            except NotImplementedError as exc:
                raise DataflowError("INVALID_CONTRACT",
                                    "Sink program lacks a durable recovery identity") from exc
            return {"sink": _stable_code_identity(describe(type(value))),
                    "declaration": declaration}
    except RecoveryContractError as exc:
        raise DataflowError("INVALID_CONTRACT", "Program code-pin refused: " + str(exc)) from exc
    raise DataflowError("INVALID_CONTRACT", "Unknown program kind: " + str(kind))


def _stage_one_program(declaration: ProgramDeclaration, *, store, bounds: Bounds) -> ArtifactRef:
    import cloudpickle
    kind = _program_kind(declaration)
    value = _program_value(declaration)
    code_identity = _code_identity_from_value(kind, value)
    try:
        payload = cloudpickle.dumps(value)
    except Exception as exc:
        raise DataflowError("INVALID_CONTRACT",
                            "Program carries a live handle (connection/lock/FD); declare a durable migration: "
                            + str(exc)) from exc
    if len(payload) > bounds.max_state_bytes:
        raise DataflowError("BUDGET_EXCEEDED", "Program payload exceeds byte budget")
    payload_ref = store.put(iter([payload]), max_bytes=bounds.max_state_bytes, media_type=PAYLOAD_MEDIA)
    manifest = {"schema": PROGRAM_SCHEMA, "occurrence_id": declaration.occurrence_id,
                "kind": kind, "code_identity": code_identity,
                "payload_ref": _artifact_json(payload_ref)}
    raw = document(manifest, bounds.max_control_bytes)
    return store.put(iter([raw]), max_bytes=bounds.max_control_bytes, media_type=PROGRAM_MEDIA)


# -- D015 -----------------------------------------------------------------
def stage_dataflow_programs(compiled: CompiledDataflow, *, action: RunAction,
                            store, bounds: Bounds) -> DataflowGraph:
    if not isinstance(compiled, CompiledDataflow):
        raise DataflowError("INVALID_CONTRACT", "stage_dataflow_programs requires a CompiledDataflow")
    if not isinstance(action, RunAction):
        raise DataflowError("INVALID_CONTRACT", "Program staging requires a driver RunAction")
    graph = compiled.graph
    declarations = {}
    for declaration in compiled.declarations:
        if declaration.occurrence_id in declarations:
            raise DataflowError("INVALID_CONTRACT", "Duplicate program declaration occurrence")
        declarations[declaration.occurrence_id] = declaration
    nodes = []
    for node in graph.nodes:
        if node.program_ref is not None:
            if not store.inspect(node.program_ref):
                raise DataflowError("CORRUPT_ARTIFACT", "Declared program artifact is absent")
            nodes.append(node)
            continue
        declaration = declarations.get(node.occurrence_id)
        if declaration is None:
            if _builtin_node(node):
                nodes.append(node)
                continue
            raise DataflowError("INVALID_CONTRACT",
                                "Executable node has no program declaration: " + node.occurrence_id)
        ref = _stage_one_program(declaration, store=store, bounds=bounds)
        nodes.append(replace(node, program_ref=ref))
    staged = replace(graph, nodes=tuple(nodes))
    for node in staged.nodes:
        if node.program_ref is not None and not store.inspect(node.program_ref):
            raise DataflowError("CORRUPT_ARTIFACT", "Staged program artifact is absent")
    return staged


# -- D016 -----------------------------------------------------------------
def verify_dataflow_program(ref: ArtifactRef, *, expected_pin, store, bounds: Bounds) -> LoadedProgram:
    manifest = _read_json(store, ref, max_bytes=bounds.max_control_bytes, media_type=PROGRAM_MEDIA)
    if type(manifest) is not dict or manifest.get("schema") != PROGRAM_SCHEMA:
        raise DataflowError("INVALID_CONTRACT", "Unsupported program artifact schema")
    kind = manifest["kind"]
    code_identity = manifest["code_identity"]
    if identity(code_identity) != identity(expected_pin):
        raise DataflowError("INVALID_CONTRACT", "Program code pin differs from the sealed expectation")
    payload_ref = _artifact_from_json(manifest["payload_ref"])
    payload = _read_bytes(store, payload_ref, max_bytes=bounds.max_state_bytes, media_type=PAYLOAD_MEDIA)
    import cloudpickle
    try:
        value = cloudpickle.loads(payload)
    except Exception as exc:
        raise DataflowError("CORRUPT_ARTIFACT", "Program payload failed to load: " + str(exc)) from exc
    recomputed = _code_identity_from_value(kind, value)
    if identity(recomputed) != identity(code_identity):
        raise DataflowError("INVALID_CONTRACT",
                            "Loaded program code differs from the sealed pin (edited code or wrong module)")
    return LoadedProgram(kind=kind, value=value, digest=identity(code_identity))


# -- D014 -----------------------------------------------------------------
def build_dataflow_identity_bridge(graph: DataflowGraph, *, report_digest: str, bindings):
    if not isinstance(graph, DataflowGraph):
        raise DataflowError("INVALID_CONTRACT", "Identity bridge requires a DataflowGraph")
    if type(report_digest) is not str or not report_digest:
        raise DataflowError("INVALID_CONTRACT", "Identity bridge requires a report digest")
    if type(bindings) is not dict:
        raise DataflowError("INVALID_CONTRACT", "Identity bridge bindings must be a mapping")
    occ_by_id = {n.occurrence_id: n for n in graph.nodes}
    logical_dag = {"dag_id": graph.dag_id,
                   "nodes": [{"occurrence_id": n.occurrence_id, "logical_node_id": n.semantic_id}
                             for n in graph.nodes]}
    items, covered = [], set()
    for report_node_id, binding in bindings.items():
        if type(binding) is not dict:
            raise DataflowError("INVALID_CONTRACT", "Identity bridge binding must be a mapping")
        occ = binding.get("occurrence_id")
        if occ not in occ_by_id:
            raise DataflowError("INVALID_CONTRACT",
                                "Identity bridge occurrence is not in the graph; no guessing")
        items.append({"report_node_id": report_node_id,
                      "report_occurrence": binding.get("report_occurrence", report_node_id),
                      "logical_node_id": occ_by_id[occ].semantic_id,
                      "occurrence_id": occ, "mode": binding.get("mode", "execute")})
        covered.add(occ)
    missing = set(occ_by_id) - covered
    if missing:
        raise DataflowError("INVALID_CONTRACT",
                            "Identity bridge must cover every occurrence; unbound: " + ",".join(sorted(missing)))
    try:
        return build_identity_bridge(logical_dag, action_id=graph.action_id,
                                     report_digest=report_digest, bindings=items)
    except RecoveryContractError as exc:
        raise DataflowError("INVALID_CONTRACT", str(exc)) from exc


# -- D017 -----------------------------------------------------------------
def _generate_constants(graph: DataflowGraph) -> dict:
    constants = {}
    for node in graph.nodes:
        config = node.kernel.configuration
        if not isinstance(config, Mapping):
            continue
        generated = {key: secrets.randbelow(2**31) for key in _SEED_KEYS
                     if key in config and config[key] is None}
        if generated:
            constants[node.occurrence_id] = generated
    return constants


def resolve_execution_constants(graph: DataflowGraph, *, action: RunAction, store) -> ArtifactRef:
    if not isinstance(graph, DataflowGraph):
        raise DataflowError("INVALID_CONTRACT", "resolve_execution_constants requires a DataflowGraph")
    if not isinstance(action, RunAction):
        raise DataflowError("INVALID_CONTRACT", "Run constants require a driver RunAction")
    journal = action.context.journal
    key = action.action_id + _CONSTANTS_KEY
    existing = journal.source_binding(key)
    if existing is not None:
        # Recovery reads the original constants; never re-generate seed=None.
        return _artifact_from_json(existing)
    constants = _generate_constants(graph)
    raw = document({"schema": CONSTANTS_SCHEMA, "action_id": action.action_id,
                    "dag_id": graph.dag_id, "constants": constants},
                   action.context.limits["max_state_bytes"])
    ref = store.put(iter([raw]), max_bytes=action.context.limits["max_state_bytes"],
                    media_type=CONSTANTS_MEDIA)
    journal.bind_source(action.context.fence, key, _artifact_json(ref))
    return ref


# -- D012 -----------------------------------------------------------------
def seal_dataflow(action: RunAction, graph: DataflowGraph, physical: PhysicalDataflow,
                  *, store, identity_bridge=None) -> SealedExecution:
    if not isinstance(action, RunAction):
        raise DataflowError("INVALID_CONTRACT", "seal_dataflow requires a driver RunAction")
    if not isinstance(graph, DataflowGraph) or not isinstance(physical, PhysicalDataflow):
        raise DataflowError("INVALID_CONTRACT", "seal_dataflow requires a graph and physical plan")
    if graph.action_id != action.action_id:
        raise DataflowError("INVALID_CONTRACT", "Graph action differs from the sealing action")
    if graph.dag_id != physical.dag_id:
        raise DataflowError("INVALID_CONTRACT", "Graph and physical dag id differ")
    bounds = physical.bounds
    context = action.context
    graph_raw = encode_wire(graph, type_name="DataflowGraph", bounds=bounds)
    physical_raw = encode_wire(physical, type_name="PhysicalDataflow", bounds=bounds)
    logical_ref = store.put(iter([graph_raw]), max_bytes=bounds.max_control_bytes, media_type=GRAPH_MEDIA)
    physical_ref = store.put(iter([physical_raw]), max_bytes=bounds.max_control_bytes, media_type=PHYSICAL_MEDIA)
    programs, code_pin = [], {}
    for node in graph.nodes:
        if node.program_ref is None:
            if not _builtin_node(node):
                raise DataflowError("INVALID_CONTRACT",
                                    "Executable node lacks a real program artifact: " + node.occurrence_id)
            continue
        if not store.inspect(node.program_ref):
            raise DataflowError("CORRUPT_ARTIFACT", "Sealed program artifact is absent")
        manifest = _read_json(store, node.program_ref, max_bytes=bounds.max_control_bytes,
                              media_type=PROGRAM_MEDIA)
        programs.append(node.program_ref)
        code_pin[node.occurrence_id] = manifest["code_identity"]
    code_pin_raw = document({"schema": CODE_PIN_SCHEMA, "action_id": action.action_id,
                             "platform_revision": context.journal.recovery_identity()["platform_revision"],
                             "programs": code_pin}, bounds.max_control_bytes)
    code_pin_ref = store.put(iter([code_pin_raw]), max_bytes=bounds.max_control_bytes, media_type=CODE_PIN_MEDIA)
    bridge = identity_bridge
    if bridge is None:
        bridge = {"schema": "demiflow.identity_bridge_unavailable.v3",
                  "action_id": action.action_id, "dag_id": graph.dag_id, "plan_id": physical.plan_id,
                  "available": False, "reason": "checked_report_binding_missing"}
    else:
        if bridge.get("action_id") != action.action_id or bridge.get("dag_id") != graph.dag_id:
            raise DataflowError("INVALID_CONTRACT", "report bridge differs from executed graph")
        from .identity_bridge import seal_identity_bridge
        seal_identity_bridge(context, action, bridge)
    bridge_raw = document(bridge, bounds.max_control_bytes)
    identity_bridge_ref = store.put(iter([bridge_raw]), max_bytes=bounds.max_control_bytes, media_type=BRIDGE_MEDIA)
    source_refs = tuple(s.manifest_ref for s in physical.sources if s.manifest_ref is not None)
    constants_ref = physical.constants_ref
    facts = {"schema": SEAL_SCHEMA, "run_id": context.ref.run_id, "action_id": action.action_id,
             "dag_id": graph.dag_id, "plan_id": physical.plan_id,
             "plan_revision": physical.plan_revision,
             "logical_ref": _artifact_json(logical_ref), "physical_ref": _artifact_json(physical_ref),
             "programs": [_artifact_json(r) for r in programs],
             "code_pin_ref": _artifact_json(code_pin_ref),
             "identity_bridge_ref": _artifact_json(identity_bridge_ref),
             "source_refs": [_artifact_json(r) for r in source_refs],
             "constants_ref": _artifact_json(constants_ref),
             "graph_digest": compute_graph_digest(graph),
             "physical_digest": compute_physical_digest(physical)}
    # The immutable journal fact is written last: a failed seal never starts a worker,
    # and a differing re-seal of the same action is refused by the native binding.
    try:
        context.journal.bind_source(context.fence, action.action_id + _SEAL_KEY, facts)
    except RecoveryContractError as exc:
        raise DataflowError("INVALID_CONTRACT", "Action already sealed with a different plan: " + str(exc)) from exc
    return SealedExecution(logical_ref=logical_ref, physical_ref=physical_ref,
                           programs=tuple(programs), code_pin_ref=code_pin_ref,
                           identity_bridge_ref=identity_bridge_ref, source_refs=source_refs,
                           constants_ref=constants_ref)


# -- D013 -----------------------------------------------------------------
def load_dataflow(directory: Path, action_id: str, *, store, bounds: Bounds):
    identifier(action_id)
    journal = RunJournal(directory)
    if not journal.path.is_file():
        raise DataflowError("INCOMPATIBLE_HISTORY", "Run journal is absent")
    facts = journal.source_binding(action_id + _SEAL_KEY)
    if facts is None:
        from .dataflow_history import load_execution_version
        binding = load_execution_version(directory, action_id, max_bytes=bounds.max_state_bytes)
        raise DataflowError("INCOMPATIBLE_HISTORY",
                            "Historical action schema " + str(binding.schema_version)
                            + " is not a v3 sealed dataflow")
    if type(facts) is not dict or facts.get("schema") != SEAL_SCHEMA:
        raise DataflowError("INCOMPATIBLE_HISTORY", "Unsupported or future sealed dataflow version")
    if facts.get("action_id") != action_id:
        raise DataflowError("INVALID_CONTRACT", "Sealed action differs from the requested action")
    logical_ref = _artifact_from_json(facts["logical_ref"])
    graph = decode_wire(_read_bytes(store, logical_ref, max_bytes=bounds.max_control_bytes,
                                    media_type=GRAPH_MEDIA),
                        type_name="DataflowGraph", bounds=bounds)
    physical_ref = _artifact_from_json(facts["physical_ref"])
    physical = decode_wire(_read_bytes(store, physical_ref, max_bytes=bounds.max_control_bytes,
                                       media_type=PHYSICAL_MEDIA),
                           type_name="PhysicalDataflow", bounds=bounds)
    if compute_graph_digest(graph) != facts["graph_digest"]:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed graph digest differs")
    if compute_physical_digest(physical) != facts["physical_digest"]:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed physical digest differs")
    if graph.action_id != action_id or graph.dag_id != facts["dag_id"]:
        raise DataflowError("INVALID_CONTRACT", "Sealed graph identity differs")
    if physical.plan_id != facts["plan_id"] or physical.dag_id != facts["dag_id"]:
        raise DataflowError("INVALID_CONTRACT", "Sealed physical identity differs")
    programs = tuple(_artifact_from_json(r) for r in facts["programs"])
    code_pin_ref = _artifact_from_json(facts["code_pin_ref"])
    identity_bridge_ref = _artifact_from_json(facts["identity_bridge_ref"])
    source_refs = tuple(_artifact_from_json(r) for r in facts["source_refs"])
    constants_ref = _artifact_from_json(facts["constants_ref"])
    for ref in (*programs, code_pin_ref, identity_bridge_ref, *source_refs, constants_ref):
        if not store.inspect(ref):
            raise DataflowError("CORRUPT_ARTIFACT", "Sealed artifact is missing; partial seal is not loadable")
    sealed = SealedExecution(logical_ref=logical_ref, physical_ref=physical_ref,
                             programs=programs, code_pin_ref=code_pin_ref,
                             identity_bridge_ref=identity_bridge_ref, source_refs=source_refs,
                             constants_ref=constants_ref)
    return graph, physical, sealed


def load_sealed_dataflow(sealed: SealedExecution, *, action_id: str, store, bounds: Bounds):
    """Read the driver's sealed refs without opening its journal on a worker.

    The assignment is the authenticated root. Content hashes, semantic IDs,
    program membership and the code-pin manifest must all agree before loading
    any user pickle. Files merely present in a worker cache confer no authority.
    """
    identifier(action_id)
    if not isinstance(sealed, SealedExecution):
        raise DataflowError("INVALID_CONTRACT", "Expected a sealed execution")
    graph = decode_wire(_read_bytes(store, sealed.logical_ref,
                        max_bytes=bounds.max_control_bytes, media_type=GRAPH_MEDIA),
                        type_name="DataflowGraph", bounds=bounds)
    physical = decode_wire(_read_bytes(store, sealed.physical_ref,
                           max_bytes=bounds.max_control_bytes, media_type=PHYSICAL_MEDIA),
                           type_name="PhysicalDataflow", bounds=bounds)
    if graph.action_id != action_id:
        raise DataflowError("CODE_MISMATCH", "Sealed action differs from assignment")
    if graph.dag_id != compute_graph_digest(graph):
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed graph identity differs")
    if physical.dag_id != graph.dag_id or physical.plan_id != compute_physical_digest(physical):
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed physical identity differs")
    programs = tuple(node.program_ref for node in graph.nodes if node.program_ref is not None)
    if programs != sealed.programs or physical.constants_ref != sealed.constants_ref:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed program/constants membership differs")
    if tuple(s.manifest_ref for s in physical.sources if s.manifest_ref is not None) != sealed.source_refs:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed source membership differs")
    pin = _read_json(store, sealed.code_pin_ref, max_bytes=bounds.max_control_bytes,
                     media_type=CODE_PIN_MEDIA)
    expected = {node.occurrence_id for node in graph.nodes if node.program_ref is not None}
    if (pin.get("schema") != CODE_PIN_SCHEMA or pin.get("action_id") != action_id
            or type(pin.get("programs")) is not dict or set(pin["programs"]) != expected):
        raise DataflowError("CODE_MISMATCH", "Code-pin manifest membership differs")
    for ref in (*programs, sealed.identity_bridge_ref, *sealed.source_refs, sealed.constants_ref):
        if not store.inspect(ref):
            raise DataflowError("CORRUPT_ARTIFACT", "Sealed artifact is absent")
    return graph, physical, pin
