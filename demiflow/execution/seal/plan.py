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
from time import time_ns

from ...data.datasink import Datasink
from ...data.plan import CallableSpec, LogicalPlan, MapOp
from .business_sources import stable_code_identity, plan_code_pin
from ..contracts.dataflow import (
    ArtifactRef, Bounds, CompiledDataflow, DataflowGraph, LoadedProgram,
    PhysicalDataflow, ProgramDeclaration, SealedExecution,
)
from ..contracts.dataflow_codec import (
    compute_graph_digest, compute_physical_digest, decode_wire, encode_wire, _thaw_json,
)
from ..contracts.dataflow_errors import DataflowError
from ..identity_bridge import build_identity_bridge
from ..recovery_contract import RecoveryContractError, document, identifier, identity
from ..recovery_identity import describe
from ..run_context import RunAction
from ..run_journal import RunJournal

SEAL_SCHEMA = "demiflow.dataflow_seal.v3"
TRAINING_SEAL_SCHEMA = "demiflow.dataflow_seal.v4"
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

_SEAL_FACT_KEYS = frozenset({"schema", "run_id", "action_id", "dag_id", "plan_id",
                            "logical_ref", "physical_ref", "programs", "code_pin_ref",
                            "identity_bridge_ref", "source_refs", "constants_ref",
                            "plan_revision", "graph_digest", "physical_digest"})


def _validate_seal_marker(marker):
    """Validate the original marker and its optional producer metadata."""
    if type(marker) is not dict:
        raise DataflowError("INCOMPATIBLE_HISTORY", "v3 seal marker is not a JSON object")
    if marker.get("schema") not in {SEAL_SCHEMA, TRAINING_SEAL_SCHEMA}:
        raise DataflowError("INCOMPATIBLE_HISTORY", "unsupported v3 seal schema: " + repr(marker.get("schema")))
    required = _SEAL_FACT_KEYS
    if marker["schema"] == TRAINING_SEAL_SCHEMA:
        required = required | {"fit_occurrences"}
        occurrences = marker.get("fit_occurrences")
        if (type(occurrences) is not list or not occurrences or any(type(i) is not str or
                len(i) != 64 or any(c not in "0123456789abcdef" for c in i) for i in occurrences)
                or len(set(occurrences)) != len(occurrences)):
            raise DataflowError("INVALID_CONTRACT", "training seal requires original unique fit occurrences")
    if not required <= set(marker) <= required | {"sealed_at", "observation_labels"}:
        raise DataflowError("INVALID_CONTRACT", "v3 seal marker key set is unsupported")
    if "sealed_at" in marker:
        value = marker["sealed_at"]
        if type(value) is not int or not 0 <= value <= 2**63 - 1:
            raise DataflowError("INVALID_CONTRACT", "v3 sealed_at must be nonnegative int64 epoch milliseconds")


def _builtin_node(node):
    """Only registered platform implementations may omit a user program."""
    from ...planning.dataflow_registry import operator_registry, source_registry, terminal_registry
    if node.kind == "terminal" and node.kernel.kind == "fixed_dataset":
        return True
    if node.kernel.kind == "PredictOp" or node.kernel.configuration.get("predict_step") is not None:
        return False
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


def _artifact_chunks(store, ref: ArtifactRef, *, media_type: str, max_bytes=None):
    if ref.media_type != media_type:
        raise DataflowError("INVALID_CONTRACT", "Sealed artifact media type differs")
    if max_bytes is None:
        max_bytes = max(1, ref.size_bytes)
    if ref.size_bytes > max_bytes:
        raise DataflowError("BUDGET_EXCEEDED", "Sealed artifact exceeds read budget")
    digest = hashlib.sha256()
    size = 0
    for chunk in store.read(ref, max_bytes=max_bytes):
        size += len(chunk)
        if size > ref.size_bytes:
            raise DataflowError("CORRUPT_ARTIFACT", "Sealed artifact length differs")
        digest.update(chunk)
        yield chunk
    if size != ref.size_bytes or digest.hexdigest() != ref.digest:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed artifact length or digest differs")


def _read_bytes(store, ref: ArtifactRef, *, max_bytes: int, media_type: str) -> bytes:
    return b"".join(_artifact_chunks(store, ref, media_type=media_type, max_bytes=max_bytes))


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
        from ...data.sources import DatasourceSource
        from ..queue_source import QueueDatasource
        source = declaration.source_spec
        if isinstance(source, DatasourceSource) and type(source.datasource) is QueueDatasource:
            from ..dataflow_acquisition_call import QueueSourceDeclaration
            return replace(source, datasource=QueueSourceDeclaration(source.datasource.declaration()))
        return source
    if declaration.action_program is not None:
        return declaration.action_program.value
    raise DataflowError("INVALID_CONTRACT", "ProgramDeclaration has no program body")


def _base_code_identity_from_value(kind: str, value):
    """Recompute the historical code-pin from an actually-loaded program value."""
    try:
        if kind == "callable":
            return plan_code_pin(LogicalPlan((MapOp(callable=value),)))
        if kind == "source":
            return plan_code_pin(LogicalPlan(()), source=value)
        if kind == "aggregate":
            from ...data.aggregate import AggregateFnV2
            aggregates = value if isinstance(value, tuple) else (value,)
            if not aggregates or any(not isinstance(a, AggregateFnV2) for a in aggregates):
                raise DataflowError("INVALID_CONTRACT", "aggregate program must declare AggregateFnV2")
            return {"aggregates": [
                {"configuration": describe(vars(a)), "methods": [plan_code_pin(LogicalPlan((MapOp(
                    callable=CallableSpec.create(getattr(type(a), method))),)))
                    for method in ("aggregate_partial", "aggregate_block", "merge_states", "combine", "finalize_state", "finalize")]}
                for a in aggregates]}
        if kind == "sink":
            from ...lance.model import LanceWriteSpec
            if isinstance(value, LanceWriteSpec):
                return {"sink": "demiflow.lance.spec", "declaration": value.to_dict()}
            if not isinstance(value, Datasink):
                raise DataflowError("INVALID_CONTRACT", "Terminal sink program is not a Datasink")
            try:
                declaration = value.recovery_identity()
            except NotImplementedError as exc:
                raise DataflowError("INVALID_CONTRACT",
                                    "Sink program lacks a durable recovery identity") from exc
            return {"sink": stable_code_identity(describe(type(value))),
                    "declaration": declaration}
    except RecoveryContractError as exc:
        raise DataflowError("INVALID_CONTRACT", "Program code-pin refused: " + str(exc)) from exc
    raise DataflowError("INVALID_CONTRACT", "Unknown program kind: " + str(kind))


def _code_identity_from_value(kind, value, *, sources=None):
    from .business_sources import business_sources, source_identity
    return {**_base_code_identity_from_value(kind, value),
            'imports': source_identity(business_sources(kind, value) if sources is None else sources)}



def _matches_original_program(kind, value, expected):
    actual = _code_identity_from_value(kind, value)
    expected_digest = identity(expected)
    if identity(actual) == expected_digest:
        return True
    if kind != 'callable' or type(value) is not CallableSpec:
        return False
    # This is a fresh plan_code_pin for one MapOp. Business arguments, the
    # entire code record and every dependency remain in the digest comparison.
    from ..recovery_sdk_v3 import routing_source_revisions
    revisions = routing_source_revisions(value.target)
    if revisions is None:
        return False
    target = actual['operations'][0]['fields']['callable']['fields']['target']
    if target['source_hash'] != revisions[0]:
        return False
    target['source_hash'] = revisions[1]
    return identity(actual) == expected_digest


def _stage_one_program(declaration: ProgramDeclaration, *, store, bounds: Bounds,
                       live_binding=None, force_live=False, shared_payload=None) -> ArtifactRef:
    from ..dataflow_program_pickle import stage_payload
    kind = _program_kind(declaration)
    value = _program_value(declaration)
    if kind == 'source':
        from ..literal_source import literal_reference
        literal = literal_reference(value, bounds)
        if literal is not None:
            value = literal
    from .business_sources import business_sources, stage_sources
    sources = business_sources(kind, value)
    code_identity = _code_identity_from_value(kind, value, sources=sources)
    manifest = {"schema": PROGRAM_SCHEMA, "occurrence_id": declaration.occurrence_id,
                "kind": kind, "code_identity": code_identity}
    if shared_payload is not None:
        payload_ref, binding, shared_sources, imports = shared_payload
        manifest['shared_actor'] = binding
        manifest['shared_sources'] = {'refs': shared_sources, 'imports': imports}
    else:
        payload_ref = None if force_live else stage_payload(
            value, store=store, media_type=PAYLOAD_MEDIA, allow_live=live_binding is not None)
    if payload_ref is None:
        from ..dataflow_thread_programs import marker
        manifest['thread_attempt'] = marker(declaration.occurrence_id, code_identity)
    else:
        manifest['payload_ref'] = _artifact_json(payload_ref)
    manifest['business_sources'] = stage_sources(sources, store=store)
    raw = document(manifest, bounds.max_control_bytes)
    ref = store.put(iter([raw]), max_bytes=bounds.max_control_bytes, media_type=PROGRAM_MEDIA)
    if 'thread_attempt' in manifest:
        from ..dataflow_thread_programs import register
        register(value, binding=live_binding, occurrence_id=declaration.occurrence_id,
                 max_entries=bounds.max_nodes, program_marker=manifest['thread_attempt'])
    return ref


def _owned_factory_declaration(spec):
    """A class blueprint is durable only when it borrows no mutable state.

    Reuse the existing passive function-state policy for methods and constructor
    arguments. A serializable external list is still shared caller state; pickle
    success alone cannot justify replacing it with a disconnected copy.
    """
    import types
    from ..kernels.task_scope import function_residency_kind
    if spec is None or not spec.is_class:
        return False
    immutable = (str, bytes, int, float, bool, type(None))
    classes = type.__getattribute__(spec.target, '__mro__')
    if len(classes) > 32:
        return False
    visited = 0
    for cls in classes:
        if cls is object:
            continue
        fields = type.__getattribute__(cls, '__dict__')
        visited += len(fields)
        if visited > 1024:
            return False
        for name, value in fields.items():
            if name in {'__dict__', '__weakref__', '__module__', '__doc__', '__qualname__'}:
                continue
            if type(value) in (classmethod, staticmethod):
                value = value.__func__
            if type(value) is types.FunctionType:
                args, kwargs = (spec.constructor_args, spec.constructor_kwargs) if name in {'__new__', '__init__'} else (
                    (spec.call_args, spec.call_kwargs) if name == '__call__' else ((), {}))
                if function_residency_kind(value, call_args=args, call_kwargs=kwargs) != 'none':
                    return False
            elif type(value) not in immutable:
                return False
    return True

# -- D015 -----------------------------------------------------------------
def stage_dataflow_programs(compiled: CompiledDataflow, *, action: RunAction,
                            store, bounds: Bounds, worker_mode=None) -> DataflowGraph:
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
    from ..dataflow_shared import shared_binding
    shared_payloads = {}
    for node in graph.nodes:
        binding = shared_binding(node.kernel.configuration, node.occurrence_id)
        if binding is None or binding['owner'] in shared_payloads:
            continue
        from ..dataflow_program_pickle import stage_payload
        from .business_sources import business_sources, source_identity, stage_sources, MAX_SOURCES
        sources = {}
        for occ in binding['members']:
            member = declarations[occ]
            for name, source in business_sources(_program_kind(member), _program_value(member)).items():
                if name in sources and sources[name].digest != source.digest:
                    raise DataflowError('CODE_MISMATCH', 'Shared actor bindings name different business source bytes')
                sources[name] = source
            if len(sources) > MAX_SOURCES:
                raise DataflowError('BUDGET_EXCEEDED', 'Shared actor business imports exceed original source budget')
        members = tuple(_program_value(declarations[o]) for o in binding['members'])
        payload_ref = stage_payload(members, store=store, media_type=PAYLOAD_MEDIA,
            serialization_error='Shared actor requires a serializable recoverable declaration')
        shared_payloads[binding['owner']] = (payload_ref, binding,
            stage_sources(sources, store=store), source_identity(sources))
    nodes = []
    shared_stream = worker_mode == 'thread' and any(n.kernel.configuration.get('streaming_map')
        and n.kernel.configuration.get('execution') == 'thread' for n in graph.nodes)
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
        config = node.kernel.configuration
        # An explicit thread stream also owns its lazy iterator in the same
        # attempt. Preserve shared locks/counters rather than serializing a
        # disconnected source closure; its source snapshot remains attempt_only
        # and the planner keeps blocking next() in an independent worker.
        live_iterator = (shared_stream and node.kind == 'source'
                         and node.kernel.kind == 'iterable')
        # A class without shared captures is a declaration, not a borrowed actor.
        # Serialize that declaration and keep its per-owner checkpoint state.
        # Unserializable constructors still use the existing attempt-only path;
        # ordinary shared functions/instances must retain their live identity.
        thread_stream = bool(config.get('streaming_map') and config.get('execution') == 'thread')
        owned_factory = (worker_mode == 'thread' and thread_stream
                         and _owned_factory_declaration(declaration.callable_spec))
        force_live = bool(worker_mode == 'thread' and (live_iterator
                          or thread_stream and not owned_factory or shared_stream
                          and node.kernel.kind == 'AsyncMapOp' and not config.get('task_batch_rows')))
        eligible = worker_mode == 'thread' and not config.get('task_batch_rows') and (
            thread_stream or force_live or node.kernel.kind == 'AsyncMapOp')
        from ..dataflow_thread_programs import owner
        ref = _stage_one_program(declaration, store=store, bounds=bounds,
            live_binding=owner(action) if eligible else None, force_live=force_live,
            shared_payload=shared_payloads.get(config.get("shared_actor", {}).get("owner")))
        manifest = _read_json(store, ref, max_bytes=bounds.max_control_bytes, media_type=PROGRAM_MEDIA)
        if 'thread_attempt' in manifest:
            from ...planning.dataflow_compile import _plain
            node = replace(node, kernel=replace(node.kernel, state='nonrecoverable',
                configuration={**_plain(config), 'thread_attempt': True}))
        nodes.append(replace(node, program_ref=ref))
    jobs = []
    for job in graph.jobs:
        declaration = declarations.get(job.occurrence_id)
        if declaration is None or declaration.callable_spec is None:
            raise DataflowError("INVALID_CONTRACT", "fit requires an original trainer declaration")
        ref = _stage_one_program(declaration, store=store, bounds=bounds)
        jobs.append(replace(job, program_ref=ref, configuration=_thaw_json(job.configuration)))
    staged = replace(graph, nodes=tuple(nodes), jobs=tuple(jobs))
    for node in (*staged.nodes, *staged.jobs):
        if node.program_ref is not None and not store.inspect(node.program_ref):
            raise DataflowError("CORRUPT_ARTIFACT", "Staged program artifact is absent")
    return staged


# -- D016 -----------------------------------------------------------------
def verify_dataflow_program(ref: ArtifactRef, *, expected_pin, store, bounds: Bounds, cache=None, live_binding=None, shared_cache=None) -> LoadedProgram:
    manifest = _read_json(store, ref, max_bytes=bounds.max_control_bytes, media_type=PROGRAM_MEDIA)
    if type(manifest) is not dict or manifest.get("schema") != PROGRAM_SCHEMA:
        raise DataflowError("INVALID_CONTRACT", "Unsupported program artifact schema")
    kind = manifest["kind"]
    code_identity = manifest["code_identity"]
    if identity(code_identity) != identity(expected_pin):
        raise DataflowError("INVALID_CONTRACT", "Program code pin differs from the sealed expectation")
    shared_sources = manifest.get('shared_sources')
    if 'shared_actor' in manifest and (type(shared_sources) is not dict or set(shared_sources) != {'refs', 'imports'}):
        raise DataflowError('CORRUPT_ARTIFACT', 'Shared actor program has no complete source bundle')
    if cache is not None:
        from .business_sources import mount_sources
        if shared_sources is not None:
            mount_sources(shared_sources['refs'], shared_sources['imports'], store=store, cache=cache)
        mount_sources(manifest.get('business_sources', {}), code_identity.get('imports', {}),
                      store=store, cache=cache)
    if 'thread_attempt' in manifest:
        if 'payload_ref' in manifest:
            raise DataflowError('CORRUPT_ARTIFACT', 'live program cannot also claim a pickle payload')
        from ..dataflow_thread_programs import lookup
        value = lookup(manifest['thread_attempt'], binding=live_binding,
                       occurrence_id=manifest['occurrence_id'])
    else:
        payload_ref = _artifact_from_json(manifest["payload_ref"])
        from ..dataflow_shared import shared_binding
        binding = shared_binding(manifest, manifest['occurrence_id'])
        cached = shared_cache.get(binding['owner']) if binding and shared_cache is not None else None
        if cached is not None and (cached[0] != payload_ref or cached[1] != binding):
            raise DataflowError('CORRUPT_ARTIFACT', 'Shared actor program bundle differs between members')
        from ..dataflow_program_pickle import load_payload
        value = cached[2] if cached is not None else load_payload(
            _artifact_chunks(store, payload_ref, media_type=PAYLOAD_MEDIA))
        if binding:
            if type(value) is not tuple or len(value) != len(binding['members']):
                raise DataflowError('CORRUPT_ARTIFACT', 'Invalid shared actor program membership')
            if shared_cache is not None:
                shared_cache[binding['owner']] = (payload_ref, binding, value)
            value = value[binding['members'].index(manifest['occurrence_id'])]
    if not _matches_original_program(kind, value, code_identity):
        raise DataflowError("INVALID_CONTRACT",
                            "Loaded program code differs from the sealed pin (edited code or wrong module)")
    return LoadedProgram(kind=kind, value=value, digest=identity(code_identity))


def verify_completed_live_program(ref, *, expected_pin, declaration, store, bounds):
    """Check code/source bytes without loading a retired attempt-local value.

    This grants no executable program. The caller must independently recover an
    original final ACKed cut before using it to read an already published result.
    Current declarations are inspected, never instantiated or invoked.
    """
    from .business_sources import MAX_SOURCES, source_media
    from ..dataflow_thread_programs import marker
    manifest = _read_json(store, ref, max_bytes=bounds.max_control_bytes, media_type=PROGRAM_MEDIA)
    if type(manifest) is not dict:
        raise DataflowError('CORRUPT_ARTIFACT', 'completed live program manifest must be an object')
    code = manifest.get('code_identity')
    if (manifest.get('schema') != PROGRAM_SCHEMA or 'payload_ref' in manifest
            or declaration is None or manifest.get('occurrence_id') != declaration.occurrence_id
            or manifest.get('thread_attempt') != marker(declaration.occurrence_id, code)
            or identity(code) != identity(expected_pin)):
        raise DataflowError('CORRUPT_ARTIFACT', 'completed live program differs from its sealed code pin')
    kind = _program_kind(declaration)
    if kind != manifest.get('kind') or not _matches_original_program(kind, _program_value(declaration), code):
        raise DataflowError('CODE_MISMATCH', 'completed live program code differs from the sealed pin')
    sources, imports = manifest.get('business_sources', {}), code.get('imports', {})
    if type(sources) is not dict or set(sources) != set(imports) or len(sources) > MAX_SOURCES:
        raise DataflowError('CORRUPT_ARTIFACT', 'completed live program source membership differs')
    for name, fact in imports.items():
        source = _artifact_from_json(sources[name])
        if source.digest != fact['sha256'] or source.size_bytes != fact['bytes']:
            raise DataflowError('CODE_MISMATCH', 'completed live source differs from the sealed pin')
        for _ in _artifact_chunks(store, source, media_type=source_media(name)):
            pass


# -- D014 -----------------------------------------------------------------
def build_dataflow_identity_bridge(graph: DataflowGraph, *, report_digest: str, bindings):
    if not isinstance(graph, DataflowGraph):
        raise DataflowError("INVALID_CONTRACT", "Identity bridge requires a DataflowGraph")
    if type(report_digest) is not str or not report_digest:
        raise DataflowError("INVALID_CONTRACT", "Identity bridge requires a report digest")
    if type(bindings) is not dict:
        raise DataflowError("INVALID_CONTRACT", "Identity bridge bindings must be a mapping")
    members = (*graph.nodes, *graph.jobs)
    occ_by_id = {n.occurrence_id: n for n in members}
    logical_dag = {"dag_id": graph.dag_id,
                   "nodes": [{"occurrence_id": n.occurrence_id, "logical_node_id": n.semantic_id}
                             for n in members]}
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


def resolve_execution_constants(graph: DataflowGraph, *, action: RunAction, store, policy=None) -> ArtifactRef:
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
    from ..dataflow_managed_devices import service_constants
    for occurrence, value in service_constants(graph, policy=policy).items():
        constants.setdefault(occurrence, {}).update(value)
    raw = document({"schema": CONSTANTS_SCHEMA, "action_id": action.action_id,
                    "dag_id": graph.dag_id, "constants": constants},
                   action.context.limits["max_state_bytes"])
    ref = store.put(iter([raw]), max_bytes=action.context.limits["max_state_bytes"],
                    media_type=CONSTANTS_MEDIA)
    journal.bind_source(action.context.fence, key, _artifact_json(ref))
    return ref


# -- D012 -----------------------------------------------------------------
def seal_dataflow(action: RunAction, graph: DataflowGraph, physical: PhysicalDataflow,
                  *, store, identity_bridge=None, observation_labels=None) -> SealedExecution:
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
    existing = context.journal.source_binding(action.action_id + _SEAL_KEY)
    if existing is not None:
        _validate_seal_marker(existing)
    graph_raw = encode_wire(graph, type_name="DataflowGraph", bounds=bounds)
    physical_raw = encode_wire(physical, type_name="PhysicalDataflow", bounds=bounds)
    logical_ref = store.put(iter([graph_raw]), max_bytes=bounds.max_control_bytes, media_type=GRAPH_MEDIA)
    physical_ref = store.put(iter([physical_raw]), max_bytes=bounds.max_control_bytes, media_type=PHYSICAL_MEDIA)
    programs, code_pin = [], {}
    for node in (*graph.nodes, *graph.jobs):
        if node.program_ref is None:
            if node in graph.jobs or not _builtin_node(node):
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
        from ..identity_bridge import seal_identity_bridge
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
    if graph.jobs:
        facts["schema"] = TRAINING_SEAL_SCHEMA
        facts["fit_occurrences"] = [job.occurrence_id for job in graph.jobs]
    original = existing
    if observation_labels is not None:
        labels = dict(observation_labels)
        if (not set(labels) <= {n.occurrence_id for n in graph.nodes}
                or any(type(v) is not str or not v for v in labels.values())
                or len(set(labels.values())) != len(labels)):
            raise DataflowError('INVALID_CONTRACT', 'observation labels must uniquely name graph occurrences')
        document(labels, bounds.max_control_bytes)
        facts['observation_labels'] = labels
    elif original is not None and 'observation_labels' in original:
        facts['observation_labels'] = original['observation_labels']
    if original is None:
        # Match the existing artifact/workbench epoch-millisecond contract.
        # This is journal metadata, never part of graph/plan/artifact identity.
        facts["sealed_at"] = time_ns() // 1_000_000
    elif type(original) is dict and "sealed_at" in original:
        facts["sealed_at"] = original["sealed_at"]
    # Older v3 seals omitted the timestamp. Re-sealing must preserve that exact
    # fact rather than inventing a time or rewriting immutable history.
    _validate_seal_marker(facts)
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
def load_dataflow(directory: Path, action_id: str, *, store, bounds: Bounds, _reader=None,
                  _max_carrier_bytes=None, _deadline=None):
    identifier(action_id)
    def check_read():
        import time
        import math
        if _deadline is not None and (type(_deadline) not in (int, float) or not math.isfinite(_deadline) or time.monotonic() >= _deadline):
            raise DataflowError('BUDGET_EXCEEDED', 'Historical read deadline exceeded or invalid')
        check = getattr(_reader, 'check_read', None)
        if check is not None:
            check()
    check_read()
    journal = RunJournal(directory) if _reader is None else _reader
    if not journal.path.is_file():
        raise DataflowError("INCOMPATIBLE_HISTORY", "Run journal is absent")
    facts = journal.source_binding(action_id + _SEAL_KEY)
    if facts is None:
        from ..dataflow_history import load_execution_version
        binding = load_execution_version(directory, action_id, max_bytes=bounds.max_state_bytes)
        raise DataflowError("INCOMPATIBLE_HISTORY",
                            "Historical action schema " + str(binding.schema_version)
                            + " is not a v3 sealed dataflow")
    _validate_seal_marker(facts)
    if _max_carrier_bytes is not None:
        if type(_max_carrier_bytes) is not int or _max_carrier_bytes <= 0:
            raise DataflowError('INVALID_CONTRACT', 'Invalid carrier read budget')
        references = [facts[key] for key in ('logical_ref','physical_ref','code_pin_ref','identity_bridge_ref','constants_ref')]
        references.extend(facts['programs']); references.extend(facts['source_refs'])
        for raw_ref in references:
            check_read()
            if _artifact_from_json(raw_ref).size_bytes > _max_carrier_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'Historical carrier exceeds read budget')
    if facts.get("action_id") != action_id:
        raise DataflowError("INVALID_CONTRACT", "Sealed action differs from the requested action")
    logical_ref = _artifact_from_json(facts["logical_ref"])
    graph = decode_wire(_read_bytes(store, logical_ref, max_bytes=bounds.max_control_bytes,
                                    media_type=GRAPH_MEDIA),
                        type_name="DataflowGraph", bounds=bounds)
    check_read()
    physical_ref = _artifact_from_json(facts["physical_ref"])
    physical = decode_wire(_read_bytes(store, physical_ref, max_bytes=bounds.max_control_bytes,
                                       media_type=PHYSICAL_MEDIA),
                           type_name="PhysicalDataflow", bounds=bounds)
    verify_fit_membership(facts, graph)
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
        check_read()
        if not store.inspect(ref):
            raise DataflowError("CORRUPT_ARTIFACT", "Sealed artifact is missing; partial seal is not loadable")
        check_read()
    sealed = SealedExecution(logical_ref=logical_ref, physical_ref=physical_ref,
                             programs=programs, code_pin_ref=code_pin_ref,
                             identity_bridge_ref=identity_bridge_ref, source_refs=source_refs,
                             constants_ref=constants_ref)
    check_read()
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
    pin = _verify_sealed_membership(sealed, action_id, graph, physical, store, bounds)
    return graph, physical, pin


def verify_fit_membership(marker, graph):
    if marker["schema"] == SEAL_SCHEMA:
        if getattr(graph, "jobs", ()):
            raise DataflowError("CORRUPT_ARTIFACT", "training graph cannot be bound by a row-only seal")
        return
    if marker.get("fit_occurrences") != [job.occurrence_id for job in graph.jobs]:
        raise DataflowError("CORRUPT_ARTIFACT", "seal marker differs from original fit occurrences")


def _verify_sealed_membership(sealed, action_id, graph, physical, store, bounds):
    if graph.action_id != action_id:
        raise DataflowError("CODE_MISMATCH", "Sealed action differs from assignment")
    if graph.dag_id != compute_graph_digest(graph):
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed graph identity differs")
    if physical.dag_id != graph.dag_id or physical.plan_id != compute_physical_digest(physical):
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed physical identity differs")
    if any(job.program_ref is None for job in graph.jobs):
        raise DataflowError("CODE_MISMATCH", "training job lacks its sealed trainer")
    programs = tuple(node.program_ref for node in (*graph.nodes, *graph.jobs) if node.program_ref is not None)
    if programs != sealed.programs or physical.constants_ref != sealed.constants_ref:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed program/constants membership differs")
    if tuple(s.manifest_ref for s in physical.sources if s.manifest_ref is not None) != sealed.source_refs:
        raise DataflowError("CORRUPT_ARTIFACT", "Sealed source membership differs")
    pin = _read_json(store, sealed.code_pin_ref, max_bytes=bounds.max_control_bytes,
                     media_type=CODE_PIN_MEDIA)
    expected = {node.occurrence_id for node in (*graph.nodes, *graph.jobs) if node.program_ref is not None}
    if (pin.get("schema") != CODE_PIN_SCHEMA or pin.get("action_id") != action_id
            or type(pin.get("programs")) is not dict or set(pin["programs"]) != expected):
        raise DataflowError("CODE_MISMATCH", "Code-pin manifest membership differs")
    for ref in (*programs, sealed.identity_bridge_ref, *sealed.source_refs, sealed.constants_ref):
        if not store.inspect(ref):
            raise DataflowError("CORRUPT_ARTIFACT", "Sealed artifact is absent")
    return pin


def verify_action_programs(action, graph, physical, sealed, *, store, bounds):
    """Verify programs from the journal-authenticated plan. Defer attempt-only code to final ACK proof."""
    pin = _verify_sealed_membership(sealed, action.action_id, graph, physical, store, bounds)
    pending = []
    for node in (*graph.nodes, *graph.jobs):
        if node.program_ref is None:
            continue
        expected = pin["programs"][node.occurrence_id]
        live_binding = None
        if node not in graph.jobs and node.kernel.configuration.get("thread_attempt"):
            if action.replaying:
                pending.append((node, expected))
                continue
            from ..dataflow_thread_programs import owner
            live_binding = owner(action)
        verify_dataflow_program(node.program_ref, expected_pin=expected, store=store,
                                bounds=bounds, live_binding=live_binding)
    return pending
