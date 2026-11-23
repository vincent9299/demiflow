"""Pure interpretation of sealed execution inputs into a read-only layout.

This module owns the four frozen data classes, the contract error type and the
single ``to_wire`` conversion shared by every consumer. It never reads files,
never imports the sealed-plan reader and never re-derives business semantics:
it only validates and explains the already-sealed logical dag / physical plan /
execution bindings. The stronger relational checks here close the gaps the v1
validators leave open (unique ownership, program/group agreement, channel edge
coverage and endpoint correspondence).
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
import re
from types import MappingProxyType

from demiflow.execution.recovery_contract import RecoveryContractError, document, identifier, identity
from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.contracts.worker import validate_assignment
from demiflow.planning.dag import partition_id_for, validate_channel_plan, validate_logical_dag, validate_physical_execution_plan
from demiflow.planning.graph_channels import RouteTable, materialize_channels
from demiflow.resources.contracts import ResourceError, ResourceProfile

ReadonlyDocument = Mapping[str, object]

EXECUTION_BINDINGS_SCHEMA = "demiflow.execution_bindings.v1"
SUPPORTED_SLICES = frozenset({"chain_v1", "queue_transform_v1", "static_fanout_v1"})
MAX_DETAIL = 512

_REASONS = frozenset({
    "missing_plan", "missing_bindings", "unsafe_artifact", "artifact_too_large",
    "corrupt_artifact", "identity_mismatch", "invalid_contract",
    "unsupported_layout", "invalid_assignment",
})
_BINDINGS_KEYS = frozenset({
    "schema", "run_id", "action_id", "dag_id", "plan_id", "artifact_manifest_ref",
    "supported_slice", "profiles", "programs", "driver_stage_ids",
    "source_bindings", "channel_options", "checkpoint_rows",
})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_STAGE_KEYS = frozenset({"stage_id", "logical_node_id", "occurrence_id",
                         "partitions", "traits", "budgets"})
_PROGRAM_KEYS = frozenset({"group_id", "stage_ids", "plan_ref",
                           "sealed_operators", "operator_bindings"})
_SLICE_SOURCE_KIND = {"chain_v1": "fixed_source",
                      "queue_transform_v1": "queue_declaration",
                      "static_fanout_v1": "fixed_source"}

# v2 is additive: it never changes the v1 key set or behaviour. It replaces the
# v1 ``programs`` list with explicit driver/worker ``executions`` so a sealed
# layout binds the real backend, program and profile. Historical v1 is never
# auto-upgraded; an unknown schema is refused.
EXECUTION_BINDINGS_SCHEMA_V2 = "demiflow.execution_bindings.v2"
SUPPORTED_SLICES_V2 = frozenset({"native_driver_v1"})
_BINDINGS_KEYS_V2 = frozenset({
    "schema", "run_id", "action_id", "dag_id", "plan_id", "artifact_manifest_ref",
    "supported_slice", "profiles", "executions", "driver_stage_ids",
    "source_bindings", "channel_options", "checkpoint_rows",
})
_EXECUTION_KEYS = frozenset({
    "execution_id", "placement", "group_id", "backend", "stage_ids",
    "profile_ref", "programs", "recovery",
})
_PROGRAM_BINDING_KEYS = frozenset({
    "program_id", "input_path", "plan_ref", "source_identity_ref",
    "sealed_operators", "operator_bindings",
})
_PLACEMENTS = frozenset({"driver", "worker"})
_BACKENDS = frozenset({"local_stream", "local_pull", "datafusion", "arrow_batch", "lance_scan"})
_RECOVERY_GUARANTEES = frozenset({
    "native_checkpoint", "final_commit", "delivery_only", "attempt_only",
})
_STAGE_BUDGET_KEYS = frozenset({"inflight_rows", "inflight_bytes", "output_bytes"})
_V2_SOURCE_KIND = {"native_driver_v1": "fixed_source"}


class PlanContractError(RecoveryContractError):
    """A sealed input cannot be interpreted or an assignment contradicts it.

    Consumers branch on ``.reason`` only; the message string is not a contract.
    """

    def __init__(self, reason, detail=""):
        if reason not in _REASONS:
            raise ValueError("Unknown plan contract reason: " + str(reason))
        self.reason = reason
        self.detail = str(detail)[:MAX_DETAIL]
        super().__init__(self.detail)


@dataclass(frozen=True)
class CheckpointPolicy:
    checkpoint_rows: int
    interval_ms: int
    unconfirmed_rows: int
    unconfirmed_bytes: int


@dataclass(frozen=True)
class WorkerPlacement:
    group_id: str
    stage_ids: tuple[str, ...]
    partition_ids: tuple[str, ...]
    profile: ReadonlyDocument
    program_ref: ReadonlyDocument
    sealed_operators: tuple[Mapping[str, str], ...]
    operator_bindings: tuple[ReadonlyDocument, ...]


@dataclass(frozen=True)
class ChannelBinding:
    channel_id: str
    producer_stage_ids: tuple[str, ...]
    consumer_stage_ids: tuple[str, ...]
    channel_plan: ReadonlyDocument
    options: ReadonlyDocument
    in_process: bool


@dataclass(frozen=True)
class ExecutionPlacement:
    """A sealed v2 execution unit: real backend, profile, programs and budgets.

    ``profile`` is the recursively frozen profile document resolved from the
    entry's profile_ref; ``programs`` are read-only ProgramBinding mappings;
    ``stage_limits`` is derived solely from physical.stages[].budgets (no second
    wire authority). ``recovery`` is descriptive execution metadata, never a
    user-selectable mode.
    """
    execution_id: str
    placement: str
    group_id: str | None
    backend: str
    stage_ids: tuple[str, ...]
    profile: ReadonlyDocument
    programs: tuple[ReadonlyDocument, ...]
    stage_limits: Mapping[str, Mapping[str, int]]
    recovery: str


@dataclass(frozen=True)
class ExecutionLayout:
    dag_id: str
    plan_id: str
    plan_revision: int
    artifact_manifest_ref: ReadonlyDocument
    bindings_ref: ReadonlyDocument
    supported_slice: str
    stages: tuple[ReadonlyDocument, ...]
    stage_owners: Mapping[str, ReadonlyDocument]
    source_bindings: tuple[ReadonlyDocument, ...]
    workers: tuple[WorkerPlacement, ...]
    channels: tuple[ChannelBinding, ...]
    checkpoint: CheckpointPolicy
    executions: tuple[ExecutionPlacement, ...] = ()


def _require(condition, reason, detail):
    if not condition:
        raise PlanContractError(reason, detail)


def _freeze(value):
    """Recursive read-only view detached from the caller's dict/list aliases."""
    if type(value) is dict:
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if type(value) in (list, tuple):
        return tuple(_freeze(item) for item in value)
    if value is None or type(value) in (str, int, bool, float):
        return value
    raise PlanContractError("invalid_contract", "non-JSON value in sealed document")


def to_wire(value):
    """Deep-convert read-only values into independent plain JSON dict/list.

    Mutating the result never touches the layout. Data classes become field
    dicts, mappings become dicts, tuples/lists become lists.
    """
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: to_wire(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {key: to_wire(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_wire(item) for item in value]
    if value is None or type(value) in (str, int, bool, float):
        return value
    raise PlanContractError("invalid_contract", "to_wire: unsupported value type")


def _check_identifier(value, detail):
    try:
        identifier(value)
    except (RecoveryContractError, ValueError) as exc:
        raise PlanContractError("invalid_contract", detail + ": " + str(exc))


def _validate_manifest_ref(ref):
    _require(type(ref) is dict and set(ref) == {"manifest_id", "sha256"},
             "invalid_contract", "manifest_ref needs manifest_id/sha256")
    _check_identifier(ref["manifest_id"], "manifest_id")
    _check_identifier(ref["sha256"], "manifest sha256")


def _validate_artifact_ref(ref, prefix):
    _require(type(ref) is dict and set(ref) == {"path", "sha256", "bytes"},
             "invalid_contract", prefix + " ref key set is unsupported")
    path = ref["path"]
    _require(type(path) is str and path and not Path(path).is_absolute()
             and ".." not in Path(path).parts,
             "invalid_contract", prefix + " path must be run-relative")
    _require(type(ref["sha256"]) is str and _SHA256.fullmatch(ref["sha256"]) is not None,
             "invalid_contract", prefix + " sha256 must be 64 lowercase hex")
    _require(type(ref["bytes"]) is int and ref["bytes"] >= 0,
             "invalid_contract", prefix + " bytes must be nonnegative")


def _validate_program_ref(ref):
    _require(type(ref) is dict and set(ref) == {
        "path", "sha256", "bytes", "code_pin_digest", "code_sources"},
        "invalid_contract", "program_ref key set is unsupported")
    _validate_artifact_ref({key: ref[key] for key in ("path", "sha256", "bytes")}, "program")
    _require(type(ref["code_pin_digest"]) is str
             and _SHA256.fullmatch(ref["code_pin_digest"]) is not None,
             "invalid_contract", "code_pin_digest must be 64 lowercase hex")
    sources = ref["code_sources"]
    _require(type(sources) is list, "invalid_contract", "code_sources must be a list")
    for source in sources:
        _require(type(source) is dict and set(source) == {"path", "sha256", "bytes", "source"},
                 "invalid_contract", "code source ref key set is unsupported")
        _validate_artifact_ref({key: source[key] for key in ("path", "sha256", "bytes")},
                               "code source")
        _require(type(source["source"]) is str and source["source"],
                 "invalid_contract", "code source origin must be a nonempty string")


def _validate_profile(profile_ref, profile_document):
    _require(type(profile_document) is dict
             and "requests" in profile_document
             and set(profile_document) <= {"requests", "limits", "requirements"},
             "invalid_contract", "profile document key set is unsupported")
    try:
        ResourceProfile(**profile_document)
    except (ResourceError, TypeError, ValueError) as exc:
        raise PlanContractError("invalid_contract", "profile document invalid: " + str(exc))
    _require(profile_ref == identity(profile_document),
             "identity_mismatch", "profile_ref is not the content identity of its document")


def _validate_routing_options(channel, options):
    routing = channel["routing"]
    try:
        if routing == "hash":
            RouteTable(channel, key_fields=options.get("key_fields"))
        elif routing == "target":
            target_field = options.get("target_field")
            _require(type(target_field) is str and target_field,
                     "invalid_contract", "target routing needs a nonempty target_field")
            RouteTable(channel, target_classes=options.get("target_classes"))
    except PlanContractError:
        raise
    except (ValueError, ProtocolError) as exc:
        raise PlanContractError("invalid_contract", "routing options invalid: " + str(exc))


def _validate_bindings(doc, logical_dag, physical_plan, manifest_ref):
    """Shared structural + relational validation used by sealing and interpreting.

    Returns derived mappings the interpreter reuses; raises PlanContractError on
    any violation. The v1 validators have already accepted the dag/plan shapes.
    """
    if type(doc) is dict and doc.get("schema") == EXECUTION_BINDINGS_SCHEMA_V2:
        return _validate_bindings_v2(doc, logical_dag, physical_plan, manifest_ref)
    _require(type(doc) is dict and set(doc) == _BINDINGS_KEYS,
             "invalid_contract", "execution bindings key set is unsupported")
    _require(doc["schema"] == EXECUTION_BINDINGS_SCHEMA,
             "invalid_contract", "unsupported execution bindings schema")
    supported_slice = doc["supported_slice"]
    _require(type(supported_slice) is str and supported_slice in SUPPORTED_SLICES,
             "unsupported_layout", "unsupported execution slice: " + str(supported_slice))
    for key in ("run_id", "action_id", "dag_id", "plan_id"):
        _check_identifier(doc[key], key)

    _validate_manifest_ref(doc["artifact_manifest_ref"])
    _require(doc["artifact_manifest_ref"] == manifest_ref == physical_plan["artifact_manifest_ref"],
             "identity_mismatch", "artifact manifest reference differs")
    _require(doc["dag_id"] == logical_dag["dag_id"] == physical_plan["logical_dag_ref"]["dag_id"],
             "identity_mismatch", "dag_id differs across sealed inputs")
    _require(doc["plan_id"] == physical_plan["plan_id"],
             "identity_mismatch", "plan_id differs across sealed inputs")
    _require(logical_dag["action_id"] == doc["action_id"],
             "identity_mismatch", "logical dag action_id differs from bindings")
    _require(physical_plan["logical_dag_ref"]["sha256"] == identity(logical_dag),
             "identity_mismatch", "physical logical_dag_ref does not bind the dag")

    nodes = logical_dag["nodes"]
    node_by_id = {node["logical_node_id"]: node for node in nodes}
    source_node_ids = {node["logical_node_id"] for node in nodes if not node["input_ports"]}
    terminal_node_ids = {node["logical_node_id"] for node in nodes if not node["output_ports"]}
    operation_node_ids = {node["logical_node_id"] for node in nodes
                          if node["input_ports"] and node["output_ports"]}
    _require(terminal_node_ids == {t["logical_node_id"] for t in logical_dag["terminals"]},
             "invalid_contract", "terminal nodes differ from declared terminals")

    sources = logical_dag["sources"]
    _require(bool(sources), "invalid_contract", "logical dag has no sources")
    _require(all(s["partition_scheme"]["kind"] == "single"
                 and s["partition_scheme"]["partition_count"] == 1 for s in sources),
             "unsupported_layout", "supported slices are single-partition single-scheme")
    scheme_versions = {s["partition_scheme"]["scheme_version"] for s in sources}
    _require(len(scheme_versions) == 1, "unsupported_layout", "mixed partition scheme versions")
    scheme_version = scheme_versions.pop()

    stage_by_id, stage_by_node, partition_owner = {}, {}, {}
    seen_occurrence = set()
    for stage in physical_plan["stages"]:
        _require(type(stage) is dict and set(stage) == _STAGE_KEYS,
                 "invalid_contract", "physical stage key set is unsupported")
        stage_id = stage["stage_id"]
        _check_identifier(stage_id, "stage_id")
        _require(stage_id not in stage_by_id, "invalid_contract", "duplicate stage_id")
        node = node_by_id.get(stage["logical_node_id"])
        _require(node is not None, "invalid_contract", "stage references an unknown logical node")
        _require(stage["occurrence_id"] == node["occurrence_id"],
                 "identity_mismatch", "stage occurrence differs from its logical node")
        _require(stage["occurrence_id"] not in seen_occurrence,
                 "invalid_contract", "duplicate stage occurrence")
        seen_occurrence.add(stage["occurrence_id"])
        _require(stage_id == identity([logical_dag["dag_id"], node["occurrence_id"]]),
                 "identity_mismatch", "stage_id does not bind dag/occurrence")
        partitions = stage["partitions"]
        _require(type(partitions) is list and len(partitions) == 1,
                 "unsupported_layout", "supported slices use exactly one partition per stage")
        partition = partitions[0]
        _check_identifier(partition, "partition")
        _require(partition not in partition_owner, "invalid_contract", "duplicate partition id")
        _require(partition == partition_id_for(node["logical_node_id"], scheme_version, 0),
                 "identity_mismatch", "partition id does not bind node/scheme")
        _require(stage["logical_node_id"] not in stage_by_node,
                 "invalid_contract", "logical node has multiple occurrence stages")
        partition_owner[partition] = stage_id
        stage_by_id[stage_id] = stage
        stage_by_node[stage["logical_node_id"]] = stage

    groups = physical_plan["worker_groups"]
    _require(bool(groups), "invalid_contract", "physical plan has no worker groups")
    group_by_id, owner = {}, {}
    for group in groups:
        group_id = group["group_id"]
        _require(group_id not in group_by_id, "invalid_contract", "duplicate worker group")
        _require(group["min_workers"] == group["initial_workers"] == group["max_workers"] == 1,
                 "unsupported_layout", "supported slices use a single worker per group")
        for stage_id in group["stage_refs"]:
            _require(stage_id in stage_by_id, "invalid_contract", "group references an unknown stage")
            _require(stage_id not in owner, "invalid_contract", "stage owned by multiple groups")
            owner[stage_id] = ("worker", group_id)
        group_by_id[group_id] = group

    driver_ids = doc["driver_stage_ids"]
    _require(type(driver_ids) is list, "invalid_contract", "driver_stage_ids must be a list")
    driver_set = set()
    for stage_id in driver_ids:
        _check_identifier(stage_id, "driver stage_id")
        _require(stage_id in stage_by_id, "invalid_contract", "driver references an unknown stage")
        _require(stage_id not in owner and stage_id not in driver_set,
                 "invalid_contract", "stage owned by both a worker and the driver")
        driver_set.add(stage_id)
        owner[stage_id] = ("driver", None)
    _require(set(owner) == set(stage_by_id), "invalid_contract", "stages are not fully owned")
    source_terminal_stages = {stage_by_node[nid]["stage_id"]
                              for nid in source_node_ids | terminal_node_ids
                              if nid in stage_by_node}
    _require(driver_set == source_terminal_stages,
             "invalid_contract", "driver must own exactly the source/terminal stages")

    profiles = doc["profiles"]
    _require(type(profiles) is dict, "invalid_contract", "profiles must be a map")
    for profile_ref, profile_document in profiles.items():
        _check_identifier(profile_ref, "profile_ref")
        _validate_profile(profile_ref, profile_document)
    for group in groups:
        _require(group["profile_ref"] in profiles,
                 "invalid_contract", "unknown profile_ref: " + str(group["profile_ref"]))

    programs = doc["programs"]
    _require(type(programs) is list and len(programs) == len(groups),
             "invalid_contract", "exactly one program per worker group is required")
    program_by_group = {}
    for program in programs:
        _require(type(program) is dict and set(program) == _PROGRAM_KEYS,
                 "invalid_contract", "program key set is unsupported")
        group_id = program["group_id"]
        _require(group_id in group_by_id and group_id not in program_by_group,
                 "invalid_contract", "program references an unknown or duplicate group")
        group = group_by_id[group_id]
        stage_ids = program["stage_ids"]
        _require(type(stage_ids) is list and all(type(s) is str for s in stage_ids),
                 "invalid_contract", "program stage_ids must be a list of ids")
        _require(len(set(stage_ids)) == len(stage_ids),
                 "invalid_contract", "duplicate program stage_id")
        _require(set(stage_ids) == set(group["stage_refs"]),
                 "invalid_contract", "program and group stage sets differ")
        for stage_id in stage_ids:
            _require(stage_by_id[stage_id]["logical_node_id"] in operation_node_ids,
                     "invalid_contract", "worker program must own operation stages only")
        _validate_program_ref(program["plan_ref"])
        sealed_operators = program["sealed_operators"]
        _require(type(sealed_operators) is list, "invalid_contract", "sealed_operators must be a list")
        for operator in sealed_operators:
            _require(type(operator) is dict and "logical_node_id" in operator
                     and set(operator) <= {"logical_node_id", "occurrence_id"},
                     "invalid_contract", "sealed operator key set is unsupported")
            _check_identifier(operator["logical_node_id"], "sealed operator logical_node_id")
            _require(operator["logical_node_id"] in operation_node_ids,
                     "invalid_contract", "sealed operator is not a program operation")
            if "occurrence_id" in operator:
                _check_identifier(operator["occurrence_id"], "sealed operator occurrence_id")
        operator_bindings = program["operator_bindings"]
        _require(type(operator_bindings) is list and len(operator_bindings) == len(sealed_operators),
                 "invalid_contract", "operator_bindings must cover sealed_operators once")
        seen_index, bound_stages = set(), set()
        for binding in operator_bindings:
            _require(type(binding) is dict and set(binding) == {"program_index", "stage_id"},
                     "invalid_contract", "operator binding key set is unsupported")
            index = binding["program_index"]
            _require(type(index) is int and 0 <= index < len(sealed_operators),
                     "invalid_contract", "program_index out of range")
            _require(index not in seen_index, "invalid_contract", "duplicate program_index")
            seen_index.add(index)
            stage_id = binding["stage_id"]
            _require(stage_id in set(stage_ids), "invalid_contract", "binding stage outside program")
            _require(stage_id not in bound_stages, "invalid_contract", "stage bound twice")
            bound_stages.add(stage_id)
            stage = stage_by_id[stage_id]
            operator = sealed_operators[index]
            _require(stage["logical_node_id"] == operator["logical_node_id"],
                     "identity_mismatch", "operator binding maps to a different logical node")
            if "occurrence_id" in operator:
                _require(stage["occurrence_id"] == operator["occurrence_id"],
                         "identity_mismatch", "operator binding occurrence differs")
        _require(seen_index == set(range(len(sealed_operators))),
                 "invalid_contract", "operator indices are not contiguous from 0")
        _require(bound_stages == set(stage_ids),
                 "invalid_contract", "program stages are not fully bound to operations")
        program_by_group[group_id] = program

    source_bindings = doc["source_bindings"]
    _require(type(source_bindings) is list, "invalid_contract", "source_bindings must be a list")
    expected_kind = _SLICE_SOURCE_KIND[supported_slice]
    bound_source_stages = set()
    for source_binding in source_bindings:
        _require(type(source_binding) is dict and set(source_binding) == {"stage_id", "kind", "ref"},
                 "invalid_contract", "source binding key set is unsupported")
        stage_id = source_binding["stage_id"]
        _require(stage_id in stage_by_id, "invalid_contract", "source binding references unknown stage")
        _require(stage_by_id[stage_id]["logical_node_id"] in source_node_ids,
                 "invalid_contract", "source binding on a non-source stage")
        _require(owner[stage_id][0] == "driver",
                 "invalid_contract", "source stage must be driver-owned")
        _require(stage_id not in bound_source_stages,
                 "invalid_contract", "duplicate source binding")
        bound_source_stages.add(stage_id)
        _require(source_binding["kind"] == expected_kind,
                 "invalid_contract", "source kind differs from the supported slice")
        ref = source_binding["ref"]
        _require(type(ref) is dict, "invalid_contract", "source binding ref must be a document")
        if source_binding["kind"] == "queue_declaration":
            _require(set(ref) == {"source_id", "source_identity"},
                     "invalid_contract", "queue declaration ref needs source_id/source_identity")
            _check_identifier(ref["source_id"], "queue source_id")
        try:
            document(ref)
        except RecoveryContractError as exc:
            raise PlanContractError("invalid_contract", "source binding ref invalid: " + str(exc))
    _require(bound_source_stages == {stage_by_node[nid]["stage_id"]
                                     for nid in source_node_ids if nid in stage_by_node},
             "invalid_contract", "source stages are not fully bound")

    channels = physical_plan["channels"]
    channel_by_id = {channel["channel_id"]: channel for channel in channels}
    _require(len(channel_by_id) == len(channels), "invalid_contract", "duplicate physical channel")
    edges = logical_dag["edges"]
    edge_by_id = {edge["edge_id"]: edge for edge in edges}
    _require(len(edge_by_id) == len(edges), "invalid_contract", "duplicate logical edge")
    channel_options = doc["channel_options"]
    _require(type(channel_options) is list, "invalid_contract", "channel_options must be a list")
    covered_edges, options_by_channel = {}, {}
    for options in channel_options:
        _require(type(options) is dict, "invalid_contract", "channel option must be a document")
        channel_id = options.get("channel_id")
        _require(channel_id in channel_by_id and channel_id not in options_by_channel,
                 "invalid_contract", "channel option references an unknown or duplicate channel")
        channel = channel_by_id[channel_id]
        routing = channel["routing"]
        allowed = {"channel_id", "logical_edge_ids", "queue_depth", "storage_bytes"}
        if routing == "hash":
            allowed.add("key_fields")
        if routing == "target":
            allowed.update({"target_field", "target_classes"})
        _require({"channel_id", "logical_edge_ids"} <= set(options) <= allowed,
                 "invalid_contract", "channel option key set is unsupported for its routing")
        if routing == "hash":
            _require("key_fields" in options, "invalid_contract", "hash routing needs key_fields")
        if routing == "target":
            _require("target_field" in options and "target_classes" in options,
                     "invalid_contract", "target routing needs target_field/target_classes")
        for name in ("queue_depth", "storage_bytes"):
            if name in options:
                _require(type(options[name]) is int and options[name] >= 1,
                         "invalid_contract", name + " must be a positive int")
        logical_edge_ids = options["logical_edge_ids"]
        _require(type(logical_edge_ids) is list and bool(logical_edge_ids),
                 "invalid_contract", "logical_edge_ids must be a nonempty list")
        producer_nodes = set()
        producer_union, consumer_union = set(), set()
        for edge_id in logical_edge_ids:
            _require(edge_id in edge_by_id, "invalid_contract", "channel option references unknown edge")
            _require(edge_id not in covered_edges, "invalid_contract", "logical edge covered twice")
            covered_edges[edge_id] = channel_id
            edge = edge_by_id[edge_id]
            producer_node = edge["from"]["logical_node_id"]
            consumer_node = edge["to"]["logical_node_id"]
            producer_nodes.add(producer_node)
            producer_stage = stage_by_node[producer_node]
            consumer_stage = stage_by_node[consumer_node]
            producer_union.update(producer_stage["partitions"])
            consumer_union.update(consumer_stage["partitions"])
            _require(set(producer_stage["partitions"]) <= set(channel["producer_partitions"]),
                     "invalid_contract", "producer endpoint partition is outside the channel")
            _require(set(consumer_stage["partitions"]) <= set(channel["consumer_partitions"]),
                     "invalid_contract", "consumer endpoint partition is outside the channel")
        _require(len(producer_nodes) == 1,
                 "invalid_contract", "joint routing must share one producer ancestor")
        _require(producer_union == set(channel["producer_partitions"]),
                 "invalid_contract", "channel producer partitions are not fully covered")
        _require(consumer_union == set(channel["consumer_partitions"]),
                 "invalid_contract", "channel consumer partitions are not fully covered")
        if len(logical_edge_ids) == 1:
            edge = edge_by_id[logical_edge_ids[0]]
            producer_stage = stage_by_node[edge["from"]["logical_node_id"]]
            consumer_stage = stage_by_node[edge["to"]["logical_node_id"]]
            _require(producer_stage["partitions"] == channel["producer_partitions"],
                     "invalid_contract", "producer partition order differs from the edge endpoint")
            _require(consumer_stage["partitions"] == channel["consumer_partitions"],
                     "invalid_contract", "consumer partition order differs from the edge endpoint")
        _validate_routing_options(channel, options)
        options_by_channel[channel_id] = options
    _require(set(options_by_channel) == set(channel_by_id),
             "invalid_contract", "channel_options must cover each physical channel exactly once")
    _require(set(covered_edges) == set(edge_by_id),
             "invalid_contract", "logical edges are not covered exactly once")

    _require(type(doc["checkpoint_rows"]) is int and doc["checkpoint_rows"] >= 1,
             "invalid_contract", "checkpoint_rows must be a positive int")

    if supported_slice in ("chain_v1", "queue_transform_v1"):
        _require(len(groups) == 1, "unsupported_layout", "chain slice uses one worker group")
        _require(len(source_node_ids) == 1 and len(terminal_node_ids) == 1,
                 "unsupported_layout", "chain slice has one source and one terminal")
        in_degree, out_degree = {}, {}
        for edge in edges:
            out_degree[edge["from"]["logical_node_id"]] = out_degree.get(edge["from"]["logical_node_id"], 0) + 1
            in_degree[edge["to"]["logical_node_id"]] = in_degree.get(edge["to"]["logical_node_id"], 0) + 1
        _require(all(count <= 1 for count in in_degree.values())
                 and all(count <= 1 for count in out_degree.values()),
                 "unsupported_layout", "chain slice must be a linear path")
        _require(all(channel["routing"] == "forward" for channel in channels),
                 "unsupported_layout", "chain slice channels must be forward")

    return {"stage_by_id": stage_by_id, "stage_by_node": stage_by_node,
            "partition_owner": partition_owner, "owner": owner,
            "group_by_id": group_by_id, "program_by_group": program_by_group,
            "options_by_channel": options_by_channel, "profiles": profiles}


def _validate_program_binding(binding, execution_stage_ids):
    """Validate one v2 ProgramBinding and return the full-DAG stages it binds.

    sealed_operators stay in the sub-program's linear LOCAL order (the namespace
    verify_loaded_plan expects); operator_bindings map local indices to full-DAG
    occurrence stages. The local logical_node_id is deliberately not cross-checked
    against the full DAG. The original writer assigned each local identity.
    """
    _require(type(binding) is dict and _PROGRAM_BINDING_KEYS <= set(binding)
             <= _PROGRAM_BINDING_KEYS | {'source_stage_id'},
             "invalid_contract", "program binding key set is unsupported")
    _check_identifier(binding["program_id"], "program_id")
    input_path = binding["input_path"]
    _require(type(input_path) is list
             and all(type(part) is str or (type(part) is int and part >= 0) for part in input_path),
             "invalid_contract", "input_path must be a list of fields or nonnegative indices")
    _validate_program_ref(binding["plan_ref"])
    _check_identifier(binding["source_identity_ref"], "source_identity_ref")
    sealed_operators = binding["sealed_operators"]
    _require(type(sealed_operators) is list, "invalid_contract", "sealed_operators must be a list")
    for operator in sealed_operators:
        _require(type(operator) is dict and "logical_node_id" in operator
                 and set(operator) <= {"logical_node_id", "occurrence_id"},
                 "invalid_contract", "sealed operator key set is unsupported")
        _check_identifier(operator["logical_node_id"], "sealed operator logical_node_id")
        if "occurrence_id" in operator:
            _check_identifier(operator["occurrence_id"], "sealed operator occurrence_id")
    operator_bindings = binding["operator_bindings"]
    _require(type(operator_bindings) is list and len(operator_bindings) == len(sealed_operators),
             "invalid_contract", "operator_bindings must cover sealed_operators once")
    seen_index, bound_stages = set(), set()
    for item in operator_bindings:
        _require(type(item) is dict and set(item) == {"program_index", "stage_id"},
                 "invalid_contract", "operator binding key set is unsupported")
        index = item["program_index"]
        _require(type(index) is int and 0 <= index < len(sealed_operators),
                 "invalid_contract", "program_index out of range")
        _require(index not in seen_index, "invalid_contract", "duplicate program_index")
        seen_index.add(index)
        stage_id = item["stage_id"]
        _require(stage_id in execution_stage_ids, "invalid_contract", "binding stage outside execution")
        _require(stage_id not in bound_stages, "invalid_contract", "stage bound twice")
        bound_stages.add(stage_id)
    _require(seen_index == set(range(len(sealed_operators))),
             "invalid_contract", "operator indices are not contiguous from 0")
    if 'source_stage_id' in binding:
        source_stage = binding['source_stage_id']
        _check_identifier(source_stage, 'source_stage_id')
        _require(source_stage in execution_stage_ids and source_stage not in bound_stages,
                 'invalid_contract', 'composite source stage is outside execution or duplicated')
        bound_stages.add(source_stage)
    return bound_stages


def _validate_bindings_v2(doc, logical_dag, physical_plan, manifest_ref):
    """Structural + relational validation for execution_bindings.v2.

    Additive to v1: the v1 path is never relaxed. The first v2 batch is
    driver-only (native_driver_v1); a v2 worker execution is explicitly refused
    until implemented. stage_limits derive solely from physical.stages[].budgets.
    """
    _require(set(doc) == _BINDINGS_KEYS_V2,
             "invalid_contract", "v2 execution bindings key set is unsupported")
    supported_slice = doc["supported_slice"]
    _require(type(supported_slice) is str and supported_slice in SUPPORTED_SLICES_V2,
             "unsupported_layout", "unsupported v2 execution slice: " + str(supported_slice))
    for key in ("run_id", "action_id", "dag_id", "plan_id"):
        _check_identifier(doc[key], key)

    _validate_manifest_ref(doc["artifact_manifest_ref"])
    _require(doc["artifact_manifest_ref"] == manifest_ref == physical_plan["artifact_manifest_ref"],
             "identity_mismatch", "artifact manifest reference differs")
    _require(doc["dag_id"] == logical_dag["dag_id"] == physical_plan["logical_dag_ref"]["dag_id"],
             "identity_mismatch", "dag_id differs across sealed inputs")
    _require(doc["plan_id"] == physical_plan["plan_id"],
             "identity_mismatch", "plan_id differs across sealed inputs")
    _require(logical_dag["action_id"] == doc["action_id"],
             "identity_mismatch", "logical dag action_id differs from bindings")
    _require(physical_plan["logical_dag_ref"]["sha256"] == identity(logical_dag),
             "identity_mismatch", "physical logical_dag_ref does not bind the dag")

    nodes = logical_dag["nodes"]
    node_by_id = {node["logical_node_id"]: node for node in nodes}
    source_node_ids = {node["logical_node_id"] for node in nodes if not node["input_ports"]}
    terminal_node_ids = {node["logical_node_id"] for node in nodes if not node["output_ports"]}
    operation_node_ids = {node["logical_node_id"] for node in nodes
                          if node["input_ports"] and node["output_ports"]}
    _require(terminal_node_ids == {t["logical_node_id"] for t in logical_dag["terminals"]},
             "invalid_contract", "terminal nodes differ from declared terminals")

    sources = logical_dag["sources"]
    _require(bool(sources), "invalid_contract", "logical dag has no sources")
    _require(all(s["partition_scheme"]["kind"] == "single"
                 and s["partition_scheme"]["partition_count"] == 1 for s in sources),
             "unsupported_layout", "supported slices are single-partition single-scheme")
    scheme_versions = {s["partition_scheme"]["scheme_version"] for s in sources}
    _require(len(scheme_versions) == 1, "unsupported_layout", "mixed partition scheme versions")
    scheme_version = scheme_versions.pop()

    stage_by_id, stage_by_node, partition_owner = {}, {}, {}
    seen_occurrence = set()
    for stage in physical_plan["stages"]:
        _require(type(stage) is dict and set(stage) == _STAGE_KEYS,
                 "invalid_contract", "physical stage key set is unsupported")
        stage_id = stage["stage_id"]
        _check_identifier(stage_id, "stage_id")
        _require(stage_id not in stage_by_id, "invalid_contract", "duplicate stage_id")
        node = node_by_id.get(stage["logical_node_id"])
        _require(node is not None, "invalid_contract", "stage references an unknown logical node")
        _require(stage["occurrence_id"] == node["occurrence_id"],
                 "identity_mismatch", "stage occurrence differs from its logical node")
        _require(stage["occurrence_id"] not in seen_occurrence,
                 "invalid_contract", "duplicate stage occurrence")
        seen_occurrence.add(stage["occurrence_id"])
        _require(stage_id == identity([logical_dag["dag_id"], node["occurrence_id"]]),
                 "identity_mismatch", "stage_id does not bind dag/occurrence")
        partitions = stage["partitions"]
        _require(type(partitions) is list and len(partitions) == 1,
                 "unsupported_layout", "supported slices use exactly one partition per stage")
        partition = partitions[0]
        _check_identifier(partition, "partition")
        _require(partition not in partition_owner, "invalid_contract", "duplicate partition id")
        _require(partition == partition_id_for(node["logical_node_id"], scheme_version, 0),
                 "identity_mismatch", "partition id does not bind node/scheme")
        _require(stage["logical_node_id"] not in stage_by_node,
                 "invalid_contract", "logical node has multiple occurrence stages")
        budgets = stage["budgets"]
        _require(type(budgets) is dict and set(budgets) == _STAGE_BUDGET_KEYS
                 and all(type(value) is int and value >= 1 for value in budgets.values()),
                 "invalid_contract", "stage budgets need positive row/byte bounds")
        partition_owner[partition] = stage_id
        stage_by_id[stage_id] = stage
        stage_by_node[stage["logical_node_id"]] = stage

    groups = physical_plan["worker_groups"]
    _require(type(groups) is list and not groups,
             "unsupported_layout", "native_driver_v1 is driver-only; worker groups are not supported yet")

    profiles = doc["profiles"]
    _require(type(profiles) is dict, "invalid_contract", "profiles must be a map")
    for profile_ref, profile_document in profiles.items():
        _check_identifier(profile_ref, "profile_ref")
        _validate_profile(profile_ref, profile_document)

    executions = doc["executions"]
    _require(type(executions) is list and bool(executions),
             "invalid_contract", "v2 bindings need at least one execution")
    owner, execution_by_id, operation_stage_owner = {}, {}, {}
    for entry in executions:
        _require(type(entry) is dict and set(entry) == _EXECUTION_KEYS,
                 "invalid_contract", "execution key set is unsupported")
        execution_id = entry["execution_id"]
        _check_identifier(execution_id, "execution_id")
        _require(execution_id not in execution_by_id, "invalid_contract", "duplicate execution_id")
        _require(entry["placement"] in _PLACEMENTS, "invalid_contract", "unknown execution placement")
        _require(entry["placement"] == "driver",
                 "unsupported_layout", "v2 worker executions are not implemented; driver only")
        _require(entry["group_id"] is None,
                 "invalid_contract", "driver execution must not carry a worker group_id")
        _require(entry["backend"] in _BACKENDS, "invalid_contract", "unknown execution backend")
        _require(entry["recovery"] in _RECOVERY_GUARANTEES,
                 "invalid_contract", "unknown recovery guarantee")
        _require(entry["profile_ref"] in profiles,
                 "invalid_contract", "unknown profile_ref: " + str(entry["profile_ref"]))
        stage_ids = entry["stage_ids"]
        _require(type(stage_ids) is list and all(type(s) is str for s in stage_ids),
                 "invalid_contract", "execution stage_ids must be a list of ids")
        _require(len(set(stage_ids)) == len(stage_ids), "invalid_contract", "duplicate execution stage_id")
        execution_stage_set = set()
        for stage_id in stage_ids:
            _require(stage_id in stage_by_id, "invalid_contract", "execution references an unknown stage")
            _require(stage_by_id[stage_id]["logical_node_id"] in operation_node_ids,
                     "invalid_contract", "driver execution must own operation stages only")
            _require(stage_id not in operation_stage_owner,
                     "invalid_contract", "operation stage owned by multiple executions")
            operation_stage_owner[stage_id] = execution_id
            execution_stage_set.add(stage_id)
            owner[stage_id] = ("driver", None)
        programs = entry["programs"]
        _require(type(programs) is list, "invalid_contract", "execution programs must be a list")
        bound_stages, program_ids = set(), set()
        for binding in programs:
            bound = _validate_program_binding(binding, execution_stage_set)
            _require(binding["program_id"] not in program_ids,
                     "invalid_contract", "duplicate program_id in execution")
            program_ids.add(binding["program_id"])
            _require(not (bound & bound_stages), "invalid_contract", "stage bound by multiple programs")
            bound_stages |= bound
        _require(bound_stages == execution_stage_set,
                 "invalid_contract", "execution stages are not fully bound to programs")
        execution_by_id[execution_id] = entry

    _require(set(operation_stage_owner) == {stage_by_node[nid]["stage_id"]
                                            for nid in operation_node_ids if nid in stage_by_node},
             "invalid_contract", "operation stages are not fully owned by executions")

    driver_ids = doc["driver_stage_ids"]
    _require(type(driver_ids) is list, "invalid_contract", "driver_stage_ids must be a list")
    driver_set = set()
    for stage_id in driver_ids:
        _check_identifier(stage_id, "driver stage_id")
        _require(stage_id in stage_by_id, "invalid_contract", "driver references an unknown stage")
        _require(stage_id not in driver_set, "invalid_contract", "duplicate driver stage_id")
        driver_set.add(stage_id)
        owner[stage_id] = ("driver", None)
    source_terminal_stages = {stage_by_node[nid]["stage_id"]
                              for nid in source_node_ids | terminal_node_ids
                              if nid in stage_by_node}
    _require(driver_set == source_terminal_stages | set(operation_stage_owner),
             "invalid_contract", "driver must own exactly the source/terminal and driver-execution stages")
    _require(set(owner) == set(stage_by_id), "invalid_contract", "stages are not fully owned")

    source_bindings = doc["source_bindings"]
    _require(type(source_bindings) is list, "invalid_contract", "source_bindings must be a list")
    expected_kind = _V2_SOURCE_KIND[supported_slice]
    bound_source_stages = set()
    for source_binding in source_bindings:
        _require(type(source_binding) is dict and set(source_binding) == {"stage_id", "kind", "ref"},
                 "invalid_contract", "source binding key set is unsupported")
        stage_id = source_binding["stage_id"]
        _require(stage_id in stage_by_id, "invalid_contract", "source binding references unknown stage")
        _require(stage_by_id[stage_id]["logical_node_id"] in source_node_ids,
                 "invalid_contract", "source binding on a non-source stage")
        _require(owner[stage_id][0] == "driver",
                 "invalid_contract", "source stage must be driver-owned")
        _require(stage_id not in bound_source_stages, "invalid_contract", "duplicate source binding")
        bound_source_stages.add(stage_id)
        _require(source_binding["kind"] == expected_kind,
                 "invalid_contract", "source kind differs from the supported slice")
        ref = source_binding["ref"]
        _require(type(ref) is dict, "invalid_contract", "source binding ref must be a document")
        try:
            document(ref)
        except RecoveryContractError as exc:
            raise PlanContractError("invalid_contract", "source binding ref invalid: " + str(exc))
    _require(bound_source_stages == {stage_by_node[nid]["stage_id"]
                                     for nid in source_node_ids if nid in stage_by_node},
             "invalid_contract", "source stages are not fully bound")

    channels = physical_plan["channels"]
    channel_by_id = {channel["channel_id"]: channel for channel in channels}
    _require(len(channel_by_id) == len(channels), "invalid_contract", "duplicate physical channel")
    edges = logical_dag["edges"]
    edge_by_id = {edge["edge_id"]: edge for edge in edges}
    _require(len(edge_by_id) == len(edges), "invalid_contract", "duplicate logical edge")
    channel_options = doc["channel_options"]
    _require(type(channel_options) is list, "invalid_contract", "channel_options must be a list")
    covered_edges, options_by_channel = {}, {}
    for options in channel_options:
        _require(type(options) is dict, "invalid_contract", "channel option must be a document")
        channel_id = options.get("channel_id")
        _require(channel_id in channel_by_id and channel_id not in options_by_channel,
                 "invalid_contract", "channel option references an unknown or duplicate channel")
        channel = channel_by_id[channel_id]
        routing = channel["routing"]
        allowed = {"channel_id", "logical_edge_ids", "queue_depth", "storage_bytes"}
        if routing == "hash":
            allowed.add("key_fields")
        if routing == "target":
            allowed.update({"target_field", "target_classes"})
        _require({"channel_id", "logical_edge_ids"} <= set(options) <= allowed,
                 "invalid_contract", "channel option key set is unsupported for its routing")
        if routing == "hash":
            _require("key_fields" in options, "invalid_contract", "hash routing needs key_fields")
        if routing == "target":
            _require("target_field" in options and "target_classes" in options,
                     "invalid_contract", "target routing needs target_field/target_classes")
        for name in ("queue_depth", "storage_bytes"):
            if name in options:
                _require(type(options[name]) is int and options[name] >= 1,
                         "invalid_contract", name + " must be a positive int")
        logical_edge_ids = options["logical_edge_ids"]
        _require(type(logical_edge_ids) is list and bool(logical_edge_ids),
                 "invalid_contract", "logical_edge_ids must be a nonempty list")
        producer_nodes = set()
        producer_union, consumer_union = set(), set()
        for edge_id in logical_edge_ids:
            _require(edge_id in edge_by_id, "invalid_contract", "channel option references unknown edge")
            _require(edge_id not in covered_edges, "invalid_contract", "logical edge covered twice")
            covered_edges[edge_id] = channel_id
            edge = edge_by_id[edge_id]
            producer_node = edge["from"]["logical_node_id"]
            consumer_node = edge["to"]["logical_node_id"]
            producer_nodes.add(producer_node)
            producer_stage = stage_by_node[producer_node]
            consumer_stage = stage_by_node[consumer_node]
            producer_union.update(producer_stage["partitions"])
            consumer_union.update(consumer_stage["partitions"])
            _require(set(producer_stage["partitions"]) <= set(channel["producer_partitions"]),
                     "invalid_contract", "producer endpoint partition is outside the channel")
            _require(set(consumer_stage["partitions"]) <= set(channel["consumer_partitions"]),
                     "invalid_contract", "consumer endpoint partition is outside the channel")
        _require(len(producer_nodes) == 1,
                 "invalid_contract", "joint routing must share one producer ancestor")
        _require(producer_union == set(channel["producer_partitions"]),
                 "invalid_contract", "channel producer partitions are not fully covered")
        _require(consumer_union == set(channel["consumer_partitions"]),
                 "invalid_contract", "channel consumer partitions are not fully covered")
        if len(logical_edge_ids) == 1:
            edge = edge_by_id[logical_edge_ids[0]]
            producer_stage = stage_by_node[edge["from"]["logical_node_id"]]
            consumer_stage = stage_by_node[edge["to"]["logical_node_id"]]
            _require(producer_stage["partitions"] == channel["producer_partitions"],
                     "invalid_contract", "producer partition order differs from the edge endpoint")
            _require(consumer_stage["partitions"] == channel["consumer_partitions"],
                     "invalid_contract", "consumer partition order differs from the edge endpoint")
        _validate_routing_options(channel, options)
        options_by_channel[channel_id] = options
    _require(set(options_by_channel) == set(channel_by_id),
             "invalid_contract", "channel_options must cover each physical channel exactly once")
    _require(set(covered_edges) == set(edge_by_id),
             "invalid_contract", "logical edges are not covered exactly once")

    _require(type(doc["checkpoint_rows"]) is int and doc["checkpoint_rows"] >= 1,
             "invalid_contract", "checkpoint_rows must be a positive int")

    return {"stage_by_id": stage_by_id, "stage_by_node": stage_by_node,
            "partition_owner": partition_owner, "owner": owner,
            "group_by_id": {}, "program_by_group": {},
            "options_by_channel": options_by_channel, "profiles": profiles,
            "executions": executions}


def _ordered_stage_ids(partitions, partition_owner):
    ordered = []
    for partition in partitions:
        stage_id = partition_owner[partition]
        if stage_id not in ordered:
            ordered.append(stage_id)
    return tuple(ordered)


def interpret_physical_plan(logical_dag, physical_plan, *, bindings, manifest_ref) -> ExecutionLayout:
    """Explain sealed inputs as a read-only layout; no I/O, no business calls."""
    _require(type(logical_dag) is dict, "invalid_contract", "logical_dag must be a document")
    _require(type(physical_plan) is dict, "invalid_contract", "physical_plan must be a document")
    _require(type(bindings) is dict and set(bindings) == {"ref", "document"},
             "invalid_contract", "bindings must be the {ref, document} bundle")
    _validate_manifest_ref(manifest_ref)
    _validate_artifact_ref(bindings["ref"], "bindings")
    try:
        validate_logical_dag(logical_dag)
        validate_physical_execution_plan(physical_plan)
    except (ValueError, RecoveryContractError) as exc:
        raise PlanContractError("invalid_contract", str(exc))
    doc = bindings["document"]
    ctx = _validate_bindings(doc, logical_dag, physical_plan, manifest_ref)
    stage_by_id = ctx["stage_by_id"]
    partition_owner = ctx["partition_owner"]
    owner = ctx["owner"]

    stages = tuple(_freeze(stage) for stage in physical_plan["stages"])
    stage_owners = MappingProxyType({
        stage_id: _freeze({"kind": "worker", "group_id": group_id} if kind == "worker"
                          else {"kind": "driver"})
        for stage_id, (kind, group_id) in owner.items()})

    workers = []
    for group in physical_plan["worker_groups"]:
        group_id = group["group_id"]
        program = ctx["program_by_group"][group_id]
        partition_ids = tuple(partition for stage_id in program["stage_ids"]
                              for partition in stage_by_id[stage_id]["partitions"])
        workers.append(WorkerPlacement(
            group_id=group_id,
            stage_ids=tuple(program["stage_ids"]),
            partition_ids=partition_ids,
            profile=_freeze(ctx["profiles"][group["profile_ref"]]),
            program_ref=_freeze(program["plan_ref"]),
            sealed_operators=tuple(_freeze(op) for op in program["sealed_operators"]),
            operator_bindings=tuple(_freeze(b) for b in program["operator_bindings"])))

    channels = []
    for channel in physical_plan["channels"]:
        producer_stages = _ordered_stage_ids(channel["producer_partitions"], partition_owner)
        consumer_stages = _ordered_stage_ids(channel["consumer_partitions"], partition_owner)
        endpoint_owners = {owner[stage_id] for stage_id in producer_stages + consumer_stages}
        in_process = (len(endpoint_owners) == 1
                      and next(iter(endpoint_owners))[0] == "worker")
        channels.append(ChannelBinding(
            channel_id=channel["channel_id"],
            producer_stage_ids=producer_stages,
            consumer_stage_ids=consumer_stages,
            channel_plan=_freeze(channel),
            options=_freeze(ctx["options_by_channel"][channel["channel_id"]]),
            in_process=in_process))

    recovery = physical_plan["recovery"]
    checkpoint = CheckpointPolicy(
        checkpoint_rows=doc["checkpoint_rows"],
        interval_ms=recovery["checkpoint_interval_ms"],
        unconfirmed_rows=recovery["unconfirmed_rows"],
        unconfirmed_bytes=recovery["unconfirmed_bytes"])

    # v1 seals no backend/recovery authority, so it derives no execution
    # placements (fabricating them would invent capability v1 never bound); its
    # workers tuple and carrier are unchanged. v2 binds explicit driver/worker
    # executions; stage_limits come solely from the physical stage budgets.
    executions = ()
    if doc["schema"] == EXECUTION_BINDINGS_SCHEMA_V2:
        placements = []
        for entry in ctx["executions"]:
            stage_limits = MappingProxyType({
                stage_id: _freeze(ctx["stage_by_id"][stage_id]["budgets"])
                for stage_id in entry["stage_ids"]})
            placements.append(ExecutionPlacement(
                execution_id=entry["execution_id"],
                placement=entry["placement"],
                group_id=entry["group_id"],
                backend=entry["backend"],
                stage_ids=tuple(entry["stage_ids"]),
                profile=_freeze(ctx["profiles"][entry["profile_ref"]]),
                programs=tuple(_freeze(program) for program in entry["programs"]),
                stage_limits=stage_limits,
                recovery=entry["recovery"]))
        executions = tuple(placements)

    return ExecutionLayout(
        dag_id=logical_dag["dag_id"],
        plan_id=physical_plan["plan_id"],
        plan_revision=physical_plan["plan_revision"],
        artifact_manifest_ref=_freeze(manifest_ref),
        bindings_ref=_freeze(bindings["ref"]),
        supported_slice=doc["supported_slice"],
        stages=stages,
        stage_owners=stage_owners,
        source_bindings=tuple(_freeze(sb) for sb in doc["source_bindings"]),
        workers=tuple(workers),
        channels=tuple(channels),
        checkpoint=checkpoint,
        executions=executions)


def _group_channel_ids(layout, placement):
    """Physical channel ids a worker actually owns: cross-worker edges only.

    Driver edges ride the existing input/commit carriers and in-process edges
    ride run_stream, so neither becomes a worker socket channel.
    """
    group_partitions = set(placement.partition_ids)
    inputs, outputs = set(), set()
    for channel in layout.channels:
        if channel.in_process:
            continue
        producer_kinds = {layout.stage_owners[sid]["kind"] for sid in channel.producer_stage_ids}
        consumer_kinds = {layout.stage_owners[sid]["kind"] for sid in channel.consumer_stage_ids}
        if "driver" in producer_kinds or "driver" in consumer_kinds:
            continue
        for physical in materialize_channels(to_wire(channel.channel_plan)):
            channel_id = physical["channel_id"]
            producer_here = physical["producer_partition"] in group_partitions
            consumer_here = physical["consumer_partition"] in group_partitions
            if consumer_here and not producer_here:
                inputs.add(channel_id)
            if producer_here and not consumer_here:
                outputs.add(channel_id)
    return inputs, outputs


def validate_worker_assignment(layout, assignment, *, group_id) -> WorkerPlacement:
    """Check a K3 assignment against the sealed layout before business execution."""
    _require(isinstance(layout, ExecutionLayout), "invalid_assignment", "layout is not an ExecutionLayout")
    placement = next((worker for worker in layout.workers if worker.group_id == group_id), None)
    _require(placement is not None, "invalid_assignment", "unknown worker group: " + str(group_id))
    _require(group_id is not None, "invalid_assignment", "a driver placement cannot be validated as a worker")
    for execution in layout.executions:
        if execution.placement == "driver":
            _require(group_id not in (execution.execution_id, execution.group_id),
                     "invalid_assignment", "a driver execution cannot masquerade as a worker")
    try:
        validate_assignment(assignment)
    except (ProtocolError, RecoveryContractError, ValueError) as exc:
        raise PlanContractError("invalid_assignment", str(exc))
    _require(assignment["artifact_manifest_ref"] == to_wire(layout.artifact_manifest_ref),
             "invalid_assignment", "assignment manifest differs from the layout")
    _require(assignment["plan_revision"] == layout.plan_revision
             and assignment["expected_plan_revision"] == layout.plan_revision,
             "invalid_assignment", "assignment plan revision differs from the layout")
    stages = assignment["stages"]
    _require(len(set(stages)) == len(stages) and set(stages) == set(placement.stage_ids),
             "invalid_assignment", "assignment stages differ from the worker placement")
    partitions = assignment["partitions"]
    _require(len(set(partitions)) == len(partitions) and set(partitions) == set(placement.partition_ids),
             "invalid_assignment", "assignment partitions differ from the worker placement")
    expected_inputs, expected_outputs = _group_channel_ids(layout, placement)
    input_ids = [item["channel_id"] for item in assignment["channels"]["inputs"]]
    output_ids = list(assignment["channels"]["outputs"])
    _require(len(set(input_ids)) == len(input_ids) and set(input_ids) == expected_inputs,
             "invalid_assignment", "assignment input channels differ from the layout")
    _require(len(set(output_ids)) == len(output_ids) and set(output_ids) == expected_outputs,
             "invalid_assignment", "assignment output channels differ from the layout")
    return placement
