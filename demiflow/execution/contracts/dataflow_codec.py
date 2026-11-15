"""Closed v3 wire codec and canonical identity digests (C01; cards C001-C005).

The wire is a closed whitelist of persistent frozen DTOs. Top-level frames are
``{"schema": "demiflow.runtime_wire.v3", "type": <fixed name>, "payload": {...}}``
with explicit closed fields only; unknown versions, unknown fields, runtime
service objects, arbitrary classes, ``__reduce__`` payloads, NaN/Infinity and
bool-as-int are rejected. Depth/byte/element budgets are enforced before the
output is allocated. Identity formulas (IDENTITY.md) live here exactly once:
``compute_graph_digest`` for dag_id and ``compute_physical_digest`` for plan_id.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Mapping

from demiflow.data.datasink import WriteResult
from demiflow.resources.contracts import ResourceProfile

from .dataflow import (
    ActionProgram,
    AggregateState,
    ArtifactCommand,
    ArtifactRef,
    ArtifactReply,
    Bounds,
    CapabilityReport,
    CompiledDataflow,
    CompletedControl,
    ControlReply,
    CreditLease,
    CutContribution,
    CutRequest,
    DataLocation,
    DataflowGraph,
    DomainProof,
    EffectCommand,
    EffectReply,
    ExchangeSpec,
    ExecutionStatus,
    FiniteScope,
    GraphEdge,
    GraphNode,
    HistoricalBinding,
    HostDescriptor,
    InputPort,
    KernelSpec,
    NativeJournalCommand,
    NativeJournalReply,
    NodeObservation,
    OperatorDefinition,
    PhysicalDataflow,
    ProducerCompletion,
    QueueBinding,
    QueueCommand,
    QueueReply,
    ReadyWork,
    Record,
    BlockPartition,
    RecoveryImage,
    ResidencyPlan,
    RoutedRecord,
    RuntimePolicy,
    ScheduleChoice,
    SealedExecution,
    SourceCursor,
    SourceDefinition,
    SourcePoll,
    SourceSnapshot,
    SourceSplit,
    StageSpec,
    TerminalDefinition,
    TerminalResult,
    WorkerAssignment,
    WorkerEvent,
    WorkerLease,
    WorkerStep,
    _artifact_key,
)
from .dataflow_errors import DATAFLOW_ERROR_CODES, DataflowError

WIRE_SCHEMA = "demiflow.runtime_wire.v3"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENT = re.compile(r"[A-Za-z0-9_.:\-]{1,128}\Z")
_DECIMAL = re.compile(r"[0-9]{1,20}\Z")
_WIRE_MAX_NODES = 65536

_PROVIDERS = frozenset({"local", "ray_core"})
_MODES = frozenset({"memory_incremental", "durable_incremental", "finite_sealed"})
_RECOVERIES = frozenset({"checkpoint", "final_commit", "delivery_only", "attempt_only"})
_NODE_KINDS = frozenset({"source", "transform", "relational", "terminal"})
_INPUT_REQUIREMENTS = frozenset({"incremental", "finite_complete"})
_ORDERINGS = frozenset({"none", "source_partition", "key_sequence", "global"})
_KERNEL_STATES = frozenset({"none", "auto_json", "adapter", "nonrecoverable"})
_KERNEL_EFFECTS = frozenset({"pure", "journaled", "unknown_replay_forbidden"})
_RESOURCE_ROLES = frozenset({"cpu", "gpu", "remote_api", "io"})
_REPLAYS = frozenset({"seek", "durable_spool", "lease_queue", "attempt_only"})
_ROUTINGS = frozenset({"forward", "hash", "rebalance", "broadcast", "target"})
_DURABILITIES = frozenset({"ephemeral", "durable"})
_EFFECT_KINDS = frozenset({"lookup", "reserve", "mark_sent", "confirm", "unknown", "release_unsent"})
_EFFECT_STATUSES = frozenset({"absent", "reserved", "sent", "confirmed", "unknown", "unsent"})
_EVENT_KINDS = frozenset({
    "ready", "progress", "cut", "effect", "artifact", "reserve", "read_action",
    "read_configuration", "restrict_recovery", "check_grant", "queue", "native_request", "failure", "eoi", "closed", "data", "ack", "idle", "terminal",
})
_COMPLETIONS = frozenset({"source_exhausted", "terminal_satisfied", "cancelled", "incomplete"})
_PRODUCER_STATUSES = frozenset({"succeeded", "failed", "cancelled", "unknown"})
_POLL_STATUSES = frozenset({"record", "pending", "eoi"})
_ARTIFACT_KINDS = frozenset({"pin", "unpin", "inspect", "reserve_staging", "begin_staging", "prepare_staging", "grow_staging", "settle_staging", "release_staging"})
_REPLY_STATUSES = frozenset({"ok", "error", "pending"})
_SCOPE_CONDITIONS = frozenset({"input_eoi", "terminal_limit", "queue_drain_declared"})
_QUEUE_MODES = frozenset({"sink", "explicit", "raw_fetch", "producer", "native_search"})
_QUEUE_OPS = frozenset({"poll", "acquire", "finalize", "status", "search"})
_QUEUE_STATUSES = frozenset({"record", "pending", "eoi", "confirmed", "unknown"})
_JOURNAL_OPS = frozenset({
    "lookup", "reserve", "reserve_http_retry", "response", "failed",
    "references", "media_references", "bind_membership", "stage_image", "offline_lookup", "stream_lance", "batch_membership",
})
_EXECUTION_STATES = frozenset({"preparing", "running", "draining", "succeeded", "failed", "cancelled"})


def _err(message: str) -> DataflowError:
    return DataflowError("INVALID_CONTRACT", message)


def _budget_err(message: str) -> DataflowError:
    return DataflowError("BUDGET_EXCEEDED", message)

# Field specs: (kind, *args). The closed whitelist of persistent wire DTOs.
# In-process declarations/services (ActionRequest, RuntimeServices, DriverServices,
# ProgramDeclaration, CompiledDataflow, NativeOperation, Native*View, LoadedProgram,
# ActionProgram, PlanInputBinding), bytes-bearing EncodedRecord and every Protocol
# are deliberately absent and cannot be encoded.
_S = ("sha256",)
_OS = ("opt_sha256",)
_I = ("ident",)
_OI = ("opt_ident",)
_B = ("bool",)
_OB = ("opt_bool",)
_J = ("json",)
_JM = ("jmap",)
_OJM = ("opt_jmap",)
_R = ("ref",)
_OR = ("opt_ref",)
_RS = ("refs",)
_STRS = ("strs",)
_IM = ("imap",)
_FM = ("fmap",)
_RM = ("refmap",)
_JMM = ("jmapmap",)
_P = ("profile",)
_PS = ("profiles",)


def _i(minimum=0):
    return ("int", minimum)


def _oi(minimum=0):
    return ("opt_int", minimum)


def _t(maxlen=256):
    return ("text", maxlen)


def _ot(maxlen=256):
    return ("opt_text", maxlen)


def _e(values):
    return ("enum", values)


def _dto(name):
    return ("dto", name)


def _odto(name):
    return ("opt_dto", name)


def _dtos(name):
    return ("dtos", name)


_WIRE_TYPES = {
    "ArtifactRef": (ArtifactRef, (
        ("digest", _S), ("size_bytes", _i()), ("store_id", _I), ("key", ("key",)), ("media_type", _t()),
    )),
    "Bounds": (Bounds, tuple((name, _i(1)) for name in (
        "max_nodes", "max_edges", "max_partitions", "max_depth", "max_control_bytes",
        "max_row_bytes", "max_batch_bytes", "max_resident_bytes", "max_spill_bytes",
        "max_state_bytes", "max_pending_events", "max_open_files", "max_output_rows", "timeout_ms",
    ))),
    "InputPort": (InputPort, (
        ("name", _I), ("requirement", _e(_INPUT_REQUIREMENTS)), ("ordering", _e(_ORDERINGS)), ("schema_ref", _OR),
    )),
    "KernelSpec": (KernelSpec, (
        ("kind", _I), ("implementation_ref", _OR), ("configuration", _JM), ("inputs", _dtos("InputPort")),
        ("state", _e(_KERNEL_STATES)), ("effect", _e(_KERNEL_EFFECTS)), ("resource_role", _e(_RESOURCE_ROLES)),
        ("profile", _P), ("output_bound_bytes", _i()), ("mergeable", _B),
    )),
    "GraphNode": (GraphNode, (
        ("occurrence_id", _S), ("semantic_id", _S), ("kind", _e(_NODE_KINDS)), ("kernel", _dto("KernelSpec")),
        ("program_ref", _OR), ("shared_token", ("decimal",)),
    )),
    "GraphEdge": (GraphEdge, (
        ("edge_id", _S), ("producer", _S), ("output_port", _I), ("consumer", _S), ("input_port", _I),
    )),
    "DataflowGraph": (DataflowGraph, (
        ("schema", ("lit", "demiflow.dataflow_graph.v3")), ("action_id", _I), ("dag_id", _S),
        ("nodes", _dtos("GraphNode")), ("edges", _dtos("GraphEdge")), ("terminals", _STRS), ("sources", _STRS),
    )),
    "SourceSnapshot": (SourceSnapshot, (
        ("source_id", _S), ("kind", _I), ("immutable_identity", _JM), ("finite", _B),
        ("replay", _e(_REPLAYS)), ("manifest_ref", _OR),
    )),
    "SourceSplit": (SourceSplit, (
        ("split_id", _S), ("source_id", _S), ("locator", _JM), ("start_cursor", _JM),
        ("estimated_bytes", _oi()), ("required_domain", _I),
    )),
    "SourceCursor": (SourceCursor, (
        ("split_id", _S), ("offset", _JM), ("next_sequence", _i()), ("end_of_input", _B),
        ("lease_ref", _OR), ("pending_refs", _RS),
    )),
    "BlockPartition": (BlockPartition, (("occurrence_id", _S), ("index", _i()), ("count", _i(1)))),
    "Record": (Record, (
        ("record_id", _S), ("source_position", _JM), ("payload_ref", _OR), ("inline", ("opt_row",)),
        ("encoded_bytes", _i()), ("sequence", _i()), ("schema_ref", _OR), ("block_partition", _odto("BlockPartition")),
    )),
    "SourcePoll": (SourcePoll, (
        ("status", _e(_POLL_STATUSES)), ("record", _odto("Record")), ("retry_after_ms", _i()),
    )),
    "StageSpec": (StageSpec, (
        ("stage_id", _I), ("occurrences", _STRS), ("partitions", _STRS), ("profile", _P),
        ("replica_min", _i(1)), ("replica_target", _i(1)), ("replica_max", _i(1)),
        ("resident_bytes", _i()), ("output_bytes", _i()), ("state_bytes", _i()), ("kernel_refs", _RS),
    )),
    "ExchangeSpec": (ExchangeSpec, (
        ("channel_id", _I), ("edge_id", _S), ("producer_partitions", _STRS), ("consumer_partitions", _STRS),
        ("mode", _e(_MODES)), ("routing", _e(_ROUTINGS)), ("keys", _STRS), ("scope_id", _OI),
        ("route_epoch", _i()), ("credit_rows", _i(1)), ("credit_bytes", _i(1)), ("storage_bytes", _i()),
        ("read_domain", _I), ("reason", _t(1024)),
    )),
    "RuntimePolicy": (RuntimePolicy, (
        ("provider", _e(_PROVIDERS)), ("durability", _e(_DURABILITIES)), ("default_partitions", _i(1)),
        ("max_replicas", _i(1)), ("fusion", _B), ("checkpoint_rows", _i(1)), ("checkpoint_interval_ms", _i(1)),
        ("fairness_quantum_bytes", _i(1)), ("control_timeout_ms", _i(1)), ("store_id", _I),
        ("required_recovery", _e(_RECOVERIES)),
        ("max_active_workers", _oi(1)),
        ("native_limits", _OJM),
    )),
    "ResidencyPlan": (ResidencyPlan, (
        ("simultaneous_groups", ("strs_nested",)), ("control_reserved_bytes", _i()),
        ("consumer_reserved_slots", _i()), ("required_spill_bytes", _i()),
    )),
    "FiniteScope": (FiniteScope, (
        ("scope_id", _I), ("generation", _i()), ("membership", _STRS), ("completion_condition", _e(_SCOPE_CONDITIONS)),
    )),
    "PhysicalDataflow": (PhysicalDataflow, (
        ("schema", ("lit", "demiflow.physical_dataflow.v3")), ("dag_id", _S), ("plan_id", _S),
        ("plan_revision", _i()), ("stages", _dtos("StageSpec")), ("exchanges", _dtos("ExchangeSpec")),
        ("residency", _dto("ResidencyPlan")), ("scopes", _dtos("FiniteScope")), ("sources", _dtos("SourceSnapshot")),
        ("splits", _dtos("SourceSplit")), ("policy", _dto("RuntimePolicy")), ("bounds", _dto("Bounds")),
        ("recovery", _e(_RECOVERIES)), ("limitations", ("texts", 1024)), ("constants_ref", _R),
    )),
    "SealedExecution": (SealedExecution, (
        ("logical_ref", _R), ("physical_ref", _R), ("programs", _RS), ("code_pin_ref", _R),
        ("identity_bridge_ref", _R), ("source_refs", _RS), ("constants_ref", _R),
    )),
    "ReadyWork": (ReadyWork, (
        ("stage_id", _I), ("partition_id", _I), ("input_refs", _RS), ("resident_bytes", _i()), ("operation_id", _S),
    )),
    "WorkerLease": (WorkerLease, (
        ("allocation_id", _I), ("boot_id", _I), ("stage_id", _I), ("partition_ids", _STRS),
        ("host_id", _I), ("endpoint_ref", _t(1024)), ("grant_ref", _R), ("expires_at_ms", _i()),
    )),
    "WorkerAssignment": (WorkerAssignment, (
        ("schema", ("lit", "demiflow.dataflow_assignment.v3")), ("run_id", _I), ("action_id", _I),
        ("attempt", _i()), ("fence", _I), ("plan_id", _S), ("plan_revision", _i()),
        ("lease", _dto("WorkerLease")), ("sealed", _dto("SealedExecution")), ("stages", _dtos("StageSpec")),
        ("inputs", _dtos("ExchangeSpec")), ("outputs", _dtos("ExchangeSpec")), ("source_splits", _dtos("SourceSplit")),
        ("restore_refs", _RS), ("bounds", _dto("Bounds")), ("native_limits", _OJM),
    )),
    "CutRequest": (CutRequest, (
        ("cut_id", _S), ("sequence", _i()), ("plan_revision", _i()), ("route_epoch", _i()),
        ("participants", _STRS), ("final", _B),
    )),
    "CutContribution": (CutContribution, (
        ("cut_id", _S), ("participant_id", _I), ("boot_id", _I), ("sources", _dtos("SourceCursor")),
        ("operator_states", _RM), ("channels", _JMM), ("buffers", _RS), ("effects", _RS),
        ("sinks", _RM), ("final", _B),
    )),
    "RecoveryImage": (RecoveryImage, (
        ("commit_id", _S), ("cut", _dto("CutRequest")), ("contributions", _dtos("CutContribution")),
        ("confirmed_sinks", _RM), ("source_vector", _dtos("SourceCursor")),
    )),
    "EffectCommand": (EffectCommand, (
        ("operation_id", _S), ("kind", _e(_EFFECT_KINDS)), ("effect_id", _I), ("actor_id", _I),
        ("request_digest", _S), ("payload_ref", _OR), ("budget", _IM),
    )),
    "EffectReply": (EffectReply, (
        ("operation_id", _S), ("status", _e(_EFFECT_STATUSES)), ("result_ref", _OR), ("receipt_ref", _OR),
    )),
    "WorkerEvent": (WorkerEvent, (
        ("schema", ("lit", "demiflow.dataflow_event.v3")), ("kind", _e(_EVENT_KINDS)), ("operation_id", _S),
        ("run_id", _I), ("attempt", _i()), ("fence", _I), ("allocation_id", _I), ("boot_id", _I),
        ("plan_revision", _i()), ("scope_id", _I), ("controller_epoch", _i()),
        ("payload_ref", _OR), ("payload", _OJM),
    )),
    "TerminalResult": (TerminalResult, (
        ("kind", _I), ("rows", _oi()), ("scalar", _J), ("result_ref", _OR),
        ("write_result", ("opt_writeresult",)), ("complete", _B), ("completion", _e(_COMPLETIONS)),
    )),
    "CapabilityReport": (CapabilityReport, (
        ("supported", _B), ("reasons", ("texts", 1024)), ("recovery", _e(_RECOVERIES)),
        ("required_domains", _STRS), ("required_profiles", _PS),
    )),
    "HostDescriptor": (HostDescriptor, (
        ("host_id", _I), ("provider", _e(_PROVIDERS)), ("runtime_revision", _t()),
        ("store_domains", _STRS), ("logical_resources", _FM), ("isolation_capabilities", _STRS),
    )),
    "HistoricalBinding": (HistoricalBinding, (
        ("schema_version", _t()), ("original_plan_ref", _R), ("original_code_pin_ref", _R),
        ("compatible", _B), ("reason", _ot(1024)), ("restore_image", _odto("RecoveryImage")),
    )),
    "OperatorDefinition": (OperatorDefinition, (
        ("kind", _I), ("compiler_symbol", _t()), ("kernel_symbol", _t()), ("inputs", _STRS),
    )),
    "SourceDefinition": (SourceDefinition, (
        ("kind", _I), ("reader_symbol", _t()), ("cursor_fields", _STRS),
    )),
    "TerminalDefinition": (TerminalDefinition, (
        ("kind", _I), ("adapter_symbol", _t()), ("result_kind", _I),
    )),
    "RoutedRecord": (RoutedRecord, (
        ("reader_id", _I), ("delivery_id", _I), ("record", _dto("Record")),
    )),
    "ProducerCompletion": (ProducerCompletion, (
        ("partition_id", _I), ("generation", _i()), ("status", _e(_PRODUCER_STATUSES)),
        ("last_sequence", _i(-1)), ("manifest_ref", _R),
    )),
    "CreditLease": (CreditLease, (
        ("operation_id", _S), ("channel_id", _I), ("reader_id", _I), ("rows", _i()), ("size_bytes", _i()),
    )),
    "AggregateState": (AggregateState, (
        ("kind", _I), ("schema_version", _t()), ("state_ref", _R), ("count", _i()),
    )),
    "DomainProof": (DomainProof, (
        ("host_id", _I), ("store_id", _I), ("challenge_digest", _S), ("namespace_digest", _S), ("durable", _B),
    )),
    "DataLocation": (DataLocation, (
        ("domain", _I), ("temporary_locator", _ot(1024)), ("durable_ref", _R),
    )),
    "WorkerStep": (WorkerStep, (
        ("processed", _i()), ("blocked", _B), ("end_of_input", _B), ("events", _dtos("WorkerEvent")),
    )),
    "ExecutionStatus": (ExecutionStatus, (
        ("state", _e(_EXECUTION_STATES)), ("pending_events", _i()), ("confirmed_rows", _i()),
    )),
    "NodeObservation": (NodeObservation, (
        ("occurrence_id", _S), ("attempt", _i()), ("plan_id", _S), ("observed", _B), ("metrics", _JM),
    )),
    "ScheduleChoice": (ScheduleChoice, (
        ("work", _odto("ReadyWork")), ("retry_after_ms", _i()), ("reason", _ot(1024)),
    )),
    "ArtifactCommand": (ArtifactCommand, (
        ("operation_id", _S), ("kind", _e(_ARTIFACT_KINDS)), ("ref", _OR), ("owner", _I), ("size_bytes", _i()),
    )),
    "ArtifactReply": (ArtifactReply, (
        ("operation_id", _S), ("present", _OB), ("charged_bytes", _i()),
    )),
    "ControlReply": (ControlReply, (
        ("operation_id", _S), ("status", _e(_REPLY_STATUSES)), ("payload", _OJM),
        ("payload_ref", _OR), ("error_code", ("opt_errorcode",)),
    )),
    "CompletedControl": (CompletedControl, (
        ("allocation_id", _I), ("boot_id", _I), ("reply", _dto("ControlReply")),
    )),
    "NativeJournalCommand": (NativeJournalCommand, (
        ("operation_id", _S), ("operation", _e(_JOURNAL_OPS)), ("action_id", _I), ("node_id", _I),
        ("request_key", _t(512)), ("request_ref", _R), ("value_ref", _OR),
        ("reservation", ("opt_reservation",)), ("options", _JM),
    )),
    "NativeJournalReply": (NativeJournalReply, (
        ("operation_id", _S), ("value", _J), ("value_ref", _OR),
    )),
    "QueueBinding": (QueueBinding, (
        ("action_id", _I), ("source_id", _OS), ("mode", _e(_QUEUE_MODES)), ("contract_digest", _S),
        ("generation", _ot()), ("pool", _ot()), ("source_occurrence", _OS),
        ("fetch_occurrence", _S), ("final_occurrence", _OS),
    )),
    "QueueCommand": (QueueCommand, (
        ("operation_id", _S), ("operation", _e(_QUEUE_OPS)), ("binding", _dto("QueueBinding")),
        ("record_id", _OS), ("delivery_ref", _OR), ("row_ref", _OR), ("node_id", _OS),
    )),
    "QueueReply": (QueueReply, (
        ("operation_id", _S), ("status", _e(_QUEUE_STATUSES)), ("delivery_ref", _OR),
        ("row_ref", _OR), ("native_fact_ref", _OR), ("retry_after_ms", _i()),
    )),
}


def _thaw_json(value):
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if type(value) in (tuple, list):
        return [_thaw_json(item) for item in value]
    return value


def _is_json(value, depth=0):
    if depth > 32:
        return False
    if value is None or type(value) in (bool, int, str):
        return True
    if type(value) is float:
        return math.isfinite(value)
    if type(value) is dict:
        return all(type(k) is str and _is_json(v, depth + 1) for k, v in value.items())
    if type(value) is list:
        return all(_is_json(v, depth + 1) for v in value)
    return False


def _check_ident(value, label):
    if type(value) is not str or _IDENT.fullmatch(value) is None:
        raise _err(label + " must be a bounded identifier")


def _check_sha(value, label):
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _err(label + " must be lowercase sha256 hex")


def _check_text(value, label, maxlen):
    if type(value) is not str or not 1 <= len(value) <= maxlen:
        raise _err(label + " must be a bounded nonempty string")


def _check_int(value, label, minimum):
    if type(value) is not int or value < minimum:
        raise _err(label + " must be an integer >= " + str(minimum))


def _validate_field(value, spec, label):
    kind = spec[0]
    if kind == "sha256":
        _check_sha(value, label)
    elif kind == "opt_sha256":
        if value is not None:
            _check_sha(value, label)
    elif kind == "ident":
        _check_ident(value, label)
    elif kind == "opt_ident":
        if value is not None:
            _check_ident(value, label)
    elif kind == "text":
        _check_text(value, label, spec[1])
    elif kind == "opt_text":
        if value is not None:
            _check_text(value, label, spec[1])
    elif kind == "int":
        _check_int(value, label, spec[1])
    elif kind == "opt_int":
        if value is not None:
            _check_int(value, label, spec[1])
    elif kind == "bool":
        if type(value) is not bool:
            raise _err(label + " must be a bool")
    elif kind == "opt_bool":
        if value is not None and type(value) is not bool:
            raise _err(label + " must be a bool or null")
    elif kind == "enum":
        if type(value) is not str or value not in spec[1]:
            raise _err(label + " must be one of " + ",".join(sorted(spec[1])))
    elif kind == "lit":
        if value != spec[1]:
            raise _err(label + " must equal " + repr(spec[1]))
    elif kind == "decimal":
        if type(value) is not str or _DECIMAL.fullmatch(value) is None:
            raise _err(label + " must be a decimal index string")
    elif kind == "key":
        _artifact_key(value, label)
    elif kind == "json":
        if not _is_json(value):
            raise _err(label + " must be finite Json")
    elif kind == "jmap":
        _validate_jmap(value, label)
    elif kind == "opt_jmap":
        if value is not None:
            _validate_jmap(value, label)
    elif kind == "opt_errorcode":
        if value is not None and value not in DATAFLOW_ERROR_CODES:
            raise _err(label + " must be a closed dataflow error code")
    elif kind in ("ref", "opt_ref"):
        if value is None and kind == "opt_ref":
            return
        _validate_payload(value, "ArtifactRef")
    elif kind == "refs":
        _validate_list(value, label, lambda v, l: _validate_payload(v, "ArtifactRef"))
    elif kind == "dto":
        _validate_payload(value, spec[1])
    elif kind == "opt_dto":
        if value is not None:
            _validate_payload(value, spec[1])
    elif kind == "dtos":
        _validate_list(value, label, lambda v, l: _validate_payload(v, spec[1]))
    elif kind == "strs":
        _validate_list(value, label, lambda v, l: _check_ident(v, l))
    elif kind == "strs_nested":
        _validate_list(value, label, lambda v, l: _validate_list(v, l, lambda x, lx: _check_ident(x, lx)))
    elif kind == "texts":
        _validate_list(value, label, lambda v, l: _check_text(v, l, spec[1]))
    elif kind == "imap":
        _validate_map(value, label, lambda v, l: _check_int(v, l, 0))
    elif kind == "fmap":
        _validate_map(value, label, _check_finite_nonneg)
    elif kind == "refmap":
        _validate_map(value, label, lambda v, l: _validate_payload(v, "ArtifactRef"))
    elif kind == "jmapmap":
        _validate_map(value, label, _validate_jmap)
    elif kind == "profile":
        _validate_profile(value, label)
    elif kind == "profiles":
        _validate_list(value, label, _validate_profile)
    elif kind == "opt_writeresult":
        if value is not None:
            _validate_writeresult(value, label)
    elif kind == "opt_row":
        if value is not None:
            _validate_jmap(value, label)
    elif kind == "opt_reservation":
        if value is not None:
            _validate_reservation(value, label)
    else:
        raise _err("unknown wire field kind " + kind)


def _validate_list(value, label, check):
    if type(value) is not list:
        raise _err(label + " must be a list")
    for index, item in enumerate(value):
        check(item, label + "[" + str(index) + "]")


def _validate_map(value, label, check):
    if type(value) is not dict:
        raise _err(label + " must be an object")
    for key, item in value.items():
        _check_ident(key, label + " key")
        check(item, label + "." + key)


def _validate_jmap(value, label):
    if type(value) is not dict:
        raise _err(label + " must be an object")
    for key, item in value.items():
        if type(key) is not str or not 1 <= len(key) <= 256:
            raise _err(label + " keys must be bounded strings")
        if not _is_json(item):
            raise _err(label + "." + key + " must be finite Json")


def _check_finite_nonneg(value, label):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise _err(label + " must be a finite nonnegative number")


def _validate_profile(value, label):
    if type(value) is not dict or not {"requests"} <= set(value) or set(value) - {"requests", "limits", "requirements"}:
        raise _err(label + " must be a closed ResourceProfile object")
    for section in ("requests", "limits", "requirements"):
        if section in value and value[section] is not None:
            if not _is_json(value[section]):
                raise _err(label + "." + section + " must be finite Json")


def _validate_writeresult(value, label):
    keys = {"written_rows", "failed_rows", "blocks_written", "target", "metadata"}
    if type(value) is not dict or set(value) != keys:
        raise _err(label + " must be a closed WriteResult object")
    for name in ("written_rows", "failed_rows", "blocks_written"):
        if value[name] is not None:
            _check_int(value[name], label + "." + name, 0)
    _check_text(value["target"], label + ".target", 4096)
    if not _is_json(value["metadata"]):
        raise _err(label + ".metadata must be finite Json")


def _validate_reservation(value, label):
    if type(value) is not list or len(value) != 2:
        raise _err(label + " must be an [id, charges] pair")
    _check_ident(value[0], label + " id")
    charges = value[1]
    if type(charges) is not dict:
        raise _err(label + " charges must be an object")
    for key, amount in charges.items():
        _check_ident(key, label + " charge key")
        if type(amount) is not list or len(amount) != 2:
            raise _err(label + " charge must be an [amount, limit] pair")
        _check_int(amount[0], label + " amount", 0)
        _check_int(amount[1], label + " limit", 0)


def _validate_payload(payload, type_name):
    entry = _WIRE_TYPES.get(type_name)
    if entry is None:
        raise _err("unknown wire type " + str(type_name))
    if type(payload) is not dict:
        raise _err(type_name + " payload must be an object")
    fields = entry[1]
    expected = {name for name, _ in fields}
    if type_name == "Record" and "schema_ref" not in payload:
        # The optional typed-row sidecar does not rewrite legacy row frames.
        payload = dict(payload, schema_ref=None)
    if type_name == "Record" and "block_partition" not in payload:
        payload = dict(payload, block_partition=None)
    if type_name == "RuntimePolicy" and "max_active_workers" not in payload:
        payload = dict(payload, max_active_workers=None)
    if type_name in ("RuntimePolicy", "WorkerAssignment") and "native_limits" not in payload:
        payload = dict(payload, native_limits=None)
    if set(payload) != expected:
        raise _err(type_name + " payload key set is not closed: " + str(sorted(set(payload) ^ expected)))
    for name, spec in fields:
        _validate_field(payload[name], spec, type_name + "." + name)
    cross = _CROSS_VALIDATORS.get(type_name)
    if cross is not None:
        cross(payload)


def _cross_record(p):
    has_ref = p["payload_ref"] is not None
    has_inline = p["inline"] is not None
    if has_ref == has_inline:
        raise _err("Record payload must have exactly one of inline/payload_ref")


def _cross_sourcepoll(p):
    status = p["status"]
    retry = p["retry_after_ms"]
    if status == "record":
        if p["record"] is None:
            raise _err("SourcePoll.record status requires a Record")
        if retry != 0:
            raise _err("SourcePoll.record status requires retry_after_ms=0")
    elif status == "pending":
        if p["record"] is not None:
            raise _err("SourcePoll.pending status must not carry a record")
        if retry <= 0:
            raise _err("SourcePoll.pending status requires retry_after_ms>0")
    else:
        if p["record"] is not None:
            raise _err("SourcePoll.eoi status must not carry a record")
        if retry != 0:
            raise _err("SourcePoll.eoi status requires retry_after_ms=0")


def _cross_workerevent(p):
    if (p["payload"] is not None) == (p["payload_ref"] is not None):
        raise _err("WorkerEvent payload must have exactly one of payload/payload_ref")


def _cross_controlreply(p):
    if p["payload"] is not None and p["payload_ref"] is not None:
        raise _err("ControlReply payload must have at most one of payload/payload_ref")
    if p["status"] == "error":
        if p["error_code"] is None:
            raise _err("ControlReply.error status requires error_code")
    elif p["error_code"] is not None:
        raise _err("ControlReply non-error status must not carry error_code")


def _cross_journalreply(p):
    if p["value"] is not None and p["value_ref"] is not None:
        raise _err("NativeJournalReply must have at most one of value/value_ref")


def _cross_queuecommand(p):
    if p['operation'] == 'search' and p['node_id'] is None:
        raise _err('Native search requires an explicit owning occurrence')
    if p['node_id'] is not None and not (p['operation'] == 'search' or
            p['operation'] == 'acquire' and p['binding']['mode'] == 'raw_fetch'):
        raise _err('Only native search or ordinary fetch accepts an owning occurrence')
    if p["operation"] in ("acquire", "finalize", "search"):
        if p["row_ref"] is None:
            raise _err("QueueCommand." + p["operation"] + " requires row_ref")
    else:
        if p["row_ref"] is not None or p["delivery_ref"] is not None:
            raise _err("QueueCommand." + p["operation"] + " must not carry business payload")


def _cross_schedulechoice(p):
    if p["work"] is None and p["retry_after_ms"] <= 0:
        raise _err("ScheduleChoice.work=None must be a bounded wait (retry_after_ms>0)")


def _cross_stagespec(p):
    if not p["replica_min"] <= p["replica_target"] <= p["replica_max"]:
        raise _err("StageSpec replicas must satisfy min<=target<=max")


def _cross_finitescope(p):
    if len(p["membership"]) != len(set(p["membership"])):
        raise _err("FiniteScope.membership must be unique")


def _cross_cutrequest(p):
    if len(p["participants"]) != len(set(p["participants"])):
        raise _err("CutRequest.participants must be unique")


_CROSS_VALIDATORS = {
    "Record": _cross_record,
    "SourcePoll": _cross_sourcepoll,
    "WorkerEvent": _cross_workerevent,
    "ControlReply": _cross_controlreply,
    "NativeJournalReply": _cross_journalreply,
    "QueueCommand": _cross_queuecommand,
    "ScheduleChoice": _cross_schedulechoice,
    "StageSpec": _cross_stagespec,
    "FiniteScope": _cross_finitescope,
    "CutRequest": _cross_cutrequest,
}


def _construct_field(value, spec, label):
    kind = spec[0]
    if kind in ("ref",):
        return _construct_payload(value, "ArtifactRef")
    if kind == "opt_ref":
        return None if value is None else _construct_payload(value, "ArtifactRef")
    if kind == "refs":
        return tuple(_construct_payload(v, "ArtifactRef") for v in value)
    if kind == "refmap":
        return {k: _construct_payload(v, "ArtifactRef") for k, v in value.items()}
    if kind == "dto":
        return _construct_payload(value, spec[1])
    if kind == "opt_dto":
        return None if value is None else _construct_payload(value, spec[1])
    if kind == "dtos":
        return tuple(_construct_payload(v, spec[1]) for v in value)
    if kind == "profile":
        return _construct_profile(value, label)
    if kind == "profiles":
        return tuple(_construct_profile(v, label) for v in value)
    if kind == "opt_writeresult":
        return None if value is None else WriteResult(**value)
    if kind == "opt_reservation":
        return None if value is None else (value[0], {k: tuple(v) for k, v in value[1].items()})
    if kind in ("strs", "texts"):
        return tuple(value)
    if kind == "strs_nested":
        return tuple(tuple(group) for group in value)
    return value


def _construct_profile(value, label):
    return ResourceProfile(
        requests=value["requests"],
        limits=value.get("limits"),
        requirements=value.get("requirements"),
    )


def _construct_payload(payload, type_name):
    entry = _WIRE_TYPES[type_name]
    cls, fields = entry
    kwargs = {}
    for name, spec in fields:
        optional = (type_name, name) in {("Record", "schema_ref"), ("Record", "block_partition"), ("RuntimePolicy", "max_active_workers"), ("RuntimePolicy", "native_limits"), ("WorkerAssignment", "native_limits")}
        value = payload.get(name) if optional else payload[name]
        kwargs[name] = _construct_field(value, spec, type_name + "." + name)
    return cls(**kwargs)


def _project_field(value, spec):
    kind = spec[0]
    if kind in ("json", "jmap", "opt_jmap", "opt_row"):
        return _thaw_json(value)
    if kind == "ref":
        return _project_payload(value, "ArtifactRef")
    if kind == "opt_ref":
        return None if value is None else _project_payload(value, "ArtifactRef")
    if kind == "refs":
        return [_project_payload(v, "ArtifactRef") for v in value]
    if kind == "dto":
        return _project_payload(value, spec[1])
    if kind == "opt_dto":
        return None if value is None else _project_payload(value, spec[1])
    if kind == "dtos":
        return [_project_payload(v, spec[1]) for v in value]
    if kind == "profile":
        return value.to_dict()
    if kind == "profiles":
        return [p.to_dict() for p in value]
    if kind == "opt_writeresult":
        if value is None:
            return None
        return {
            "written_rows": value.written_rows,
            "failed_rows": value.failed_rows,
            "blocks_written": value.blocks_written,
            "target": value.target,
            "metadata": _thaw_json(value.metadata),
        }
    if kind == "opt_reservation":
        if value is None:
            return None
        return [value[0], {k: list(v) for k, v in value[1].items()}]
    if kind == "refmap":
        return {k: _project_payload(v, "ArtifactRef") for k, v in value.items()}
    if kind == "jmapmap":
        return {k: _thaw_json(v) for k, v in value.items()}
    if kind in ("imap", "fmap"):
        return dict(value)
    if kind in ("strs", "texts"):
        return list(value)
    if kind == "strs_nested":
        return [list(group) for group in value]
    return value


def _project_payload(value, type_name):
    entry = _WIRE_TYPES[type_name]
    fields = entry[1]
    payload = {}
    for name, spec in fields:
        if type_name == "Record" and name in ("schema_ref", "block_partition") and getattr(value, name) is None:
            continue
        if type_name == "RuntimePolicy" and name in ("max_active_workers", "native_limits") and getattr(value, name) is None:
            continue
        if type_name == "WorkerAssignment" and name == "native_limits" and value.native_limits is None:
            continue
        payload[name] = _project_field(getattr(value, name), spec)
    return payload


def _walk_budget(value, max_depth, depth=0, state=None):
    if state is None:
        state = [0, 0]
    state[0] += 1
    if state[0] > _WIRE_MAX_NODES:
        raise _budget_err("wire frame exceeds node budget")
    if depth > max_depth:
        raise _budget_err("wire frame exceeds depth budget")
    kind = type(value)
    if kind is dict:
        state[1] += 2 + 3 * len(value)
        for key, item in value.items():
            if type(key) is not str:
                raise _err("wire frame keys must be strings")
            state[1] += len(key.encode("utf-8")) + 3
            _walk_budget(item, max_depth, depth + 1, state)
    elif kind is list:
        state[1] += 2 + len(value)
        for item in value:
            _walk_budget(item, max_depth, depth + 1, state)
    elif kind is str:
        state[1] += len(value.encode("utf-8")) + 2
    elif kind is bool or value is None:
        state[1] += 5
    elif kind in (int, float):
        state[1] += 32
    else:
        raise _err("wire frame must be finite Json")
    return state


def _scan_depth(raw, max_depth):
    depth = 0
    in_string = False
    escaped = False
    for byte in raw:
        if in_string:
            if escaped:
                escaped = False
            elif byte == 0x5C:
                escaped = True
            elif byte == 0x22:
                in_string = False
            continue
        if byte == 0x22:
            in_string = True
        elif byte in (0x5B, 0x7B):
            depth += 1
            if depth > max_depth:
                raise _budget_err("wire frame exceeds depth budget")
        elif byte in (0x5D, 0x7D):
            depth -= 1


def _reject_constant(token):
    raise _err("wire frame must not contain NaN/Infinity")


def _no_duplicate_keys(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise _err("wire frame has duplicate key " + key)
        obj[key] = value
    return obj


def _check_type_name(type_name):
    if type(type_name) is not str or type_name not in _WIRE_TYPES:
        raise _err("unknown wire type " + str(type_name))


def _check_bounds(bounds):
    if not isinstance(bounds, Bounds):
        raise _err("bounds must be a Bounds")


def validate_dataflow_record(value, *, type_name: str, bounds: Bounds) -> None:
    _check_type_name(type_name)
    _check_bounds(bounds)
    cls = _WIRE_TYPES[type_name][0]
    if isinstance(value, cls):
        payload = _project_payload(value, type_name)
    elif type(value) is dict:
        payload = value
    else:
        raise _err(type_name + " must be a frozen DTO or a closed payload object")
    state = _walk_budget(payload, bounds.max_depth)
    if state[1] > bounds.max_control_bytes:
        raise _budget_err("wire frame exceeds control byte budget")
    _validate_payload(payload, type_name)


def encode_wire(value: object, *, type_name: str, bounds: Bounds) -> bytes:
    _check_type_name(type_name)
    _check_bounds(bounds)
    cls = _WIRE_TYPES[type_name][0]
    if not isinstance(value, cls):
        raise _err(type_name + " encode requires the frozen DTO instance")
    payload = _project_payload(value, type_name)
    state = _walk_budget(payload, bounds.max_depth)
    if state[1] > bounds.max_control_bytes:
        raise _budget_err("wire frame exceeds control byte budget")
    _validate_payload(payload, type_name)
    frame = {"schema": WIRE_SCHEMA, "type": type_name, "payload": payload}
    raw = json.dumps(
        frame, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    if len(raw) > bounds.max_control_bytes:
        raise _budget_err("wire frame exceeds control byte budget")
    return raw


def decode_wire(raw: bytes, *, type_name: str, bounds: Bounds) -> object:
    _check_type_name(type_name)
    _check_bounds(bounds)
    if type(raw) is not bytes:
        raise _err("decode_wire requires bytes")
    if len(raw) > bounds.max_control_bytes:
        raise _budget_err("wire frame exceeds control byte budget")
    _scan_depth(raw, bounds.max_depth)
    text = raw.decode("utf-8")
    frame = json.loads(text, parse_constant=_reject_constant, object_pairs_hook=_no_duplicate_keys)
    if type(frame) is not dict or set(frame) != {"schema", "type", "payload"}:
        raise _err("wire frame must have closed schema/type/payload")
    if frame["schema"] != WIRE_SCHEMA:
        raise _err("unsupported wire schema " + str(frame["schema"]))
    if frame["type"] != type_name:
        raise _err("wire type mismatch: expected " + type_name)
    payload = frame["payload"]
    state = _walk_budget(payload, bounds.max_depth)
    if state[1] > bounds.max_control_bytes:
        raise _budget_err("wire frame exceeds control byte budget")
    _validate_payload(payload, type_name)
    dto = _construct_payload(payload, type_name)
    validate_dataflow_record(dto, type_name=type_name, bounds=bounds)
    return dto


def _canonical_bytes(document):
    return json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _canonical_sha(document):
    return hashlib.sha256(_canonical_bytes(document)).hexdigest()


def _identity_project(value):
    if isinstance(value, ArtifactRef):
        return {
            "digest": value.digest,
            "size_bytes": value.size_bytes,
            "media_type": value.media_type,
        }
    if isinstance(value, ResourceProfile):
        return value.to_dict()
    if isinstance(value, WriteResult):
        return {
            "written_rows": value.written_rows,
            "failed_rows": value.failed_rows,
            "blocks_written": value.blocks_written,
            "target": value.target,
            "metadata": _thaw_json(value.metadata),
        }
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _identity_project(getattr(value, field.name))
            for field in dataclasses.fields(value)
            if not (isinstance(value, RuntimePolicy) and field.name in ("max_active_workers", "native_limits")
                    and getattr(value, field.name) is None)
            and not (isinstance(value, WorkerAssignment) and field.name == "native_limits"
                     and value.native_limits is None)
            and not (isinstance(value, Record) and field.name == "block_partition"
                     and value.block_partition is None)
        }
    if isinstance(value, Mapping):
        return {key: _identity_project(item) for key, item in value.items()}
    if type(value) in (tuple, list):
        return [_identity_project(item) for item in value]
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise _err("identity projection requires finite numbers")
        return value
    raise _err("identity projection rejects non-Json value " + type(value).__name__)


def compute_graph_digest(graph: DataflowGraph) -> str:
    if not isinstance(graph, DataflowGraph):
        raise _err("compute_graph_digest requires a DataflowGraph")
    nodes = [
        [node.occurrence_id, node.semantic_id, node.kind, node.shared_token]
        for node in graph.nodes
    ]
    edges = [
        [edge.edge_id, edge.producer, edge.output_port, edge.consumer, edge.input_port]
        for edge in graph.edges
    ]
    document = [
        "dataflow-dag-v3",
        graph.action_id,
        nodes,
        edges,
        list(graph.terminals),
        list(graph.sources),
    ]
    return _canonical_sha(document)


def compute_physical_digest(physical: PhysicalDataflow) -> str:
    if not isinstance(physical, PhysicalDataflow):
        raise _err("compute_physical_digest requires a PhysicalDataflow")
    projection = {
        field.name: _identity_project(getattr(physical, field.name))
        for field in dataclasses.fields(physical)
        if field.name != "plan_id"
    }
    return _canonical_sha(["dataflow-physical-v3", projection])


def compute_participant_id(stage_id: str, partition_id: str) -> str:
    """Bounded identity of one stage/partition checkpoint participant.

    Both source names can already be 128 characters. Concatenating them does
    not satisfy the C identifier contract; one domain-separated digest does.
    """
    from .dataflow import _ident
    _ident(stage_id, "participant stage")
    _ident(partition_id, "participant partition")
    return _canonical_sha(["dataflow-participant-v3", stage_id, partition_id])


def runtime_scope_id(plan_id: str, attempt: int) -> str:
    """Resource authority is attempt-scoped; semantic plan identity is unchanged."""
    if type(attempt) is not int or attempt < 1:
        raise _err("runtime scope needs a positive attempt")
    return "df-" + plan_id + ("-a" + str(attempt) if attempt > 1 else "")
