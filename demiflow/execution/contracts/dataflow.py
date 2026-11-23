"""Frozen v3 dataflow contracts (C01; cards C007-C074, C076-C088).

Design authority: ``docs/unified-runtime-v3-20261114/interfaces.pyi``. Every DTO
is a frozen dataclass whose inner Json/mapping/tuple state is deep-frozen and
detached from the caller in ``__post_init__``; live runtime objects (services,
callables, plans, sinks) are held by reference and never force-frozen. Runtime
type imports are TYPE_CHECKING-only so contracts never reverse-import
run_context/runtime; the few structural base/value types come from lower-level
modules that cannot import execution.
"""
from __future__ import annotations

import math
import re
from collections.abc import Mapping as _RuntimeMapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Callable,
    Iterator,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    runtime_checkable,
)

from demiflow.data.datasink import Datasink, WriteResult
from demiflow.data.plan import LogicalOp
from demiflow.resources.contracts import ResourceProfile

from .dataflow_errors import DATAFLOW_ERROR_CODES, DataflowError

if TYPE_CHECKING:
    from demiflow.data.datasource import Datasource, ReadTask
    from demiflow.data.plan import CallableSpec, LogicalPlan
    from demiflow.data.sources import SourcePlan
    from demiflow.execution.executors.base import DatasetExecutor
    from demiflow.execution.recovery_contract import SourcePosition
    from demiflow.execution.run_context import RunAction
    from demiflow.resources.contracts import WorkerLaunchSpec
    from demiflow.resources.manager import ResourceManager

Json = None | bool | int | float | str | list["Json"] | dict[str, "Json"]
Row = Mapping[str, object]
Provider = Literal["local", "ray_core"]
Mode = Literal["memory_incremental", "durable_incremental", "finite_sealed"]
Recovery = Literal["checkpoint", "final_commit", "delivery_only", "attempt_only"]

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENT = re.compile(r"[A-Za-z0-9_.:\-]{1,128}\Z")
_JSON_MAX_DEPTH = 32
_JSON_MAX_NODES = 65536
_ROW_MAX_DEPTH = 64

# A fixed source is staged as native batches in its co-located terminal.
FIXED_SCAN_BATCHES = 'fixed_scan_batches.v1'

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
    "read_configuration", "restrict_recovery", "check_grant", "queue", "native_request", "failure", "eoi", "closed", "data", "ack", "idle", "terminal", "checkpoint_wait",
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
_PROGRAM_KINDS = frozenset({"source", "callable", "aggregate", "sink", "native"})
_ACTION_PROGRAM_KINDS = frozenset({"aggregate", "sink"})
_EXECUTION_STATES = frozenset({"preparing", "running", "draining", "succeeded", "failed", "cancelled"})
_ACTION_KINDS = frozenset({
    "take", "take_all", "take_batch", "iter_rows", "iter_batches", "count", "aggregate",
    "schema", "columns", "size_bytes", "materialize", "write_datasink", "write_lance",
    "write_file", "run_stream", "collect_summary",
})


def _err(message: str) -> DataflowError:
    return DataflowError("INVALID_CONTRACT", message)


def _budget_err(message: str) -> DataflowError:
    return DataflowError("BUDGET_EXCEEDED", message)


def _sha256(value, label: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _err(label + " must be lowercase sha256 hex")
    return value


def _ident(value, label: str) -> str:
    if type(value) is not str or _IDENT.fullmatch(value) is None:
        raise _err(label + " must be a bounded identifier")
    return value


def _text(value, label: str, *, max_len: int = 256) -> str:
    if type(value) is not str or not 1 <= len(value) <= max_len:
        raise _err(label + " must be a bounded nonempty string")
    return value


def _opt_text(value, label: str, *, max_len: int = 256):
    if value is None:
        return None
    return _text(value, label, max_len=max_len)


def _int(value, label: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise _err(label + " must be an integer >= " + str(minimum))
    return value


def _bool(value, label: str) -> bool:
    if type(value) is not bool:
        raise _err(label + " must be a bool")
    return value


def _enum(value, label: str, allowed) -> str:
    if type(value) is not str or value not in allowed:
        raise _err(label + " must be one of " + ",".join(sorted(allowed)))
    return value


def _artifact_key(value, label: str) -> str:
    if type(value) is not str or not 1 <= len(value) <= 1024:
        raise _err(label + " must be a bounded relative store key")
    if value.startswith("/") or "\\" in value:
        raise _err(label + " must be a relative POSIX key")
    if ".." in value.split("/"):
        raise _err(label + " must not traverse parent directories")
    if "?" in value or "#" in value or "://" in value or "@" in value:
        raise _err(label + " must not carry URL query, scheme or credentials")
    return value


def _freeze_json(value, label: str, depth: int = 0, budget: list | None = None):
    if budget is None:
        budget = [0, 0]
    budget[0] += 1
    if budget[0] > _JSON_MAX_NODES:
        raise _budget_err(label + " exceeds Json node budget")
    if depth > _JSON_MAX_DEPTH:
        raise _budget_err(label + " exceeds Json depth budget")
    if value is None or type(value) is bool or type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise _err(label + " must be a finite number")
        return value
    if type(value) is str:
        budget[1] += len(value.encode("utf-8"))
        return value
    if type(value) is dict:
        frozen = {}
        for key, item in value.items():
            if type(key) is not str:
                raise _err(label + " keys must be strings")
            frozen[key] = _freeze_json(item, label, depth + 1, budget)
        return MappingProxyType(frozen)
    if type(value) is list:
        return tuple(_freeze_json(item, label, depth + 1, budget) for item in value)
    raise _err(label + " must be finite Json (no tuple/bytes/NaN)")


def _json_map(value, label: str) -> Mapping[str, Json]:
    if not isinstance(value, _RuntimeMapping):
        raise _err(label + " must be a mapping")
    budget = [0, 0]
    frozen = {}
    for key, item in value.items():
        if type(key) is not str or not 1 <= len(key) <= 256:
            raise _err(label + " keys must be bounded strings")
        frozen[key] = _freeze_json(item, label + "." + key, 1, budget)
    return MappingProxyType(frozen)


def _json_scalar(value, label: str):
    return _freeze_json(value, label)


def _tuple(value, label: str, check) -> tuple:
    if type(value) not in (tuple, list):
        raise _err(label + " must be a tuple or list")
    items = tuple(value)
    for index, item in enumerate(items):
        check(item, label + "[" + str(index) + "]")
    return items


def _ref(value, label: str):
    if not isinstance(value, ArtifactRef):
        raise _err(label + " must be an ArtifactRef")
    return value


def _opt_ref(value, label: str):
    if value is None:
        return None
    return _ref(value, label)


def _ref_tuple(value, label: str) -> tuple:
    return _tuple(value, label, _ref)


def _str_tuple(value, label: str) -> tuple:
    return _tuple(value, label, lambda v, l: _ident(v, l))


def _ref_map(value, label: str) -> Mapping[str, ArtifactRef]:
    if not isinstance(value, _RuntimeMapping):
        raise _err(label + " must be a mapping")
    frozen = {}
    for key, item in value.items():
        _ident(key, label + " key")
        frozen[key] = _ref(item, label + "." + key)
    return MappingProxyType(frozen)


def _int_map(value, label: str) -> Mapping[str, int]:
    if not isinstance(value, _RuntimeMapping):
        raise _err(label + " must be a mapping")
    frozen = {}
    for key, item in value.items():
        _ident(key, label + " key")
        frozen[key] = _int(item, label + "." + key)
    return MappingProxyType(frozen)


def _float_map(value, label: str) -> Mapping[str, float]:
    if not isinstance(value, _RuntimeMapping):
        raise _err(label + " must be a mapping")
    frozen = {}
    for key, item in value.items():
        _ident(key, label + " key")
        if type(item) not in (int, float) or not math.isfinite(item) or item < 0:
            raise _err(label + "." + key + " must be a finite nonnegative number")
        frozen[key] = float(item)
    return MappingProxyType(frozen)


class _FrozenRowList(tuple):
    """Immutable list payload retaining its business type across decoding."""


class _FrozenRowSet(frozenset):
    """Immutable set payload retaining its mutable business container type."""


def thaw_record_value(value):
    """Return ordinary business containers without changing list/tuple identity."""
    kind = type(value)
    if (value is None or kind is str or kind is int or kind is float
            or kind is bool or kind is bytes):
        return value
    if isinstance(value, _RuntimeMapping):
        return {key: thaw_record_value(item) for key, item in value.items()}
    if isinstance(value, _FrozenRowList):
        return [thaw_record_value(item) for item in value]
    if type(value) is tuple:
        return tuple(thaw_record_value(item) for item in value)
    if type(value) is list:
        return [thaw_record_value(item) for item in value]
    if type(value) in (_FrozenRowSet, set):
        return {thaw_record_value(item) for item in value}
    if type(value) is frozenset:
        return frozenset(thaw_record_value(item) for item in value)
    from .array_values import numpy_value
    if numpy_value(value) is not None:
        return value.copy()
    return value


def _freeze_row_value(value, depth: int):
    if depth > _ROW_MAX_DEPTH:
        raise _budget_err("record row exceeds depth budget")
    if isinstance(value, _RuntimeMapping):
        return MappingProxyType({k: _freeze_row_value(v, depth + 1) for k, v in value.items()})
    if type(value) is list or isinstance(value, _FrozenRowList):
        return _FrozenRowList(_freeze_row_value(v, depth + 1) for v in value)
    if type(value) is tuple:
        return tuple(_freeze_row_value(v, depth + 1) for v in value)
    if type(value) in (set, _FrozenRowSet):
        return _FrozenRowSet(_freeze_row_value(v, depth + 1) for v in value)
    if type(value) is frozenset:
        return frozenset(_freeze_row_value(v, depth + 1) for v in value)
    from .array_values import numpy_value, freeze_array
    if numpy_value(value) is not None:
        try:
            return freeze_array(value)
        except ValueError as exc:
            raise _err(str(exc)) from exc
    return value


def _row(value, label: str) -> Mapping[str, object]:
    if not isinstance(value, _RuntimeMapping):
        raise _err(label + " must be a mapping row")
    frozen = {}
    for key, item in value.items():
        if type(key) is not str:
            raise _err(label + " keys must be strings")
        frozen[key] = _freeze_row_value(item, 1)
    return MappingProxyType(frozen)


def _record_value(value, label: str):
    """Freeze the same closed business domain at the payload root.

    Column rows retain their original string-key check. The shared typed codec
    validates every leaf and its byte/node budget before transport; this root
    check does not admit code objects or invoke serialization hooks.
    """
    if isinstance(value, _RuntimeMapping):
        return _row(value, label)
    if type(value) in (bool, int, float, str, bytes, list, tuple, set, frozenset,
                       _FrozenRowList, _FrozenRowSet):
        return _freeze_row_value(value, 0)
    from .scalar_values import _TYPES
    if type(value) in _TYPES:
        return value  # Closed immutable temporal/decimal types; codec checks details.
    from .array_values import numpy_value
    if numpy_value(value) is not None:
        return _freeze_row_value(value, 0)
    raise _err(label + " must be a non-null data value supported by the record codec")


def _profile(value, label: str) -> ResourceProfile:
    if not isinstance(value, ResourceProfile):
        raise _err(label + " must be a ResourceProfile")
    return value


def _write_result(value, label: str):
    if value is None:
        return None
    if not isinstance(value, WriteResult):
        raise _err(label + " must be a WriteResult")
    return value


def _exactly_one(a, b, label: str) -> None:
    if (a is None) == (b is None):
        raise _err(label + " requires exactly one of the two alternatives")


def _at_most_one(a, b, label: str) -> None:
    if a is not None and b is not None:
        raise _err(label + " allows at most one of the two alternatives")

@dataclass(frozen=True)
class ArtifactRef:
    digest: str
    size_bytes: int
    store_id: str
    key: str
    media_type: str

    def __post_init__(self):
        _sha256(self.digest, "ArtifactRef.digest")
        _int(self.size_bytes, "ArtifactRef.size_bytes")
        _ident(self.store_id, "ArtifactRef.store_id")
        _artifact_key(self.key, "ArtifactRef.key")
        _text(self.media_type, "ArtifactRef.media_type")


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

    def __post_init__(self):
        for name in (
            "max_nodes", "max_edges", "max_partitions", "max_depth", "max_control_bytes",
            "max_row_bytes", "max_batch_bytes", "max_resident_bytes", "max_spill_bytes",
            "max_state_bytes", "max_pending_events", "max_open_files", "max_output_rows",
            "timeout_ms",
        ):
            _int(getattr(self, name), "Bounds." + name, minimum=1)


@dataclass(frozen=True)
class InputPort:
    name: str
    requirement: Literal["incremental", "finite_complete"]
    ordering: Literal["none", "source_partition", "key_sequence", "global"]
    schema_ref: ArtifactRef | None

    def __post_init__(self):
        _ident(self.name, "InputPort.name")
        _enum(self.requirement, "InputPort.requirement", _INPUT_REQUIREMENTS)
        _enum(self.ordering, "InputPort.ordering", _ORDERINGS)
        _opt_ref(self.schema_ref, "InputPort.schema_ref")


@dataclass(frozen=True)
class KernelSpec:
    kind: str
    implementation_ref: ArtifactRef | None
    configuration: Mapping[str, Json]
    inputs: tuple[InputPort, ...]
    state: Literal["none", "auto_json", "adapter", "nonrecoverable"]
    effect: Literal["pure", "journaled", "unknown_replay_forbidden"]
    resource_role: Literal["cpu", "gpu", "remote_api", "io"]
    profile: ResourceProfile
    output_bound_bytes: int
    mergeable: bool

    def __post_init__(self):
        _ident(self.kind, "KernelSpec.kind")
        _opt_ref(self.implementation_ref, "KernelSpec.implementation_ref")
        object.__setattr__(self, "configuration", _json_map(self.configuration, "KernelSpec.configuration"))
        object.__setattr__(self, "inputs", _tuple(self.inputs, "KernelSpec.inputs", lambda v, l: _is_input_port(v, l)))
        _enum(self.state, "KernelSpec.state", _KERNEL_STATES)
        _enum(self.effect, "KernelSpec.effect", _KERNEL_EFFECTS)
        _enum(self.resource_role, "KernelSpec.resource_role", _RESOURCE_ROLES)
        _profile(self.profile, "KernelSpec.profile")
        _int(self.output_bound_bytes, "KernelSpec.output_bound_bytes")
        _bool(self.mergeable, "KernelSpec.mergeable")


def _is_input_port(value, label):
    if not isinstance(value, InputPort):
        raise _err(label + " must be an InputPort")
    return value


@dataclass(frozen=True)
class GraphNode:
    occurrence_id: str
    semantic_id: str
    kind: Literal["source", "transform", "relational", "terminal"]
    kernel: KernelSpec
    program_ref: ArtifactRef | None
    shared_token: str

    def __post_init__(self):
        _sha256(self.occurrence_id, "GraphNode.occurrence_id")
        _sha256(self.semantic_id, "GraphNode.semantic_id")
        _enum(self.kind, "GraphNode.kind", _NODE_KINDS)
        if not isinstance(self.kernel, KernelSpec):
            raise _err("GraphNode.kernel must be a KernelSpec")
        _opt_ref(self.program_ref, "GraphNode.program_ref")
        if type(self.shared_token) is not str or not re.fullmatch(r"[0-9]{1,20}", self.shared_token):
            raise _err("GraphNode.shared_token must be the decimal first-occurrence index")


@dataclass(frozen=True)
class GraphEdge:
    edge_id: str
    producer: str
    output_port: str
    consumer: str
    input_port: str

    def __post_init__(self):
        _sha256(self.edge_id, "GraphEdge.edge_id")
        _sha256(self.producer, "GraphEdge.producer")
        _ident(self.output_port, "GraphEdge.output_port")
        _sha256(self.consumer, "GraphEdge.consumer")
        _ident(self.input_port, "GraphEdge.input_port")


@dataclass(frozen=True)
class FitJobNode:
    occurrence_id: str
    semantic_id: str
    configuration: Mapping[str, Json]
    program_ref: ArtifactRef | None

    def __post_init__(self):
        _sha256(self.occurrence_id, "FitJobNode.occurrence_id")
        _sha256(self.semantic_id, "FitJobNode.semantic_id")
        _opt_ref(self.program_ref, "FitJobNode.program_ref")
        object.__setattr__(self, "configuration", _json_map(self.configuration, "FitJobNode.configuration"))


@dataclass(frozen=True)
class FitJobStage:
    stage_id: str
    occurrence_id: str

    def __post_init__(self):
        _ident(self.stage_id, "FitJobStage.stage_id")
        _sha256(self.occurrence_id, "FitJobStage.occurrence_id")


@dataclass(frozen=True)
class ValueEdge:
    """One complete value. Logical endpoints name occurrences, physical endpoints name stages."""
    producer: str
    output_port: str
    consumer: str
    input_port: str
    kind: Literal["fixed_dataset", "model_manifest"]
    step: int | None = None

    def __post_init__(self):
        for name in ("producer", "consumer", "output_port", "input_port"):
            _ident(getattr(self, name), "ValueEdge." + name)
        if self.kind == "fixed_dataset" and self.step is None and self.output_port == "dataset":
            return
        if self.kind == "model_manifest" and self.output_port == "manifest":
            _int(self.step, "ValueEdge.step", minimum=1)
            return
        raise _err("invalid value edge type, selector or output port")


def _value_ports(owner, job_type, schema_name):
    for name, expected in (("jobs", job_type), ("value_edges", ValueEdge)):
        value = getattr(owner, name)
        if type(value) not in (tuple, list) or any(type(item) is not expected for item in value):
            raise _err(name + " has incorrect member type")
        object.__setattr__(owner, name, tuple(value))
    expected_schema = schema_name + (".v4" if owner.jobs else ".v3")
    if owner.schema != expected_schema or bool(owner.jobs) != bool(owner.value_edges):
        raise _err("value ports require a training graph and its original format")


@dataclass(frozen=True)
class DataflowGraph:
    schema: Literal["demiflow.dataflow_graph.v3", "demiflow.dataflow_graph.v4"]
    action_id: str
    dag_id: str
    nodes: tuple[GraphNode, ...]
    edges: tuple[GraphEdge, ...]
    terminals: tuple[str, ...]
    sources: tuple[str, ...]

    jobs: tuple[FitJobNode, ...] = ()
    value_edges: tuple[ValueEdge, ...] = ()

    def __post_init__(self):
        _value_ports(self, FitJobNode, "demiflow.dataflow_graph")
        _ident(self.action_id, "DataflowGraph.action_id")
        _sha256(self.dag_id, "DataflowGraph.dag_id")
        object.__setattr__(self, "nodes", _tuple(self.nodes, "DataflowGraph.nodes", lambda v, l: _is_graph_node(v, l)))
        object.__setattr__(self, "edges", _tuple(self.edges, "DataflowGraph.edges", lambda v, l: _is_graph_edge(v, l)))
        object.__setattr__(self, "terminals", _str_tuple(self.terminals, "DataflowGraph.terminals"))
        object.__setattr__(self, "sources", _str_tuple(self.sources, "DataflowGraph.sources"))


def _is_graph_node(value, label):
    if not isinstance(value, GraphNode):
        raise _err(label + " must be a GraphNode")
    return value


def _is_graph_edge(value, label):
    if not isinstance(value, GraphEdge):
        raise _err(label + " must be a GraphEdge")
    return value


@dataclass(frozen=True)
class SourceSnapshot:
    source_id: str
    kind: str
    immutable_identity: Mapping[str, Json]
    finite: bool
    replay: Literal["seek", "durable_spool", "lease_queue", "attempt_only"]
    manifest_ref: ArtifactRef | None

    def __post_init__(self):
        _sha256(self.source_id, "SourceSnapshot.source_id")
        _ident(self.kind, "SourceSnapshot.kind")
        object.__setattr__(self, "immutable_identity", _json_map(self.immutable_identity, "SourceSnapshot.immutable_identity"))
        _bool(self.finite, "SourceSnapshot.finite")
        _enum(self.replay, "SourceSnapshot.replay", _REPLAYS)
        _opt_ref(self.manifest_ref, "SourceSnapshot.manifest_ref")


@dataclass(frozen=True)
class SourceSplit:
    split_id: str
    source_id: str
    locator: Mapping[str, Json]
    start_cursor: Mapping[str, Json]
    estimated_bytes: int | None
    required_domain: str

    def __post_init__(self):
        _sha256(self.split_id, "SourceSplit.split_id")
        _sha256(self.source_id, "SourceSplit.source_id")
        object.__setattr__(self, "locator", _json_map(self.locator, "SourceSplit.locator"))
        object.__setattr__(self, "start_cursor", _json_map(self.start_cursor, "SourceSplit.start_cursor"))
        if self.estimated_bytes is not None:
            _int(self.estimated_bytes, "SourceSplit.estimated_bytes")
        _ident(self.required_domain, "SourceSplit.required_domain")


@dataclass(frozen=True)
class SourceCursor:
    split_id: str
    offset: Mapping[str, Json]
    next_sequence: int
    end_of_input: bool
    lease_ref: ArtifactRef | None
    pending_refs: tuple[ArtifactRef, ...]

    def __post_init__(self):
        _sha256(self.split_id, "SourceCursor.split_id")
        frozen = _json_map(self.offset, "SourceCursor.offset")
        if not frozen:
            raise _err("SourceCursor.offset must not be empty")
        for key, value in frozen.items():
            if type(value) is int and value < 0:
                raise _err("SourceCursor.offset." + key + " must be nonnegative")
        object.__setattr__(self, "offset", frozen)
        _int(self.next_sequence, "SourceCursor.next_sequence")
        _bool(self.end_of_input, "SourceCursor.end_of_input")
        _opt_ref(self.lease_ref, "SourceCursor.lease_ref")
        object.__setattr__(self, "pending_refs", _ref_tuple(self.pending_refs, "SourceCursor.pending_refs"))


@dataclass(frozen=True)
class BlockPartition:
    """Logical output block, independent of its bounded physical worker host."""
    occurrence_id: str
    index: int
    count: int

    def __post_init__(self):
        _sha256(self.occurrence_id, "BlockPartition.occurrence_id")
        _int(self.index, "BlockPartition.index")
        _int(self.count, "BlockPartition.count", minimum=1)
        if self.index >= self.count:
            raise _err("BlockPartition.index must be below count")


@dataclass(frozen=True)
class Record:
    """One typed business value or payload ref; None still means no inline value.

    Row remains the mapping contract for operations addressing named columns.
    Reducer outputs may instead be any non-null value in the closed data codec.
    """
    record_id: str
    source_position: Mapping[str, Json]
    payload_ref: ArtifactRef | None
    inline: object | None
    encoded_bytes: int
    sequence: int
    schema_ref: ArtifactRef | None = None
    block_partition: BlockPartition | None = None

    def __post_init__(self):
        _sha256(self.record_id, "Record.record_id")
        object.__setattr__(self, "source_position", _json_map(self.source_position, "Record.source_position"))
        _opt_ref(self.payload_ref, "Record.payload_ref")
        _exactly_one(self.payload_ref, self.inline, "Record payload")
        if self.inline is not None:
            object.__setattr__(self, "inline", _record_value(self.inline, "Record.inline"))
        _int(self.encoded_bytes, "Record.encoded_bytes")
        _int(self.sequence, "Record.sequence")
        _opt_ref(self.schema_ref, "Record.schema_ref")
        if self.block_partition is not None and type(self.block_partition) is not BlockPartition:
            raise _err("Record.block_partition must be a BlockPartition")


@dataclass(frozen=True)
class SourcePoll:
    status: Literal["record", "pending", "eoi"]
    record: Record | None
    retry_after_ms: int

    def __post_init__(self):
        _enum(self.status, "SourcePoll.status", _POLL_STATUSES)
        _int(self.retry_after_ms, "SourcePoll.retry_after_ms")
        if self.status == "record":
            if not isinstance(self.record, Record):
                raise _err("SourcePoll.record status requires a Record")
            if self.retry_after_ms != 0:
                raise _err("SourcePoll.record status requires retry_after_ms=0")
        elif self.status == "pending":
            if self.record is not None:
                raise _err("SourcePoll.pending status must not carry a record")
            if self.retry_after_ms <= 0:
                raise _err("SourcePoll.pending status requires retry_after_ms>0")
        else:
            if self.record is not None:
                raise _err("SourcePoll.eoi status must not carry a record")
            if self.retry_after_ms != 0:
                raise _err("SourcePoll.eoi status requires retry_after_ms=0")


@dataclass(frozen=True)
class StageSpec:
    stage_id: str
    occurrences: tuple[str, ...]
    partitions: tuple[str, ...]
    profile: ResourceProfile
    replica_min: int
    replica_target: int
    replica_max: int
    resident_bytes: int
    output_bytes: int
    state_bytes: int
    kernel_refs: tuple[ArtifactRef, ...]

    def __post_init__(self):
        _ident(self.stage_id, "StageSpec.stage_id")
        object.__setattr__(self, "occurrences", _str_tuple(self.occurrences, "StageSpec.occurrences"))
        object.__setattr__(self, "partitions", _str_tuple(self.partitions, "StageSpec.partitions"))
        _profile(self.profile, "StageSpec.profile")
        _int(self.replica_min, "StageSpec.replica_min", minimum=1)
        _int(self.replica_target, "StageSpec.replica_target", minimum=1)
        _int(self.replica_max, "StageSpec.replica_max", minimum=1)
        if not self.replica_min <= self.replica_target <= self.replica_max:
            raise _err("StageSpec replicas must satisfy min<=target<=max")
        _int(self.resident_bytes, "StageSpec.resident_bytes")
        _int(self.output_bytes, "StageSpec.output_bytes")
        _int(self.state_bytes, "StageSpec.state_bytes")
        object.__setattr__(self, "kernel_refs", _ref_tuple(self.kernel_refs, "StageSpec.kernel_refs"))


@dataclass(frozen=True)
class ExchangeSpec:
    channel_id: str
    edge_id: str
    producer_partitions: tuple[str, ...]
    consumer_partitions: tuple[str, ...]
    mode: Mode
    routing: Literal["forward", "hash", "rebalance", "broadcast", "target"]
    keys: tuple[str, ...]
    scope_id: str | None
    route_epoch: int
    credit_rows: int
    credit_bytes: int
    storage_bytes: int
    read_domain: str
    reason: str

    def __post_init__(self):
        _ident(self.channel_id, "ExchangeSpec.channel_id")
        _sha256(self.edge_id, "ExchangeSpec.edge_id")
        object.__setattr__(self, "producer_partitions", _str_tuple(self.producer_partitions, "ExchangeSpec.producer_partitions"))
        object.__setattr__(self, "consumer_partitions", _str_tuple(self.consumer_partitions, "ExchangeSpec.consumer_partitions"))
        _enum(self.mode, "ExchangeSpec.mode", _MODES)
        _enum(self.routing, "ExchangeSpec.routing", _ROUTINGS)
        object.__setattr__(self, "keys", _str_tuple(self.keys, "ExchangeSpec.keys"))
        _opt_text(self.scope_id, "ExchangeSpec.scope_id")
        _int(self.route_epoch, "ExchangeSpec.route_epoch")
        _int(self.credit_rows, "ExchangeSpec.credit_rows", minimum=1)
        _int(self.credit_bytes, "ExchangeSpec.credit_bytes", minimum=1)
        _int(self.storage_bytes, "ExchangeSpec.storage_bytes")
        _ident(self.read_domain, "ExchangeSpec.read_domain")
        _text(self.reason, "ExchangeSpec.reason", max_len=1024)


def _native_limits(value):
    allowed = {'memory_bytes', 'max_rss_bytes', 'max_scratch_bytes', 'max_row_bytes',
               'batch_rows', 'threads', 'partitions', 'timeout_s', 'admission_timeout_s',
               'cgroup_headroom_bytes'}
    if not isinstance(value, _RuntimeMapping) or set(value) - allowed:
        raise _err('unknown native resource limit')
    for key, amount in value.items():
        valid = (type(amount) in (int, float) and math.isfinite(amount) and amount > 0
                 if key.endswith('_s') else type(amount) is int and amount > 0)
        if not valid:
            raise _err('native resource limit must be finite and positive: ' + key)
    return _json_map(value, 'native_limits')


@dataclass(frozen=True)
class RuntimePolicy:
    provider: Provider
    durability: Literal["ephemeral", "durable"]
    default_partitions: int
    max_replicas: int
    fusion: bool
    checkpoint_rows: int
    checkpoint_interval_ms: int
    fairness_quantum_bytes: int
    control_timeout_ms: int
    store_id: str
    required_recovery: Recovery
    max_active_workers: int | None = None
    native_limits: Mapping[str, Json] | None = None
    worker_mode: Literal["thread"] | None = None

    def __post_init__(self):
        _enum(self.provider, "RuntimePolicy.provider", _PROVIDERS)
        _enum(self.durability, "RuntimePolicy.durability", _DURABILITIES)
        _int(self.default_partitions, "RuntimePolicy.default_partitions", minimum=1)
        _int(self.max_replicas, "RuntimePolicy.max_replicas", minimum=1)
        _bool(self.fusion, "RuntimePolicy.fusion")
        _int(self.checkpoint_rows, "RuntimePolicy.checkpoint_rows", minimum=1)
        _int(self.checkpoint_interval_ms, "RuntimePolicy.checkpoint_interval_ms", minimum=1)
        _int(self.fairness_quantum_bytes, "RuntimePolicy.fairness_quantum_bytes", minimum=1)
        _int(self.control_timeout_ms, "RuntimePolicy.control_timeout_ms", minimum=1)
        _ident(self.store_id, "RuntimePolicy.store_id")
        _enum(self.required_recovery, "RuntimePolicy.required_recovery", _RECOVERIES)
        if self.worker_mode is not None:
            _enum(self.worker_mode, "RuntimePolicy.worker_mode", frozenset({"thread"}))
            if self.provider != "local":
                raise _err("thread worker mode requires the local provider")
        if self.max_active_workers is not None:
            _int(self.max_active_workers, "RuntimePolicy.max_active_workers", minimum=1)
        if self.native_limits is not None:
            object.__setattr__(self, 'native_limits', _native_limits(self.native_limits))


@dataclass(frozen=True)
class ResidencyPlan:
    simultaneous_groups: tuple[tuple[str, ...], ...]
    control_reserved_bytes: int
    consumer_reserved_slots: int
    required_spill_bytes: int

    def __post_init__(self):
        groups = _tuple(self.simultaneous_groups, "ResidencyPlan.simultaneous_groups", lambda v, l: _str_tuple(v, l))
        object.__setattr__(self, "simultaneous_groups", groups)
        _int(self.control_reserved_bytes, "ResidencyPlan.control_reserved_bytes")
        _int(self.consumer_reserved_slots, "ResidencyPlan.consumer_reserved_slots")
        _int(self.required_spill_bytes, "ResidencyPlan.required_spill_bytes")


@dataclass(frozen=True)
class FiniteScope:
    scope_id: str
    generation: int
    membership: tuple[str, ...]
    completion_condition: Literal["input_eoi", "terminal_limit", "queue_drain_declared"]

    def __post_init__(self):
        _ident(self.scope_id, "FiniteScope.scope_id")
        _int(self.generation, "FiniteScope.generation")
        object.__setattr__(self, "membership", _str_tuple(self.membership, "FiniteScope.membership"))
        if len(self.membership) != len(set(self.membership)):
            raise _err("FiniteScope.membership must be unique")
        _enum(self.completion_condition, "FiniteScope.completion_condition", _SCOPE_CONDITIONS)


@dataclass(frozen=True)
class PhysicalDataflow:
    schema: Literal["demiflow.physical_dataflow.v3", "demiflow.physical_dataflow.v4"]
    dag_id: str
    plan_id: str
    plan_revision: int
    stages: tuple[StageSpec, ...]
    exchanges: tuple[ExchangeSpec, ...]
    residency: ResidencyPlan
    scopes: tuple[FiniteScope, ...]
    sources: tuple[SourceSnapshot, ...]
    splits: tuple[SourceSplit, ...]
    policy: RuntimePolicy
    bounds: Bounds
    recovery: Recovery
    limitations: tuple[str, ...]
    constants_ref: ArtifactRef

    jobs: tuple[FitJobStage, ...] = ()
    value_edges: tuple[ValueEdge, ...] = ()

    def __post_init__(self):
        _value_ports(self, FitJobStage, "demiflow.physical_dataflow")
        _sha256(self.dag_id, "PhysicalDataflow.dag_id")
        _sha256(self.plan_id, "PhysicalDataflow.plan_id")
        _int(self.plan_revision, "PhysicalDataflow.plan_revision")
        object.__setattr__(self, "stages", _tuple(self.stages, "PhysicalDataflow.stages", lambda v, l: _is_stage(v, l)))
        object.__setattr__(self, "exchanges", _tuple(self.exchanges, "PhysicalDataflow.exchanges", lambda v, l: _is_exchange(v, l)))
        if not isinstance(self.residency, ResidencyPlan):
            raise _err("PhysicalDataflow.residency must be a ResidencyPlan")
        object.__setattr__(self, "scopes", _tuple(self.scopes, "PhysicalDataflow.scopes", lambda v, l: _is_scope(v, l)))
        object.__setattr__(self, "sources", _tuple(self.sources, "PhysicalDataflow.sources", lambda v, l: _is_snapshot(v, l)))
        object.__setattr__(self, "splits", _tuple(self.splits, "PhysicalDataflow.splits", lambda v, l: _is_split(v, l)))
        if not isinstance(self.policy, RuntimePolicy):
            raise _err("PhysicalDataflow.policy must be a RuntimePolicy")
        if not isinstance(self.bounds, Bounds):
            raise _err("PhysicalDataflow.bounds must be a Bounds")
        _enum(self.recovery, "PhysicalDataflow.recovery", _RECOVERIES)
        object.__setattr__(self, "limitations", _tuple(self.limitations, "PhysicalDataflow.limitations", lambda v, l: _text(v, l, max_len=1024)))
        _ref(self.constants_ref, "PhysicalDataflow.constants_ref")


def _is_stage(value, label):
    if not isinstance(value, StageSpec):
        raise _err(label + " must be a StageSpec")
    return value


def _is_exchange(value, label):
    if not isinstance(value, ExchangeSpec):
        raise _err(label + " must be an ExchangeSpec")
    return value


def _is_scope(value, label):
    if not isinstance(value, FiniteScope):
        raise _err(label + " must be a FiniteScope")
    return value


def _is_snapshot(value, label):
    if not isinstance(value, SourceSnapshot):
        raise _err(label + " must be a SourceSnapshot")
    return value


def _is_split(value, label):
    if not isinstance(value, SourceSplit):
        raise _err(label + " must be a SourceSplit")
    return value


@dataclass(frozen=True)
class SealedExecution:
    logical_ref: ArtifactRef
    physical_ref: ArtifactRef
    programs: tuple[ArtifactRef, ...]
    code_pin_ref: ArtifactRef
    identity_bridge_ref: ArtifactRef
    source_refs: tuple[ArtifactRef, ...]
    constants_ref: ArtifactRef

    def __post_init__(self):
        _ref(self.logical_ref, "SealedExecution.logical_ref")
        _ref(self.physical_ref, "SealedExecution.physical_ref")
        object.__setattr__(self, "programs", _ref_tuple(self.programs, "SealedExecution.programs"))
        _ref(self.code_pin_ref, "SealedExecution.code_pin_ref")
        _ref(self.identity_bridge_ref, "SealedExecution.identity_bridge_ref")
        object.__setattr__(self, "source_refs", _ref_tuple(self.source_refs, "SealedExecution.source_refs"))
        _ref(self.constants_ref, "SealedExecution.constants_ref")


@dataclass(frozen=True)
class ReadyWork:
    stage_id: str
    partition_id: str
    input_refs: tuple[ArtifactRef, ...]
    resident_bytes: int
    operation_id: str

    def __post_init__(self):
        _ident(self.stage_id, "ReadyWork.stage_id")
        _ident(self.partition_id, "ReadyWork.partition_id")
        object.__setattr__(self, "input_refs", _ref_tuple(self.input_refs, "ReadyWork.input_refs"))
        _int(self.resident_bytes, "ReadyWork.resident_bytes")
        _sha256(self.operation_id, "ReadyWork.operation_id")


@dataclass(frozen=True)
class WorkerLease:
    allocation_id: str
    boot_id: str
    stage_id: str
    partition_ids: tuple[str, ...]
    host_id: str
    endpoint_ref: str
    grant_ref: ArtifactRef
    expires_at_ms: int

    def __post_init__(self):
        _ident(self.allocation_id, "WorkerLease.allocation_id")
        _ident(self.boot_id, "WorkerLease.boot_id")
        _ident(self.stage_id, "WorkerLease.stage_id")
        object.__setattr__(self, "partition_ids", _str_tuple(self.partition_ids, "WorkerLease.partition_ids"))
        _ident(self.host_id, "WorkerLease.host_id")
        _text(self.endpoint_ref, "WorkerLease.endpoint_ref", max_len=1024)
        _ref(self.grant_ref, "WorkerLease.grant_ref")
        _int(self.expires_at_ms, "WorkerLease.expires_at_ms")


@dataclass(frozen=True)
class WorkerAssignment:
    schema: Literal["demiflow.dataflow_assignment.v3", "demiflow.dataflow_assignment.v4"]
    run_id: str
    action_id: str
    attempt: int
    fence: str
    plan_id: str
    plan_revision: int
    lease: WorkerLease
    sealed: SealedExecution
    stages: tuple[StageSpec, ...]
    inputs: tuple[ExchangeSpec, ...]
    outputs: tuple[ExchangeSpec, ...]
    source_splits: tuple[SourceSplit, ...]
    restore_refs: tuple[ArtifactRef, ...]
    bounds: Bounds
    native_limits: Mapping[str, Json] | None = None

    model_bindings: Mapping[str, Json] | None = None

    def __post_init__(self):
        if self.model_bindings is not None:
            object.__setattr__(self, "model_bindings", _json_map(self.model_bindings, "WorkerAssignment.model_bindings"))
        version = "v4" if self.model_bindings is not None else "v3"
        if self.schema != "demiflow.dataflow_assignment." + version:
            raise _err("assignment model bindings differ from its schema")
        _ident(self.run_id, "WorkerAssignment.run_id")
        _ident(self.action_id, "WorkerAssignment.action_id")
        _int(self.attempt, "WorkerAssignment.attempt")
        _ident(self.fence, "WorkerAssignment.fence")
        _sha256(self.plan_id, "WorkerAssignment.plan_id")
        _int(self.plan_revision, "WorkerAssignment.plan_revision")
        if not isinstance(self.lease, WorkerLease):
            raise _err("WorkerAssignment.lease must be a WorkerLease")
        if not isinstance(self.sealed, SealedExecution):
            raise _err("WorkerAssignment.sealed must be a SealedExecution")
        object.__setattr__(self, "stages", _tuple(self.stages, "WorkerAssignment.stages", _is_stage))
        object.__setattr__(self, "inputs", _tuple(self.inputs, "WorkerAssignment.inputs", _is_exchange))
        object.__setattr__(self, "outputs", _tuple(self.outputs, "WorkerAssignment.outputs", _is_exchange))
        object.__setattr__(self, "source_splits", _tuple(self.source_splits, "WorkerAssignment.source_splits", _is_split))
        object.__setattr__(self, "restore_refs", _ref_tuple(self.restore_refs, "WorkerAssignment.restore_refs"))
        if not isinstance(self.bounds, Bounds):
            raise _err("WorkerAssignment.bounds must be a Bounds")
        if self.native_limits is not None:
            object.__setattr__(self, 'native_limits', _native_limits(self.native_limits))


@dataclass(frozen=True)
class CutRequest:
    cut_id: str
    sequence: int
    plan_revision: int
    route_epoch: int
    participants: tuple[str, ...]
    final: bool

    def __post_init__(self):
        _sha256(self.cut_id, "CutRequest.cut_id")
        _int(self.sequence, "CutRequest.sequence")
        _int(self.plan_revision, "CutRequest.plan_revision")
        _int(self.route_epoch, "CutRequest.route_epoch")
        object.__setattr__(self, "participants", _str_tuple(self.participants, "CutRequest.participants"))
        if len(self.participants) != len(set(self.participants)):
            raise _err("CutRequest.participants must be unique")
        _bool(self.final, "CutRequest.final")


@dataclass(frozen=True)
class CutContribution:
    cut_id: str
    participant_id: str
    boot_id: str
    sources: tuple[SourceCursor, ...]
    operator_states: Mapping[str, ArtifactRef]
    channels: Mapping[str, Mapping[str, Json]]
    buffers: tuple[ArtifactRef, ...]
    effects: tuple[ArtifactRef, ...]
    sinks: Mapping[str, ArtifactRef]
    final: bool

    def __post_init__(self):
        _sha256(self.cut_id, "CutContribution.cut_id")
        _ident(self.participant_id, "CutContribution.participant_id")
        _ident(self.boot_id, "CutContribution.boot_id")
        object.__setattr__(self, "sources", _tuple(self.sources, "CutContribution.sources", lambda v, l: _is_cursor(v, l)))
        object.__setattr__(self, "operator_states", _ref_map(self.operator_states, "CutContribution.operator_states"))
        if not isinstance(self.channels, _RuntimeMapping):
            raise _err("CutContribution.channels must be a mapping")
        channels = {}
        for key, value in self.channels.items():
            _ident(key, "CutContribution.channels key")
            channels[key] = _json_map(value, "CutContribution.channels." + key)
        object.__setattr__(self, "channels", MappingProxyType(channels))
        object.__setattr__(self, "buffers", _ref_tuple(self.buffers, "CutContribution.buffers"))
        object.__setattr__(self, "effects", _ref_tuple(self.effects, "CutContribution.effects"))
        object.__setattr__(self, "sinks", _ref_map(self.sinks, "CutContribution.sinks"))
        _bool(self.final, "CutContribution.final")


def _is_cursor(value, label):
    if not isinstance(value, SourceCursor):
        raise _err(label + " must be a SourceCursor")
    return value


@dataclass(frozen=True)
class RecoveryImage:
    commit_id: str
    cut: CutRequest
    contributions: tuple[CutContribution, ...]
    confirmed_sinks: Mapping[str, ArtifactRef]
    source_vector: tuple[SourceCursor, ...]

    model_bindings: Mapping[str, Json] | None = None

    def __post_init__(self):
        if self.model_bindings is not None:
            object.__setattr__(self, "model_bindings", _json_map(self.model_bindings, "RecoveryImage.model_bindings"))
        _sha256(self.commit_id, "RecoveryImage.commit_id")
        if not isinstance(self.cut, CutRequest):
            raise _err("RecoveryImage.cut must be a CutRequest")
        object.__setattr__(self, "contributions", _tuple(self.contributions, "RecoveryImage.contributions", lambda v, l: _is_contribution(v, l)))
        object.__setattr__(self, "confirmed_sinks", _ref_map(self.confirmed_sinks, "RecoveryImage.confirmed_sinks"))
        object.__setattr__(self, "source_vector", _tuple(self.source_vector, "RecoveryImage.source_vector", _is_cursor))


def _is_contribution(value, label):
    if not isinstance(value, CutContribution):
        raise _err(label + " must be a CutContribution")
    return value


@dataclass(frozen=True)
class EffectCommand:
    operation_id: str
    kind: Literal["lookup", "reserve", "mark_sent", "confirm", "unknown", "release_unsent"]
    effect_id: str
    actor_id: str
    request_digest: str
    payload_ref: ArtifactRef | None
    budget: Mapping[str, int]

    def __post_init__(self):
        _sha256(self.operation_id, "EffectCommand.operation_id")
        _enum(self.kind, "EffectCommand.kind", _EFFECT_KINDS)
        _ident(self.effect_id, "EffectCommand.effect_id")
        _ident(self.actor_id, "EffectCommand.actor_id")
        _sha256(self.request_digest, "EffectCommand.request_digest")
        _opt_ref(self.payload_ref, "EffectCommand.payload_ref")
        object.__setattr__(self, "budget", _int_map(self.budget, "EffectCommand.budget"))


@dataclass(frozen=True)
class EffectReply:
    operation_id: str
    status: Literal["absent", "reserved", "sent", "confirmed", "unknown", "unsent"]
    result_ref: ArtifactRef | None
    receipt_ref: ArtifactRef | None

    def __post_init__(self):
        _sha256(self.operation_id, "EffectReply.operation_id")
        _enum(self.status, "EffectReply.status", _EFFECT_STATUSES)
        _opt_ref(self.result_ref, "EffectReply.result_ref")
        _opt_ref(self.receipt_ref, "EffectReply.receipt_ref")


@dataclass(frozen=True)
class WorkerEvent:
    schema: Literal["demiflow.dataflow_event.v3"]
    kind: Literal["ready", "progress", "cut", "effect", "artifact", "reserve", "read_action", "read_configuration", "restrict_recovery", "check_grant", "queue", "native_request", "failure", "eoi", "closed", "data", "ack", "idle", "terminal", "checkpoint_wait"]
    operation_id: str
    run_id: str
    attempt: int
    fence: str
    allocation_id: str
    boot_id: str
    plan_revision: int
    scope_id: str
    controller_epoch: int
    payload_ref: ArtifactRef | None
    payload: Mapping[str, Json] | None

    def __post_init__(self):
        if self.schema != "demiflow.dataflow_event.v3":
            raise _err("WorkerEvent.schema must be demiflow.dataflow_event.v3")
        _enum(self.kind, "WorkerEvent.kind", _EVENT_KINDS)
        _sha256(self.operation_id, "WorkerEvent.operation_id")
        _ident(self.run_id, "WorkerEvent.run_id")
        _int(self.attempt, "WorkerEvent.attempt")
        _ident(self.fence, "WorkerEvent.fence")
        _ident(self.allocation_id, "WorkerEvent.allocation_id")
        _ident(self.boot_id, "WorkerEvent.boot_id")
        _int(self.plan_revision, "WorkerEvent.plan_revision")
        _ident(self.scope_id, "WorkerEvent.scope_id")
        _int(self.controller_epoch, "WorkerEvent.controller_epoch")
        _opt_ref(self.payload_ref, "WorkerEvent.payload_ref")
        _exactly_one(self.payload_ref, self.payload, "WorkerEvent payload")
        if self.payload is not None:
            object.__setattr__(self, "payload", _json_map(self.payload, "WorkerEvent.payload"))


@dataclass(frozen=True)
class TerminalResult:
    kind: str
    rows: int | None
    scalar: Json
    result_ref: ArtifactRef | None
    write_result: WriteResult | None
    complete: bool
    completion: Literal["source_exhausted", "terminal_satisfied", "cancelled", "incomplete"]

    def __post_init__(self):
        _ident(self.kind, "TerminalResult.kind")
        if self.rows is not None:
            _int(self.rows, "TerminalResult.rows")
        object.__setattr__(self, "scalar", _json_scalar(self.scalar, "TerminalResult.scalar"))
        _opt_ref(self.result_ref, "TerminalResult.result_ref")
        _write_result(self.write_result, "TerminalResult.write_result")
        _bool(self.complete, "TerminalResult.complete")
        _enum(self.completion, "TerminalResult.completion", _COMPLETIONS)


@dataclass(frozen=True)
class CapabilityReport:
    supported: bool
    reasons: tuple[str, ...]
    recovery: Recovery
    required_domains: tuple[str, ...]
    required_profiles: tuple[ResourceProfile, ...]

    def __post_init__(self):
        _bool(self.supported, "CapabilityReport.supported")
        object.__setattr__(self, "reasons", _tuple(self.reasons, "CapabilityReport.reasons", lambda v, l: _text(v, l, max_len=1024)))
        _enum(self.recovery, "CapabilityReport.recovery", _RECOVERIES)
        object.__setattr__(self, "required_domains", _str_tuple(self.required_domains, "CapabilityReport.required_domains"))
        object.__setattr__(self, "required_profiles", _tuple(self.required_profiles, "CapabilityReport.required_profiles", _profile))


@dataclass(frozen=True)
class HostDescriptor:
    host_id: str
    provider: Provider
    runtime_revision: str
    store_domains: tuple[str, ...]
    logical_resources: Mapping[str, float]
    isolation_capabilities: tuple[str, ...]

    def __post_init__(self):
        _ident(self.host_id, "HostDescriptor.host_id")
        _enum(self.provider, "HostDescriptor.provider", _PROVIDERS)
        _text(self.runtime_revision, "HostDescriptor.runtime_revision")
        object.__setattr__(self, "store_domains", _str_tuple(self.store_domains, "HostDescriptor.store_domains"))
        object.__setattr__(self, "logical_resources", _float_map(self.logical_resources, "HostDescriptor.logical_resources"))
        object.__setattr__(self, "isolation_capabilities", _str_tuple(self.isolation_capabilities, "HostDescriptor.isolation_capabilities"))


@dataclass(frozen=True)
class HistoricalBinding:
    schema_version: str
    original_plan_ref: ArtifactRef
    original_code_pin_ref: ArtifactRef
    compatible: bool
    reason: str | None
    restore_image: RecoveryImage | None

    def __post_init__(self):
        _text(self.schema_version, "HistoricalBinding.schema_version")
        _ref(self.original_plan_ref, "HistoricalBinding.original_plan_ref")
        _ref(self.original_code_pin_ref, "HistoricalBinding.original_code_pin_ref")
        _bool(self.compatible, "HistoricalBinding.compatible")
        _opt_text(self.reason, "HistoricalBinding.reason", max_len=1024)
        if self.restore_image is not None and not isinstance(self.restore_image, RecoveryImage):
            raise _err("HistoricalBinding.restore_image must be a RecoveryImage")


@dataclass(frozen=True)
class PlanInputBinding:
    port: str
    source: SourcePlan
    plan: LogicalPlan
    path: tuple[str, ...]

    def __post_init__(self):
        _ident(self.port, "PlanInputBinding.port")
        object.__setattr__(self, "path", _str_tuple(self.path, "PlanInputBinding.path"))


@dataclass(frozen=True)
class OperatorDefinition:
    kind: str
    compiler_symbol: str
    kernel_symbol: str
    inputs: tuple[str, ...]

    def __post_init__(self):
        _ident(self.kind, "OperatorDefinition.kind")
        _text(self.compiler_symbol, "OperatorDefinition.compiler_symbol")
        _text(self.kernel_symbol, "OperatorDefinition.kernel_symbol")
        object.__setattr__(self, "inputs", _str_tuple(self.inputs, "OperatorDefinition.inputs"))


@dataclass(frozen=True)
class SourceDefinition:
    kind: str
    reader_symbol: str
    cursor_fields: tuple[str, ...]

    def __post_init__(self):
        _ident(self.kind, "SourceDefinition.kind")
        _text(self.reader_symbol, "SourceDefinition.reader_symbol")
        object.__setattr__(self, "cursor_fields", _str_tuple(self.cursor_fields, "SourceDefinition.cursor_fields"))


@dataclass(frozen=True)
class TerminalDefinition:
    kind: str
    adapter_symbol: str
    result_kind: str

    def __post_init__(self):
        _ident(self.kind, "TerminalDefinition.kind")
        _text(self.adapter_symbol, "TerminalDefinition.adapter_symbol")
        _ident(self.result_kind, "TerminalDefinition.result_kind")


@dataclass(frozen=True)
class EncodedRecord:
    record_id: str
    encoding: str
    data: bytes | None
    ref: ArtifactRef | None
    encoded_bytes: int
    decoded_upper_bytes: int
    sequence: int

    def __post_init__(self):
        _sha256(self.record_id, "EncodedRecord.record_id")
        _text(self.encoding, "EncodedRecord.encoding")
        if self.data is not None and type(self.data) is not bytes:
            raise _err("EncodedRecord.data must be bytes")
        _opt_ref(self.ref, "EncodedRecord.ref")
        _exactly_one(self.data, self.ref, "EncodedRecord payload")
        _int(self.encoded_bytes, "EncodedRecord.encoded_bytes")
        _int(self.decoded_upper_bytes, "EncodedRecord.decoded_upper_bytes")
        _int(self.sequence, "EncodedRecord.sequence")


@dataclass(frozen=True)
class RoutedRecord:
    reader_id: str
    delivery_id: str
    record: Record

    def __post_init__(self):
        _ident(self.reader_id, "RoutedRecord.reader_id")
        _ident(self.delivery_id, "RoutedRecord.delivery_id")
        if not isinstance(self.record, Record):
            raise _err("RoutedRecord.record must be a Record")


@dataclass(frozen=True)
class ProducerCompletion:
    partition_id: str
    generation: int
    status: Literal["succeeded", "failed", "cancelled", "unknown"]
    last_sequence: int
    manifest_ref: ArtifactRef

    def __post_init__(self):
        _ident(self.partition_id, "ProducerCompletion.partition_id")
        _int(self.generation, "ProducerCompletion.generation")
        _enum(self.status, "ProducerCompletion.status", _PRODUCER_STATUSES)
        # No record was emitted by an empty producer: its last index is -1.
        _int(self.last_sequence, "ProducerCompletion.last_sequence", minimum=-1)
        _ref(self.manifest_ref, "ProducerCompletion.manifest_ref")


@dataclass(frozen=True)
class CreditLease:
    operation_id: str
    channel_id: str
    reader_id: str
    rows: int
    size_bytes: int

    def __post_init__(self):
        _sha256(self.operation_id, "CreditLease.operation_id")
        _ident(self.channel_id, "CreditLease.channel_id")
        _ident(self.reader_id, "CreditLease.reader_id")
        _int(self.rows, "CreditLease.rows")
        _int(self.size_bytes, "CreditLease.size_bytes")


@dataclass(frozen=True)
class AggregateState:
    kind: str
    schema_version: str
    state_ref: ArtifactRef
    count: int

    def __post_init__(self):
        _ident(self.kind, "AggregateState.kind")
        _text(self.schema_version, "AggregateState.schema_version")
        _ref(self.state_ref, "AggregateState.state_ref")
        _int(self.count, "AggregateState.count")


@dataclass(frozen=True)
class DomainProof:
    host_id: str
    store_id: str
    challenge_digest: str
    namespace_digest: str
    durable: bool

    def __post_init__(self):
        _ident(self.host_id, "DomainProof.host_id")
        _ident(self.store_id, "DomainProof.store_id")
        _sha256(self.challenge_digest, "DomainProof.challenge_digest")
        _sha256(self.namespace_digest, "DomainProof.namespace_digest")
        _bool(self.durable, "DomainProof.durable")


@dataclass(frozen=True)
class DataLocation:
    domain: str
    temporary_locator: str | None
    durable_ref: ArtifactRef

    def __post_init__(self):
        _ident(self.domain, "DataLocation.domain")
        _opt_text(self.temporary_locator, "DataLocation.temporary_locator", max_len=1024)
        _ref(self.durable_ref, "DataLocation.durable_ref")


@dataclass(frozen=True)
class WorkerStep:
    processed: int
    blocked: bool
    end_of_input: bool
    events: tuple[WorkerEvent, ...]

    def __post_init__(self):
        _int(self.processed, "WorkerStep.processed")
        _bool(self.blocked, "WorkerStep.blocked")
        _bool(self.end_of_input, "WorkerStep.end_of_input")
        object.__setattr__(self, "events", _tuple(self.events, "WorkerStep.events", lambda v, l: _is_event(v, l)))


def _is_event(value, label):
    if not isinstance(value, WorkerEvent):
        raise _err(label + " must be a WorkerEvent")
    return value


@dataclass(frozen=True)
class ExecutionStatus:
    state: Literal["preparing", "running", "draining", "succeeded", "failed", "cancelled"]
    pending_events: int
    confirmed_rows: int

    def __post_init__(self):
        _enum(self.state, "ExecutionStatus.state", _EXECUTION_STATES)
        _int(self.pending_events, "ExecutionStatus.pending_events")
        _int(self.confirmed_rows, "ExecutionStatus.confirmed_rows")


@dataclass(frozen=True)
class NodeObservation:
    occurrence_id: str
    attempt: int
    plan_id: str
    observed: bool
    metrics: Mapping[str, Json]

    def __post_init__(self):
        _sha256(self.occurrence_id, "NodeObservation.occurrence_id")
        _int(self.attempt, "NodeObservation.attempt")
        _sha256(self.plan_id, "NodeObservation.plan_id")
        _bool(self.observed, "NodeObservation.observed")
        object.__setattr__(self, "metrics", _json_map(self.metrics, "NodeObservation.metrics"))


@dataclass(frozen=True)
class RuntimeServices:
    store: ArtifactStore
    journal: WorkerJournalPort
    bounds: Bounds
    assignment: WorkerAssignment
    clock_ms: Callable[[], int]
    check_stop: Callable[[], None]
    constants: Mapping[str, Json]
    programs: Mapping[str, LoadedProgram]

    # Installed by the worker host. SQL and all relational semantics belong
    # to the operator; this one port supervises the process under the grant.
    query_region_offsets: Callable[..., list[tuple[int | None, ...]] | None] | None = None

    def __post_init__(self):
        if not isinstance(self.bounds, Bounds):
            raise _err("RuntimeServices.bounds must be a Bounds")
        if not isinstance(self.assignment, WorkerAssignment):
            raise _err("RuntimeServices.assignment must be a WorkerAssignment")
        object.__setattr__(self, "constants", _json_map(self.constants, "RuntimeServices.constants"))
        if not isinstance(self.programs, _RuntimeMapping):
            raise _err("RuntimeServices.programs must be a mapping")
        programs = {}
        for key, value in self.programs.items():
            _ident(key, "RuntimeServices.programs key")
            if not isinstance(value, LoadedProgram):
                raise _err("RuntimeServices.programs values must be LoadedProgram")
            programs[key] = value
        object.__setattr__(self, "programs", MappingProxyType(programs))


@dataclass(frozen=True)
class DriverServices:
    action: RunAction
    journal_service: DriverAuthority
    store: ArtifactStore
    resources: ResourceManager
    policy: RuntimePolicy
    bounds: Bounds
    hosts: tuple[HostDescriptor, ...]
    clock_ms: Callable[[], int]
    job_resources: Callable[[str], ResourceManager] | None = None
    job_gpu: int = 0

    def __post_init__(self):
        if not isinstance(self.policy, RuntimePolicy):
            raise _err("DriverServices.policy must be a RuntimePolicy")
        if not isinstance(self.bounds, Bounds):
            raise _err("DriverServices.bounds must be a Bounds")
        object.__setattr__(self, "hosts", _tuple(self.hosts, "DriverServices.hosts", lambda v, l: _is_host(v, l)))


def _is_host(value, label):
    if not isinstance(value, HostDescriptor):
        raise _err(label + " must be a HostDescriptor")
    return value


@dataclass(frozen=True)
class ScheduleChoice:
    work: ReadyWork | None
    retry_after_ms: int
    reason: str | None

    def __post_init__(self):
        if self.work is not None and not isinstance(self.work, ReadyWork):
            raise _err("ScheduleChoice.work must be a ReadyWork")
        _int(self.retry_after_ms, "ScheduleChoice.retry_after_ms")
        if self.work is None and self.retry_after_ms <= 0:
            raise _err("ScheduleChoice.work=None must be a bounded wait (retry_after_ms>0)")
        _opt_text(self.reason, "ScheduleChoice.reason", max_len=1024)


@dataclass(frozen=True)
class ArtifactCommand:
    """Artifact control only; business bytes remain in immutable objects.

    begin_staging: operation_id=reservation, owner=store_id, ref=None,
    size_bytes=initial admission; the writer already holds its physical lease.
    prepare_staging: owner=reservation, ref=exact future published object,
    size_bytes=ref.size_bytes; durable intent must precede any hardlink.
    grow/settle/release retain owner=reservation and existing idempotency rules.
    """
    operation_id: str
    kind: Literal["pin", "unpin", "inspect", "reserve_staging", "begin_staging", "prepare_staging", "grow_staging", "settle_staging", "release_staging"]
    ref: ArtifactRef | None
    owner: str
    size_bytes: int

    def __post_init__(self):
        _sha256(self.operation_id, "ArtifactCommand.operation_id")
        _enum(self.kind, "ArtifactCommand.kind", _ARTIFACT_KINDS)
        _opt_ref(self.ref, "ArtifactCommand.ref")
        _ident(self.owner, "ArtifactCommand.owner")
        _int(self.size_bytes, "ArtifactCommand.size_bytes")


@dataclass(frozen=True)
class ArtifactReply:
    operation_id: str
    present: bool | None
    charged_bytes: int

    def __post_init__(self):
        _sha256(self.operation_id, "ArtifactReply.operation_id")
        if self.present is not None:
            _bool(self.present, "ArtifactReply.present")
        _int(self.charged_bytes, "ArtifactReply.charged_bytes")


@dataclass(frozen=True)
class ControlReply:
    operation_id: str
    status: Literal["ok", "error", "pending"]
    payload: Mapping[str, Json] | None
    payload_ref: ArtifactRef | None
    error_code: str | None

    def __post_init__(self):
        _sha256(self.operation_id, "ControlReply.operation_id")
        _enum(self.status, "ControlReply.status", _REPLY_STATUSES)
        _at_most_one(self.payload, self.payload_ref, "ControlReply payload")
        if self.payload is not None:
            object.__setattr__(self, "payload", _json_map(self.payload, "ControlReply.payload"))
        _opt_ref(self.payload_ref, "ControlReply.payload_ref")
        if self.error_code is not None and self.error_code not in DATAFLOW_ERROR_CODES:
            raise _err("ControlReply.error_code must be a closed dataflow error code")
        if self.status == "error":
            if self.error_code is None:
                raise _err("ControlReply.error status requires error_code")
        elif self.error_code is not None:
            raise _err("ControlReply non-error status must not carry error_code")


@dataclass(frozen=True)
class CompletedControl:
    allocation_id: str
    boot_id: str
    reply: ControlReply

    def __post_init__(self):
        _ident(self.allocation_id, "CompletedControl.allocation_id")
        _ident(self.boot_id, "CompletedControl.boot_id")
        if not isinstance(self.reply, ControlReply):
            raise _err("CompletedControl.reply must be a ControlReply")


@dataclass(frozen=True)
class NativeJournalCommand:
    operation_id: str
    operation: Literal["lookup", "reserve", "reserve_http_retry", "response", "failed", "references", "media_references", "bind_membership", "stage_image", "offline_lookup", "stream_lance", "batch_membership"]
    action_id: str
    node_id: str
    request_key: str
    request_ref: ArtifactRef
    value_ref: ArtifactRef | None
    reservation: tuple[str, Mapping[str, tuple[int, int]]] | None
    options: Mapping[str, Json]

    def __post_init__(self):
        _sha256(self.operation_id, "NativeJournalCommand.operation_id")
        _enum(self.operation, "NativeJournalCommand.operation", _JOURNAL_OPS)
        _ident(self.action_id, "NativeJournalCommand.action_id")
        _ident(self.node_id, "NativeJournalCommand.node_id")
        _text(self.request_key, "NativeJournalCommand.request_key", max_len=512)
        _ref(self.request_ref, "NativeJournalCommand.request_ref")
        _opt_ref(self.value_ref, "NativeJournalCommand.value_ref")
        if self.reservation is not None:
            object.__setattr__(self, "reservation", _freeze_reservation(self.reservation))
        object.__setattr__(self, "options", _json_map(self.options, "NativeJournalCommand.options"))


def _freeze_reservation(value):
    if type(value) not in (tuple, list) or len(value) != 2:
        raise _err("NativeJournalCommand.reservation must be a (id, charges) pair")
    reservation_id, charges = value
    _ident(reservation_id, "NativeJournalCommand.reservation id")
    if not isinstance(charges, _RuntimeMapping):
        raise _err("NativeJournalCommand.reservation charges must be a mapping")
    frozen = {}
    for key, amount in charges.items():
        _ident(key, "NativeJournalCommand.reservation charge key")
        if type(amount) not in (tuple, list) or len(amount) != 2:
            raise _err("NativeJournalCommand.reservation charge must be an (amount, limit) pair")
        frozen[key] = (_int(amount[0], "reservation amount"), _int(amount[1], "reservation limit"))
    return (reservation_id, MappingProxyType(frozen))


@dataclass(frozen=True)
class NativeJournalReply:
    operation_id: str
    value: Json
    value_ref: ArtifactRef | None

    def __post_init__(self):
        _sha256(self.operation_id, "NativeJournalReply.operation_id")
        object.__setattr__(self, "value", _json_scalar(self.value, "NativeJournalReply.value"))
        _opt_ref(self.value_ref, "NativeJournalReply.value_ref")
        _at_most_one(self.value, self.value_ref, "NativeJournalReply value")


@dataclass(frozen=True)
class NativeRunView:
    limits: Mapping[str, Json]
    fence: str
    check_stop: Callable[[], None]

    def __post_init__(self):
        object.__setattr__(self, "limits", _json_map(self.limits, "NativeRunView.limits"))
        _ident(self.fence, "NativeRunView.fence")


@dataclass(frozen=True)
class NativeActionView:
    action_id: str
    context: NativeRunView
    object_sinks: Mapping[str, object]
    native_image_effects: bool

    def __post_init__(self):
        _ident(self.action_id, "NativeActionView.action_id")
        if not isinstance(self.context, NativeRunView):
            raise _err("NativeActionView.context must be a NativeRunView")
        if not isinstance(self.object_sinks, _RuntimeMapping):
            raise _err("NativeActionView.object_sinks must be a mapping")
        object.__setattr__(self, "object_sinks", MappingProxyType(dict(self.object_sinks)))
        _bool(self.native_image_effects, "NativeActionView.native_image_effects")


@dataclass(frozen=True)
class LoadedProgram:
    kind: Literal["source", "callable", "aggregate", "sink", "native"]
    value: object
    digest: str

    def __post_init__(self):
        _enum(self.kind, "LoadedProgram.kind", _PROGRAM_KINDS)
        _sha256(self.digest, "LoadedProgram.digest")


@dataclass(frozen=True)
class ActionProgram:
    name: str
    kind: Literal["aggregate", "sink"]
    value: object

    def __post_init__(self):
        _ident(self.name, "ActionProgram.name")
        _enum(self.kind, "ActionProgram.kind", _ACTION_PROGRAM_KINDS)


@dataclass(frozen=True)
class QueueBinding:
    action_id: str
    source_id: str | None
    mode: Literal["sink", "explicit", "raw_fetch", "producer", "native_search"]
    contract_digest: str
    generation: str | None
    pool: str | None
    source_occurrence: str | None
    fetch_occurrence: str
    final_occurrence: str | None

    def __post_init__(self):
        _ident(self.action_id, "QueueBinding.action_id")
        if self.source_id is not None:
            _sha256(self.source_id, "QueueBinding.source_id")
        _enum(self.mode, "QueueBinding.mode", _QUEUE_MODES)
        _sha256(self.contract_digest, "QueueBinding.contract_digest")
        _opt_text(self.generation, "QueueBinding.generation")
        _opt_text(self.pool, "QueueBinding.pool")
        if self.source_occurrence is not None:
            _sha256(self.source_occurrence, "QueueBinding.source_occurrence")
        _sha256(self.fetch_occurrence, "QueueBinding.fetch_occurrence")
        if self.final_occurrence is not None:
            _sha256(self.final_occurrence, "QueueBinding.final_occurrence")


@dataclass(frozen=True)
class QueueCommand:
    operation_id: str
    operation: Literal["poll", "acquire", "finalize", "status", "search"]
    binding: QueueBinding
    record_id: str | None
    delivery_ref: ArtifactRef | None
    row_ref: ArtifactRef | None
    node_id: str | None = None

    def __post_init__(self):
        _sha256(self.operation_id, "QueueCommand.operation_id")
        _enum(self.operation, "QueueCommand.operation", _QUEUE_OPS)
        if not isinstance(self.binding, QueueBinding):
            raise _err("QueueCommand.binding must be a QueueBinding")
        if self.record_id is not None:
            _sha256(self.record_id, "QueueCommand.record_id")
        _opt_ref(self.delivery_ref, "QueueCommand.delivery_ref")
        _opt_ref(self.row_ref, "QueueCommand.row_ref")
        if self.node_id is not None:
            _sha256(self.node_id, "QueueCommand.node_id")
        if self.operation == 'search' and self.node_id is None:
            raise _err('Native search requires an explicit owning occurrence')
        if self.node_id is not None and not (self.operation == 'search' or
                self.operation == 'acquire' and self.binding.mode == 'raw_fetch'):
            raise _err('Only native search or ordinary fetch accepts an owning occurrence')
        if self.operation in ("acquire", "finalize", "search"):
            if self.row_ref is None:
                raise _err("QueueCommand." + self.operation + " requires row_ref")
        else:
            if self.row_ref is not None or self.delivery_ref is not None:
                raise _err("QueueCommand." + self.operation + " must not carry business payload")


@dataclass(frozen=True)
class QueueReply:
    operation_id: str
    status: Literal["record", "pending", "eoi", "confirmed", "unknown"]
    delivery_ref: ArtifactRef | None
    row_ref: ArtifactRef | None
    native_fact_ref: ArtifactRef | None
    retry_after_ms: int

    def __post_init__(self):
        _sha256(self.operation_id, "QueueReply.operation_id")
        _enum(self.status, "QueueReply.status", _QUEUE_STATUSES)
        _opt_ref(self.delivery_ref, "QueueReply.delivery_ref")
        _opt_ref(self.row_ref, "QueueReply.row_ref")
        _opt_ref(self.native_fact_ref, "QueueReply.native_fact_ref")
        _int(self.retry_after_ms, "QueueReply.retry_after_ms")


@dataclass(frozen=True)
class ProgramDeclaration:
    occurrence_id: str
    callable_spec: CallableSpec | None
    source_spec: SourcePlan | None
    action_program: ActionProgram | None
    code_identity: Mapping[str, Json]

    def __post_init__(self):
        _sha256(self.occurrence_id, "ProgramDeclaration.occurrence_id")
        present = sum(
            value is not None
            for value in (self.callable_spec, self.source_spec, self.action_program)
        )
        if present != 1:
            raise _err("ProgramDeclaration requires exactly one of callable_spec/source_spec/action_program")
        if self.action_program is not None and not isinstance(self.action_program, ActionProgram):
            raise _err("ProgramDeclaration.action_program must be an ActionProgram")
        object.__setattr__(self, "code_identity", _json_map(self.code_identity, "ProgramDeclaration.code_identity"))


@dataclass(frozen=True)
class CompiledDataflow:
    graph: DataflowGraph
    declarations: tuple[ProgramDeclaration, ...]
    report_bindings: tuple[Mapping[str, Json], ...] = ()
    observation_labels: Mapping[str, str] | None = None

    def __post_init__(self):
        if not isinstance(self.graph, DataflowGraph):
            raise _err("CompiledDataflow.graph must be a DataflowGraph")
        object.__setattr__(self, "declarations", _tuple(self.declarations, "CompiledDataflow.declarations", lambda v, l: _is_declaration(v, l)))
        object.__setattr__(self, "report_bindings", tuple(_json_map(v, "report binding") for v in self.report_bindings))
        if self.observation_labels is not None:
            labels = _json_map(self.observation_labels, 'observation labels')
            if (not set(labels) <= {n.occurrence_id for n in self.graph.nodes}
                    or any(type(v) is not str or not v for v in labels.values())
                    or len(set(labels.values())) != len(labels)):
                raise _err('observation labels must uniquely name graph occurrences')
            object.__setattr__(self, 'observation_labels', labels)


def _is_declaration(value, label):
    if not isinstance(value, ProgramDeclaration):
        raise _err(label + " must be a ProgramDeclaration")
    return value


@dataclass(frozen=True)
class ActionRequest:
    kind: str
    source: SourcePlan
    plan: LogicalPlan
    arguments: Mapping[str, Json]
    sink: Datasink | None
    programs: tuple[ActionProgram, ...]
    provider: Provider
    action: RunAction

    def __post_init__(self):
        _enum(self.kind, "ActionRequest.kind", _ACTION_KINDS)
        object.__setattr__(self, "arguments", _json_map(self.arguments, "ActionRequest.arguments"))
        if self.sink is not None and not isinstance(self.sink, Datasink):
            raise _err("ActionRequest.sink must be a Datasink")
        object.__setattr__(self, "programs", _tuple(self.programs, "ActionRequest.programs", lambda v, l: _is_action_program(v, l)))
        _enum(self.provider, "ActionRequest.provider", _PROVIDERS)


def _is_action_program(value, label):
    if not isinstance(value, ActionProgram):
        raise _err(label + " must be an ActionProgram")
    return value


# One declaration type is shared by public API, compiler and runtime. Two
# structurally identical dataclasses still fail isinstance and split identity.
from ...data.dataflow_operation import NativeOperation


@runtime_checkable
class ArtifactStore(Protocol):
    def scratch(self, *, max_bytes: int, max_io_bytes: int) -> Any: ...
    def put(self, chunks: Iterator[bytes], *, max_bytes: int, media_type: str) -> ArtifactRef: ...
    def read(self, ref: ArtifactRef, *, max_bytes: int) -> Iterator[bytes]: ...
    def inspect(self, ref: ArtifactRef) -> bool: ...
    def retain(self, ref: ArtifactRef, *, owner: str) -> None: ...
    def release(self, ref: ArtifactRef, *, owner: str) -> None: ...


@runtime_checkable
class SourceReader(Protocol):
    def open(self, split: SourceSplit, cursor: SourceCursor | None) -> None: ...
    def next_record(self) -> SourcePoll: ...
    def snapshot(self) -> SourceCursor: ...
    def close(self) -> None: ...


@runtime_checkable
class Kernel(Protocol):
    def start(self) -> None: ...
    def push(self, port: str, record: Record) -> Iterator[Record]: ...
    def finish(self, port: str) -> Iterator[Record]: ...
    def snapshot(self) -> ArtifactRef | None: ...
    def restore(self, ref: ArtifactRef | None) -> None: ...
    def close(self) -> None: ...


@runtime_checkable
class WorkerJournalPort(Protocol):
    def native_request(self, command: NativeJournalCommand) -> NativeJournalReply: ...
    def queue(self, command: QueueCommand) -> QueueReply: ...
    def effect(self, command: EffectCommand) -> EffectReply: ...
    def artifact(self, command: ArtifactCommand) -> ArtifactReply: ...
    def reserve(self, operation_id: str, amounts: Mapping[str, int]) -> None: ...
    def read_action(self, action_id: str) -> Mapping[str, Json]: ...
    def restrict_recovery(self, occurrence_id: str) -> None: ...
    def read_configuration(self) -> Mapping[str, Json]: ...
    def check_grant(self) -> None: ...


@runtime_checkable
class HostEndpoint(Protocol):
    def deliver_reply(self, reply: ControlReply) -> None: ...
    def activate(self, assignment: WorkerAssignment) -> None: ...
    def poll(self, *, max_events: int, deadline_ms: int) -> tuple[WorkerEvent, ...]: ...
    def request_cut(self, request: CutRequest) -> None: ...
    def cancel(self, reason: str, *, deadline_ms: int) -> None: ...
    def closed(self) -> bool: ...


@runtime_checkable
class ControlTransport(Protocol):
    def request(self, event: WorkerEvent, *, deadline_ms: int) -> ControlReply: ...


@runtime_checkable
class ActionHandle(Protocol):
    def rows(self) -> Iterator[Record]: ...
    def result(self) -> TerminalResult: ...
    def close(self) -> None: ...


@runtime_checkable
class CallableInstance(Protocol):
    def __call__(self, row: object) -> object: ...


@runtime_checkable
class NativeEffectTransport(Protocol):
    def lookup(self, command: EffectCommand) -> EffectReply: ...
    def submit(self, command: EffectCommand) -> EffectReply: ...


@runtime_checkable
class MaterializedHandle(Protocol):
    def num_blocks(self) -> int: ...
    def rows(self) -> Iterator[object]: ...
    def release(self) -> None: ...


@runtime_checkable
class ChannelSet(Protocol):
    def close(self) -> None: ...


@runtime_checkable
class RoutingState(Protocol):
    def next_ordinal(self, channel_id: str) -> int: ...


@runtime_checkable
class SocketConnection(Protocol):
    def send_bytes(self, value: bytes) -> None: ...
    def recv_bytes(self, maxlength: int) -> bytes: ...
    def poll(self, timeout: float) -> bool: ...
    def close(self) -> None: ...


@runtime_checkable
class WorkerControlMailbox(Protocol):
    def request(self, event: WorkerEvent, *, deadline_ms: int) -> ControlReply: ...
    def drain(self, *, max_events: int) -> tuple[WorkerEvent, ...]: ...
    def deliver(self, reply: ControlReply) -> None: ...
    def close(self) -> None: ...


@runtime_checkable
class ArtifactAuthorityPort(Protocol):
    def reserve(self, operation_id: str, amounts: Mapping[str, int]) -> None: ...
    def artifact(self, command: ArtifactCommand) -> ArtifactReply: ...


@runtime_checkable
class DriverAuthority(Protocol):
    def submit(self, event: WorkerEvent) -> bool: ...
    def poll_replies(self, *, max_replies: int) -> tuple[CompletedControl, ...]: ...
    def close(self, *, deadline_ms: int) -> None: ...
    def bind_queue(self, service: object) -> None: ...
