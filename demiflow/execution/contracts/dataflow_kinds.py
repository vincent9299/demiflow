"""One row per kernel kind: what the compiler and the runtime must agree on.

Before this table the facts about one kind were spread over the operator
registry, two trait tables and two kind sets in the compiler, seven kind sets
and two derived sets in the kernel modules, and three more sets elsewhere. A
new operation had to be added to each of them, and nothing said they agreed.
Every one of those tables is now a view of the rows below, built where it was
defined before, under the name it had before.

A row gives, for one kind name:

``entry``     how a plan can declare it: ``TYPED`` is a typed operation class,
              ``NATIVE`` is a ``NativeOperation`` kind, ``None`` means it is
              not a registered operator (a relational source kind, the
              aggregate terminal, or a kernel kind only the model factory's
              direct callers produce).
``roles``     every role the kind has; see ``ROLES``. A registered operator
              with no kernel family is one the kernel factory does not admit:
              the eighteen native names at the end of the operator rows.

A ``NATIVE`` row is a declaration name: what a public entry preflights against
before it appends a typed operation, which is what gets lowered and run. The
compiler refuses a native declaration placed in a plan directly, for all
twenty-one names, by name. So the symbols and traits of a native row describe
no lowering that exists today; they are kept because the registry record needs
them and because the names, and their order, are load-bearing (see below).
``operator``  compiler symbol, kernel symbol and input ports, for a registered
              operator. The symbols are strings and are resolved on use.
``traits``    default state model, effect, resource role and mergeability a
              lowering starts from; a lowering may only tighten them.

This module holds data only and imports nothing from the engine, so the worker
can load it for its kind sets without loading the compiler.

The names registered as operators, and their order, are load-bearing: sealing
admits a node without a user program by membership in the operator registry.
"""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

SHUFFLE_ORDER_FIELD = "shuffle_order_version"
SHUFFLE_ORDER_VERSION = "typed-multiset-v1"

TYPED = "typed"
NATIVE = "native"

# Authoring aliases only affect admission, not the declared operation.kind.
# The two declaration-only names are not additional runtime kernel registrations.
NATIVE_DECLARATION_ALIASES = MappingProxyType({
    "embeddings": "embedding", "fetch": "raw_fetch", "enqueue": "queue_produce",
    "ack_queue": "queue_ack", "lookup": "lance_lookup", "prepend": "stream_prepend",
    "read_object": "read_local_object", "admit": "deduplicate",
})
NATIVE_DECLARATION_ONLY = frozenset({"cache", "run_stream"})

# Kernel families. The kernel factory admits a kind that is in at least one.
FAMILIES = ("unary", "batch", "model", "relational", "ordering", "aggregate", "stream_group")
ROLES = FAMILIES + (
    "instance",          # the operator kernel constructs and owns a callable instance
    "batch_instance",    # the instance is called with a batch
    "task",              # task-scoped: rows are grouped into tasks unless the map is streaming
    "batch_membership",  # batches are recorded in the native batch-membership journal
    "keeps_layout",      # a declared block layout survives this kind
    "global_ordering",   # its input port requires a globally ordered input
    "finite_complete",   # its input port requires a finite, complete input
)

_COMPILE = "demiflow.planning.dataflow_compile:compile_kernel_spec"
_MODEL_COMPILE = "demiflow.planning.dataflow_models:build_model_factory"
_UNARY = "demiflow.execution.kernels.dataflow:invoke_unary"
_BATCH = "demiflow.execution.kernels.dataflow:invoke_batch"
_ORDER = "demiflow.execution.kernels.ordering:build_sort_runs"
_REPARTITION = "demiflow.execution.exchange.dataflow_channels:repartition_records"
_MODEL_KERNEL = "demiflow.execution.operator_bindings.dataflow_models:bind_model_services"


@dataclass(frozen=True)
class KindRecord:
    name: str
    entry: str | None
    roles: frozenset
    operator: tuple | None
    traits: tuple | None


def _kind(name, entry, roles, operator, traits):
    unknown = set(roles) - set(ROLES)
    if unknown:
        raise ValueError("unknown kind role: " + ", ".join(sorted(unknown)))
    if (entry is None) != (operator is None) or (operator is None) != (traits is None):
        raise ValueError("a registered operator has an entry, symbols and traits together: " + name)
    return KindRecord(name, entry, frozenset(roles), operator, traits)


# Unary operations take a single ``main`` port; StreamPrependOp adds a finite
# ``prefix`` port ahead of ``main``. Model, acquisition and cache nodes are
# assembled inside workers from the sealed CallableSpec factory.
_RECORDS = (
    # -- registered operators, in registry order ---------------------------------
    _kind("MapOp", TYPED, ("unary", "instance", "task", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("FlatMapOp", TYPED, ("unary", "instance", "task", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("PredictOp", TYPED, (), (_COMPILE, _BATCH, ("main",)), ("auto_json", "pure", "cpu", False)),
    _kind("MapBatchesOp", TYPED, ("batch", "instance", "batch_instance", "keeps_layout"), (_COMPILE, _BATCH, ("main",)), ("auto_json", "pure", "cpu", False)),
    _kind("BoundMapOp", TYPED, ("unary", "instance", "task", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("OperatorLLMMapOp", TYPED, ("model", "instance", "finite_complete"), (_MODEL_COMPILE, _MODEL_KERNEL, ("main",)), ("adapter", "journaled", "remote_api", False)),
    _kind("FilterOp", TYPED, ("unary", "instance", "task", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("AsyncMapOp", TYPED, ("model", "instance"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", False)),
    _kind("DeduplicateOp", TYPED, ("model", "instance"), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "cpu", False)),
    _kind("AgentMapOp", TYPED, ("model", "instance", "finite_complete"), (_MODEL_COMPILE, _MODEL_KERNEL, ("main",)), ("adapter", "journaled", "remote_api", False)),
    _kind("BatchMapOp", TYPED, ("batch", "instance", "batch_instance", "keeps_layout"), (_COMPILE, _BATCH, ("main",)), ("auto_json", "pure", "cpu", False)),
    _kind("EmbeddingMapOp", TYPED, ("batch", "model", "instance", "batch_instance", "batch_membership", "keeps_layout", "finite_complete"), (_MODEL_COMPILE, _MODEL_KERNEL, ("main",)), ("adapter", "journaled", "remote_api", False)),
    _kind("StreamGroupBatchesOp", TYPED, ("stream_group",), (_COMPILE, _BATCH, ("main",)), ("auto_json", "pure", "cpu", False)),
    _kind("StreamPrependOp", TYPED, ("model", "instance"), (_COMPILE, _UNARY, ("main", "prefix")), ("none", "pure", "cpu", False)),
    _kind("LimitOp", TYPED, ("unary", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", False)),
    _kind("SelectColumnsOp", TYPED, ("unary", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("DropColumnsOp", TYPED, ("unary", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("RenameColumnsOp", TYPED, ("unary", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("AddColumnOp", TYPED, ("unary", "instance", "task", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("RandomSampleOp", TYPED, ("unary", "keeps_layout"), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("SortOp", TYPED, ("ordering", "global_ordering", "finite_complete"), (_COMPILE, _ORDER, ("main",)), ("none", "pure", "cpu", False)),
    _kind("RepartitionOp", TYPED, ("ordering", "keeps_layout", "finite_complete"), (_COMPILE, _REPARTITION, ("main",)), ("none", "pure", "cpu", False)),
    _kind("RandomShuffleOp", TYPED, ("ordering", "keeps_layout", "global_ordering", "finite_complete"), (_COMPILE, _ORDER, ("main",)), ("none", "pure", "cpu", False)),
    _kind("RandomizeBlockOrderOp", TYPED, ("ordering", "keeps_layout", "global_ordering", "finite_complete"), (_COMPILE, _ORDER, ("main",)), ("none", "pure", "cpu", False)),
    _kind("VectorSearchOp", TYPED, ("model", "instance"), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("SearchWebOp", TYPED, ("model", "instance"), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "remote_api", False)),
    _kind("RegisterDocumentsOp", TYPED, ("model", "instance"), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("RegisterDocumentsBatchOp", TYPED, ("batch", "model", "instance", "batch_instance", "keeps_layout"), (_COMPILE, _BATCH, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("ReadDocumentsOp", TYPED, ("model", "instance"), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("SaveLanceOp", TYPED, ("batch", "model", "instance", "batch_instance", "batch_membership", "keeps_layout"), (_COMPILE, _BATCH, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("prompt", NATIVE, ("model", "instance"), (_MODEL_COMPILE, _MODEL_KERNEL, ("main",)), ("adapter", "journaled", "remote_api", False)),
    _kind("embedding", NATIVE, ("model", "instance", "batch_instance", "batch_membership"), (_MODEL_COMPILE, _MODEL_KERNEL, ("main",)), ("adapter", "journaled", "remote_api", False)),
    _kind("agent", NATIVE, ("model", "instance"), (_MODEL_COMPILE, _MODEL_KERNEL, ("main",)), ("adapter", "journaled", "remote_api", False)),
    _kind("raw_fetch", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("queue_produce", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("queue_ack", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("lance_lookup", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("search_vectors", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("search_web", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "remote_api", False)),
    _kind("deduplicate", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "cpu", False)),
    _kind("group_batches", NATIVE, (), (_COMPILE, _BATCH, ("main",)), ("auto_json", "pure", "cpu", False)),
    _kind("stream_prepend", NATIVE, (), (_COMPILE, _UNARY, ("main", "prefix")), ("none", "pure", "cpu", False)),
    _kind("save_lance", NATIVE, (), (_COMPILE, _BATCH, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("register_documents", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("register_documents_batch", NATIVE, (), (_COMPILE, _BATCH, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("read_documents", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("adapter", "journaled", "io", False)),
    _kind("pack_prompt_inputs", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("read_local_object", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "io", True)),
    _kind("decode_document", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("encode_image", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    _kind("parse_wikitext", NATIVE, (), (_COMPILE, _UNARY, ("main",)), ("none", "pure", "cpu", True)),
    # -- kernel kinds that are not registered operators --------------------------
    # 'fetch' and 'document' are produced only by direct callers of the model
    # factory. The relational kinds are registered as sources and 'aggregate' as
    # a terminal; here they carry their kernel family.
    _kind("fetch", None, ("model", "instance"), None, None),
    _kind("document", None, ("model", "instance"), None, None),
    _kind("join", None, ("relational",), None, None),
    _kind("exclude_keys", None, ("relational",), None, None),
    _kind("reduce", None, ("relational",), None, None),
    _kind("groups", None, ("relational",), None, None),
    _kind("union", None, ("relational",), None, None),
    _kind("aggregate", None, ("aggregate",), None, None),
)


def _index():
    table = {}
    for record in _RECORDS:
        if record.name in table:
            raise ValueError("duplicate kind: " + record.name)
        table[record.name] = record
    return MappingProxyType(table)


_KINDS = _index()


def kind_records() -> Mapping[str, KindRecord]:
    """Every kind, in table order."""
    return _KINDS


def kinds_with(role: str) -> frozenset:
    """The names of the kinds that have ``role``."""
    if role not in ROLES:
        raise ValueError("unknown kind role: " + role)
    return frozenset(record.name for record in _RECORDS if role in record.roles)


def operator_rows() -> tuple:
    """(kind, compiler symbol, kernel symbol, input ports) of every registered operator, in order."""
    return tuple((record.name, *record.operator) for record in _RECORDS if record.operator is not None)


def traits_of(entry: str) -> Mapping[str, tuple]:
    """kind -> (state, effect, resource role, mergeable) for the operators declared through ``entry``."""
    return MappingProxyType({record.name: record.traits for record in _RECORDS if record.entry == entry})


TASK_KINDS = kinds_with('task')
