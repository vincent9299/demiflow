"""Single immutable v3 operator/source/terminal registries.

These tables are the one admission authority shared by the compiler, the kernel
factory and the workbench. They bind kind names to compiler and kernel factory
symbols; they never hold live sources, actors, clients or executors. Consumers
must not build a second registry or register kinds at runtime.
"""
from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from ..execution.contracts.dataflow import (
    OperatorDefinition,
    SourceDefinition,
    TerminalDefinition,
)

_COMPILE = "demiflow.planning.dataflow_compile:compile_kernel_spec"
_MODEL_COMPILE = "demiflow.execution.dataflow_models:build_model_factory"
_UNARY = "demiflow.execution.kernels.dataflow:invoke_unary"
_BATCH = "demiflow.execution.kernels.dataflow:invoke_batch"
_ORDER = "demiflow.execution.kernels.ordering:build_sort_runs"
_REPARTITION = "demiflow.execution.kernels.ordering:repartition_records"
_MODEL_KERNEL = "demiflow.execution.dataflow_models:bind_model_services"
_RELATIONAL = "demiflow.execution.kernels.relational:"
_READERS = "demiflow.execution.source_readers:"
_TERMINALS = "demiflow.execution.dataflow_terminals:"

# kind -> (compiler_symbol, kernel_symbol, input ports). Every public LogicalOp
# in demiflow.data.plan is registered exactly once, plus the native declaration
# kinds produced by make_native_operation. Unary ops take a single ``main``
# port; StreamPrependOp adds a finite ``prefix`` port ahead of ``main``.
_OPERATORS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("MapOp", _COMPILE, _UNARY, ("main",)),
    ("FlatMapOp", _COMPILE, _UNARY, ("main",)),
    ("MapBatchesOp", _COMPILE, _BATCH, ("main",)),
    ("BoundMapOp", _COMPILE, _UNARY, ("main",)),
    ("OperatorLLMMapOp", _MODEL_COMPILE, _MODEL_KERNEL, ("main",)),
    ("FilterOp", _COMPILE, _UNARY, ("main",)),
    ("AsyncMapOp", _COMPILE, _UNARY, ("main",)),
    ("DeduplicateOp", _COMPILE, _UNARY, ("main",)),
    ("AgentMapOp", _MODEL_COMPILE, _MODEL_KERNEL, ("main",)),
    ("BatchMapOp", _COMPILE, _BATCH, ("main",)),
    ("EmbeddingMapOp", _MODEL_COMPILE, _MODEL_KERNEL, ("main",)),
    ("StreamGroupBatchesOp", _COMPILE, _BATCH, ("main",)),
    ("StreamPrependOp", _COMPILE, _UNARY, ("main", "prefix")),
    ("LimitOp", _COMPILE, _UNARY, ("main",)),
    ("SelectColumnsOp", _COMPILE, _UNARY, ("main",)),
    ("DropColumnsOp", _COMPILE, _UNARY, ("main",)),
    ("RenameColumnsOp", _COMPILE, _UNARY, ("main",)),
    ("AddColumnOp", _COMPILE, _UNARY, ("main",)),
    ("RandomSampleOp", _COMPILE, _UNARY, ("main",)),
    ("SortOp", _COMPILE, _ORDER, ("main",)),
    ("RepartitionOp", _COMPILE, _REPARTITION, ("main",)),
    ("RandomShuffleOp", _COMPILE, _ORDER, ("main",)),
    ("RandomizeBlockOrderOp", _COMPILE, _ORDER, ("main",)),
    ("VectorSearchOp", _COMPILE, _UNARY, ("main",)),
    ("SearchWebOp", _COMPILE, _UNARY, ("main",)),
    ("RegisterDocumentsOp", _COMPILE, _UNARY, ("main",)),
    ("RegisterDocumentsBatchOp", _COMPILE, _BATCH, ("main",)),
    ("ReadDocumentsOp", _COMPILE, _UNARY, ("main",)),
    ("SaveLanceOp", _COMPILE, _BATCH, ("main",)),
    # Native declaration kinds (NativeOperation.kind). Model/acquisition/cache
    # nodes are assembled inside workers from the sealed CallableSpec factory.
    ("prompt", _MODEL_COMPILE, _MODEL_KERNEL, ("main",)),
    ("embedding", _MODEL_COMPILE, _MODEL_KERNEL, ("main",)),
    ("agent", _MODEL_COMPILE, _MODEL_KERNEL, ("main",)),
    ("raw_fetch", _COMPILE, _UNARY, ("main",)),
    ("queue_produce", _COMPILE, _UNARY, ("main",)),
    ("queue_ack", _COMPILE, _UNARY, ("main",)),
    ("lance_lookup", _COMPILE, _UNARY, ("main",)),
    ("search_vectors", _COMPILE, _UNARY, ("main",)),
    ("search_web", _COMPILE, _UNARY, ("main",)),
    ("deduplicate", _COMPILE, _UNARY, ("main",)),
    ("group_batches", _COMPILE, _BATCH, ("main",)),
    ("stream_prepend", _COMPILE, _UNARY, ("main", "prefix")),
    ("save_lance", _COMPILE, _BATCH, ("main",)),
    ("register_documents", _COMPILE, _UNARY, ("main",)),
    ("register_documents_batch", _COMPILE, _BATCH, ("main",)),
    ("read_documents", _COMPILE, _UNARY, ("main",)),
    ("pack_prompt_inputs", _COMPILE, _UNARY, ("main",)),
    ("read_local_object", _COMPILE, _UNARY, ("main",)),
    ("decode_document", _COMPILE, _UNARY, ("main",)),
    ("encode_image", _COMPILE, _UNARY, ("main",)),
    ("parse_wikitext", _COMPILE, _UNARY, ("main",)),
)

# kind -> (reader_symbol, cursor offset fields per C04). Composite relational
# kinds are computed from their expanded input ports and carry no source cursor.
_SOURCES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("items", _READERS + "ItemsReader", ("index",)),
    ("range", _READERS + "RangeReader", ("next",)),
    ("file", _READERS + "FileReader", ("file_index", "record_index")),
    ("lance", _READERS + "LanceReader", ("version", "fragment_id", "row_offset")),
    ("sql", _READERS + "SqlReader", ("snapshot_id", "partition", "last_key")),
    ("materialized", _READERS + "ItemsReader", ("index",)),
    ("iterable", _READERS + "IteratorReader", ("segment", "record")),
    ("datasource", _READERS + "TaskReader", ("segment", "record")),
    ("queue", _READERS + "QueueReader", ("claim_ref", "record_index")),
    ("producer", _READERS + "ProducerReader", ("producer_position", "producer_cursor")),
    ("queue_history", _READERS + "HistoryReader", ("claim_ref", "record_index")),
    ("join", _RELATIONAL + "join_partitions", ()),
    ("exclude_keys", _RELATIONAL + "join_partitions", ()),
    ("reduce", _RELATIONAL + "reduce_partition", ()),
    ("groups", _RELATIONAL + "emit_groups", ()),
    ("union", _RELATIONAL + "union_inputs", ()),
)

# kind -> (adapter_symbol, result_kind). All terminals share one scheduling
# entry; a write terminal is never routed as an ordinary take.
_TERMINALS_TABLE: tuple[tuple[str, str, str], ...] = (
    ("take", _TERMINALS + "collect_rows", "rows"),
    ("take_all", _TERMINALS + "collect_rows", "rows"),
    ("take_batch", _TERMINALS + "collect_rows", "batches"),
    ("iter_rows", _TERMINALS + "iterate_rows", "row_iterator"),
    ("iter_batches", _TERMINALS + "iterate_batches", "batch_iterator"),
    ("count", _TERMINALS + "collect_rows", "scalar"),
    ("aggregate", _TERMINALS + "collect_rows", "scalar"),
    ("schema", _TERMINALS + "inspect_result", "schema"),
    ("columns", _TERMINALS + "inspect_result", "columns"),
    ("size_bytes", _TERMINALS + "inspect_result", "scalar"),
    ("materialize", _TERMINALS + "materialized_handle", "materialized"),
    ("write_datasink", _TERMINALS + "write_result", "write_result"),
    ("write_lance", _TERMINALS + "write_result", "write_result"),
    ("write_file", _TERMINALS + "write_result", "write_result"),
    ("run_stream", _TERMINALS + "iterate_rows", "row_iterator"),
    ("collect_summary", _TERMINALS + "collect_rows", "rows"),
)


def _freeze_operator_table() -> Mapping[str, OperatorDefinition]:
    table: dict[str, OperatorDefinition] = {}
    for kind, compiler, kernel, inputs in _OPERATORS:
        if kind in table:
            raise ValueError("duplicate operator kind: " + kind)
        table[kind] = OperatorDefinition(
            kind=kind, compiler_symbol=compiler, kernel_symbol=kernel, inputs=inputs
        )
    return MappingProxyType(table)


def _freeze_source_table() -> Mapping[str, SourceDefinition]:
    table: dict[str, SourceDefinition] = {}
    for kind, reader, cursor_fields in _SOURCES:
        if kind in table:
            raise ValueError("duplicate source kind: " + kind)
        table[kind] = SourceDefinition(
            kind=kind, reader_symbol=reader, cursor_fields=cursor_fields
        )
    return MappingProxyType(table)


def _freeze_terminal_table() -> Mapping[str, TerminalDefinition]:
    table: dict[str, TerminalDefinition] = {}
    for kind, adapter, result_kind in _TERMINALS_TABLE:
        if kind in table:
            raise ValueError("duplicate terminal kind: " + kind)
        table[kind] = TerminalDefinition(
            kind=kind, adapter_symbol=adapter, result_kind=result_kind
        )
    return MappingProxyType(table)


_OPERATOR_REGISTRY = _freeze_operator_table()
_SOURCE_REGISTRY = _freeze_source_table()
_TERMINAL_REGISTRY = _freeze_terminal_table()


def operator_registry() -> Mapping[str, OperatorDefinition]:
    """The single immutable operator admission table."""
    return _OPERATOR_REGISTRY


def source_registry() -> Mapping[str, SourceDefinition]:
    """The single immutable source/reader admission table."""
    return _SOURCE_REGISTRY


def terminal_registry() -> Mapping[str, TerminalDefinition]:
    """The single immutable terminal admission table."""
    return _TERMINAL_REGISTRY
