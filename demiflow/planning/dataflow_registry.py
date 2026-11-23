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
from ..execution.contracts.dataflow_kinds import operator_rows

_RELATIONAL = "demiflow.execution.kernels.relational:"
_READERS = "demiflow.execution.sources.source_readers:"
_TERMINALS = "demiflow.execution.dataflow_terminals:"

# kind -> (compiler_symbol, kernel_symbol, input ports). Every public LogicalOp
# in demiflow.data.plan is registered exactly once, plus the native declaration
# kinds produced by make_native_operation. The rows live in the kind table, with
# everything else the compiler and the runtime agree on about each kind.
_OPERATORS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = operator_rows()

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
