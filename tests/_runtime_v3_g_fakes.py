"""Test-domain spec-bound fakes for cross-lane modules not yet landed in this tree.

These implement the frozen interfaces.pyi / IDENTITY.md contracts exactly so lane
G unit tests can run before C (DTO/codec/errors) and M (model factory) land. They
are injected into sys.modules only inside the test process; production code never
imports them and never falls back to them. The final integration runs the real C
and M implementations.
"""
from __future__ import annotations

import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

_TREE = Path(__file__).resolve().parents[1]

FAKE_MODULE_NAMES = (
    "demiflow.execution.contracts.dataflow",
    "demiflow.execution.contracts.dataflow_codec",
    "demiflow.execution.contracts.dataflow_errors",
    'demiflow.execution.operator_bindings.dataflow_models',
)


def _freeze(value):
    if isinstance(value, Mapping):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    return value


@dataclass(frozen=True)
class ArtifactRef:
    digest: str
    size_bytes: int
    store_id: str
    key: str
    media_type: str


@dataclass(frozen=True)
class Bounds:
    max_nodes: int = 4096
    max_edges: int = 16384
    max_partitions: int = 4096
    max_depth: int = 64
    max_control_bytes: int = 65536
    max_row_bytes: int = 1024 * 1024
    max_batch_bytes: int = 8 * 1024 * 1024
    max_resident_bytes: int = 256 * 1024 * 1024
    max_spill_bytes: int = 1024 * 1024 * 1024
    max_state_bytes: int = 64 * 1024 * 1024
    max_pending_events: int = 1024
    max_open_files: int = 256
    max_output_rows: int = 1000000
    timeout_ms: int = 600000


@dataclass(frozen=True)
class ActionProgram:
    name: str
    kind: str
    value: Any


@dataclass(frozen=True)
class ActionRequest:
    kind: str
    source: Any
    plan: Any
    arguments: Mapping[str, Any]
    sink: Any
    programs: tuple
    provider: str
    action: Any


@dataclass(frozen=True)
class InputPort:
    name: str
    requirement: str
    ordering: str
    schema_ref: Any


@dataclass(frozen=True)
class KernelSpec:
    kind: str
    implementation_ref: Any
    configuration: Mapping[str, Any]
    inputs: tuple
    state: str
    effect: str
    resource_role: str
    profile: Any
    output_bound_bytes: int
    mergeable: bool


@dataclass(frozen=True)
class GraphNode:
    occurrence_id: str
    semantic_id: str
    kind: str
    kernel: KernelSpec
    program_ref: Any
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
class PlanInputBinding:
    port: str
    source: Any
    plan: Any
    path: tuple


@dataclass(frozen=True)
class ProgramDeclaration:
    occurrence_id: str
    callable_spec: Any
    source_spec: Any
    action_program: Any
    code_identity: Mapping[str, Any]


@dataclass(frozen=True)
class CompiledDataflow:
    graph: DataflowGraph
    declarations: tuple


@dataclass(frozen=True)
class OperatorDefinition:
    kind: str
    compiler_symbol: str
    kernel_symbol: str
    inputs: tuple


@dataclass(frozen=True)
class SourceDefinition:
    kind: str
    reader_symbol: str
    cursor_fields: tuple


@dataclass(frozen=True)
class TerminalDefinition:
    kind: str
    adapter_symbol: str
    result_kind: str


class DataflowError(Exception):
    def __init__(self, code, message, *, retryable=False, context=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.context = dict(context or {})


def _real_module_available(name):
    import importlib
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def _install_fakes():
    if _real_module_available("demiflow.execution.contracts.dataflow"):
        return
    from demiflow.execution.recovery_contract import identity

    dataflow = types.ModuleType("demiflow.execution.contracts.dataflow")
    for name in ("ArtifactRef", "Bounds", "ActionProgram", "ActionRequest", "InputPort",
                 "KernelSpec", "GraphNode", "GraphEdge", "DataflowGraph", "PlanInputBinding",
                 "ProgramDeclaration", "CompiledDataflow", "OperatorDefinition",
                 "SourceDefinition", "TerminalDefinition"):
        setattr(dataflow, name, globals()[name])
    sys.modules["demiflow.execution.contracts.dataflow"] = dataflow

    codec = types.ModuleType("demiflow.execution.contracts.dataflow_codec")

    def compute_graph_digest(graph):
        nodes = [[n.occurrence_id, n.semantic_id, n.kind, n.shared_token] for n in graph.nodes]
        edges = [[e.edge_id, e.producer, e.output_port, e.consumer, e.input_port] for e in graph.edges]
        payload = ["dataflow-dag-v3", graph.action_id, nodes, edges,
                   list(graph.terminals), list(graph.sources)]
        return identity(payload)

    codec.compute_graph_digest = compute_graph_digest
    sys.modules["demiflow.execution.contracts.dataflow_codec"] = codec

    errors = types.ModuleType("demiflow.execution.contracts.dataflow_errors")
    errors.DataflowError = DataflowError
    sys.modules["demiflow.execution.contracts.dataflow_errors"] = errors

    models = types.ModuleType('demiflow.execution.operator_bindings.dataflow_models')

    def build_model_factory(operation, *, program_ref, bounds):
        from demiflow.data.dataflow_operation import NativeOperation
        kind = operation.kind if isinstance(operation, NativeOperation) else type(operation).__name__
        return KernelSpec(
            kind=kind, implementation_ref=program_ref, configuration=_freeze({}),
            inputs=(InputPort("main", "incremental", "none", None),),
            state="adapter", effect="journaled", resource_role="remote_api",
            profile=None, output_bound_bytes=bounds.max_row_bytes, mergeable=False,
        )

    models.build_model_factory = build_model_factory
    sys.modules['demiflow.execution.operator_bindings.dataflow_models'] = models


def assert_tree_binding():
    """In-process assertion that every real demiflow module comes from this tree."""
    import demiflow
    root = Path(demiflow.__file__).resolve()
    assert root.is_relative_to(_TREE), f"demiflow not from worktree: {root}"
    loaded = {}
    for name, module in list(sys.modules.items()):
        if not name.startswith("demiflow"):
            continue
        path = getattr(module, "__file__", None)
        if path is None:
            assert name in FAKE_MODULE_NAMES, f"unexpected file-less module: {name}"
            continue
        resolved = Path(path).resolve()
        assert resolved.is_relative_to(_TREE), f"module outside worktree: {name} -> {resolved}"
        loaded[name] = str(resolved)
    return loaded