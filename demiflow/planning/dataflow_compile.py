"""Pure structural capture and semantic compilation of a v3 dataflow action.

The compiler traverses real SourcePlan/PlanInput/LogicalPlan objects and registers
a complete multi-port DAG. It never executes a source, UDF, actor constructor,
network call or model. Shared prefixes are aliased by construction reference
within a single traversal; two independent calls are never merged because they
share a function. Identity formulas follow IDENTITY.md and the canonical encoder
is reused from recovery_contract; dag_id is computed once by C004.
"""
from __future__ import annotations

from dataclasses import fields, is_dataclass, replace
from types import MappingProxyType
from typing import Any, Mapping

from ..data import plan as ops
from ..data.dataflow_operation import NativeOperation
from ..data.plan import CallableSpec, LogicalOp, LogicalPlan
from ..data.sources import (
    DatasourceSource,
    FileSource,
    ItemsSource,
    IterableSource,
    LanceSource,
    MaterializedSource,
    RangeSource,
    SourcePlan,
    SqlSource,
)
from ..execution.contracts.dataflow import (
    ActionProgram,
    ActionRequest,
    ArtifactRef,
    Bounds,
    CompiledDataflow,
    DataflowGraph,
    GraphEdge,
    GraphNode,
    InputPort,
    KernelSpec,
    PlanInputBinding,
    ProgramDeclaration,
)
from ..execution.contracts.dataflow_codec import compute_graph_digest
from ..execution.contracts.dataflow_errors import DataflowError
from ..execution.contracts.dataflow_kinds import NATIVE, TYPED, kinds_with, traits_of
from ..execution.local_kernel import KeyedSource, PlanInput, UnionSource
from ..execution.recovery_contract import identity
from ..execution.recovery_identity import describe
from .dataflow_budget import (
    codec_window, default_profile, fit_codec_windows, relational_profile, terminal_budget,
)
from .dataflow_registry import operator_registry, source_registry, terminal_registry

_SCHEMA = "demiflow.dataflow_graph.v3"
_NON_SEMANTIC_FIELDS = frozenset({"runtime_contract", "label", "node_id", "source_dataset"})
_MODEL_COMPILER = "demiflow.execution.dataflow_models:build_model_factory"

# kind -> (state, effect, resource_role, mergeable). Traits are declarations; the
# physical planner refines resources. Unknown UDF state is never claimed pure.
# Typed operations and native declarations are looked up separately, as before:
# a NativeOperation that names a typed operation finds no traits and is refused.
_TRAITS: Mapping[str, tuple[str, str, str, bool]] = traits_of(TYPED)
_NATIVE_TRAITS: Mapping[str, tuple[str, str, str, bool]] = traits_of(NATIVE)
# Service declarations whose state hooks and original commit scopes own recovery
# replace the traits of the operation that carries them. The names are the ones
# execution.dataflow_native.native_variant reports. A stream write that carries a
# user predicate has unproven state and is deliberately absent: it is not upgraded.
_VARIANT_TRAITS: Mapping[str, tuple[str, str, str, bool]] = MappingProxyType({
    "producer": ("adapter", "journaled", "io", False),
    "search": ("adapter", "journaled", "io", False),
    "stream_write": ("adapter", "journaled", "io", False),
})
_ORDERING_KINDS = kinds_with("global_ordering")
_FINITE_COMPLETE_KINDS = kinds_with("finite_complete")


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    return value


def _plain(value: Any) -> Any:
    """Deep-convert frozen declarations back to plain Json for identity hashing."""
    if isinstance(value, Mapping):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _error(code: str, message: str, **context: Any) -> DataflowError:
    return DataflowError(code, message, context=context)


def _hash(value: Any) -> str:
    try:
        return identity(value)
    except Exception as exc:  # canonical encoder budget/strict-Json failure
        raise _error("BUDGET_EXCEEDED", "identity projection exceeds budget: " + str(exc)) from exc


def _callable_code_identity(spec: CallableSpec) -> Mapping[str, Any]:
    return {
        "target": describe(spec.target),
        "constructor_args": describe(spec.constructor_args),
        "constructor_kwargs": describe(dict(spec.constructor_kwargs)),
        "call_args": describe(spec.call_args),
        "call_kwargs": describe(dict(spec.call_kwargs)),
    }


def _operation_semantic_facts(operation: LogicalOp) -> Mapping[str, Any]:
    if isinstance(operation, NativeOperation):
        return {
            "native_kind": operation.kind,
            "configuration": _plain(operation.configuration),
            "bindings": _plain(operation.bindings),
            "factory": _callable_code_identity(operation.factory),
        }
    if is_dataclass(operation):
        from ..execution.plan_facts import operation_facts
        from ..execution.recovery_identity import operation_identity
        try:
            fact = operation_facts(operation)
        except (RuntimeError, ValueError, TypeError):
            fact = {}
        if fact.get('native') is not None or type(operation).__name__ == 'OperatorLLMMapOp':
            return operation_identity(operation)
        return {
            "type": type(operation).__module__ + "." + type(operation).__qualname__,
            "fields": {
                f.name: describe(getattr(operation, f.name))
                for f in fields(operation) if f.name not in _NON_SEMANTIC_FIELDS
            },
        }
    raise _error("INVALID_CONTRACT", "operation is not a declared logical op")


def _source_semantic_facts(source: SourcePlan) -> Mapping[str, Any]:
    """Structural source identity; never reads file contents or executes a source."""
    if isinstance(source, ItemsSource):
        return {"kind": "items", "rows": len(source.items),
                "items": describe(source.items)}
    if isinstance(source, RangeSource):
        return {"kind": "range", "count": source.count}
    if isinstance(source, FileSource):
        # Only newly supported native option objects need a semantic projection;
        # previously valid JSON option bytes keep their original representation.
        options = {key: describe(value) if (type(value).__module__ == 'pyarrow._csv'
                   and type(value).__name__ in {'ReadOptions', 'ParseOptions', 'ConvertOptions'})
                   else value for key, value in source.options.items()}
        return {"kind": "file", "format": source.format,
                "paths": list(source.paths), "options": options}
    if isinstance(source, SqlSource):
        return {"kind": "sql", "sql": source.sql,
                "options": dict(source.options),
                "connection_factory": describe(source.connection_factory)}
    if isinstance(source, LanceSource):
        from ..execution.dataflow_native import is_key_only
        key_only = is_key_only(source)
        return {"kind": "lance", "query": describe(source.query),
                **({"key_only": True} if key_only else {})}
    if isinstance(source, MaterializedSource):
        ref = getattr(source.handle, "recovery_reference", None)
        return {"kind": "materialized", "row_count": source.row_count,
                "reference": describe(ref) if ref is not None else None}
    if isinstance(source, IterableSource):
        return {"kind": "iterable", "factory": describe(source.factory)}
    if isinstance(source, DatasourceSource):
        return {"kind": "datasource",
                "datasource": describe(type(source.datasource))}
    if isinstance(source, KeyedSource):
        return {"kind": "keyed", "op": source.kind,
                "on": list(source.on), "right_on": list(source.right_on),
                "how": source.how, "suffix": source.suffix,
                "chunk_bytes": source.chunk_bytes, "max_rows": source.max_rows,
                "output": source.output}
    if isinstance(source, UnionSource):
        return {"kind": "union", "inputs": len(source.inputs)}
    if type(source).__name__ == "_LiteralEmptySource":
        return {"kind": "literal_empty"}
    raise _error("INVALID_CONTRACT", "unknown source plan: " + type(source).__name__)


def _source_kind(source: SourcePlan) -> str:
    if isinstance(source, KeyedSource):
        return source.kind
    if isinstance(source, UnionSource):
        return "union"
    if isinstance(source, ItemsSource):
        return "items"
    if isinstance(source, RangeSource):
        return "range"
    if isinstance(source, FileSource):
        return "file"
    if isinstance(source, SqlSource):
        return "sql"
    if isinstance(source, LanceSource):
        return "lance"
    if isinstance(source, MaterializedSource):
        return "materialized"
    if isinstance(source, IterableSource):
        return "iterable"
    if isinstance(source, DatasourceSource):
        from ..execution.dataflow_native import datasource_kind
        return datasource_kind(source.datasource)
    if type(source).__name__ == "_LiteralEmptySource":
        return "literal_empty"
    raise _error("INVALID_CONTRACT", "unknown source plan: " + type(source).__name__)


def _input_port(name: str, kind: str) -> InputPort:
    finite = kind in _FINITE_COMPLETE_KINDS or name in {"left", "right", "prefix"}
    ordering = "global" if kind in _ORDERING_KINDS else "none"
    return InputPort(
        name=name,
        requirement="finite_complete" if finite else "incremental",
        ordering=ordering,
        schema_ref=None,
    )


def compile_kernel_spec(
    operation: LogicalOp, *, program_ref: ArtifactRef | None, bounds: Bounds
) -> KernelSpec:
    """Translate a real operation into a KernelSpec without instantiating it."""
    registry = operator_registry()
    if type(operation) is NativeOperation:
        kind = operation.kind
    else:
        kind = type(operation).__name__
    definition = registry.get(kind)
    if definition is None:
        raise _error("UNSUPPORTED_CAPABILITY", "operator kind is not registered: " + kind)
    # Synchronous and streaming plans share one resource/host policy. Retired
    # Ray Data pool controls must not be silently discarded by this compiler.
    from ..data.native_options import validate_stream_native_options
    validate_stream_native_options(getattr(operation, 'native_options', None))
    from ..execution.dataflow_model_factory import native_model_declaration
    from ..execution.dataflow_native import cache_runtime_state, model_variant, native_variant
    native_declaration = native_model_declaration(operation)
    # The compiler asks which declaration this is; the classes stay on the execution side.
    model_kind = model_variant(native_declaration)
    if model_kind == 'routed_prompt':
        from ..execution.dataflow_routed_prompt import compile_routed_prompt
        return compile_routed_prompt(operation, native_declaration, program_ref=program_ref, bounds=bounds)
    if model_kind == 'prompt':
        from ..execution.dataflow_models import build_model_factory, _model_profile
        native_op, settings = native_declaration.constructor_args
        from ..data.plan import AgentMapOp
        model = build_model_factory(operation if isinstance(operation, AgentMapOp) else native_op,
                                    program_ref=program_ref, bounds=bounds)
        configuration = _plain(model.configuration)
        configuration.update(concurrency=settings.get('concurrency', 1),
                             catch=[c.__module__ + '.' + c.__qualname__ for c in getattr(operation, 'catch', ())])
        model = replace(model, configuration=configuration,
            profile=_model_profile(model.resource_role, bounds, configuration["concurrency"],
                                   model.state, operation=operation))
        from ..execution.dataflow_prompt_service import service_kernel
        return service_kernel(model, settings)
    if definition.compiler_symbol == _MODEL_COMPILER:
        from ..execution.dataflow_models import build_model_factory
        return build_model_factory(operation, program_ref=program_ref, bounds=bounds)
    traits = (_NATIVE_TRAITS if isinstance(operation, NativeOperation) else _TRAITS).get(kind)
    if traits is None:
        raise _error("UNSUPPORTED_CAPABILITY", "operator traits are not registered: " + kind)
    state, effect, role, mergeable = traits
    declaration = getattr(operation, "callable", None)
    if isinstance(declaration, CallableSpec):
        import functools
        import types
        from ..execution.operator_policy import classify_operator_state
        target = declaration.target
        if (type(target) is types.MethodWrapperType and target.__name__ == '__call__'
                and type(target.__self__) is functools.partial):
            target = target.__self__
        while isinstance(target, functools.partial):
            target = target.func
        policy = classify_operator_state(target, phase="declaration")
        state = {"function": "none", "factory": "auto_json", "auto_json": "auto_json",
                 "hooks": "adapter", "unsupported": "nonrecoverable"}[policy.mode]
        if policy.mode == 'function':
            from ..execution.kernels.task_scope import function_residency_kind
            state = function_residency_kind(declaration.target, call_args=declaration.call_args,
                                            call_kwargs=declaration.call_kwargs)
        elif model_kind == 'cache':
            if cache_runtime_state(declaration) is not None:
                # The wrapper's hooks do not make unadapted nested callback
                # state safe to park and reconstruct within the same attempt.
                state = 'nonrecoverable'
        if state != "none":
            mergeable = False
    # Semantic descriptions are for identity, not executable parameters. The
    # kernel consumes the declared field values (e.g. columns, seed, limit).
    configuration = {
        field.name: ([c.__module__ + "." + c.__qualname__ for c in operation.catch]
                     if field.name == "catch" else _plain(getattr(operation, field.name)))
        for field in fields(operation)
        if field.name not in _NON_SEMANTIC_FIELDS | {"callable", "native_options", "stream_options"}
    }
    if kind == 'RandomShuffleOp':
        # New plans opt in. A sealed graph without this field keeps the exact
        # original occurrence/record-id shuffle formula when loaded or resumed.
        from ..execution.kernels.shuffle_order import FIELD, VERSION
        configuration[FIELD] = VERSION
    # Ordinary CPU kernels do not use native services. Importing every native
    # adapter here initializes Lance/Arrow and model modules even for a map.
    variant = native_variant(operation, native_declaration)
    if variant is not None:
        configuration.update(variant.configuration)
        if variant.name in _VARIANT_TRAITS:
            state, effect, role, mergeable = _VARIANT_TRAITS[variant.name]
    stream_options = getattr(operation, 'stream_options', None)
    if stream_options is not None:
        configuration.update(streaming_map=True, concurrency=stream_options.concurrency,
            callable_scope=stream_options.callable_scope, execution=stream_options.execution,
            queue_depth=stream_options.queue_depth,
            catch=[c.__module__ + '.' + c.__qualname__ for c in stream_options.catch])
    inputs = tuple(_input_port(name, kind) for name in definition.inputs)
    profile = default_profile(operation, bounds=bounds, state=state)
    if profile.requests.get("accelerators"):
        role, mergeable = "gpu", False
    return KernelSpec(
        kind=kind,
        implementation_ref=program_ref,
        configuration=configuration,
        inputs=inputs,
        state=state,
        effect=effect,
        resource_role=role,
        profile=profile,
        output_bound_bytes=bounds.max_row_bytes,
        mergeable=mergeable,
    )


def expand_relational_inputs(
    source: SourcePlan, *, path: tuple[str, ...], bounds: Bounds
) -> tuple[PlanInputBinding, ...]:
    """Expand KeyedSource left/right and UnionSource ordinal ports."""
    if len(path) > bounds.max_depth:
        raise _error("BUDGET_EXCEEDED", "relational input path exceeds depth budget")
    if isinstance(source, KeyedSource):
        if source.kind in ("join", "exclude_keys"):
            if source.right is None:
                raise _error("INVALID_CONTRACT", "join requires a right input")
            bindings = (
                PlanInputBinding("left", source.left.source, source.left.plan, path + ("left",)),
                PlanInputBinding("right", source.right.source, source.right.plan, path + ("right",)),
            )
        elif source.kind in {"reduce", "groups"}:
            if source.right is not None:
                raise _error("INVALID_CONTRACT", source.kind + " takes a single input")
            bindings = (
                PlanInputBinding("left", source.left.source, source.left.plan, path + ("left",)),
            )
        else:
            raise _error("INVALID_CONTRACT", "unknown keyed source kind: " + source.kind)
        return bindings
    if isinstance(source, UnionSource):
        if len(source.inputs) > bounds.max_partitions:
            raise _error("BUDGET_EXCEEDED", "union input count exceeds partition budget")
        return tuple(
            PlanInputBinding(str(i), item.source, item.plan, path + (str(i),))
            for i, item in enumerate(source.inputs)
        )
    return ()


def expand_operation_inputs(
    operation: LogicalOp, *, path: tuple[str, ...], bounds: Bounds
) -> tuple[PlanInputBinding, ...]:
    """Capture extra Dataset inputs (StreamPrependOp prefix) as explicit ports."""
    if len(path) > bounds.max_depth:
        raise _error("BUDGET_EXCEEDED", "operation input path exceeds depth budget")
    if isinstance(operation, ops.StreamPrependOp):
        dataset = operation.source_dataset
        if dataset is None:
            return ()
        source = getattr(dataset, "_source", None)
        plan = getattr(dataset, "_plan", None)
        if not isinstance(source, SourcePlan) or not isinstance(plan, LogicalPlan):
            raise _error("INVALID_CONTRACT", "prepend source must be a real Dataset")
        return (PlanInputBinding("prefix", source, plan, path + ("prefix",)),)
    return ()


def bind_occurrences(
    nodes: tuple[GraphNode, ...], edges: tuple[GraphEdge, ...], *, action_id: str
) -> tuple[GraphNode, ...]:
    """Assign stable preorder occurrence identities and decimal shared tokens."""
    positions = {node.occurrence_id for node in nodes}
    for edge in edges:
        if edge.producer not in positions or edge.consumer not in positions:
            raise _error("INVALID_CONTRACT", "edge references an unknown occurrence")
    bound = []
    for index, node in enumerate(nodes):
        occurrence_id = _hash(["dataflow-occurrence-v3", action_id, index])
        bound.append(GraphNode(
            occurrence_id=occurrence_id,
            semantic_id=node.semantic_id,
            kind=node.kind,
            kernel=node.kernel,
            program_ref=node.program_ref,
            shared_token=str(index),
        ))
    return tuple(bound)


def validate_graph(graph: DataflowGraph, *, bounds: Bounds) -> None:
    """Validate DAG structure, ports, reachability and recomputable identity."""
    if graph.schema != _SCHEMA:
        raise _error("INVALID_CONTRACT", "unexpected graph schema")
    if len(graph.nodes) > bounds.max_nodes:
        raise _error("BUDGET_EXCEEDED", "graph exceeds node budget")
    if len(graph.edges) > bounds.max_edges:
        raise _error("BUDGET_EXCEEDED", "graph exceeds edge budget")
    if not graph.sources:
        raise _error("INVALID_CONTRACT", "graph has no source")
    if not graph.terminals:
        raise _error("INVALID_CONTRACT", "graph has no terminal")
    by_id: dict[str, GraphNode] = {}
    for node in graph.nodes:
        if node.occurrence_id in by_id:
            raise _error("INVALID_CONTRACT", "duplicate occurrence: " + node.occurrence_id)
        by_id[node.occurrence_id] = node
    if set(graph.sources) - by_id.keys() or set(graph.terminals) - by_id.keys():
        raise _error("INVALID_CONTRACT", "source/terminal references an unknown node")
    for node in graph.nodes:
        if node.kind == "source" and node.occurrence_id not in graph.sources:
            raise _error("INVALID_CONTRACT", "source node not declared as a source")
    incoming: dict[str, list[GraphEdge]] = {nid: [] for nid in by_id}
    outgoing: dict[str, list[GraphEdge]] = {nid: [] for nid in by_id}
    seen_edges: set[tuple[str, str, str, str]] = set()
    for edge in graph.edges:
        if edge.producer not in by_id or edge.consumer not in by_id:
            raise _error("INVALID_CONTRACT", "dangling edge endpoint")
        key = (edge.producer, edge.output_port, edge.consumer, edge.input_port)
        if key in seen_edges:
            raise _error("INVALID_CONTRACT", "duplicate edge")
        seen_edges.add(key)
        expected = _hash(["dataflow-edge-v3", edge.producer, edge.output_port,
                          edge.consumer, edge.input_port])
        if edge.edge_id != expected:
            raise _error("INVALID_CONTRACT", "edge identity is not recomputable")
        incoming[edge.consumer].append(edge)
        outgoing[edge.producer].append(edge)
    for node in graph.nodes:
        ports = {port.name for port in node.kernel.inputs}
        used = [edge.input_port for edge in incoming[node.occurrence_id]]
        if len(used) != len(set(used)):
            raise _error("INVALID_CONTRACT", "input port connected more than once")
        if set(used) - ports:
            raise _error("INVALID_CONTRACT", "edge targets an undeclared input port")
        if node.kind != "source" and len(used) != len(ports):
            raise _error("INVALID_CONTRACT", "node has an unconnected input port")
    # Acyclicity and depth via Kahn topological order, checked at the frontier.
    indegree = {nid: len(incoming[nid]) for nid in by_id}
    ready = [nid for nid, degree in indegree.items() if degree == 0]
    depth = {nid: 0 for nid in by_id}
    visited = 0
    while ready:
        nid = ready.pop()
        visited += 1
        if depth[nid] > bounds.max_depth:
            raise _error("BUDGET_EXCEEDED", "graph exceeds depth budget")
        for edge in outgoing[nid]:
            child = edge.consumer
            depth[child] = max(depth[child], depth[nid] + 1)
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
    if visited != len(by_id):
        raise _error("INVALID_CONTRACT", "graph contains a cycle")
    for terminal in graph.terminals:
        if not _reachable(terminal, graph.sources, incoming):
            raise _error("INVALID_CONTRACT", "terminal is not reachable from a source")
    if compute_graph_digest(graph) != graph.dag_id:
        raise _error("INVALID_CONTRACT", "graph digest is not recomputable")


def _reachable(terminal: str, sources: tuple[str, ...], incoming: Mapping[str, list[GraphEdge]]) -> bool:
    source_set = set(sources)
    seen: set[str] = set()
    pending = [terminal]
    while pending:
        nid = pending.pop()
        if nid in seen:
            continue
        seen.add(nid)
        if nid in source_set:
            return True
        pending.extend(edge.producer for edge in incoming[nid])
    return False


class _LiteralEmptySource(SourcePlan):
    """Explicit zero-read source for take(0); never reads the real source."""

    def __repr__(self) -> str:
        return "_LiteralEmptySource()"


class _Capture:
    """One bounded preorder traversal building node/edge drafts and declarations."""

    def __init__(self, request: ActionRequest, bounds: Bounds, inputs) -> None:
        self.request = request
        self.bounds = bounds
        # Everything else the capture needs was resolved before it started:
        # executor defaults and the report bindings (execution.dataflow_compile_inputs).
        self.defaults = inputs.defaults
        self.report = inputs.report
        self.nodes: list[GraphNode] = []
        self.edges: list[tuple[int, str, int, str]] = []
        self.declarations: list[tuple[int, ProgramDeclaration]] = []
        self.alias_source: dict[int, int] = {}
        self.alias_op: dict[tuple[int, int], int] = {}
        self.visiting: set[int] = set()
        self.report_bindings = []
        self.observation_labels = {}
        # Optional codec targets are not independent hard per-node minima.
        # Keep their provenance private until all simultaneous inputs are known.
        self.codec_windows: dict[int, tuple[int, int]] = {}
        self.publication_windows: dict[int, tuple[int, int]] = {}

    def _remember_codec_window(self, index, operation=None):
        window = codec_window(self.nodes[index].kernel, operation, self.bounds)
        if window is not None:
            self.codec_windows[index] = window

    def _bind_report(self, index, key):
        if key is None or self.report is None:
            return
        node = self.nodes[index]
        reused = self.report.reused(key)
        self.report_bindings.append({"report_node_id": key[1], "report_occurrence": key[2],
            "occurrence_id": node.occurrence_id, "logical_node_id": node.semantic_id,
            "mode": "reuse" if reused else "execute"})

    def _check_node_budget(self) -> None:
        if len(self.nodes) >= self.bounds.max_nodes:
            raise _error("BUDGET_EXCEEDED", "capture exceeds node budget")

    def _check_edge_budget(self) -> None:
        if len(self.edges) >= self.bounds.max_edges:
            raise _error("BUDGET_EXCEEDED", "capture exceeds edge budget")

    def _add_node(self, kind: str, kernel: KernelSpec, semantic: Mapping[str, Any],
                  arguments: Mapping[str, Any], code_identity: Mapping[str, Any],
                  program_ref: ArtifactRef | None) -> int:
        self._check_node_budget()
        index = len(self.nodes)
        occurrence = _hash(["dataflow-occurrence-v3", self.request.action.action_id, index])
        configuration = _plain(kernel.configuration)
        configuration["occurrence_id"] = occurrence
        if code_identity and kind == "transform":
            configuration["program"] = occurrence
        kernel = replace(kernel, configuration=configuration)
        self.nodes.append(GraphNode(
            occurrence_id=occurrence,
            semantic_id=_hash(["dataflow-semantic-v3", semantic, arguments, code_identity]),
            kind=kind,
            kernel=kernel,
            program_ref=program_ref,
            shared_token=str(index),
        ))
        return index

    def _add_edge(self, producer: int, output_port: str, consumer: int, input_port: str) -> None:
        self._check_edge_budget()
        self.edges.append((producer, output_port, consumer, input_port))

    def capture_source(self, source: SourcePlan, path: tuple[str, ...]) -> int:
        if len(path) > self.bounds.max_depth:
            raise _error("BUDGET_EXCEEDED", "source path exceeds depth budget")
        key = id(source)
        if key in self.alias_source:
            return self.alias_source[key]
        if key in self.visiting:
            raise _error("INVALID_CONTRACT", "cyclic source reference detected before reading")
        self.visiting.add(key)
        try:
            if isinstance(source, (KeyedSource, UnionSource)):
                index = self._capture_relational(source, path)
            else:
                index = self._capture_leaf(source, path)
            if self.report is not None:
                self._bind_report(index, self.report.source(source))
            self.alias_source[key] = index
            return index
        finally:
            self.visiting.discard(key)

    def _capture_leaf(self, source: SourcePlan, path: tuple[str, ...]) -> int:
        kind = _source_kind(source)
        if source_registry().get(kind) is None and kind != "literal_empty":
            raise _error("UNSUPPORTED_CAPABILITY", "source kind is not registered: " + kind)
        semantic = _source_semantic_facts(source)
        from ..execution.dataflow_schema import literal_schema, encode, KEY
        schema = literal_schema(source, bounds=self.bounds)
        if schema is not None:
            semantic = {**semantic, KEY: encode(schema, bounds=self.bounds)}
        kernel = KernelSpec(
            kind=kind, implementation_ref=None, configuration=_plain(semantic), inputs=(),
            state="none", effect="pure", resource_role="io",
            profile=default_profile(None, bounds=self.bounds), output_bound_bytes=self.bounds.max_row_bytes,
            mergeable=False,
        )
        index = self._add_node("source", kernel, semantic, {}, {"source_kind": kind}, None)
        self._remember_codec_window(index)
        self.declarations.append((index, ProgramDeclaration(
            occurrence_id=self.nodes[index].occurrence_id, callable_spec=None, source_spec=source,
            action_program=None, code_identity={"source_kind": kind},
        )))
        return index

    def _capture_relational(self, source: SourcePlan, path: tuple[str, ...]) -> int:
        bindings = expand_relational_inputs(source, path=path, bounds=self.bounds)
        kind = _source_kind(source)
        child_indices = []
        for binding in bindings:
            if isinstance(source, KeyedSource) and source.kind == "exclude_keys" and binding.port == "left":
                from ..execution.dataflow_native import narrow_to_keys
                narrowed = narrow_to_keys(binding.source, source.on)
                child = self.capture_chain(narrowed, binding.plan, binding.path)
            else:
                child = self.capture_chain(binding.source, binding.plan, binding.path)
            child_indices.append((child, binding.port))
        semantic = _source_semantic_facts(source)
        reducer = None
        if isinstance(source, KeyedSource) and source.kind == 'reduce':
            # One code-pinned declaration owns the reducer and its fixed data.
            # call_args carries the initial accumulator; the fold kernel copies
            # it once per key and calls fn(state, row, **call_kwargs).
            reducer = CallableSpec.create(source.reducer, fn_args=(source.initial,),
                                          fn_kwargs=source.reducer_kwargs)
            reducer = replace(reducer, runtime_contract=source.reducer_contract)
        code_identity = _callable_code_identity(reducer) if reducer is not None else {}
        inputs = tuple(_input_port(port, kind) for _, port in child_indices)
        profile, reducer_memory = relational_profile(kind, source, self.bounds)
        kernel = KernelSpec(
            kind=kind, implementation_ref=None, configuration=_plain(semantic), inputs=inputs,
            state="none", effect="pure", resource_role="cpu",
            profile=profile, output_bound_bytes=self.bounds.max_row_bytes,
            mergeable=False,
        )
        if reducer_memory is not None:
            field, declared = reducer_memory
            kernel = replace(kernel, configuration={**_plain(kernel.configuration), field: declared})
        if isinstance(source, KeyedSource) and source.kind == "exclude_keys":
            configuration = _plain(kernel.configuration)
            configuration.update(lance_key_source=self.nodes[child_indices[0][0]].occurrence_id,
                                 payload_columns=list(source.left.source.query.columns))
            kernel = replace(kernel, configuration=configuration)
        index = self._add_node("relational", kernel, semantic, {}, code_identity, None)
        self._remember_codec_window(index)
        if reducer is not None:
            node = self.nodes[index]
            configuration = _plain(node.kernel.configuration)
            configuration['reducer_program'] = node.occurrence_id
            self.nodes[index] = replace(node, kernel=replace(node.kernel, configuration=configuration))
            self.declarations.append((index, ProgramDeclaration(
                occurrence_id=node.occurrence_id, callable_spec=reducer,
                source_spec=None, action_program=None, code_identity=_plain(code_identity))))
        for child, port in child_indices:
            self._add_edge(child, "main", index, port)
        return index

    def capture_chain(self, source: SourcePlan, plan: LogicalPlan, path: tuple[str, ...]) -> int:
        if len(path) > self.bounds.max_depth:
            raise _error("BUDGET_EXCEEDED", "plan path exceeds depth budget")
        root = id(source)
        index = self.capture_source(source, path)
        for operation in plan.operations:
            op_key = (root, id(operation))
            if op_key in self.alias_op:
                index = self.alias_op[op_key]
                continue
            kernel = compile_kernel_spec(operation, program_ref=None, bounds=self.bounds)
            semantic = _operation_semantic_facts(operation)
            if kernel.kind == 'MapBatchesOp' and not kernel.configuration.get('batch_size'):
                default_batch = self.defaults.map_batch_size
                if default_batch is not None:
                    kernel = replace(kernel, configuration={**dict(kernel.configuration),
                        'default_batch_size': int(default_batch)})
            if kernel.kind == 'RandomShuffleOp' and kernel.configuration.get('num_blocks') is None:
                partitions = self.defaults.partitions
                if partitions is not None:
                    kernel = replace(kernel, configuration={**dict(kernel.configuration),
                        'num_blocks': int(partitions)})
            if kernel.kind == 'RepartitionOp':
                config = kernel.configuration
                if (config.get('keys') or config.get('sort')) and not config.get('shuffle'):
                    raise _error('INVALID_CONTRACT', 'repartition keys/sort require shuffle=True')
                if config.get('sort') and not config.get('keys'):
                    raise _error('INVALID_CONTRACT', 'sorted repartition requires keys')
            if kernel.kind in ('RepartitionOp', 'RandomShuffleOp'):
                count = kernel.configuration.get('num_blocks')
                if count is not None and count > self.bounds.max_partitions:
                    raise _error('BUDGET_EXCEEDED', 'logical block count exceeds max_partitions')
            callable_spec = getattr(operation, "callable", None)
            from ..execution.dataflow_model_factory import native_model_declaration
            native_declaration = native_model_declaration(operation)
            if native_declaration is not None:
                callable_spec = replace(native_declaration,
                    runtime_contract=getattr(callable_spec, 'runtime_contract', None))
            if isinstance(operation, NativeOperation):
                callable_spec = operation.factory
            from ..execution.kernels.task_scope import TASK_KINDS
            if kernel.kind in TASK_KINDS and not kernel.configuration.get('streaming_map'):
                task_rows = self.defaults.task_batch_rows
                state = kernel.state
                group = self.nodes[index].kernel.configuration.get('task_group')
                origin = group is None
                if origin:
                    group = _hash(['dataflow-task-group-v3', self.request.action.action_id, len(self.nodes)])
                kernel = replace(kernel, configuration={**_plain(kernel.configuration),
                    'task_batch_rows': int(task_rows), 'task_group': group, 'task_origin': origin}, state=state,
                    mergeable=kernel.mergeable and state == 'none',
                    profile=default_profile(operation, bounds=self.bounds, state=state))
            elif kernel.kind in {'SelectColumnsOp', 'DropColumnsOp', 'RenameColumnsOp', 'RandomSampleOp'}:
                group = self.nodes[index].kernel.configuration.get('task_group')
                if group is not None:
                    kernel = replace(kernel, configuration={**_plain(kernel.configuration), 'task_group': group})
            code_identity = (
                _callable_code_identity(callable_spec)
                if isinstance(callable_spec, CallableSpec) else {}
            )
            new_index = self._add_node("transform", kernel, semantic, {}, code_identity, None)
            if native_declaration is None or kernel.kind in {'prompt', 'embedding', 'agent', 'fetch', 'document'}:
                self._remember_codec_window(new_index, operation)
            options = getattr(operation, 'stream_options', None)
            base = (getattr(options, 'label', None) or getattr(operation, 'label', None)
                    or getattr(callable_spec, 'name', None) or kernel.kind)
            label, suffix = base, 1
            used = set(self.observation_labels.values())
            while label in used:
                suffix += 1
                label = base + '#' + str(suffix)
            self.observation_labels[self.nodes[new_index].occurrence_id] = label
            if self.report is not None:
                self._bind_report(new_index, self.report.operation(operation))
            self._add_edge(index, "main", new_index, "main")
            if isinstance(callable_spec, CallableSpec):
                self.declarations.append((new_index, ProgramDeclaration(
                    occurrence_id=self.nodes[new_index].occurrence_id, callable_spec=callable_spec,
                    source_spec=None, action_program=None,
                    code_identity=_plain(_callable_code_identity(callable_spec)),
                )))
            for binding in expand_operation_inputs(operation, path=path, bounds=self.bounds):
                child = self.capture_chain(binding.source, binding.plan, binding.path)
                self._add_edge(child, "main", new_index, binding.port)
            self.alias_op[op_key] = new_index
            index = new_index
        return index

    def capture_terminal(self, producer: int) -> int:
        request = self.request
        definition = terminal_registry().get(request.kind)
        if definition is None:
            raise _error("UNSUPPORTED_CAPABILITY", "terminal kind is not registered: " + request.kind)
        semantic = {"kind": request.kind, "arguments": _plain(request.arguments)}
        budget = terminal_budget(request, self.nodes, producer, bounds=self.bounds, semantic=semantic)
        kernel = KernelSpec(
            kind=request.kind, implementation_ref=None, configuration=budget.configuration,
            inputs=(replace(_input_port("main", request.kind), requirement='finite_complete')
                    if budget.phased_publication else _input_port("main", request.kind),),
            state="none", effect="pure", resource_role="cpu",
            profile=budget.profile, output_bound_bytes=self.bounds.max_row_bytes,
            mergeable=False,
        )
        index = self._add_node("terminal", kernel, semantic, _plain(request.arguments), {}, None)
        if budget.publication_window is not None:
            self.publication_windows[index] = budget.publication_window
        if budget.codec_window is not None:
            self.codec_windows[index] = budget.codec_window
        if self.report is not None:
            key = (request.action.action_id, request.action.action_id, 0)
            if self.report.observed(key):
                self._bind_report(index, key)
        self._add_edge(producer, "main", index, "main")
        programs = list(request.programs)
        if request.kind == "aggregate":
            programs = [ActionProgram(name="aggregates", kind="aggregate",
                                      value=tuple(p.value for p in programs))]
        if request.sink is not None:
            programs.append(ActionProgram(name="sink", kind="sink", value=request.sink))
        for program in programs:
            self.declarations.append((index, ProgramDeclaration(
                occurrence_id=self.nodes[index].occurrence_id, callable_spec=None, source_spec=None,
                action_program=program,
                code_identity={"program_kind": program.kind, "name": program.name},
            )))
        return index


def _lower_request(request: ActionRequest) -> ActionRequest:
    """Zero-row take terminals become an explicit literal_empty zero-read plan."""
    if (request.kind == "take" and request.arguments.get("limit") == 0
            or request.kind == "take_batch" and request.arguments.get("batch_size") == 0):
        return ActionRequest(
            kind=request.kind, source=ItemsSource(()), plan=LogicalPlan(),
            arguments=request.arguments, sink=request.sink, programs=request.programs,
            provider=request.provider, action=request.action,
        )
    return request


def capture_graph(request: ActionRequest, *, bounds: Bounds, inputs=None) -> CompiledDataflow:
    """Capture the real source/plan/PlanInput topology into a v3 graph.

    ``inputs`` is a ``CompileInputs``: the executor defaults and report bindings
    this plan is compiled with. When it is not given, it is resolved here once,
    from the request's action and the current report context.
    """
    if terminal_registry().get(request.kind) is None:
        raise _error("UNSUPPORTED_CAPABILITY", "terminal kind is not registered: " + request.kind)
    request = _lower_request(request)
    if inputs is None:
        from ..execution.dataflow_compile_inputs import compile_inputs
        inputs = compile_inputs(request)
    capture = _Capture(request, bounds, inputs)
    producer = capture.capture_chain(request.source, request.plan, ())
    terminal = capture.capture_terminal(producer)
    from .dataflow_graph_adapter import decorate_graph_capture
    terminal = decorate_graph_capture(capture, request, terminal=terminal)
    fit_codec_windows(capture.nodes, capture.edges, capture.codec_windows, capture.publication_windows, bounds)
    # DTO constructors validate identities. Keep integer graph indices private;
    # never construct invalid public DTOs merely to replace their IDs later.
    bound_nodes = tuple(capture.nodes)
    edges = tuple(
        GraphEdge(
            edge_id=_hash(["dataflow-edge-v3", bound_nodes[p].occurrence_id, op,
                           bound_nodes[c].occurrence_id, ip]),
            producer=bound_nodes[p].occurrence_id, output_port=op,
            consumer=bound_nodes[c].occurrence_id, input_port=ip,
        )
        for p, op, c, ip in capture.edges
    )
    declarations = tuple(decl for _, decl in capture.declarations)
    from .dataflow_shared import bind_shared_actors
    bound_nodes = bind_shared_actors(bound_nodes, edges, declarations)
    sources = tuple(node.occurrence_id for node in bound_nodes if node.kind == "source")
    terminals = (bound_nodes[terminal].occurrence_id,)
    graph = DataflowGraph(
        schema=_SCHEMA, action_id=request.action.action_id, dag_id="0" * 64,
        nodes=bound_nodes, edges=edges, terminals=terminals, sources=sources,
    )
    graph = DataflowGraph(
        schema=graph.schema, action_id=graph.action_id,
        dag_id=compute_graph_digest(graph), nodes=graph.nodes, edges=graph.edges,
        terminals=graph.terminals, sources=graph.sources,
    )
    validate_graph(graph, bounds=bounds)
    # Report-only: silent unless the configuration schema logger is enabled for DEBUG.
    from .dataflow_configuration import report_configuration
    report_configuration(graph)
    return CompiledDataflow(graph=graph, declarations=declarations,
                            report_bindings=tuple(capture.report_bindings),
                            observation_labels=capture.observation_labels)


def lower_action(request: ActionRequest, *, bounds: Bounds, inputs=None) -> CompiledDataflow:
    """Compile terminal collect/statistics/write semantics into the graph.

    The compiler's whole input is ``request``, ``bounds`` and ``inputs``
    (``execution.dataflow_compile_inputs.CompileInputs``). It reads nothing else
    from its surroundings; when ``inputs`` is omitted it is resolved once at entry.
    """
    definition = terminal_registry().get(request.kind)
    if definition is None:
        raise _error("UNSUPPORTED_CAPABILITY", "terminal kind is not registered: " + request.kind)
    if request.kind == "write_datasink" and request.sink is None:
        raise _error("INVALID_CONTRACT", "write terminal requires a registered sink declaration")
    return capture_graph(request, bounds=bounds, inputs=inputs)
