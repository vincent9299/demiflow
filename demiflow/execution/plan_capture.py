"""Capture the first real Dataset action without starting it.

This executes trusted Python *construction* code, not Dataset rows. It is not
a Python sandbox: callers accepting entry paths must run it in a read-only,
network-disabled worker. An action result is never fabricated, so construction
stops at the first execution boundary. Later branches/actions remain unknown.
"""
from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from collections.abc import Mapping
import inspect
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .contracts.dataflow import Bounds, CompiledDataflow, DataflowGraph, RuntimePolicy, ProgramDeclaration

_CURRENT = ContextVar('demiflow_plan_capture', default=None)


def current_capture():
    return _CURRENT.get()


class _Captured(BaseException):
    """Unwind construction before data, workers, journals or sinks start."""


class CompileContext:
    fence = None
    attempt = 0
    ref = None

    def __init__(self, limits=None, explicit_limits=None):
        from .runtime_limits import execution_limits, declared_limit_keys
        self._limits = execution_limits(limits)
        self.explicit_limits = declared_limit_keys(limits, explicit_limits)

    @property
    def limits(self):
        from .payload_limits import active_limits
        return active_limits(self, self._limits)

    def check_stop(self):
        return None


@dataclass(frozen=True)
class CapturedAction:
    compiled: CompiledDataflow
    kind: str
    provider: str
    policy: RuntimePolicy
    bounds: Bounds
    entry_completed: bool = False
    submission: dict | None = None


class _Capture:
    def __init__(self, on_compiled=None, *, action_id='preview', limits=None, explicit_limits=None, submission=False):
        self.result = None
        self.context = CompileContext(limits, explicit_limits)
        self.on_compiled = on_compiled
        self.action_id = action_id
        self.submission = submission

    def intercept(self, request, *, policy, bounds):
        from ..planning.dataflow_compile import lower_action
        from .contracts.dataflow_errors import DataflowError
        if self.result is not None:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'construction caught the planning boundary; later actions cannot be guessed')
        compiled = lower_action(request, bounds=bounds, on_compiled=self.on_compiled)
        contract = None
        if self.submission:
            from .managed_native import submission_contract
            contract = submission_contract(request, compiled, bounds=bounds)
        self.result = CapturedAction(compiled, request.kind, request.provider, policy, bounds, submission=contract)
        raise _Captured()


def capture_pipeline(entry, arguments, *, on_compiled=None, limits=None, explicit_limits=None, _submission=False) -> CapturedAction:
    """Compile the exact first action request from a trusted callable and kwargs.

    The same make_action_request/resolve_action_runtime/lower_action functions
    are used by execution. No placeholder receipt, source snapshot, physical
    placement, worker or completion fact is produced. The returned graph covers
    one action, never a claim about the rest of an arbitrary Python program.
    Optional host callback ``on_compiled(CompilationProvenance)`` runs once after
    final graph validation, before unwinding the active SDK call. It receives
    only bounded, process-local origin links; it never changes sealed identity.
    Budget exhaustion is reported as unavailable provenance. Callback errors
    propagate (capture context still resets); no success is fabricated.
    """
    from .contracts.dataflow_errors import DataflowError
    from .run_context import current_run
    if not callable(entry) or type(arguments) is not dict:
        raise TypeError('capture_pipeline requires a callable and keyword arguments')
    if on_compiled is not None and not callable(on_compiled):
        raise TypeError('on_compiled must be a callable or None')
    from .checked import current as current_checked
    if current_capture() is not None or current_run() is not None or current_checked() is not None:
        raise DataflowError('INVALID_CONTRACT', 'plan capture cannot nest inside execution or another capture')
    capture = _Capture(on_compiled, action_id='action-1' if _submission else 'preview',
                       limits=limits, explicit_limits=explicit_limits, submission=_submission)
    token = _CURRENT.set(capture)
    try:
        try:
            returned = entry(**arguments)
        except _Captured:
            if capture.result is not None:
                return capture.result
            raise
        if inspect.iscoroutine(returned) or inspect.isgenerator(returned):
            returned.close()
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'entry did not reach a Dataset action, or swallowed the planning boundary')
    finally:
        _CURRENT.reset(token)


def build_logical_plan_view(graph: DataflowGraph, *, declarations: tuple[ProgramDeclaration, ...] = (),
                            observation_labels: Mapping[str, str] | None = None) -> dict:
    """Project a compiled graph without inventing placement or execution facts.

    IDs and all edges come directly from the engine's immutable DataflowGraph.
    This view is suitable for an explicitly unexecuted preview, not a historical
    run binding. It performs no source inspection, compilation or external I/O.
    """
    from .contracts.dataflow import DataflowGraph, ProgramDeclaration
    if not isinstance(graph, DataflowGraph):
        raise TypeError('build_logical_plan_view requires a DataflowGraph')
    semantic = {node.occurrence_id: node.semantic_id for node in (*graph.nodes, *graph.jobs)}
    labels = observation_labels if observation_labels is not None else {}
    if (not isinstance(labels, Mapping) or not set(labels) <= set(semantic)
            or any(type(v) is not str or not v for v in labels.values())):
        raise ValueError('labels must name compiled graph occurrences')
    if type(declarations) is not tuple or any(type(d) is not ProgramDeclaration for d in declarations):
        raise TypeError('declarations must be the compiled ProgramDeclaration tuple')
    programs = {}
    for declaration in declarations:
        if declaration.occurrence_id not in semantic:
            raise ValueError('declaration occurrence is outside the compiled graph')
        # Read only frozen JSON produced by the compiler. Never getattr a user
        # callable, descriptor, class or actor merely to give a node its label.
        target = declaration.code_identity.get('target')
        if isinstance(target, Mapping):
            target = target.get('actor', target)
        if isinstance(target, Mapping):
            module, name = target.get('module'), target.get('name')
            if type(module) is str and type(name) is str:
                programs[declaration.occurrence_id] = dict(module=module[:256], qualname=name[:256])

    def presentation(node):
        program = programs.get(node.occurrence_id)
        prompt = node.kernel.configuration.get('prompt_name')
        source = None
        config = node.kernel.configuration
        if node.kernel.kind == 'lance':
            query = config.get('query', {})
            fields = query.get('fields', {}) if isinstance(query, Mapping) else {}
            uri = fields.get('uri') if isinstance(fields, Mapping) else None
            if type(uri) is str:
                from urllib.parse import urlsplit
                # Only the frozen table basename, never credentials/query strings.
                name = urlsplit(uri).path.rstrip('/').rsplit('/', 1)[-1][:160]
                version = fields.get('version')
                source = dict(name=name, version=version if type(version) is int else None)
        label = labels.get(node.occurrence_id) or (prompt if type(prompt) is str else
                 program['qualname'] if program is not None else
                 '读取 ' + source['name'] if source and source['name'] else node.kernel.kind)
        return dict(label=label[:256], program=program, data_source=source)

    ports = {}
    for edge in (*graph.edges, *graph.value_edges):
        ports.setdefault(edge.producer, [])
        if edge.output_port not in ports[edge.producer]:
            ports[edge.producer].append(edge.output_port)
    result = dict(
        schema_version=graph.schema, dag_id=graph.dag_id, action_id=graph.action_id,
        nodes=[dict(logical_node_id=node.semantic_id, occurrence_id=node.occurrence_id,
                    op_kind=node.kernel.kind, input_ports=[p.name for p in node.kernel.inputs],
                    output_ports=ports.get(node.occurrence_id, []),
                    **presentation(node),
                    traits=dict(state=node.kernel.state, effect=node.kernel.effect,
                                resource_role=node.kernel.resource_role, mergeable=node.kernel.mergeable))
               for node in graph.nodes],
        edges=[dict(edge_id=edge.edge_id,
                    **{'from':dict(logical_node_id=semantic[edge.producer],
                                   occurrence_id=edge.producer, port=edge.output_port),
                       'to':dict(logical_node_id=semantic[edge.consumer],
                                 occurrence_id=edge.consumer, port=edge.input_port)})
               for edge in graph.edges],
        sources=list(graph.sources), terminals=list(graph.terminals))
    if graph.jobs:
        project_training_ports(graph, result)
    return result


def project_training_ports(graph, dag):
    """Append sealed jobs and typed dependencies to a logical or historical view."""
    from .recovery_contract import identity
    by_id = {n.occurrence_id: n.semantic_id for n in (*graph.nodes, *graph.jobs)}
    inputs, outputs = {}, {}
    for edge in graph.value_edges:
        inputs.setdefault(edge.consumer, []).append(edge.input_port)
        outputs.setdefault(edge.producer, set()).add(edge.output_port)
        dag["edges"].append({
            "edge_id": identity(["dataflow-value-edge-v4", edge.producer, edge.output_port, edge.consumer, edge.input_port]),
            "kind": edge.kind, "step": edge.step,
            "from": {"logical_node_id": by_id[edge.producer], "occurrence_id": edge.producer, "port": edge.output_port},
            "to": {"logical_node_id": by_id[edge.consumer], "occurrence_id": edge.consumer, "port": edge.input_port}})
    for job in graph.jobs:
        dag["nodes"].append({"logical_node_id": job.semantic_id, "occurrence_id": job.occurrence_id, "op_kind": "fit",
                             "label": "fit", "input_ports": [], "output_ports": []})
    for node in dag["nodes"]:
        key = node["occurrence_id"]
        node["input_ports"].extend(inputs.get(key, ()))
        for name in sorted(outputs.get(key, ())):
            if name not in node["output_ports"]:
                node["output_ports"].append(name)
