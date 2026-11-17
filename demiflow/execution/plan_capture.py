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
    explicit_limits = ()

    def __init__(self):
        from .runtime_limits import execution_limits
        self._limits = execution_limits()

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


class _Capture:
    def __init__(self):
        self.result = None
        self.context = CompileContext()

    def intercept(self, request, *, policy, bounds):
        from ..planning.dataflow_compile import lower_action
        from .contracts.dataflow_errors import DataflowError
        if self.result is not None:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'construction caught the planning boundary; later actions cannot be guessed')
        compiled = lower_action(request, bounds=bounds)
        self.result = CapturedAction(compiled, request.kind, request.provider, policy, bounds)
        raise _Captured()


def capture_pipeline(entry, arguments) -> CapturedAction:
    """Compile the exact first action request from a trusted callable and kwargs.

    The same make_action_request/resolve_action_runtime/lower_action functions
    are used by execution. No placeholder receipt, source snapshot, physical
    placement, worker or completion fact is produced. The returned graph covers
    one action, never a claim about the rest of an arbitrary Python program.
    """
    from .contracts.dataflow_errors import DataflowError
    from .run_context import current_run
    if not callable(entry) or type(arguments) is not dict:
        raise TypeError('capture_pipeline requires a callable and keyword arguments')
    from .checked import current as current_checked
    if current_capture() is not None or current_run() is not None or current_checked() is not None:
        raise DataflowError('INVALID_CONTRACT', 'plan capture cannot nest inside execution or another capture')
    capture = _Capture()
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
    semantic = {node.occurrence_id: node.semantic_id for node in graph.nodes}
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
        label = labels.get(node.occurrence_id) or (prompt if type(prompt) is str else
                 program['qualname'] if program is not None else node.kernel.kind)
        return dict(label=label[:256], program=program)

    ports = {}
    for edge in graph.edges:
        ports.setdefault(edge.producer, [])
        if edge.output_port not in ports[edge.producer]:
            ports[edge.producer].append(edge.output_port)
    return dict(
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
