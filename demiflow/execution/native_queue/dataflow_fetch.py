"""Bind ordinary native fetch declarations to their sealed graph occurrences."""
from copy import copy
from dataclasses import asdict

from demiflow.execution.contracts.dataflow import QueueBinding
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.native_queue.raw_fetch import RawFetch
from demiflow.execution.contracts.recovery_contract import identity


def fetch_declaration_key(actor):
    # Runtime concurrency is absent from the business identity, but selecting
    # an actor must preserve each declaration's resource settings as well.
    return identity([actor.recovery_identity(), actor.concurrency, actor.queue_depth])


def configure_fetch_graph(service, source, plan, graph):
    """Reuse original RawFetch transactions, with one actor per graph occurrence.

    Relational declarations may contain independent equal actors or a shared
    upstream. The compiler's occurrence is the execution identity; a declaration
    digest only finds the immutable constructor facts, never merges executions.
    """
    from demiflow.planning.dataflow_compile import iter_declared_operations
    bounds = service._services.bounds
    actors = {}
    for operation in iter_declared_operations(source, plan, bounds=bounds):
        target = getattr(getattr(operation, 'callable', None), 'target', None)
        actor = getattr(target, '__self__', target)
        if type(actor) is RawFetch:
            if actor.io is not None or actor.queue_source is not None:
                raise DataflowError('INVALID_CONTRACT', 'Ordinary fetch requires an unstarted declaration')
            actors[fetch_declaration_key(actor)] = actor
    nodes = [node for node in graph.nodes
             if node.kernel.configuration.get('native_operation') == 'acquire']
    if not nodes or len(nodes) > bounds.max_nodes:
        raise DataflowError('INVALID_CONTRACT', 'Fetch authority requires bounded sealed occurrences')
    fetches = {}
    # Preserve the original namespace for the existing single top-level route.
    top = [(i, getattr(getattr(op, 'callable', None), 'target', None))
           for i, op in enumerate(plan.operations)]
    top = [(i, getattr(target, '__self__', target)) for i, target in top]
    top = [(i, actor) for i, actor in top if type(actor) is RawFetch]
    for node in nodes:
        key = node.kernel.configuration.get('native_key')
        if key is None and len(nodes) == len(actors) == 1:
            key = next(iter(actors))  # Previously sealed single-fetch plans.
        if key not in actors:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Fetch occurrence has no matching declaration')
        actor = copy(actors[key])
        suffix = str(top[0][0]) if len(nodes) == len(top) == 1 else node.occurrence_id
        actor.bind_execution(service._action.context, service.action_id + '-node-' + suffix)
        fetches[node.occurrence_id] = actor
    service._fetches = fetches
    service._fetch = None
    service._mode = 'raw_fetch'
    service._io_concurrency = min(bounds.max_pending_events, sum(a.concurrency for a in fetches.values()))
    service._binding = QueueBinding(service.action_id, None, 'raw_fetch',
        identity([[key, actor.recovery_identity()] for key, actor in sorted(fetches.items())]),
        None, None, None, next(iter(fetches)), None)
    key = service.action_id + '-fetch-binding'
    journal = service._action.context.journal
    service._prior_fetch_binding = journal.source_binding(key)
    journal.bind_source(service._action.context.fence, key, asdict(service._binding))
    service._state = 'configured'
    return service._binding
