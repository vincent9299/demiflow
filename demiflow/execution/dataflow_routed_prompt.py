"""Native routed prompts over the original occurrence and request journal.

The public route column chooses the prompt; placement never changes that choice.
Child factories are passive declarations. RPC authority remains the real graph
occurrence, while request identity and per-route quota have stable subnamespaces.
"""
from dataclasses import replace
from types import SimpleNamespace

from demiflow.data.plan import CallableSpec
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import identity
from demiflow.execution.operator_bindings.dataflow_model_journal import RemotePromptJournal


class _RoutedPromptJournal(RemotePromptJournal):
    """Same RPC/SQLite transaction, adding the declared whole-node ceiling."""
    def __init__(self, *, group_limit, route, **kwargs):
        super().__init__(**kwargs)
        self._group_limit = group_limit
        self._route = route

    def _command(self, operation, request, *, value_ref=None, reservation=None, options=None):
        if reservation is not None:
            key, charges = reservation
            reservation = (key, {**charges, 'requests-' + identity(self._node_id):
                                  (1, self._group_limit)})
        command = super()._command(operation, request, value_ref=value_ref,
                                    reservation=reservation, options=options)
        if operation == 'offline_lookup':
            if options:
                raise DataflowError('INVALID_CONTRACT', 'Offline route cannot override its sealed declaration')
            # Equal prompts in different explicitly declared stores can have
            # equal source request digests. Keep their RPC waiters distinct.
            command = replace(command, options={'route': self._route},
                operation_id=identity(['routed-offline-input-v1', command.operation_id, self._route]))
        return command


def routed_declaration(actor, target):
    from demiflow.operator_llm.routed import RoutedPromptActor
    from demiflow.operator_llm.native_actor import is_native_prompt_actor
    from demiflow.execution.dataflow_model_factory import native_model_declaration, RoutedPromptFactory
    if (getattr(target, '__func__', None) is not RoutedPromptActor.__call__
            or type(actor.actors) is not dict or not 1 <= len(actor.actors) <= 32
            or type(actor.limits) is not dict or set(actor.actors) != set(actor.limits)
            or not isinstance(actor.route, str) or not actor.route
            or type(actor.isolate_failures) is not bool or actor.gates
            or actor.max_requests is not None and (type(actor.max_requests) is not int
                                                   or actor.max_requests < 0)):
        raise DataflowError('INVALID_CONTRACT', 'Routed prompt requires an unstarted native declaration')
    children = {}
    for name, child in actor.actors.items():
        capacity = actor.limits[name]
        if (not isinstance(name, str) or not name or type(capacity) is not int
                or not 1 <= capacity <= 256 or not is_native_prompt_actor(child)
                or child._runtime.concurrency != capacity
                or child._prompt.response_format == 'image'):
            raise DataflowError('INVALID_CONTRACT', 'Routed prompt child differs from its declared capacity/model')
        children[name] = native_model_declaration(SimpleNamespace(callable=CallableSpec.create(child)))
    return CallableSpec.create(RoutedPromptFactory, fn_constructor_args=(
        children, dict(actor.limits), actor.route, actor.isolate_failures, actor.max_requests))





def bind_routed_prompt(actor, *, services, node_id, configuration):
    from demiflow.execution.operator_bindings.dataflow_model_journal import make_native_action_view, RemotePromptRecovery, _rebuild_request_gate
    action = make_native_action_view(services, node_id=node_id)
    for name, child in actor.actors.items():
        if configuration['routes'][name].get('private_journal') is not None:
            from demiflow.execution.dataflow_prompt_journal import bind_private_prompt_adapter
            bind_private_prompt_adapter(child, services=services, node_id=node_id,
                                        configuration=configuration['routes'][name], route=name)
            continue
        recovery = RemotePromptRecovery(child, action, node_id, services=services)
        journal = _RoutedPromptJournal(port=services.journal, action=action, bounds=services.bounds, route=name,
            group_limit=(action.context.limits['max_requests'] if actor.max_requests is None
                         else actor.max_requests))
        journal._bind(node_id, services.store)
        recovery.journal = journal
        # RemotePromptJournal retains node_id for the original lease check.
        # Only the native request and budget namespace is route-specific.
        semantic_node = identity(['routed-prompt-route-v1', node_id, name])
        recovery.node_id = semantic_node
        child._runtime.native_recovery = recovery
        coordinator = child._runtime.coordinator
        coordinator.bind_execution(action.context, semantic_node)
        coordinator._native_group_journal = True
        child._runtime.request_gate = _rebuild_request_gate(child, child._runtime.concurrency)
