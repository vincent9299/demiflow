"""Actual worker model journal binding; no driver database authority."""
from __future__ import annotations

import re
from demiflow.execution.contracts.dataflow import thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from dataclasses import replace
from demiflow.execution.operator_bindings.dataflow_model_journal import RemotePromptJournal, make_native_action_view, _rebuild_request_gate
from demiflow.operator_llm.flow_contract import private_prompt_declaration
from demiflow.execution.contracts.dataflow_codec import private_prompt_rpc_id as _rpc_id


_OPERATIONS = frozenset({'lookup', 'reserve', 'reserve_http_retry', 'response', 'failed', 'references'})


class RemotePrivatePromptJournal(RemotePromptJournal):
    def __init__(self, *, services, action, node_id, declaration, route, maximum):
        super().__init__(port=services.journal, action=action, bounds=services.bounds)
        self._bind(node_id, services.store)
        self.declaration, self.route = dict(declaration), route
        self.read_only = declaration.get('read_only', False)
        self.limit = declaration.get('max_requests', maximum)
        self.timeout_s = min(self.timeout_s, declaration.get('timeout_s', 30))

    def _command(self, operation, request, *, value_ref=None, reservation=None, options=None):
        from demiflow.execution.row_scope import current_row_scope
        scope = current_row_scope()
        if scope is None or not re.fullmatch('[0-9a-f]{64}', scope.occurrence):
            raise DataflowError('INVALID_CONTRACT', 'Private Prompt RPC lost its original row scope')
        if reservation is not None or operation not in _OPERATIONS:
            raise DataflowError('INVALID_CONTRACT', 'Private Prompt cannot send native HTTP recovery commands')
        command = super()._command(operation, request, value_ref=value_ref, options={
            **(options or {}), 'prompt_route': self.route, 'row_scope': scope.occurrence})
        return replace(command, operation_id=_rpc_id(command, scope.occurrence, self.route))

    def reserve(self, request):
        return bool(self._rpc(self._command('reserve', request)))

    def reserve_http_retry(self, request, **kwargs):
        if set(kwargs) != {'expected_attempt', 'expected_status', 'max_attempts'}:
            raise DataflowError('INVALID_CONTRACT', 'Private Prompt retry requires the original bounded retry declaration')
        return bool(self._rpc(self._command('reserve_http_retry', request, options=kwargs)))


class _PrivatePromptBinding:
    row_local = False  # A private cache does not grant native pending-call recovery.
    copy_inputs = False
    semantic_owners = ()

    def __init__(self, action, journal):
        self.action, self.journal = action, journal

    def bind_client(self, client):
        if client.journal is not self.journal:
            raise RecoveryContractError('Private Prompt client lost its original bounded driver journal')


def bind_private_prompt_adapter(actor, *, services, node_id, configuration, route=None):
    declared = thaw_record_value(configuration['private_journal'])
    if (private_prompt_declaration(actor._runtime.options) != declared
            or actor._operation.max_requests != configuration['max_requests']
            or getattr(actor._runtime, 'native_recovery', None) is not None):
        raise RecoveryContractError('Private Prompt actor differs from its sealed declaration')
    action = make_native_action_view(services, node_id=node_id)
    journal = RemotePrivatePromptJournal(services=services, action=action, node_id=node_id,
        declaration=declared, route=route, maximum=configuration['max_requests'])
    actor._runtime.native_recovery = _PrivatePromptBinding(action, journal)
    coordinator = actor._runtime.coordinator
    coordinator.bind_execution(action.context, node_id if route is None else identity(['routed-prompt-route-v1', node_id, route]))
    coordinator._native_group_journal = True  # driver derives charges; no worker DB
    actor._runtime.request_gate = _rebuild_request_gate(actor, actor._runtime.concurrency)
