"""Actual worker model journal binding; no driver database authority."""
from __future__ import annotations

import re
from demiflow.execution.contracts.dataflow import thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import RecoveryContractError

from demiflow.execution.operator_bindings.dataflow_model_journal import make_native_action_view


def bind_private_embedding_adapter(actor, *, services, node_id, configuration):
    from demiflow.execution.contracts.dataflow import thaw_record_value
    from demiflow.execution.operator_bindings.dataflow_model_journal import make_native_action_view, RemotePrivateEmbeddingJournal
    declared = thaw_record_value(configuration['private_journal'])
    if (actor.options.get('sqlite_journal') != declared
            or actor.max_requests != configuration['max_requests']
            or actor._native_recovery is not None or actor._journal is not None):
        raise RecoveryContractError('Private embedding actor differs from its sealed journal declaration')
    action = make_native_action_view(services, node_id=node_id)
    actor._journal_binding = RemotePrivateEmbeddingJournal(
        services=services, action=action, node_id=node_id, declaration=declared)
    from demiflow.execution.worker.row_scope import current_row_scope
    from demiflow.execution.operator_bindings.dataflow_model_journal import _PRIVATE_EMBEDDING_SCOPE
    maximum = int(configuration.get('max_batch') or 256)
    exchange = actor._exchange
    async def scoped_exchange(request, encoded, timings, *, request_part=0):
        # _invoke_batch already bound its real members and invocation cursor.
        # Original byte packing yields at least one input per part, so part is
        # bounded by the declared batch. Do not add it to the HTTP request.
        scope = current_row_scope()
        if (scope is None or not re.fullmatch('[0-9a-f]{64}', scope.occurrence)
                or type(request_part) is not int or not 0 <= request_part < maximum):
            raise DataflowError('INVALID_CONTRACT', 'Private embedding batch/request scope is invalid')
        token = _PRIVATE_EMBEDDING_SCOPE.set((scope.occurrence, request_part))
        try:
            return await exchange(request, encoded, timings, request_part=request_part)
        finally:
            _PRIVATE_EMBEDDING_SCOPE.reset(token)
    actor._exchange = scoped_exchange
