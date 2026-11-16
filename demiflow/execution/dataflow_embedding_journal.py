"""Explicit legacy delivery cache over the original driver-owned SQLite journal.

This is not native HTTP recovery. A run's finite request admission precedes the
private journal's reservation; failure between those databases conservatively
retains admission. The original run records attempt-only before any HTTP, so it
cannot resume that window. Only the private journal owns HTTP intent/results.
"""
from __future__ import annotations

import threading
import time

from .contracts.dataflow import NativeJournalReply
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import RecoveryContractError, identity

_DELIVERY = frozenset({'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches', 'run_stream'})
_OPERATIONS = frozenset({'lookup', 'reserve', 'response', 'failed', 'references'})


def bind_private_embedding_contracts(service, graph):
    """Register sealed declarations, never a path supplied by a worker command."""
    from .contracts.dataflow import thaw_record_value
    nodes = {node.occurrence_id: thaw_record_value(node.kernel.configuration)
             for node in graph.nodes if node.kernel.kind == 'embedding'
             and node.kernel.configuration.get('private_journal') is not None}
    if not nodes:
        return
    if service._action.declaration.get('kind') not in _DELIVERY:
        raise RecoveryContractError('Native embedding recovery cannot coexist with a private SQLite journal; '
                                    'explicit private journals require a delivery-only terminal')
    from .checked import current
    if current() is not None:
        raise RecoveryContractError('Managed execution requires the native embedding journal')
    if service._native is not None or any(node.kernel.kind == 'SaveLanceOp' for node in graph.nodes):
        raise RecoveryContractError('Native acquisition/writes cannot coexist with a private SQLite journal')
    if service._action.replaying:
        raise RecoveryContractError('Cannot resume attempt-only private embedding journal delivery')
    service._private_embedding_nodes = nodes
    service._private_embedding_journals = {}
    service._private_embedding_lock = threading.Lock()
    # The existing run/manager admission consults this original restriction key.
    # A separate descriptive fact explains its cause without changing that key's
    # immutable schema (other attempt-only actors may share the same action).
    service._restrict_recovery(next(iter(nodes)))
    service._journal.bind_source(service._fence, service._action.action_id + '-private-embedding-journal', {
        'schema': 'demiflow.private_embedding_delivery.v1', 'recovery': 'attempt_only',
        'request_authority': 'declared_private_sqlite', 'admission': 'run_before_private',
        'nodes': sorted(nodes)})


def bind_private_embedding_adapter(actor, *, services, node_id, configuration):
    from .contracts.dataflow import thaw_record_value
    from .dataflow_model_journal import make_native_action_view, RemotePrivateEmbeddingJournal
    declared = thaw_record_value(configuration['private_journal'])
    if (actor.options.get('sqlite_journal') != declared
            or actor.max_requests != configuration['max_requests']
            or actor._native_recovery is not None or actor._journal is not None):
        raise RecoveryContractError('Private embedding actor differs from its sealed journal declaration')
    action = make_native_action_view(services, node_id=node_id)
    actor._journal_binding = RemotePrivateEmbeddingJournal(
        services=services, action=action, node_id=node_id, declaration=declared)


def _journal(service, node_id):
    from ..operator_llm.sqlite_journal import SQLitePromptJournal
    from .dataflow_private_storage import private_storage
    with service._private_embedding_lock:
        if service._closed:
            raise DataflowError('FENCED', 'Private embedding journal owner is closed')
        journal = service._private_embedding_journals.get(node_id)
        if journal is None:
            config = service._private_embedding_nodes[node_id]
            options = dict(config['private_journal'])
            options['timeout_s'] = min(options.get('timeout_s', 30), service._bounds.timeout_ms / 1000)
            options['path'], disk = private_storage(service, options['path'])
            journal = SQLitePromptJournal(max_requests=config['max_requests'],
                                          max_disk_bytes=disk, **options)
            service._private_embedding_journals[node_id] = journal
        return journal


def private_embedding_request(service, command, request):
    from .dataflow_authority import _NATIVE_HANDLERS
    from .dataflow_model_journal import _rpc_operation_id
    if (command.operation not in _OPERATIONS or command.reservation is not None
            or 'native_occurrence' in request or 'native_response_bytes' in request
            or command.operation_id != _rpc_operation_id(command.action_id, command.node_id,
                                                          command.request_key, command.operation)):
        raise DataflowError('INVALID_CONTRACT', 'Private embedding does not accept native recovery operations')
    expected_options = {'with_references'} if command.operation == 'lookup' and command.options else set()
    if (set(command.options) != expected_options
            or expected_options and type(command.options['with_references']) is not bool
            or command.operation in {'lookup', 'reserve', 'references'} and command.value_ref is not None):
        raise DataflowError('INVALID_CONTRACT', 'Private embedding command contains undeclared fields')
    journal = _journal(service, command.node_id)
    if command.operation == 'lookup':
        with_references = command.options.get('with_references', False)
        value = journal.lookup_bounded(request, max_bytes=service._bounds.max_control_bytes,
                                       with_references=with_references)
        if with_references:
            response, references = value
            value = None if response is None else {'response': response, 'references': references}
        return NativeJournalReply(command.operation_id, value, None)
    if command.operation != 'reserve':
        return _NATIVE_HANDLERS[command.operation](service, journal, command, request)
    # Confirmed cache replay consumes no fresh request budget. Pending/failed
    # intent remains an error in the ORIGINAL journal, never a cache miss.
    if journal.lookup_bounded(request, max_bytes=service._bounds.max_control_bytes) is not None:
        return NativeJournalReply(command.operation_id, False, None)
    if journal.read_only:
        raise LookupError('Embedding request missing from read-only journal: ' + command.request_key)
    config = service._private_embedding_nodes[command.node_id]
    ceiling = service._action.context.limits['max_requests']
    node_ceiling = ceiling if config['max_requests'] is None else config['max_requests']
    reservation = identity(['private-embedding-admission', command.action_id,
                            command.node_id, command.request_key])
    from .run_journal import ExecutionBudgetExceeded
    from ..operator_llm.errors import PromptBudgetExceededError
    try:
        service._journal.reserve_budgets(service._fence, reservation, {
            'requests': (1, ceiling), 'requests-' + identity(command.node_id): (1, node_ceiling)})
    except ExecutionBudgetExceeded as error:
        raise PromptBudgetExceededError('Operator LLM request budget exhausted') from error
    # Deliberately no refund/second result write/cross-DB atomicity claim. A
    # crash here keeps the finite admission and makes this run nonresumable.
    reserved = journal.reserve(request)
    return NativeJournalReply(command.operation_id, bool(reserved), None)


def close_private_embedding_journals(service, *, deadline):
    if not hasattr(service, '_private_embedding_journals') and not hasattr(service, '_private_prompt_journals'):
        return

    def close_owned():
        # Never close a DB underneath an accepted service command. Retain this
        # owned thread until both commands and the original journals are closed;
        # physically_stopped is false while it is alive.
        for worker in service._workers:
            worker.join()
        journals = (*getattr(service, '_private_embedding_journals', {}).values(),
                    *getattr(service, '_private_prompt_journals', {}).values())
        for journal in journals:
            try:
                journal.close()
            except BaseException as error:
                service._private_close_error = error

    thread = threading.Thread(target=close_owned, name='dataflow-private-journal-close', daemon=True)
    service._private_close_thread = thread
    thread.start()
    thread.join(max(0.0, deadline - time.monotonic()))
    if not thread.is_alive() and getattr(service, '_private_close_error', None) is not None:
        raise service._private_close_error
