"""Driver-owned sealed private model journals and request admission."""
from __future__ import annotations

import re
from demiflow.execution.contracts.dataflow import NativeJournalReply, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import RecoveryContractError, identity
import threading

from dataclasses import replace
from demiflow.execution.contracts.dataflow_codec import private_prompt_rpc_id as _rpc_id


_DELIVERY = frozenset({'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches', 'run_stream'})


_OPERATIONS = frozenset({'lookup', 'reserve', 'reserve_http_retry', 'response', 'failed', 'references'})


def bind_private_prompt_contracts(service, graph):
    nodes, native_routes = {}, set()
    for node in graph.nodes:
        if node.kernel.kind != 'prompt':
            continue
        config = thaw_record_value(node.kernel.configuration)
        if 'routes' in config:
            for route, child in config['routes'].items():
                if child.get('private_journal') is not None:
                    nodes[(node.occurrence_id, route)] = {**child, 'group_max_requests': config['max_requests']}
                elif not set(child.get('options', {})) & {'offline_dir', 'offline_store'}:
                    native_routes.add(node.occurrence_id)
        elif config.get('private_journal') is not None:
            nodes[(node.occurrence_id, None)] = config
    if not nodes:
        return
    from demiflow.execution.pipeline.checked import current
    kind = service._action.declaration.get('kind')
    # Ordinary Prompt may finish its current attempt into the ORIGINAL result
    # manifest. A result commit does not make private HTTP intents resumable.
    materialize = kind == 'materialize' and all(route is None for _, route in nodes)
    if (kind not in _DELIVERY and not materialize or current() is not None
            or service._native is not None or any(node.kernel.kind == 'SaveLanceOp' for node in graph.nodes)):
        raise RecoveryContractError('Private Prompt SQLite requires unmanaged delivery or ordinary current-attempt materialization; managed writes/recovery cannot coexist')
    if service._action.replaying:
        raise RecoveryContractError('Cannot resume attempt-only private Prompt journal delivery')
    service._private_prompt_nodes = nodes
    service._private_prompt_native_routes = native_routes
    service._private_prompt_journals = {}
    service._private_prompt_lock = threading.Lock()
    service._restrict_recovery(next(iter(nodes))[0])
    service._journal.bind_source(service._fence, service._action.action_id + '-private-prompt-journal', {
        'schema': 'demiflow.private_prompt_delivery.v1', 'recovery': 'attempt_only',
        'request_authority': 'declared_private_sqlite', 'admission': 'run_before_private',
        'nodes': [{'occurrence': node, 'route': route} for node, route in nodes]})


def is_private_prompt_request(service, command, request):
    nodes = getattr(service, '_private_prompt_nodes', {})
    if (command.node_id, None) in nodes or 'prompt_route' in command.options:
        return True
    if any(node == command.node_id for node, _ in nodes):
        if 'native_occurrence' in request and command.node_id in service._private_prompt_native_routes:
            return False
        raise DataflowError('INVALID_CONTRACT', 'Private routed Prompt requires its sealed route and row scope')
    return False


def _journal(service, node_id, route):
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    from demiflow.execution.journal.dataflow_authority import private_storage
    with service._private_prompt_lock:
        if service._closed:
            raise DataflowError('FENCED', 'Private Prompt journal owner is closed')
        key = node_id, route
        journal = service._private_prompt_journals.get(key)
        if journal is None:
            config = service._private_prompt_nodes[key]
            options = dict(config['private_journal'])
            maximum = options.pop('max_requests', config['max_requests'])
            options['timeout_s'] = min(options.get('timeout_s', 30), service._bounds.timeout_ms / 1000)
            options['path'], disk = private_storage(service, options['path'])
            journal = SQLitePromptJournal(max_requests=maximum, max_disk_bytes=disk, **options)
            service._private_prompt_journals[key] = journal
        return journal


def private_prompt_request(service, command, request):
    from demiflow.execution.journal.dataflow_authority import _NATIVE_HANDLERS
    route, scope = command.options.get('prompt_route'), command.options.get('row_scope')
    if (command.operation not in _OPERATIONS or command.reservation is not None
            or type(scope) is not str or not re.fullmatch('[0-9a-f]{64}', scope)
            or route is not None and type(route) is not str
            or (command.node_id, route) not in service._private_prompt_nodes
            or 'native_occurrence' in request or 'native_response_bytes' in request
            or command.operation_id != _rpc_id(command, scope, route)):
        raise DataflowError('INVALID_CONTRACT', 'Private Prompt RPC differs from its sealed route/row scope')
    options = {key: value for key, value in command.options.items() if key not in {'prompt_route', 'row_scope'}}
    expected = ({'with_references'} if command.operation == 'lookup' and options else
                {'expected_attempt', 'expected_status', 'max_attempts'} if command.operation == 'reserve_http_retry' else set())
    if (set(options) != expected or 'with_references' in options and type(options['with_references']) is not bool
            or command.operation in {'lookup', 'reserve', 'references', 'reserve_http_retry'} and command.value_ref is not None):
        raise DataflowError('INVALID_CONTRACT', 'Private Prompt RPC contains undeclared fields')
    config = service._private_prompt_nodes[(command.node_id, route)]
    if command.operation == 'reserve_http_retry':
        from demiflow.operator_llm.http_retry import http_error_retry_policy
        policy = http_error_retry_policy(config['options'].get('http_error_retry'))
        if (policy is None or type(options['expected_attempt']) is not int
                or not 1 <= options['expected_attempt'] <= policy['max_retries']
                or type(options['expected_status']) is not int
                or options['expected_status'] not in policy['statuses']
                or type(options['max_attempts']) is not int
                or options['max_attempts'] != policy['max_retries'] + 1):
            raise DataflowError('INVALID_CONTRACT', 'Private Prompt retry differs from its sealed finite policy')
    journal = _journal(service, command.node_id, route)
    if command.operation == 'lookup':
        refs = options.get('with_references', False)
        value = journal.lookup_bounded(request, max_bytes=service._bounds.max_control_bytes, with_references=refs)
        if refs:
            response, references = value
            value = None if response is None else {'response': response, 'references': references}
        return NativeJournalReply(command.operation_id, value, None)
    if command.operation not in {'reserve', 'reserve_http_retry'}:
        return _NATIVE_HANDLERS[command.operation](service, journal, replace(command, options=options), request)
    if command.operation == 'reserve' and journal.lookup_bounded(request, max_bytes=service._bounds.max_control_bytes) is not None:
        return NativeJournalReply(command.operation_id, False, None)
    if journal.read_only:
        raise PermissionError('Read-only private Prompt cache cannot send provider requests')
    ceiling = service._action.context.limits['max_requests']
    maximum = ceiling if config['max_requests'] is None else config['max_requests']
    semantic = command.node_id if route is None else identity(['routed-prompt-route-v1', command.node_id, route])
    charges = {'requests': (1, ceiling), 'requests-' + identity(semantic): (1, maximum)}
    if route is not None:
        maximum = config['group_max_requests']
        charges['requests-' + identity(command.node_id)] = (1, ceiling if maximum is None else maximum)
    retry = options.get('expected_attempt') if command.operation == 'reserve_http_retry' else 0
    reservation = identity(['private-prompt-admission', command.action_id, command.node_id, route, scope, command.request_key, retry])
    from demiflow.execution.run_journal import ExecutionBudgetExceeded
    from demiflow.operator_llm.errors import PromptBudgetExceededError
    try:
        service._journal.reserve_budgets(service._fence, reservation, charges)
    except ExecutionBudgetExceeded as error:
        raise PromptBudgetExceededError('Operator LLM request budget exhausted') from error
    # No refund or cross-database atomicity claim. Original SQLite owns the HTTP
    # lease/retry decision and response; run admission only precedes that work.
    reserved = journal.reserve_http_retry(request, **options) if retry else journal.reserve(request)
    return NativeJournalReply(command.operation_id, bool(reserved), None)
