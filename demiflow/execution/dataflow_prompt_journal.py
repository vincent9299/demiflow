"""Delivery-only Prompt caches, using the original driver-owned private SQLite.

HTTP identities/results remain exclusively in that cache. RunJournal owns only
finite execution admission and commits. The two databases are not an atomic
transaction: admission is retained across failures and crash resume is refused.
"""
from dataclasses import replace
import math
import re
import threading

from .contracts.dataflow import NativeJournalReply, thaw_record_value
from .contracts.dataflow_errors import DataflowError
from .dataflow_model_journal import RemotePromptJournal, make_native_action_view, _rebuild_request_gate
from .recovery_contract import RecoveryContractError, identity

_DELIVERY = frozenset({'take', 'take_all', 'take_batch', 'iter_rows', 'iter_batches', 'run_stream'})
_OPERATIONS = frozenset({'lookup', 'reserve', 'reserve_http_retry', 'response', 'failed', 'references'})


def private_prompt_declaration(options):
    value = (options or {}).get('sqlite_journal')
    if value is None:
        return None
    if (type(value) is not dict or set(value) - {'path', 'max_requests', 'read_only', 'timeout_s'}
            or type(value.get('path')) is not str or not value['path']):
        raise ValueError('Private Prompt cache requires an explicit bounded SQLite declaration')
    limit = value.get('max_requests')
    if limit is not None and (type(limit) is not int or not 0 <= limit <= 2**40):
        raise ValueError('Invalid private Prompt request ceiling')
    if type(value.get('read_only', False)) is not bool:
        raise ValueError('Private Prompt read_only must be boolean')
    timeout = value.get('timeout_s', 30)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 3600:
        raise ValueError('Private Prompt lock deadline must be finite')
    if any(key in options for key in ('journal_dir', 'offline_dir', 'offline_store', 'codex_exec', 'codex_agent')):
        raise ValueError('Private Prompt cache requires a single explicit HTTP journal')
    return dict(value)


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
    from .checked import current
    if (service._action.declaration.get('kind') not in _DELIVERY or current() is not None
            or service._native is not None or any(node.kernel.kind == 'SaveLanceOp' for node in graph.nodes)):
        raise RecoveryContractError('Private Prompt SQLite requires unmanaged delivery-only execution; native writes/recovery cannot coexist')
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


def _rpc_id(command, scope, route):
    return identity(['private-prompt-rpc-v1', command.action_id, command.node_id,
                     command.request_key, command.operation, scope, route, dict(command.options)])


class RemotePrivatePromptJournal(RemotePromptJournal):
    def __init__(self, *, services, action, node_id, declaration, route, maximum):
        super().__init__(port=services.journal, action=action, bounds=services.bounds)
        self._bind(node_id, services.store)
        self.declaration, self.route = dict(declaration), route
        self.read_only = declaration.get('read_only', False)
        self.limit = declaration.get('max_requests', maximum)
        self.timeout_s = min(self.timeout_s, declaration.get('timeout_s', 30))

    def _command(self, operation, request, *, value_ref=None, reservation=None, options=None):
        from .row_scope import current_row_scope
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
        # Preserve record()/decode() exactly; no native_occurrence is inserted.


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
    from ..operator_llm.sqlite_journal import SQLitePromptJournal
    from .dataflow_private_storage import private_storage
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
    from .dataflow_authority import _NATIVE_HANDLERS
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
        from ..operator_llm.http_retry import http_error_retry_policy
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
    reservation = identity(['private-prompt-admission', command.action_id, command.node_id, route, command.request_key, retry])
    from .run_journal import ExecutionBudgetExceeded
    from ..operator_llm.errors import PromptBudgetExceededError
    try:
        service._journal.reserve_budgets(service._fence, reservation, charges)
    except ExecutionBudgetExceeded as error:
        raise PromptBudgetExceededError('Operator LLM request budget exhausted') from error
    # No refund or cross-database atomicity claim. Original SQLite owns the HTTP
    # lease/retry decision and response; run admission only precedes that work.
    reserved = journal.reserve_http_retry(request, **options) if retry else journal.reserve(request)
    return NativeJournalReply(command.operation_id, bool(reserved), None)
