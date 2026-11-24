"""Offline model contexts are external data; execution still has one journal.

Only the driver opens the explicitly declared external store. Workers submit
the original bounded model request through their existing occurrence/fence
capability. No provider request, paid-effect lease, or second execution ledger
is created. Pending responses remain pending rather than becoming cache misses.
"""
from demiflow.operator_llm.journal import canonical, request_key
from demiflow.operator_llm.offline import PromptResponsePending
from demiflow.execution.contracts.dataflow import NativeJournalReply
from demiflow.execution.contracts.dataflow_errors import DataflowError





def _sealed_declaration(service, node_id, *, route=None):
    """Read the original passive factory; never construct an actor on driver."""
    if route is not None and (type(route) is not str or not route):
        raise DataflowError('INVALID_CONTRACT', 'Offline route must name a sealed prompt declaration')
    cache_key = node_id if route is None else (node_id, route)
    cached = getattr(service, '_offline_declarations', {}).get(cache_key)
    if cached is not None:
        return cached
    from demiflow.execution.seal.history import offline_model_declaration
    declaration, maximum = offline_model_declaration(service._action.context.directory,
        service._action.action_id, node_id, route=route, store=service._store, bounds=service._bounds)
    with service._lock:
        if not hasattr(service, '_offline_declarations'):
            service._offline_declarations = {}
        if len(service._offline_declarations) >= maximum and cache_key not in service._offline_declarations:
            raise DataflowError('BUDGET_EXCEEDED', 'Offline declaration cache exceeds sealed graph')
        service._offline_declarations[cache_key] = declaration
    return declaration


def offline_lookup(service, command, record):
    if (command.reservation is not None or command.value_ref is not None
            or set(command.options) not in (set(), {'route'})
            or command.options and (type(command.options['route']) is not str or not command.options['route'])):
        raise DataflowError('INVALID_CONTRACT', 'Offline lookup cannot reserve or override its sealed store')
    original = dict(record)
    digest = original.pop('request_sha256', None)
    if digest != request_key(original):
        raise DataflowError('INVALID_CONTRACT', 'Offline request content digest differs')
    options, model = _sealed_declaration(service, command.node_id, route=command.options.get('route'))
    if record.get('model') != model.name:
        raise DataflowError('INVALID_CONTRACT', 'Offline model differs from its sealed declaration')
    maximum = service._bounds.max_control_bytes
    if 'offline_store' in options:
        from demiflow.operator_llm.sqlite_offline import SQLiteOfflinePromptClient
        client = SQLiteOfflinePromptClient(model, options)
    else:
        from demiflow.operator_llm.offline import OfflinePromptClient
        client = OfflinePromptClient(model, options)
    try:
        try:
            response = client.lookup_record(dict(record), max_bytes=maximum)
            result = {'status': 'response', 'content': response.content,
                      'metadata': response.metadata}
        except PromptResponsePending as error:
            result = {'status': 'pending', 'detail': str(error), 'call': error.call}
    finally:
        journal = getattr(client, 'journal', None)
        if journal is not None:
            journal.close()
    encoded = canonical(result).encode('utf-8')
    if len(encoded) > maximum:
        raise DataflowError('BUDGET_EXCEEDED', 'Offline input exceeds the admitted record budget')
    ref = service._store.put(iter((encoded,)), max_bytes=maximum, media_type='application/json')
    return NativeJournalReply(command.operation_id, None, ref)
