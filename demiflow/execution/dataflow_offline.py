"""Offline model contexts are external data; execution still has one journal.

Only the driver opens the explicitly declared external store. Workers submit
the original bounded model request through their existing occurrence/fence
capability. No provider request, paid-effect lease, or second execution ledger
is created. Pending responses remain pending rather than becoming cache misses.
"""
from ..operator_llm.journal import canonical, request_key
from ..operator_llm.offline import PromptResponsePending, request_record
from ..operator_llm.model import OperatorLLMResponse
from .contracts.dataflow import NativeJournalReply
from .contracts.dataflow_errors import DataflowError


class RemoteOfflinePromptClient:
    def __init__(self, model, options, journal):
        from .dataflow_model_journal import RemotePromptJournal
        if not isinstance(journal, RemotePromptJournal):
            raise ValueError('Offline input requires the verified native worker port')
        if set(options) not in ({'offline_dir'}, {'offline_store'}):
            raise ValueError('Offline input accepts exactly one declared store')
        self.model, self.options, self.journal = model, options, journal

    def lookup(self, request):
        record = request_record(request)
        reply = self.journal._rpc(self.journal._command('offline_lookup', record))
        if reply['status'] == 'pending':
            error = PromptResponsePending(reply['detail'])
            error.call = reply['call']
            raise error
        if reply['status'] != 'response':
            raise DataflowError('INTERNAL_PROTOCOL', 'Unknown offline lookup result')
        return OperatorLLMResponse(reply['content'], metadata=reply['metadata'])

    async def execute(self, request):
        from ..operator_llm.client import _journal_io
        return await _journal_io(self.lookup, request)

    async def aclose(self):
        pass  # The run owns the port; no worker file/database was opened.


def _sealed_declaration(service, node_id):
    """Read the original passive factory; never construct an actor on driver."""
    from .dataflow_seal import load_dataflow, load_sealed_dataflow, verify_dataflow_program
    from .dataflow_model_factory import PromptFactory
    from ..data.plan import CallableSpec
    cached = getattr(service, '_offline_declarations', {}).get(node_id)
    if cached is not None:
        return cached
    action, bounds, store = service._action, service._bounds, service._store
    graph, _, sealed = load_dataflow(action.context.directory, action.action_id,
                                    store=store, bounds=bounds)
    node = next((node for node in graph.nodes if node.occurrence_id == node_id), None)
    if node is None or node.kernel.kind not in {'prompt', 'agent'} or node.program_ref is None:
        raise DataflowError('INVALID_CONTRACT', 'Offline lookup has no sealed model occurrence')
    _, _, pin = load_sealed_dataflow(sealed, action_id=action.action_id, store=store, bounds=bounds)
    program = verify_dataflow_program(node.program_ref,
        expected_pin=pin['programs'][node_id], store=store, bounds=bounds)
    spec = program.value
    if (not isinstance(spec, CallableSpec) or spec.target is not PromptFactory
            or len(spec.constructor_args) != 2):
        raise DataflowError('INVALID_CONTRACT', 'Offline lookup requires the original prompt factory')
    operation, _ = spec.constructor_args
    options = dict(operation.options or {})
    if set(options) not in ({'offline_dir'}, {'offline_store'}):
        raise DataflowError('INVALID_CONTRACT', 'Occurrence declares no offline input store')
    declaration = options, operation.config.prompt_definitions[operation.prompt_name].model
    with service._lock:
        if not hasattr(service, '_offline_declarations'):
            service._offline_declarations = {}
        if len(service._offline_declarations) >= len(graph.nodes) and node_id not in service._offline_declarations:
            raise DataflowError('BUDGET_EXCEEDED', 'Offline declaration cache exceeds sealed graph')
        service._offline_declarations[node_id] = declaration
    return declaration


def offline_lookup(service, command, record):
    if command.reservation is not None or command.value_ref is not None or command.options:
        raise DataflowError('INVALID_CONTRACT', 'Offline lookup cannot reserve or override its sealed store')
    original = dict(record)
    digest = original.pop('request_sha256', None)
    if digest != request_key(original):
        raise DataflowError('INVALID_CONTRACT', 'Offline request content digest differs')
    options, model = _sealed_declaration(service, command.node_id)
    if record.get('model') != model.name:
        raise DataflowError('INVALID_CONTRACT', 'Offline model differs from its sealed declaration')
    maximum = service._bounds.max_control_bytes
    if 'offline_store' in options:
        from ..operator_llm.sqlite_offline import SQLiteOfflinePromptClient
        client = SQLiteOfflinePromptClient(model, options)
    else:
        from ..operator_llm.offline import OfflinePromptClient
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
