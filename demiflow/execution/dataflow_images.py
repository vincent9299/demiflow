"""Native image ownership through the driver and the existing object/row sink."""
from dataclasses import asdict

from .contracts.dataflow import NativeJournalReply
from .contracts.dataflow_errors import DataflowError
from .object_sink import ObjectRowsSink


class RemoteImageStore:
    """Only encoded bytes cross this port; workers cannot publish objects."""

    def __init__(self, journal, store, options):
        from ..operator_llm.media import media_limits
        self.journal, self.store = journal, store
        self.maximum = media_limits(options)['max_image_bytes']

    def store_image(self, raw):
        if type(raw) is not bytes or len(raw) > self.maximum:
            raise ValueError('Image exceeds max_image_bytes')
        ref = self.store.put(iter((raw,)), max_bytes=self.maximum, media_type='application/octet-stream')
        request = {'image_ref': asdict(ref)}
        return self.journal._rpc(self.journal._command('stage_image', request))


def bind_image_sinks(graph, *, services, sinks):
    """Resolve object roots and policy from sealed nodes, never RPC arguments."""
    from .contracts.dataflow import thaw_record_value
    declarations, specs = {}, {}
    for node in graph.nodes:
        config = node.kernel.configuration
        if node.kernel.kind != 'prompt' or config.get('response_format') != 'image':
            continue
        from ..operator_llm.media import media_limits
        options = thaw_record_value(config['options'])
        maximum = media_limits(options)['max_image_bytes']
        root = options['object_store']
        specs[root] = max(specs.get(root, 0), maximum)
        declarations[node.occurrence_id] = options
    if not specs:
        return
    if len(sinks) != 1:
        raise DataflowError('INVALID_CONTRACT', 'An image action must have one native terminal owner')
    key, rows = next(iter(sinks.items()))
    owner = ObjectRowsSink(services.action, object_specs=specs, rows_sink=rows)
    owner.sink_id = key
    sinks[key] = owner
    services.action.object_sinks = owner.object_sinks
    services.action.native_image_effects = True
    services.journal_service._image_declarations = declarations
    # Per-node admission remains the original ObjectSink authority.
    for node in graph.nodes:
        if node.occurrence_id in declarations:
            options = declarations[node.occurrence_id]
            maximum = media_limits(options)['max_image_bytes']
            requests = node.kernel.configuration.get('max_requests')
            if requests is None:
                requests = services.action.context.limits['max_requests']
            owner.object_sinks[options['object_store']].reserve_capacity(requests, maximum, node.occurrence_id)


def stage_image(service, journal, command, request):
    from .seal.plan import _artifact_from_json
    from ..operator_llm.media import store_image, media_limits
    options = getattr(service, '_image_declarations', {}).get(command.node_id)
    if options is None or set(request) != {'image_ref'}:
        raise DataflowError('INVALID_CONTRACT', 'Image command has no sealed image declaration')
    ref = _artifact_from_json(request['image_ref'])
    raw = b''.join(service._store.read(ref, max_bytes=media_limits(options)['max_image_bytes']))
    owner = service._action.object_sinks[options['object_store']]
    result = store_image(raw, options, store=owner)
    return NativeJournalReply(command.operation_id, result, None)


def row_preparation(sink, prepared):
    """The confirmed row artifact is inside the original composite protocol."""
    return prepared['rows'] if isinstance(sink, ObjectRowsSink) else prepared
