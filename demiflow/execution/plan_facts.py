"""Side-effect-free facts extracted from actual native plan objects.

No expected report, candidate callback, property or repr participates in these
descriptions. Unknown operation subclasses do not inherit checked authority.
"""
from dataclasses import fields
import inspect

from ..data import plan as ops
from ..data.runtime_contract import RuntimeContractError


def execution_instances(nodes, edges, actions, *, maximum=1024):
    """Bounded preorder occurrences, retaining repeated inputs and action reuse."""
    by_id = {node['id']: node for node in nodes}
    parents = {identity: [] for identity in by_id}
    for edge in sorted(edges, key=lambda item: item['port']):
        parents[edge['to']].append(edge['from'])
    instances = []
    for action in actions:
        counts, pending = {}, [(action, 0)]
        while pending:
            identity, depth = pending.pop()
            if depth > 128 or len(instances) >= maximum:
                raise ValueError('Execution observation reservation exceeds depth/count/byte budget')
            occurrence = counts.get(identity, 0)
            counts[identity] = occurrence + 1
            reuse = identity != action and by_id[identity]['role'] == 'bridge'
            instances.append({'action_id': action, 'node_id': identity, 'occurrence': occurrence,
                              'mode': 'reuse' if reuse else 'execute'})
            if not reuse:
                pending.extend((child, depth + 1) for child in reversed(parents[identity]))
    return instances


def source_facts(source):
    from ..data.sources import LanceSource, MaterializedSource
    from ..lance.model import LanceScanSpec
    from .local_kernel import KeyedSource, UnionSource

    from ..data.sources import DatasourceSource
    from .queue_source import QueueDatasource
    from .queue_history import QueueHistoryDatasource
    if type(source) is DatasourceSource and type(source.datasource) is QueueHistoryDatasource:
        if source.native_options is not None:raise RuntimeContractError('History has no alternate execution backend')
        from .queue_flow import resource
        return {'kind':'read_queue_records','arguments':source.datasource.declaration(),'inputs':0,
                'effects':['queue_read','objects_read'], 'resources':[resource(source.datasource.config,'read')]}
    if type(source) is DatasourceSource and type(source.datasource) is QueueDatasource:
        if source.native_options is not None:
            raise RuntimeContractError('Queue source has no alternate execution backend')
        from .queue_flow import resource
        return {'kind':'read_queue','arguments':source.datasource.declaration(),'inputs':0,
                'effects':['queue'], 'resources':[resource(source.datasource.config,'claim')]}
    if type(source) is LanceSource:
        query = source.query
        if type(query) is not LanceScanSpec or source.native_options is not None:
            raise RuntimeContractError('Unknown native scan contract')
        if query.storage_options:
            raise RuntimeContractError('External storage options have no managed binding')
        names = ('uri', 'version', 'filter', 'limit', 'batch_size', 'batch_readahead', 'fragment_readahead')
        arguments = {name: getattr(query, name) for name in names}
        arguments.update(columns=list(query.columns), projection=dict(query.projection))
        return {'kind': 'read_table', 'arguments': arguments, 'inputs': 0}
    if type(source) is KeyedSource and source.kind == 'join':
        arguments = {name: getattr(source, name) for name in ('how', 'suffix', 'chunk_bytes')}
        arguments.update(on=list(source.on), right_on=list(source.right_on))
        return {'kind': 'join', 'arguments': arguments, 'inputs': 2}
    if type(source) is KeyedSource and source.kind == 'reduce':
        return {'kind': 'reduce', 'arguments': {'on': list(source.on), 'initial': source.initial,
                'chunk_bytes': source.chunk_bytes, 'fn_kwargs': dict(source.reducer_kwargs or {})}, 'inputs': 1}
    if type(source) is UnionSource:
        return {'kind': 'union', 'arguments': {}, 'inputs': len(source.inputs)}
    if type(source) is MaterializedSource:
        return {'kind': 'materialize', 'arguments': {}, 'inputs': 0}
    raise RuntimeContractError('Unknown actual source contract: ' + type(source).__name__)


def operation_capability(operation):
    kinds = {
        ops.MapOp: "map", ops.BoundMapOp: "map", ops.FilterOp: "filter",
        ops.FlatMapOp: "flat_map", ops.MapBatchesOp: "map_batches",
        ops.AsyncMapOp: "async_map", ops.BatchMapOp: "batch_map",
        ops.LimitOp: "limit", ops.SelectColumnsOp: "project", ops.DropColumnsOp: "drop",
        ops.RenameColumnsOp: "rename", ops.AddColumnOp: "add_column",
        ops.RandomSampleOp: "sample", ops.OperatorLLMMapOp: "model_map",
    }
    kind = kinds.get(type(operation))
    # Native specializations have their own domain facts, while sharing the
    # same actual stream lowering. No arbitrary third-party subclass qualifies.
    if kind is None and type(operation).__module__ == ops.__name__:
        if isinstance(operation, ops.AsyncMapOp):
            kind = "async_map"
        elif isinstance(operation, ops.BatchMapOp):
            kind = "batch_map"
    if kind is None:
        kind = "unknown"
    native = getattr(operation, "native_options", None) if type(operation) in kinds else None
    stream = operation.stream_options if type(operation) in {ops.MapOp, ops.BoundMapOp} else None
    return {"kind": kind, "driver": "stream" if kind in {"async_map", "batch_map"} or stream is not None else "pull",
            "native_options": native is not None, "stream_options": stream is not None,
            "bound": type(operation) is ops.BoundMapOp}


def operation_facts(operation):
    """Read complete supported operation parameters independently of a report."""
    fact = operation_capability(operation)
    kind, arguments = fact["kind"], {}
    supported = {ops.MapOp, ops.BoundMapOp, ops.FilterOp, ops.FlatMapOp,
                 ops.AsyncMapOp, ops.SearchWebOp, ops.BatchMapOp, ops.SaveLanceOp, ops.LimitOp, ops.SelectColumnsOp,
                 ops.DropColumnsOp, ops.RenameColumnsOp}
    if type(operation) not in supported:
        raise RuntimeContractError("Unknown actual operation contract: " + type(operation).__name__)
    if fact["native_options"]:
        raise RuntimeContractError("Backend option facts require a supported native contract")
    if type(operation) in {ops.MapOp, ops.BoundMapOp}:
        arguments.update(inputs=None, output=None, outputs=None)
        if type(operation) is ops.BoundMapOp:
            arguments.update(inputs=dict(operation.inputs), output=operation.output,
                             outputs=dict(operation.outputs) if operation.outputs is not None else None)
        if operation.stream_options is not None:
            arguments.update({f.name: getattr(operation.stream_options, f.name) for f in fields(ops.MapStreamOptions)})
            arguments["queue_depth"] = arguments["queue_depth"] or arguments["concurrency"]
    elif type(operation) in {ops.AsyncMapOp, ops.SearchWebOp, ops.BatchMapOp, ops.SaveLanceOp}:
        names = ("concurrency", "queue_depth", "catch", "label", "hard_timeout")
        arguments.update({name: getattr(operation, name) for name in names})
        arguments["queue_depth"] = arguments["queue_depth"] or arguments["concurrency"]
        if type(operation) in {ops.AsyncMapOp,ops.SearchWebOp}:
            arguments["execution"] = operation.execution
        else:
            arguments.update(max_batch=operation.max_batch, flush_interval=operation.flush_interval)
    elif type(operation) is ops.LimitOp:
        arguments["limit"] = operation.limit
    elif type(operation) in {ops.SelectColumnsOp, ops.DropColumnsOp}:
        arguments["cols"] = list(operation.columns)
    elif type(operation) is ops.RenameColumnsOp:
        arguments["names"] = operation.names
    domain = {}
    if type(operation) is ops.SaveLanceOp:
        from .stream_lance import StreamLanceSink
        from ..lance.flow_contract import save_facts
        target = operation.callable.target
        target = target.__self__ if inspect.ismethod(target) else target
        if type(target) is not StreamLanceSink:
            raise RuntimeContractError('Save node must use its native streaming sink')
        native = save_facts(target, operation)
        for name in ('uri', 'stage', 'output_ref', 'mode'):
            if getattr(operation, name) != getattr(target, name):
                raise RuntimeContractError('Stream sink declaration differs from actual actor')
        actual_key = [operation.key] if isinstance(operation.key, str) else list(operation.key)
        if actual_key != native['arguments']['key']:
            raise RuntimeContractError('Stream sink key differs from actual actor')
        domain = {'native': native, 'reserved_bytes': 8 * 1024**2 + operation.max_batch * target.max_row_bytes * 3}
    if type(operation) is ops.SearchWebOp:
        from ..collect.operators import SearchWeb
        from ..collect.search_contract import search_facts
        target=getattr(operation.callable.target,'__self__',operation.callable.target)
        if type(target) is not SearchWeb or operation.requests!=target.requests or operation.output!=target.output:
            raise RuntimeContractError('Search declaration differs from actual native actor')
        if operation.checkpoint is not None or operation.checkpoint_operator!='search_web' or operation.checkpoint_lease_s!=300.0:
            raise RuntimeContractError('Explicit search checkpoint orchestration is retired')
        domain=search_facts(target)
    if type(operation) is ops.AsyncMapOp:
        from .stream_lookup import LanceLookup
        from ..lance.flow_contract import lookup_facts
        from ..operator_llm.runtime import PromptActor
        from ..operator_llm.flow_contract import prompt_facts
        target = operation.callable.target
        target = target.__self__ if inspect.ismethod(target) else target
        from .raw_fetch import RawFetch, raw_fetch_facts
        from .queue_producer import QueueProducer, producer_facts
        from .queue_ack import QueueAcknowledge, ack_facts
        if type(target) is QueueAcknowledge:
            domain=ack_facts(target)
        elif type(target) is QueueProducer:
            domain = producer_facts(target)
        elif type(target) is RawFetch:
            domain = raw_fetch_facts(target)
        elif type(target) is LanceLookup:
            domain = {'native': lookup_facts(target), 'reserved_bytes': 8 * 1024**2 + 2 * target.max_bytes}
        elif type(target) is PromptActor:
            native = prompt_facts(target)
            options = native['options'] or {}
            token_bytes = 0
            if native['token_budget'] and native['token_budget']['unit'] == 'tokens':
                token_bytes = 64 * target._runtime.token_budget.counter.encoding_bytes + 64 * 1024**2
            domain = {'native': native, 'reserved_bytes': operation.concurrency * 8 *
                      (options.get('max_request_bytes', 0) + options.get('max_response_bytes', 0)) + token_bytes}
            if native['prompt']['response_format']=='image':
                from ..operator_llm.media import reserved_media_bytes
                domain['reserved_bytes']=reserved_media_bytes(options,operation.concurrency)
        else:
            from ..objects import ReadLocalObject
            from ..operator_llm.packing import PackPromptInputs, packing_facts
            from ..collect.documents import DecodeDocument
            from ..collect.image_resize import EncodeImage
            from ..collect.wiki_syntax import ParseWikitext
            from ..collect.flow_contract import collect_facts
            if type(target) in {ReadLocalObject, DecodeDocument, EncodeImage, ParseWikitext}:
                domain = collect_facts(target)
            elif type(target) is PackPromptInputs:
                domain = packing_facts(target)
    return {**fact, "arguments": arguments, **domain}
