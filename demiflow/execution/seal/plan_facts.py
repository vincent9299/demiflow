"""Side-effect-free facts extracted from actual native plan objects.

No expected report, candidate callback, property or repr participates in these
descriptions. Unknown operation subclasses do not inherit checked authority.
"""
from collections.abc import Mapping
from dataclasses import fields
import inspect

from demiflow.data import plan as ops
from demiflow.data.runtime_contract import RuntimeContractError


def execution_instances(nodes, edges, actions, *, maximum=1024, value_edges=()):
    """Bounded preorder occurrences, retaining repeated inputs and action reuse."""
    by_id = {node['id']: node for node in nodes}
    parents = {identity: [] for identity in by_id}
    for edge in sorted(edges, key=lambda item: item['port']):
        parents[edge['to']].append(edge['from'])
    for edge in value_edges:
        if by_id[edge['from']].get('kind') == 'fit':
            parents[edge['to']].append(edge['from'])
    instances = []
    for action in actions:
        counts, pending = {}, [(action, 0)]
        while pending:
            identity, depth = pending.pop()
            if depth > 128 or len(instances) >= maximum:
                raise ValueError('Execution observation reservation exceeds depth/count/byte budget')
            occurrence = counts.get(identity, 0)
            if by_id[identity].get('kind') == 'fit' and occurrence:
                continue
            counts[identity] = occurrence + 1
            reuse = identity != action and by_id[identity]['role'] == 'bridge'
            instances.append({'action_id': action, 'node_id': identity, 'occurrence': occurrence,
                              'mode': 'reuse' if reuse else 'execute'})
            if not reuse:
                pending.extend((child, depth + 1) for child in reversed(parents[identity]))
    return instances


def source_facts(source):
    from demiflow.data.sources import DatasourceSource, KeyedSource, LanceSource, MaterializedSource, UnionSource
    from demiflow.lance.model import LanceScanSpec

    from demiflow.execution.native_queue.queue_source import QueueDatasource
    from demiflow.execution.sources.queue_history import QueueHistoryDatasource
    if type(source) is DatasourceSource and type(source.datasource) is QueueHistoryDatasource:
        if source.native_options is not None:raise RuntimeContractError('History has no alternate execution backend')
        from demiflow.execution.queue_flow import resource
        return {'kind':'read_queue_records','arguments':source.datasource.declaration(),'inputs':0,
                'effects':['queue_read','objects_read'], 'resources':[resource(source.datasource.config,'read')]}
    if type(source) is DatasourceSource and type(source.datasource) is QueueDatasource:
        if source.native_options is not None:
            raise RuntimeContractError('Queue source has no alternate execution backend')
        from demiflow.execution.queue_flow import resource
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
    if type(source) is KeyedSource and source.kind == 'groups':
        return {'kind':'group_batches','arguments':dict(on=list(source.on),max_rows=source.max_rows,
                output=source.output,chunk_bytes=source.chunk_bytes),'inputs':1}
    if type(source) is KeyedSource and source.kind == 'reduce':
        return {'kind': 'reduce', 'arguments': {'on': list(source.on), 'initial': source.initial,
                'chunk_bytes': source.chunk_bytes, 'fn_kwargs': dict(source.reducer_kwargs or {})}, 'inputs': 1}
    if type(source) is UnionSource:
        return {'kind': 'union', 'arguments': {}, 'inputs': len(source.inputs)}
    if type(source) is MaterializedSource:
        return {'kind': 'materialize', 'arguments': {}, 'inputs': 0}
    raise RuntimeContractError('Unknown actual source contract: ' + type(source).__name__)


def operation_capability(operation):
    from demiflow.data.dataflow_operation import NativeOperation
    if isinstance(operation, NativeOperation):
        from demiflow.planning.dataflow_registry import operator_registry
        definition = operator_registry().get(operation.kind)
        kind = operation.kind if definition is not None else "unknown"
        return {"kind": kind, "driver": "stream", "native_options": False,
                "stream_options": False, "bound": False, "native": True}
    kinds = {
        ops.MapOp: "map", ops.BoundMapOp: "map", ops.FilterOp: "filter",
        ops.FlatMapOp: "flat_map", ops.MapBatchesOp: "map_batches", ops.PredictOp: "predict",
        ops.AsyncMapOp: "async_map", ops.BatchMapOp: "batch_map",
        ops.LimitOp: "limit", ops.SelectColumnsOp: "project", ops.DropColumnsOp: "drop",
        ops.RenameColumnsOp: "rename", ops.AddColumnOp: "add_column",
        ops.RandomSampleOp: "sample", ops.OperatorLLMMapOp: "model_map",
        ops.SortOp: "sort", ops.RepartitionOp: "repartition",
        ops.RandomShuffleOp: "shuffle", ops.RandomizeBlockOrderOp: "block_order",
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
    if type(operation) is ops.PredictOp:
        native = operation.fit.recipe.predictor.native_options
    stream = operation.stream_options if type(operation) in {ops.MapOp, ops.BoundMapOp} else None
    fact = {"kind": kind, "driver": "stream" if kind in {"async_map", "batch_map"} or stream is not None else "pull",
            "native_options": native is not None, "stream_options": stream is not None,
            "bound": type(operation) is ops.BoundMapOp}
    if native is not None:
        from demiflow.data.native_options import resource_only_options
        try:
            fact['resource_declaration'] = resource_only_options(native,
                family='batch_transform' if kind in {'map_batches', 'batch_map','predict'} else 'row_transform')
        except (ValueError, TypeError):
            # Retain the original unsupported-fact/error path for legacy
            # compute/ranges and unknown options. No declaration means no grant.
            pass
    return fact


def _catch_facts(catch):
    # Empty/None keep their exact prior JSON form ([] / null) so sealed dag_id never drifts.
    # Not collision-free: a function-local exception class carries "<locals>" in
    # __qualname__, so distinct classes from repeated definitions canonicalize alike.
    # This buys determinism, not a uniqueness guarantee (accepted edge case).
    if catch is None:
        return None
    return sorted(exc.__module__ + "." + exc.__qualname__ for exc in catch)


def _thaw_json(value):
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(item) for item in value]
    return value


def _native_operation_facts(operation):
    """Lazy v3 native declaration projection; never instantiates the factory."""
    from demiflow.planning.dataflow_registry import operator_registry
    from demiflow.execution.seal.program_identity import describe
    definition = operator_registry().get(operation.kind)
    if definition is None:
        raise RuntimeContractError("Unknown native operation kind: " + operation.kind)
    return {"kind": operation.kind, "driver": "stream", "native_options": False,
            "stream_options": False, "bound": False, "native": True,
            "arguments": {"configuration": _thaw_json(operation.configuration),
                          "bindings": _thaw_json(operation.bindings)},
            "code_identity": {"factory": describe(operation.factory)}}


def operation_facts(operation):
    """Read complete supported operation parameters independently of a report."""
    from demiflow.data.dataflow_operation import NativeOperation
    if isinstance(operation, NativeOperation):
        return _native_operation_facts(operation)
    fact = operation_capability(operation)
    kind, arguments = fact["kind"], {}
    # operation_capability is the single admission authority: a kind it refuses
    # cannot be described here, and a kind it grants must be described here.
    if kind == "unknown":
        raise RuntimeContractError("Unknown actual operation contract: " + type(operation).__name__)
    if fact['native_options'] and 'resource_declaration' not in fact:
        raise RuntimeContractError("Backend option facts require a supported native contract")
    if kind == "map":
        arguments.update(inputs=None, output=None, outputs=None)
        if fact["bound"]:
            arguments.update(inputs=dict(operation.inputs), output=operation.output,
                             outputs=dict(operation.outputs) if operation.outputs is not None else None)
        if operation.stream_options is not None:
            arguments.update({f.name: getattr(operation.stream_options, f.name) for f in fields(ops.MapStreamOptions)})
            arguments["catch"] = _catch_facts(arguments["catch"])
            arguments["queue_depth"] = arguments["queue_depth"] or arguments["concurrency"]
    elif kind in {"async_map", "batch_map"}:
        names = ("concurrency", "queue_depth", "catch", "label", "hard_timeout")
        arguments.update({name: getattr(operation, name) for name in names})
        arguments["catch"] = _catch_facts(arguments["catch"])
        arguments["queue_depth"] = arguments["queue_depth"] or arguments["concurrency"]
        if kind == "async_map":
            arguments["execution"] = operation.execution
        else:
            arguments.update(max_batch=operation.max_batch, flush_interval=operation.flush_interval)
    elif kind == "limit":
        arguments["limit"] = operation.limit
    elif kind in {"project", "drop"}:
        arguments["cols"] = list(operation.columns)
    elif kind == "rename":
        arguments["names"] = operation.names
    elif kind == "predict":
        arguments.update(steps=list(operation.steps), inputs=_thaw_json(operation.inputs),
                         parameters=_thaw_json(operation.parameters), model_format=operation.fit.recipe.model_format)
    elif kind == "map_batches":
        arguments.update(batch_size=operation.batch_size, batch_format=operation.batch_format,
                         zero_copy_batch=operation.zero_copy_batch)
    elif kind == "add_column":
        arguments.update(column=operation.column, batch_format=operation.batch_format)
    elif kind == "sample":
        arguments.update(fraction=operation.fraction, seed=operation.seed)
    elif kind == "sort":
        descending = operation.descending
        if type(descending) not in (bool, tuple):
            raise RuntimeContractError("Sort directions must be a bool or tuple of bools")
        directions = ([descending] * len(operation.keys) if type(descending) is bool
                      else list(descending))
        if len(directions) != len(operation.keys) or any(type(v) is not bool for v in directions):
            raise RuntimeContractError("Sort directions must match the declared keys")
        arguments.update(key=list(operation.keys), descending=directions,
                         boundaries=list(operation.boundaries) if operation.boundaries is not None else None)
    elif kind == "repartition":
        arguments.update(num_blocks=operation.num_blocks,
                         target_num_rows_per_block=operation.target_num_rows_per_block,
                         shuffle=operation.shuffle,
                         keys=list(operation.keys) if operation.keys is not None else None,
                         sort=operation.sort)
    elif kind == "shuffle":
        arguments.update(seed=operation.seed, num_blocks=operation.num_blocks)
    elif kind == "block_order":
        arguments.update(seed=operation.seed)
    elif kind == "model_map":
        from demiflow.operator_llm.flow_contract import model_map_options
        arguments.update(prompt_name=operation.prompt_name, inputs=dict(operation.inputs),
                         output=operation.output,
                         outputs=dict(operation.outputs) if operation.outputs is not None else None,
                         options=model_map_options(operation.options),
                         max_requests=operation.max_requests,
                         config={"schema_version": str(operation.config.schema_version),
                                 "content_hash": operation.config.content_hash})
    domain = {}
    if type(operation) is ops.SaveLanceOp:
        from demiflow.execution.native_queue.stream_lance import StreamLanceSink
        from demiflow.lance.flow_contract import save_facts
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
        from demiflow.collect.operators import SearchWeb
        from demiflow.collect.search_contract import search_facts
        target=getattr(operation.callable.target,'__self__',operation.callable.target)
        if type(target) is not SearchWeb or operation.requests!=target.requests or operation.output!=target.output:
            raise RuntimeContractError('Search declaration differs from actual native actor')
        if operation.checkpoint is not None or operation.checkpoint_operator!='search_web' or operation.checkpoint_lease_s!=300.0:
            raise RuntimeContractError('Explicit search checkpoint orchestration is retired')
        domain=search_facts(target)
    if type(operation) is ops.AsyncMapOp:
        from demiflow.operators.lookup import LanceLookup
        from demiflow.lance.flow_contract import lookup_facts
        from demiflow.operator_llm.native_actor import is_native_prompt_actor
        target = operation.callable.target
        target = target.__self__ if inspect.ismethod(target) else target
        from demiflow.execution.native_queue.raw_fetch import RawFetch, raw_fetch_facts
        from demiflow.execution.native_queue.queue_producer import QueueProducer, producer_facts
        from demiflow.execution.native_queue.queue_ack import QueueAcknowledge, ack_facts
        if type(target) is QueueAcknowledge:
            domain=ack_facts(target)
        elif type(target) is QueueProducer:
            domain = producer_facts(target)
        elif type(target) is RawFetch:
            domain = raw_fetch_facts(target)
        elif type(target) is LanceLookup:
            domain = {'native': lookup_facts(target), 'reserved_bytes': 8 * 1024**2 + 2 * target.max_bytes}
        elif is_native_prompt_actor(target):
            from demiflow.operator_llm.flow_contract import prompt_facts
            native = prompt_facts(target)
            options = native['options'] or {}
            token_bytes = 0
            if native['token_budget'] and native['token_budget']['unit'] == 'tokens':
                token_bytes = 64 * target._runtime.token_budget.counter.encoding_bytes + 64 * 1024**2
            domain = {'native': native, 'reserved_bytes': operation.concurrency * 8 *
                      (options.get('max_request_bytes', 0) + options.get('max_response_bytes', 0)) + token_bytes}
            if native['prompt']['response_format']=='image':
                from demiflow.operator_llm.media import reserved_media_bytes
                domain['reserved_bytes']=reserved_media_bytes(options,operation.concurrency)
        else:
            from demiflow.objects import ReadLocalObject
            from demiflow.operator_llm.packing import PackPromptInputs, packing_facts
            from demiflow.collect.documents import DecodeDocument
            from demiflow.collect.image_resize import EncodeImage
            from demiflow.collect.wiki_syntax import ParseWikitext
            from demiflow.collect.flow_contract import collect_facts
            if type(target) in {ReadLocalObject, DecodeDocument, EncodeImage, ParseWikitext}:
                domain = collect_facts(target)
            elif type(target) is PackPromptInputs:
                domain = packing_facts(target)
    specialization = _specialization_domain(operation)
    if specialization is not None:
        domain = {"native": specialization}
    return {**fact, "arguments": arguments, **domain}


# Business-semantic prompt options projected into permanent model_map facts.
# Credentials (request_options.api_key/extra_headers) and runtime environment
# paths (journal/log/offline/codex/object_store) never enter the sealed identity.









def _specialization_domain(operation):
    """Bounded native facts for the in-module stream specializations.

    Without these, recovery_identity and chain fall back to describe(), whose
    32-level budget cannot carry a specialization's nested declarations.
    """
    if type(operation) is ops.VectorSearchOp:
        from demiflow.lance.flow_contract import vector_search_facts
        return vector_search_facts(operation)
    if type(operation) is ops.StreamGroupBatchesOp:
        from demiflow.operators.grouping import group_batches_facts
        return group_batches_facts(operation)
    if type(operation) is ops.DeduplicateOp:
        from demiflow.operators.admission import StreamAdmission
        from demiflow.execution.contracts.recovery_contract import identity
        target = operation.callable.target
        actor = getattr(target, '__self__', None)
        if type(actor) is not StreamAdmission or getattr(target, '__func__', None) is not StreamAdmission.__call__:
            raise RuntimeContractError('Deduplicate must bind the native admission actor')
        arguments = _deduplicate_arguments(actor)
        if (tuple(actor.unique_on) != tuple(operation.on) or actor.key != operation.key
                or actor.output != operation.output
                or getattr(actor, '_deduplicate_declaration', None) != identity(arguments)):
            raise RuntimeContractError('Deduplicate actor differs from its declared plan')
        return {'contract': 'demiflow.deduplicate', 'arguments': arguments}
    if type(operation) is ops.EmbeddingMapOp:
        from demiflow.embeddings.flow_contract import embedding_facts
        return embedding_facts(operation)
    if type(operation) is ops.StreamPrependOp:
        from demiflow.execution.seal.program_identity import plan_identity
        other = operation.source_dataset
        return {'contract': 'demiflow.stream_prepend',
                'arguments': dict(max_rows=operation.max_rows, max_row_bytes=operation.max_row_bytes,
                                  source=plan_identity('input', other._source, other._plan))}
    return None


def _deduplicate_arguments(actor):
    from demiflow.execution.seal.program_identity import describe
    from demiflow.operators.admission import validate_deduplicate_policy
    validate_deduplicate_policy(actor)
    return dict(on=list(actor.unique_on), key=actor.key, output=actor.output,
                when=describe(actor.when), quotas=describe(actor.quotas),
                **{name: getattr(actor, name) for name in ('max_entries', 'max_key_bytes', 'max_disk_bytes')})
