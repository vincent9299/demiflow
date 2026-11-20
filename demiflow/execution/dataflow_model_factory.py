"""Passive native declarations; live clients/coordinators are worker-owned."""
from dataclasses import replace

from ..data.plan import CallableSpec, OperatorLLMMapOp, EmbeddingMapOp


class PromptFactory:
    def __new__(cls, operation, settings):
        from ..operator_llm.runtime import PromptActor, InProcessOperatorLLMCoordinator
        service = None
        if settings.get('service') is not None:
            from .dataflow_managed_http import restore_worker_managed_http
            service = restore_worker_managed_http(settings['service'])
        actor = PromptActor(operation, operation.config,
                            InProcessOperatorLLMCoordinator(operation.max_requests), operation.options, service=service)
        if service is not None:
            import hashlib
            from .recovery_contract import document
            actor._sealed_service_declaration = hashlib.sha256(document(settings['service'], 65536)).hexdigest()
        for name in ('when', 'call_output', 'error_output', 'environment'):
            setattr(actor, name, settings.get(name))
        actor._runtime.concurrency = settings.get('concurrency', 1)
        actor._runtime.token_budget = settings.get('token_budget')
        policy = settings.get('request_policy')
        if policy is not None:
            actor._request_policy = tuple(sorted(policy.items()))
        return actor


class EmbeddingFactory:
    def __new__(cls, declaration, policy):
        from ..embeddings.runtime import EmbeddingActor
        from ..inference import _resolve_request_gate
        if declaration.get('service') is not None:
            from .dataflow_managed_http import restore_worker_managed_http
            declaration = dict(declaration, service=restore_worker_managed_http(declaration['service']))
        actor = EmbeddingActor(**declaration, request_gate=_resolve_request_gate(
            policy=policy, gate=None, concurrency=declaration['concurrency']))
        actor._request_policy = policy
        return actor


class RoutedPromptFactory:
    def __new__(cls, declarations, limits, route, isolate_failures, max_requests):
        from ..operator_llm.routed import RoutedPromptActor
        return RoutedPromptActor({name: value.instantiate() for name, value in declarations.items()},
            limits, route, isolate_failures=isolate_failures, max_requests=max_requests)


class CacheFactory:
    def __new__(cls, declaration, directory, version, options, synchronous):
        from ..data.result_cache import CachedMap, CachedSyncMap
        from .kernels.task_scope import function_residency_kind, clone_resident_function
        fn = declaration.instantiate()
        actor = (CachedSyncMap if synchronous else CachedMap)(fn, directory, version, **options)
        if actor.validate_runtime_state() is not None:
            return actor  # Retain borrowed/aliased identity during this attempt.
        # A loaded program is a sealed declaration, never the mutable owner of
        # admitted closure/default/partial slots for a running cache instance.
        cloned = {}
        for name in ('fn', 'when', 'cache_when'):
            value = getattr(actor, name)
            if value is not None and function_residency_kind(value) == 'adapter':
                if id(value) not in cloned:
                    cloned[id(value)] = clone_resident_function(value, max_bytes=options['max_input_bytes'])
                setattr(actor, name, cloned[id(value)])
        return actor


def native_model_declaration(operation):
    """Return a factory only after validating the original native declaration.

    No started actor, lock, client or mutable admission gate crosses the wire.
    Native fact guards remain the authority for rejecting modified/live actors.
    """
    if isinstance(operation, OperatorLLMMapOp):
        from ..operator_llm.runtime import validate_prompt_binding
        validate_prompt_binding(operation, operation.config)
        # The execution occurrence supplies the per-node request namespace.
        return CallableSpec.create(PromptFactory, fn_constructor_args=(replace(operation, node_id=None), {}))
    target = getattr(getattr(operation, 'callable', None), 'target', None)
    actor = getattr(target, '__self__', target)
    from ..data.plan import SaveLanceOp
    if type(operation) is SaveLanceOp:
        from .dataflow_stream_lance import StreamLanceFactory, declaration
        return CallableSpec.create(StreamLanceFactory, fn_constructor_args=(declaration(operation, actor), actor.when))
    # None and ordinary functions cannot be one of the exact native actor
    # classes below. Typed model operations retain their original fact guards,
    # including rejection of a forged function in place of their native actor.
    import types
    if not isinstance(operation, EmbeddingMapOp) and (
            actor is None or type(actor) in (types.FunctionType, types.BuiltinFunctionType)):
        return None
    from .stream_lookup import LanceLookup
    from ..lance.search import VectorSearch
    if type(actor) is LanceLookup:
        from ..lance.flow_contract import lookup_facts
        from .contracts.dataflow_errors import DataflowError
        declaration = lookup_facts(actor)['arguments']
        if getattr(target, '__func__', None) is not LanceLookup.__call__:
            raise DataflowError('INVALID_CONTRACT', 'Lookup must bind the native lookup callable')
        if actor.table is not None:
            raise DataflowError('INVALID_CONTRACT', 'Lookup requires an unstarted fixed-version declaration')
        # LRU entries and table handles are reconstructible resources. Reuse the
        # existing CallableSpec wire/code-pin contract; instantiate only in worker.
        return CallableSpec.create(LanceLookup, fn_constructor_kwargs=declaration)
    if type(actor) is VectorSearch:
        from ..data.plan import VectorSearchOp
        from .plan_facts import operation_facts
        from .contracts.dataflow_errors import DataflowError
        if type(operation) is not VectorSearchOp:
            raise DataflowError('INVALID_CONTRACT', 'Vector search requires its typed declaration')
        operation_facts(operation)  # Exact native actor, bound method and spec agree.
        if (actor._dataset is not None or actor._vector_type is not None or actor._version is not None
                or actor._queries or actor._hits):
            raise DataflowError('INVALID_CONTRACT', 'Vector search requires an unstarted declaration')
        return CallableSpec.create(VectorSearch, fn_constructor_kwargs={
            **actor.spec, 'query': actor.query, 'output': actor.output, 'label': actor.label})
    from ..collect.search_contract import is_native_search_actor, search_facts
    if is_native_search_actor(actor):
        from .dataflow_search import NativeSearchCall
        from .recovery_contract import identity
        search_facts(actor)
        if actor.started:
            raise ValueError('Native search requires an unstarted actor declaration')
        return CallableSpec.create(NativeSearchCall, fn_constructor_args=(identity(actor.recovery_identity()),))
    from .queue_producer import QueueProducer, producer_facts
    if type(actor) is QueueProducer:
        from .dataflow_producer import ProducerCall
        producer_facts(actor)
        return CallableSpec.create(ProducerCall)
    from .raw_fetch import RawFetch, raw_fetch_facts
    from .queue_ack import QueueAcknowledge, ack_facts
    if type(actor) in (RawFetch, QueueAcknowledge):
        from .dataflow_acquisition_call import AcquisitionCall
        (raw_fetch_facts if type(actor) is RawFetch else ack_facts)(actor)
        return CallableSpec.create(AcquisitionCall, fn_constructor_args=(
            'acquire' if type(actor) is RawFetch else 'finalize', getattr(actor, 'keep_row', True)))
    from ..data.result_cache import CachedMap, CachedSyncMap
    if type(actor) in (CachedMap, CachedSyncMap):
        from types import SimpleNamespace
        actor.validate_runtime_state()
        wrapped = native_model_declaration(SimpleNamespace(callable=CallableSpec.create(actor.fn)))
        wrapped = wrapped if wrapped is not None else CallableSpec.create(actor.fn)
        options = {name: getattr(actor, name) for name in ('inputs', 'output', 'outputs',
            'when', 'cache_when', 'wait_timeout_s', 'max_input_bytes')}
        options.update(namespace=actor.store.namespace, max_result_bytes=actor.store.max_value_bytes,
                       max_entries=actor.store.max_entries, max_cache_bytes=actor.store.max_bytes)
        return CallableSpec.create(CacheFactory, fn_constructor_args=(wrapped, str(actor.directory),
            actor.version, options, type(actor) is CachedSyncMap))
    from ..operator_llm.routed import RoutedPromptActor
    if type(actor) is RoutedPromptActor:
        from .dataflow_routed_prompt import routed_declaration
        return routed_declaration(actor, target)
    from ..operator_llm.runtime import PromptActor
    if type(actor) is PromptActor:
        from ..operator_llm.flow_contract import prompt_facts, agent_facts
        if actor.environment is not None:
            agent_facts(actor, getattr(operation, 'environment', actor.environment))
        else:
            prompt_facts(actor)
        settings = {name: getattr(actor, name) for name in ('when', 'call_output', 'error_output', 'environment')}
        settings.update(concurrency=actor._runtime.concurrency,
                        token_budget=actor._runtime.token_budget,
                        request_policy=dict(actor._request_policy) if hasattr(actor, '_request_policy') else None)
        from .dataflow_prompt_service import prompt_service_declaration
        service = prompt_service_declaration(actor)
        if service is not None:
            settings['service'] = service
        return CallableSpec.create(PromptFactory, fn_constructor_args=(replace(actor._operation, node_id=None), settings))
    if isinstance(operation, EmbeddingMapOp):
        from ..embeddings.flow_contract import embedding_facts
        from .contracts.dataflow_errors import DataflowError
        embedding_facts(operation)
        if actor._started or any(getattr(actor, name, None) is not None for name in (
                '_pool', '_prepare_pool', '_response_pool', '_client', '_owner',
                '_journal', '_document_reader', '_native_recovery')):
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Embedding factory requires an unstarted passive declaration')
        service = None
        if actor.service is not None:
            from .dataflow_managed_http import freeze_managed_http, managed_http_digest
            if getattr(actor, '_sealed_service_declaration', None) != managed_http_digest(actor.service):
                raise DataflowError('INVALID_CONTRACT', 'Managed embedding service differs from its sealed declaration')
            service = freeze_managed_http(actor.service)
        elif hasattr(actor, '_sealed_service_declaration'):
            raise DataflowError('INVALID_CONTRACT', 'Managed embedding service was removed after declaration')
        policy = getattr(actor, '_request_policy', None)
        # Caller-owned gates have live state and cannot be silently discarded.
        from ..inference import _resolve_request_gate
        expected = _resolve_request_gate(policy=policy, gate=None, concurrency=actor.concurrency)
        if expected is None:
            from .request_limits import RequestGate
            expected = RequestGate(actor.concurrency)
        if type(expected) is not type(actor.request_gate) or getattr(expected, 'policy', None) != getattr(actor.request_gate, 'policy', None):
            raise DataflowError('INVALID_CONTRACT', 'Embedding request gate differs from its declared policy')
        declaration = {name: getattr(actor, name) for name in (
            'model', 'inputs', 'output', 'call_output', 'error_output', 'concurrency',
            'label', 'options', 'max_requests', 'service')}
        declaration['service'] = service
        return CallableSpec.create(EmbeddingFactory, fn_constructor_args=(declaration, policy))
    return None
