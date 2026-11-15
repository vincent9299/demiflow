"""Passive native declarations; live clients/coordinators are worker-owned."""
from dataclasses import replace

from ..data.plan import CallableSpec, OperatorLLMMapOp, EmbeddingMapOp


class PromptFactory:
    def __new__(cls, operation, settings):
        from ..operator_llm.runtime import PromptActor, InProcessOperatorLLMCoordinator
        actor = PromptActor(operation, operation.config,
                            InProcessOperatorLLMCoordinator(operation.max_requests), operation.options)
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
        actor = EmbeddingActor(**declaration, request_gate=_resolve_request_gate(
            policy=policy, gate=None, concurrency=declaration['concurrency']))
        actor._request_policy = policy
        return actor


class CacheFactory:
    def __new__(cls, declaration, directory, version, options, synchronous):
        from ..data.result_cache import CachedMap, CachedSyncMap
        fn = declaration.instantiate()
        return (CachedSyncMap if synchronous else CachedMap)(fn, directory, version, **options)


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
    from ..collect.operators import SearchWeb
    if type(actor) is SearchWeb and actor.queue_producer is not None:
        from .dataflow_producer import ProducerSearchCall
        return CallableSpec.create(ProducerSearchCall, fn_constructor_args=(actor.queue_node_id,))
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
        return CallableSpec.create(PromptFactory, fn_constructor_args=(replace(actor._operation, node_id=None), settings))
    if isinstance(operation, EmbeddingMapOp):
        from ..embeddings.flow_contract import embedding_facts
        from .contracts.dataflow_errors import DataflowError
        embedding_facts(operation)
        if actor._started or actor.service is not None:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Embedding factory requires an unstarted external-service declaration')
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
        return CallableSpec.create(EmbeddingFactory, fn_constructor_args=(declaration, policy))
    return None
