"""Passive native declarations; live clients/coordinators are worker-owned."""
from dataclasses import replace

from ..data.plan import CallableSpec, OperatorLLMMapOp, EmbeddingMapOp


class PromptFactory:
    def __new__(cls, operation, settings):
        from ..operator_llm.runtime import PromptActor, InProcessOperatorLLMCoordinator
        actor = PromptActor(operation, operation.config,
                            InProcessOperatorLLMCoordinator(operation.max_requests), operation.options)
        for name in ('when', 'call_output', 'error_output'):
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
    from ..operator_llm.runtime import PromptActor
    if type(actor) is PromptActor:
        from ..operator_llm.flow_contract import prompt_facts
        prompt_facts(actor)
        settings = {name: getattr(actor, name) for name in ('when', 'call_output', 'error_output')}
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
