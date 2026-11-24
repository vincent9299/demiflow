"""Passive declaration of the actual embedding actor, excluding live resources."""
import json

from ..data.runtime_contract import RuntimeContractError


def actor_declaration(actor):
    # Endpoint and credential *reference* bind execution, never credential values.
    # Model.contract is the vector-space identity; it is not the whole program.
    return dict(model_contract=actor.model.contract(), inputs=dict(actor.inputs),
                output=actor.output, call_output=actor.call_output,
                error_output=actor.error_output, options=dict(actor.options),
                max_requests=actor.max_requests, concurrency=actor.concurrency,
                endpoint=actor.model.base_url, api_key_env=actor.model.api_key_env,
                request_policy=getattr(actor, '_request_policy', None))


def freeze_declaration(actor):
    actor._sealed_declaration = json.dumps(actor_declaration(actor), sort_keys=True,
                                          separators=(',', ':'), allow_nan=False)
    if actor.service is not None:
        # Deployment identity is separate from the historical vector/request
        # identity. Default service=None facts stay byte-for-byte unchanged.
        from demiflow.services.http import managed_http_digest
        actor._sealed_service_declaration = managed_http_digest(actor.service)


def embedding_facts(operation):
    from .runtime import EmbeddingActor
    target = operation.callable.target
    actor = getattr(target, '__self__', None)
    if type(actor) is not EmbeddingActor or getattr(target, '__func__', None) is not EmbeddingActor.__call__:
        raise RuntimeContractError('Embedding specialization must bind the native EmbeddingActor')
    actual = actor_declaration(actor)
    if (operation.model_contract != actual['model_contract'] or
            operation.inputs != actual['inputs'] or operation.output != actual['output'] or
            getattr(actor, '_sealed_declaration', None) != json.dumps(
                actual, sort_keys=True, separators=(',', ':'), allow_nan=False)):
        raise RuntimeContractError('Embedding actor differs from its declared plan')
    return {'contract': 'demiflow.embeddings.v2', 'arguments': actual}


def embedding_constructor_parameters(operation, actor):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    embedding_facts(operation)
    if actor._started or any(getattr(actor, name, None) is not None for name in (
            '_pool', '_prepare_pool', '_response_pool', '_client', '_owner',
            '_journal', '_document_reader', '_native_recovery')):
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Embedding factory requires an unstarted passive declaration')
    service = None
    if actor.service is not None:
        from demiflow.services.http import freeze_managed_http, managed_http_digest
        if getattr(actor, '_sealed_service_declaration', None) != managed_http_digest(actor.service):
            raise DataflowError('INVALID_CONTRACT', 'Managed embedding service differs from its sealed declaration')
        service = freeze_managed_http(actor.service)
    elif hasattr(actor, '_sealed_service_declaration'):
        raise DataflowError('INVALID_CONTRACT', 'Managed embedding service was removed after declaration')
    policy = getattr(actor, '_request_policy', None)
    # Caller-owned gates have live state and cannot be silently discarded.
    from demiflow.inference import resolve_request_gate as _resolve_request_gate
    expected = _resolve_request_gate(policy=policy, gate=None, concurrency=actor.concurrency)
    if expected is None:
        from demiflow.services.request_limits import RequestGate
        expected = RequestGate(actor.concurrency)
    if type(expected) is not type(actor.request_gate) or getattr(expected, 'policy', None) != getattr(actor.request_gate, 'policy', None):
        raise DataflowError('INVALID_CONTRACT', 'Embedding request gate differs from its declared policy')
    declaration = {name: getattr(actor, name) for name in (
        'model', 'inputs', 'output', 'call_output', 'error_output', 'concurrency',
        'label', 'options', 'max_requests', 'service')}
    declaration['service'] = service
    return declaration, policy
