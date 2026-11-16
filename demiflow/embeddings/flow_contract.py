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
        from ..execution.dataflow_managed_http import managed_http_digest
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
