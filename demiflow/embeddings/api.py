"""Single-text native embedding API, using the same actor as Dataset.map_embeddings.

Vectors are durable bounded objects, so callers pass an ObjectRef to search
without copying thousands of floating point values through a language model.
"""
from .model import EmbeddingModel, canonical
from .runtime import EmbeddingActor
from ..objects import LocalObjectStore
from ..operator_api import OperatorCallError


def object_schema(properties, required=()):
    return dict(type='object', properties=properties, required=list(required), additionalProperties=False)


REFERENCE = object_schema({'uri': {'type': 'string', 'maxLength': 4096},
                           'sha256': {'type': 'string', 'minLength': 64, 'maxLength': 64}}, ['uri', 'sha256'])
MODEL = object_schema({
    **{key: {'type': 'string'} for key in ('name', 'revision', 'base_url', 'api_key_env', 'input_format', 'image_transport')},
    'dimensions': {'type': 'integer', 'minimum': 1, 'maximum': 65536},
    'normalize': {'type': 'boolean'},
    'request_options': {'type': 'object', 'additionalProperties': True},
    'encoding_parameters': {'type': 'object', 'additionalProperties': True},
}, ['name', 'revision', 'base_url', 'dimensions'])


async def map_embeddings(text, *, model, object_directory, timeout_s=30):
    """Encode one text with one native request; return a verified vector object.

    Deployment is external. This API never starts a service or child Dataset.
    Each call owns/drains its bounded actor pools; the enclosing caller limits
    total calls and concurrency. Persistent storage grows by <=2 MiB per call.
    """
    if not isinstance(text, str) or not text.strip() or len(text) > 4096:
        raise OperatorCallError('invalid_arguments', 'Embedding text requires 1..4096 nonblank characters')
    declaration = EmbeddingModel(**model)
    if declaration.dimensions > 65536:
        raise ValueError('Embedding dimensions exceed 65536')
    actor = EmbeddingActor(model=declaration, inputs={'text': 'text'}, output='vector',
        call_output='call', error_output=None, concurrency=1, label='map_embeddings API',
        options={'timeout_s': timeout_s, 'max_request_bytes': 256 * 1024,
                 'max_response_bytes': 4 * 1024**2, 'io_workers': 1, 'prepare_workers': 1, 'response_workers': 1},
        max_requests=1, service=None, request_gate=None)
    try:
        await actor.astart()
        import httpx
        from .payload import EmbeddingProtocolError
        try:
            row, = await actor([{'text': text}])
        except (httpx.HTTPError, EmbeddingProtocolError) as exc:
            return {'status': 'error', 'code': 'embedding_execution_error', 'detail': str(exc)[:1024]}
        vector = row['vector']
        body = canonical({'format': 'demiflow.embedding.v1', 'encoder_id': declaration.fingerprint,
                          'dimensions': declaration.dimensions, 'vector': vector}).encode()
        if len(body) > 2 * 1024**2:
            raise ValueError('Serialized embedding exceeds 2 MiB')
        ref = await actor._io(LocalObjectStore(object_directory).put, body)
        return {'embedding_ref': ref.to_dict(), 'encoder_id': declaration.fingerprint,
                'dimensions': declaration.dimensions, 'call': row['call']}
    finally:
        await actor.aclose()


DEFINITION = dict(
    name='map_embeddings', function='demiflow.embeddings.api:map_embeddings',
    description='Encode a text query in the configured vector space. Pass the returned embedding_ref unchanged to search_vectors.query_ref; do not invent or transcribe a vector.',
    arguments=object_schema({'text': {'type': 'string', 'minLength': 1, 'maxLength': 4096}}, ['text']),
    fixed_schema=object_schema({'model': MODEL, 'object_directory': {'type': 'string', 'minLength': 1}},
                               ['model', 'object_directory']),
    bindings={'timeout_s': 'limits.timeout_s'}, replay='recorded')
