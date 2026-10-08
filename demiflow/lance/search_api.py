"""Native single-query API over VectorSearch; fixed snapshots and encoder identity."""
import hashlib
import json

from .search import VectorSearch
from ..embeddings.api import REFERENCE, object_schema
from ..embeddings.model import canonical
from ..objects import LocalObjectStore, ObjectRef
from ..operator_api import OperatorCallError


def search_vectors(query_ref, top_k=4, *, uri, version, vector_column, columns,
                   encoder_id, object_directory, contract_metadata_key,
                   metric='cosine', max_top_k=8, options=None):
    """Read one bounded vector object and execute the native fixed-version search.

    No raw-text reinterpretation or agent-only aliases. Vector objects must be
    within the configured content-addressed store and use the table's declared
    vector space. Results are the native projected rows plus _distance.
    """
    try:
        ref = ObjectRef(**query_ref)
    except (TypeError, ValueError) as exc:
        raise OperatorCallError('invalid_arguments', str(exc)) from exc
    if ref != LocalObjectStore(object_directory).reference(ref.sha256):
        raise OperatorCallError('invalid_arguments', 'query_ref must be a returned embedding object in the configured store')
    if type(top_k) is not int or not 1 <= top_k <= max_top_k:
        raise OperatorCallError('invalid_arguments', 'top_k exceeds the configured search limit')
    try:
        encoded = json.loads(ref.read(max_bytes=2 * 1024**2))
    except (ValueError, OSError) as exc:
        raise OperatorCallError('invalid_arguments', 'Unreadable embedding object: ' + str(exc)[:512]) from exc
    if not isinstance(encoded, dict) or encoded.get('format') != 'demiflow.embedding.v1' or encoded.get('encoder_id') != encoder_id:
        raise OperatorCallError('invalid_arguments', 'Embedding format or encoder_id differs from the configured vector space')
    actor = VectorSearch(query='vector', output='candidates', uri=uri, version=version,
        vector_column=vector_column, columns=columns, top_k=top_k, metric=metric,
        filter=None, filter_column=None, storage_options=None, options=options, label='search_vectors API')
    try:
        table = actor._open()
        contract = (table.schema.metadata or {}).get(contract_metadata_key.encode())
        if contract is not None and len(contract) > 1024**2:
            raise ValueError('Search table encoding contract exceeds 1 MiB')
        if contract is None or hashlib.sha256(canonical(json.loads(contract)).encode()).hexdigest() != encoder_id:
            raise ValueError('Fixed search table encoding contract differs from encoder_id')
        if encoded.get('dimensions') != actor._vector_type.list_size:
            raise OperatorCallError('invalid_arguments', 'Embedding dimensions differ from the search column')
        return {'candidates': actor({'vector': encoded['vector']})['candidates'],
                'source': {'uri': uri, 'version': version}, 'encoder_id': encoder_id}
    finally:
        # Blocking dispatch drains this function before cancellation completes.
        actor._dataset = actor._vector_type = None


DEFINITION = dict(
    name='search_vectors', function='demiflow.lance.search_api:search_vectors', execution='thread', replay='recorded',
    description='Search the configured fixed vector table with map_embeddings.embedding_ref. Image receipts identify which candidates were actually attached; select only attached images.',
    arguments=object_schema({'query_ref': REFERENCE,
                             'top_k': {'type': 'integer', 'minimum': 1, 'maximum': 64}}, ['query_ref']),
    fixed_schema=object_schema({
        **{key: {'type': 'string', 'minLength': 1} for key in
           ('uri', 'vector_column', 'encoder_id', 'object_directory', 'contract_metadata_key')},
        'version': {'type': 'integer', 'minimum': 1},
        'columns': {'type': 'array', 'minItems': 1, 'maxItems': 32, 'items': {'type': 'string'}},
        'metric': {'type': 'string', 'enum': ['l2', 'cosine', 'dot']},
        'max_top_k': {'type': 'integer', 'minimum': 1, 'maximum': 64},
        'options': {'type': 'object', 'additionalProperties': True},
    }, ['uri', 'version', 'vector_column', 'columns', 'encoder_id', 'object_directory', 'contract_metadata_key']))
