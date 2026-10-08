"""JSON input contract of the native read_documents(request, ...) API.

Execution context, row and worker limits are supplied by the owning node. The
request is the same value passed by Dataset.read_documents, without rewriting.
"""
from copy import deepcopy


def object_schema(properties, required=()):
    return {'type': 'object', 'properties': properties, 'required': list(required), 'additionalProperties': False}


STRING = {'type': 'string', 'maxLength': 4096}
STRINGS = {'type': 'array', 'maxItems': 256, 'items': STRING}
REFERENCE = object_schema({'uri': STRING, 'sha256': STRING}, ('uri', 'sha256'))
DOCUMENT = object_schema({'document_ref': REFERENCE, 'url': STRING,
                         'bindings': STRINGS, 'eligible': {'type': 'boolean'}}, ('document_ref', 'url'))
SELECTION = object_schema({'evidence_id': STRING, 'document_ref': REFERENCE,
                          'block_id': STRING, 'bindings': STRINGS},
                         ('evidence_id', 'document_ref', 'block_id', 'bindings'))
AUTOMATIC_SELECTION = object_schema({
    'heading_weight': {'type': 'integer', 'minimum': 1, 'maximum': 8},
    'include_neighbors': {'type': 'boolean'},
    'excluded_kinds': {'type': 'array', 'maxItems': 16, 'items': STRING},
    'min_matches': {'type': 'integer', 'minimum': 0, 'maximum': 32},
    'fallback_blocks': {'type': 'integer', 'minimum': 0, 'maximum': 4},
})
REQUEST = object_schema({
    'documents': {'type': 'array', 'items': DOCUMENT},
    'questions': {'type': 'array', 'items': object_schema({'id': STRING, 'text': STRING}, ('id', 'text'))},
    'selection': AUTOMATIC_SELECTION,
    'retained': {'type': 'array', 'items': SELECTION},
    'requests': {'type': 'array', 'items': object_schema({
        'request_id': STRING, 'document_ref': REFERENCE, 'block_ids': STRINGS,
        'section_id': STRING, 'bindings': STRINGS}, ('request_id', 'document_ref', 'block_ids', 'bindings'))},
}, ('documents', 'questions'))


def read_documents_arguments(unit='chars'):
    """Expose native request fields for the bound context's budget counter."""
    if unit not in ('chars', 'tokens'):
        raise ValueError('Unsupported reading budget unit')
    request = deepcopy(REQUEST)
    for name in ('new_' + unit, 'total_' + unit):
        request['properties'][name] = {'type': 'integer', 'minimum': 0}
        request['required'].append(name)
    return object_schema({'request': request}, ('request',))


READ_DOCUMENTS_DESCRIPTION = '''Native read_documents(request, *, context, row=None, document_concurrency=2, timeout_s=30, max_bytes=8388608).
Pass request unchanged, exactly as the Dataset request-column value: documents contains document_ref (uri, sha256), url and optional bindings/eligible; questions contains id/text; optional selection controls automatic heading weight, neighbors, excluded block kinds, minimum lexical matches and bounded fallback blocks. Omitted selection retains the legacy ranking. Explicit requests and retained blocks are not excluded by automatic selection. Optional retained preserves selected blocks; optional requests locates block_ids/section_id. new_chars and total_chars bound material under the supplied CharacterBudget. Copy document references from this row's resources. The platform binds context and execution limits; row uses its native default; they cannot be overridden. Returns full source blocks, unread ranges and per-document status. Does not fetch new URLs.'''
