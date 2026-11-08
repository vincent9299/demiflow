"""Pure bounded Arrow schema declarations, shared by authoring and writes.

Field order, nested physical types, nullability and UTF-8 metadata are explicit.
Declarations contain data only; no Python expression or callable is evaluated.
"""
import json
import hashlib

from ..data.flow_contract import flow_api, arrow_fields, scalar


def _bounded(value):
    size = 0
    for chunk in json.JSONEncoder(allow_nan=False).iterencode(value):
        size += len(chunk.encode('utf-8'))
        if size > 256 * 1024:
            raise ValueError('Schema declaration exceeds 256 KiB')


def _metadata(value):
    if value is None:
        return None
    if type(value) is not dict or len(value) > 128 or any(type(k) is not str or type(v) is not str for k, v in value.items()):
        raise ValueError('Schema metadata requires a bounded UTF-8 string mapping')
    return {k.encode('utf-8'): value[k].encode('utf-8') for k in sorted(value)}


def schema_identity(schema):
    """Full Arrow identity, including metadata ignored by legacy table hashes."""
    raw = schema.serialize()
    if raw.size > 1024 * 1024:
        raise ValueError('Serialized schema exceeds 1 MiB')
    return hashlib.sha256(raw).hexdigest()


def build_schema(declaration):
    import pyarrow as pa
    _bounded(declaration)
    if type(declaration) is not dict or set(declaration) - {'fields', 'metadata'} or 'fields' not in declaration:
        raise ValueError('Schema declaration requires fields and optional metadata')
    count = 0

    def fields(values, depth):
        if type(values) is not list or len(values) > 2048:
            raise ValueError('Schema fields require a bounded ordered list')
        result = [field(value, depth) for value in values]
        if len({item.name for item in result}) != len(result):
            raise ValueError('Duplicate schema field name')
        return result

    def field(value, depth):
        nonlocal count
        count += 1
        if depth > 16 or count > 2048:
            raise ValueError('Schema depth or field count exceeds budget')
        if type(value) is not dict or set(value) - {'name', 'type', 'nullable', 'metadata'} or not {'name', 'type'} <= set(value):
            raise ValueError('Invalid schema field declaration')
        name = value['name']
        if type(name) is not str or not name or len(name.encode('utf-8')) > 1024 or type(value.get('nullable', True)) is not bool:
            raise ValueError('Invalid schema field name or nullability')
        return pa.field(name, dtype(value['type'], depth + 1), nullable=value.get('nullable', True), metadata=_metadata(value.get('metadata')))

    def dtype(value, depth):
        primitives = {'null': pa.null, 'bool': pa.bool_, 'int8': pa.int8, 'int16': pa.int16,
                      'int32': pa.int32, 'int64': pa.int64, 'uint8': pa.uint8, 'uint16': pa.uint16,
                      'uint32': pa.uint32, 'uint64': pa.uint64, 'float32': pa.float32, 'float64': pa.float64,
                      'string': pa.string, 'large_string': pa.large_string, 'binary': pa.binary, 'large_binary': pa.large_binary}
        if type(value) is str and value in primitives:
            return primitives[value]()
        if type(value) is dict and set(value) == {'struct'}:
            return pa.struct(fields(value['struct'], depth))
        if type(value) is dict and set(value) in ({'list'}, {'large_list'}):
            kind = next(iter(value))
            return (pa.list_ if kind == 'list' else pa.large_list)(field(value[kind], depth))
        raise ValueError('Unsupported physical schema type')

    return pa.schema(fields(declaration['fields'], 0), metadata=_metadata(declaration.get('metadata')))


def normalize_schema(arguments):
    declaration = arguments['declaration']
    build_schema(declaration)
    return {'declaration': declaration}


def analyze_schema(arguments, *, inputs, callables, resources):
    from .storage import schema_hash
    schema = build_schema(arguments['declaration'])
    return {'fields': {}, 'driver': 'pull', 'value_type': scalar('schema'),
            'value': {'schema_declaration': arguments['declaration']},
            'schema_hash': schema_hash(schema), 'schema_identity': schema_identity(schema), 'schema_fields': arrow_fields(schema),
            'cardinality': 'none', 'order': 'preserved'}


@flow_api('arrow_schema', role='value', normalize=normalize_schema, analyze=analyze_schema)
def schema(declaration):
    """Construct a real Arrow Schema from an explicit, pure data declaration."""
    return build_schema(declaration)
