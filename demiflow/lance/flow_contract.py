"""Pure native contracts for fixed-version Lance resources."""
def normalize_lookup(arguments):
    values = dict(arguments)
    source, on, output = values['source'], values['on'], values['output']
    if not isinstance(source, dict) or set(source) != {'uri', 'version'} or type(source['version']) is not int or source['version'] < 1:
        raise ValueError('Lookup requires a fixed uri/version')
    if not isinstance(on, str) or not on or not isinstance(output, str) or not output or on == output:
        raise ValueError('Lookup requires distinct key and output fields')
    if type(values['max_matches']) is not int or not 1 <= values['max_matches'] <= 64 or type(values['max_bytes']) is not int or not 1 <= values['max_bytes'] <= 16 * 1024**2:
        raise ValueError('Invalid lookup bounds')
    if isinstance(values.get('columns'), (list, tuple)):
        values['columns'] = list(values['columns'])
    return values


def lookup_resources(arguments):
    return [{'kind': 'table', 'parameter': 'source', 'value': arguments['source']}]


def analyze_lookup(arguments, *, inputs, callables, resources):
    from ..data.flow_contract import scalar, field, require_fields, compatible
    schema, table = inputs[0], resources['source']['fields']
    if arguments['columns'] is not None and (type(arguments['columns']) is not list or not arguments['columns'] or any(type(c) is not str or not c for c in arguments['columns'])):
        raise ValueError('Managed lookup columns require a nonempty list of fields')
    key = arguments['on']
    require_fields(schema, [key])
    require_fields(table, [key])
    if schema[key]['type']['kind'] not in {'str', 'int', 'bool'} or schema[key]['type'].get('nullable') or not compatible(schema[key]['type'], table[key]['type']):
        raise ValueError('Lookup requires a non-null scalar key compatible with the fixed table')
    if arguments['output'] in schema:
        raise ValueError('Lookup output would overwrite an input field')
    columns = arguments['columns'] or list(table)
    require_fields(table, columns)
    result = {**schema, arguments['output']: field({**scalar('list'), 'item': {**scalar('record'), 'fields': {c: table[c] for c in columns}}})}
    return {'fields': result, 'cardinality': 'one', 'order': 'preserved', 'driver': 'stream',
            'kind': 'async_map', 'callable_form': 'awaitable', 'placement': 'inline', 'scope': 'stage',
            'native': {'contract': 'demiflow.lookup_lance', 'arguments': arguments},
            'resources': [{'kind': 'table', 'access': 'read', 'value': arguments['source']}],
            'reserved_bytes': 8 * 1024**2 + 2 * arguments['max_bytes'],
            'stream_arguments': {'concurrency': 1, 'queue_depth': 1, 'catch': [], 'label': 'lookup_' + arguments['output'], 'execution': 'inline', 'hard_timeout': None}}


def lookup_facts(actor):
    return {'contract': 'demiflow.lookup_lance', 'arguments': normalize_lookup({
        name: getattr(actor, name) for name in ('source', 'on', 'columns', 'output', 'max_matches', 'max_bytes')})}


def normalize_save(arguments):
    from .schema import build_schema, schema_identity
    import pyarrow as pa
    import math
    values = dict(arguments)
    schema = values['schema']
    if type(schema) is dict and set(schema) == {'schema_declaration'}:
        schema = build_schema(schema['schema_declaration'])
    if not isinstance(schema, pa.Schema):
        raise ValueError('Managed save requires an explicit native Arrow schema')
    values['schema'] = {'identity': schema_identity(schema)}
    key = values['key']
    values['key'] = [key] if isinstance(key, str) else list(key)
    if not values['key'] or len(values['key']) > 32 or len(set(values['key'])) != len(values['key']) or set(values['key']) - set(schema.names):
        raise ValueError('Save keys must be unique fields in the declared schema')
    if values['mode'] not in {'append', 'overwrite'}:
        raise ValueError('Managed stream save requires append or overwrite')
    if type(values['stage']) is not str or not values['stage'].isidentifier() or len(values['stage']) > 64:
        raise ValueError('Invalid save stage identity')
    for name, maximum in [('max_batch', 128), ('queue_depth', 128), ('max_rows', 100_000_000),
                          ('max_row_bytes', 64 * 1024**2), ('max_key_bytes', 65536), ('max_index_bytes', 16 * 1024**3)]:
        if type(values[name]) is not int or not 1 <= values[name] <= maximum:
            raise ValueError('Invalid managed save bound: ' + name)
    if type(values['flush_interval']) not in (int, float) or not math.isfinite(values['flush_interval']) or values['flush_interval'] <= 0:
        raise ValueError('Invalid stream flush interval')
    output = values['output_ref']
    if output is not None and (type(output) is not str or not 1 <= len(output) <= 128 or output in schema.names):
        raise ValueError('Save output_ref must be distinct from saved fields')
    return values


def analyze_save(arguments, *, inputs, callables, resources):
    from ..data.flow_contract import scalar, field, compatible, require_fields
    fields, saved = inputs[0], resources['schema_fields']
    if not compatible({**scalar('record'), 'fields': fields}, {**scalar('record'), 'fields': saved}):
        raise ValueError('Stream save schema conflicts with input fields or nullability')
    require_fields(saved, arguments['key'])
    if any(saved[k]['type']['kind'] not in {'str', 'int', 'bool'} for k in arguments['key']):
        raise ValueError('Stream save keys require scalar field types')
    output = arguments['output_ref']
    if output in fields:
        raise ValueError('Save output_ref would overwrite an input field')
    if output is not None:
        fields = {**fields, output: field({**scalar('record'), 'fields': {
            'uri': field(scalar('str')), 'version': field(scalar('int'))}}, required=not callables)}
    stream = {'concurrency': 1, 'queue_depth': arguments['queue_depth'], 'catch': [],
              'label': 'save_' + arguments['stage'], 'hard_timeout': None,
              'max_batch': arguments['max_batch'], 'flush_interval': arguments['flush_interval']}
    return {'fields': fields, 'driver': 'stream', 'kind': 'batch_map', 'input_driver': 'either',
            'cardinality': 'one', 'order': 'preserved', 'stream_arguments': stream,
            'native': {'contract': 'demiflow.save_lance', 'arguments': arguments, 'when': bool(callables)},
            'reserved_bytes': 8 * 1024**2 + arguments['max_batch'] * arguments['max_row_bytes'] * 3}


def save_facts(actor, operation):
    values = {name: getattr(actor, name) for name in ('uri', 'schema', 'key', 'stage', 'output_ref',
              'mode', 'max_rows', 'max_row_bytes', 'max_key_bytes', 'max_index_bytes')}
    values.update({name: getattr(operation, name) for name in ('max_batch', 'flush_interval', 'queue_depth')})
    return {'contract': 'demiflow.save_lance', 'arguments': normalize_save(values), 'when': actor.when is not None}


def lookup_declaration(actor, target):
    from demiflow.operators.lookup import LanceLookup
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    declaration = lookup_facts(actor)['arguments']
    if getattr(target, '__func__', None) is not LanceLookup.__call__:
        raise DataflowError('INVALID_CONTRACT', 'Lookup must bind the native lookup callable')
    if actor.table is not None:
        raise DataflowError('INVALID_CONTRACT', 'Lookup requires an unstarted fixed-version declaration')
    return declaration


def vector_search_declaration(actor):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    if (actor._dataset is not None or actor._vector_type is not None or actor._version is not None
            or actor._queries or actor._hits):
        raise DataflowError('INVALID_CONTRACT', 'Vector search requires an unstarted declaration')
    return {**actor.spec, 'query': actor.query, 'output': actor.output, 'label': actor.label}


def vector_search_facts(operation):
    from demiflow.data.runtime_contract import RuntimeContractError
    from demiflow.execution.recovery_contract import identity
    from demiflow.lance.search import VectorSearch
    target = operation.callable.target
    actor = getattr(target, '__self__', None)
    if (type(actor) is not VectorSearch or getattr(target, '__func__', None) is not VectorSearch.__call__
            or actor.spec != operation.search_spec or actor.query != operation.query
            or actor.output != operation.output or actor.options != operation.search_spec.get('options')
            or actor.filter_column != operation.search_spec.get('filter_column')):
        raise RuntimeContractError('VectorSearch actor differs from its declared plan')
    spec = dict(operation.search_spec)
    storage = spec.get("storage_options") or {}
    # Reference storage options by bounded identity; never copy credential
    # material into the permanent plan fact.
    spec["storage_options"] = {"sha256": identity(sorted(storage.items())),
                               "keys": sorted(storage)}
    return {'contract': 'demiflow.search_vectors',
            'arguments': dict(spec, query=operation.query, output=operation.output)}


def stream_lance_output_schema(schema, output_ref):
    """A native publication passes input fields through and may add a receipt."""
    if schema is None or output_ref is None:
        return schema
    if output_ref in schema.names:
        raise ValueError('output_ref would overwrite an input field')
    import pyarrow as pa
    return schema.append(pa.field(output_ref,
        pa.struct([pa.field('uri', pa.string()), pa.field('version', pa.int64())]), nullable=True))
