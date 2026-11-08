"""Pure contracts for bounded local objects, documents and raster transforms."""
from pathlib import PurePosixPath

from ..data.flow_contract import field, scalar, require_fields


def _integer(a, name, low, high):
    if type(a[name]) is not int or not low <= a[name] <= high:
        raise ValueError(f'{name} must be an integer in {low}..{high}')


def _base(arguments, source):
    a = dict(arguments)
    for name in (source, 'output', 'label'):
        if type(a[name]) is not str or not 1 <= len(a[name]) <= 128:
            raise ValueError('Invalid transform field/label: ' + name)
    error = a['error_output']
    if error is not None and (type(error) is not str or not 1 <= len(error) <= 128 or error == a['output']):
        raise ValueError('Error output must be a distinct field')
    _integer(a, 'concurrency', 1, 64)
    a['queue_depth'] = a['concurrency'] if a['queue_depth'] is None else a['queue_depth']
    _integer(a, 'queue_depth', 1, 256)
    return a


def normalize_object(arguments):
    a = _base(arguments, 'reference')
    _integer(a, 'max_bytes', 1, 128 * 1024**2)
    if a['layout'] not in {'path', 'sha256_prefix2'}:
        raise ValueError('Unknown local object path layout')
    if type(a['relocations']) not in (list, tuple) or len(a['relocations']) > 16:
        raise ValueError('Object relocations must have at most 16 explicit roots')
    a['relocations'] = list(a['relocations'])
    roots = [a['root'], *a['relocations']]
    for value in roots:
        if type(value) is not str or len(value) > 4096:
            raise ValueError('Object roots must be absolute normalized paths')
        path = PurePosixPath(value)
        if not path.is_absolute() or '..' in path.parts or str(path) != value or len(path.parts) > 64:
            raise ValueError('Object roots must be absolute normalized paths')
    for i, left in enumerate(roots):
        for right in roots[i + 1:]:
            if PurePosixPath(left).is_relative_to(right) or PurePosixPath(right).is_relative_to(left):
                raise ValueError('Overlapping object roots make relocation ambiguous')
    return a


def object_resources(a):
    return [{'kind': 'objects', 'parameter': 'objects', 'root': a['root'],
             'relocations': a['relocations'], 'layout': a['layout']}]


def normalize_document(arguments):
    a = _base(arguments, 'column')
    _integer(a, 'max_bytes', 1, 16 * 1024**2)
    _integer(a, 'max_blocks', 1, 65536)
    return a


def normalize_image(arguments):
    a = _base(arguments, 'column')
    for name, low, high in [('max_side', 1, 4096), ('quality', 1, 95),
        ('max_source_pixels', 1, 160000000), ('max_bytes', 1, 32 * 1024**2),
        ('memory_bytes', 256 * 1024**2, 2 * 1024**3)]:
        _integer(a, name, low, high)
    if type(a['timeout_s']) not in (int, float) or not 0 < a['timeout_s'] <= 120:
        raise ValueError('Image timeout must be finite in (0, 120]')
    if a['profile'] not in {'thumbnail_lanczos_v1', 'oriented_white_bicubic_v1'}:
        raise ValueError('Unknown image encoding profile')
    return a


def normalize_wiki(arguments):
    a = _base(arguments, 'column')
    _integer(a, 'max_chars', 1, 1024**2)
    _integer(a, 'max_nodes', 1, 65536)
    _integer(a, 'max_depth', 1, 64)
    _integer(a, 'max_bytes', 1024, 32 * 1024**2)
    return a


def record(fields):
    return {**scalar('record'), 'fields': {k: field(v) for k, v in fields.items()}}


def sequence(item):
    return {**scalar('list'), 'item': item}


def _analysis(a, inputs, source, source_kind, result, kind, reserved, resources=()):
    schema = inputs[0]
    require_fields(schema, [a[source]])
    actual = schema[a[source]]['type']
    if actual['kind'] != source_kind:
        raise ValueError('Native transform input requires ' + source_kind)
    fields = dict(schema)
    for name, typ in [(a['output'], {**result, 'nullable': True}), (a['error_output'], scalar('str', nullable=True))]:
        if name is not None:
            if name in fields:
                raise ValueError('Native transform destination must not overwrite input: ' + name)
            fields[name] = field(typ)
    return {'fields': fields, 'cardinality': 'one', 'order': 'unspecified', 'driver': 'stream',
        'kind': 'async_map', 'callable_form': 'sync', 'placement': 'thread', 'scope': 'stage',
        'native': {'contract': 'demiflow.collect.' + kind + '.v1', 'arguments': a},
        'stream_arguments': dict(concurrency=a['concurrency'], queue_depth=a['queue_depth'], catch=[],
            label=a['label'], hard_timeout=None, execution='thread'),
        'reserved_bytes': reserved, 'resources': list(resources),
        'drain_timeout_s': a['timeout_s'] + 2 if 'timeout_s' in a else 0}


def analyze_object(a, *, inputs, callables, resources):
    spec = inputs[0][a['reference']]['type']
    if spec['kind'] != 'record':
        raise ValueError('Local object input must be a URI/SHA256 record')
    require_fields(spec['fields'], ['uri', 'sha256'])
    if any(spec['fields'][key]['type']['kind'] != 'str' or spec['fields'][key]['type'].get('nullable') for key in ('uri', 'sha256')):
        raise ValueError('Object URI and SHA256 must be non-null strings')
    return _analysis(a, inputs, 'reference', 'record', scalar('bytes'), 'read_object',
                     a['concurrency'] * (2 * a['max_bytes'] + 65536), [resources['objects']])


def analyze_document(a, *, inputs, callables, resources):
    result = record({'source': record({k: scalar('str') for k in ('url', 'final_url', 'title', 'retrieved_at', 'content_type')}),
        'blocks': sequence(record({'block_id': scalar('str'), 'position': scalar('int'), 'kind': scalar('str'),
                                   'text': scalar('str'), 'headings': sequence(scalar('str'))}))})
    return _analysis(a, inputs, 'column', 'bytes', result, 'decode_document', a['concurrency'] * 16 * a['max_bytes'])


def analyze_image(a, *, inputs, callables, resources):
    return {**_analysis(a, inputs, 'column', 'bytes', record({'data_url': scalar('str'), 'sha256': scalar('str')}),
        'encode_image', a['concurrency'] * (a['memory_bytes'] + 2 * a['max_bytes'] + 32 * 1024**2)),
        'required_distributions': ['pillow']}


def wiki_type():
    ids = sequence(scalar('int'))
    return record({'roots': ids, 'nodes': sequence(record({'id': scalar('int'), 'kind': scalar('str'),
        'text': scalar('str'), 'name': scalar('str'), 'children': ids, 'title': ids, 'label': ids,
        'has_label': scalar('bool'), 'parameters': sequence(record({'name': scalar('str'), 'raw': scalar('str'), 'value': ids}))}))})


def analyze_wiki(a, *, inputs, callables, resources):
    return {**_analysis(a, inputs, 'column', 'str', wiki_type(), 'parse_wikitext',
        a['concurrency'] * (256 * a['max_chars'] + 2 * a['max_bytes'])), 'required_distributions': ['mwparserfromhell']}


def row_result(row, a, value, error=None):
    if a['output'] in row or a['error_output'] is not None and a['error_output'] in row:
        raise ValueError('Native transform destination overlaps an input field')
    if error is not None and a['error_output'] is None:
        raise error
    result = {**row, a['output']: value}
    if a['error_output'] is not None:
        result[a['error_output']] = (type(error).__name__ + ': ' + str(error)[:2048]) if error else None
    return result


def collect_facts(actor):
    """Only exact native actor types have authority; no candidate callback."""
    from ..objects import ReadLocalObject
    from .documents import DecodeDocument
    from .image_resize import EncodeImage
    from .wiki_syntax import ParseWikitext
    a = actor.arguments
    if type(actor) is ReadLocalObject:
        kind, reservation = 'read_object', a['concurrency'] * (2 * a['max_bytes'] + 65536)
    elif type(actor) is DecodeDocument:
        kind, reservation = 'decode_document', a['concurrency'] * 16 * a['max_bytes']
    elif type(actor) is EncodeImage:
        kind, reservation = 'encode_image', a['concurrency'] * (a['memory_bytes'] + 2 * a['max_bytes'] + 32 * 1024**2)
    elif type(actor) is ParseWikitext:
        kind, reservation = 'parse_wikitext', a['concurrency'] * (256 * a['max_chars'] + 2 * a['max_bytes'])
    else:
        raise TypeError('No native collect actor contract')
    return {'native': {'contract': 'demiflow.collect.' + kind + '.v1', 'arguments': dict(a)}, 'reserved_bytes': reservation}
