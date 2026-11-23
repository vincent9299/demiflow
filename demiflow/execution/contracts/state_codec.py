"""Data-only snapshots for kernel buffers (no pickle of mappingproxy/actors)."""
import json
from dataclasses import asdict
from collections.abc import Mapping

from demiflow.execution.dataflow_record_frames import raw_workspace
from demiflow.execution.contracts.dataflow import Record, ArtifactRef, AggregateState
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.exchange.dataflow_bridge import _encode_value, _decode_value, _encode_record_document, _decode_record_document

RECORD_LIST_PREFIX = b'["demiflow.kernel-state.v3",["list",['
RECORD_LIST_SUFFIX = b']]]'


def size_error(message, limit, *, actual=None, occurrence=None):
    context = {'limit_bytes': limit}
    if actual is not None:
        context['actual_bytes'] = actual
    if occurrence is not None:
        context['occurrence_id'] = occurrence
    detail = ', '.join(f'{name}={value}' for name, value in context.items())
    return DataflowError('BUDGET_EXCEEDED', f'{message} ({detail})', context=context)


def record_fragment(record, *, max_bytes, services=None, retained=0):
    """One original list element, reusable for byte admission and publication."""
    if services is None:
        raw = json.dumps(['record', record.record_id, record.sequence,
            _encode_record_document(record, max_bytes=max_bytes).decode()],
            separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    else:
        from demiflow.execution.dataflow_record_frames import admit_workspace, encode_document, retained_record_bytes
        bounds = services.bounds
        document = encode_document(record, bounds, services=services, retained=retained)
        # The document is already valid compact JSON, so embedding its UTF-8
        # in a JSON string escapes only quotes and backslashes. Check the
        # complete encoded fragment and its copy peak before building either.
        prefix = json.dumps(['record', record.record_id, record.sequence],
                            separators=(',', ':'), ensure_ascii=False).encode()[:-1] + b',"'
        escaped = len(document) + document.count(b'\\') + document.count(b'"')
        size = len(prefix) + escaped + 2
        total = size + len(RECORD_LIST_PREFIX) + len(RECORD_LIST_SUFFIX)
        if total > max_bytes:
            raise size_error('kernel image exceeds state byte budget', max_bytes, actual=total)
        live = retained + retained_record_bytes(record, bounds) + len(prefix) + len(document)
        admit_workspace(live + 2 * size, bounds, services=services)
        raw = b''.join((prefix, document.replace(b'\\', b'\\\\').replace(b'"', b'\\"'), b'"]'))
    size = len(raw) + len(RECORD_LIST_PREFIX) + len(RECORD_LIST_SUFFIX)
    if size > max_bytes:
        raise size_error('kernel image exceeds state byte budget', max_bytes, actual=size)
    return raw


def spool_fragment_workspace(raw, maximum):
    """Decode projection for one unchanged state-codec record fragment."""
    outer = 2 * raw_workspace(raw, typed=False)
    if outer > maximum:
        raise DataflowError('BUDGET_EXCEEDED', 'ordering input record exceeds its reader subwindow')
    item = json.loads(raw)
    if item[0] != 'record':
        raise DataflowError('CORRUPT_ARTIFACT', 'ordering input fragment is not a record')
    size = outer + raw_workspace(item[3].encode('utf-8')) + 256
    if size + 4096 > maximum:
        raise DataflowError('BUDGET_EXCEEDED', 'decoded ordering input record exceeds its reader subwindow')
    return size


def admit_spool_image(raw, maximum):
    """Pre-admit an existing bounded segment before decoding its record list."""
    outer = 2 * raw_workspace(raw, typed=False)
    if outer > maximum:
        raise DataflowError('BUDGET_EXCEEDED', 'ordering input segment exceeds its reader subwindow')
    envelope = json.loads(raw)
    held = outer
    for item in envelope[1][1]:
        if item[0] != 'record':
            raise DataflowError('CORRUPT_ARTIFACT', 'ordering input segment contains a non-record')
        held += raw_workspace(item[3].encode('utf-8'))
        if held > maximum:
            raise DataflowError('BUDGET_EXCEEDED', 'decoded ordering segment exceeds its reader subwindow')


def record_list_chunks(fragments):
    yield RECORD_LIST_PREFIX
    for index, raw in enumerate(fragments):
        if index:
            yield b','
        yield raw
    yield RECORD_LIST_SUFFIX


def measure(value, *, max_bytes, workspace_bytes):
    """Preflight the existing kernel encoding before constructing a packed tree.

    Count occurrences, including aliased children which JSON will expand again.
    Return retained bytes and the peak workspace charged to this encoding.
    """
    import sys
    from demiflow.execution.dataflow_record_frames import _admit, _json_shape, _typed_shape, _record_metadata
    wire = len(b'["demiflow.kernel-state.v3",]')
    native = 0
    tree = 0

    def peak():
        # JSON may hold encoder chunks plus a four-byte Unicode result.
        # The UTF-8 encoding and later base64 scalar fit this same window.
        return native + tree + 10 * wire + 1024

    def add(size, held, temporary):
        nonlocal wire, native, tree
        wire += size
        native += held
        tree += temporary
        if wire > max_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'kernel image exceeds state byte budget')
        _admit(peak(), workspace_bytes)

    def visit(item, depth):
        if depth > 64:
            raise DataflowError('BUDGET_EXCEEDED', 'kernel state exceeds bounded nesting depth')
        if isinstance(item, Mapping):
            count = len(item)
            add(len(b'["dict",[]]') + max(0, count - 1) + 3 * count,
                sys.getsizeof(item), 256 + 112 * count)
            for key, child in item.items():
                visit(key, depth + 1)
                visit(child, depth + 1)
        elif isinstance(item, (list, tuple)):
            count = len(item)
            tag = b'["tuple",[]]' if isinstance(item, tuple) else b'["list",[]]'
            add(len(tag) + max(0, count - 1), sys.getsizeof(item), 256 + 16 * count)
            for child in item:
                visit(child, depth + 1)
        elif isinstance(item, Record):
            # An embedded Record carries an escaped document. Admit the old
            # document encoder before computing its exact JSON string length.
            from demiflow.execution.dataflow_record_frames import _string_size
            n, held, temporary = _typed_shape(item.inline, max_bytes)
            metadata, meta_held = _json_shape(_record_metadata(item), max_bytes)
            _admit(native + tree + 10 * wire + held + temporary
                                  + meta_held + 10 * (metadata + n) + 2048, workspace_bytes)
            raw = _encode_record_document(item, max_bytes=max_bytes).decode()
            add(len(b'["record",,,]') + _string_size(item.record_id)
                + len(str(item.sequence)) + _string_size(raw), held + meta_held,
                len(raw) * 4 + 256)
        elif isinstance(item, ArtifactRef):
            n, held = _json_shape(asdict(item), max_bytes)
            add(len(b'["ref",]') + n, held, held + 256)
        elif isinstance(item, AggregateState):
            add(len(b'["aggregate",]'), 64, 128)
            visit(asdict(item), depth + 1)
        else:
            # These are the original encode_value leaves (including arrays,
            # typed sets, binary and temporal scalars).
            n, held, temporary = _typed_shape(item, min(max_bytes, workspace_bytes))
            add(len(b'["value",]') + n, held, temporary + 96)

    visit(value, 0)
    return native, peak()


def dumps(value, *, max_bytes, occurrence=None, admit=None):
    def document(array, literal):
        def pair(tag, item):
            return array(2, (literal(tag), item))

        def pack(v):
            if isinstance(v, Record):
                return array(4, (literal('record'), literal(v.record_id), literal(v.sequence),
                    literal(_encode_record_document(v, max_bytes=max_bytes).decode())))
            if isinstance(v, ArtifactRef):
                return pair('ref', literal(asdict(v)))
            if isinstance(v, AggregateState):
                return pair('aggregate', pack(asdict(v)))
            if isinstance(v, Mapping):
                entries = (array(2, (pack(k), pack(x))) for k, x in v.items())
                return pair('dict', array(len(v), entries))
            if isinstance(v, (tuple, list)):
                return pair('tuple' if isinstance(v, tuple) else 'list',
                            array(len(v), (pack(x) for x in v)))
            return pair('value', literal(_encode_value(v)))

        return pair('demiflow.kernel-state.v3', pack(value))

    if admit is not None:
        import sys
        from demiflow.execution.dataflow_record_frames import json_encoding_shape

        header = sys.getsizeof([])
        pointer = sys.getsizeof([None]) - header
        def array_shape(count, parts):
            resident = header + pointer * count
            encoded, width = 2 + max(0, count - 1), 1
            for child_bytes, child_encoded, child_width in parts:
                resident += child_bytes
                encoded += child_encoded
                width = max(width, child_width)
            return resident, encoded, width

        # The first pass folds the SAME format one child at a time. No packed
        # container or complete output exists until its full cost is admitted.
        resident, encoded, width = document(array_shape, json_encoding_shape)
        if encoded > max_bytes:
            raise size_error('kernel image exceeds state byte budget', max_bytes, actual=encoded,
                             occurrence=occurrence)
        # C JSON encoding builds text chunks, then their joined string, then
        # the UTF-8 image. Encoded length bounds the character count.
        admit(resident + max(2 * width, width + 1) * encoded)

    def array(count, parts):
        result = [None] * count
        for index, item in enumerate(parts):
            result[index] = item
        return result

    envelope = document(array, lambda item: item)
    raw = json.dumps(envelope, separators=(',', ':'),
                     ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > max_bytes:
        raise size_error('kernel image exceeds state byte budget', max_bytes, actual=len(raw), occurrence=occurrence)
    return raw


def loads(raw, *, max_bytes, occurrence=None):
    if len(raw) > max_bytes:
        raise size_error('kernel image exceeds state byte budget', max_bytes, actual=len(raw), occurrence=occurrence)
    def unpack(v):
        kind = v[0]
        if kind == 'record':
            return _decode_record_document(v[3].encode(), record_id=v[1], sequence=v[2], max_bytes=max_bytes)
        if kind == 'ref':
            return ArtifactRef(**v[1])
        if kind == 'aggregate':
            return AggregateState(**unpack(v[1]))
        if kind == 'dict':
            return {unpack(k): unpack(x) for k, x in v[1]}
        if kind in ('tuple', 'list'):
            result = [unpack(x) for x in v[1]]
            return tuple(result) if kind == 'tuple' else result
        if kind == 'value':
            return _decode_value(v[1])
        raise DataflowError('CORRUPT_ARTIFACT', 'unknown kernel state value')
    envelope = json.loads(raw)
    if envelope[0] != 'demiflow.kernel-state.v3':
        raise DataflowError('CORRUPT_ARTIFACT', 'unknown kernel state schema')
    return unpack(envelope[1])
