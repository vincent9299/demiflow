"""Immutable append-only terminal segments in the original artifact store.

A binary forest bounds each block's current index by log(segment count). Prior
prefix bytes are never rewritten. Historical direct record refs remain readable.
Pins still belong to the original commit/handle owners; no parallel ledger.
"""
from dataclasses import asdict
import json

from .contracts.dataflow import ArtifactRef
from .contracts.dataflow_errors import DataflowError

RECORDS_MEDIA = 'application/x-demiflow-records-v3'
FINITE_RECORDS_MEDIA = 'application/x-demiflow-finite-records-v3'
SEGMENTS_MEDIA = 'application/x-demiflow-terminal-segments.v1'
SCHEMA = 'demiflow.terminal-segments.v1'


def _fail(message):
    raise DataflowError('CORRUPT_ARTIFACT', message)


def _read(store, ref, bounds, *, frame_call=None):
    if ref.media_type != SEGMENTS_MEDIA or ref.size_bytes > bounds.max_control_bytes:
        _fail('terminal segment index is invalid or exceeds its bound')
    if frame_call is None:
        value = json.loads(b''.join(store.read(ref, max_bytes=bounds.max_control_bytes)))
    else:
        from .dataflow_record_frames import read_document, decode_metadata
        raw = frame_call(read_document, ref, store, bounds, _maximum=bounds.max_control_bytes)
        value = frame_call(decode_metadata, raw, bounds)
        del raw
    if type(value) is not dict or set(value) != {'schema', 'height', 'rows', 'bytes', 'children'} or value['schema'] != SCHEMA:
        _fail('terminal segment index has an unknown shape')
    if any(type(value[k]) is not int or value[k] < 0 for k in ('height', 'rows', 'bytes')):
        _fail('terminal segment index counts must be nonnegative integers')
    if value['height'] > bounds.max_depth:
        _fail('terminal segment depth exceeds bound')
    if type(value['children']) is not list or not 1 <= len(value['children']) <= bounds.max_depth + 1:
        _fail('terminal forest fanout exceeds bound')
    return value


def _entry(ref, *, rows, size, height):
    return {'ref': asdict(ref), 'rows': rows, 'bytes': size, 'height': height}


def _ref(entry):
    if type(entry) is not dict or set(entry) != {'ref', 'rows', 'bytes', 'height'}:
        _fail('terminal tree entry has an unknown shape')
    if any(type(entry[k]) is not int or entry[k] < 0 for k in ('rows', 'bytes', 'height')):
        _fail('terminal tree entry counters are invalid')
    return ArtifactRef(**entry['ref'])


def tree_refs(store, root, bounds, *, leaves_only=False, frame_call=None, on_retained=None):
    """Depth-first bounded traversal, authenticates metadata and rejects cycles."""
    stack = [(root, None, frozenset())]
    visited = 0
    # There can be at most one nonempty segment per emitted row, plus empty
    # logical blocks; each binary tree has < 2N nodes. No unbounded visited set.
    ceiling = 4 * (bounds.max_output_rows + bounds.max_partitions + 1)
    while stack:
        ref, expected, ancestors = stack.pop()
        if on_retained is not None:
            from .dataflow_record_frames import metadata_bytes
            on_retained(metadata_bytes((stack, ref, expected, ancestors)))
        visited += 1
        key = (ref.store_id, ref.key)
        if visited > ceiling or len(ancestors) > bounds.max_depth or key in ancestors:
            _fail('terminal tree exceeds node/depth budget or contains a cycle')
        if ref.media_type != SEGMENTS_MEDIA:
            if expected is not None and ref.media_type not in (RECORDS_MEDIA, FINITE_RECORDS_MEDIA):
                _fail('terminal tree leaf must be a record segment')
            if expected is not None and (expected['height'] != 0 or expected['bytes'] != ref.size_bytes):
                _fail('terminal segment size/height disagrees with its parent')
            yield ref
            continue
        node = _read(store, ref, bounds, frame_call=frame_call)
        if frame_call is not None:
            from .dataflow_record_frames import admit_workspace, metadata_bytes
            frame_call(admit_workspace, 2 * metadata_bytes(node), bounds)
        if expected is not None and any(node[k] != expected[k] for k in ('rows', 'bytes', 'height')):
            _fail('terminal index counters disagree with its parent')
        children = node['children']
        if expected is not None and len(children) != 2:
            _fail('terminal internal node must be binary')
        for child in children:
            _ref(child)
        if sum(c['rows'] for c in children) != node['rows'] or sum(c['bytes'] for c in children) != node['bytes']:
            _fail('terminal child totals disagree with their index')
        if node['height'] <= max(c['height'] for c in children):
            _fail('terminal child depth is not decreasing')
        if not leaves_only:
            yield ref
        for child in reversed(children):
            stack.append((_ref(child), child, ancestors | {key}))
        del node, children, child


def segment_refs(store, root, bounds, *, frame_call=None, on_retained=None):
    try:
        yield from tree_refs(store, root, bounds, leaves_only=True,
                            frame_call=frame_call, on_retained=on_retained)
    finally:
        if on_retained is not None:
            on_retained(0)


def block_chunks(store, root, bounds, *, chunk_bytes=None, frame_call=None, on_retained=None):
    refs = segment_refs(store, root, bounds, frame_call=frame_call, on_retained=on_retained)
    try:
        for ref in refs:
            options = {} if chunk_bytes is None else {'chunk_bytes': chunk_bytes}
            if frame_call is None:
                yield from store.read(ref, max_bytes=max(1, ref.size_bytes), **options)
                continue
            from .dataflow_record_frames import admit_workspace
            chunks = store.read(ref, max_bytes=max(1, ref.size_bytes), **options)
            try:
                while True:
                    # Metadata traversal can grow the held index AFTER the
                    # line reader's first growth check. Recheck before next()
                    # allocates a physical chunk with that index now live.
                    frame_call(admit_workspace, min(ref.size_bytes, chunk_bytes or bounds.max_row_bytes), bounds)
                    try:
                        chunk = next(chunks)
                    except StopIteration:
                        break
                    yield chunk
                    del chunk
            finally:
                chunks.close()
    finally:
        refs.close()


def block_totals(store, root, bounds, *, frame_call=None):
    """Read bounded aggregate metadata; traversal still validates each leaf."""
    if root.media_type == SEGMENTS_MEDIA:
        value = _read(store, root, bounds, frame_call=frame_call)
        return value['rows'], value['bytes']
    return None, root.size_bytes


class SegmentForest:
    def __init__(self, store, bounds):
        self.store, self.bounds = store, bounds
        self.roots = []
        self.rows = self.bytes = 0
        self._sealed = None

    def restore(self, ref, *, rows, size):
        if ref.media_type == SEGMENTS_MEDIA:
            node = _read(self.store, ref, self.bounds)
            if node['rows'] != rows or node['bytes'] != size:
                _fail('restored terminal index differs from its manifest')
            # Full integrity is checked before exposing historical records.
            self.roots = node['children']
        else:
            self.roots = [_entry(ref, rows=rows, size=size, height=0)]
        self.rows, self.bytes, self._sealed = rows, size, ref

    def _put(self, children):
        value = {'schema': SCHEMA, 'height': 1 + max(c['height'] for c in children),
                 'rows': sum(c['rows'] for c in children),
                 'bytes': sum(c['bytes'] for c in children), 'children': children}
        if value['height'] > self.bounds.max_depth:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal segment index depth exceeds bound')
        raw = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
        ref = self.store.put(iter([raw]), max_bytes=self.bounds.max_control_bytes, media_type=SEGMENTS_MEDIA)
        return _entry(ref, rows=value['rows'], size=value['bytes'], height=value['height'])

    def append(self, ref, *, rows):
        entry = _entry(ref, rows=rows, size=ref.size_bytes, height=0)
        self.rows += rows
        self.bytes += ref.size_bytes
        while self.roots and self.roots[-1]['height'] == entry['height']:
            entry = self._put([self.roots.pop(), entry])
        self.roots.append(entry)
        if len(self.roots) > self.bounds.max_depth + 1:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal forest root count exceeds bound')
        self._sealed = None

    def seal(self):
        if self._sealed is not None:
            return self._sealed
        if not self.roots:
            ref = self.store.put(iter(()), max_bytes=1, media_type=RECORDS_MEDIA)
            self._sealed = ref
        else:
            self._sealed = _ref(self._put(self.roots))
        return self._sealed
