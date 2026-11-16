"""Large literal inputs are data artifacts, never actor checkpoint state.

Small/old ItemsSource programs keep their original carrier. New large sources
retain the historical protocol-4 identity and use the original store, segment
index, typed row codec and source index cursor. No independent recovery ledger.
"""
from dataclasses import dataclass, asdict
import hashlib
import pickle

from .contracts.dataflow import ArtifactRef, Record, thaw_record_value
from .contracts.dataflow_errors import DataflowError
from .dataflow_record_frames import (admit_workspace, metadata_bytes, read_document,
    decode_metadata, decode_frame, decode_document, frame_parts, frame_stream,
    read_chunk_bytes, line_limit, _typed_shape, _json_shape)
from .dataflow_segments import SegmentForest, RECORDS_MEDIA, SEGMENTS_MEDIA, range_segments, block_totals
from .recovery_contract import document, identity

SCHEMA = 'demiflow.literal-source.v1'
MEDIA = 'application/vnd.demiflow.source-manifest.v3'


def items_fingerprint(items):
    """Exact old pickle protocol-4 bytes, hashed without an all-input bytes copy.

    Pickle retains its original alias memo; this is not a claim of constant
    memory for arbitrary Python declarations. Existing literal structural
    admission still applies before execution. Large bytes are borrowed writes.
    """
    class DigestWriter:
        def __init__(self):
            self.digest, self.size = hashlib.sha256(), 0
        def write(self, chunk):
            self.digest.update(chunk)
            self.size += len(chunk)
            return len(chunk)
    writer = DigestWriter()
    pickle.Pickler(writer, protocol=4).dump(items)
    return writer.digest.hexdigest(), writer.size


@dataclass(frozen=True)
class LiteralSourceReference:
    count: int
    items_digest: str
    schema: str = SCHEMA
    kind: str = "items"


def literal_reference(source, bounds):
    from ..data.sources import ItemsSource, MaterializedSource
    from .dataflow_inputs import InputMaterializedHandle
    if type(source) is ItemsSource:
        value, count, kind = source.items, len(source.items), 'items'
    elif type(source) is MaterializedSource and type(source.handle) is InputMaterializedHandle:
        value, count, kind = source.handle, source.handle.row_count, 'materialized'
    else:
        return None
    digest, size = items_fingerprint(value)
    # A bounded tiny program keeps the legacy zero-data-artifact fast path.
    # Both program and snapshot use this function; resume uses sealed carriers.
    if size <= min(65536, max(1, bounds.max_state_bytes // 4)):
        return None
    return LiteralSourceReference(count, digest, kind=kind)


def _forest_workspace(forest, reference, bounds):
    # Only one bounded index document is built at a time. Its largest fanout is
    # the final forest (or a two-child merge). Use the declared upper counts and
    # fixed artifact-reference field widths, not the unrelated full control cap.
    ref = ArtifactRef('f' * 64, bounds.max_spill_bytes, forest.store.store_id,
                      'f' * 64, SEGMENTS_MEDIA)
    entry = {'ref': asdict(ref), 'rows': reference.count,
             'bytes': bounds.max_spill_bytes, 'height': bounds.max_depth}
    value = {'schema': 'demiflow.terminal-segments.v1', 'height': bounds.max_depth,
             'rows': reference.count, 'bytes': bounds.max_spill_bytes,
             'children': [entry] * max(2, len(forest.roots) + 1)}
    encoded, tree = _json_shape(value, bounds.max_control_bytes)
    # Existing roots, prospective JSON tree, ASCII text and encoded bytes coexist.
    return metadata_bytes(forest.roots) + tree + 2 * encoded


def stage_literal(source, reference, *, source_id, store, bounds):
    forest = SegmentForest(store, bounds)
    index = 0
    pending = None
    from ..data.sources import ItemsSource
    from .record_schema import RecordSchemas
    schemas = RecordSchemas(store=store, bounds=bounds)
    source_rows = ((row, None) for row in source.items) if type(source) is ItemsSource else source.handle.typed_rows()
    rows_per_segment = min(1024, bounds.max_pending_events)
    # A frame's control/escaping bytes are not its business row byte allowance.
    target = max(bounds.max_batch_bytes, line_limit(bounds))
    while index < reference.count or pending is not None:
        rows = 0
        def chunks():
            nonlocal index, pending, rows
            size = 0
            while pending is not None or index < reference.count:
                if pending is None:
                    row, schema = next(source_rows)
                    schema_ref = schemas.put(schema)
                    encoded, native, _ = _typed_shape(row, bounds.max_row_bytes)
                    admit_workspace(2 * native + 512 + _forest_workspace(forest, reference, bounds), bounds)
                    record = Record(identity([source_id, 'literal', index]), {'index': index},
                                    None, row, encoded, index, schema_ref)
                    total, parts = frame_parts(record, bounds,
                        retained=_forest_workspace(forest, reference, bounds))
                    pending = total, parts
                    del record, row
                total, parts = pending
                if rows and (rows >= rows_per_segment or size + total > target):
                    return
                pending = None
                yield from parts
                size += total
                rows += 1
                index += 1
                del parts
        ref = store.put(chunks(), max_bytes=target, media_type=RECORDS_MEDIA)
        forest.append(ref, rows=rows)
    root = forest.seal()
    manifest = {'schema': SCHEMA, 'count': reference.count,
                'items_digest': reference.items_digest, 'kind': reference.kind, 'source_id': source_id,
                'data_ref': asdict(root)}
    raw = document(manifest, bounds.max_control_bytes)
    return store.put(iter((raw,)), max_bytes=bounds.max_control_bytes, media_type=MEDIA)


class LiteralRows:
    """One owned source cursor; its buffer shares the original worker grant."""
    def __init__(self, snapshot, reference, *, services, start, end, frame_call):
        self.snapshot, self.reference, self.services = snapshot, reference, services
        self.index, self.end = start, end
        self._frame_call = frame_call
        self._stream = self._segments = None
        self._index_bytes = 0
        self._metadata_bytes = 0
        self._segment_index = self._segment_end = 0
        self._closed = False

    @property
    def buffered_bytes(self):
        return self._stream.buffered_bytes if self._stream is not None else 0

    @property
    def retained_bytes(self):
        return self.buffered_bytes + self._index_bytes + self._metadata_bytes

    def _call(self, function, *args, growth=False, **kwargs):
        return self._frame_call(function, *args, source_growth=growth, **kwargs)

    def _retain_index(self, size):
        self._index_bytes = size

    def _open(self):
        bounds, store = self.services.bounds, self.services.store
        ref = self.snapshot.manifest_ref
        if ref is None or ref.media_type != MEDIA:
            raise DataflowError('CORRUPT_ARTIFACT', 'literal source has no versioned manifest')
        raw = self._call(read_document, ref, store, bounds, _maximum=bounds.max_control_bytes)
        manifest = self._call(decode_metadata, raw, bounds)
        del raw
        expected = {'schema', 'count', 'items_digest', 'kind', 'source_id', 'data_ref'}
        if (type(manifest) is not dict or set(manifest) != expected
                or manifest['schema'] != SCHEMA or manifest['source_id'] != self.snapshot.source_id
                or manifest['count'] != self.reference.count or manifest['kind'] != self.reference.kind
                or manifest['items_digest'] != self.reference.items_digest):
            raise DataflowError('SOURCE_CHANGED', 'literal input manifest differs from its original declaration')
        self._metadata_bytes = metadata_bytes(manifest)
        # The root carrier and decoded manifest coexist during conversion.
        self._call(admit_workspace, metadata_bytes(manifest['data_ref']) + 512, bounds)
        root = ArtifactRef(**manifest['data_ref'])
        del manifest
        self._metadata_bytes = metadata_bytes((root, self.reference.count, self.reference.items_digest))
        self._call(admit_workspace, 0, bounds)
        total, _ = block_totals(store, root, bounds, frame_call=self._call)
        if total != self.reference.count:
            raise DataflowError('CORRUPT_ARTIFACT', 'literal root count differs from its original declaration')
        self._segments = range_segments(store, root, bounds, start=self.index, end=self.end,
            frame_call=self._call, on_retained=self._retain_index)

    def _chunks(self, ref):
        bounds, store = self.services.bounds, self.services.store
        width = read_chunk_bytes(bounds, self.services)
        # Authenticate the immutable leaf before exposing any business input.
        for expose in (False, True):
            chunks = store.read(ref, max_bytes=max(1, ref.size_bytes), chunk_bytes=width)
            try:
                while True:
                    self._call(admit_workspace, 2 * width, bounds)
                    try:
                        chunk = next(chunks)
                    except StopIteration:
                        break
                    if expose:
                        yield chunk
                    del chunk
            finally:
                chunks.close()

    def next_row(self):
        if self._closed or self.index >= self.end:
            self.close()
            raise StopIteration
        bounds = self.services.bounds
        try:
            if self._segments is None:
                self._open()
            while True:
                if self._stream is None:
                    try:
                        ref, start, rows = next(self._segments)
                    except StopIteration as exc:
                        raise DataflowError('SOURCE_CHANGED', 'literal source ended before its sealed row count') from exc
                    self._segment_index, self._segment_end = start, start + rows
                    self._stream = frame_stream(self._chunks(ref), bounds, services=self.services,
                        check_workspace=lambda size: self._call(admit_workspace, size, bounds, growth=True))
                line = self._stream.readline(line_limit(bounds))
                if not line:
                    if self._segment_index != self._segment_end:
                        raise DataflowError('CORRUPT_ARTIFACT', 'literal segment row count differs')
                    self._stream.close()
                    self._stream = None
                    continue
                current = self._segment_index
                self._segment_index += 1
                if self._segment_index > self._segment_end:
                    raise DataflowError('CORRUPT_ARTIFACT', 'literal segment exceeds declared row count')
                if current < self.index:
                    del line
                    continue
                raw, item = self._call(decode_frame, line, bounds)
                del line
                if item != {'record_id':identity([self.snapshot.source_id, 'literal', current]), 'sequence':current}:
                    raise DataflowError('CORRUPT_ARTIFACT', 'literal row position differs')
                record = self._call(decode_document, raw, record_id=item['record_id'], sequence=current, bounds=bounds)
                del raw, item
                if thaw_record_value(record.source_position) != {'index':current}:
                    raise DataflowError('CORRUPT_ARTIFACT', 'literal source position differs')
                # Retain the already typed immutable value. The source's normal
                # Record boundary owns its final container; do not thaw/copy a
                # second business tree merely to refreeze it immediately.
                row = record.inline
                self.last_encoded_bytes = record.encoded_bytes
                self.last_schema_ref = record.schema_ref
                del record
                self.index += 1
                if self.index == self.end:
                    self.close()
                return row
        except BaseException:
            self.close()
            raise

    def close(self):
        self._closed = True
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        if self._segments is not None:
            self._segments.close()
            self._segments = None
        self._index_bytes = self._metadata_bytes = 0
