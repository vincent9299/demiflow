"""Nine frozen source readers (lane S, C04).

Each reader owns its snapshot, split, next-read cursor, read-but-not-sent refs
and a bounded decoder/FD. ``next_record`` advances at most one bounded record;
``snapshot`` reports the next read position plus pending refs (the sent frontier
lives in the channel, never here). QueueReader only talks to the driver queue
service port and never ACKs on close.
"""
from __future__ import annotations

import csv
import gzip
import json
import pickle
import itertools
from collections import deque
from pathlib import Path
from typing import Any, Iterator, Mapping

from ..data.sources import (
    DatasourceSource,
    FileSource,
    ItemsSource,
    IterableSource,
    LanceSource,
    MaterializedSource,
    RangeSource,
    SqlSource,
)
from .contracts.dataflow import (
    ArtifactRef,
    QueueBinding,
    QueueCommand,
    Record,
    RuntimeServices,
    SourceCursor,
    SourcePoll,
    SourceSnapshot,
    SourceSplit,
)
from .contracts.dataflow_errors import DataflowError
from .dataflow_sources import spool_record
from .contracts.dataflow import thaw_record_value
from .recovery_contract import identity

_RECORD_MEDIA_TYPE = "application/vnd.demiflow.record.v3"


def _load_row(ref: ArtifactRef, store: Any, max_bytes: int) -> Any:
    raw = b"".join(store.read(ref, max_bytes=max_bytes))
    from .exchange.dataflow_bridge import _decode_payload
    return _decode_payload(raw, max_bytes=max_bytes)


def _source_plan(snapshot: SourceSnapshot, services: RuntimeServices) -> Any:
    program = services.programs.get(snapshot.source_id)
    if program is None:
        raise DataflowError("INVALID_CONTRACT", "no verified source program for this occurrence")
    return program.value


def _encoded_size(row: Any, *, max_bytes: int) -> int:
    from .exchange.dataflow_bridge import _encode_payload
    return len(_encode_payload(row, max_bytes=max_bytes))


class _BaseReader:
    """Shared cursor/sequence/pending bookkeeping; subclasses read one row at a time."""

    def __init__(self, snapshot: SourceSnapshot, *, services: RuntimeServices) -> None:
        self._snapshot = snapshot
        self._services = services
        self._split: SourceSplit | None = None
        self._offset: dict[str, Any] = {}
        self._next_sequence = 0
        self._eoi = False
        self._pending: tuple[ArtifactRef, ...] = ()
        self._replay_pending = deque()
        self._pending_record = None
        self._lease_ref: ArtifactRef | None = None
        self._closed = False
        self._opened = False
        from .record_schema import RecordSchemas
        self._schemas = RecordSchemas(services)
        self._schema_ref = None

    # -- lifecycle ---------------------------------------------------------
    def open(self, split: SourceSplit, cursor: SourceCursor | None) -> None:
        if self._opened:
            raise DataflowError("INVALID_CONTRACT", "reader is already open")
        if split.source_id != self._snapshot.source_id:
            raise DataflowError("INVALID_CONTRACT", "split belongs to another source")
        if split.split_id != identity([self._snapshot.source_id, thaw_record_value(split.locator)]):
            raise DataflowError("INVALID_CONTRACT", "split_id does not match its locator")
        if cursor is not None:
            if cursor.split_id != split.split_id:
                raise DataflowError("INVALID_CONTRACT", "cursor belongs to another split")
            if self._snapshot.replay == "attempt_only":
                raise DataflowError("INVALID_CONTRACT", "attempt_only source cannot resume from a cursor")
            self._offset = thaw_record_value(cursor.offset)
            self._next_sequence = cursor.next_sequence
            self._eoi = cursor.end_of_input
            self._pending = tuple(cursor.pending_refs)
            self._replay_pending.extend(cursor.pending_refs)
            self._lease_ref = cursor.lease_ref
        else:
            self._offset = dict(split.start_cursor)
            self._next_sequence = 0
            self._eoi = False
            self._pending = ()
            self._lease_ref = None
        self._split = split
        self._setup(split)
        self._opened = True

    def next_record(self) -> SourcePoll:
        self._check_open()
        if self._replay_pending:
            from .dataflow_record_frames import read_pending_frame, decode_document
            ref = self._replay_pending.popleft()
            if ref.media_type != "application/vnd.demiflow.source-pending.v3":
                raise DataflowError("INCOMPATIBLE_HISTORY", "pending source record lacks provenance")
            if getattr(self, '_literal_rows', None) is not None:
                from .dataflow_record_frames import read_document, decode_frame, line_limit
                raw = self._literal_frame_call(read_document, ref, self._services.store,
                    self._services.bounds, _maximum=line_limit(self._services.bounds))
                encoded, saved = self._literal_frame_call(decode_frame, raw, self._services.bounds, newline=False)
                del raw
                record = self._literal_frame_call(decode_document, encoded,
                    record_id=saved['record_id'], sequence=saved['sequence'], bounds=self._services.bounds)
            else:
                encoded, saved = read_pending_frame(ref, self._services.store,
                    self._services.bounds, services=self._services)
                record = decode_document(encoded,
                    record_id=saved["record_id"], sequence=saved["sequence"],
                    bounds=self._services.bounds, services=self._services)
            # Retain the replayed item until the next successful pull confirms
            # ordered publication, just as on the original attempt.
            self._pending = (ref, *self._replay_pending)
            return SourcePoll("record", record, 0)
        if self._eoi:
            return SourcePoll("eoi", None, 0)
        # Pulling again confirms the previous record reached the ordered output.
        self._pending = ()
        self._pending_record = None
        result = self._advance()
        if result is None:
            self._eoi = True
            return SourcePoll("eoi", None, 0)
        row, position = result
        sequence = self._next_sequence
        record_id = identity([self._snapshot.source_id, self._split.split_id, position])
        from .arrow_payload import ArrowPayload
        if isinstance(row, ArrowPayload):
            record = Record(record_id, position, row.ref, None, row.encoded_bytes, sequence, self._schema_ref)
        else:
            encoded_bytes = getattr(self, '_decoded_row_bytes', None)
            self._decoded_row_bytes = None
            if encoded_bytes is None:
                encoded_bytes = _encoded_size(row, max_bytes=self._services.bounds.max_row_bytes)
            else:
                # The decoded immutable row and the source Record's frozen
                # containers/array copy briefly coexist before publication.
                from .dataflow_record_frames import admit_workspace, _typed_shape
                native = _typed_shape(row, self._services.bounds.max_row_bytes)[1]
                self._literal_frame_call(admit_workspace, 2 * native + 512, self._services.bounds)
            record = Record(record_id, position, None, row, encoded_bytes, sequence, self._schema_ref)
        if self._snapshot.replay != "attempt_only":
            # Keep one bounded in-flight row. Persist it only if a snapshot
            # precedes its downstream acknowledgement, not on every seek read.
            self._pending_record = record
        self._next_sequence += 1
        # Some immutable sources prove exhaustion from the sealed split alone.
        # Do not require a post-checkpoint pull merely to discover that the last
        # published row was already the end. Keep its pending replay reference
        # until the normal downstream acknowledgement, even with EOI known.
        if self._at_known_end():
            self._eoi = True
        return SourcePoll("record", record, 0)

    def acknowledge_output(self) -> None:
        """A quiescent cut has captured every already-published record downstream.

        Cursor admission may precede publication. Pending transfer documents are
        retained until this acknowledgement; queue claim references stay owned
        by the native queue settlement protocol.
        """
        self._check_open()
        self._pending_record = None
        self._pending = tuple(ref for ref in self._pending
            if ref.media_type != "application/vnd.demiflow.source-pending.v3")

    def snapshot(self) -> SourceCursor:
        self._check_open()
        if self._pending_record is not None:
            from .dataflow_record_frames import frame_parts
            record = self._pending_record
            if getattr(self, '_literal_rows', None) is not None:
                size, chunks = self._literal_frame_call(frame_parts, record, self._services.bounds, newline=False)
            else:
                size, chunks = frame_parts(record, self._services.bounds, services=self._services, newline=False)
            ref = self._services.store.put(chunks, max_bytes=size,
                media_type="application/vnd.demiflow.source-pending.v3")
            self._pending = (ref,)
            self._pending_record = None
        return SourceCursor(
            split_id=self._split.split_id, offset=dict(self._offset),
            next_sequence=self._next_sequence, end_of_input=self._eoi,
            lease_ref=self._lease_ref, pending_refs=self._pending,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._teardown()
        finally:
            self._pending = ()

    # -- hooks -------------------------------------------------------------
    def _setup(self, split: SourceSplit) -> None:
        raise NotImplementedError

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        raise NotImplementedError

    def _at_known_end(self) -> bool:
        # File/materialized/task iterators may authenticate/flush on exhaustion;
        # a claimed row count must never skip that actual final read.
        return False

    def _teardown(self) -> None:
        return None

    def _check_open(self) -> None:
        if self._closed:
            raise DataflowError("CANCELLED", "reader is closed")
        if not self._opened:
            raise DataflowError("INVALID_CONTRACT", "reader is not open")


class ItemsReader(_BaseReader):
    """Materialized list or Arrow/numpy/pandas block read by reference and index."""

    def _setup(self, split: SourceSplit) -> None:
        plan = _source_plan(self._snapshot, self._services)
        self._start = int(split.locator.get("start", 0))
        self._index = int(self._offset.get("index", self._start))
        self._typed_rows = False
        self._literal_rows = None
        from .literal_source import LiteralSourceReference, LiteralRows
        self._literal_items = isinstance(plan, (ItemsSource, LiteralSourceReference))
        if isinstance(plan, LiteralSourceReference) or self._snapshot.manifest_ref is not None and isinstance(plan, (ItemsSource, MaterializedSource)):
            facts = self._snapshot.immutable_identity
            count = facts.get('count') if facts['kind'] == 'ItemsSource' else facts.get('row_count')
            digest = facts.get('items_digest') if facts['kind'] == 'ItemsSource' else facts.get('handle_digest')
            kind = 'items' if facts['kind'] == 'ItemsSource' else 'materialized'
            reference = (plan if isinstance(plan, LiteralSourceReference) else
                         LiteralSourceReference(count, digest, kind=kind))
            if (reference.schema != 'demiflow.literal-source.v1'
                    or reference.count != count or reference.kind != kind
                    or reference.items_digest != digest):
                raise DataflowError('SOURCE_CHANGED', 'literal source program differs from its snapshot')
            self._end = int(split.locator.get('end', reference.count))
            if not self._start <= self._index <= self._end <= reference.count:
                raise DataflowError('INVALID_CONTRACT', 'items cursor outside split range')
            self._literal_rows = LiteralRows(self._snapshot, reference, services=self._services,
                start=self._index, end=self._end, frame_call=self._literal_frame_call)
            self._rows = self._literal_rows
            return
        if isinstance(plan, ItemsSource):
            count = len(plan.items)
            rows = iter(plan.items)
        elif isinstance(plan, MaterializedSource):
            # A materialized handle may contain many disk blocks: never turn its
            # entire row iterator into a tuple merely to seek to this split.
            count = plan.handle.row_count
            self._typed_rows = hasattr(plan.handle, 'typed_rows')
            rows = plan.handle.typed_rows() if self._typed_rows else plan.handle.rows()
        else:
            raise DataflowError("INVALID_CONTRACT", "ItemsReader requires items/materialized source")
        self._end = int(split.locator.get("end", count))
        if not self._start <= self._index <= self._end <= count:
            raise DataflowError("INVALID_CONTRACT", "items cursor outside split range")
        self._rows = rows
        self._iterator = itertools.islice(rows, self._index, self._end)

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        if self._index >= self._end:
            return None
        try:
            row = (self._literal_rows.next_row() if self._literal_rows is not None
                   else next(self._iterator))
            if self._literal_rows is not None:
                self._decoded_row_bytes = self._literal_rows.last_encoded_bytes
                self._schema_ref = self._literal_rows.last_schema_ref
        except StopIteration as exc:
            raise DataflowError("SOURCE_CHANGED", "materialized input ended before its sealed row count") from exc
        if self._typed_rows:
            row, schema = row
            self._schema_ref = self._schemas.put(schema)
        position = {"index": self._index}
        self._index += 1
        self._offset = {"index": self._index}
        return row, position

    @property
    def pending_inline_bytes(self):
        record = self._pending_record
        if self._literal_rows is None or record is None or record.inline is None:
            return 0
        from .dataflow_record_frames import _typed_shape
        return _typed_shape(record.inline, self._services.bounds.max_row_bytes)[1]

    @property
    def pending_record_bytes(self):
        record = self._pending_record
        if self._literal_rows is None or record is None:
            return 0
        from .dataflow_record_frames import _json_shape, metadata_bytes
        import sys
        return (self.pending_inline_bytes + sys.getsizeof(record) + sys.getsizeof(vars(record))
                + _json_shape(record.source_position, self._services.bounds.max_control_bytes)[1]
                + metadata_bytes((record.record_id, record.sequence, record.encoded_bytes,
                                  record.payload_ref, record.schema_ref)))

    @property
    def retained_bytes(self):
        return ((self._literal_rows.retained_bytes + self.pending_record_bytes)
                if self._literal_rows is not None else 0)

    @property
    def buffered_bytes(self):
        return self._literal_rows.buffered_bytes if self._literal_rows is not None else 0

    def _literal_frame_call(self, function, *args, source_growth=False, **kwargs):
        shared = getattr(self, '_shared_frame_call', None)
        if shared is not None:
            return shared(function, *args,
                _frame_source_reader_growth=self if source_growth else None, **kwargs)
        retained = self.retained_bytes - (self.buffered_bytes if source_growth else 0)
        if any(value is self._pending_record for value in args):
            retained -= self.pending_inline_bytes
        return function(*args, services=self._services, retained=retained, **kwargs)

    def _teardown(self) -> None:
        self._pending_record = None
        close = getattr(self._rows, "close", None)
        if close is not None:
            close()

    def _at_known_end(self) -> bool:
        return self._literal_items and self._index == self._end


class RangeReader(_BaseReader):
    """Range shard determined by split start/end; no full expansion."""

    def _setup(self, split: SourceSplit) -> None:
        plan = _source_plan(self._snapshot, self._services)
        if not isinstance(plan, RangeSource):
            raise DataflowError("INVALID_CONTRACT", "RangeReader requires a range source")
        self._start = int(split.locator.get("start", 0))
        self._end = int(split.locator.get("end", plan.count))
        value = int(self._offset.get("next", self._start))
        if value < self._start or value > self._end:
            raise DataflowError("INVALID_CONTRACT", "range cursor outside split range")
        self._value = value

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        if self._value >= self._end:
            return None
        position = {"next": self._value}
        row = {"id": self._value}
        self._value += 1
        self._offset = {"next": self._value}
        return row, position

    def _at_known_end(self) -> bool:
        return self._value == self._end


class FileReader(_BaseReader):
    """Fixed file manifest with a per-format bounded decoder and one open FD."""

    def _setup(self, split: SourceSplit) -> None:
        plan = _source_plan(self._snapshot, self._services)
        if not isinstance(plan, FileSource):
            raise DataflowError("INVALID_CONTRACT", "FileReader requires a file source")
        self._plan = plan
        self._paths = tuple(plan.paths)
        self._format = plan.format
        self._options = dict(plan.options)
        self._file_start = int(split.locator.get("file_index", 0))
        self._file_stop = min(len(self._paths), self._file_start + 1)
        self._file_index = int(self._offset.get("file_index", self._file_start))
        if not self._file_start <= self._file_index <= self._file_stop:
            raise DataflowError("INVALID_CONTRACT", "file cursor outside assigned split")
        self._record_index = int(self._offset.get("record_index", 0))
        self._iter: Iterator[Any] | None = None
        self._stream: Any = None
        if self._file_index < self._file_stop and not split.locator.get("empty"):
            self._open_file(skip=self._record_index)

    def _open_file(self, *, skip: int) -> None:
        self._close_stream()
        path = self._paths[self._file_index]
        self._iter = self._file_rows(path)
        for _ in range(skip):
            if next(self._iter, None) is None:
                break
        self._record_index = skip

    def _file_rows(self, path: str) -> Iterator[Any]:
        fmt = self._format
        options = self._options
        if fmt in ("json", "jsonl"):
            opener = gzip.open if path.endswith(".gz") else open
            with opener(path, "rb") as stream:
                prefix = stream.read(1)
                while prefix and prefix.isspace():
                    prefix = stream.read(1)
                stream.seek(0)
                if prefix == b"[":
                    import ijson
                    rows = ijson.items(stream, options.get("item_prefix") or "item", use_float=True)
                else:
                    def lines():
                        while True:
                            line = stream.readline(self._services.bounds.max_row_bytes + 1)
                            if not line:
                                return
                            if len(line) > self._services.bounds.max_row_bytes:
                                raise DataflowError("BUDGET_EXCEEDED", "JSON line exceeds max_row_bytes")
                            if line.strip():
                                yield json.loads(line)
                    rows = lines()
                for row in rows:
                    if not isinstance(row, dict):
                        raise DataflowError("INVALID_CONTRACT", "JSON Dataset requires object rows")
                    if options.get("include_paths"):
                        row = {**row, "path": path}
                    yield row
            return
        if fmt == "records":
            from ..data.records import iter_file_records

            record_format = "json" if fmt == "json" else ("text" if fmt == "text" else "jsonl")
            if fmt == "records":
                record_format = options.get("format", "jsonl")
            yield from iter_file_records(
                path, format=record_format, item_prefix=options.get("item_prefix"),
                max_records=options.get("max_records"), missing=options.get("missing", "error"),
            )
            return
        if fmt == "text":
            encoding = str(options.get("encoding", "utf-8"))
            drop_empty = bool(options.get("drop_empty_lines", True))
            opener = gzip.open if path.endswith(".gz") else open
            with opener(path, "rt", encoding=encoding) as stream:
                for line in stream:
                    text = line.rstrip("\n")
                    if text or not drop_empty:
                        yield {"text": text}
            return
        if fmt == "binary_files":
            include_paths = bool(options.get("include_paths", False))
            maximum = self._services.bounds.max_row_bytes
            with open(path, 'rb') as stream:
                data = stream.read(maximum + 1)
            if len(data) > maximum:
                raise DataflowError('BUDGET_EXCEEDED', 'Binary file exceeds max_row_bytes')
            yield {'bytes': data, **({'path': path} if include_paths else {})}
            return
        if fmt == "csv":
            if any(options.get(key) is not None for key in
                   ('read_options', 'parse_options', 'convert_options')):
                yield from self._native_csv_rows(path)
                return
            opener = gzip.open if path.endswith(".gz") else open
            with opener(path, "rt", newline="", encoding="utf-8") as stream:
                for row in csv.DictReader(stream):
                    yield {**row, 'path': path} if options.get('include_paths') else row
            return
        if fmt == "parquet":
            import pyarrow.parquet as pq

            columns = options.get("columns")
            with pq.ParquetFile(path) as parquet:
                # One row per decoded batch bounds conversion independently of
                # row-group size; oversize rows are rejected before publication.
                for batch in parquet.iter_batches(batch_size=1, columns=columns):
                    if batch.nbytes > self._services.bounds.max_row_bytes:
                        raise DataflowError("BUDGET_EXCEEDED", "Parquet row exceeds max_row_bytes")
                    self._schema_ref = self._schemas.put(batch.schema)
                    yield from batch.to_pylist()
            return
        if fmt == "images":
            yield from self._image_rows(path)
            return
        raise DataflowError("UNSUPPORTED_CAPABILITY", f"unsupported file format: {fmt}")

    def _native_csv_rows(self, path: str) -> Iterator[Any]:
        """Preserve Arrow CSV semantics with one streamed batch and one row.

        The native reader's block size controls type inference, so an explicit
        value is never silently reduced. Admit room for its input blocks before
        opening; check retained Arrow buffers before Python conversion. Arrow's
        parser/decompressor internals are not a hard process-RSS allocator.
        """
        import pyarrow as pa
        import pyarrow.csv as pacsv

        options, bounds = self._options, self._services.bounds
        read_options = options.get('read_options')
        if read_options is None:
            read_options = pacsv.ReadOptions()
        if type(read_options) is not pacsv.ReadOptions:
            raise DataflowError('INVALID_CONTRACT', 'CSV read_options must be Arrow ReadOptions')
        block_bytes = read_options.block_size
        # The source profile owns max_batch_bytes + max_row_bytes. Keep the
        # input block and lookahead together with the converted batch within
        # the first window; the yielded Python row uses the second window.
        batch_window = min(bounds.max_batch_bytes,
                           bounds.max_resident_bytes - bounds.max_row_bytes)
        if 2 * block_bytes >= batch_window:
            raise DataflowError('BUDGET_EXCEEDED',
                                'CSV input blocks leave no decoded batch budget')
        with pacsv.open_csv(path, read_options=read_options,
                            parse_options=options.get('parse_options'),
                            convert_options=options.get('convert_options')) as reader:
            schema = reader.schema
            if options.get('include_paths'):
                index = schema.get_field_index('path')
                field = pa.field('path', pa.string())
                schema = schema.set(index, field) if index >= 0 else schema.append(field)
            self._schema_ref = self._schemas.put(schema)
            while True:
                self._services.check_stop()
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    return
                try:
                    # Total buffer size includes dictionary values and parent
                    # buffers retained by slices; logical nbytes is insufficient.
                    if batch.get_total_buffer_size() + 2 * block_bytes > batch_window:
                        raise DataflowError('BUDGET_EXCEEDED',
                                            'CSV decoded buffers and input blocks exceed batch budget')
                    for index in range(batch.num_rows):
                        self._services.check_stop()
                        part = batch.slice(index, 1)
                        try:
                            if part.nbytes > bounds.max_row_bytes:
                                raise DataflowError('BUDGET_EXCEEDED', 'CSV row exceeds max_row_bytes')
                            row = part.to_pylist()[0]
                        finally:
                            del part
                        if options.get('include_paths'):
                            row['path'] = path
                        yield row
                        del row
                finally:
                    # Do not retain a previous batch while asking Arrow to
                    # allocate and decode the next one.
                    del batch

    def _image_rows(self, path: str) -> Iterator[Any]:
        from PIL import Image
        import numpy as np
        options, bounds = self._options, self._services.bounds
        maximum = int(options.get('max_pixels', 64 * 1024 * 1024))
        size, mode = options.get('size'), options.get('mode')
        if size is not None and (not isinstance(size, (tuple, list)) or len(size) != 2
                or any(type(n) is not int or n <= 0 for n in size)):
            raise DataflowError('INVALID_CONTRACT', 'Image size requires positive (height, width)')
        if mode is not None and (not isinstance(mode, str) or mode not in Image.MODES):
            raise DataflowError('INVALID_CONTRACT', 'Image mode is not supported by Pillow')
        with Image.open(path) as opened:
            width, height = opened.size
            target_height, target_width = size or (height, width)
            source_bytes = width * height * max(4, len(opened.getbands()))
            target_bytes = target_width * target_height * 4
            if max(width * height, target_width * target_height) > maximum:
                raise DataflowError('BUDGET_EXCEEDED', 'Image exceeds decode pixel budget')
            if max(source_bytes, target_bytes) > bounds.max_row_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'Decoded image exceeds max_row_bytes')
            # Input decoder, resize/conversion and ndarray/serialization copies
            # can coexist. Admit their conservative upper bound before load.
            peak = source_bytes + 4 * target_bytes
            if peak > min(bounds.max_resident_bytes, bounds.max_batch_bytes):
                raise DataflowError('BUDGET_EXCEEDED', 'Image decode/conversion copies exceed resident budget')
            opened.load()
            image, temporary = opened, []
            try:
                if image.size != (target_width, target_height):
                    image = image.resize((target_width, target_height), resample=Image.Resampling.BILINEAR)
                    temporary.append(image)
                if mode is not None and image.mode != mode:
                    image = image.convert(mode)
                    temporary.append(image)
                # Own the ndarray bytes; closing Pillow cannot retain a decoder
                # buffer or mutate a row already handed to the next stage.
                array = np.array(image, copy=True)
                yield {'image': array, **({'path': path} if options.get('include_paths', False) else {})}
            finally:
                for image in reversed(temporary):
                    image.close()

    def _close_stream(self) -> None:
        if self._stream is not None:
            try:
                self._stream.close()
            finally:
                self._stream = None
        if self._iter is not None:
            close = getattr(self._iter, "close", None)
            if close is not None:
                close()
        self._iter = None

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        while self._file_index < self._file_stop:
            if self._iter is None:
                self._open_file(skip=0)
            row = next(self._iter, None)
            if row is not None:
                position = {"file_index": self._file_index, "record_index": self._record_index}
                self._record_index += 1
                self._offset = {"file_index": self._file_index, "record_index": self._record_index}
                return row, position
            self._close_stream()
            self._file_index += 1
            self._record_index = 0
            self._offset = {"file_index": self._file_index, "record_index": 0}
        return None

    def _teardown(self) -> None:
        self._close_stream()


class LanceReader(_BaseReader):
    batch_carrier = 'original_scan_batches'

    """Fixed-version fragment/row offset; projection/filter/search keep original semantics."""

    def _setup(self, split: SourceSplit) -> None:
        plan = _source_plan(self._snapshot, self._services)
        if not isinstance(plan, LanceSource):
            raise DataflowError("INVALID_CONTRACT", "LanceReader requires a Lance source")
        self._plan = plan
        self._version = self._snapshot.immutable_identity["version"]
        if int(self._offset.get("version", self._version)) != self._version:
            raise DataflowError("INCOMPATIBLE_HISTORY", "Lance cursor differs from the pinned source version")
        self._fragment_ids = tuple(split.locator.get("fragment_ids", ()))
        self._global = bool(split.locator.get("global_query", False))
        self._arrow_cursor = None
        target_fragment = int(self._offset.get("fragment_id", 0))
        target_offset = int(self._offset.get("row_offset", 0))
        self._fragment_id = target_fragment
        self._row_offset = target_offset
        saved = self._offset.get('arrow_batch')
        self._rows = self._iter_rows()
        prefix = None
        if saved is not None:
            from .arrow_payload import ArrowBatchCache, ArrowPayload
            ref = ArtifactRef(**saved)
            cache = ArrowBatchCache(self._services.store, self._services.bounds, services=self._services)
            from .arrow_payload import ARROW_BATCH_MEDIA
            if ref.media_type != ARROW_BATCH_MEDIA:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Lance cursor does not reference a source batch')
            batch, header = cache.load(ref)
            if (header['source_id'] != self._snapshot.source_id or header['split_id'] != split.split_id
                    or header['version'] != self._version or header['fragment_id'] != target_fragment
                    or not header['start'] <= target_offset <= header['start'] + header['count']):
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Arrow cursor differs from the original source binding')
            self._schema_ref = self._schemas.put(batch.schema)
            begin = target_offset
            target_offset = header['start'] + header['count']
            prefix = ((target_fragment, offset, ArrowPayload(ref, header['sizes'][offset - header['start']]))
                      for offset in range(begin, target_offset))
            cache.close()
            del batch
        if target_fragment or target_offset:
            # Offset denotes the next unread row. Keep the matching row and
            # advance into the following fragment when the saved fragment ended.
            order = {fragment: index for index, fragment in enumerate(self._fragment_ids)}
            target_index = order.get(target_fragment, 0)
            self._rows = itertools.dropwhile(
                lambda item: (order.get(item[0], 0), item[1]) < (target_index, target_offset),
                self._rows)
        if prefix is not None:
            self._rows = itertools.chain(prefix, self._rows)

    def pinned_keys(self, field, *, positions):
        from .lance_key_source import PinnedKeyScan
        return PinnedKeyScan.open(self, field, positions=positions)

    def supports_native_batches(self):
        """Only an exact pinned table with ordinary Arrow values may skip the row codec."""
        import pyarrow as pa
        from ..lance.storage import open_lance_dataset
        from ..lance.model import LanceScanSpec
        query = self._plan.query
        if (type(query) is not LanceScanSpec or self._global or self._replay_pending
                or self._offset.get('arrow_batch') is not None
                or query.version != self._version or self._version != self._snapshot.immutable_identity['version']):
            return False
        dataset = open_lance_dataset(query.uri, self._version, query.storage_options)
        fields = [dataset.schema.field(name) for name in query.columns] if query.columns else dataset.schema
        if all(pa.types.is_integer(f.type) or pa.types.is_boolean(f.type)
               or pa.types.is_string(f.type) or pa.types.is_binary(f.type) for f in fields):
            self._native_dataset = dataset
            return True
        return False

    def _native_batches(self):
        from dataclasses import replace
        from ..lance.read import _iter_scan
        from .dataflow_native_limits import limit_value
        from .arrow_payload import _window
        query = self._plan.query
        workspace = _window(self._services.bounds, self._services)
        if workspace <= 0:
            raise DataflowError('BUDGET_EXCEEDED', 'native scan requires an admitted byte window')
        count = min(65536, workspace, self._services.bounds.max_output_rows,
                    limit_value(self._services, 'batch_rows', query.batch_size or 256))
        query = replace(query, version=self._version, batch_size=count, batch_readahead=1, fragment_readahead=1)
        dataset = self._native_dataset
        by_id = {int(f.fragment_id): f for f in dataset.get_fragments()}
        if self._next_sequence and self._fragment_id not in self._fragment_ids:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'native scan cursor names a fragment outside its split')
        first = self._fragment_ids.index(self._fragment_id) if self._next_sequence else 0
        for index, frag_id in enumerate(self._fragment_ids[first:]):
            offset = self._row_offset if index == 0 else 0
            if frag_id not in by_id:
                raise DataflowError('SOURCE_CHANGED', 'pinned source fragment disappeared')
            for batch in _iter_scan(query, dataset=dataset, batch_size=count, batch_bytes=workspace,
                                    fragments=[by_id[frag_id]], offset=offset):
                if batch.num_rows:
                    yield frag_id, offset, batch
                    offset += batch.num_rows

    def next_arrow_batch(self, max_rows, max_bytes):
        """Advance to the next unread source row only for the returned contiguous slice."""
        from .arrow_payload import _value_upper, row_upper
        self._check_open()
        if self._eoi:
            return None
        if max_rows <= 0 or max_bytes <= 0:
            raise DataflowError('INVALID_CONTRACT', 'native scan needs positive row and byte credit')
        if not hasattr(self, '_batch_scan'):
            self._batch_scan = self._native_batches()
            self._batch_remainder = None
        if self._batch_remainder is None:
            self._batch_remainder = next(self._batch_scan, None)
        if self._batch_remainder is None:
            self._eoi = True
            return None
        fragment, offset, batch = self._batch_remainder
        bounds = self._services.bounds
        if batch.get_total_buffer_size() > bounds.max_batch_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'decoded scan batch exceeds retained buffer allowance')
        size = min(max_rows, batch.num_rows, 65536)
        piece = batch.slice(0, size)
        while size > 1 and piece.nbytes > max_bytes:
            size = max(1, size // 2)
            piece = batch.slice(0, size)
        if piece.nbytes > max_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'source row exceeds unconfirmed byte allowance')
        # A whole-column upper bound cheaply proves that every row fits.
        # An ambiguous upper bound falls back to the original individual check.
        aggregate = 32 + sum(len(json.dumps(f.name).encode()) + 8 + _value_upper(col)
                             for f, col in zip(piece.schema, piece.columns))
        if aggregate > bounds.max_row_bytes:
            from .arrow_payload import fallback_row
            for index in range(size):
                if row_upper(piece, index) > bounds.max_row_bytes:
                    _encoded_size(fallback_row(piece, index), max_bytes=bounds.max_row_bytes)
        self._batch_remainder = ((fragment, offset + size, batch.slice(size))
                                 if size < batch.num_rows else None)
        self._fragment_id, self._row_offset = fragment, offset + size
        self._offset = {'version': self._version, 'fragment_id': fragment, 'row_offset': offset + size}
        self._next_sequence += size
        return piece

    def _iter_rows(self) -> Iterator[tuple[int, int, Any]]:
        from dataclasses import replace

        from ..lance.model import LanceVectorSearchSpec
        from ..lance.read import _iter_scan, _iter_vector
        from ..lance.storage import open_lance_dataset

        from .lance_key_source import LanceKeySource, key_rows
        if isinstance(self._plan, LanceKeySource):
            yield from key_rows(self)
            return
        exact = replace(self._plan.query, version=self._version)
        if hasattr(exact, 'batch_readahead'):
            # One native prefetch plus the current batch fits the same source
            # grant; do not inherit Lance's larger implicit prefetch window.
            exact = replace(exact, batch_readahead=1, fragment_readahead=1)
        batch_rows = max(1, min(256, self._services.bounds.max_batch_bytes // self._services.bounds.max_row_bytes))
        from .dataflow_native_limits import limit_value
        batch_rows = limit_value(self._services, 'batch_rows', batch_rows)
        if getattr(exact, 'batch_size', None) is not None:
            exact = replace(exact, batch_size=min(exact.batch_size, batch_rows))
        self._arrow_scan_rows = getattr(exact, 'batch_size', None) or batch_rows
        def rows(batch, fragment_id, start):
            if batch.nbytes > self._services.bounds.max_batch_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'Lance decoded batch exceeds max_batch_bytes')
            self._schema_ref = self._schemas.put(batch.schema)
            from .arrow_payload import publish_batch, ArrowPayload, fallback_row
            carrier = (publish_batch(batch, reader=self, fragment_id=fragment_id, start=start)
                       if (self.batch_carrier is not None or (batch.num_rows > 1 and
                           self._snapshot.immutable_identity.get('record_carrier_v1') == 'arrow_batch')) else None)
            if carrier is not None:
                ref, sizes = carrier
                for size in sizes:
                    yield ArrowPayload(ref, size)
                return
            for index in range(batch.num_rows):
                part = batch.slice(index, 1)
                if part.nbytes > self._services.bounds.max_row_bytes:
                    raise DataflowError('BUDGET_EXCEEDED', 'Lance row exceeds max_row_bytes')
                yield fallback_row(batch, index)
        def scan_batches(fragments=None):
            batches = _iter_scan(exact, batch_size=batch_rows, fragments=fragments)
            if self.batch_carrier == 'mapped_arrow.v1':
                # A scanner may admit just one worst-case row per fetch. Group
                # its *actual* small buffers before creating an IPC artifact.
                from .arrow_payload import _MAX_ROWS as ARROW_MAX_ROWS
                from .arrow_payload import compact_scan_batches
                target = limit_value(self._services, 'batch_rows', self._plan.query.batch_size or ARROW_MAX_ROWS)
                yield from compact_scan_batches(batches, services=self._services, target_rows=target)
            else:
                yield from batches

        if isinstance(exact, LanceVectorSearchSpec):
            index = 0
            for batch in _iter_vector(exact, batch_size=batch_rows):
                for row in rows(batch, 0, index):
                    yield 0, index, row
                    index += 1
            return
        if self._global:
            row_offset = 0
            for batch in scan_batches():
                for row in rows(batch, 0, row_offset):
                    yield 0, row_offset, row
                    row_offset += 1
            return
        dataset = open_lance_dataset(exact.uri, self._version, exact.storage_options)
        by_id = {int(fragment.fragment_id): fragment for fragment in dataset.get_fragments()}
        for fragment_id in self._fragment_ids:
            fragment = by_id.get(fragment_id)
            if fragment is None:
                raise DataflowError("SOURCE_CHANGED", f"Lance fragment missing: {fragment_id}")
            row_offset = 0
            for batch in scan_batches([fragment]):
                for row in rows(batch, fragment_id, row_offset):
                    yield fragment_id, row_offset, row
                    row_offset += 1

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        item = next(self._rows, None)
        if item is None:
            return None
        fragment_id, row_offset, row = item
        position = {"version": self._version, "fragment_id": fragment_id, "row_offset": row_offset}
        from .lance_key_source import LanceKeySource
        if isinstance(self._plan, LanceKeySource):
            row = dict(row)
            position["row_id"] = row.pop("_rowid")
        self._fragment_id = fragment_id
        self._row_offset = row_offset + 1
        self._offset = {"version": self._version, "fragment_id": fragment_id,
                        "row_offset": row_offset + 1}
        from .arrow_payload import ArrowPayload
        if isinstance(row, ArrowPayload):
            from dataclasses import asdict
            self._offset['arrow_batch'] = asdict(row.ref)
        return row, position

    def _teardown(self):
        if hasattr(self, '_batch_scan'):
            self._batch_scan.close()
            self._batch_remainder = None
        self._native_dataset = None
        rows, self._rows = self._rows, iter(())
        close = getattr(rows, 'close', None)
        if close is not None:
            close()


class SqlReader(_BaseReader):
    """Fixed snapshot + stable keyset; otherwise the snapshot declares durable spool."""

    def _setup(self, split: SourceSplit) -> None:
        plan = _source_plan(self._snapshot, self._services)
        if not isinstance(plan, SqlSource):
            raise DataflowError("INVALID_CONTRACT", "SqlReader requires a SQL source")
        self._plan = plan
        self._snapshot_id = self._offset.get("snapshot_id")
        self._keyset = self._snapshot.immutable_identity.get("keyset")
        self._last_key = self._offset.get("last_key")
        self._connection = None
        self._cursor = None
        self._columns: tuple[str, ...] = ()
        self._spooled: Iterator[Any] | None = None
        self._spool_index = 0
        if self._snapshot.replay == "durable_spool":
            self._open_spool()
        else:
            self._open_cursor()

    def _open_spool(self) -> None:
        from .dataflow_handles import _ChunkStream
        from .exchange.dataflow_bridge import _encode_payload, _decode_payload
        store, bounds = self._services.store, self._services.bounds
        spool = self._offset.get("spool")
        media_type = "application/vnd.demiflow.sql-rows.v3+ndjson"
        if spool is None:
            def chunks():
                rows = self._capture()
                try:
                    for row in rows:
                        self._services.check_stop()
                        yield _encode_payload(row, max_bytes=bounds.max_row_bytes) + b'\n'
                finally:
                    rows.close()
            # put reserves the disk allowance before the generator queries SQL.
            # It charges only published bytes and releases a failed capture.
            ref = store.put(chunks(), max_bytes=bounds.max_spill_bytes, media_type=media_type)
            self._offset = {**self._offset, "spool": {
                "digest": ref.digest, "size_bytes": ref.size_bytes, "store_id": ref.store_id,
                "key": ref.key, "media_type": ref.media_type}}
        else:
            ref = ArtifactRef(**dict(spool))
        self._spool_index = int(self._offset.get("last_key") or 0)
        if ref.media_type == "application/vnd.demiflow.sql-spool.v3":
            # Already sealed legacy images keep their identity. Admit their
            # whole-object format before decoding; new captures never use it.
            raw = b''.join(store.read(ref, max_bytes=bounds.max_batch_bytes))
            self._spooled = iter(pickle.loads(raw))
        elif ref.media_type == media_type:
            stream = _ChunkStream(store.read(ref, max_bytes=bounds.max_spill_bytes))
            def rows():
                try:
                    while True:
                        raw = stream.readline(bounds.max_row_bytes + 1)
                        if not raw:
                            return
                        if not raw.endswith(b'\n'):
                            raise DataflowError('CORRUPT_ARTIFACT', 'SQL spool row is incomplete')
                        yield _decode_payload(raw[:-1], max_bytes=bounds.max_row_bytes)
                finally:
                    stream.close()
            self._spooled = rows()
        else:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Unknown SQL spool encoding')
        for _ in range(self._spool_index):
            try:
                next(self._spooled)
            except StopIteration:
                raise DataflowError('CORRUPT_ARTIFACT', 'SQL cursor exceeds its sealed spool')

    def _connect(self) -> Any:
        return self._plan.connection_factory()

    def _capture(self) -> Iterator[Any]:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            params = self._plan.options.get("sql_params") or ()
            cursor.execute(self._plan.sql, params)
            names = tuple(item[0] for item in cursor.description)
            for values in cursor:
                yield dict(zip(names, values))
        finally:
            connection.close()

    def _open_cursor(self) -> None:
        self._connection = self._connect()
        self._cursor = self._connection.cursor()
        params = self._plan.options.get("sql_params") or ()
        if self._keyset and self._last_key is not None:
            column = self._keyset[0]
            sql = f"SELECT * FROM ({self._plan.sql}) AS _snapshot WHERE {column} > ? ORDER BY {column}"
            self._cursor.execute(sql, (*params, self._last_key))
        else:
            self._cursor.execute(self._plan.sql, params)
        self._columns = tuple(item[0] for item in self._cursor.description)

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        if self._spooled is not None:
            try:
                row = next(self._spooled)
            except StopIteration:
                return None
            position = {"snapshot_id": self._snapshot_id, "partition": "0",
                        "last_key": self._spool_index}
            self._spool_index += 1
            self._offset = {**self._offset, "last_key": self._spool_index}
            return row, position
        values = self._cursor.fetchone()
        if values is None:
            return None
        row = dict(zip(self._columns, values))
        last_key = row.get(self._keyset[0]) if self._keyset else None
        position = {"snapshot_id": self._snapshot_id, "partition": "0", "last_key": last_key}
        self._last_key = last_key
        self._offset = dict(position)
        return row, position

    def _teardown(self) -> None:
        if self._spooled is not None:
            close = getattr(self._spooled, 'close', None)
            if close is not None:
                close()
            self._spooled = None
        if self._cursor is not None:
            try:
                self._cursor.close()
            finally:
                self._cursor = None
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None


class TaskReader(_BaseReader):
    """Bounded execution of an existing Datasource/ReadTask with a declared cursor."""

    def _setup(self, split: SourceSplit) -> None:
        plan = _source_plan(self._snapshot, self._services)
        if not isinstance(plan, DatasourceSource):
            raise DataflowError("INVALID_CONTRACT", "TaskReader requires a datasource source")
        self._plan = plan
        task_index = int(split.locator.get("task_index", 0))
        tasks = plan.datasource.get_read_tasks(1)
        if split.locator.get("empty"):
            self._rows = iter(())
        else:
            if task_index >= len(tasks):
                raise DataflowError("INVALID_CONTRACT", "task index outside datasource plan")
            self._rows = self._task_rows(tasks[task_index])
        self._index = int(self._offset.get("index", 0))
        for _ in range(self._index):
            if next(self._rows, None) is None:
                break

    def _task_rows(self, task: Any) -> Iterator[Any]:
        for block in task.read_fn():
            yield from _block_rows(block)

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        row = next(self._rows, None)
        if row is None:
            return None
        position = {"index": self._index}
        self._index += 1
        self._offset = {"index": self._index}
        return row, position


class IteratorReader(_BaseReader):
    """Zero-arg iterator factory invoked on the worker; one-shot unless re-openable."""

    def _setup(self, split: SourceSplit) -> None:
        plan = _source_plan(self._snapshot, self._services)
        if not isinstance(plan, IterableSource):
            raise DataflowError("INVALID_CONTRACT", "IteratorReader requires an iterable source")
        _precheck_factory(plan.factory)
        self._iterator = iter(plan.factory())
        self._index = int(self._offset.get("index", 0))
        self._generator_closed = False
        for _ in range(self._index):
            if next(self._iterator, None) is None:
                break

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        row = next(self._iterator, None)
        if row is None:
            return None
        position = {"index": self._index}
        self._index += 1
        self._offset = {"index": self._index}
        return row, position

    def _teardown(self) -> None:
        if not self._generator_closed:
            self._generator_closed = True
            close = getattr(self._iterator, "close", None)
            if callable(close):
                close()


def _precheck_factory(factory: Any) -> None:
    closure = getattr(factory, "__closure__", None) or ()
    for cell in closure:
        try:
            value = cell.cell_contents
        except ValueError:
            continue
        module = type(value).__module__
        if module.startswith("demiflow.execution.executors") or hasattr(value, "_executor"):
            raise DataflowError(
                "INVALID_CONTRACT",
                "iterator factory captures a live executor; move construction into the worker",
            )


class QueueReader(_BaseReader):
    """Claims only through the driver queue service port; close never ACKs."""

    def _setup(self, split: SourceSplit) -> None:
        facts = self._services.journal.read_configuration()
        self._binding = QueueBinding(**dict(facts['queue_binding']))
        if (self._binding.action_id != self._services.assignment.action_id or
                self._binding.source_occurrence != self._snapshot.source_id):
            raise DataflowError('FENCED', 'Queue reader differs from the driver-sealed source')
        self._record_index = int(self._offset.get("record_index", 0))
        self._claim_ref = self._offset.get("claim_ref", "")
        self._delivery_ref: ArtifactRef | None = None

    def next_record(self) -> SourcePoll:
        self._check_open()
        if self._replay_pending:
            # Re-deliver the already captured record, without claiming another
            # task. This restores provenance only; the driver still owns lease
            # validation before any acquire/finalize operation.
            poll = super().next_record()
            encoded = poll.record.source_position.get("queue_delivery_ref")
            if not isinstance(encoded, str):
                raise DataflowError("INCOMPATIBLE_HISTORY", "pending queue record lacks delivery provenance")
            try:
                delivery = ArtifactRef(**json.loads(encoded))
            except (TypeError, ValueError) as exc:
                raise DataflowError("INCOMPATIBLE_HISTORY", "pending queue delivery provenance is invalid") from exc
            if self._lease_ref != delivery:
                raise DataflowError("INCOMPATIBLE_HISTORY", "pending queue delivery differs from cursor lease")
            self._delivery_ref = delivery
            return poll
        if self._eoi:
            return SourcePoll("eoi", None, 0)
        self._pending = ()
        # Match _BaseReader's ordered-publication acknowledgement. The native
        # lease remains held independently; pending/eoi must not reseal a row
        # already handed off before this pull.
        self._pending_record = None
        from .dataflow_queue import queue_service_request

        operation_id = identity([
            self._binding.action_id, self._snapshot.source_id, self._record_index,
            self._claim_ref, "poll",
        ])
        command = QueueCommand(
            operation_id=operation_id, operation="poll", binding=self._binding,
            record_id=None, delivery_ref=None, row_ref=None,
        )
        reply = queue_service_request(command, services=self._services)
        if reply.status == "pending":
            return SourcePoll("pending", None, max(1, reply.retry_after_ms))
        if reply.status == "eoi":
            self._eoi = True
            return SourcePoll("eoi", None, 0)
        if reply.status != "record" or reply.row_ref is None:
            raise DataflowError("INTERNAL_PROTOCOL", f"unexpected queue reply: {reply.status}")
        from dataclasses import asdict
        if reply.delivery_ref is None:
            raise DataflowError('FENCED', 'Queue delivery lacks native ownership')
        row = json.loads(b''.join(self._services.store.read(reply.row_ref,
                         max_bytes=self._services.bounds.max_row_bytes)))
        position = {"claim_ref": reply.delivery_ref.key,
                    "queue_delivery_ref": json.dumps(asdict(reply.delivery_ref), sort_keys=True),
                    "record_index": self._record_index}
        sequence = self._next_sequence
        record_id = identity([self._snapshot.source_id, self._split.split_id, position])
        record = Record(record_id, position, None, row,
                        _encoded_size(row, max_bytes=self._services.bounds.max_row_bytes), sequence)
        self._delivery_ref = reply.delivery_ref
        self._claim_ref = position["claim_ref"]
        self._record_index += 1
        self._next_sequence += 1
        self._offset = {"claim_ref": self._claim_ref, "record_index": self._record_index}
        self._pending_record = record
        self._lease_ref = reply.delivery_ref
        return SourcePoll("record", record, 0)

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        raise DataflowError("INTERNAL_PROTOCOL", "QueueReader uses its own poll loop")


class ProducerReader(QueueReader):
    """Fixed native source enumeration; transformations run in graph workers."""

    def _setup(self, split):
        super()._setup(split)
        self._offset.setdefault('record_index', self._record_index)

    def next_record(self):
        from .dataflow_records import clone_record
        from .contracts.dataflow_codec import _thaw_json
        import json
        replaying = bool(self._replay_pending)
        poll = super().next_record()
        if poll.status != 'record' or replaying:
            # Producer provenance and next cursor were already sealed together.
            # Rewriting them would advance the producer a second time.
            return poll
        descriptor = json.loads(b''.join(self._services.store.read(self._delivery_ref,
            max_bytes=self._services.bounds.max_control_bytes)))
        self._offset.update(producer_position=descriptor['position'] + 1,
                            producer_cursor=descriptor['next_cursor'])
        position = _thaw_json(poll.record.source_position)
        position.update(queue_delivery_kind='producer', producer_unit=descriptor['position'])
        record = clone_record(poll.record, record_id=descriptor['occurrence'], source_position=position)
        self._pending_record = record
        return SourcePoll('record', record, 0)


class HistoryReader(_BaseReader):
    """Fixed historical queue/legacy/document receipts, read-only; no claim or fetch."""

    def _setup(self, split: SourceSplit) -> None:
        plan = _source_plan(self._snapshot, self._services)
        if not isinstance(plan, DatasourceSource):
            raise DataflowError("INVALID_CONTRACT", "HistoryReader requires a history datasource")
        self._plan = plan
        task_index = int(split.locator.get("task_index", 0))
        if split.locator.get("empty"):
            self._rows = iter(())
        else:
            tasks = plan.datasource.get_read_tasks(1)
            if task_index >= len(tasks):
                raise DataflowError("INVALID_CONTRACT", "history task index outside plan")
            self._rows = self._task_rows(tasks[task_index])
        self._index = int(self._offset.get("index", 0))
        for _ in range(self._index):
            if next(self._rows, None) is None:
                break

    def _task_rows(self, task: Any) -> Iterator[Any]:
        for block in task.read_fn():
            yield from _block_rows(block)

    def _advance(self) -> tuple[Any, dict[str, Any]] | None:
        row = next(self._rows, None)
        if row is None:
            return None
        position = {"index": self._index}
        self._index += 1
        self._offset = {"index": self._index}
        return row, position


def _block_rows(block: Any) -> Iterator[Any]:
    if isinstance(block, Mapping):
        names = tuple(block)
        if not names:
            return
        columns = [block[name] for name in names]
        lengths = {len(column) for column in columns}
        if len(lengths) != 1:
            raise DataflowError("INVALID_CONTRACT", "block columns must have equal lengths")
        for index in range(lengths.pop()):
            yield {name: column[index] for name, column in zip(names, columns)}
        return
    if hasattr(block, "to_pylist"):
        yield from block.to_pylist()
        return
    if hasattr(block, "to_dict"):
        yield from block.to_dict(orient="records")
        return
    if isinstance(block, (list, tuple)):
        yield from block
        return
    yield block


__all__ = [
    "ItemsReader", "RangeReader", "FileReader", "LanceReader", "SqlReader",
    "TaskReader", "IteratorReader", "QueueReader", "HistoryReader",
]
