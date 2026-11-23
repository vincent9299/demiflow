"""Native terminal logical blocks backed by incremental artifact forests."""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
from dataclasses import asdict
from collections.abc import Mapping
from contextlib import contextmanager

from demiflow.execution.contracts.dataflow import BlockPartition
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.storage.scratch import ScratchStream
from demiflow.execution.storage.segments import SegmentForest, block_chunks, RECORDS_MEDIA
from demiflow.execution.recovery_contract import identity


def declared_blocks(graph, terminal, bounds):
    """Infer only an explicitly declared layout through preserving unary nodes."""
    nodes = {node.occurrence_id: node for node in graph.nodes}
    incoming = {}
    for edge in graph.edges:
        incoming.setdefault(edge.consumer, []).append(edge.producer)
    from demiflow.execution.contracts.dataflow_kinds import kinds_with
    preserving = kinds_with('keeps_layout')
    current = terminal
    reorder = None
    seen = set()
    while len(incoming.get(current, ())) == 1:
        current = incoming[current][0]
        if current in seen or len(seen) > bounds.max_depth:
            raise DataflowError('INVALID_CONTRACT', 'cyclic terminal block declaration')
        seen.add(current)
        node = nodes[current]
        if node.kernel.kind not in preserving:
            return None
        if node.kernel.kind == 'RandomizeBlockOrderOp' and reorder is None:
            reorder = current
        if node.kernel.kind in ('RepartitionOp', 'RandomShuffleOp'):
            count = node.kernel.configuration.get('num_blocks')
            if count is not None:
                if type(count) is not int or not 1 <= count <= bounds.max_partitions:
                    raise DataflowError('BUDGET_EXCEEDED', 'declared logical block count exceeds bound')
                return reorder or current, count
            return None
    return None


class TerminalBlocks:
    """At most one scratch FD is open; block switches seal only new tail bytes."""
    def __init__(self, services, partition_id, *, declaration=None, partitioned=True, budget=None, frame_call=None,
                 coalesce_allowed=None):
        self.services, self.bounds = services, services.bounds
        self.partition_id, self.partitioned = partition_id, partitioned
        self._reference_only = False
        self._workspace_limit = None
        self._frame_call = frame_call
        self._coalesce_allowed = coalesce_allowed
        self._pending_resident = 0
        self._metadata_owners = []
        self.columns = None
        stages = getattr(getattr(services, 'assignment', None), 'stages', ())
        granted = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) for s in stages) if stages else self.bounds.max_resident_bytes
        from demiflow.execution.scheduler.dataflow_publication_budget import MIN_TERMINAL_INDEX, MIN_TERMINAL_WORKSPACE, TERMINAL_WORKSPACE
        if isinstance(budget, Mapping) and budget.get('schema') == TERMINAL_WORKSPACE:
            if (set(budget) != {'schema', 'resident_bytes'}
                    or type(budget['resident_bytes']) is not int
                    or not MIN_TERMINAL_WORKSPACE <= budget['resident_bytes'] <= granted):
                raise DataflowError('INVALID_CONTRACT', 'invalid bounded terminal workspace')
            self._workspace_limit = budget['resident_bytes']
            self._metadata_limit = min(self.bounds.max_state_bytes, self._workspace_limit)
        elif budget is None:
            # Historical sealed plans and finite-channel helpers keep their
            # original window; a new compiler never rewrites their grants.
            self._metadata_limit = min(self.bounds.max_state_bytes, granted // 2)
        else:
            from demiflow.execution.scheduler.dataflow_publication_budget import REF_CARRIER
            self._reference_only = isinstance(budget, Mapping) and budget.get('carrier') == REF_CARRIER
            fields = {'payload_bytes', 'metadata_bytes'} | ({'carrier'} if self._reference_only else set())
            if (not isinstance(budget, Mapping) or set(budget) != fields
                    or any(type(budget[k]) is not int for k in ('payload_bytes', 'metadata_bytes'))):
                raise DataflowError('INVALID_CONTRACT', 'invalid sealed terminal memory windows')
            payload, metadata = budget['payload_bytes'], budget['metadata_bytes']
            # New plans may reserve a larger codec workspace within the same
            # grant; historical plans retain their original batch/row window.
            minimum = 4096 if self._reference_only else self.bounds.max_batch_bytes + self.bounds.max_row_bytes
            if (payload < minimum
                    or not MIN_TERMINAL_INDEX <= metadata <= self.bounds.max_state_bytes
                    or payload + metadata > granted):
                raise DataflowError('RESOURCE_INFEASIBLE', 'terminal payload and metadata exceed the original grant')
            self._metadata_limit = metadata
        self.blocks = {}
        self._layouts = set()
        self.rows = self.bytes = 0
        self._tail = None
        self._tail_key = None
        self._tail_rows = self._tail_bytes = 0
        if declaration is not None and partitioned:
            self._layouts.add(declaration)
            occurrence, count = declaration
            for index in range(count):
                self._block(BlockPartition(occurrence, index, count))

    @property
    def retained_bytes(self):
        if self._workspace_limit is None:
            return 0  # Historical windows keep their original interpretation.
        return self._retained_metadata()

    def _retained_metadata(self, extra=()):
        from demiflow.execution.contracts.record_codec import metadata_bytes
        # The fixed charge covers each block/forest and its lookup/layout
        # containers. Count the actual variable trees in one traversal: a
        # restored manifest, its columns and forest leaves may alias strings.
        values = [self.columns, *self._metadata_owners, *extra]
        for tag, forest in self.blocks.values():
            values.extend((forest.roots, forest._sealed))
            if tag is not None:
                values.append(tag.occurrence_id)
        return len(self.blocks) * 1024 + metadata_bytes(values)

    @contextmanager
    def hold_metadata(self, value):
        if self._workspace_limit is None:
            yield
            return
        self._check_workspace(metadata=self._retained_metadata((value,)))
        self._metadata_owners.append(value)
        try:
            yield
        finally:
            self._metadata_owners.pop()

    def retain_columns(self, columns):
        if self._workspace_limit is not None:
            self._check_workspace(metadata=self._retained_metadata((columns,)))
        self.columns = columns

    def manifest_chunks(self, manifest):
        from demiflow.execution.recovery_contract import document
        if self._workspace_limit is None:
            raw = document(manifest, self.bounds.max_control_bytes)
            return len(raw), iter((raw,))
        import json
        from demiflow.execution.contracts.record_codec import _json_shape, _string_size
        count = [0]
        size, _ = _json_shape(manifest, self.bounds.max_control_bytes, count=count)
        def token_size(value):
            if isinstance(value, Mapping):
                return max((max(_string_size(k), token_size(v)) for k, v in value.items()), default=2)
            if type(value) in (list, tuple):
                return max((token_size(v) for v in value), default=2)
            if type(value) is float:
                import math
                if not math.isfinite(value):
                    from demiflow.execution.recovery_contract import RecoveryContractError
                    raise RecoveryContractError('Nonfinite execution metadata')
            return _json_shape(value, self.bounds.max_control_bytes)[0]
        # No complete output string/list of chunks is constructed. Charge the
        # JSON iterator's sorted-key/recursion state, current Unicode fragment,
        # UTF-8 fragment and the writer's previous fragment before advancing.
        workspace = 8 * token_size(manifest) + 64 * count[0] + 1024
        self._check_workspace(workspace)
        def chunks():
            encoder = json.JSONEncoder(sort_keys=True, ensure_ascii=False, allow_nan=False,
                                       separators=(',', ':')).iterencode(manifest)
            total = 0
            try:
                while True:
                    self._check_workspace(workspace)
                    try:
                        part = next(encoder)
                    except StopIteration:
                        break
                    raw = part.encode('utf-8')
                    total += len(raw)
                    if total > size:
                        raise DataflowError('INVALID_CONTRACT', 'terminal manifest changed while encoding')
                    yield raw
                    del part, raw
                if total != size:
                    raise DataflowError('INVALID_CONTRACT', 'terminal manifest differs from measured size')
            finally:
                encoder.close()
        return size, chunks()

    def _check_workspace(self, extra=0, *, metadata=None):
        if self._workspace_limit is None:
            return
        from demiflow.execution.contracts.record_codec import admit_workspace
        current = self.retained_bytes
        peak = (current if metadata is None else metadata) + self._pending_resident + extra
        if peak > self._workspace_limit:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal index, row and scratch exceed sealed workspace')
        if self._frame_call is not None:
            # Replace the index already included by the shared frame owner;
            # peer buffers, queued commands and readers remain charged there.
            self._frame_call(admit_workspace, peak, self.bounds, _frame_retained=-current)
        else:
            admit_workspace(peak, self.bounds, resident_bytes=stage_resident_bytes(getattr(self.services, 'assignment', None)))

    def _admit_metadata(self, *, blocks=0, roots=0):
        # Conservative managed-object charge, in addition to payload/scratch.
        used = (len(self.blocks) + blocks) * 1024
        used += (sum(len(f.roots) for _, f in self.blocks.values()) + roots) * 2048
        if used > self._metadata_limit:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal block index exceeds its admitted state window')
        self._check_workspace(metadata=self.retained_bytes + blocks * 1024 + max(0, roots) * 2048)

    def _index_call(self, function, *args, _frame_retained=0, **kwargs):
        retained = self._pending_resident + _frame_retained
        if self._frame_call is not None:
            return self._frame_call(function, *args, _frame_retained=retained, **kwargs)
        return function(*args, resident_bytes=stage_resident_bytes(getattr(self.services, 'assignment', None)),
                        retained=self.retained_bytes + retained, **kwargs)

    def _block(self, tag):
        key = (tag.occurrence_id, tag.index) if tag is not None else (self.partition_id, -1)
        if key not in self.blocks:
            if len(self.blocks) >= self.bounds.max_partitions:
                raise DataflowError('BUDGET_EXCEEDED', 'terminal logical blocks exceed max_partitions')
            self._admit_metadata(blocks=1)
            self.blocks[key] = (tag, SegmentForest(self.services.store, self.bounds,
                frame_call=self._index_call if self._workspace_limit is not None else None,
                metadata_context=self.hold_metadata if self._workspace_limit is not None else None))
        elif self.blocks[key][0] != tag:
            raise DataflowError('INVALID_CONTRACT', 'logical block count changed within an action')
        return key

    def append_arrow_batch(self, batch):
        """Retain one native batch in the same checked forest as the checkpoint."""
        import pyarrow as pa
        from demiflow.execution.storage.segments import ARROW_STREAM_MEDIA
        rows = batch.num_rows
        size = pa.ipc.get_record_batch_size(batch) + len(batch.schema.serialize()) + 8
        if (size > self.bounds.max_batch_bytes or self.rows + rows > self.bounds.max_output_rows
                or self.bytes + size > self.bounds.max_spill_bytes):
            raise DataflowError('BUDGET_EXCEEDED', 'terminal Arrow batch exceeds output allowance')
        self._admit_metadata(roots=1)
        self._check_workspace(batch.get_total_buffer_size() + size * 2 + 65536)
        # Exact-size buffer avoids an unbudgeted geometric growth during IPC write.
        encoded = pa.allocate_buffer(size)
        with pa.FixedSizeBufferWriter(encoded) as stream:
            with pa.ipc.new_stream(stream, batch.schema) as writer:
                writer.write_batch(batch)
            size = stream.tell()
        chunks = (encoded.slice(start, min(65536, size - start)).to_pybytes()
                  for start in range(0, size, 65536))
        ref = self.services.store.put(chunks, max_bytes=size, media_type=ARROW_STREAM_MEDIA)
        key = self._block(None)
        self.blocks[key][1].append(ref, rows=rows)
        self.rows += rows
        self.bytes += size
        return ref

    def append(self, record, raw):
        self.append_parts(record, iter((raw,)), size=len(raw))

    def append_parts(self, record, chunks, *, size):
        if self._workspace_limit is not None:
            from demiflow.execution.contracts.record_codec import _typed_shape
            # frame_parts retains the original encoded document and its
            # bounded escaping/suffix buffers while a block switch flushes.
            native = _typed_shape(record.inline, self.bounds.max_row_bytes)[1] if record.inline is not None else 0
            self._pending_resident = native + size + 4096
        try:
            self._append_parts(record, chunks, size=size)
        finally:
            self._pending_resident = 0
            close = getattr(chunks, 'close', None)
            if close is not None:
                close()

    def _append_parts(self, record, chunks, *, size):
        if self._reference_only:
            ref = record.payload_ref
            if (record.inline is not None or ref is None or ref.media_type != 'application/octet-stream'
                    or ref.size_bytes != record.encoded_bytes
                    or not 0 <= record.encoded_bytes <= self.bounds.max_row_bytes):
                raise DataflowError('INVALID_CONTRACT', 'compact terminal requires its sealed stored row carrier')
        tag = record.block_partition if self.partitioned else None
        # A concrete count also creates empty blocks. No fake business rows.
        if tag is not None and (tag.occurrence_id, tag.count) not in self._layouts:
            if tag.count > self.bounds.max_partitions:
                raise DataflowError('BUDGET_EXCEEDED', 'logical block count exceeds max_partitions')
            self._layouts.add((tag.occurrence_id, tag.count))
            for index in range(tag.count):
                self._block(BlockPartition(tag.occurrence_id, index, tag.count))
        key = self._block(tag)
        self._check_workspace()
        if self._tail_key != key:
            self.flush()
            self._tail_key = key
        if self._tail is None:
            self._tail = ScratchStream(self.services.store, max_bytes=self.bounds.max_spill_bytes,
                max_io_bytes=self.bounds.max_row_bytes + self.bounds.max_control_bytes)
        if self.bytes + size > self.bounds.max_spill_bytes or self.rows >= self.bounds.max_output_rows:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal output exceeds disk/row budget')
        from demiflow.execution.contracts.record_codec import _RecordFrameParts
        joined = (chunks.coalesce(record, self.bounds, size=size, max_write_bytes=self._tail._io,
                                 admit=self._coalescing_fits)
                  if (type(chunks) is _RecordFrameParts
                      and (self._coalesce_allowed is None or self._coalesce_allowed())) else None)
        written = 0
        if joined is not None:
            try:
                self._tail.write(joined)
                written = len(joined)
            finally:
                # Do not add our own full-row reference to an I/O traceback.
                # The original scratch writer still owns its failure frames.
                del joined
        else:
            for raw in chunks:
                if written + len(raw) > size:
                    raise DataflowError('CORRUPT_ARTIFACT', 'record frame exceeds measured size')
                self._tail.write(raw)
                written += len(raw)
        if written != size:
            raise DataflowError('CORRUPT_ARTIFACT', 'record frame differs from measured size')
        self._tail_rows += 1
        self._tail_bytes += size
        self.rows += 1
        self.bytes += size

    def _coalescing_fits(self, workspace, record):
        from demiflow.execution.contracts.record_codec import resident_limit
        # Called after a possible old-tail flush. The proposed workspace
        # includes this row/raw and replaces _pending_resident, not adds to it.
        # Old metadata/payload windows remain unchanged on the fallback path.
        try:
            metadata = self._retained_metadata()
        except DataflowError as error:
            if error.code == 'BUDGET_EXCEEDED':
                return False  # Optional sizing never replaces legacy admission.
            raise
        peak = workspace + metadata
        if self._workspace_limit is not None and peak > self._workspace_limit:
            return False
        if self._frame_call is not None:
            def fits(record, size, bounds, *, resident_bytes, retained):
                return retained + size <= resident_limit(bounds, resident_bytes)
            # The shared owner already counts this terminal's workspace
            # metadata. Finite spools are distinct owners and count theirs here.
            return self._frame_call(fits, record, peak - self.retained_bytes, self.bounds)
        return peak <= resident_limit(self.bounds, stage_resident_bytes(getattr(self.services, 'assignment', None)))

    def flush(self):
        if self._tail is None:
            return
        forest = self.blocks[self._tail_key][1]
        height, merged = 0, 0
        for entry in reversed(forest.roots):
            if entry['height'] != height:
                break
            height, merged = height + 1, merged + 1
        self._admit_metadata(roots=1-merged)
        # The artifact writer retains the preceding chunk/view while asking
        # for the next. Both chunks share the original batch window; a pending
        # terminal append occupies at most max_row_bytes plus its original
        # newline. Reserve that existing byte without shrinking valid rows.
        batch_window = self.bounds.max_batch_bytes - (1 if self.partitioned else 0)
        chunk_bytes = min(65536, batch_window // 2,
                          self.bounds.max_row_bytes + self.bounds.max_control_bytes)
        if chunk_bytes < 1:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal batch window cannot hold consecutive scratch chunks')
        if self._workspace_limit is not None:
            available = self._workspace_limit - self.retained_bytes - self._pending_resident - 512
            if available < 2:
                raise DataflowError('BUDGET_EXCEEDED', 'terminal workspace cannot hold scratch chunks')
            chunk_bytes = min(chunk_bytes, available // 2)
        stream = self._tail
        stream.flush()
        stream.seek(0)
        def chunks():
            while True:
                self._check_workspace(2 * chunk_bytes + 512)
                chunk = stream.read(chunk_bytes)
                if not chunk:
                    break
                yield chunk
        self._check_workspace(metadata=self.retained_bytes + 2048)
        ref = self.services.store.put(chunks(), max_bytes=max(1, self._tail_bytes), media_type=RECORDS_MEDIA)
        self.blocks[self._tail_key][1].append(ref, rows=self._tail_rows)
        stream.close()
        self._tail = None
        self._tail_rows = self._tail_bytes = 0

    def manifest_partitions(self):
        self.flush()
        if not self.blocks:
            self._block(None)
        self._check_workspace(64 + 32 * len(self.blocks))
        keys = sorted(self.blocks)
        out = []
        with self.hold_metadata(keys), self.hold_metadata(out):
            for key in keys:
                tag, forest = self.blocks[key]
                ref = forest.seal()
                # Dict/list growth and a copied ArtifactRef descriptor, before
                # asdict/identity construct them. Immutable strings are shared.
                self._check_workspace(2048)
                partition = {'partition_id': identity(['terminal-block-v1', self.partition_id, asdict(tag)]) if tag else self.partition_id,
                             'rows': forest.rows, 'bytes': forest.bytes, 'data_ref': asdict(ref)}
                if tag is not None:
                    partition['block_partition'] = asdict(tag)
                out.append(partition)
        return out

    def restore(self, manifest):
        from demiflow.execution.contracts.dataflow import ArtifactRef
        with self.hold_metadata(manifest):
            self.blocks.clear()
            self._layouts.clear()
            for part in manifest.get('partitions', [manifest]):
                tag = BlockPartition(**part['block_partition']) if 'block_partition' in part else None
                key = self._block(tag)
                self.blocks[key][1].restore(ArtifactRef(**part['data_ref']), rows=part['rows'], size=part['bytes'])
                self._check_workspace()
            self._admit_metadata()
            self.rows, self.bytes = manifest['rows'], manifest['bytes']
            if sum(f.rows for _, f in self.blocks.values()) != self.rows or sum(f.bytes for _, f in self.blocks.values()) != self.bytes:
                raise DataflowError('CORRUPT_ARTIFACT', 'terminal partition totals disagree with manifest')

    def scalar_workspace(self, *, resident_bytes, retained):
        # The sealed index reservation and any previous worker-side Arrow
        # parent are still live beside this scalar's input and result.
        from demiflow.execution.contracts.record_codec import resident_limit
        memory = resident_limit(self.bounds, resident_bytes) - self._metadata_limit - retained
        cached = getattr(self.services.store, '_arrow_payload_cache', None)
        if cached is not None and cached.ref is not None:
            from demiflow.execution.contracts.record_codec import metadata_bytes
            memory -= cached.ref.size_bytes + cached.batch.get_total_buffer_size() + metadata_bytes(cached.header)
        return memory

    def records(self, partitions, *, codec_call):
        """Replay the exact sealed prefix. The consumer shares its retained
        block/state charge with segment traversal, physical reads and decoding.
        """
        from demiflow.execution.contracts.dataflow import ArtifactRef
        from demiflow.execution.contracts.record_codec import admit_workspace, metadata_bytes, raw_workspace
        from demiflow.execution.contracts.record_codec import _decode_payload
        from demiflow.execution.contracts.record_codec import decode_frame, decode_document, line_limit, read_chunk_bytes
        from demiflow.execution.storage.streams import frame_stream
        from demiflow.execution.storage.streams import read_document
        from demiflow.execution.kernels.dataflow import _materialize_row
        from demiflow.execution.arrow_payload import ArrowBatchCache, is_arrow_record
        index_bytes = 0
        stream = None
        arrow = ArrowBatchCache(self.services.store, self.bounds, services=self.services)

        def reader_bytes():
            return index_bytes + (0 if arrow.ref is None else
                arrow.ref.size_bytes + arrow.batch.get_total_buffer_size() + metadata_bytes(arrow.header))

        def retain_index(size):
            nonlocal index_bytes
            index_bytes = size

        def index_call(function, *args, **kwargs):
            return codec_call(function, *args, retained_input=reader_bytes() + stream.buffered_bytes, **kwargs)

        for part in partitions:
            chunks = block_chunks(self.services.store, ArtifactRef(**part['data_ref']), self.bounds,
                chunk_bytes=read_chunk_bytes(self.bounds, stage_resident_bytes(getattr(self.services, 'assignment', None))),
                frame_call=index_call, on_retained=retain_index)
            stream = frame_stream(chunks, self.bounds, resident_bytes=stage_resident_bytes(getattr(self.services, 'assignment', None)),
                check_workspace=lambda peak: codec_call(admit_workspace, peak, self.bounds, retained_input=reader_bytes()))
            try:
                while True:
                    self.services.check_stop()
                    line = stream.readline(line_limit(self.bounds))
                    if not line:
                        break
                    held = reader_bytes() + stream.buffered_bytes
                    raw, item = codec_call(decode_frame, line, self.bounds, retained_input=held)
                    del line
                    # The decoder's admitted reconstruction peak also bounds
                    # the resulting inline record and its later thawed row.
                    measured = []
                    record = codec_call(decode_document, raw, record_id=item['record_id'],
                        sequence=item['sequence'], bounds=self.bounds, retained_input=held,
                        check_workspace=measured.append)
                    del raw, item
                    charge = measured[0]
                    size = record.encoded_bytes
                    if size > min(self.bounds.max_row_bytes, self.bounds.max_batch_bytes):
                        raise DataflowError('BUDGET_EXCEEDED', 'aggregate row exceeds its sealed payload budget')
                    if record.inline is not None:
                        row = _materialize_row(record, self.services)
                    elif is_arrow_record(record):
                        # IPC authentication, cached parent, and row conversion
                        # share the exact same grant as the aggregate block.
                        codec_call(admit_workspace, charge + 12 * record.payload_ref.size_bytes + 16384,
                                   self.bounds, retained_input=held)
                        row = arrow.row(record)
                        from demiflow.execution.contracts.record_codec import _typed_shape
                        charge = _typed_shape(row, self.bounds.max_resident_bytes)[1] + 512
                    else:
                        raw = codec_call(read_document, record.payload_ref, self.services.store, self.bounds,
                                         retained_input=held + charge, _maximum=self.bounds.max_row_bytes)
                        charge += raw_workspace(raw)
                        codec_call(admit_workspace, charge, self.bounds, retained_input=held)
                        row = _decode_payload(raw, max_bytes=self.bounds.max_row_bytes)
                        del raw
                    del record
                    yield row, size, charge, reader_bytes() + stream.buffered_bytes
                    del row
            finally:
                stream.close()
                arrow.close()

    def stream(self):
        from demiflow.execution.storage.streams import frame_stream
        from demiflow.execution.contracts.record_codec import read_chunk_bytes
        owner = self
        class View:
            def __init__(self):
                self.seek(0)
            def flush(self):
                owner.flush()
            def seek(self, offset, whence=0):
                if hasattr(self, '_stream'):
                    self._stream.close()
                if offset != 0 or whence not in (0, 2):
                    raise ValueError('terminal segment view supports rewind/end only')
                def chunks():
                    for part in owner.manifest_partitions():
                        from demiflow.execution.contracts.dataflow import ArtifactRef
                        yield from block_chunks(owner.services.store, ArtifactRef(**part['data_ref']), owner.bounds,
                            chunk_bytes=read_chunk_bytes(owner.bounds, stage_resident_bytes(getattr(owner.services, 'assignment', None))))
                self._stream = frame_stream(chunks() if whence == 0 else iter(()), owner.bounds, resident_bytes=stage_resident_bytes(getattr(owner.services, 'assignment', None)))
            def readline(self, size=-1):
                return self._stream.readline(size)
            @property
            def buffered_bytes(self):
                return self._stream.buffered_bytes
        return View()

    def close(self):
        if self._tail is not None:
            self._tail.close()
            self._tail = None
