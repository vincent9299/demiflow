"""Native terminal logical blocks backed by incremental artifact forests."""
from dataclasses import asdict
from collections.abc import Mapping

from ..contracts.dataflow import BlockPartition
from ..contracts.dataflow_errors import DataflowError
from ..dataflow_scratch import ScratchStream
from ..dataflow_segments import SegmentForest, block_chunks, RECORDS_MEDIA
from ..recovery_contract import identity


def declared_blocks(graph, terminal, bounds):
    """Infer only an explicitly declared layout through preserving unary nodes."""
    nodes = {node.occurrence_id: node for node in graph.nodes}
    incoming = {}
    for edge in graph.edges:
        incoming.setdefault(edge.consumer, []).append(edge.producer)
    from ..kernels.dataflow import _UNARY_KINDS, _BATCH_KINDS
    preserving = _UNARY_KINDS | _BATCH_KINDS | {'RepartitionOp', 'RandomShuffleOp', 'RandomizeBlockOrderOp'}
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
    def __init__(self, services, partition_id, *, declaration=None, partitioned=True, budget=None):
        self.services, self.bounds = services, services.bounds
        self.partition_id, self.partitioned = partition_id, partitioned
        stages = getattr(getattr(services, 'assignment', None), 'stages', ())
        granted = sum(max(s.resident_bytes, s.profile.requests['memory_bytes']) for s in stages) if stages else self.bounds.max_resident_bytes
        if budget is None:
            # Historical sealed plans and finite-channel helpers keep their
            # original window; a new compiler never rewrites their grants.
            self._metadata_limit = min(self.bounds.max_state_bytes, granted // 2)
        else:
            if (not isinstance(budget, Mapping) or set(budget) != {'payload_bytes', 'metadata_bytes'}
                    or any(type(v) is not int for v in budget.values())):
                raise DataflowError('INVALID_CONTRACT', 'invalid sealed terminal memory windows')
            payload, metadata = budget['payload_bytes'], budget['metadata_bytes']
            if (payload != self.bounds.max_batch_bytes + self.bounds.max_row_bytes
                    or not 3072 <= metadata <= self.bounds.max_state_bytes
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

    def _admit_metadata(self, *, blocks=0, roots=0):
        # Conservative managed-object charge, in addition to payload/scratch.
        used = (len(self.blocks) + blocks) * 1024
        used += (sum(len(f.roots) for _, f in self.blocks.values()) + roots) * 2048
        if used > self._metadata_limit:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal block index exceeds its admitted state window')

    def _block(self, tag):
        key = (tag.occurrence_id, tag.index) if tag is not None else (self.partition_id, -1)
        if key not in self.blocks:
            if len(self.blocks) >= self.bounds.max_partitions:
                raise DataflowError('BUDGET_EXCEEDED', 'terminal logical blocks exceed max_partitions')
            self._admit_metadata(blocks=1)
            self.blocks[key] = (tag, SegmentForest(self.services.store, self.bounds))
        elif self.blocks[key][0] != tag:
            raise DataflowError('INVALID_CONTRACT', 'logical block count changed within an action')
        return key

    def append(self, record, raw):
        tag = record.block_partition if self.partitioned else None
        # A concrete count also creates empty blocks. No fake business rows.
        if tag is not None and (tag.occurrence_id, tag.count) not in self._layouts:
            if tag.count > self.bounds.max_partitions:
                raise DataflowError('BUDGET_EXCEEDED', 'logical block count exceeds max_partitions')
            self._layouts.add((tag.occurrence_id, tag.count))
            for index in range(tag.count):
                self._block(BlockPartition(tag.occurrence_id, index, tag.count))
        key = self._block(tag)
        if self._tail_key != key:
            self.flush()
            self._tail_key = key
        if self._tail is None:
            self._tail = ScratchStream(self.services.store, max_bytes=self.bounds.max_spill_bytes,
                max_io_bytes=self.bounds.max_row_bytes + self.bounds.max_control_bytes)
        if self.bytes + len(raw) > self.bounds.max_spill_bytes or self.rows >= self.bounds.max_output_rows:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal output exceeds disk/row budget')
        self._tail.write(raw)
        self._tail_rows += 1
        self._tail_bytes += len(raw)
        self.rows += 1
        self.bytes += len(raw)

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
        stream = self._tail
        stream.flush()
        stream.seek(0)
        def chunks():
            while chunk := stream.read(chunk_bytes):
                yield chunk
        ref = self.services.store.put(chunks(), max_bytes=max(1, self._tail_bytes), media_type=RECORDS_MEDIA)
        self.blocks[self._tail_key][1].append(ref, rows=self._tail_rows)
        stream.close()
        self._tail = None
        self._tail_rows = self._tail_bytes = 0

    def manifest_partitions(self):
        self.flush()
        if not self.blocks:
            self._block(None)
        out = []
        for key, (tag, forest) in sorted(self.blocks.items()):
            partition = {'partition_id': identity(['terminal-block-v1', self.partition_id, asdict(tag)]) if tag else self.partition_id,
                         'rows': forest.rows, 'bytes': forest.bytes, 'data_ref': asdict(forest.seal())}
            if tag is not None:
                partition['block_partition'] = asdict(tag)
            out.append(partition)
        return out

    def restore(self, manifest):
        from ..contracts.dataflow import ArtifactRef
        self.blocks.clear()
        self._layouts.clear()
        for part in manifest.get('partitions', [manifest]):
            tag = BlockPartition(**part['block_partition']) if 'block_partition' in part else None
            key = self._block(tag)
            self.blocks[key][1].restore(ArtifactRef(**part['data_ref']), rows=part['rows'], size=part['bytes'])
        self._admit_metadata()
        self.rows, self.bytes = manifest['rows'], manifest['bytes']
        if sum(f.rows for _, f in self.blocks.values()) != self.rows or sum(f.bytes for _, f in self.blocks.values()) != self.bytes:
            raise DataflowError('CORRUPT_ARTIFACT', 'terminal partition totals disagree with manifest')

    def stream(self):
        from ..dataflow_handles import _ChunkStream
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
                        from ..contracts.dataflow import ArtifactRef
                        yield from block_chunks(owner.services.store, ArtifactRef(**part['data_ref']), owner.bounds)
                self._stream = _ChunkStream(chunks() if whence == 0 else iter(()))
            def readline(self, size=-1):
                return self._stream.readline(size)
        return View()

    def close(self):
        if self._tail is not None:
            self._tail.close()
            self._tail = None
