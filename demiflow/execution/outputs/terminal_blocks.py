"""Terminal blocks implementation."""
from __future__ import annotations

from dataclasses import asdict
from demiflow.execution.contracts.dataflow import BlockPartition, stage_resident_bytes
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.storage.segments import RecordBlocks, block_chunks
from demiflow.execution.contracts.recovery_contract import identity


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

class TerminalBlocks(RecordBlocks):
    """Final-result block layout and receipt descriptors over one physical store."""
    def __init__(self, services, partition_id, *, declaration=None, partitioned=True, budget=None,
                 frame_call=None, coalesce_allowed=None):
        super().__init__(getattr(services, 'store', None), services.bounds, partition_id,
            assignment=getattr(services, 'assignment', None), check_stop=getattr(services, 'check_stop', None),
            partitioned=partitioned, budget=budget, frame_call=frame_call,
            coalesce_allowed=coalesce_allowed)
        if declaration is not None and partitioned:
            self._layouts.add(declaration)
            occurrence, count = declaration
            for index in range(count):
                self._block(BlockPartition(occurrence, index, count))


    def manifest_partitions(self):
        keys = self.prepare_blocks()
        out = []
        with self.hold_metadata(keys), self.hold_metadata(out):
            for key in keys:
                tag, ref, rows, size = self.seal_block(key)
                partition = {'partition_id': identity(['terminal-block-v1', self.partition_id, asdict(tag)]) if tag else self.partition_id,
                             'rows': rows, 'bytes': size, 'data_ref': asdict(ref)}
                if tag is not None:
                    partition['block_partition'] = asdict(tag)
                out.append(partition)
        return out

    def scalar_workspace(self, *, resident_bytes, retained):
        # The sealed index reservation and any previous worker-side Arrow
        # parent are still live beside this scalar's input and result.
        from demiflow.execution.contracts.record_codec import resident_limit
        memory = resident_limit(self.bounds, resident_bytes) - self._metadata_limit - retained
        cached = getattr(self.store, '_arrow_payload_cache', None)
        if cached is not None and cached.ref is not None:
            from demiflow.execution.contracts.record_codec import metadata_bytes
            memory -= cached.ref.size_bytes + cached.batch.get_total_buffer_size() + metadata_bytes(cached.header)
        return memory

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
                        yield from block_chunks(owner.store, ArtifactRef(**part['data_ref']), owner.bounds,
                            chunk_bytes=read_chunk_bytes(owner.bounds, stage_resident_bytes(owner.assignment)))
                self._stream = frame_stream(chunks() if whence == 0 else iter(()), owner.bounds, resident_bytes=stage_resident_bytes(owner.assignment))
            def readline(self, size=-1):
                return self._stream.readline(size)
            @property
            def buffered_bytes(self):
                return self._stream.buffered_bytes
        return View()
