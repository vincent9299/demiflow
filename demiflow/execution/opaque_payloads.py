"""Hydrate immutable opaque row envelopes after native key-only relations."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from ..lance.arrow_batches import ARROW_BATCH_BYTES


def payload_schema(schema, references):
    active = [ref for ref in references if ref['field'] in schema.names]
    fields = {ref['field'] for ref in active}
    return pa.schema([pa.field(f.name, pa.large_binary(), nullable=f.nullable)
                      if f.name in fields else f for f in schema], metadata=schema.metadata), active


def hydrate_payloads(batches, references, *, max_row_bytes, stats):
    """Bound lookups before allocation using lengths saved with each envelope.

    Offset/length arrays are query-batch bounded. Combined decoded envelope
    bytes per lookup batch are <= 8 MiB, or one finite oversized row. Native
    buffers, take/copy buffers and Lance decoding still require RSS headroom.
    """
    import lance
    snapshots = {ref['field']: lance.dataset(**ref['source']) for ref in references}
    for batch in batches:
        sizes = [batch.column(ref['size_field']).to_pylist() for ref in references]
        start, total = 0, 0
        ranges = []
        for i in range(batch.num_rows):
            values = [column[i] for column in sizes]
            if any(value is not None and (not isinstance(value, int) or value < 0) for value in values):
                raise ValueError('Invalid private payload length')
            cost = sum(value or 0 for value in values) + batch.slice(i, 1).nbytes
            if cost > max_row_bytes:
                raise MemoryError(f'Opaque joined row exceeds max_row_bytes={max_row_bytes}')
            if i > start and total + cost > ARROW_BATCH_BYTES:
                ranges.append((start, i))
                start, total = i, 0
            total += cost
        if start < batch.num_rows:
            ranges.append((start, batch.num_rows))
        for start, end in ranges:
            part = batch.slice(start, end - start)
            for ref in references:
                field = ref['field']
                offsets = part.column(field).to_pylist()
                lengths = part.column(ref['size_field']).to_pylist()
                indices, positions, expected = [], [], []
                for offset, length in zip(offsets, lengths):
                    if offset is None:
                        if length is not None:
                            raise ValueError('Unmatched payload has a length')
                        positions.append(None)
                    else:
                        if length is None:
                            raise ValueError('Matched payload has no length')
                        positions.append(len(indices))
                        indices.append(offset)
                        expected.append(length)
                if indices:
                    lookup = snapshots[field].take(indices, columns=[field])
                    if lookup.num_rows != len(indices) or pc.binary_length(lookup[field]).to_pylist() != expected:
                        raise RuntimeError('Pinned opaque payload lookup changed size or lost rows')
                    stats['max_lookup_bytes'] = max(stats['max_lookup_bytes'], lookup.nbytes)
                    values = pc.take(lookup[field], pa.array(positions, type=pa.int32())).combine_chunks()
                else:
                    values = pa.nulls(part.num_rows, type=pa.large_binary())
                part = part.set_column(part.schema.get_field_index(field), field, values)
            stats['rows'] += part.num_rows
            stats['batches'] += 1
            stats['max_batch_bytes'] = max(stats['max_batch_bytes'], part.nbytes)
            yield part
