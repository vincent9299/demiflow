"""Arrow batch admission; legal slice offsets pass through unchanged.

These helpers bound logical batch bytes, not process RSS. A borrowed slice may
retain a larger parent buffer; readers and writers need separate protection.
"""

from demiflow.execution.contracts.dataflow import BATCH_TARGET_BYTES
from collections.abc import Iterable, Iterator
import pyarrow as pa

LANCE_BATCH_ROWS = 8192
LANCE_FILE_ROWS = 1048576
ARROW_BATCH_BYTES = 8 * 1024 * 1024


def _limits(batch_rows, batch_bytes):
    if type(batch_rows) is not int or batch_rows < 1:
        raise ValueError('batch_rows must be a positive integer')
    if type(batch_bytes) is not int or batch_bytes < 1:
        raise ValueError('batch_bytes must be a positive integer')


def _check(batch, schema):
    if not isinstance(batch, pa.RecordBatch):
        raise TypeError('Expected a pyarrow.RecordBatch')
    if not batch.schema.equals(schema, check_metadata=True):
        raise ValueError('Arrow batch schema changed during write')


def bounded_record_batches(batches: Iterable[pa.RecordBatch], schema: pa.Schema, *,
    batch_rows=LANCE_BATCH_ROWS, batch_bytes=ARROW_BATCH_BYTES,
    max_row_bytes=None) -> Iterator[pa.RecordBatch]:
    """Split oversized batches without copying or combining small batches.

    Check nbytes before sink admission. Search fitting row prefixes using Arrow
    metadata. An explicitly budgeted wide row is emitted alone; values above
    that separate finite limit raise. No value is truncated. The default keeps
    the strict single-row batch limit for callers that do not opt in.
    """
    _limits(batch_rows, batch_bytes)
    if max_row_bytes is None:
        max_row_bytes = batch_bytes
    if type(max_row_bytes) is not int or max_row_bytes < batch_bytes:
        raise ValueError('max_row_bytes must be an integer >= batch_bytes')
    for batch in batches:
        _check(batch, schema)
        if batch.num_rows <= batch_rows and batch.nbytes <= batch_bytes:
            if batch.num_rows:
                yield batch
            continue
        start = 0
        while start < batch.num_rows:
            size = min(batch_rows, batch.num_rows-start)
            piece = batch.slice(start, size)
            if piece.nbytes > batch_bytes:
                single = batch.slice(start, 1)
                if single.nbytes > batch_bytes:
                    if single.nbytes > max_row_bytes:
                        limit = f'batch_bytes={batch_bytes}' if max_row_bytes == batch_bytes else f'max_row_bytes={max_row_bytes}'
                        raise MemoryError(f'One Arrow row exceeds {limit}; cannot split the row')
                    yield single
                    start += 1
                    continue
                low, high = 1, size
                while low < high:
                    middle = (low+high+1)//2
                    if batch.slice(start, middle).nbytes <= batch_bytes:
                        low = middle
                    else:
                        high = middle-1
                size = low
                piece = batch.slice(start, size)
            yield piece
            start += size


def fixed_row_tables(batches, schema, *, batch_rows, batch_bytes=ARROW_BATCH_BYTES):
    """Keep exact callback grouping using borrowed Arrow chunks.

    This is a callback contract, not a Lance workaround. An exact group exceeding
    byte admission fails instead of silently changing callback scope.
    """
    _limits(batch_rows, batch_bytes)
    pieces, count, size = [], 0, 0
    for batch in batches:
        _check(batch, schema)
        start = 0
        while start < batch.num_rows:
            rows = min(batch_rows-count, batch.num_rows-start)
            piece = batch.slice(start, rows)
            if size + piece.nbytes > batch_bytes:
                raise MemoryError(f'Configured callback group exceeds batch_bytes={batch_bytes}; reduce callback batch_rows')
            pieces.append(piece)
            size += piece.nbytes
            count += rows
            start += rows
            if count == batch_rows:
                output = pa.Table.from_batches(pieces, schema=schema)
                pieces, count, size = [], 0, 0
                yield output
    if count:
        yield pa.Table.from_batches(pieces, schema=schema)


def batch_target(limits):
    return min(BATCH_TARGET_BYTES, limits['max_batch_bytes'], limits['max_row_bytes'])

def arrow_batches(batches, limits):
    """Split borrowed Arrow batches under one logical row/batch contract.

    Borrowed slice offsets are legal. Their retained parent buffers count against
    the memory allowance, rather than being mistaken for logical batch bytes.
    No payload copy is performed by this admission helper.
    """
    import pyarrow as pa

    from demiflow.execution.recovery_contract import RecoveryContractError
    for value in batches:
        if not isinstance(value, (pa.RecordBatch, pa.Table)):
            raise RecoveryContractError('Lance staging requires native Arrow batches')
        if value.get_total_buffer_size() > limits['memory_bytes']:
            raise RecoveryContractError('Arrow retained buffers exceed memory budget')
        incoming = value.to_batches() if isinstance(value, pa.Table) else (value,)
        yield from bounded_record_batches(incoming, value.schema,
            batch_rows=limits['batch_rows'], batch_bytes=batch_target(limits),
            max_row_bytes=limits['max_row_bytes'])
