"""Key selection semantics on fixed typed batches; no source or worker ownership."""
from demiflow.execution.contracts.dataflow_errors import DataflowError


class BatchKeySelection:
    """Own only projected equality keys and their selection, never the right or left payloads."""
    @classmethod
    def admits(cls, left, right, *, sql):
        from demiflow.execution.contracts.relational_region import KEY_SET_WINDOW, SQL_CHILD_RSS, selection_wire_bytes
        if left.dtype != right.dtype:
            return False
        if not sql:
            # Retained key and physical row-id buffers, build array and hash
            # buckets, one projected input and the transient per-batch mask.
            maximum = (left.count * 18 + right.count * 64 +
                       64 * max(left.batch_rows, right.batch_rows) +
                       1024 * (left.chunk_bound + right.chunk_bound))
            return maximum <= KEY_SET_WINDOW
        window = selection_wire_bytes(SQL_CHILD_RSS)
        # Preserve the original primitive Arrow type and NULL in the SQL input.
        # Include key/row-id buffers and request overhead before either scan starts.
        return (left.dtype == right.dtype and max(left.count, right.count) <= window // 8
                and (left.count + right.count) * 26 + 4096
                    + 1024 * (left.chunk_bound + right.chunk_bound) <= window)

    def __init__(self, *, sql, dtype):
        self.sql, self.dtype = sql, dtype
        self.columns = {'lkeys': [], 'rkeys': []}
        self.left_positions = []

    def append(self, side, fragment, offset, batch):
        key = batch.column(0)
        self.columns[side].append(key)
        if side == 'lkeys':
            self.left_positions.append((fragment, offset, batch.column('_rowid')))

    def survivors(self, *, query, check_stop, skip, batch_size):
        import numpy as np
        import pyarrow as pa
        import pyarrow.compute as pc
        from demiflow.execution.contracts.relational_region import SQL_CHILD_RSS
        if self.sql:
            from .relational_sql import exclusion_query
            dtype = self.dtype
            tables = {side: pa.chunked_array(parts, type=dtype) for side, parts in self.columns.items()}
            total = len(tables['lkeys'])
            sql, key_functions = exclusion_query(dtype)
            bits = query(sql, tables=tables, max_pairs=total,
                rss_bytes=SQL_CHILD_RSS, check_stop=check_stop, selection=True,
                key_functions=key_functions)
            keep = pa.Array.from_buffers(pa.bool_(), total, [None, pa.py_buffer(bits)])
            self.columns.clear()
            del tables
        else:
            from .relational import key_membership
            build = self.columns.pop('rkeys')
            right = pa.concat_arrays(build) if build else None
            del build
        start = 0
        for i, (fragment, row_offset, row_ids) in enumerate(self.left_positions):
            check_stop()
            if self.sql:
                mask = keep.slice(start, len(row_ids))
            elif right is not None:
                mask = pc.invert(key_membership(self.columns['lkeys'][i], right))
            else:
                mask = None
            index = pc.indices_nonzero(mask) if mask is not None else pa.array(np.arange(len(row_ids), dtype=np.uint64))
            start += len(row_ids)
            if skip:
                used = min(len(index), skip)
                index = index.slice(used)
                skip -= used
            for begin in range(0, len(index), batch_size):
                local = index.slice(begin, batch_size)
                yield pa.record_batch([pa.array(np.full(len(local), fragment, np.uint64)),
                    pc.add(local, pa.scalar(row_offset, type=pa.uint64())), pc.take(row_ids, local)],
                    names=['fragment_id', 'row_offset', 'row_id'])
        if skip:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'terminal exceeds replayed key selection')
