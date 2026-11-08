"""Fixed-version keyed row lookup for a stream, with bounded matches/cache."""
import asyncio
from collections import OrderedDict
from contextlib import closing


from ..lance.flow_contract import normalize_lookup


class LanceLookup:
    label = 'lookup_lance'
    concurrency = 1
    queue_depth = 1
    catch = ()

    def __init__(self, source, on, columns, output, max_matches, max_bytes):
        normalize_lookup(dict(source=source, on=on, columns=columns, output=output, max_matches=max_matches, max_bytes=max_bytes))
        self.source, self.on, self.columns, self.output = source, on, columns, output
        self.max_matches, self.max_bytes, self.table = max_matches, max_bytes, None
        self.cache, self.cache_bytes = OrderedDict(), 0
        self._key_scan = False

    def snapshot_state(self):
        # source uri/version and lookup policy are bound by lookup_facts.
        # The read-only table handle/index plan and LRU are reconstructible
        # resources, not progress: astart reopens the same pinned version.
        return {}

    def restore_state(self, state):
        if type(state) is not dict or state:
            raise ValueError('Fixed Lance lookup has no mutable semantic state')
        self.cache.clear()
        self.cache_bytes = 0

    async def astart(self):
        self.table, self._key_scan = await asyncio.to_thread(self._open)

    def _open(self):
        import lance
        table = lance.dataset(**self.source)
        names = table.schema.names
        if self.on not in names:
            raise ValueError('Lookup key absent from source')
        # An indexed lookup already fetches only matching rows. Keep its native
        # plan, and leave multi-match or unusual projections to that scanner:
        # many separate payload takes are slower than one native multi-row read.
        columns = self.columns
        direct_projection = columns is None or (
            isinstance(columns, (list, tuple)) and bool(columns)
            and all(isinstance(c, str) and c in names for c in columns)
            and len(set(columns)) == len(columns))
        key_scan = False
        if self.max_matches == 1 and direct_projection:
            key_scan = not any(self.on in index.field_names for index in table.describe_indices())
        return table, key_scan

    def _unindexed_batches(self, key):
        from .lance_predicate import scalar_equal
        predicate = scalar_equal(self.on, key)
        # batch_size=1 also splits an unindexed filter's *input* into individual
        # rows. Scan only keys/row IDs in finite native batches instead. The
        # result holds at most two uint64 IDs; no payload or full key index stays
        # resident. Row IDs belong to this same pinned dataset, not row offsets.
        batches = self.table.scanner(columns=[], filter=predicate, with_row_id=True,
            limit=self.max_matches+1, batch_size=8192, batch_size_bytes=64*1024,
            io_buffer_size=1024**2, batch_readahead=1, fragment_readahead=1).to_batches()
        with closing(batches):
            for batch in batches:
                for row_id in batch.column('_rowid').to_pylist():
                    # Keep the original one-row Arrow/Python payload checks.
                    # Taking all matches together could allocate max_matches
                    # oversized payloads before any size check can reject them.
                    payload = self.table._take_rows([row_id], columns=self.columns)
                    if payload.num_rows != 1:
                        raise RuntimeError('Pinned Lance lookup did not return its matching row')
                    yield payload

    def lookup(self, row):
        from .stream_grouping import row_size
        from .lance_predicate import scalar_equal
        if self.output in row:
            raise ValueError('Lookup output would overwrite an input field')
        key = row[self.on]
        if not isinstance(key, (str, int, bool)) or isinstance(key, str) and len(key) > 65536:
            raise ValueError('Lookup key requires a bounded scalar')
        ident = (type(key), key)
        if ident in self.cache:
            matches, size = self.cache[ident]
            self.cache.move_to_end(ident)
        else:
            matches, size = [], 0
            batches = (self._unindexed_batches(key) if self._key_scan else
                self.table.scanner(columns=self.columns, filter=scalar_equal(self.on, key),
                    limit=self.max_matches+1, batch_size=1, batch_readahead=1,
                    fragment_readahead=1).to_batches())
            with closing(batches):
                for batch in batches:
                    if batch.nbytes > self.max_bytes:
                        raise ValueError('Lookup Arrow payload exceeds max_bytes')
                    for value in batch.to_pylist():
                        size += row_size(value, self.max_bytes)
                        if size > self.max_bytes or len(matches) >= self.max_matches:
                            raise ValueError('Lookup result exceeds match/byte bound')
                        matches.append(value)
            # Cache only small entries; total cache is at most 8MiB / 128 keys.
            cache_size = size + row_size(key, self.max_bytes)
            if cache_size <= 1024**2:
                while self.cache and (len(self.cache) >= 128 or self.cache_bytes + cache_size > 8*1024**2):
                    _, (_, removed) = self.cache.popitem(last=False); self.cache_bytes -= removed
                self.cache[ident] = (matches, cache_size); self.cache_bytes += cache_size
        # Downstream may annotate fields; do not expose mutable cached values.
        import copy
        return {**row, self.output: copy.deepcopy(matches)}

    async def __call__(self, row):
        task = asyncio.create_task(asyncio.to_thread(self.lookup, row))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def aclose(self):
        self.table = None; self.cache.clear(); self.cache_bytes = 0
        self._key_scan = False
