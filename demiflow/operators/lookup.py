"""Fixed-version keyed row lookup for a stream, with bounded matches/cache."""
import asyncio
from collections import OrderedDict
from contextlib import closing


from demiflow.lance.flow_contract import normalize_lookup


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
        from demiflow.lance.read import open_lookup_source
        return open_lookup_source(self.source, self.on, self.columns, self.max_matches)

    def _unindexed_batches(self, key):
        from demiflow.lance.read import lookup_batches
        # Bind only when this original generator is first advanced.
        yield from lookup_batches(self.table, self.on, key, self.columns,
                                  self.max_matches, key_scan=True)

    def lookup(self, row):
        from demiflow.operators.grouping import row_size
        from demiflow.lance.read import lookup_batches
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
                lookup_batches(self.table, self.on, key, self.columns,
                    self.max_matches, key_scan=False))
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
