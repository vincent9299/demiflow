"""Fixed-version keyed row lookup for a stream, with bounded matches/cache."""
import asyncio
from collections import OrderedDict
import lance
from .stream_grouping import row_size
from .lance_predicate import scalar_equal


class LanceLookup:
    label = 'lookup_lance'
    concurrency = 1
    queue_depth = 1
    catch = ()

    def __init__(self, source, on, columns, output, max_matches, max_bytes):
        if not isinstance(source, dict) or set(source) != {'uri', 'version'} or type(source['version']) is not int or source['version'] < 1:
            raise ValueError('Lookup requires a fixed uri/version')
        if not isinstance(on, str) or not on or not isinstance(output, str) or not output or on == output:
            raise ValueError('Lookup requires distinct key and output fields')
        if type(max_matches) is not int or not 1 <= max_matches <= 64 or type(max_bytes) is not int or not 1 <= max_bytes <= 16*1024**2:
            raise ValueError('Invalid lookup bounds')
        self.source, self.on, self.columns, self.output = source, on, columns, output
        self.max_matches, self.max_bytes, self.table = max_matches, max_bytes, None
        self.cache, self.cache_bytes = OrderedDict(), 0

    async def astart(self):
        self.table = await asyncio.to_thread(lance.dataset, **self.source)
        if self.on not in self.table.schema.names:
            raise ValueError('Lookup key absent from source')

    def lookup(self, row):
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
            for batch in self.table.scanner(columns=self.columns, filter=scalar_equal(self.on, key),
                    limit=self.max_matches+1, batch_size=1, batch_readahead=1, fragment_readahead=1).to_batches():
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
