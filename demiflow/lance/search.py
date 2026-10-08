"""Action-scoped, row-preserving native Lance vector search actor."""
from __future__ import annotations

import math
from collections.abc import Mapping
from numbers import Real
from threading import Lock

import pyarrow as pa

from ..errors import InvalidLanceRequest, LanceExecutionError
from .model import normalize_columns, normalize_filter
from .storage import normalize_lance_uri, normalize_storage_options, open_lance_dataset


_DEFAULTS = {
    'use_index': True,
    'nprobes': None,
    'refine_factor': None,
    'ef': None,
    'max_search_candidates': 10000,
    'index_cache_size_bytes': 256 * 2**20,
    'metadata_cache_size_bytes': 32 * 2**20,
    'io_buffer_size': 16 * 2**20,
    'batch_size': 64,
    'batch_size_bytes': 2**20,
    'max_result_bytes': 4 * 2**20,
    'max_python_result_bytes': 32 * 2**20,
    'max_query_dimensions': 65536,
}


def positive_int(value, name):
    if type(value) is not int or value < 1:
        raise InvalidLanceRequest(f'{name} must be a positive integer')


def _name(value, name):
    if not isinstance(value, str) or not value.strip():
        raise InvalidLanceRequest(f'{name} must be a nonempty column name')
    return value


def _options(value):
    if value is not None and not isinstance(value, Mapping):
        raise InvalidLanceRequest('search_vectors options must be a mapping')
    result = {**_DEFAULTS, **(value or {})}
    unknown = set(result) - set(_DEFAULTS)
    if unknown:
        raise InvalidLanceRequest(f'Unknown search_vectors options: {sorted(unknown)}')
    for name, val in result.items():
        if name == 'use_index':
            if type(val) is not bool:
                raise InvalidLanceRequest('use_index must be boolean')
        elif name in ('nprobes', 'refine_factor', 'ef') and val is None:
            continue
        else:
            positive_int(val, name)
    return result


def _python_values_size(array):
    """Conservative conversion allowance, checked before Python materialization.

    Covers primitive scalars and list/struct/map containers, including empty/null
    values. Sliced children may overcount retained parents. Unsupported complex
    Arrow types are rejected, rather than assigned an unverified allowance.
    """
    kind = array.type
    size = 128 * len(array)
    if pa.types.is_struct(kind):
        size += len(array) * 72 * kind.num_fields
        return size + sum(_python_values_size(array.field(i)) for i in range(kind.num_fields))
    if (pa.types.is_list(kind) or pa.types.is_large_list(kind)
            or pa.types.is_fixed_size_list(kind)):
        return size + _python_values_size(array.values)
    if pa.types.is_map(kind):
        return size + 128 * len(array.keys) + _python_values_size(array.keys) + _python_values_size(array.items)
    if (pa.types.is_null(kind) or pa.types.is_boolean(kind) or pa.types.is_integer(kind)
            or pa.types.is_floating(kind) or pa.types.is_decimal(kind)
            or pa.types.is_temporal(kind) or pa.types.is_string(kind)
            or pa.types.is_large_string(kind) or pa.types.is_binary(kind)
            or pa.types.is_large_binary(kind) or pa.types.is_fixed_size_binary(kind)):
        return size
    raise InvalidLanceRequest(f'Unsupported search result Arrow type: {kind}')


class VectorSearch:
    # Marks the object as a lifecycle-managed actor for Dataset.map_async.
    concurrency = 1

    def __init__(self, *, query, output, uri, vector_column, columns, top_k,
                 version, metric, filter, filter_column, storage_options,
                 options, label):
        self.query = _name(query, 'query')
        self.output = _name(output, 'output')
        self.label = _name(label, 'label')
        if query == output or output == filter_column:
            raise InvalidLanceRequest('output must differ from input query/filter columns')
        if filter_column is not None:
            _name(filter_column, 'filter_column')
        self.filter_column = filter_column
        positive_int(top_k, 'top_k')
        if version is not None:
            positive_int(version, 'version')
        if metric not in (None, 'l2', 'cosine', 'dot'):
            raise InvalidLanceRequest('metric must be l2, cosine, dot or None')
        columns = normalize_columns(columns)
        if not columns:
            raise InvalidLanceRequest('search_vectors requires explicit nonempty columns')
        if '_distance' in columns:
            raise InvalidLanceRequest('_distance is added automatically; omit it from columns')
        self.options = _options(options)
        # HNSW's beam must cover the expanded candidate count, not just the
        # final output k. Do not silently raise an explicitly chosen ef.
        candidates = top_k
        if self.options['use_index']:
            candidates *= self.options['refine_factor'] or 1
            ef = self.options['ef']
            if ef is not None and ef < candidates:
                raise InvalidLanceRequest(
                    f'ef must be >= top_k * refine_factor ({candidates}); '
                    'omit ef to use the Lance default')
            # Pinned pylance 12 uses k + k//2 when ef is omitted, with k
            # already expanded for refinement. Conservative for non-HNSW
            # indices, which do not use this beam. It is a candidate-count
            # guard, not a bound on all native allocations or visited nodes.
            beam = candidates + candidates // 2 if ef is None else ef
            candidates = max(candidates, beam)
        if candidates > self.options['max_search_candidates']:
            raise MemoryError('Search candidate count exceeds max_search_candidates; '
                              'reduce top_k/refine_factor/ef or raise the explicit budget')
        # Reserve top-k row containers before native search scheduling. Payload
        # bytes and nested values receive separate admission before to_pylist.
        if top_k * (128 + 200 * (len(columns) + 1) + 24) > self.options['max_python_result_bytes']:
            raise MemoryError('top_k row containers exceed max_python_result_bytes')
        self.spec = dict(uri=normalize_lance_uri(uri), version=version,
            vector_column=_name(vector_column, 'vector_column'), columns=list(columns),
            top_k=top_k, metric=metric, filter=normalize_filter(filter),
            filter_column=filter_column,
            storage_options=dict(normalize_storage_options(storage_options)),
            options=dict(self.options))
        self.resources = (self,)
        self._lock = Lock()
        self._dataset = None
        self._vector_type = None
        self._version = None
        self._queries = self._hits = 0

    async def astart(self):
        if self._dataset is not None:
            raise RuntimeError('VectorSearch actor already active')
        self._version = None
        self._queries = self._hits = 0

    async def aclose(self):
        # The streaming executor drains blocking calls before invoking this.
        self._dataset = self._vector_type = None

    def snapshot_metrics(self):
        return dict(uri=self.spec['uri'], version=self._version,
                    queries=self._queries, hits=self._hits)

    def _open(self):
        with self._lock:
            if self._dataset is None:
                dataset = open_lance_dataset(self.spec['uri'], self.spec['version'],
                    self.spec['storage_options'],
                    index_cache_size_bytes=self.options['index_cache_size_bytes'],
                    metadata_cache_size_bytes=self.options['metadata_cache_size_bytes'])
                schema = dataset.schema
                if not isinstance(schema, pa.Schema):
                    raise LanceExecutionError('Lance returned a non-Arrow schema')
                missing = set(self.spec['columns'] + [self.spec['vector_column']]) - set(schema.names)
                if missing:
                    raise InvalidLanceRequest(f'Lance columns not found: {sorted(missing)}')
                if '_distance' in schema.names:
                    raise InvalidLanceRequest("Lance source schema reserves '_distance'")
                kind = schema.field(self.spec['vector_column']).type
                if not pa.types.is_fixed_size_list(kind) or not pa.types.is_floating(kind.value_type):
                    raise InvalidLanceRequest('Lance vector column must be a fixed-size floating list')
                if kind.list_size > self.options['max_query_dimensions']:
                    raise InvalidLanceRequest('Vector column exceeds max_query_dimensions')
                self._vector_type, self._version = kind, dataset.version
                self._dataset = dataset
            return self._dataset

    def __call__(self, row):
        value = row[self.query]
        if (isinstance(value, (str, bytes, Mapping)) or not hasattr(value, '__len__')
                or not hasattr(value, '__iter__') or len(value) == 0):
            raise InvalidLanceRequest('query must contain a nonempty one-dimensional numeric vector')
        if len(value) > self.options['max_query_dimensions']:
            raise InvalidLanceRequest('query exceeds max_query_dimensions')
        dataset = self._open()
        if len(value) != self._vector_type.list_size:
            raise InvalidLanceRequest('Lance query vector dimension differs from column')
        # Check before allocating a converted array; no generator/implicit batch.
        limit = float.fromhex('0x1.ffcp+15') if self._vector_type.value_type == pa.float16() else (
            float.fromhex('0x1.fffffep+127') if self._vector_type.value_type == pa.float32() else float('inf'))
        nonzero = False
        for entry in value:
            if isinstance(entry, bool) or not isinstance(entry, Real):
                raise InvalidLanceRequest('query vector entries must be real numbers')
            if not math.isfinite(entry) or abs(entry) > limit:
                raise InvalidLanceRequest('query vector entries must be finite and representable')
            nonzero = nonzero or entry != 0
        if self.spec['metric'] == 'cosine' and not nonzero:
            raise InvalidLanceRequest('cosine query vector must be nonzero')
        predicate = self.spec['filter']
        if self.filter_column is not None:
            per_row = normalize_filter(row[self.filter_column])
            if per_row is not None:
                predicate = f'({predicate}) AND ({per_row})' if predicate else per_row
        nearest = dict(column=self.spec['vector_column'],
            q=pa.array(value, type=self._vector_type.value_type), k=self.spec['top_k'],
            use_index=self.options['use_index'])
        if self.spec['metric'] is not None:
            nearest['metric'] = self.spec['metric']
        if self.options['use_index']:
            for name in ('nprobes', 'refine_factor', 'ef'):
                if self.options[name] is not None:
                    nearest[name] = self.options[name]
        scanner = dataset.scanner(columns=[*self.spec['columns'], '_distance'], nearest=nearest,
            filter=predicate, prefilter=True, limit=self.spec['top_k'],
            batch_size=min(self.options['batch_size'], self.spec['top_k']),
            batch_size_bytes=min(self.options['batch_size_bytes'], self.options['max_result_bytes']),
            io_buffer_size=self.options['io_buffer_size'], batch_readahead=1,
            fragment_readahead=1, late_materialization=True)
        # Reserve key/sort workspace before native execution / conversion.
        # Native parallel readers can deliver sorted chunks out of global order.
        hits, arrow_bytes, python_bytes = [], 0, self.spec['top_k'] * 24
        batches = scanner.to_batches()
        try:
            for batch in batches:
                arrow_bytes += batch.get_total_buffer_size()
                if arrow_bytes > self.options['max_result_bytes']:
                    raise MemoryError('Search results exceed max_result_bytes; project fewer fields or reduce top_k')
                python_bytes += (4 * batch.get_total_buffer_size()
                    + batch.num_rows * (128 + 72 * batch.num_columns)
                    + sum(_python_values_size(c) for c in batch.columns))
                if python_bytes > self.options['max_python_result_bytes']:
                    raise MemoryError('Search results exceed max_python_result_bytes')
                if len(hits) + batch.num_rows > self.spec['top_k']:
                    raise LanceExecutionError('Lance returned more than top_k hits')
                hits.extend(batch.to_pylist())
        finally:
            close = getattr(batches, 'close', None)
            if close is not None:
                close()
        # Each query already has a bounded top-k result. Restore distance order
        # across chunks; row completion order in the outer stream is independent.
        hits.sort(key=lambda hit: hit['_distance'])
        with self._lock:
            self._queries += 1
            self._hits += len(hits)
        return {**row, self.output: hits}
