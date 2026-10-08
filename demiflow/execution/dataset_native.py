"""Internal Dataset lowering. DataFusion owns keyed relations; Python owns UDFs.

Typed Lance join chains stay columnar. Opaque Python boundaries use a bounded,
lossless row envelope; its key and stable ordinal are sorted/joined by the same
native engine. Neither path infers SQL from Python bytecode or retries callbacks.
"""
from __future__ import annotations

import itertools
import json
import pickle
import tempfile
import time
from contextlib import ExitStack, closing
from dataclasses import dataclass, field, replace
from pathlib import Path

from ..data import plan as ops
from ..data.local_relational import key_of, _merge_row
from ..data.sources import IterableSource, LanceSource, FileSource
from ..data.stats import ExecutionMetadata, StageStats
from .local_kernel import KeyedSource, UnionSource, PlanInput


SORT_STAGE_ROWS = 1_000_000


def has_relational(source, plan):
    if isinstance(source, KeyedSource):
        return True
    return isinstance(source, UnionSource) and any(
        has_relational(item.source, item.plan) for item in source.inputs)


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def json_key_type(dtype):
    """Only lower types whose Python values obey the existing JSON key codec."""
    import pyarrow as pa
    if (pa.types.is_null(dtype) or pa.types.is_boolean(dtype)
            or pa.types.is_integer(dtype) or pa.types.is_floating(dtype)
            or pa.types.is_string(dtype) or pa.types.is_large_string(dtype)):
        return True
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
        return json_key_type(dtype.value_type)
    if pa.types.is_struct(dtype):
        return all(json_key_type(f.type) for f in dtype)
    return False


def register_key_functions(context, spec):
    """Worker-side canonical keys preserve the existing Python key contract."""
    import pyarrow as pa
    from datafusion import udf
    for item in spec['keys']:
        def canonical(*arrays, names=item['fields']):
            values = [a.to_pylist() for a in arrays]
            count = len(names)
            result = []
            for i in range(len(arrays[0])):
                for j, name in enumerate(names):
                    if not values[count + j][i]:
                        raise KeyError(name)
                result.append(json.dumps([values[j][i] for j in range(count)],
                    ensure_ascii=False, sort_keys=True, separators=(',', ':')))
            return pa.array(result, type=pa.string())
        context.register_udf(udf(canonical, [*item['types'], *[pa.bool_()] * len(item['fields'])],
            pa.string(), 'immutable', name=item['name']))


def finish_typed_batch(batch, *, columns, encoded_operations, schema):
    """Partition-local callbacks, one instance per configured input task batch."""
    import pyarrow as pa
    import cloudpickle
    from .local_tasks import _apply
    operations = cloudpickle.loads(encoded_operations)
    rows = ({c.name: row[c.value] for c in columns if row[c.present]} for row in batch.to_pylist())
    return pa.RecordBatch.from_pylist(list(_apply(rows, operations)), schema=schema)


@dataclass
class Column:
    name: str
    value: str
    present: str
    dtype: object
    optional: bool = False


@dataclass
class Relation:
    view: str
    columns: list[Column] | None
    order: tuple[str, ...]
    decoder: object = None
    keys: dict = field(default_factory=dict)
    joins: int = 0
    payload_row_bytes: int = 0


class Engine:
    def __init__(self, executor, stack, directory, stats):
        self.executor, self.stack = executor, stack
        self.directory, self.stats = Path(directory), stats
        self.views, self.sources, self.key_functions = {}, {}, []
        self.counter = itertools.count()
        self._session = None
        self.typed_cache = {}
        self.large = False

    def name(self, prefix='n'):
        return f'{prefix}{next(self.counter)}'

    def view(self, sql):
        name = self.name('v')
        self.views[name] = sql
        return name

    @staticmethod
    def batch_rows(rel):
        # A join can carry one payload from each packed input in an output row.
        # Row count alone lets a native output batch grow to multiple GiB.
        return min(8192, max(1, (8 * 2**20) // max(1, rel.payload_row_bytes)))

    @property
    def session(self):
        if self._session is None:
            from .native_resources import dataset_session
            self._session = self.stack.enter_context(dataset_session(
                self.executor, self.directory,
                prefer_hash_join=any(s.get('format') == 'csv' for s in self.sources.values())))
        # Key-only operations using the session directly have no opaque payload.
        self._session.options = replace(self._session.options, batch_rows=8192)
        return self._session

    def query(self, rel, sql, **kwargs):
        # A previously materialized sibling is not carried by this query.
        # Repacking also replaces its inputs, rather than adding their sizes.
        session = self.session
        session.options = replace(session.options, batch_rows=self.batch_rows(rel))
        return session.query(sql, **kwargs)

    def typed(self, item):
        """Compile schema-known chains without running any Python callback."""
        source, plan = item.source, item.plan
        cache_key = (id(source), id(plan))
        if cache_key in self.typed_cache:
            return self.typed_cache[cache_key][2]
        allowed = (ops.SelectColumnsOp, ops.DropColumnsOp, ops.RenameColumnsOp, ops.LimitOp)
        if any(not isinstance(op, allowed) for op in plan.operations):
            return None
        if isinstance(source, KeyedSource) and source.kind == 'join':
            left, right = self.typed(source.left), self.typed(source.right)
            if left is None or right is None:
                return None
            # Cross-position shared key names have value-dependent skip rules.
            if plan.operations:
                return None  # row projections must retain missing-field errors
            shared = set(source.on) & set(source.right_on)
            import pyarrow as pa
            if any(c.name in shared and not (pa.types.is_string(c.dtype) or pa.types.is_large_string(c.dtype) or pa.types.is_integer(c.dtype) or pa.types.is_boolean(c.dtype)) for c in left.columns + right.columns):
                return None
            right = self.clone_typed(right)
            if any(source.on.index(k) != source.right_on.index(k) for k in shared):
                return None
            names = {c.name for c in left.columns}
            columns = list(left.columns)
            for c in right.columns:
                if c.name in shared:
                    continue
                name = c.name if c.name not in names else c.name + source.suffix
                if name in names:
                    return None  # collision is raised only for actual matches
                names.add(name)
                columns.append(Column(name, c.value, c.present, c.dtype, c.optional or source.how == 'left'))
            if not all(k in {c.name for c in left.columns if not c.optional} for k in source.on):
                return None
            if not all(k in {c.name for c in right.columns if not c.optional} for k in source.right_on):
                return None
            if any(not json_key_type(c.dtype) for columns, keys in (
                    (left.columns, source.on), (right.columns, source.right_on))
                    for c in columns if c.name in keys):
                return None
            if (any(s.get('format') == 'csv' for s in self.sources.values())
                    and len(source.on) == 1 and all(
                    pa.types.is_string(c.dtype) or pa.types.is_large_string(c.dtype)
                    for r, keys in ((left, source.on), (right, source.right_on))
                    for c in r.columns if c.name in keys)):
                # String equality is identical to canonical JSON equality. Encode
                # ordering keys only after the join, so rejected CSV rows never
                # cross a Python UDF boundary.
                rel = self.string_join(left, right, source)
            else:
                rel = self.join(self.keyed(left, source.on), self.keyed(right, source.right_on), source)
            rel.columns = columns if source.how in {'inner', 'left'} else list(left.columns)
        elif isinstance(source, FileSource) and source.format == 'csv':
            from .csv_source import csv_spec
            spec = csv_spec(source)
            if spec is None:
                return None
            table, present, ordinal = self.name('csv'), self.name('p'), self.name('o')
            self.sources[table] = spec
            import pyarrow as pa
            columns = [Column(n, self.name('c'), present, pa.string()) for n in spec['columns']]
            # Arrow strings_can_be_null=False retains empty cells. The Rust
            # CSV decoder returns null for an empty cell even with null_regex.
            projection = [f"coalesce({quote(c.name)}, '') AS {c.value}" for c in columns]
            projection += [f'TRUE AS {present}', f'row_number() OVER () AS {ordinal}']
            rel = Relation(self.view(f'SELECT {", ".join(projection)} FROM {table}'), columns, (ordinal,))
        elif isinstance(source, LanceSource):
            from ..lance.model import LanceScanSpec
            from ..lance.storage import open_lance_dataset, resolve_local_uri
            query = source.query
            # Other reader features run through their existing exact reader.
            if (not isinstance(query, LanceScanSpec) or query.filter or query.projection
                    or query.storage_options or query.limit is not None
                    or not Path(query.uri).is_absolute()
                    or any(getattr(query, name) is not None for name in
                           ('batch_size', 'batch_readahead', 'fragment_readahead'))):
                return None
            ds = open_lance_dataset(query.uri, query.version, query.storage_options)
            # LanceScanSpec normalizes omitted/empty columns to () meaning all.
            fields = list(query.columns or ds.schema.names)
            if any(k not in ds.schema.names for k in fields) or len(set(fields)) != len(fields):
                return None
            table, present, ordinal = self.name('t'), self.name('p'), self.name('o')
            self.sources[table] = {'uri': str(resolve_local_uri(query.uri)), 'version': ds.version}
            self.large = self.large or ds.count_rows() >= SORT_STAGE_ROWS
            columns = [Column(k, self.name('c'), present, ds.schema.field(k).type) for k in fields]
            projection = [f'{quote(c.name)} AS {c.value}' for c in columns]
            projection += [f'TRUE AS {present}', f'_rowaddr AS {ordinal}']
            rel = Relation(self.view(f'SELECT {", ".join(projection)} FROM {table}'), columns, (ordinal,))
        else:
            return None
        for op in plan.operations:
            if isinstance(op, ops.SelectColumnsOp):
                available = {c.name: c for c in rel.columns}
                if any(k not in available for k in op.columns) or len(set(op.columns)) != len(op.columns):
                    return None
                rel = Relation(rel.view, [available[k] for k in op.columns], rel.order, keys=rel.keys.copy(),
                    payload_row_bytes=rel.payload_row_bytes)
            elif isinstance(op, ops.DropColumnsOp):
                if set(op.columns) - {c.name for c in rel.columns}:
                    return None
                rel = Relation(rel.view, [c for c in rel.columns if c.name not in op.columns], rel.order, keys=rel.keys.copy(),
                    payload_row_bytes=rel.payload_row_bytes)
            elif isinstance(op, ops.RenameColumnsOp):
                names = [op.names.get(c.name, c.name) for c in rel.columns] if isinstance(op.names, dict) else list(op.names)
                if len(names) != len(rel.columns) or len(set(names)) != len(names):
                    return None
                rel = Relation(rel.view, [Column(k, c.value, c.present, c.dtype, c.optional) for k, c in zip(names, rel.columns)],
                    rel.order, payload_row_bytes=rel.payload_row_bytes)
            else:
                rel = Relation(self.view(f'SELECT * FROM {rel.view} ORDER BY {self.order_sql(rel)} LIMIT {op.limit}'),
                               rel.columns, rel.order, keys=rel.keys.copy(), payload_row_bytes=rel.payload_row_bytes)
        self.typed_cache[cache_key] = (source, plan, rel)
        return rel

    def clone_typed(self, rel):
        columns = list(dict.fromkeys([*(c.value for c in rel.columns), *(c.present for c in rel.columns),
            *rel.order, *(v for pair in rel.keys.values() for v in pair)]))
        rename = {c: self.name('a') for c in columns}
        view = self.view('SELECT ' + ', '.join(f'{c} AS {rename[c]}' for c in columns) + f' FROM {rel.view}')
        return Relation(view, [Column(c.name, rename[c.value], rename[c.present], c.dtype, c.optional) for c in rel.columns],
            tuple(rename[c] for c in rel.order), keys={k:tuple(rename[c] for c in v) for k,v in rel.keys.items()},
            joins=rel.joins, payload_row_bytes=rel.payload_row_bytes)

    def string_join(self, left, right, source):
        lc = next(c for c in left.columns if c.name == source.on[0])
        rc = next(c for c in right.columns if c.name == source.right_on[0])
        how = {'inner': 'INNER', 'left': 'LEFT', 'semi': 'LEFT SEMI', 'anti': 'LEFT ANTI'}[source.how]
        projection = 'l.*, r.*' if source.how in {'left', 'inner'} else 'l.*'
        view = self.view(f'SELECT {projection} FROM {left.view} l {how} JOIN {right.view} r '
                         f'ON l.{lc.value}=r.{rc.value} AND l.{lc.present} AND r.{rc.present}')
        joined = self.keyed(Relation(view, left.columns, ()), source.on)
        key = joined.keys[tuple(source.on)][0]
        joined.order = tuple(dict.fromkeys((key, *left.order,
            *(right.order if source.how in {'left', 'inner'} else ()))))
        joined.joins = left.joins + right.joins + 1
        return joined

    def keyed(self, rel, fields):
        fields = tuple(fields)
        if fields in rel.keys:
            return rel
        columns = {c.name: c for c in rel.columns}
        args = [columns[k].value for k in fields] + [columns[k].present for k in fields]
        name, key, valid = self.name('key_fn'), self.name('k'), self.name('valid')
        self.key_functions.append({'name': name, 'fields': fields, 'types': [columns[k].dtype for k in fields]})
        nonnull = ' AND '.join(f'{columns[k].value} IS NOT NULL AND coalesce({columns[k].present}, FALSE)' for k in fields)
        view = self.view(f'SELECT *, {name}({", ".join(args)}) AS {key}, ({nonnull}) AS {valid} FROM {rel.view}')
        return Relation(view, rel.columns, rel.order, rel.decoder, {**rel.keys, fields: (key, valid)},
            rel.joins, rel.payload_row_bytes)

    def packed(self, item, fields):
        """Lossless Python boundary; no schema guesses from a leading sample."""
        import pyarrow as pa
        import lance
        from ..lance.arrow_batches import LANCE_FILE_ROWS
        key, valid, ordinal, payload = (self.name(k) for k in ('k', 'valid', 'o', 'payload'))
        # Joins may duplicate/rebatch wide rows past Binary's 2 GiB offset range,
        # even though each input batch is small. Keep opaque payload offsets 64-bit.
        schema = pa.schema([(key, pa.string()), (valid, pa.bool_()), (ordinal, pa.uint64()), (payload, pa.large_binary())])
        path = self.directory / (self.name('input') + '.lance')
        max_row_bytes = 0
        def batches():
            nonlocal max_row_bytes
            chunk = []
            size = 0
            with closing(self.rows(item)) as rows:
                for i, row in enumerate(rows):
                    encoded = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
                    max_row_bytes = max(max_row_bytes, len(encoded))
                    chunk.append({key: key_of(row, fields), valid: all(row[k] is not None for k in fields),
                        ordinal: i, payload: encoded})
                    size += len(encoded)
                    if len(chunk) == 8192 or size >= 8 * 2**20:
                        yield pa.RecordBatch.from_pylist(chunk, schema=schema)
                        chunk = []
                        size = 0
                if chunk:
                    yield pa.RecordBatch.from_pylist(chunk, schema=schema)
        callback_errors = []
        def guarded_batches():
            try:
                yield from batches()
            except Exception as exc:
                callback_errors.append(exc)
                raise
        try:
            ds = lance.write_dataset(pa.RecordBatchReader.from_batches(schema, guarded_batches()),
                str(path), mode='create', schema=schema, max_rows_per_file=LANCE_FILE_ROWS)
        except Exception:
            # Arrow's C stream can wrap a Python source failure as an OSError.
            # Preserve the original callback error; never reinterpret storage errors.
            if callback_errors:
                raise callback_errors[0] from None
            raise
        table = self.name('t')
        self.sources[table] = {'uri': str(path), 'version': ds.version}
        self.large = self.large or ds.count_rows() >= SORT_STAGE_ROWS
        rel = Relation(self.view(f'SELECT {key}, {valid}, {ordinal}, {payload} FROM {table}'), None,
            (ordinal,), ('payload', payload), {tuple(fields): (key, valid)}, payload_row_bytes=max_row_bytes)
        self.stats['stages'].append({'name': 'python_boundary', 'rows_output': ds.count_rows(),
            'max_payload_row_bytes': max_row_bytes, 'native_batch_rows': self.batch_rows(rel)})
        return rel

    def input(self, item, fields):
        rel = self.typed(item)
        if (rel is not None
                and all(k in {c.name for c in rel.columns if not c.optional} for k in fields)
                and all(json_key_type(c.dtype) for c in rel.columns if c.name in fields)):
            return self.keyed(rel, fields)
        if (isinstance(item.source, KeyedSource) and item.source.kind == 'join'
                and not item.plan.operations and tuple(fields) == item.source.on):
            source = item.source
            return self.join(self.input(source.left, source.on), self.input(source.right, source.right_on), source)
        return self.packed(item, fields)

    def materialize_relation(self, rel):
        import lance
        result = self.query(rel, f'SELECT * FROM {rel.view}', sources=self.sources, views=self.views,
            label='dataset_checkpoint', _dataset_spec={'keys': self.key_functions})
        self.record(result, 'relation_checkpoint')
        table = self.name('checkpoint')
        self.sources[table] = result.source()
        names = lance.dataset(**result.source()).schema.names
        view = self.view('SELECT ' + ', '.join(quote(c) for c in names) + f' FROM {table}')
        return Relation(view, rel.columns, rel.order, rel.decoder, rel.keys.copy(), 0, rel.payload_row_bytes)

    def join(self, left, right, source):
        if self.large and left.joins + right.joins >= 2:
            if left.joins:
                left = self.materialize_relation(left)
            if right.joins:
                right = self.materialize_relation(right)
        lk, lv = left.keys[tuple(source.on)]
        rk, rv = right.keys[tuple(source.right_on)]
        how = {'inner': 'INNER', 'left': 'LEFT', 'semi': 'LEFT SEMI', 'anti': 'LEFT ANTI'}[source.how]
        projection = 'l.*, r.*' if source.how in {'left', 'inner'} else 'l.*'
        view = self.view(f'SELECT {projection} FROM {left.view} l {how} JOIN {right.view} r ON l.{lk}=r.{rk} AND l.{lv} AND r.{rv}')
        order = (lk, *left.order, *(right.order if source.how in {'left', 'inner'} else ()))
        decoder = ('join', left, right, source) if source.how in {'left', 'inner'} else ('left', left)
        payload_row_bytes = left.payload_row_bytes + (right.payload_row_bytes if source.how in {'left', 'inner'} else 0)
        return Relation(view, None, tuple(dict.fromkeys(order)), decoder, left.keys.copy(),
            left.joins + right.joins + 1, payload_row_bytes)

    @staticmethod
    def order_sql(rel):
        return ', '.join(f'{c} ASC NULLS LAST' for c in rel.order)

    def decode(self, rel, row):
        if rel.columns is not None:
            return {c.name: row[c.value] for c in rel.columns if row[c.present]}
        kind, *args = rel.decoder
        if kind == 'payload':
            return pickle.loads(row[args[0]]) if row[args[0]] is not None else None
        if kind == 'left':
            return self.decode(args[0], row)
        left, right, source = args
        a = self.decode(left, row)
        # The right input's stable ordinal is always nonnull before an outer join.
        if row[right.keys[tuple(source.right_on)][1]] is None:
            return a
        b = self.decode(right, row)
        return _merge_row(a, b, source.on, source.right_on, source.suffix)

    def record(self, result, purpose):
        self.stats['stages'].append({'name': 'datafusion', 'purpose': purpose, 'rows_output': result.row_count,
            'query': result.report['diagnostic_directory'], 'seconds': result.report['seconds'],
            'peak_rss_bytes': result.report['peak_rss_bytes']})

    def render_batches(self, rel, *, transform=None, schema=None, transform_batch_rows=None, ordered=False):
        import lance
        # A plain join has no output-order contract. Order restoration belongs
        # only to consumers such as sequential reducers/group batches, whose
        # contiguous keys and within-group callback order require it.
        sql = f'SELECT * FROM {rel.view}'
        if ordered:
            sql += f' ORDER BY {self.order_sql(rel)}'
        separate_sort = ordered and self.large
        if separate_sort:
            # Only keys and row IDs enter the final sort. Carrying opaque or
            # nested payloads through it can exhaust merge workspace even when
            # the sort itself spills. Hydrate from this fixed private snapshot
            # inside the guarded worker, preserving order and callback batches.
            joined = self.query(rel, f'SELECT * FROM {rel.view}', sources=self.sources, views=self.views,
                label='dataset_relations', _dataset_spec={'keys': self.key_functions})
            self.record(joined, 'relations')
            payload_schema = lance.dataset(**joined.source()).schema
            sql = f'SELECT _rowid AS selected_row_id FROM stage ORDER BY {self.order_sql(rel)}'
            result = self.query(rel, sql, sources={'stage': joined}, label='dataset_order',
                schema=schema if schema is not None else payload_schema, batch_transform=transform,
                _dataset_spec={'keys': [], 'transform_batch_rows': transform_batch_rows,
                    'payload_read': {'source': 'stage', 'row_id': 'selected_row_id',
                        'columns': payload_schema.names, 'batch_rows': self.batch_rows(rel)}})
            self.record(result, 'stable_order_and_callbacks')
        else:
            result = self.query(rel, sql, sources=self.sources, views=self.views,
                label='dataset', schema=schema, batch_transform=transform,
                _dataset_spec={'keys': self.key_functions, 'transform_batch_rows': transform_batch_rows})
            self.record(result, 'combined')
        yield from lance.dataset(**result.source()).to_batches(batch_size=self.batch_rows(rel), batch_readahead=1, fragment_readahead=1)

    def render(self, rel):
        with closing(self.render_batches(rel)) as batches:
            for batch in batches:
                for row in batch.to_pylist():
                    yield self.decode(rel, row)

    def render_keyed(self, rel, key):
        with closing(self.render_batches(rel, ordered=True)) as batches:
            for batch in batches:
                for row in batch.to_pylist():
                    yield row[key], self.decode(rel, row)

    def python_rows(self, executor, source, plan):
        try:
            yield from executor._apply_plan(source, plan, _native=False)
        finally:
            child = executor._local_kernel_stats
            if child is not self.stats and child.get('engine') != 'datafusion':
                self.stats.setdefault('python_stages', []).extend(child.get('stages', []))
                workers = self.stats.setdefault('workers_used', [])
                for worker in child.get('workers_used', []):
                    if worker not in workers:
                        workers.append(worker)
                self.stats['peak_pending_tasks'] = max(self.stats.get('peak_pending_tasks', 0), child.get('peak_pending_tasks', 0))
                executor._local_kernel_stats = self.stats

    def rows(self, item):
        source, plan = item.source, item.plan
        executor = item.executor or self.executor
        if isinstance(source, KeyedSource):
            typed = self.typed(PlanInput(source, ops.LogicalPlan(), executor))
            if source.kind == 'exclude_keys':
                from .lance_key_filter import exclude_lance_rows
                base = exclude_lance_rows(self, source)
            elif typed is not None:
                base = self.render(typed)
            elif source.kind == 'join':
                left = self.input(source.left, source.on)
                right = self.input(source.right, source.right_on)
                base = self.render(self.join(left, right, source))
            else:
                rel = self.input(source.left, source.on)
                key, _ = rel.keys[tuple(source.on)]
                rel.order = tuple(dict.fromkeys((key, *rel.order)))
                from .local_tasks import _fold
                def fold():
                    with closing(self.render_keyed(rel, key)) as rows:
                        for _, row in _fold(rows, source.on, source.reducer, source.initial,
                            group_rows=source.max_rows if source.kind == 'groups' else None,
                            output=source.output, encoded_keys=True):
                            yield row
                base = fold()
            with closing(base):
                if plan.operations:
                    yield from self.python_rows(executor, IterableSource(lambda: base), plan)
                else:
                    yield from base
        elif isinstance(source, UnionSource):
            def union():
                for child in source.inputs:
                    with closing(self.rows(child)) as rows:
                        yield from rows
            with closing(union()) as rows:
                if plan.operations:
                    yield from self.python_rows(executor, IterableSource(lambda: rows), plan)
                else:
                    # Concatenation needs no identity-map worker pool. Nested
                    # source unions otherwise keep one pool per level alive.
                    yield from rows
        else:
            yield from self.python_rows(executor, source, plan)


def execute(executor, source, plan, *, arrow=False, sink_schema=None):
    if executor._local_kernel_closed:
        raise RuntimeError('local_execution is closed; execute actions inside its context')
    # Preflight every branch before any source or callback has side effects.
    def validate(item):
        ex = item.executor or executor
        if ex._local_kernel_closed:
            raise RuntimeError('local_execution is closed')
        ex.plan(item.source, item.plan, 'iter_rows')
        if isinstance(item.source, KeyedSource):
            validate(item.source.left)
            if item.source.right:
                validate(item.source.right)
        elif isinstance(item.source, UnionSource):
            for child in item.source.inputs:
                validate(child)
        if ex._local_kernel is None:
            if any(isinstance(op, (ops.SortOp, ops.RepartitionOp, ops.RandomShuffleOp, ops.RandomizeBlockOrderOp)) for op in item.plan.operations):
                ex._require_ray_backend(next(op for op in item.plan.operations if isinstance(op, (ops.SortOp, ops.RepartitionOp, ops.RandomShuffleOp, ops.RandomizeBlockOrderOp))))
            allowed = (ops.MapOp, ops.BoundMapOp, ops.FilterOp, ops.FlatMapOp, ops.MapBatchesOp,
                ops.LimitOp, ops.SelectColumnsOp, ops.DropColumnsOp, ops.RenameColumnsOp,
                ops.AddColumnOp, ops.RandomSampleOp, ops.OperatorLLMMapOp)
            for op in item.plan.operations:
                if not isinstance(op, allowed):
                    raise TypeError(f'unknown logical operation: {type(op).__name__}')
        if ex._local_kernel is not None:
            from .local_tasks import NARROW
            for op in item.plan.operations:
                if not isinstance(op, (*NARROW, ops.MapBatchesOp, ops.LimitOp)) or isinstance(op, ops.MapBatchesOp) and op.zero_copy_batch:
                    raise NotImplementedError(f'local partition kernel does not support {type(op).__name__}')
    stats = {'engine': 'datafusion', 'status': 'running', 'stages': []}
    executor._local_kernel_stats = stats
    started = time.monotonic()
    try:
        validate(PlanInput(source, plan, executor))
        temp = executor._local_kernel.temp_directory if executor._local_kernel else None
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory(prefix='demiflow-dataset-', dir=temp))
            engine = Engine(executor, stack, directory, stats)
            item = PlanInput(source, plan, executor)
            rel = engine.typed(item) if arrow else None
            from .local_tasks import NARROW
            finish_rel = None
            if (arrow and sink_schema is not None and executor._local_kernel is not None
                    and executor._local_kernel.worker_mode == 'process'
                    and plan.operations and all(isinstance(op, NARROW)
                        and not isinstance(op, ops.FlatMapOp) for op in plan.operations)):
                finish_rel = engine.typed(PlanInput(source, ops.LogicalPlan(), executor))
            if finish_rel is not None:
                from functools import partial
                import cloudpickle
                transform = partial(finish_typed_batch, columns=finish_rel.columns,
                    encoded_operations=cloudpickle.dumps(plan.operations), schema=sink_schema)
                stats['stages'].append({'name':'python_batch_callbacks', 'operators':[type(op).__name__ for op in plan.operations],
                    'batch_rows':executor._local_kernel.batch_rows, 'placement':'native_worker_outside_engine'})
                with closing(engine.render_batches(finish_rel, transform=transform, schema=sink_schema,
                        transform_batch_rows=executor._local_kernel.batch_rows)) as batches:
                    yield from batches
            elif rel is not None:
                import pyarrow as pa
                with closing(engine.render_batches(rel)) as batches:
                    for batch in batches:
                        yield pa.RecordBatch.from_arrays([batch.column(c.value) for c in rel.columns],
                            names=[c.name for c in rel.columns])
            elif arrow:
                # Arbitrary callbacks retain Python semantics and execution context.
                with closing(engine.rows(item)) as rows:
                    yield from executor._rows_as_batches(rows, batch_size=8192, batch_format='pyarrow', schema=sink_schema)
            else:
                with closing(engine.rows(item)) as rows:
                    yield from rows
        stats['status'] = 'complete'
    except GeneratorExit:
        stats['status'] = 'closed_early'
        raise
    except BaseException as exc:
        stats.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        stats['seconds'] = time.monotonic() - started
        executor._local_kernel_stats = stats
        executor._last_metadata = ExecutionMetadata(type(source).__name__,
            stages=tuple(StageStats(s['name'], rows_output=s.get('rows_output'), elapsed_seconds=s.get('seconds')) for s in stats['stages']),
            diagnostics=stats)
