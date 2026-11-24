"""Type-aware relational SQL construction; no source, process or action ownership.

The query-local builders are retained from dataset_native.Engine. Callers bind
already prepared input tables; host execution and payload reads have separate
owners. No query construction reads a run, a Dataset or a storage directory.
"""
import itertools
import json
from dataclasses import dataclass, field


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

class RelationalSQL:
    def __init__(self):
        self.views, self.key_functions = {}, []
        self.counter = itertools.count()


    def name(self, prefix='n'):
        return f'{prefix}{next(self.counter)}'

    def view(self, sql):
        name = self.name('v')
        self.views[name] = sql
        return name

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

    def join(self, left, right, source):
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

    def input_table(self, name, fields, *, ordinal='off'):
        """Bind schema-known columns; source versions/reads belong to the caller."""
        present, order = self.name('p'), self.name('o')
        columns = [Column(f.name, self.name('c'), present, f.type) for f in fields]
        projection = [f'{quote(c.name)} AS {c.value}' for c in columns]
        projection += [f'TRUE AS {present}', f'{quote(ordinal)} AS {order}']
        view = self.view(f'SELECT {", ".join(projection)} FROM {quote(name)}')
        return Relation(view, columns, (order,))

    def query(self, sql):
        """One read-only statement, preserving each query-local shared view once."""
        if not self.views:
            return sql
        return 'WITH ' + ', '.join(f'{name} AS ({body})' for name, body in self.views.items()) + ' ' + sql


def exclusion_query(dtype):
    """Original typed anti-join: compare keys and return selected left offsets."""
    import pyarrow as pa
    from types import SimpleNamespace

    if not json_key_type(dtype):
        raise ValueError('typed equality requires a JSON-equivalent Arrow key')
    builder = RelationalSQL()
    fields = pa.schema([('k', dtype)])
    left = builder.input_table('lkeys', fields)
    right = builder.input_table('rkeys', fields)
    right = builder.clone_typed(right)
    source = SimpleNamespace(on=('k',), right_on=('k',), how='anti')
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        joined = builder.string_join(left, right, source)
    else:
        joined = builder.join(builder.keyed(left, source.on), builder.keyed(right, source.right_on), source)
    # The host returns a membership bitmap, not ordered rows. Its consumer walks
    # the fixed source offsets in order; sorting here duplicates that work and
    # retains a whole result unnecessarily inside the SQL child's memory grant.
    sql = builder.query(f'SELECT {left.order[0]} AS off FROM {joined.view}')
    return sql, builder.key_functions


def offset_join_query(first, second, maximum):
    """Build both joins with the original templates, retaining each logical boundary."""
    import pyarrow as pa
    from types import SimpleNamespace

    if type(maximum) is not int or maximum < 1:
        raise ValueError('offset query requires a positive result ceiling')
    if first not in ('inner', 'left') or second not in ('inner', 'left'):
        raise ValueError('offset region requires inner/left joins')
    builder = RelationalSQL()
    fields = pa.schema([('k', pa.binary()), ('off', pa.int64())])
    relations = []
    for name in ('akeys', 'bkeys', 'ckeys'):
        rel = builder.input_table(name, fields, ordinal='ord')
        column = rel.columns[0]
        valid = builder.name('valid')
        rel.view = builder.view(f'SELECT *, {column.value} IS NOT NULL AS {valid} FROM {rel.view}')
        rel.keys[('k',)] = (column.value, valid)
        relations.append(rel)
    a, b, c = relations
    middle = builder.join(a, b, SimpleNamespace(on=('k',), right_on=('k',), how=first))
    joined = builder.join(middle, c, SimpleNamespace(on=('k',), right_on=('k',), how=second))
    ao, bo, co = (rel.columns[1].value for rel in relations)
    ai, bi, ci = (rel.order[0] for rel in relations)
    combined = builder.view(
        f'SELECT CAST(0 AS BIGINT) AS phase, {ao} AS ao, {bo} AS bo, '
        f'CAST(NULL AS BIGINT) AS co, {ai} AS ai, {bi} AS bi, CAST(NULL AS BIGINT) AS ci '
        f'FROM {middle.view} UNION ALL '
        f'SELECT CAST(1 AS BIGINT), {ao}, {bo}, {co}, {ai}, {bi}, {ci} FROM {joined.view}')
    order = builder.order_sql(Relation(combined, None, ('phase', 'ai', 'bi', 'ci')))
    return builder.query(f'SELECT phase, ao, bo, co FROM {combined} ORDER BY {order} LIMIT {maximum + 1}')


def typed_join_query(types, how):
    """Original typed join templates over already-bound key columns and row offsets."""
    import pyarrow as pa
    from types import SimpleNamespace
    if not types or how not in ('inner', 'left', 'semi', 'anti'):
        raise ValueError('invalid typed join')
    builder = RelationalSQL()
    keys = tuple('k' + str(index) for index in range(len(types)))
    schema = pa.schema([*zip(keys, types), ('off', pa.int64())])
    left = builder.input_table('lkeys', schema)
    right = builder.input_table('rkeys', schema)
    source = SimpleNamespace(on=keys, right_on=keys, how=how)
    if len(types) == 1 and (pa.types.is_string(types[0]) or pa.types.is_large_string(types[0])):
        # Compare raw strings before the canonical-key UDF sees the surviving rows.
        joined = builder.string_join(left, right, source)
    else:
        joined = builder.join(builder.keyed(left, keys), builder.keyed(right, keys), source)
    lo = left.columns[-1].value
    ro = right.columns[-1].value if how in ('inner', 'left') else 'CAST(NULL AS BIGINT)'
    return builder.query(f'SELECT {lo} AS lo, {ro} AS ro FROM {joined.view}'), builder.key_functions


def query_typed_join(tables, types, *, how, left_count, right_count, retained, query, check_stop):
    from demiflow.execution.contracts.relational_region import SQL_CHILD_RSS, SQL_PARENT_WINDOW
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    # Prove a complete result bound before starting SQL. Once started, errors
    # and cancellation propagate; there is no retry through another algorithm.
    upper = left_count if how in ('semi', 'anti') else left_count * max(1, right_count)
    if upper * 256 + retained > SQL_PARENT_WINDOW or upper * 224 > SQL_CHILD_RSS // 16:
        return None
    sql, functions = typed_join_query(types, how)
    pairs = query(sql, tables=tables, max_pairs=upper, rss_bytes=SQL_CHILD_RSS,
                                         check_stop=check_stop, key_functions=functions)
    for li, ri in pairs:
        if type(li) is not int or not 0 <= li < left_count or (
                ri is not None and (type(ri) is not int or not 0 <= ri < right_count)):
            raise DataflowError('INTERNAL_PROTOCOL', 'typed join returned invalid input offsets')
    # Recovery replays an emitted prefix. Order only these pre-admitted offset
    # pairs, never the business keys or full input rows. The 256 bytes/pair
    # above include tuple/int/list storage, sort keys and Timsort scratch.
    pairs.sort(key=lambda pair: (pair[0], -1 if pair[1] is None else pair[1]))
    return pairs
