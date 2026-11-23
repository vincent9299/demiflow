"""Original typed SQL reached through public Dataset actions and real source batches."""
import pytest
import pyarrow as pa
import pyarrow.csv as pacsv

from demiflow import data
from demiflow.execution import RunContext
from demiflow.execution import datafusion as host
from demiflow.execution.sealed_plans import load_sealed_plan


def csv_table(path, columns, rows):
    import csv
    with path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerows(rows)
    return data.read_csv(str(path), read_options=pacsv.ReadOptions(block_size=128),
        convert_options=pacsv.ConvertOptions(column_types={k: pa.string() for k in columns}))


@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
@pytest.mark.parametrize('mixed', [False, True])
def test_original_string_join_uses_raw_columns_after_structural_chain(tmp_path, monkeypatch, how, mixed):
    import lance
    def build():
        left = csv_table(tmp_path/'left.csv', ['k','body','drop'],
                         [('x','first','z'), ('','empty','z'), ('x','second','z'), ('absent','tail','z'), ('x','limited','z')])
        left = left.drop_columns('drop').rename_columns({'k':'key'}).select_columns(['key','body']).limit(4)
        if mixed:
            lance.write_dataset(pa.table({'j':['x','x',''], 'r':['a','b','blank']}), str(tmp_path/'right'))
            right = data.read_lance(str(tmp_path/'right'), version=1)
        else:
            right = csv_table(tmp_path/'right.csv', ['j','r'], [('x','a'), ('x','b'), ('','blank')])
        return left.join(right, on="key", right_on="j", how=how)
    from demiflow.execution import arrow_payload
    monkeypatch.setattr(arrow_payload, 'fallback_row', lambda *a, **k: pytest.fail('qualified typed source lost original batch'))
    queries = []
    original = host.query_region_offsets
    def query(sql, **kwargs):
        assert 'ORDER BY' not in sql.upper()  # Ordinary join adds no business-key sort.
        assert all(isinstance(value, pa.Table) for value in kwargs['tables'].values())
        assert kwargs['tables']['lkeys'].column('k0').to_pylist() == ['x','','x','absent']
        queries.append((sql, kwargs['key_functions']))
        # Native hash output can vary between processes; only bounded occurrence
        # offsets, including duplicate-key pairs, define the replay prefix.
        return list(reversed(original(sql, **kwargs)))
    monkeypatch.setattr(host, 'query_region_offsets', query)
    with RunContext.start(tmp_path/'run', limits={'memory_bytes': 1<<30, 'max_row_bytes':8192,
                                                 'max_batch_bytes':65536}) as run, data.local_execution(
            workers=1, partitions=1, worker_mode='thread', memory_bytes=1<<30):
        rows = build().take_all()
        plan = load_sealed_plan(run.directory, 'action-1')['physical']
        assert any(s.stage_id.endswith('-typed-join1') for s in plan.stages)
    assert len(queries) == 1
    assert len(queries[0][1]) == 1  # Canonicalize survivors, not both raw input columns.
    expected = []
    for key, body in [('x','first'), ('','empty'), ('x','second'), ('absent','tail')]:
        matches = [r for j,r in [('x','a'), ('x','b'), ('','blank')] if j == key]
        if how in ('inner','left'):
            expected.extend([{'key':key,'body':body,'j':key,'r':r} for r in matches])
            if how == 'left' and not matches:
                expected.append({'key':key,'body':body})
        elif bool(matches) == (how == 'semi'):
            expected.append({'key':key,'body':body})
    assert rows == expected


def test_fixed_typed_lance_join_keeps_original_sql_when_right_payload_exceeds_index(tmp_path, monkeypatch):
    import lance
    for name, keys in [('left', [1,1,2,None,123]), ('right', [2,1,1,3,4,None] + list(range(200,550)))]:
        columns = {'k':pa.array(keys, type=pa.int32()), name:list(range(len(keys)))}
        if name == 'right':
            columns['body'] = ['r' * 2048] * len(keys)
        lance.write_dataset(pa.table(columns), str(tmp_path/name))
    from demiflow.execution import arrow_payload
    monkeypatch.setattr(arrow_payload, 'fallback_row', lambda *a, **k: pytest.fail('qualified typed source lost original batch'))
    queries = []
    original = host.query_region_offsets
    def query(sql, **kwargs):
        queries.append(kwargs['key_functions'])
        return original(sql, **kwargs)
    monkeypatch.setattr(host, 'query_region_offsets', query)
    with RunContext.start(tmp_path/'run', limits={'memory_bytes':1<<30,'max_row_bytes':8192,'max_batch_bytes':65536}), \
            data.local_execution(workers=1, partitions=1, worker_mode='thread', memory_bytes=1<<30):
        rows = data.read_lance(str(tmp_path/'left'), version=1).join(
            data.read_lance(str(tmp_path/'right'), version=1), on='k', how='left').take_all()
    assert len(queries) == 1 and len(queries[0]) == 2
    assert sorted((r['left'],r.get('right',-1)) for r in rows) == [(0,1),(0,2),(1,1),(1,2),(2,0),(3,-1),(4,-1)]


def test_typed_projection_preserves_schema_and_field_metadata_at_both_public_terminals(tmp_path):
    import lance
    schema = pa.schema([pa.field('k', pa.int32(), nullable=False, metadata={b'unit':b'key'}),
                        pa.field('v', pa.int16(), metadata={b'unit':b'metric'})],
                       metadata={b'contract':b'logical-table-metadata'})
    lance.write_dataset(pa.table({'k':[1,2], 'v':[11,22]}, schema=schema), str(tmp_path/'left'))
    lance.write_dataset(pa.table({'key':pa.array([1], type=pa.int32())}), str(tmp_path/'right'))
    expected_schema = pa.schema([schema.field('k'), schema.field('v').with_name('value')],
                                metadata=schema.metadata)
    with RunContext.start(tmp_path/'run', limits={'memory_bytes':1<<30, 'max_batch_bytes':65536,
                                                 'max_row_bytes':8192}), data.local_execution(
            workers=1, partitions=1, memory_bytes=1<<30, worker_mode='thread'):
        left = data.read_lance(str(tmp_path/'left'), version=1).select_columns(['k','v']).rename_columns({'v':'value'})
        result = left.join(data.read_lance(str(tmp_path/'right'), version=1), on='k', right_on='key', how='semi')
        taken = result.take_batch(5, batch_format='pyarrow')
        result.write_lance(str(tmp_path/'target'), mode='create')
    written = lance.dataset(str(tmp_path/'target')).to_table()
    for table in (taken, written):
        assert table.to_pylist() == [{'k':1, 'value':11}]
        assert table.schema.equals(expected_schema, check_metadata=True)


@pytest.mark.parametrize('reason', ['udf', 'native_grant'])
def test_typed_join_ineligible_conditions_keep_the_original_input(tmp_path, monkeypatch, reason):
    def no_sql(*args, **kwargs):
        pytest.fail('ineligible join requested SQL')
    monkeypatch.setattr(host, 'query_region_offsets', no_sql)
    grant = 128 << 20 if reason == 'native_grant' else 1 << 30
    with RunContext.start(tmp_path/'run', limits={'memory_bytes':grant}), data.local_execution(
            workers=1, partitions=1, worker_mode='thread', memory_bytes=grant):
        left = csv_table(tmp_path/'left.csv', ['k','v'], [('a','1'),('b','2'),('a','3')])
        right = csv_table(tmp_path/'right.csv', ['k','r'], [('a','4')])
        if reason == 'udf':
            left = left.map(lambda row: row)
        rows = left.join(right, on='k').take_all()
    assert sorted(rows, key=lambda row:row['v']) == [{'k':'a','v':'1','r':'4'}, {'k':'a','v':'3','r':'4'}]


def test_typed_join_completed_resume_does_not_scan_or_run_sql(tmp_path, monkeypatch):
    left_path, right_path = tmp_path/'left.csv', tmp_path/'right.csv'
    csv_table(left_path, ['k','v'], [('x','a'), ('x','b'), ('z','c')])
    csv_table(right_path, ['k','r'], [('x','c')])
    def build():
        def read(path, fields):
            return data.read_csv(str(path), read_options=pacsv.ReadOptions(block_size=128),
                convert_options=pacsv.ConvertOptions(column_types={k:pa.string() for k in fields}))
        return read(left_path, ['k','v']).limit(2).join(read(right_path, ['k','r']), on='k')
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits={'memory_bytes':1<<30}) as run, data.local_execution(
                workers=1, partitions=1, worker_mode='thread', memory_bytes=1<<30):
            ref = run.ref
            expected = build().take_all()
            commits = run.journal.execution_commits('action-1')
            raise Interrupted()
    def forbidden(*args, **kwargs):
        pytest.fail('completed result re-executed')
    monkeypatch.setattr(host, 'query_region_offsets', forbidden)
    monkeypatch.setattr(pacsv, 'open_csv', forbidden)
    with RunContext.resume(ref) as run, data.local_execution(workers=1, partitions=1, worker_mode='thread', memory_bytes=1<<30):
        assert build().take_all() == expected
        assert run.journal.execution_commits('action-1') == commits


def test_csv_shared_batch_cursor_and_projection_keep_authenticated_occurrences(tmp_path):
    from dataclasses import replace
    from demiflow.data.sources import FileSource
    from demiflow.execution.source_readers import FileReader
    from demiflow.execution.arrow_payload import ArrowBatchCache, is_arrow_record
    from demiflow.execution.record_schema import RecordSchemas
    from test_runtime_v3_s import FakeStore, make_bounds, make_snapshot, first_split, make_services, drain
    path = tmp_path/'rows.csv'
    path.write_text('k,v\na,1\nb,2\nc,3\nd,4\n')
    source = FileSource('csv', (str(path),), {'read_options':pacsv.ReadOptions(block_size=128),
        'convert_options':pacsv.ConvertOptions(column_types={'k':pa.string(),'v':pa.string()})})
    bounds = make_bounds(max_row_bytes=8192, max_batch_bytes=65536)
    store = FakeStore()
    snapshot = make_snapshot(source, store, bounds=bounds)
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    snapshot = replace(snapshot, immutable_identity={**_thaw_json(snapshot.immutable_identity),'record_carrier_v1':'mapped_arrow'})
    split = first_split(snapshot, source, bounds=bounds)
    services = make_services(store, source, snapshot.source_id, bounds=bounds)
    reader = FileReader(snapshot, services=services)
    reader.open(split, None)
    first = [reader.next_record().record for _ in range(2)]
    assert all(is_arrow_record(r) for r in first)
    cursor = reader.snapshot()
    reader.close()
    restored = FileReader(snapshot, services=services)
    restored.open(split, cursor)
    tail = drain(restored)
    restored.close()
    cache = ArrowBatchCache(store, bounds, services=services)
    assert [cache.row(r) for r in tail] == [{'k':'b','v':'2'},{'k':'c','v':'3'},{'k':'d','v':'4'}]
    assert (tail[0].record_id, tail[0].sequence) == (first[-1].record_id, first[-1].sequence)
    schemas = RecordSchemas(services)
    projected = cache.project(tail[0], (1,0), ('value','key'), schemas)
    assert projected.record_id == tail[0].record_id and projected.payload_ref != tail[0].payload_ref
    assert ArrowBatchCache(store, bounds).row(projected) == {'value':'2','key':'b'}
    with pytest.raises(Exception, match='schema differs'):
        ArrowBatchCache(store, bounds).row(replace(projected, schema_ref=tail[0].schema_ref))


def test_typed_join_output_bound_falls_back_on_the_same_finite_inputs(tmp_path, monkeypatch):
    opened = []
    original = pacsv.open_csv
    def opening(*args, **kwargs):
        opened.append(args[0])
        return original(*args, **kwargs)
    monkeypatch.setattr(pacsv, 'open_csv', opening)
    monkeypatch.setattr(host, 'query_region_offsets', lambda *a, **k: pytest.fail('unbounded pairs reached SQL'))
    with RunContext.start(tmp_path/'run', limits={'memory_bytes':1<<30}), data.local_execution(
            workers=1, partitions=1, worker_mode='thread', memory_bytes=1<<30):
        left = csv_table(tmp_path/'left.csv', ['k'], [('left'+str(i),) for i in range(300)])
        right = csv_table(tmp_path/'right.csv', ['k'], [('right'+str(i),) for i in range(300)])
        assert left.join(right, on='k').take_all() == []
    assert len(opened) == 2


def test_typed_join_sql_failure_is_not_retried_through_a_second_algorithm(tmp_path, monkeypatch):
    from demiflow.execution.kernels import join_spool
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    calls = []
    original = host.query_region_offsets
    def failing(sql, **kwargs):
        calls.append(sql)
        return original('SELECT absent_column FROM lkeys', **kwargs)
    monkeypatch.setattr(host, 'query_region_offsets', failing)
    monkeypatch.setattr(join_spool, 'join_pairs', lambda *a, **k: pytest.fail('SQL failure retried'))
    with pytest.raises(DataflowError, match='absent_column'):
        with RunContext.start(tmp_path/'run', limits={'memory_bytes':1<<30}) as run, data.local_execution(
                workers=1, partitions=1, worker_mode='thread', memory_bytes=1<<30):
            left = csv_table(tmp_path/'left.csv', ['k'], [('x',)])
            right = csv_table(tmp_path/'right.csv', ['k'], [('x',)])
            try:
                left.join(right, on='k').take_all()
            except DataflowError:
                assert not run.journal.execution_commits('action-1')
                raise
    assert len(calls) == 1
