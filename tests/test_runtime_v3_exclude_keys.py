import pyarrow as pa
import lance
from demiflow import data
from demiflow.execution.run_context import RunContext


def test_exclude_keys_keeps_left_scan_order_duplicates_and_null(tmp_path):
    rows=[{'k':'b','v':3},{'k':'a','v':2},{'k':'b','v':3},{'k':None,'v':7}]
    uri=str(tmp_path/'source.lance')
    lance.write_dataset(pa.Table.from_pylist(rows),uri,mode='create')
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        result=data.read_lance(uri,version=1).exclude_keys(data.from_items([{'k':'a'},{'k':None}]),on='k').take_all()
        assert result==[rows[0],rows[2],rows[3]]
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'


def test_exclude_filters_only_keys_then_hydrates_pinned_fragment_row_ids(tmp_path):
    rows=[{'k':'drop','payload':'x'*200000},{'k':'b','payload':'keep-b'},
          {'k':'gone','payload':'deleted'},{'k':None,'payload':'keep-null'},
          {'k':'a','payload':'keep-a'},{'k':'drop','payload':'y'*200000}]
    uri=str(tmp_path/'many.lance')
    ds=lance.write_dataset(pa.Table.from_pylist(rows),uri,mode='create',max_rows_per_file=2,max_rows_per_group=2)
    ds.delete("k = 'gone'")
    pinned=ds.version
    lance.write_dataset(pa.Table.from_pylist([{'k':'later','payload':'new head'}]),uri,mode='append')
    with RunContext.start(tmp_path/'run',limits={'max_row_bytes':65536,'max_batch_bytes':262144}),data.local_execution():
        output=data.read_lance(uri,version=pinned).exclude_keys(data.from_items([{'k':'drop'}]),on='k').take_all()
    assert output==[rows[1],rows[3],rows[4]]


def test_exclude_empty_left_and_right_keep_public_semantics(tmp_path):
    uri=str(tmp_path/'rows.lance')
    schema=pa.schema([('k',pa.string()),('v',pa.int64())])
    lance.write_dataset(pa.Table.from_pylist([{'k':'b','v':2},{'k':'a','v':1}],schema=schema),uri,mode='create')
    with RunContext.start(tmp_path/'run'),data.local_execution():
        empty=data.from_arrow(pa.Table.from_pylist([],schema=pa.schema([('k',pa.string())])))
        assert data.read_lance(uri,version=1).exclude_keys(empty,on='k').take_all()==[{'k':'b','v':2},{'k':'a','v':1}]
        assert data.read_lance(uri,version=1,filter='v < 0').exclude_keys(data.from_items([{'k':'a'}]),on='k').take_all()==[]


def test_exclude_stream_total_may_exceed_individual_row_bound(tmp_path):
    rows=[{'k':str(i),'payload':'x'*40000} for i in range(3)]
    uri=str(tmp_path/'wide.lance')
    lance.write_dataset(pa.Table.from_pylist(rows),uri,mode='create')
    with RunContext.start(tmp_path/'run',limits={'max_row_bytes':65536,'max_batch_bytes':262144}),data.local_execution():
        assert data.read_lance(uri).exclude_keys(data.from_items([{'k':'absent'}]),on='k').take_all()==rows


def test_public_key_exclusion_uses_only_key_sql_and_bulk_hydrates_survivors(tmp_path, monkeypatch):
    from demiflow.execution import datafusion
    from demiflow.execution.sealed_plans import load_sealed_plan
    from demiflow.execution.contracts.relational_region import KEY_SQL_SUFFIX

    keys = [None, *range(256), 127]
    source = str(tmp_path / 'fixed.lance')
    lance.write_dataset(pa.table({'k': keys, 'n': range(len(keys))}), source)
    probes, reads = [], []
    query = datafusion.query_region_offsets
    take = lance.LanceDataset._take_rows
    def key_only(sql, **kwargs):
        assert 'LEFT ANTI JOIN' in sql
        assert kwargs['selection']
        assert all(rows.type == pa.binary() for rows in kwargs['tables'].values())
        probes.append(tuple(map(len, kwargs['tables'].values())))
        return query(sql, **kwargs)
    def selected_rows(dataset, ids, *, columns, **kwargs):
        reads.append((len(ids), tuple(columns)))
        return take(dataset, ids, columns=columns, **kwargs)
    monkeypatch.setattr(datafusion, 'query_region_offsets', key_only)
    monkeypatch.setattr(lance.LanceDataset, '_take_rows', selected_rows)
    expected = [{'k': k, 'n': i} for i, k in enumerate(keys) if k not in (2, 20)]
    for grant, uses_sql in ((64, False), (1024, True)):
        probes.clear()
        reads.clear()
        with RunContext.start(tmp_path / str(grant), limits={'memory_bytes': grant << 20,
                'max_row_bytes': 65536, 'max_batch_bytes': 2 << 20}) as run, data.local_execution(
                workers=2, partitions=1, worker_mode='thread', memory_bytes=grant << 20):
            result = data.read_lance(source, version=1, batch_size=8192).exclude_keys(
                data.from_items([{'k': None}, {'k': 2}, {'k': 20}, {'k': 20}]), on='k').take_all()
            stages = load_sealed_plan(run.directory, 'action-1', version='v3')['physical'].stages
        assert result == expected
        assert any(s.stage_id.endswith(KEY_SQL_SUFFIX) for s in stages) == uses_sql
        assert probes == ([(258, 4)] if uses_sql else [])
        assert reads == [(256, ('k', 'n'))]
