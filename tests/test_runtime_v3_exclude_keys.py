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
