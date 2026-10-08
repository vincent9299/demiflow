import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.execution.executors.local import LocalDatasetExecutor


@pytest.mark.parametrize('empty',[False,True])
def test_arrow_copy_honors_projection_filter_version_and_empty_schema(tmp_path,monkeypatch,empty):
    source=str(tmp_path/'source.lance');target=str(tmp_path/'target.lance')
    schema=pa.schema([('id',pa.int64()),('values',pa.list_(pa.struct([('x',pa.string())])))])
    lance.write_dataset(pa.Table.from_pylist([{'id':1,'values':None},{'id':2,'values':[]},{'id':3,'values':[{'x':None}]}],schema=schema),source)
    lance.write_dataset(pa.Table.from_pylist([{'id':4,'values':[{'x':'later'}]}],schema=schema),source,mode='append')
    def no_python_rows(*args,**kwargs): raise AssertionError('Pure copy must retain Arrow batches')
    monkeypatch.setattr(LocalDatasetExecutor,'_apply_plan',no_python_rows)
    with data.local_execution(workers=2,partitions=2):
        receipt=data.read_lance(source,version=1,filter='false' if empty else 'id >= 2',batch_size=1).write_lance(
            target,mode='create',schema=schema,return_receipt=True)
    assert receipt.committed_version==1
    assert lance.dataset(target).to_table().to_pylist()==([] if empty else [{'id':2,'values':[]},{'id':3,'values':[{'x':None}]}])
    projection_schema=pa.schema([('id',pa.int64()),('n',pa.int64())])
    data.read_lance(source,version=1,projection={'id':'id','n':'array_length(values)'},limit=2).write_lance(
        str(tmp_path/'projection.lance'),mode='create',schema=projection_schema)
    assert lance.dataset(str(tmp_path/'projection.lance')).to_table().to_pylist()==[{'id':1,'n':None},{'id':2,'n':0}]
