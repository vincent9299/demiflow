"""Native streaming publication preserves declared input types in its result."""
import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext


@pytest.mark.parametrize('empty',[False,True])
@pytest.mark.parametrize('output_ref',[None,'receipt'])
def test_typed_passthrough_and_nullable_receipt(tmp_path,empty,output_ref):
    schema=pa.schema([pa.field('id',pa.int32(),nullable=False,metadata={b'role':b'key'}),
        pa.field('text',pa.large_string()),pa.field('nested',pa.list_(pa.large_string())),
        pa.field('keep',pa.bool_())],metadata={b'application':b'typed-pass-through'})
    rows=[] if empty else [{'id':1,'text':'one','nested':['a',None],'keep':True},
        {'id':2,'text':None,'nested':None,'keep':False}]
    table=pa.Table.from_pylist(rows,schema=schema)
    written_schema=pa.schema([schema.field('id'),schema.field('text')])
    expected=schema if output_ref is None else schema.append(pa.field(output_ref,
        pa.struct([pa.field('uri',pa.string()),pa.field('version',pa.int64())])))
    with RunContext.start(tmp_path/'run') as run,data.local_execution():
        saved=data.from_arrow(table).save_lance(tmp_path/'saved.lance',schema=written_schema,
            key='id',stage='saved',max_batch=1,when=lambda row:row['keep'],output_ref=output_ref).materialize()
        try:
            assert saved.schema().equals(expected,check_metadata=True)
            batch=saved.take_batch(10,batch_format='pyarrow')
            assert batch.schema.equals(expected,check_metadata=True)
            result=batch.to_pylist()
            assert [{k:r[k] for k in schema.names} for r in result]==rows
            if output_ref and not empty:
                assert result[0][output_ref]=={'uri':str(tmp_path/'saved.lance'),'version':1}
                assert result[1][output_ref] is None
            assert all(f['status']=='acked' for f in run.journal.execution_commits('action-1'))
        finally:saved.release()
    written=lance.dataset(str(tmp_path/'saved.lance'))
    assert written.schema.equals(written_schema,check_metadata=True)
    assert written.count_rows()==(0 if empty else 1)
