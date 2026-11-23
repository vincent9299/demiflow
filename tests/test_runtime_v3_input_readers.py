"""Typed materialized inputs must not become empty or lose primitive dtype."""
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError


@pytest.mark.parametrize('kind', ['arrow', 'pandas'])
def test_three_rows_nullable_values_nondefault_index(kind,tmp_path):
    expected=[{'id':2,'text':'two'},{'id':None,'text':None},{'id':2,'text':'two'}]
    table=pa.Table.from_pylist(expected,schema=pa.schema([('id',pa.int64()),('text',pa.string())]))
    frame=pd.DataFrame({'id':pd.Series([2,None,2],dtype='Int64'),'text':['two',None,'two']})
    frame.index=[9,3,11]
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        ds=data.from_arrow(table) if kind=='arrow' else data.from_pandas(frame)
        assert ds.take_all()==expected
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'


@pytest.mark.parametrize('kind', ['arrow','pandas','numpy'])
def test_empty_typed_input_is_really_empty(kind,tmp_path):
    values={'arrow':pa.table({'id':pa.array([],type=pa.int64())}), 'pandas':pd.DataFrame({'id':pd.Series([],dtype='int64')}), 'numpy':np.empty((0,2),dtype=np.float32)}
    with RunContext.start(tmp_path/'run'), data.local_execution():
        assert getattr(data,'from_'+kind)(values[kind]).take_all()==[]


@pytest.mark.parametrize('array', [np.array([2,1,1],dtype=np.int16),np.array([[-0.,np.nan],[1.,2.]],dtype=np.float32),np.array(7,dtype=np.uint16)])
def test_numpy_scalar_and_two_dimensional_rows_preserve_dtype(array,tmp_path):
    expected=[array[()]] if array.ndim==0 else list(array)
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        rows=data.from_numpy(array).take_all()
        assert len(rows)==len(expected)
        for row,want in zip(rows,expected):
            value=row['data']
            assert type(value) is type(want)
            assert value.dtype==want.dtype and value.shape==want.shape
            np.testing.assert_array_equal(value,want)
            if value.dtype.kind=='f': np.testing.assert_array_equal(np.signbit(value),np.signbit(want))
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'


@pytest.mark.parametrize('value',[np.array([object()],dtype=object),np.array([(1,)],dtype=[('x','i4')])])
def test_unsafe_numpy_dtype_rejected_as_contract_not_budget(value):
    with pytest.raises(DataflowError) as caught:
        data.from_numpy(value)
    assert caught.value.code=='INVALID_CONTRACT'
