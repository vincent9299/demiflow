"""Typed semantics and explicit query scopes on owned tiny Lance fixtures.

These trusted-reader calls do not execute a Notebook/analysis SQL/OCI job.
"""
import math

import pyarrow as pa
import pytest

from demiflow.workbench.analysis import case_query
from demiflow.forge.schema import ForgeError


def fixed(tmp_path,values):
    import lance
    path=tmp_path/'values.lance'
    pa.set_cpu_count(1);pa.set_io_thread_count(1)
    lance.write_dataset(pa.table({'value':pa.array(values,type=pa.float64())}),str(path))
    return lance.dataset(str(path),version=1),dict(rows=len(values),schema=[dict(name='value',type='float64',nullable=True)])


def test_nonfinite_values_keep_typed_predicate_sort_and_group_semantics(tmp_path):
    table,source=fixed(tmp_path,[None,-1.,float('nan'),1.,float('inf'),3.,float('-inf')])
    base=dict(kind='cases',columns=['value'])
    filtered=case_query.read(table,source,{**base,'filters':dict(op='range',field='value',value=dict(lower=0,upper=2))})
    assert [row['value'] for row in filtered['rows']]==[1.] and filtered['coverage']['complete']
    for op,value in [('eq',1.),('in',[1.,3.]),('is_null',True)]:
        result=case_query.read(table,source,{**base,'filters':dict(op=op,field='value',value=value)})
        assert [row['value'] for row in result['rows']]==({'eq':[1.],'in':[1.,3.],'is_null':[None]}[op])
    descending=case_query.read(table,source,{**base,'sort':[dict(field='value',direction='desc')],'grouping':['value']})
    assert [row['value'] for row in descending['rows'][:3]]==[3.,1.,-1.]
    assert sum(group['count'] for group in descending['groups'])==7
    assert {row['value']['value'] for row in descending['rows'][3:] if type(row['value']) is dict}=={'nan','inf','-inf'}
    assert len(descending['scope_digest'])==64


def test_full_is_default_and_a_tail_match_cannot_be_replaced_by_implicit_sampling(tmp_path):
    table,source=fixed(tmp_path,[0.]*1000+[7.])
    query=dict(kind='cases',columns=['value'],filters=dict(op='eq',field='value',value=7))
    with pytest.raises(ForgeError) as error:case_query.read(table,source,query)
    assert error.value.code=='resource_limit' and 'explicitly choose sample' in str(error.value)
    sample=case_query.read(table,source,{**query,'scope':'sample'})
    assert sample['matching_in_scope']==0 and sample['coverage']['rows_scanned']==1000
    assert sample['coverage']['requested_scope']=='sample' and not sample['coverage']['complete']
    with pytest.raises(ForgeError):case_query.specification({**query,'scope':'pretend-full'},source)


def test_native_bytes_refuse_full_population_and_only_explicit_sample_can_report_partial():
    class Batch:
        nbytes=1024*1024;num_rows=1
        def get_total_buffer_size(self):return self.nbytes
        def to_pylist(self):return [dict(value=1.,_rowid=0)]
    class Reader:
        def __iter__(self):return iter([Batch() for _ in range(10)])
        def close(self):pass
    class Snapshot:
        schema=pa.schema([('value',pa.float64())])
        def scanner(self,**settings):
            class Scan:
                def to_batches(self):return Reader()
            return Scan()
    source=dict(rows=10,schema=[dict(name='value',type='float64',nullable=True)])
    with pytest.raises(ForgeError) as error:case_query.read(Snapshot(),source,dict(kind='cases',columns=['value']))
    assert error.value.code=='resource_limit'
    value=case_query.read(Snapshot(),source,dict(kind='cases',columns=['value'],scope='sample'))
    assert value['coverage']['rows_scanned']==8 and value['coverage']['stopped_by']=='bytes'


def test_predicates_treat_null_nonfinite_and_missing_fields_separately():
    predicate=dict(op='range',field='value',value=dict(lower=0,upper=3))
    assert not any(case_query.matches(dict(value=value),predicate) for value in (None,math.nan,math.inf,-math.inf))
    with pytest.raises(ForgeError) as error:case_query.matches({},predicate)
    assert error.value.code=='operation_failed'
