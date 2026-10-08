"""Key-only exclusion preserves scan semantics without staging wide payloads."""
import json
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor


def test_exclusion_keeps_scope_order_duplicates_nulls_and_pinned_payloads(tmp_path):
    api=DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    values=[{'key':v,'seq':i,'payload':{'body':'x'*16384,'parts':[None,str(i)]}} for i,v in
        enumerate(['b','a','c','a',None,'b','z','d'])]
    source=lance.write_dataset(pa.Table.from_pylist(values),tmp_path/'source')
    source=lance.write_dataset(pa.Table.from_pylist(values),tmp_path/'source',mode='append')
    source.delete('seq=6')
    source.optimize.compact_files(target_rows_per_fragment=100)
    pinned=source.version
    expected=[r for r in source.to_table(filter='seq>=1',limit=9).to_pylist() if r['key']!='a']
    blocked=lance.write_dataset(pa.table({'key':['a','a',None],'irrelevant':['large']*3}),tmp_path/'keys')
    ds=api.read_lance(source.uri,version=pinned,filter='seq>=1',limit=9,batch_size=2)
    pending=ds.exclude_keys(api.read_lance(blocked.uri,version=blocked.version),on='key')
    lance.write_dataset(pa.Table.from_pylist([values[0]]),source.uri,mode='overwrite')
    assert pending.take_all()==expected
    stages=pending.execution_metadata().diagnostics['stages']
    scan=next(s for s in stages if s['name']=='lance_key_scan')
    assert scan['rows_output']==9 and scan['payload_columns_in_relation']==0
    hydration=next(s for s in stages if s['name']=='lance_payload_read')
    assert hydration['rows_output']==len(expected) and hydration['max_batch_rows']<=2
    query=next(s for s in stages if s['name']=='datafusion')
    request=json.loads((Path(query['query'])/'request.json').read_text())
    plan=(Path(query['query'])/'plan.txt').read_text()
    assert 'payload' not in plan and 'irrelevant' not in plan
    assert not Path(request['directory']).exists()


@pytest.mark.parametrize('blocked', [[],[1,2,3]])
def test_empty_and_all_excluded(tmp_path,blocked):
    source=lance.write_dataset(pa.table({'key':[1,2,3],'v':['a','b','c']}),tmp_path/'source')
    keys=lance.write_dataset(pa.table({'key':pa.array(blocked,type=pa.int64())}),tmp_path/'keys')
    result=data.read_lance(source.uri,version=1).exclude_keys(data.read_lance(keys.uri,version=1),on='key')
    assert result.take_all()==([] if blocked else source.to_table().to_pylist())


def test_composite_keys_and_early_close(tmp_path):
    api=DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    values=[{'a':1,'b':'x'},{'a':1,'b':'y'},{'a':None,'b':'x'},{'a':2,'b':'x'}]
    source=lance.write_dataset(pa.Table.from_pylist(values),tmp_path/'source')
    keys=lance.write_dataset(pa.Table.from_pylist([values[0],values[2]]),tmp_path/'keys')
    result=api.read_lance(source.uri,version=1).exclude_keys(api.read_lance(keys.uri,version=1),on=['a','b'])
    assert result.take(1)==[values[1]]
    queries=list((tmp_path/'_demiflow/dataset_native/queries').glob('*/request.json'))
    assert queries and all(not Path(json.loads(p.read_text())['directory']).exists() for p in queries)


def test_empty_source_and_typed_projection(tmp_path):
    source=lance.write_dataset(pa.table({'key':pa.array([],type=pa.string()),
        'body':pa.array([],type=pa.large_string())}),tmp_path/'empty')
    assert data.read_lance(source.uri,version=1,columns=['body']).exclude_keys(
        data.from_items([{'key':'missing'}]),on='key').take_all()==[]


def test_rejects_transforms_projection_missing_keys_and_native_failure(tmp_path,monkeypatch):
    from demiflow.execution.datafusion import DataFusionSession
    source=lance.write_dataset(pa.table({'key':[1],'v':['body']}),tmp_path/'source')
    ds=data.read_lance(source.uri,version=1)
    with pytest.raises(ValueError,match='untransformed'):
        ds.map(lambda r:r).exclude_keys(ds,on='key')
    with pytest.raises(ValueError,match='projection'):
        data.read_lance(source.uri,version=1,projection={'key':'key'}).exclude_keys(ds,on='key')
    with pytest.raises(KeyError):ds.exclude_keys(ds,on='missing').take_all()
    calls=[]
    def fail(*a,**kw):calls.append('query');raise RuntimeError('native test failure')
    monkeypatch.setattr(DataFusionSession,'query',fail)
    with pytest.raises(RuntimeError,match='native test failure'):
        ds.exclude_keys(ds,on='key').map(lambda r:calls.append('payload')).take_all()
    assert calls==['query']
