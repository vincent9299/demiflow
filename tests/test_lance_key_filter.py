"""Key-only exclusion preserves scan semantics without staging wide payloads."""
import os

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext
from _lance_key_fixtures import install_lance_key_probe
from _worker_event_fixtures import emit, events


def test_exclusion_keeps_scope_order_duplicates_nulls_and_pinned_payloads(tmp_path, monkeypatch):
    proof_path = install_lance_key_probe(tmp_path, monkeypatch)
    executor = LocalDatasetExecutor(resource_root=tmp_path)
    api=DataAPI(executor)
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
    with RunContext.start(tmp_path / 'run') as run:
        assert pending.take_all()==expected
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    proof = events(proof_path)
    scans = [e for e in proof if e['event'] == 'key_scan']
    hydration = [e for e in proof if e['event'] == 'hydrate']
    ordered = [e for e in proof if e['event'] == 'key_order']
    assert sum(e['rows'] for e in scans) == 9
    assert all(e['version'] == pinned and e['columns'] == ['key'] for e in scans)
    assert sum(e['rows'] for e in hydration) == len(expected)
    assert all(e['version'] == pinned and e['rows'] == e['requested'] <= 2 for e in hydration)
    assert sum(e['rows'] for e in ordered) == len(expected)
    # Worker probes also assert that join/sort contain keys/row IDs only.
    assert all(e['pid'] != os.getpid() for e in proof)
    _assert_resource_cleanup(run)
    executor.close()


@pytest.mark.parametrize('blocked', [[],[1,2,3]])
def test_empty_and_all_excluded(tmp_path,blocked):
    source=lance.write_dataset(pa.table({'key':[1,2,3],'v':['a','b','c']}),tmp_path/'source')
    keys=lance.write_dataset(pa.table({'key':pa.array(blocked,type=pa.int64())}),tmp_path/'keys')
    result=data.read_lance(source.uri,version=1).exclude_keys(data.read_lance(keys.uri,version=1),on='key')
    assert result.take_all()==([] if blocked else source.to_table().to_pylist())


def test_composite_keys_and_early_close(tmp_path):
    executor = LocalDatasetExecutor(resource_root=tmp_path)
    api=DataAPI(executor)
    values=[{'a':1,'b':'x'},{'a':1,'b':'y'},{'a':None,'b':'x'},{'a':2,'b':'x'}]
    source=lance.write_dataset(pa.Table.from_pylist(values),tmp_path/'source')
    keys=lance.write_dataset(pa.Table.from_pylist([values[0],values[2]]),tmp_path/'keys')
    result=api.read_lance(source.uri,version=1).exclude_keys(api.read_lance(keys.uri,version=1),on=['a','b'])
    with RunContext.start(tmp_path / 'run') as run:
        assert result.take(1)==[values[1]]
    _assert_resource_cleanup(run)
    executor.close()


def test_empty_source_and_typed_projection(tmp_path):
    source=lance.write_dataset(pa.table({'key':pa.array([],type=pa.string()),
        'body':pa.array([],type=pa.large_string())}),tmp_path/'empty')
    assert data.read_lance(source.uri,version=1,columns=['body']).exclude_keys(
        data.from_items([{'key':'missing'}]),on='key').take_all()==[]


def test_rejects_transforms_projection_missing_keys_and_native_failure(tmp_path,monkeypatch):
    from demiflow.errors import InvalidLanceRequest
    source=lance.write_dataset(pa.table({'key':[1],'v':['body']}),tmp_path/'source')
    ds=data.read_lance(source.uri,version=1)
    with pytest.raises(ValueError,match='untransformed'):
        ds.map(lambda r:r).exclude_keys(ds,on='key')
    with pytest.raises(ValueError,match='projection'):
        data.read_lance(source.uri,version=1,projection={'key':'key'}).exclude_keys(ds,on='key')
    with pytest.raises(InvalidLanceRequest, match="Lance columns not found:.*missing"):
        ds.exclude_keys(ds,on='missing').take_all()
    proof_path = install_lance_key_probe(tmp_path, monkeypatch, fail_relation=True)
    def payload(row):
        emit(proof_path, 'payload')
        return row
    with pytest.raises(RuntimeError,match='native test failure'):
        ds.exclude_keys(ds,on='key').map(payload).take_all()
    proof = events(proof_path)
    assert len([e for e in proof if e['event'] == 'relation']) == 1
    assert all(e['event'] not in {'payload', 'hydrate'} for e in proof)


def _assert_resource_cleanup(run):
    with run.journal._connection() as db:
        assert db.execute('SELECT COUNT(*) FROM resource_allocations').fetchone()[0] > 0
        assert db.execute("SELECT COUNT(*) FROM resource_allocations WHERE cleanup!='CONFIRMED'").fetchone()[0] == 0
