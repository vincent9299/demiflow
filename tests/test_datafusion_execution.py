"""Real native engine tests: semantics, process lifetime and admission bounds."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest
import pyarrow as pa

pytest.importorskip('datafusion')
lance = pytest.importorskip('lance')
from demiflow import data
from demiflow.execution.datafusion import DataFusionOptions, DataFusionSession
from demiflow.execution.datafusion import DataFusionQueryError, DataFusionQueryTimeout


def options(**changes):
    return DataFusionOptions(memory_bytes=128*1024**2, max_rss_bytes=2*1024**3,
        threads=1, partitions=2, timeout_s=15, admission_timeout_s=15, **changes)


def delayed(batch):
    time.sleep(0.3)
    return batch


def hanging(batch):
    time.sleep(30)
    return batch


def invalid_batch(batch):
    return [{'unsafe': 'not an Arrow batch'}]


def sliced_nested_batch(batch):
    values=pa.array([
        {'inner':{'text':'discard'},'items':[{'n':-1}]},
        None,
        {'inner':None,'items':[]},
        {'inner':{'text':None},'items':[None,{'n':3}]},
        {'inner':{'text':'keep'},'items':None},
    ])
    return pa.RecordBatch.from_arrays([values.slice(1,4)],names=['payload'])


def test_fixed_versions_chained_queries_and_regular_writer(tmp_path):
    source = tmp_path / 'input.lance'
    lance.write_dataset(pa.table({'k': [1, 1, 2, None], 'v': [2, 3, 4, 9]}), str(source))
    lance.write_dataset(pa.table({'k': [99], 'v': [99]}), str(source), mode='overwrite')
    output = tmp_path / 'published.lance'
    with DataFusionSession(resource_directory=tmp_path/'resources', options=options()) as session:
        first = session.query('SELECT k, sum(v) AS total FROM input GROUP BY k',
            sources={'input': {'uri': str(source), 'version': 1}})
        second = session.query('SELECT k,total FROM grouped WHERE k IS NOT NULL', sources={'grouped': first})
        assert second.row_count == 2
        second.dataset().write_lance(str(output), mode='create')
        assert second.report['success'] and second.report['complete']
        assert sorted(lance.dataset(str(output)).to_table().to_pylist(), key=lambda r:r['k']) == [{'k':1,'total':5},{'k':2,'total':4}]
        expired = second.uri
    assert not Path(expired).exists()
    with pytest.raises(RuntimeError, match='expired'):
        second.source()
    assert lance.dataset(str(output)).count_rows() == 2


def test_join_null_duplicate_and_empty_result(tmp_path):
    source = tmp_path/'left.lance'
    right = tmp_path/'right.lance'
    lance.write_dataset(pa.table({'k':[1,1,None,2],'v':['a','b','null','c']}),str(source))
    lance.write_dataset(pa.table({'k':[1,1,None,3]}),str(right))
    with DataFusionSession(resource_directory=tmp_path/'resources',options=options()) as session:
        refs={'l':{'uri':str(source),'version':1},'r':{'uri':str(right),'version':1}}
        result=session.query('SELECT l.* FROM l LEFT SEMI JOIN r ON l.k=r.k',sources=refs)
        assert sorted(r['v'] for r in result.dataset().take(10)) == ['a','b']
        empty=session.query('SELECT * FROM l WHERE false',sources={'l':refs['l']},schema=pa.schema([('k',pa.int64()),('v',pa.string())]))
        assert empty.row_count==0 and empty.dataset().take(1)==[]


def test_sort_merge_join_preserves_nulls_duplicates_and_nested_values(tmp_path):
    left=tmp_path/'left.lance';right=tmp_path/'right.lance'
    lance.write_dataset(pa.table({'k':[1,1,None,2], 'v':[['a'],[],None,['z']]}),str(left))
    lance.write_dataset(pa.table({'k':[1,1,None,3], 'w':['x','y','null','missing']}),str(right))
    with DataFusionSession(resource_directory=tmp_path/'resources',
            options=options(prefer_hash_join=False)) as session:
        result=session.query('SELECT l.k,l.v,r.w FROM l LEFT JOIN r ON l.k=r.k',
            sources={'l':{'uri':str(left),'version':1},'r':{'uri':str(right),'version':1}})
        rows=result.dataset().take(10)
        assert len(rows)==6
        assert sorted(r['w'] for r in rows if r['v']==['a'])==['x','y']
        assert sorted(r['w'] for r in rows if r['v']==[])==['x','y']
        assert [r for r in rows if r['k'] is None]==[{'k':None,'v':None,'w':None}]
        assert [r for r in rows if r['k']==2]==[{'k':2,'v':['z'],'w':None}]
        plan=(Path(result.report['diagnostic_directory'])/'plan.txt').read_text()
        assert 'SortMergeJoinExec' in plan and 'HashJoinExec' not in plan
    with pytest.raises(ValueError,match='prefer_hash_join'):
        options(prefer_hash_join='false')


def test_sliced_nested_arrow_values_survive_private_writer(tmp_path):
    expected=sliced_nested_batch(None)
    assert expected.column(0).offset==1
    with DataFusionSession(resource_directory=tmp_path/'resources',options=options()) as session:
        result=session.query('SELECT 1 AS ignored',sources={},schema=expected.schema,
            batch_transform=sliced_nested_batch)
        actual=lance.dataset(result.uri,version=result.version).to_table()
        assert actual.equals(pa.Table.from_batches([expected]))
        assert result.row_count==4


def test_native_nullable_booleans_cross_private_file_boundary(tmp_path):
    import pyarrow.compute as pc
    from demiflow.lance.arrow_batches import LANCE_FILE_ROWS
    n=1_250_000
    keys=pa.array(range(n),type=pa.int64())
    mask=pc.equal(pc.bit_wise_and(keys,3),0)
    flags=pc.if_else(mask,pa.scalar(None,type=pa.bool_()),pc.greater(keys,100))
    table=pa.table({'k':keys,'payload':pa.StructArray.from_arrays([flags],names=['flag'],mask=mask)})
    source=tmp_path/'booleans.lance'
    lance.write_dataset(pa.RecordBatchReader.from_batches(table.schema,
        table.to_batches(max_chunksize=3001)),str(source))
    expected=table.filter(pc.not_equal(pc.bit_wise_and(keys,7),0))
    assert expected.num_rows>LANCE_FILE_ROWS
    with DataFusionSession(resource_directory=tmp_path/'resources',
            options=replace(options(),batch_rows=3001,timeout_s=45)) as session:
        result=session.query('SELECT k,payload FROM input WHERE (k & 7) <> 0',
            sources={'input':{'uri':str(source),'version':1}})
        actual=lance.dataset(result.uri).to_table().sort_by('k')
        assert actual.equals(expected,check_metadata=False)
        assert len(lance.dataset(result.uri).get_fragments())>1


def test_timeout_releases_slot_and_does_not_return_partial_result(tmp_path):
    # Includes fresh-interpreter imports and filesystem startup, not just SQL.
    config=replace(options(),timeout_s=6)
    started=time.monotonic()
    with DataFusionSession(resource_directory=tmp_path/'resources', options=config) as session:
        with pytest.raises(DataFusionQueryTimeout) as caught:
            session.query('SELECT 1 AS k',sources={},batch_transform=hanging)
        assert caught.value.report['guard']=='timeout'
        assert caught.value.report['complete'] and not caught.value.report['success']
        assert not list(Path(session._temporary.name).glob('*/output.lance'))
        result=session.query('SELECT 2 AS k',sources={})
        assert result.dataset().take(1)==[{'k':2}]
    assert time.monotonic()-started < 18


@pytest.mark.parametrize('failure', ['ddl','transform','schema'])
def test_query_failure_keeps_diagnostics_and_releases_resources(tmp_path,failure):
    with DataFusionSession(resource_directory=tmp_path/'resources',options=options()) as session:
        args={'sql':'SELECT 1 AS k','sources':{}}
        if failure=='ddl':args['sql']='CREATE VIEW bad AS SELECT 1'
        if failure=='transform':args['batch_transform']=invalid_batch
        if failure=='schema':args['schema']=pa.schema([('missing',pa.int64())])
        with pytest.raises(DataFusionQueryError) as caught:session.query(**args)
        receipt=Path(caught.value.report['diagnostic_directory'])/'result.json'
        assert json.loads(receipt.read_text())['success'] is False
        assert not list(Path(session._temporary.name).glob('*/output.lance'))
        assert session.query('SELECT 1 AS k',sources={}).row_count==1


def test_admission_is_shared_between_sessions(tmp_path):
    kwargs={'resource_directory':tmp_path/'resources','options':options()}
    with DataFusionSession(**kwargs) as first, DataFusionSession(**kwargs) as second:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(s.query,'SELECT 1 AS k',sources={},batch_transform=delayed) for s in (first,second)]
            reports=sorted([f.result().report for f in futures],key=lambda r:r['started_at'])
        assert reports[1]['started_at'] >= reports[0]['finished_at']
        assert {r['slot'] for r in reports} == {0}


@pytest.mark.parametrize('guard', ['rss','scratch'])
def test_resource_guards_terminate_owned_worker(tmp_path,guard):
    config=options()
    if guard=='rss':config=replace(config,memory_bytes=1,max_rss_bytes=1024**2)
    else:config=replace(config,max_scratch_bytes=1)
    with DataFusionSession(resource_directory=tmp_path/'resources',options=config) as session:
        with pytest.raises(DataFusionQueryError) as caught:
            session.query('SELECT 1 AS k',sources={},batch_transform=hanging)
        assert caught.value.report['guard']==guard
        assert not list(Path(session._temporary.name).glob('*/output.lance'))


def test_capacity_conflict_and_source_contract(tmp_path):
    with DataFusionSession(resource_directory=tmp_path/'resources',options=options()) as session:
        with pytest.raises(ValueError,match='different slot capacity'):
            with DataFusionSession(resource_directory=tmp_path/'resources',options=replace(options(),max_rss_bytes=3*1024**3)):
                pass
        with pytest.raises(ValueError,match='fixed positive version'):
            session.query('SELECT * FROM x',sources={'x':{'uri':'unused','version':None}})
        with pytest.raises(ValueError,match='identifier'):
            session.query('SELECT 1',sources={'bad;name':{'uri':'unused','version':1}})


def wait_for_worker(directory):
    deadline=time.monotonic()+15
    while time.monotonic()<deadline:
        for path in Path(directory).glob('*/worker_result.json'):
            value=json.loads(path.read_text())
            if value.get('phase') == 'query':
                return json.loads((path.parent/'result.json').read_text())
        time.sleep(.05)
    raise AssertionError('Native worker did not reach query phase')


def test_session_close_cancels_active_query(tmp_path):
    diagnostics=tmp_path/'diagnostics'
    session=DataFusionSession(resource_directory=tmp_path/'resources',
        diagnostics_directory=diagnostics,options=options())
    session.__enter__()
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future=executor.submit(session.query,'SELECT 1 AS k',sources={},batch_transform=hanging)
            wait_for_worker(diagnostics)
            started=time.monotonic()
            session.__exit__(None,None,None)
            with pytest.raises(DataFusionQueryError) as caught:future.result()
            assert caught.value.report['guard']=='cancelled'
            assert time.monotonic()-started<5
    finally:
        if not session._closed:session.__exit__(None,None,None)


def test_parent_crash_releases_inherited_slot(tmp_path):
    import psutil
    scratch=tmp_path/'scratch';scratch.mkdir()
    code='''
import sys,time
from demiflow import data
from demiflow.execution.datafusion import DataFusionOptions, DataFusionSession
def hang(batch):
    time.sleep(30)
    return batch
options=DataFusionOptions(memory_bytes=128*1024**2,max_rss_bytes=2*1024**3,
    threads=1,partitions=2,timeout_s=20)
with DataFusionSession(resource_directory=sys.argv[1],diagnostics_directory=sys.argv[2],
        temp_directory=sys.argv[3],options=options) as s:
    s.query('SELECT 1 AS k',sources={},batch_transform=hang)
'''
    env={**os.environ,'PYTHONPATH':os.pathsep.join(str(p or Path.cwd()) for p in sys.path)}
    process=subprocess.Popen([sys.executable,'-c',code,str(tmp_path/'resources'),
        str(tmp_path/'diagnostics'),str(scratch)],env=env,start_new_session=True)
    try:
        report=wait_for_worker(tmp_path/'diagnostics')
        process.kill();process.wait(timeout=5)
        deadline=time.monotonic()+5
        while psutil.pid_exists(report['pid']) and time.monotonic()<deadline:
            if psutil.Process(report['pid']).status()==psutil.STATUS_ZOMBIE:break
            time.sleep(.05)
        assert not psutil.pid_exists(report['pid']) or psutil.Process(report['pid']).status()==psutil.STATUS_ZOMBIE
        with DataFusionSession(resource_directory=tmp_path/'resources',options=options()) as session:
            assert session.query('SELECT 1 AS k',sources={}).row_count==1
    finally:
        if process.poll() is None:process.kill();process.wait(timeout=5)


def oversized_private_row(batch):
    return pa.record_batch({'text': ['x' * (9 * 1024**2)]})


def test_private_writer_rejects_indivisible_row_without_result(tmp_path):
    schema = pa.schema([('text', pa.string())])
    with DataFusionSession(resource_directory=tmp_path/'resources', options=options()) as session:
        with pytest.raises(DataFusionQueryError) as caught:
            session.query('SELECT 1 AS ignored', sources={}, schema=schema,
                          batch_transform=oversized_private_row)
        assert caught.value.report['complete'] and not caught.value.report['success']
        diagnostic = Path(caught.value.report['diagnostic_directory'])
        worker = json.loads((diagnostic/'worker_result.json').read_text())
        assert worker['phase'] == 'private_write' and worker['error_type'] == 'OSError'
        assert 'One Arrow row exceeds batch_bytes=8388608' in worker['message']
    # The failed private output is not a committed business result.
    assert not list((tmp_path/'resources').rglob('output.lance'))
