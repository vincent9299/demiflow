"""Public Dataset contracts against independent Python answers and real native plans."""
import json
import os
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from tests.test_local_relational import _oracle_join, assert_same_rows
from _worker_event_fixtures import emit, events
from demiflow.execution.journal.run_journal import RunJournal


def api(tmp_path):
    return DataAPI(LocalDatasetExecutor(resource_root=tmp_path))


def read(api, path, values):
    lance.write_dataset(pa.Table.from_pylist(values), str(path))
    return api.read_lance(str(path), version=1)


def sealed_plan(dataset, *, action_id='action-1'):
    """Read the real engine seal through its authenticated, read-only store."""
    from demiflow.execution.storage.objects import RunArtifactStore
    from demiflow.execution.seal.plan import load_dataflow
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    metadata = dataset.execution_metadata()
    directory = Path(metadata.run_ref.directory)
    journal = RunJournal(directory)
    facts = journal.source_binding(action_id + '-dataflow-sealed')
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    store = RunArtifactStore(root=directory / '.dataflow-store',
        store_id=facts['logical_ref']['store_id'], journal=None, bounds=bounds, readonly=True)
    graph, physical, _ = load_dataflow(directory, action_id, store=store, bounds=bounds)
    assert graph.dag_id == metadata.diagnostics['dag_id']
    assert physical.plan_id == metadata.diagnostics['plan_id']
    assert journal.execution_commits(action_id)[-1]['status'] == 'acked'
    return graph, physical, journal


def worker_join_probe(tmp_path, monkeypatch, *, fail=False):
    """Observe/fault the real native library in spawned workers, not a driver double."""
    directory = tmp_path / 'worker-probe'
    directory.mkdir()
    log = tmp_path / 'native-queries'
    script = """import os
if 'DEMIFLOW_WORKER_CONTROL_FD' in os.environ:
    from demiflow.execution.kernels import join_spool, ordering
    from _worker_event_fixtures import emit
    original_join = join_spool.join_pairs
    original_sort = ordering.build_sort_runs
    def observed_join(*args, **kwargs):
        emit(LOG_PATH, 'join')
        if FAIL:
            raise RuntimeError('native failure')
        yield from original_join(*args, **kwargs)
    def observed_sort(*args, **kwargs):
        emit(LOG_PATH, 'sort')
        return original_sort(*args, **kwargs)
    join_spool.join_pairs = observed_join
    ordering.build_sort_runs = observed_sort
""".replace('LOG_PATH', repr(str(log))).replace('FAIL', repr(fail))
    (directory / 'sitecustomize.py').write_text(script)
    monkeypatch.setenv('PYTHONPATH', str(directory) + os.pathsep + os.environ.get('PYTHONPATH', ''))
    return log


def test_engine_session_is_not_a_public_dataset_api():
    assert not hasattr(data, 'datafusion_execution')
    assert not hasattr(data, 'DataFusionOptions')


@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
def test_typed_chain_preserves_duplicates_null_missing_order_and_nested_values(tmp_path, how):
    source = api(tmp_path)
    left = [{'k':'Q2','v':{'flag':None}}, {'k':'Q1','v':None},
            {'k':'Q2','v':{'flag':True}}, {'k':None,'v':{'flag':False}}, {'k':'Q10','v':None}]
    right = [{'k':'Q2','items':[None, {'text':'a'}]}, {'k':'Q2','items':[]}, {'k':None,'items':None}]
    more = [{'k':'Q2','tag':'x'}, {'k':'Q10','tag':'y'}]
    a = read(source, tmp_path/'a', left)
    b = read(source, tmp_path/'b', right)
    c = read(source, tmp_path/'c', more)
    expected = _oracle_join(_oracle_join(left, right, ['k'], ['k'], how), more, ['k'], ['k'], 'left')
    rows = a.join(b, on='k', how=how).join(c, on='k', how='left')
    assert_same_rows(rows.take_all(), expected)
    graph, _, _ = sealed_plan(rows)
    kinds = [node.kernel.kind for node in graph.nodes]
    assert kinds.count('join') == 2
    assert not set(kinds) & {'MapOp', 'MapBatchesOp', 'SortOp'}


def test_self_join_and_pinned_versions(tmp_path):
    source = api(tmp_path)
    values = [{'k':2,'v':'a'}, {'k':1,'v':'b'}, {'k':2,'v':'c'}]
    a = read(source, tmp_path/'a', values)
    lance.write_dataset(pa.table({'k':[99],'v':['new']}), str(tmp_path/'a'), mode='overwrite')
    result = a.join(a, on='k')
    assert_same_rows(result.take_all(), _oracle_join(values, values, ['k'], ['k'], 'inner'))


def test_compacted_and_deleted_lance_rows_keep_scanner_order(tmp_path):
    path = tmp_path/'fragmented'
    ds = lance.write_dataset(pa.table({'k':[1]*10,'seq':list(range(10))}),str(path))
    ds = lance.write_dataset(pa.table({'k':[1]*10,'seq':list(range(10,20))}),str(path),mode='append')
    ds = lance.write_dataset(pa.table({'k':[1]*100,'seq':list(range(20,120))}),str(path),mode='append')
    ds.optimize.compact_files(target_rows_per_fragment=100)
    ds.delete('seq = 23 OR seq = 5')
    expected = ds.to_table().to_pylist()
    source = api(tmp_path)
    right = read(source, tmp_path/'right', [{'k':1,'tag':'a'}, {'k':1,'tag':'b'}])
    rows = source.read_lance(str(path),version=ds.version).join(right,on='k')
    assert_same_rows(rows.take_all(), _oracle_join(expected,[{'k':1,'tag':'a'},{'k':1,'tag':'b'}],['k'],['k'],'inner'))


def test_unsupported_typed_key_preserves_original_codec_error(tmp_path):
    from datetime import date
    source = api(tmp_path)
    a = read(source, tmp_path/'a', [{'a':date(2026,1,1)}])
    b = read(source, tmp_path/'b', [{'b':date(2026,1,1)}])
    with pytest.raises(TypeError, match='JSON serializable'):
        a.join(b, on='a', right_on='b').take_all()
    with pytest.raises(TypeError, match='JSON serializable'):
        a.reduce_by_key('a', lambda _, row: row).take_all()


def test_thread_callbacks_keep_requested_process_context(tmp_path):
    import os
    path = tmp_path/'source'
    lance.write_dataset(pa.table({'k':[1,2]}), str(path))
    def observe(row):
        return {**row, 'pid':os.getpid()}
    with data.local_execution(workers=2, worker_mode='thread', temp_directory=str(tmp_path)):
        a = data.read_lance(str(path), version=1)
        a.join(a, on='k').map(observe).write_lance(str(tmp_path/'out'),
            mode='create', schema=pa.schema([('k',pa.int64()), ('pid',pa.int64())]))
    assert_same_rows(lance.dataset(str(tmp_path/'out')).to_table().to_pylist(), [
        {'k':1, 'pid':os.getpid()}, {'k':2, 'pid':os.getpid()}])


def test_plain_join_does_not_add_global_order_or_replay_callbacks(tmp_path,monkeypatch):
    queries = worker_join_probe(tmp_path, monkeypatch)
    source=api(tmp_path)
    a=read(source,tmp_path/'a',[{'k':2,'v':'a'},{'k':1,'v':'b'},{'k':2,'v':'c'}])
    b=read(source,tmp_path/'b',[{'k':2,'r':1},{'k':2,'r':2}])
    calls=tmp_path/'callback-calls'
    def observe(row):
        emit(calls, 'call', k=row['k'], r=row.get('r'))
        return row
    result=a.join(b,on='k',how='left').map(observe)
    expected=[{'k':1,'v':'b'},{'k':2,'v':'a','r':1},{'k':2,'v':'a','r':2},
              {'k':2,'v':'c','r':1},{'k':2,'v':'c','r':2}]
    actual = result.take_all()
    assert_same_rows(actual, expected)
    assert [(e['k'], e['r']) for e in events(calls)] == [(r['k'], r.get('r')) for r in actual]
    observed = events(queries)
    assert observed and all(e['pid'] != os.getpid() for e in observed)
    assert all(e['event'] == 'join' for e in observed)
    graph, _, _ = sealed_plan(result)
    assert not any(node.kernel.kind == 'SortOp' for node in graph.nodes)


def test_missing_projection_and_collision_only_on_matched_rows(tmp_path):
    source = api(tmp_path)
    a = read(source, tmp_path/'a', [{'k':1,'v':'a','v_right':'occupied'}])
    b = read(source, tmp_path/'b', [{'k':2,'v':'b'}])
    assert a.join(b,on='k',how='left').take_all()==[{'k':1,'v':'a','v_right':'occupied'}]
    with pytest.raises(KeyError):
        a.join(b.rename_columns({'v':'absent'}),on='k',how='left').select_columns(['absent']).take_all()
    with pytest.raises(ValueError,match='collision'):
        a.join(a.select_columns(['k','v']),on='k').take_all()
    missing=a.join(b.rename_columns({'v':'next_key'}),on='k',how='left')
    c=read(source,tmp_path/'c',[{'next_key':'b'}])
    with pytest.raises(KeyError,match='next_key'):
        missing.join(c,on='next_key').take_all()


@pytest.mark.parametrize('opaque', [False, True])
def test_large_join_chain_checkpoint_preserves_multiplicity_and_field_presence(tmp_path, monkeypatch, opaque):
    source = api(tmp_path)
    values = [{'k':2, 'v':'a'}, {'k':1, 'v':'b'}, {'k':2, 'v':'c'}, {'k':None, 'v':None}]
    rows = read(source, tmp_path/'base', values)
    if opaque:
        rows = rows.map(lambda row: row)
    expected = values
    for i in range(5):
        right = [{'k':2, f'r{i}':{'flag':None}}, {'k':2, f'r{i}':{'flag':True}}]
        other = read(source, tmp_path/f'right{i}', right)
        if opaque:
            other = other.map(lambda row: row)
        rows = rows.join(other, on='k', how='left')
        expected = _oracle_join(expected, right, ['k'], ['k'], 'left')
    calls = tmp_path / 'callback-calls'
    def observe(row):
        emit(calls, 'call', value=row['v'])
        return row
    rows = rows.map(observe)
    actual = rows.take_all()
    assert_same_rows(actual, expected)
    assert [e['value'] for e in events(calls)] == [r['v'] for r in actual]
    graph, physical, _ = sealed_plan(rows)
    assert sum(node.kernel.kind == 'join' for node in graph.nodes) == 5
    assert any(edge.mode == 'finite_sealed' for edge in physical.exchanges)


@pytest.mark.slow
def test_udf_boundary_runs_once_and_resorts_changed_keys(tmp_path):
    source = api(tmp_path)
    calls=tmp_path/'callback-calls'
    def change(row):
        emit(calls, 'call', value=row['k'])
        return {'k':3-row['k'],'v':row['v']}
    a = read(source,tmp_path/'a',[{'k':1,'v':'a'},{'k':2,'v':'b'}])
    b = read(source,tmp_path/'b',[{'k':1},{'k':2}])
    out = a.join(b,on='k').map(change).reduce_by_key('k',lambda s,r:r)
    assert out.take_all()==[{'k':1,'v':'b'},{'k':2,'v':'a'}]
    assert sorted(e['value'] for e in events(calls)) == [1,2]
    graph, _, _ = sealed_plan(out)
    kinds = [node.kernel.kind for node in graph.nodes]
    assert all(kind in kinds for kind in ('join', 'MapOp', 'reduce'))


def test_failed_native_query_does_not_rerun_python_or_commit(tmp_path, monkeypatch):
    queries=worker_join_probe(tmp_path, monkeypatch, fail=True)
    calls=tmp_path/'source-calls'
    source=api(tmp_path)
    def records():
        emit(calls, 'scan')
        yield {'k':1}
    target=tmp_path/'target'
    lance.write_dataset(pa.table({'k':[99]}),str(target))
    with pytest.raises(RuntimeError,match='native failure'):
        source.from_iter(records).join(source.from_items([{'k':1}]),on='k').write_lance(
            str(target),mode='overwrite',schema=pa.schema([('k',pa.int64())]))
    assert [e['event'] for e in events(calls)] == ['scan']
    assert len(events(queries)) == 1
    journal = RunJournal(source._executor._run_ref.directory)
    # A cut may record source progress before the failure; none publishes output.
    assert not any((sink or {}).get('manifest', {}).get('final') or (sink or {}).get('rows')
                   for commit in journal.execution_commits('action-1')
                   for sink in ((commit.get('prepared') or {}).get('sinks') or {}).values())
    assert journal.execution_status()['status'] == 'failed'
    assert lance.dataset(str(target)).version==1
    assert lance.dataset(str(target)).to_table().to_pylist() == [{'k': 99}]


def test_typed_writer_avoids_row_decoding_and_preserves_nested_nulls(tmp_path, monkeypatch):
    source=api(tmp_path)
    a=read(source,tmp_path/'a',[{'k':1,'v':{'flag':True}}, {'k':2,'v':None}])
    b=read(source,tmp_path/'b',[{'k':1,'r':[None,{'n':3}]}])
    # The retired Engine.decode hook cannot observe a v3 worker. Exact declared
    # schema and nested/null output remain contractual, not Python-row elision.
    result=a.join(b,on='k',how='left')
    schema=pa.schema([('k',pa.int64()),('v',pa.struct([('flag',pa.bool_())])),('r',pa.list_(pa.struct([('n',pa.int64())])))])
    result.write_lance(str(tmp_path/'out'),mode='create',schema=schema)
    assert lance.dataset(str(tmp_path/'out')).schema.equals(schema)
    assert_same_rows(lance.dataset(str(tmp_path/'out')).to_table().to_pylist(), [
        {'k':1,'v':{'flag':True},'r':[None,{'n':3}]},{'k':2,'v':None,'r':None}])


def test_empty_inputs_and_opaque_python_types_keep_original_semantics(tmp_path):
    from datetime import datetime
    source=api(tmp_path)
    assert source.from_items([]).join(source.from_items([]),on='unknown').take_all()==[]
    a=[{'k':v,'payload':(datetime(2026,1,1),b'\x00',{'x'})} for v in [1,1.0,True,None,{'a':2},['nested']]]
    b=[{'k':v,'r':'matched'} for v in [1,True,None,{'a':2},['nested']]]
    assert_same_rows(source.from_items(a).join(source.from_items(b),on='k',how='left').take_all(), _oracle_join(a,b,['k'],['k'],'left'))


def test_partition_callbacks_preserve_task_state_and_failure_before_commit(tmp_path):
    class Number:
        def __init__(self):
            self.i=0
        def __call__(self,row):
            self.i+=1
            return {**row,'within_task':self.i}
    left=tmp_path/'a';right=tmp_path/'b';target=tmp_path/'out'
    lance.write_dataset(pa.table({'k':list(range(7))}),str(left))
    lance.write_dataset(pa.table({'k':list(range(7))}),str(right))
    schema=pa.schema([('k',pa.int64()),('within_task',pa.int64())])
    with data.local_execution(workers=2,worker_mode='process',batch_rows=3) as session:
        rows=data.read_lance(str(left),version=1).join(data.read_lance(str(right),version=1),on='k').map(Number)
        rows.write_lance(str(target),mode='create',schema=schema)
        assert session.stats['engine'] == 'dataflow_v3'
        graph, _, _ = sealed_plan(rows, action_id=session.stats['action_id'])
        assert any(node.kernel.kind == 'MapOp' for node in graph.nodes)
        assert lance.dataset(str(target)).to_table()['within_task'].to_pylist()==[1,2,3,1,2,3,1]
        expanded = tmp_path/'expanded'
        rows.flat_map(lambda row: [row, row]).write_lance(str(expanded), mode='create', schema=schema)
        assert lance.dataset(str(expanded)).to_table()['within_task'].to_pylist()==[
            1,1,2,2,3,3,1,1,2,2,3,3,1,1]
        state = [0]
        def number_with_closure(row):
            state[0] += 1
            return {**row,'within_task':state[0]}
        for i, callback in enumerate([number_with_closure, Number().__call__]):
            another = tmp_path/f'stateful{i}'
            rows.map(callback).write_lance(str(another),mode='create',schema=schema)
            assert lance.dataset(str(another)).to_table()['within_task'].to_pylist()==[1,2,3,1,2,3,1]
        assert state == [0]
        def fail(row):
            if row['k']==4:
                raise ValueError('callback failed once')
            return row
    # A failed action leaves the run failed. Let its exception cross the run
    # boundary; swallowing it would (correctly) trip unconfirmed-history safety.
    with pytest.raises(ValueError,match='callback failed once'):
        with data.local_execution(workers=2,worker_mode='process',batch_rows=3):
            failed_rows = (data.read_lance(str(left),version=1)
                .join(data.read_lance(str(right),version=1),on='k').map(Number))
            failed_rows.map(fail).write_lance(str(target),mode='overwrite',schema=schema)
    assert lance.dataset(str(target)).version==1


@pytest.mark.slow
def test_large_group_orders_only_keys_and_preserves_sequential_reducer(tmp_path, monkeypatch):
    """80 MiB of opaque input under a 32 MiB engine pool; no wide final sort."""
    from _group_order_fixtures import install_group_order_probe
    from demiflow.execution.pipeline.run_context import RunContext
    proof_path = install_group_order_probe(tmp_path, monkeypatch)
    source = api(tmp_path)
    count = 20000
    def records():
        for i in range(count):
            yield {'k': i % 3, 'seq': i, 'payload': bytes([i % 251])*4096}
    def fold(state, row):
        assert row['payload'] == bytes([row['seq'] % 251])*4096
        return {'k': row['k'], 'n': (state or {}).get('n', 0)+1,
            'total': (state or {}).get('total', 0)+row['seq'],
            'first': state['first'] if state else row['seq'], 'last': row['seq']}
    result = source.from_iter(records).reduce_by_key('k', fold)
    limits = dict(memory_bytes=32 * 1024**2, max_rss_bytes=2 * 1024**3,
                  max_row_bytes=65536, max_batch_bytes=256 * 1024,
                  max_state_bytes=2 * 1024**2, timeout_s=120)
    with RunContext.start(tmp_path / 'run', limits=limits) as run:
        assert result.take_all() == [dict(k=k, n=len(range(k,count,3)),
            total=sum(range(k,count,3)), first=k, last=list(range(k,count,3))[-1]) for k in range(3)]
        _, physical, journal = sealed_plan(result)
        assert physical.bounds.max_resident_bytes == limits['memory_bytes']
        assert journal.execution_commits('action-1')[-1]['status'] == 'acked'
    reports = events(proof_path)
    assert len(reports) == 1
    proof = reports[0]
    assert proof['keys'] == ['k'] and proof['rows'] == count
    assert proof['max_key_bytes'] < 64  # 4 KiB payload is never a comparison field.
    assert proof['max_run_resident_bytes'] <= limits['memory_bytes']
    assert proof['spill_bytes'] > limits['memory_bytes'] and proof['runs'] > 1
    assert proof['max_batch_bytes'] <= 8 * 1024**2
    with journal._connection() as db:
        assert db.execute("SELECT COUNT(*) FROM resource_allocations WHERE cleanup!='CONFIRMED'").fetchone()[0] == 0
    assert not list(tmp_path.glob('demiflow-dataset-*'))


def test_narrow_group_batches_are_not_shrunk_by_an_unrelated_wide_input(tmp_path, monkeypatch):
    """A wide left sibling must not force thousands of tiny RHS row lookups."""
    from _group_order_fixtures import install_group_order_probe
    proof_path = install_group_order_probe(tmp_path, monkeypatch)
    source = api(tmp_path)
    wide = [{'k': k, 'payload': bytes([k])*512*1024} for k in range(4)]
    narrow = [{'k': i % 4, 'seq': i} for i in range(4000)]
    def fold(state, row):
        return {'k': row['k'], 'n': (state or {}).get('n', 0)+1,
            'last': row['seq']}
    grouped = source.from_items(narrow).reduce_by_key('k', fold)
    # Execute one wide query first, then a narrow sibling in the same engine.
    # Neither query batches nor hydration budgets leak into its narrow sibling.
    wide_query = source.from_items(wide).join(
        source.from_items([{'k': k} for k in range(4)]), on='k').map(lambda row: row)
    result = wide_query.join(grouped, on='k')
    assert_same_rows(result.take_all(), [
        {**row, 'n': 1000, 'last': 3996+row['k']} for row in wide])
    graph, physical, journal = sealed_plan(result)
    assert [node.kernel.kind for node in graph.nodes].count('reduce') == 1
    reports = events(proof_path)
    assert len(reports) == 1
    proof = reports[0]
    assert proof['keys'] == ['k'] and proof['rows'] == 4000
    assert proof['max_key_bytes'] < 64
    # The narrow side still fills ordinary native runs; a previous wide input
    # cannot shrink it into thousands of one-row materializations.
    assert proof['runs'] <= 2
    assert proof['max_run_resident_bytes'] <= proof['max_resident_bytes']
    assert proof['max_resident_bytes'] <= physical.bounds.max_resident_bytes
    assert proof['max_row_bytes'] <= physical.bounds.max_row_bytes
    assert proof['max_batch_bytes'] <= physical.bounds.max_batch_bytes
    with journal._connection() as db:
        assert db.execute("SELECT COUNT(*) FROM resource_allocations WHERE cleanup!='CONFIRMED'").fetchone()[0] == 0
    assert not list(tmp_path.glob('demiflow-dataset-*'))
