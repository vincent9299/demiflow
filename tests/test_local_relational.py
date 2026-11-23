import pytest
from demiflow.data.api import DataAPI


def ds(rows):return DataAPI().from_items(rows)


def test_join_many_to_many_and_nulls():
    left=ds([{'k':1,'v':'a'},{'k':1,'v':'b'},{'k':2,'v':'c'},{'k':None,'v':'null'}])
    right=ds([{'k':1,'v':'x'},{'k':1,'v':'y'},{'k':None,'v':'n'}])
    result=left.join(right,on='k',chunk_bytes=1).take_all()
    assert len(result)==4
    assert {r['v_right'] for r in result}=={'x','y'}
    assert len(left.join(right,on='k',how='left',chunk_bytes=1).take_all())==6
    assert len(left.join(right,on='k',how='semi',chunk_bytes=1).take_all())==2
    assert {r['k'] for r in left.join(right,on='k',how='anti').take_all()}=={2,None}


@pytest.mark.slow
def test_group_spill_batches_and_composite_keys():
    data=ds([{'a':'x','b':1,'n':i} for i in range(100)])
    batches=data.group_batches(['a','b'],max_rows=7,chunk_bytes=1).take_all()
    assert sum(len(r['items']) for r in batches)==100
    assert max(len(r['items']) for r in batches)==7
    assert sum(r['group_last'] for r in batches)==1
    assert data.reduce_by_key(['a','b'],lambda acc,r:{'a':r['a'],'b':r['b'],'count':(acc or {}).get('count',0)+1},chunk_bytes=1).take_all()[0]['count']==100
    assert ds([]).join(ds([]),on='k').take_all()==[]


def test_checkpoint_cached_map_failure_resume(tmp_path):
    from functools import partial
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.recovery_contract import RecoveryContractError
    from _native_relational_history_fixtures import cached_transform
    from _worker_event_fixtures import events
    log, failure = tmp_path / 'calls.jsonl', tmp_path / 'failed-once'
    def plan(version='v1'):
        transform = partial(cached_transform, log=str(log), failure=str(failure),
                            key='i', output='square', square=True)
        return data.from_items([{'i': i} for i in range(4)]).map_cached(
            transform, cache_dir=tmp_path / 'cache', version=version,
            concurrency=1, queue_depth=1)
    limits = {'checkpoint_rows': 1, 'checkpoint_unconfirmed_rows': 1,
              'checkpoint_interval_ms': 60000}
    with pytest.raises(RuntimeError, match='interrupted'):
        with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(worker_mode='thread'):
            plan().materialize()
    assert run.journal.execution_action('action-1')['result'] is None
    assert any(c['status'] == 'acked' for c in run.journal.execution_commits('action-1'))
    calls = [e['value'] for e in events(log) if e['event'] == 'call']
    assert calls.count(0) == 1
    errors = list((tmp_path / 'cache').glob('*.error.json'))
    assert errors and any('interrupted' in path.read_text() for path in errors)
    failed_facts, failed_calls = run.journal.execution_commits('action-1'), events(log)
    with pytest.raises(RecoveryContractError, match='identity|differs'):
        with RunContext.resume(run.ref), data.local_execution(worker_mode='thread'):
            plan('v2').materialize()
    assert events(log) == failed_calls and run.journal.execution_commits('action-1') == failed_facts
    with RunContext.resume(run.ref), data.local_execution(worker_mode='thread'):
        materialized = plan().materialize()
        result = materialized.take_all()
    assert result == [{'i': i, 'square': i * i} for i in range(4)]
    assert [e['value'] for e in events(log)].count(0) == 1
    facts = run.journal.execution_commits('action-1')
    assert facts[-1]['status'] == 'acked' and facts[-1]['prepared']['sinks']['materialize']['manifest']['final']
    calls = events(log)
    # Completed runs are immutable; reuse the original public materialized
    # result rather than inventing another attempt of a completed RunContext.
    with data.local_execution(worker_mode='thread'):
        assert materialized.take_all() == result
    assert events(log) == calls and run.journal.execution_commits('action-1') == facts
    # A new run really exercises the public result cache, independently of the
    # completed original RunRef shortcut. Existing inputs must not call again.
    with RunContext.start(tmp_path / 'new-run'), data.local_execution(worker_mode='thread'):
        assert plan().materialize().take_all() == result
    assert events(log) == calls


def test_join_collision_rejected():
    with pytest.raises(ValueError,match='collision'):
        ds([{'k':1,'x':1,'x_right':2}]).join(ds([{'k':1,'x':3}]),on='k').take_all()


def test_checkpoint_sync_and_mixed_prefix(tmp_path):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from _native_relational_history_fixtures import double
    def rows():
        return (data.from_items([{'n': 1}, {'n': 2}])
                .map(lambda row: {'n': row['n'] + 1}).filter(lambda row: row['n'] > 2))
    with RunContext.start(tmp_path / 'sync'), data.local_execution(worker_mode='thread'):
        assert rows().materialize().take_all() == [{'n': 3}]
    with RunContext.start(tmp_path / 'mixed') as run, data.local_execution(worker_mode='thread'):
        materialized = rows().map_cached(double, cache_dir=tmp_path / 'cache', version='v1').materialize()
        result = materialized.take_all()
    assert result == [{'n': 3, 'doubled': 6}]
    facts = run.journal.execution_commits('action-1')
    assert facts[-1]['status'] == 'acked' and facts[-1]['prepared']['sinks']['materialize']['manifest']['final']
    with data.local_execution(worker_mode='thread'):
        assert materialized.take_all() == result
    assert run.journal.execution_commits('action-1') == facts


def test_async_checkpoint_preserves_source_context_across_feed_chunks(tmp_path, monkeypatch):
    from functools import partial
    from demiflow import data
    from demiflow.observability import _CURRENT_ACTION
    from demiflow.execution.run_context import RunContext, current_run, current_action
    from _native_relational_history_fixtures import echo, source_with_context
    from _worker_event_fixtures import events
    before = _CURRENT_ACTION.get()
    log = tmp_path / 'source.jsonl'
    # The retired stream._FEED_CHUNK hook is replaced by the actual native
    # source/batch window. Seven rows span more than one two-row window.
    with RunContext.start(tmp_path / 'run', limits={'batch_rows': 2, 'checkpoint_rows': 2,
            'checkpoint_interval_ms': 60000}) as run, data.local_execution(worker_mode='thread', batch_rows=2):
        result = (data.from_iter(partial(source_with_context, str(log)))
                  .map_cached(echo, cache_dir=tmp_path / 'cache', version='v1')
                  .materialize().take_all())
        assert current_run() is run and current_action() is None
        assert _CURRENT_ACTION.get() is before
    assert sorted(row['n'] for row in result) == list(range(7))
    observations = events(log)
    assert [e['value'] for e in observations if e['event'] == 'pull'] == list(range(7))
    assert [e['event'] for e in observations].count('source_close') == 1
    assert all(e['run_id'] == run.ref.run_id for e in observations)
    assert all(e['directory'] == str(run.directory) and e['attempt'] == 1
               for e in observations if e['event'] == 'pull')
    assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert current_run() is None and _CURRENT_ACTION.get() is before


def test_consecutive_flat_maps_bind_separate_iterators():
    from demiflow.data.api import DataAPI
    rows = (DataAPI().from_items([{"value": 1}, {"value": 2}])
            .flat_map(lambda r: [{"value": r["value"]}, {"value": r["value"] + 10}])
            .flat_map(lambda r: [{"value": r["value"] * 2}]).take_all())
    assert [r["value"] for r in rows] == [2, 22, 4, 24]


def test_checkpoint_async_in_running_loop_resume_and_version(tmp_path):
    import asyncio
    from functools import partial
    from demiflow import data
    from demiflow.execution.run_context import RunContext, current_run
    from demiflow.execution.recovery_contract import RecoveryContractError
    from _native_relational_history_fixtures import cached_transform
    from _worker_event_fixtures import events
    log, failure = tmp_path / 'calls.jsonl', tmp_path / 'failed-once'
    def plan(version='v1'):
        return data.from_items([{'n': 1}, {'n': 2}]).map_cached(
            partial(cached_transform, log=str(log), failure=str(failure), key='n', output='twice'),
            cache_dir=tmp_path / 'cache', version=version, concurrency=1, queue_depth=1)
    async def notebook():
        # Public materialize is synchronous. The same to_thread context bridge
        # formerly used by checkpoint_async keeps a live notebook loop free.
        loop = asyncio.get_running_loop()
        with pytest.raises(RuntimeError, match='failure to preserve'):
            with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 1,
                    'checkpoint_unconfirmed_rows': 1, 'checkpoint_interval_ms': 60000}) as run, \
                    data.local_execution(worker_mode='thread'):
                await asyncio.to_thread(plan().materialize)
        assert run.journal.execution_action('action-1')['result'] is None
        assert current_run() is None and asyncio.get_running_loop() is loop
        failed_facts, failed_calls = run.journal.execution_commits('action-1'), events(log)
        with pytest.raises(RecoveryContractError, match='identity|differs'):
            with RunContext.resume(run.ref), data.local_execution(worker_mode='thread'):
                await asyncio.to_thread(plan('v2').materialize)
        assert events(log) == failed_calls and run.journal.execution_commits('action-1') == failed_facts
        with RunContext.resume(run.ref), data.local_execution(worker_mode='thread'):
            out = await asyncio.to_thread(plan().materialize)
            rows = await asyncio.to_thread(out.take_all)
        assert rows == [{'n': 1, 'twice': 2}, {'n': 2, 'twice': 4}]
        assert [e['value'] for e in events(log)].count(1) == 1
        calls, facts = events(log), run.journal.execution_commits('action-1')
        assert facts[-1]['status'] == 'acked' and facts[-1]['prepared']['sinks']['materialize']['manifest']['final']
        with data.local_execution(worker_mode='thread'):
            assert await asyncio.to_thread(out.take_all) == rows
        assert events(log) == calls and run.journal.execution_commits('action-1') == facts
        with RunContext.start(tmp_path / 'sync'), data.local_execution(worker_mode='thread'):
            sync = await asyncio.to_thread(data.from_items([{'n': 3}]).materialize)
            assert await asyncio.to_thread(sync.take_all) == [{'n': 3}]
        assert current_run() is None and asyncio.get_running_loop() is loop
    asyncio.run(notebook())


def assert_same_rows(actual, expected):
    """Compare small test multisets, including duplicate rows and nested values."""
    import json
    key = lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True, default=repr)
    assert sorted(actual, key=key) == sorted(expected, key=key)


def _oracle_join(left, right, lk, rk, how):
    """朴素参考算法独立于优化实现，保留旧 canonical 排序和组内稳定次序。"""
    import json
    def key(row, fields):
        return json.dumps([row[f] for f in fields], ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    out = []
    for row in sorted(left, key=lambda r: key(r, lk)):
        matches = [r for r in right if key(row, lk) == key(r, rk)
                   and all(row[f] is not None for f in lk)]
        if how == 'semi':
            if matches: out.append(dict(row))
        elif how == 'anti':
            if not matches: out.append(dict(row))
        elif not matches:
            if how == 'left': out.append(dict(row))
        else:
            for other in matches:
                value = dict(row)
                for k, v in other.items():
                    if k in lk and k in rk and k in value and value[k] == v:
                        continue
                    value[k if k not in value else k + '_right'] = v
                out.append(value)
    return out


@pytest.mark.parametrize('budget', [1, 1000, 32*1024*1024])
@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
def test_relational_hash_spill_match_oracle(budget, how):
    import random
    from datetime import datetime, timezone
    rng = random.Random(917)
    values = [None, True, False, 1, 1.0, '1', '中"文', ['nested'], {'k': 2}]
    left = [{'a': rng.choice(values), 'b': i % 3, 'v': i,
             'native': datetime(2026, 1, 1, tzinfo=timezone.utc), 'bytes': b'\x00\xff'} for i in range(60)]
    right = [{'x': rng.choice(values), 'y': i % 3, 'v': [i]} for i in range(55)]
    actual = ds(left).join(ds(right), on=['a', 'b'], right_on=['x', 'y'], how=how,
                           chunk_bytes=budget).take_all()
    assert_same_rows(actual, _oracle_join(left, right, ['a', 'b'], ['x', 'y'], how))


def test_join_group_order_and_callback_invalidation():
    def plan():
        return ds([{'k': 2, 'v': 'a'}, {'k': 1, 'v': 'b'}, {'k': 2, 'v': 'c'}]).join(
            ds([{'k': 1}, {'k': 2}]), on='k')
    def reducer(acc, row):
        return {'k': row['k'], 'vs': (acc or {}).get('vs', '') + row['v']}
    assert plan().reduce_by_key('k', reducer).take_all() == [{'k':1,'vs':'b'}, {'k':2,'vs':'ac'}]
    batches = plan().group_batches('k', max_rows=1).take_all()
    assert len(batches) == 3
    changed = plan().map(lambda row: {**row, 'k': 3-row['k']}).reduce_by_key('k', reducer).take_all()
    assert changed == [{'k':1,'vs':'ac'}, {'k':2,'vs':'b'}]


@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
def test_join_results_with_many_normal_groups(how):
    left = [{'k': i} for i in range(100)]
    # right table exceeds budget, individual keys fit: use sort/merge without group files
    right = [{'k': i, 'v': 'x'*50} for i in range(100)]
    assert_same_rows(ds(left).join(ds(right), on='k', how=how, chunk_bytes=1000).take_all(), _oracle_join(left, right, ['k'], ['k'], how))


def test_hot_key_cartesian_product_and_no_aliasing():
    left = [{'k': 1, 'l': i} for i in range(3)]
    right = [{'k': 1, 'nested': [i]} for i in range(40)]
    result = ds(left).join(ds(right), on='k', chunk_bytes=500).take_all()
    assert_same_rows(result, _oracle_join(left, right, ['k'], ['k'], 'inner'))
    result[0]['nested'].append('mutated')
    assert all('mutated' not in row['nested'] for row in result[1:])


@pytest.mark.parametrize('workers', [1, 2])
def test_public_sort_preserves_equal_key_order(tmp_path, workers):
    from demiflow import data
    from demiflow.execution import RunContext
    rows = [{'k': i % 7, 'i': i} for i in range(64)]
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=workers, partitions=1):
        actual = data.from_items(rows).sort('k').take_all()
    assert actual == sorted(rows, key=lambda row: row['k'])


def test_sort_and_join_cleanup_on_consumer_error(tmp_path, monkeypatch):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.storage.objects import RunArtifactStore
    from demiflow.resources.thread import ThreadResourceProvider
    scratch, exits = [], []
    original_scratch, original_absent = RunArtifactStore.scratch, ThreadResourceProvider.ensure_absent
    def tracked_scratch(store, **options):
        owned = original_scratch(store, **options)
        scratch.append(owned)
        return owned
    def tracked_absent(provider, exact, deadline):
        proof = original_absent(provider, exact, deadline)
        exits.append((provider, dict(exact), dict(proof)))
        return proof
    monkeypatch.setattr(RunArtifactStore, 'scratch', tracked_scratch)
    monkeypatch.setattr(ThreadResourceProvider, 'ensure_absent', tracked_absent)
    def fail(state, row):
        raise RuntimeError('consumer failed')
    with pytest.raises(RuntimeError, match='consumer failed'):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution(worker_mode='thread'):
            (data.from_items([{'k': 1}] * 50).join(data.from_items([{'k': 1}] * 50), on='k', chunk_bytes=1)
             .reduce_by_key('k', fail).take_all())
    assert scratch, 'real native join/reducer must borrow original store scratch'
    assert all(owned.fd is None and not owned.path.exists() and not owned.owner.exists() for owned in scratch)
    with run.journal._connection() as connection:
        for owned in scratch:
            settlement = connection.execute('SELECT bytes,charged FROM runtime_dataflow_settlements WHERE reservation_id=?',
                                             (owned.operation,)).fetchone()
            assert settlement == (0, 0)
            reservation = connection.execute("SELECT amount FROM runtime_reservations WHERE name='artifact_staging_bytes' AND reservation_id=?",
                                             (owned.operation,)).fetchone()
            assert reservation == (0,)
        assert not connection.execute('SELECT 1 FROM runtime_dataflow_staging s LEFT JOIN runtime_dataflow_settlements d '
                                      'ON s.reservation_id=d.reservation_id WHERE d.reservation_id IS NULL').fetchone()
    assert run.journal.execution_action('action-1')['result'] is None
    assert not any(sink.get('manifest', {}).get('final')
                   for c in run.journal.execution_commits('action-1')
                   for sink in (c.get('prepared') or {}).get('sinks', {}).values())
    last = {exact['allocation_id']: (provider, exact, proof) for provider, exact, proof in exits}
    assert last
    for provider, exact, proof in last.values():
        assert proof['cleanup'] == 'CONFIRMED'
        assert provider.pool.reservation_view(exact['allocation_id'])['state'] == 'RELEASED'
        assert provider.pool.snapshot()['charged']['cpu_millis'] == 0


@pytest.mark.parametrize('how,expected_rows', [('inner', 2), ('semi', 2), ('left', 100), ('anti', 100)])
def test_native_join_results_with_wide_left_inputs(how, expected_rows):
    left = [{'k':i, 'payload':'wide'*100} for i in reversed(range(100))]
    right = [{'k':1}, {'k':4}]
    assert_same_rows(ds(left).join(ds(right), on='k', how=how).take_all(), _oracle_join(left, right, ['k'], ['k'], how))


@pytest.mark.parametrize(('width', 'key_padding', 'rows', 'uses_offsets'), [
    (24, 0, 40, False), (70000, 0, 40, True), (70000, 17000, 52, True),
], ids=['narrow-values', 'wide-values', 'wide-values-and-keys'])
def test_wide_sort_writes_payload_once_across_merge_passes(
        tmp_path, monkeypatch, width, key_padding, rows, uses_offsets):
    from demiflow import data
    from demiflow.execution import RunContext
    from demiflow.execution.dataflow_scratch import KernelScratch
    from demiflow.execution.kernels import ordering

    extents, references, runs = [], [], []
    append, encode, write_run = KernelScratch.append, ordering._encode_payload, ordering._write_run

    def append_body(scratch, raw):
        offset = append(scratch, raw)
        if b'x' * 4096 in raw:
            extents.append((offset, len(raw)))
        return offset

    def encode_reference(value, **kwargs):
        if {'offset', 'length', 'sha256'} <= value.keys():
            references.append((value['offset'], value['length']))
        return encode(value, **kwargs)

    def spill(frames, services):
        ref = write_run(frames, services)
        runs.append(ref.size_bytes)
        return ref

    monkeypatch.setattr(KernelScratch, 'append', append_body)
    monkeypatch.setattr(ordering, '_encode_payload', encode_reference)
    monkeypatch.setattr(ordering, '_write_run', spill)
    monkeypatch.setattr(ordering, '_MERGE_FAN_IN', 2)

    def wide(row):
        i = row['id']
        return {'k': str(i % 3) + 'z' * key_padding, 'i': i, 'wide': 'x' * width}

    limits = {'memory_bytes': 2 << 20, 'max_row_bytes': 210000,
              'max_batch_bytes': 1 << 20, 'checkpoint_interval_ms': 1000000}
    with RunContext.start(tmp_path/'run', limits=limits), data.local_execution(
            workers=1, worker_mode='thread', partitions=1):
        order = []
        for row in data.range(rows).map(wide).sort('k').iter_rows():
            assert row['wide'] == 'x' * width
            order.append(row['i'])
    assert order == sorted(range(rows), key=lambda i: i % 3)
    assert len(extents) == len(references) == (rows if uses_offsets else 0)
    assert extents == references and len(set(references)) == len(references)
    if key_padding:
        assert len(runs) > 1
