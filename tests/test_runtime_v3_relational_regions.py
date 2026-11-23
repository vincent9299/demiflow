"""Real Dataset values, original seals, and measured relational reuse."""
from pathlib import Path

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.sealed_plans import load_sealed_plan


def build_chain():
    left = data.from_items([{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}, {'k': 1, 'v': 'c'}])
    right = data.from_items([{'k': 1, 'r': 10}, {'k': 1, 'r': 11}, {'k': 2, 'r': 20}])
    last = data.from_items([{'k': 1, 'z': 7}, {'k': 2, 'z': 8}])
    return left.join(right, on='k').join(last, on='k').reduce_by_key(
        'k', lambda s, r: {'k': r['k'], 'values': (s or {}).get('values', []) + [(r['v'], r['r'], r['z'])]})


def test_real_chain_preserves_values_order_and_explicit_region(tmp_path):
    with RunContext.start(tmp_path/'run') as run:
        with data.local_execution(workers=2, worker_mode='thread', partitions=3, batch_rows=2):
            rows = build_chain().take_all()
        assert rows == [{'k': 1, 'values': [('a', 10, 7), ('a', 11, 7), ('c', 10, 7), ('c', 11, 7)]},
                        {'k': 2, 'values': [('b', 20, 8)]}]
        bundle = load_sealed_plan(run.directory, 'action-1', version='v3')
        graph, physical = bundle['graph'], bundle['physical']
        regions = [s for s in physical.stages if s.stage_id.startswith('relational-v1-')]
        assert len(regions) == 1
        kinds = {n.occurrence_id: n.kernel.kind for n in graph.nodes}
        assert [kinds[o] for o in regions[0].occurrences] == ['join', 'join', 'reduce']
        assert regions[0].profile.requests['memory_bytes'] < 256*1024**2
        facts = run.journal.execution_commits('action-1')
        assert facts and all(f['status'] == 'acked' for f in facts)


def test_region_planner_preserves_legacy_disable_isolation_and_budget():
    from dataclasses import replace
    import pytest
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.execution.dataflow_api import _CompileContext
    from demiflow.execution.run_context import RunAction
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_place import partition_stages
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    from demiflow.planning.dataflow_relational_regions import fuse_relational_regions
    from demiflow.execution.contracts.relational_region import interpret_region
    from demiflow.resources.contracts import ResourceProfile
    from test_runtime_v3_finite_fusion import HOSTS
    ds = build_chain()
    action = RunAction(_CompileContext(), 'region-compile', {})
    request = ActionRequest(kind='take_all', source=ds._source, plan=ds._plan,
        arguments={}, sink=None, programs=(), provider='local', action=action)
    policy, bounds = resolve_runtime_policy({}, {}, provider='local')
    graph = lower_action(request, bounds=bounds).graph
    stages = partition_stages(graph, policy=policy, bounds=bounds)
    fused = fuse_relational_regions(graph, stages, policy=policy, bounds=bounds, hosts=HOSTS)
    regions = [s for s in fused if s.stage_id.startswith('relational-v1-')]
    assert len(regions) == 1
    assert all(interpret_region(graph, stage) is None for stage in stages)
    assert interpret_region(graph, regions[0]).occurrences == regions[0].occurrences
    assert fuse_relational_regions(graph, stages, policy=replace(policy, fusion=False), bounds=bounds, hosts=HOSTS) == stages
    assert fuse_relational_regions(graph, stages, policy=policy, bounds=replace(bounds,max_resident_bytes=65536),hosts=HOSTS) == stages
    blocked = tuple(replace(s,profile=ResourceProfile(requests=dict(s.profile.requests),
        requirements={'isolation':'process'})) for s in stages)
    assert fuse_relational_regions(graph,blocked,policy=policy,bounds=bounds,hosts=HOSTS)==blocked
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError,match='sealed relational region'):
        interpret_region(graph,replace(regions[0],stage_id='relational-v1-'+'0'*64))


def test_region_original_final_ack_replay_does_not_repeat_reducer(tmp_path):
    import pytest
    from demiflow.data.api import DataAPI
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    calls = tmp_path/'calls.txt'
    def reducer(state,row):
        with calls.open('a') as output:
            output.write(str(row['v'])+'\n')
        return {'k':row['k'],'values':(state or {}).get('values',[])+[row['v']]}
    def flow():
        return data.from_items([{'k':1,'v':i} for i in range(4)]).join(
            data.from_items([{'k':1,'r':2}]),on='k').join(
            data.from_items([{'k':1,'z':3}]),on='k').reduce_by_key('k',reducer)
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=2) as local:
            reference=run.ref
            rows=flow().take_all()
            assert rows==[{'k':1,'values':[0,1,2,3]}]
            from _native_relational_fixtures import native_region_evidence
            _,_,counts=native_region_evidence(local)
            assert len(counts)==1
            original=run.journal.execution_commits('action-1')
            original_calls=calls.read_text()
            raise Interrupted('after action ACK before enclosing run completion')
    with RunContext.resume(reference) as run, data.local_execution(workers=2) as local:
        assert flow().take_all()==rows
        assert run.journal.execution_commits('action-1')==original
        assert calls.read_text()==original_calls
        assert local.stats['observed_worker_count']==0


def test_region_pure_join_nonfinal_ack_restores_original_input_refs(tmp_path,monkeypatch):
    import pytest
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.dataflow_kernel_pack import read_kernel_image
    from demiflow.execution.kernels import state_codec
    def flow():
        return data.from_items([{'k':1,'v':i} for i in range(20)]).join(
            data.from_items([{'k':1,'r':i} for i in range(8)]),on='k').join(
            data.from_items([{'k':1,'z':i} for i in range(2)]),on='k')
    class Interrupted(Exception):pass
    commit=DataflowCheckpointCoordinator.commit
    caught=[]
    def cut(coordinator,*args,**kwargs):
        image=commit(coordinator,*args,**kwargs)
        for stage in coordinator._physical.stages:
            if not stage.stage_id.startswith('relational-v1-'):continue
            occ=stage.occurrences[-1]
            for contribution in image.contributions:
                ref=contribution.operator_states.get(occ)
                if ref is None:continue
                raw=read_kernel_image(coordinator._store,ref,coordinator._physical.bounds,
                                      occurrence=occ,partition=stage.partitions[0])
                state=state_codec.loads(raw,max_bytes=coordinator._physical.bounds.max_state_bytes)
                if not image.cut.final and state.get('finish_cursor',{}).get('emitted',0)>0:
                    caught.append((image.commit_id,coordinator._physical.plan_id))
                    raise Interrupted('actual region output prefix ACKed')
        return image
    limits={'checkpoint_rows':2,'checkpoint_interval_ms':60000}
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run',limits=limits) as run, data.local_execution(workers=2):
            reference=run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator,'commit',cut)
                flow().take_all()
    assert len(caught)==1
    with RunContext.resume(reference) as run,data.local_execution(workers=2) as local:
        before=run.journal.execution_commits('action-1')
        actual=flow().take_all()
        assert sorted((r['v'],r['r'],r['z']) for r in actual)==[(v,r,z) for v in range(20) for r in range(8) for z in range(2)]
        after=run.journal.execution_commits('action-1')
        assert after[:len(before)]==before and all(x['status']=='acked' for x in after)
        assert local.stats['plan_id']==caught[0][1]


def test_region_semi_anti_keeps_original_arrow_schema(tmp_path):
    import pyarrow as pa
    from _native_relational_fixtures import native_region_evidence
    schema=pa.schema([pa.field('k',pa.int32(),nullable=False),
                     pa.field('v',pa.large_string())],metadata={b'origin':b'region-source'})
    table=pa.Table.from_pylist([{'k':1,'v':'one'},{'k':2,'v':None},{'k':3,'v':'three'}],schema=schema)
    with RunContext.start(tmp_path/'run'),data.local_execution(workers=2) as local:
        actual=data.from_arrow(table).join(data.from_items([{'k':1},{'k':2}]),on='k',how='semi').join(
            data.from_items([{'k':2}]),on='k',how='anti').take_batch(10,batch_format='pyarrow')
        assert actual.equals(table.slice(0,1),check_metadata=True)
        _,_,counts=native_region_evidence(local)
        assert len(counts)==1 and counts[0]['intermediate_payload_bytes']==0


def _small_budget_region(tmp_path, row_bytes, key_bytes):
    from _native_relational_fixtures import native_region_evidence,oracle_join
    keys=['a'*key_bytes,'b'*key_bytes,None]
    left=[{'k':keys[i%3],'v':i} for i in range(24)]
    right=[{'k':key,'r':j} for key in keys for j in range(2)]
    last=[{'k':key,'z':j} for key in keys for j in range(2)]
    expected={}
    for row in oracle_join(oracle_join(left,right,how='left'),last,how='left'):
        expected.setdefault(row['k'],[]).append((row['v'],row.get('r'),row.get('z')))
    limits={'memory_bytes':256*row_bytes,'max_row_bytes':row_bytes,'max_batch_bytes':2*row_bytes,
            'max_state_bytes':32768,'checkpoint_rows':64}
    with RunContext.start(tmp_path/'run',limits=limits),data.local_execution(workers=2) as local:
        actual=data.from_items(left).join(data.from_items(right),on='k',how='left').join(
            data.from_items(last),on='k',how='left').reduce_by_key('k',lambda state,row:{
                'k':row['k'],'fold':((state or {}).get('fold',17)*131+row['v']*9+(row.get('r') or 0)*3+(row.get('z') or 0))%1000000007,
                'n':(state or {}).get('n',0)+1}).take_all()
        oracle=[]
        for key in [*sorted(k for k in expected if k is not None),None]:
            values=expected[key]; n=len(values); prime=1000000007
            value=(17*pow(131,n,prime)+sum((v*9+(r or 0)*3+(z or 0))*pow(131,n-1-i,prime) for i,(v,r,z) in enumerate(values)))%prime
            oracle.append({'k':key,'fold':value,'n':n})
        assert actual==oracle
        graph,physical,counts=native_region_evidence(local)
        assert len(counts)==1
        measured=counts[0]
        assert measured['sort_runs']>2 and measured['sort_fan_in_limit']==2
        assert measured['sort_window_bytes']==8*row_bytes
        assert measured['sort_run_bytes']==2*row_bytes
        assert 2*(3*measured['sort_frame_bytes']+measured['sort_read_chunk_bytes'])+2*measured['sort_frame_bytes']<=measured['sort_window_bytes']
        region=next(stage for stage in physical.stages if stage.stage_id.startswith('relational-v1-'))
        # State replacement/encoding coexists with the merge; unlike the old
        # one-S formula, the small-row case reserves both actual state slots.
        expected_peak=max(8*row_bytes+5*row_bytes,
            7*row_bytes+max(6*row_bytes+32768,3*row_bytes+2*32768,
                            6*row_bytes+32768))
        assert expected_peak+16*row_bytes==region.resident_bytes
        assert region.resident_bytes<physical.bounds.max_resident_bytes
        assert measured['partition_builds']==3 and measured['intermediate_key_rebuilds']==0
        assert measured['intermediate_payload_bytes']==0


def test_region_small_budget_multirun_wide_key_nonassociative_fold(tmp_path):
    _small_budget_region(tmp_path/'small',4096,400)
    _small_budget_region(tmp_path/'wide',16384,6144)


def test_sort_subwindow_uses_original_store_and_closes_readers(tmp_path,monkeypatch):
    from dataclasses import replace
    from types import SimpleNamespace
    import pytest
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.kernels.ordering import SortWindow,build_sort_runs,merge_sort_runs
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_finite_borrow import fixture,record
    from test_runtime_v3_c import make
    with store_fixture(tmp_path) as (_,store,_):
        svc,_=fixture(store)
        svc.bounds=replace(svc.bounds,max_row_bytes=4096,max_resident_bytes=65536)
        spec=make('KernelSpec',kind='SortOp',configuration={'keys':['k']})
        window=SortWindow(32768,8192,4000,64,2,True)
        rows=[record({'k':str(i%5)*400,'unused':i},i) for i in range(30,-1,-1)]
        # Offsets and source provenance survive; only internal carrier payload
        # is omitted. The original immutable runs remain caller-owned.
        runs=build_sort_runs(iter(rows),spec=spec,services=svc,window=window)
        assert len(runs)>2
        real_read=store.read
        activity={'active':0,'peak':0,'chunks':[]}
        def observed(ref,**kwargs):
            activity['chunks'].append(kwargs['chunk_bytes'])
            activity['active']+=1
            activity['peak']=max(activity['peak'],activity['active'])
            try:yield from real_read(ref,**kwargs)
            finally:activity['active']-=1
        monkeypatch.setattr(store,'read',observed)
        result=list(merge_sort_runs(runs,spec=spec,services=svc,window=window))
        assert [item.sequence for item in result]==sorted(range(31),key=lambda i:(str(i%5)*400,i))
        assert all(dict(item.inline)=={} for item in result)
        assert activity['peak']==2 and activity['active']==0 and set(activity['chunks'])=={64}
        assert all(store.inspect(run) for run in runs)
        with pytest.raises(DataflowError,match='sort subwindow'):
            build_sort_runs(iter(rows),spec=spec,services=svc,window=replace(window,resident_bytes=65537))
        with pytest.raises(DataflowError) as failure:
            build_sort_runs(iter([record({'k':'x'*3000})]),spec=spec,services=svc,window=window)
        assert failure.value.code=='BUDGET_EXCEEDED'
        closed=merge_sort_runs(runs,spec=spec,services=svc,window=window)
        next(closed)
        closed.close()
        assert activity['active']==0


def test_group_after_left_join_keeps_composite_null_groups_and_typed_keys(tmp_path):
    from demiflow.execution.kernels.dataflow import _group_key as group_key, group_key_values

    complex_key = {'x': [b'v', None], 'y': (-0.0, True, 1)}
    encoded = group_key({'a': complex_key, 'b': 'y'}, ('a', 'b')).encode()
    assert group_key_values(encoded) == (complex_key, 'y')
    assert group_key(dict(zip(('a', 'b'), group_key_values(encoded))), ('a','b')).encode() == encoded
    rows = [{'a':None, 'b':1, 'v':'first'}, {'a':None, 'b':2,'v':'second'},
            {'a':'s', 'b':1, 'v':'third'}]
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=2):
        result = data.from_items(rows).join(data.from_items([{'a':'s','b':1,'r':12}]),
            on=['a','b'], how='left').group_batches(['a','b'], max_rows=2).take_all()
    assert [(r['a'],r['b'],r['items'][0]['v']) for r in result] == [
        ('s',1,'third'), (None,1,'first'), (None,2,'second')]


def test_fixed_three_way_join_selects_sql_only_under_its_actual_stage_grant(tmp_path, monkeypatch, record_property):
    import lance
    import pyarrow as pa
    from demiflow.execution import datafusion as host
    from demiflow.execution.contracts.relational_region import SQL_CHILD_RSS, SQL_PARENT_WINDOW, interpret_region
    from _native_relational_fixtures import native_region_evidence

    # Duplicates on both joins, a null, and an unmatched nullable right side.
    for name, rows in (
        ('a', [{'k':i, 'seq':i, 'drop_me':'z'*4096} for i in range(48)] +
              [{'k':None, 'seq':48, 'drop_me':'none'}]),
        ('b', [{'j':i,'v':i+100} for i in range(48)] + [{'j':7,'v':1070}]),
        ('c', [{'j':i,'v':i+200} for i in range(47)] + [{'j':7,'v':2070}])):
        lance.write_dataset(pa.Table.from_pylist(rows), str(tmp_path/name))
    processes, queries, selections = [], [], []
    old_start, obtain = host._start_sql_worker, host.query_region_offsets
    def start(*args):
        child = old_start(*args)
        processes.append(child)
        return child
    def query(sql, **kw):
        result = obtain(sql, **kw)
        queries.append((sql, [len(t) for t in kw['tables'].values()], len(result), kw['rss_bytes']))
        return result
    monkeypatch.setattr(host, '_start_sql_worker', start)
    monkeypatch.setattr(host, 'query_region_offsets', query)
    from demiflow.execution.kernels.relational_region import _RegionSpool
    from collections import Counter
    reads, observed = Counter(), []
    raw_key, raw_header = _RegionSpool.key, _RegionSpool.header
    def count_key(self, offset, header):
        reads['keys'] += 1
        return raw_key(self, offset, header)
    def count_header(self, offset):
        reads['headers'] += 1
        return raw_header(self, offset)
    monkeypatch.setattr(_RegionSpool, 'key', count_key)
    monkeypatch.setattr(_RegionSpool, 'header', count_header)
    for grant in (128, 1024):
        reads.clear()
        limits = {'memory_bytes': grant << 20, 'max_row_bytes':65536, 'max_batch_bytes':1 << 20}
        with RunContext.start(tmp_path/str(grant), limits=limits), data.local_execution(
                workers=2, partitions=2, worker_mode='thread', memory_bytes=grant << 20) as local:
            a = data.read_lance(str(tmp_path/'a'), version=1).select_columns(['k','seq'])
            b = data.read_lance(str(tmp_path/'b'), version=1)
            c = data.read_lance(str(tmp_path/'c'), version=1)
            rows = a.join(b, on='k', right_on='j', how='left').join(c,on='k',right_on='j',how='left').take_all()
            graph, physical, counters = native_region_evidence(local)
        stage = next(s for s in physical.stages if s.stage_id.startswith('relational-v1-'))
        selected = interpret_region(graph, stage)
        assert selected.sql_offsets == (grant == 1024)
        if selected.sql_offsets:
            assert stage.resident_bytes >= SQL_CHILD_RSS + SQL_PARENT_WINDOW
            assert counters[0]['offset_sql_queries'] == 1
            assert counters[0]['offset_sql_pairs'] == 50 + len(rows)
        observed.append(reads.copy())
        selections.append(rows)
        assert len(queries) == len(processes) == int(selected.sql_offsets)
    for index, strategy in enumerate(('bounded_hash', 'offset_sql')):
        for kind, count in observed[index].items():
            record_property(strategy + '_' + kind, count)
    assert observed[1]['keys'] < observed[0]['keys']
    assert observed[1]['headers'] < observed[0]['headers']
    assert selections[0] == selections[1]
    assert len(selections[0]) == 52
    assert len(queries) == 1 and queries[0][1:] == ([49,49,48],102,SQL_CHILD_RSS)
    assert queries[0][0].count('JOIN') == 2 and processes[0].returncode == 0


def test_skewed_bucket_refuses_unbounded_native_sort_before_spawning(tmp_path, monkeypatch):
    import hashlib
    import lance
    import pyarrow as pa
    from demiflow.execution.kernels.dataflow import _group_key as group_key
    from demiflow.execution import datafusion as host
    from _native_relational_fixtures import native_region_evidence

    # Distinct keys in one hash bucket. A conservative multiplicity bound
    # rejects 34^3 possible candidates before any native query or full sort.
    keys = []
    for i in range(1000):
        token = group_key({'k':i}, ('k',)).encode()
        if int.from_bytes(hashlib.sha256(token).digest()[:8], 'big') % 16 == 0:
            keys.append(i)
        if len(keys) == 3:
            break
    for n, k in zip('abc', keys):
        lance.write_dataset(pa.table({'k':[k]*34, n:list(range(34))}), str(tmp_path/n))
    def unexpected(*args, **kwargs):
        raise AssertionError('skewed inputs must use the original bounded comparison')
    monkeypatch.setattr(host, '_start_sql_worker', unexpected)
    with RunContext.start(tmp_path/'run', limits={'memory_bytes':1<<30,'max_row_bytes':65536,'max_batch_bytes':1<<20}), \
            data.local_execution(workers=2, worker_mode='thread', memory_bytes=1<<30) as local:
        a,b,c = [data.read_lance(str(tmp_path/n),version=1) for n in 'abc']
        assert a.join(b,on='k',how='left').join(c,on='k',how='left').take_all() == [
            {'k':keys[0],'a':i} for i in range(34)]
        _, _, counters = native_region_evidence(local)
        assert counters[0]['offset_sql_queries'] == 0


def test_public_join_propagates_overflow_after_sql_started(tmp_path, monkeypatch):
    import lance
    import pyarrow as pa
    import pytest
    from demiflow.execution import datafusion
    from demiflow.execution.kernels import relational_region

    for name in ('a', 'b', 'c'):
        lance.write_dataset(pa.table({'k': [3, 6, 9], name: [1, 2, 3]}), str(tmp_path / name))
    original = datafusion.query_region_offsets
    launched = []
    def reduce_output_budget(sql, **kwargs):
        launched.append(True)
        kwargs['max_pairs'] = 0
        return original(sql, **kwargs)
    def unexpected_hash(*args, **kwargs):
        pytest.fail('SQL has started; replaying the action with hash join is invalid')
    monkeypatch.setattr(datafusion, 'query_region_offsets', reduce_output_budget)
    monkeypatch.setattr(relational_region, 'hash_join_pairs', unexpected_hash)
    with pytest.raises(Exception, match='more offset pairs than admitted'):
        with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 1 << 30,
                'max_row_bytes': 65536, 'max_batch_bytes': 1 << 20}) as run, data.local_execution(
                workers=2, partitions=2, worker_mode='thread', memory_bytes=1 << 30):
            a, b, c = [data.read_lance(str(tmp_path / name), version=1) for name in ('a', 'b', 'c')]
            a.join(b, on='k').join(c, on='k').take_all()
    assert launched == [True]
    assert run.journal.execution_action('action-1')['result'] is None


def test_six_public_self_joins_reuse_exact_input_once(tmp_path, monkeypatch):
    import lance
    import pyarrow as pa
    from demiflow.lance import read

    src = str(tmp_path / 'source')
    lance.write_dataset(pa.table({'k': [0, 1]}), src)
    scans = []
    scan = read._iter_scan
    def observed_scan(*args, **kwargs):
        for batch in scan(*args, **kwargs):
            scans.append(batch.num_rows)
            yield batch
    monkeypatch.setattr(read, '_iter_scan', observed_scan)
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(
            workers=4, partitions=1, worker_mode='thread'):
        ds = data.read_lance(src, version=1)
        for _ in range(6):
            ds = ds.join(ds, on='k')
        result = ds.take_all()
        seal = load_sealed_plan(run.directory, 'action-1', version='v3')
        graph = seal['graph']
        assert len(seal['physical'].sources) == 1
        assert len(graph.nodes) == 8
        assert sum(n.kernel.kind == 'join' for n in graph.nodes) == 6
    assert sorted(row['k'] for row in result) == [0, 1]
    assert sum(scans) == 2


def test_public_same_key_fold_reuses_keys_only_without_an_intervening_transform(tmp_path, monkeypatch):
    from collections import Counter
    from demiflow.execution.kernels import relational, relational_region

    ordered, region = relational._ordered_group_rows, relational_region.RelationalRegionRuntime._ordered_groups
    actual = Counter()
    def ordinary(*args, **kwargs):
        actual['full_record_sort'] += 1
        yield from ordered(*args, **kwargs)
    def references(self, *args, **kwargs):
        actual['key_offset_sort'] += 1
        yield from region(self, *args, **kwargs)
    monkeypatch.setattr(relational, '_ordered_group_rows', ordinary)
    monkeypatch.setattr(relational_region.RelationalRegionRuntime, '_ordered_groups', references)

    for changed in (False, True):
        actual.clear()
        path = tmp_path / str(changed)
        def fold(state, row):
            with path.open('a') as f:
                f.write('1\n')
            return {'k': row['k'], 'v': (state or {}).get('v', '') + row['v']}
        with RunContext.start(tmp_path / ('run-' + str(changed))), data.local_execution(
                workers=2, partitions=1, worker_mode='thread'):
            joined = data.from_items([{'k': 0, 'v': 'a'}, {'k': 1, 'v': 'b'}, {'k': 0, 'v': 'c'}]).join(
                data.from_items([{'k': 0}, {'k': 1}]), on='k')
            if changed:
                joined = joined.map(lambda r: {**r, 'k': 1 - r['k']})
            result = joined.reduce_by_key('k', fold).take_all()
        assert {r['k']: r['v'] for r in result} == ({0:'b', 1:'ac'} if changed else {0:'ac', 1:'b'})
        assert len(path.read_text().splitlines()) == 3
        assert actual == ({'full_record_sort':1} if changed else {'key_offset_sort':1})
