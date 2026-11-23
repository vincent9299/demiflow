"""Actual shared actor execution, lifecycle and original checkpoint authority."""
import asyncio
import json
from pathlib import Path

import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext


class Counter:
    def __init__(self, log):
        self.log = str(log)
        self.count = 0

    def record(self, event, **fields):
        with open(self.log, 'a') as stream:
            stream.write(json.dumps({'event': event, **fields}) + '\n')

    async def astart(self):
        self.record('start')

    async def __call__(self, row, amount=1):
        self.count += amount
        await asyncio.sleep(0)
        return row

    def snapshot_state(self):
        self.record('snapshot', count=self.count)
        return {'count': self.count}

    def restore_state(self, value):
        self.record('restore', count=value['count'])
        self.count = value['count']

    async def aclose(self):
        self.record('close', count=self.count)


async def identity(row):
    await asyncio.sleep(0)
    return row


def capture_images(monkeypatch):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.dataflow_kernel_pack import PACK_MEDIA, iter_kernel_images
    from demiflow.execution.contracts import state_codec
    original = DataflowCheckpointCoordinator.commit
    traces = []
    def commit(coordinator, *args, **kwargs):
        image = original(coordinator, *args, **kwargs)
        states = []
        outputs = {}
        for contribution in image.contributions:
            for occurrence, ref in contribution.operator_states.items():
                if ref.media_type == PACK_MEDIA:
                    raw = next(v for k, v in iter_kernel_images(coordinator._store, ref,
                        coordinator._physical.bounds) if k[0] == occurrence)
                elif ref.media_type == 'application/x-demiflow-kernel-image-v3':
                    raw = b''.join(coordinator._store.read(ref, max_bytes=coordinator._physical.bounds.max_state_bytes))
                else:
                    continue
                value = state_codec.loads(raw, max_bytes=coordinator._physical.bounds.max_state_bytes)
                outputs[occurrence] = value['output_cursor']
                if value['state'] is not None:
                    states.append((occurrence, value['state']))
        if not states:
            return image
        traces.append({'states': states, 'offset': sum(c.offset['index'] for c in image.source_vector),
                       'final': image.cut.final, 'image': image, 'outputs': outputs})
        return image
    monkeypatch.setattr(DataflowCheckpointCoordinator, 'commit', commit)
    return traces


def build(actor, *, second=None, api=data, concurrency=3):
    return api.from_items([{'id': i} for i in range(12)]).map_async(actor, concurrency=concurrency).map_async(
        identity, concurrency=2).map_async(actor if second is None else second, concurrency=2)


@pytest.mark.parametrize('worker_mode', ['thread', 'process'])
def test_same_actor_has_one_state_and_lifecycle(tmp_path, monkeypatch, worker_mode):
    traces = capture_images(monkeypatch)
    actor = Counter(tmp_path/'actor.jsonl')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 4, 'checkpoint_interval_ms': 60000, 'max_row_bytes': 1024**2, 'max_batch_bytes': 1024**2}), \
            data.local_execution(worker_mode=worker_mode, workers=3, partitions=1):
        result = build(actor).materialize().take_all()
    assert sorted(result, key=lambda r: r['id']) == [{'id': i} for i in range(12)]
    assert len(traces) >= 2
    for trace in traces:
        hooks = [(o, s) for o, s in trace['states'] if s['mode'] == 'hooks']
        aliases = [(o, s) for o, s in trace['states'] if s['mode'] == 'shared_owner']
        assert len(hooks) == len(aliases) == 1
        assert aliases[0][1]['owner'] == hooks[0][0]
        assert hooks[0][1]['value']['count'] == sum(trace['outputs'][o] for o, _ in hooks + aliases)
    assert traces[-1]['states'][-1][1]['value'] == {'count': 24}
    events = [json.loads(v) for v in Path(actor.log).read_text().splitlines()]
    assert [v['event'] for v in events].count('start') == 1
    assert [v['event'] for v in events].count('close') == 1
    assert len([v for v in events if v['event'] == 'snapshot']) == len(traces)
    assert events[-1] == {'event': 'close', 'count': 24}


def test_distinct_same_value_actors_keep_two_owners(tmp_path, monkeypatch):
    traces = capture_images(monkeypatch)
    first, second = Counter(tmp_path/'events'), Counter(tmp_path/'events')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 4, 'max_row_bytes': 1024**2, 'max_batch_bytes': 1024**2}), \
            data.local_execution(worker_mode='thread', workers=3, partitions=1):
        result = build(first, second=second).materialize().take_all()
    assert len(result) == 12
    final = traces[-1]['states']
    assert [s for _, s in final if s['mode'] == 'shared_owner'] == []
    assert [s['value']['count'] for _, s in final if s['mode'] == 'hooks'] == [12, 12]
    events = [json.loads(v) for v in Path(first.log).read_text().splitlines()]
    assert [v['event'] for v in events].count('start') == 2
    assert [v['event'] for v in events].count('close') == 2


def test_shared_actor_default_public_entry_fits_original_budget(tmp_path):
    actor = Counter(tmp_path/'events')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 4}):
        result = build(actor).materialize().take_all()
    assert sorted(result, key=lambda r: r['id']) == [{'id': i} for i in range(12)]
    events = [json.loads(v) for v in Path(actor.log).read_text().splitlines()]
    assert [v['event'] for v in events].count('start') == 1
    assert events[-1] == {'event': 'close', 'count': 24}


def test_shared_actor_limited_collection_stops_its_live_upstream(tmp_path):
    class Observed(Counter):
        async def __call__(self, row):
            self.record('call', row=row['id'])
            return await super().__call__(row)
    actor = Observed(tmp_path/'events')
    with pytest.raises(ValueError, match='more than the take_all limit'):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 4}):
            build(actor).take_all(limit=1)
    events = [json.loads(v) for v in Path(actor.log).read_text().splitlines()]
    seen = {event['row'] for event in events if event['event'] == 'call'}
    assert 0 < len(seen) < 12


@pytest.mark.parametrize('worker_mode', ['thread', 'process'])
def test_shared_actor_resumes_original_cut_once(tmp_path, monkeypatch, worker_mode):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    traces = capture_images(monkeypatch)
    original = DataflowCheckpointCoordinator.commit
    fault = []
    def crash(coordinator, *args, **kwargs):
        image = original(coordinator, *args, **kwargs)
        hooks = [s for _, s in traces[-1]['states'] if s['mode'] == 'hooks'] if traces else []
        if not fault and hooks and 0 < hooks[0]['value']['count'] < 24:
            fault.append(hooks[0]['value']['count'])
            raise RuntimeError('shared-owner-cut-crash')
        return image
    monkeypatch.setattr(DataflowCheckpointCoordinator, 'commit', crash)
    actor = Counter(tmp_path/'events')
    limits = {'checkpoint_rows': 4, 'checkpoint_interval_ms': 60000,
              'max_row_bytes': 1024**2, 'max_batch_bytes': 1024**2}
    with pytest.raises(RuntimeError, match='shared-owner-cut-crash'):
        with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(worker_mode=worker_mode, workers=3, partitions=1):
            build(actor).materialize()
    assert len(fault) == 1
    with RunContext.resume(run.ref), data.local_execution(worker_mode=worker_mode, workers=3, partitions=1):
        result = build(Counter(tmp_path/'events')).materialize().take_all()
    assert sorted(result, key=lambda r: r['id']) == [{'id': i} for i in range(12)]
    events = [json.loads(v) for v in Path(actor.log).read_text().splitlines()]
    assert [v['count'] for v in events if v['event'] == 'restore'] == fault
    assert [v['event'] for v in events].count('start') == 2
    assert [v['event'] for v in events].count('close') == 2
    assert events[-1] == {'event': 'close', 'count': 24}


def test_shared_actor_keeps_per_occurrence_call_bindings(tmp_path):
    from functools import partial
    actor = Counter(tmp_path/'events')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 4}):
        result = data.from_items([{'id': i} for i in range(6)]).map_async(
            partial(actor.__call__, amount=2)).map_async(
            partial(actor.__call__, amount=3)).materialize().take_all()
    assert result == [{'id': i} for i in range(6)]
    events = [json.loads(v) for v in Path(actor.log).read_text().splitlines()]
    assert [v['event'] for v in events].count('start') == 1
    assert events[-1] == {'event': 'close', 'count': 30}


def test_shared_explicit_infeasible_grant_rejects_before_start(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    actor = Counter(tmp_path/'events')
    with pytest.raises(DataflowError, match='Shared actor closure') as error:
        with RunContext.start(tmp_path/'run', limits={'max_row_bytes': 16*1024**2,
                'max_batch_bytes': 16*1024**2}), data.local_execution(worker_mode='thread', workers=3, partitions=1):
            build(actor, concurrency=4).materialize()
    assert error.value.code == 'BUDGET_EXCEEDED'
    assert not Path(actor.log).exists()


class SyncCounter(Counter):
    def __call__(self, row, amount=1):
        self.count += amount
        return row


def test_shared_sync_owner_survives_original_task_boundaries(tmp_path):
    actor = SyncCounter(tmp_path/'events')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2,
            'max_row_bytes': 1024**2, 'max_batch_bytes': 1024**2}), \
            data.local_execution(batch_rows=2, workers=3, partitions=1):
        result = data.from_items([{'id': i} for i in range(6)]).map(
            actor, fn_kwargs={'amount': 2}).map(actor, fn_kwargs={'amount': 3}).take_all()
    assert result == [{'id': i} for i in range(6)]
    events = [json.loads(v) for v in Path(actor.log).read_text().splitlines()]
    assert [v['event'] for v in events].count('start') == 1
    assert events[-1] == {'event': 'close', 'count': 30}


@pytest.fixture(scope='module')
def owned_ray():
    import tempfile
    from runtime_v3_regression_inventory import assert_private
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from demiflow.data.api import DataAPI
    ray = pytest.importorskip('ray')
    assert_private()
    assert not ray.is_initialized()
    ray.init(address='local', num_cpus=8, num_gpus=0, include_dashboard=False,
             object_store_memory=100*1024**2,
             _temp_dir=tempfile.mkdtemp(prefix='demiflow-shared-ray-', dir='/tmp'))
    executor = RayDatasetExecutor(workers=3)
    try:
        yield DataAPI(executor=executor)
    finally:
        try:
            executor.close()
        finally:
            ray.shutdown()
    assert not ray.is_initialized()


def test_shared_actor_real_ray_cut_recovery(tmp_path, monkeypatch, owned_ray):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    traces = capture_images(monkeypatch)
    original = DataflowCheckpointCoordinator.commit
    fault = []
    def crash(coordinator, *args, **kwargs):
        image = original(coordinator, *args, **kwargs)
        hooks = [s for _, s in traces[-1]['states'] if s['mode'] == 'hooks'] if traces else []
        if not fault and hooks and 0 < hooks[0]['value']['count'] < 24:
            fault.append(hooks[0]['value']['count'])
            raise RuntimeError('shared-owner-ray-cut-crash')
        return image
    monkeypatch.setattr(DataflowCheckpointCoordinator, 'commit', crash)
    limits = {'checkpoint_rows': 4, 'checkpoint_interval_ms': 60000,
              'max_row_bytes': 1024**2, 'max_batch_bytes': 1024**2}
    with pytest.raises(RuntimeError, match='shared-owner-ray-cut-crash'):
        with RunContext.start(tmp_path/'run', limits=limits) as run:
            build(Counter(tmp_path/'events'), api=owned_ray).materialize()
    assert len(fault) == 1
    with RunContext.resume(run.ref):
        result = build(Counter(tmp_path/'events'), api=owned_ray).materialize().take_all()
    assert sorted(result, key=lambda r: r['id']) == [{'id': i} for i in range(12)]
    events = [json.loads(v) for v in (tmp_path/'events').read_text().splitlines()]
    assert [v['count'] for v in events if v['event'] == 'restore'] == fault
    assert [v['event'] for v in events].count('start') == 2
    assert [v['event'] for v in events].count('close') == 2
    assert events[-1] == {'event': 'close', 'count': 24}


def test_two_identical_shared_bundles_do_not_merge_distinct_actors(tmp_path, monkeypatch):
    traces = capture_images(monkeypatch)
    first, second = Counter(tmp_path/'events'), Counter(tmp_path/'events')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2}):
        result = data.from_items([{'id': i} for i in range(4)]).map_async(first).map_async(
            first).map_async(second).map_async(second).take_all()
    assert result == [{'id': i} for i in range(4)]
    states = traces[-1]['states']
    assert sorted(s['value']['count'] for _, s in states if s['mode'] == 'hooks') == [8, 8]
    assert len({s['owner'] for _, s in states if s['mode'] == 'shared_owner'}) == 2


def test_shared_lifecycle_observation_reports_actual_close(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    original = DataflowExecution.close
    observed = []
    def close(execution):
        try:
            return original(execution)
        finally:
            observed.extend(dict(v) for _, v in getattr(execution, '_operator_lifecycles', {}).values())
    monkeypatch.setattr(DataflowExecution, 'close', close)
    actor = Counter(tmp_path/'events')
    with RunContext.start(tmp_path/'run'):
        assert data.from_items([]).map_async(actor).map_async(actor).take_all() == []
    assert observed
    assert sum(v['created'] for v in observed) == 1
    assert sum(v['started'] for v in observed) == 1
    assert sum(v['closed'] for v in observed) == 1


@pytest.mark.parametrize('shape', ['path', 'crossed'])
def test_mandatory_shared_placement_is_acyclic_and_preserves_requests(shape):
    from types import SimpleNamespace as NS
    from demiflow.execution.contracts.dataflow import StageSpec
    from demiflow.resources.contracts import ResourceProfile
    from demiflow.planning.dataflow_shared import colocate_shared_actors
    names = ['a', 'middle', 'b'] if shape == 'path' else ['a', 'b', 'c', 'd']
    groups = [['a', 'b']] if shape == 'path' else [['a', 'c'], ['b', 'd']]
    edges = [('a', 'middle'), ('middle', 'b')] if shape == 'path' else [('a', 'd'), ('b', 'c')]
    bindings = {n: {'shared_actor': {'owner': g[0], 'members': g, 'state_owner': g[-1]}}
                for g in groups for n in g}
    graph = NS(nodes=tuple(NS(occurrence_id=n, kernel=NS(configuration=bindings.get(n, {}))) for n in names),
               edges=tuple(NS(producer=a, consumer=b) for a, b in edges))
    stages = tuple(StageSpec(stage_id=n, occurrences=(n,), partitions=('p-'+n,),
        profile=ResourceProfile(requests={'cpu_millis': 1000, 'memory_bytes': 100}),
        replica_min=1, replica_target=1, replica_max=1, resident_bytes=80,
        output_bytes=10, state_bytes=20, kernel_refs=()) for n in names)
    hosts = (NS(logical_resources={'cpu_millis': 8000, 'memory_bytes': 10000}),)
    result = colocate_shared_actors(graph, stages,
        bounds=NS(max_resident_bytes=10000, max_control_bytes=1000), hosts=hosts)
    assert len(result) == 1  # Middle path and crossed owners cannot be left outside.
    assert set(result[0].occurrences) == set(names)
    assert result[0].profile.requests['cpu_millis'] == len(names)*1000
    assert result[0].profile.requests['memory_bytes'] == len(names)*100
    assert result[0].state_bytes == len(names)*20


@pytest.mark.parametrize('state', [None, {'mode': 'hooks', 'value': {}},
    {'mode': 'shared_owner', 'owner': 'wrong'}])
def test_shared_restore_rejects_missing_or_wrong_alias(state):
    from demiflow.execution.dataflow_shared import SharedActor
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    actor = SharedActor({'owner': 'a', 'members': ['a', 'b'], 'state_owner': 'b'}, object())
    with pytest.raises(DataflowError, match='Shared actor'):
        actor.restoring('a', state)
    assert actor.restored == set()


def test_repeated_class_factory_is_not_a_shared_instance(tmp_path, monkeypatch):
    traces = capture_images(monkeypatch)
    with RunContext.start(tmp_path/'run'):
        result = data.from_items([{'id': i} for i in range(4)]).map(
            SyncCounter, fn_constructor_args=[str(tmp_path/'events')]).map(
            SyncCounter, fn_constructor_args=[str(tmp_path/'events')]).take_all()
    assert result == [{'id': i} for i in range(4)]
    assert all(s['mode'] != 'shared_owner' for t in traces for _, s in t['states'])
    events = [json.loads(v) for v in (tmp_path/'events').read_text().splitlines()]
    assert [v['count'] for v in events if v['event'] == 'close'] == [4, 4]


def test_same_function_remains_two_real_invocations(tmp_path):
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    def increment(row):
        return {'id': row['id'] + 1}
    with RunContext.start(tmp_path/'run') as run:
        assert data.from_items([{'id': 0}]).map(increment).map(increment).take_all() == [{'id': 2}]
        graph = load_sealed_plan(run.ref.directory, 'action-1')['graph']
        maps = [n for n in graph.nodes if n.kernel.kind == 'MapOp']
        assert len(maps) == 2
        assert all('shared_actor' not in n.kernel.configuration for n in maps)


@pytest.mark.parametrize('original_shared,current_shared,allowed', [
    (True, True, True), (False, False, True), (True, False, False), (False, True, False)])
def test_recipe_alias_validation_checks_both_directions(tmp_path, original_shared, current_shared, allowed):
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.dataflow_api import _CompileContext
    from demiflow.execution.run_context import RunAction
    from demiflow.execution.dataflow_shared import validate_shared_actor_binding
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    action = RunAction(_CompileContext(), 'alias-history', {})
    def request(shared):
        first, second = Counter(tmp_path/'events'), Counter(tmp_path/'events')
        ds = build(first, second=first if shared else second)
        return ActionRequest(kind='take_all', source=ds._source, plan=ds._plan,
            arguments={}, sink=None, programs=(), provider='local', action=action)
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    graph = lower_action(request(original_shared), bounds=bounds).graph
    before = graph.dag_id
    assert sum(n.kernel.configuration.get('actor_binding_version') == 1 for n in graph.nodes) == 2
    if allowed:
        validate_shared_actor_binding(request(current_shared), graph, bounds=bounds)
    else:
        with pytest.raises(DataflowError, match='Actor instance sharing differs') as error:
            validate_shared_actor_binding(request(current_shared), graph, bounds=bounds)
        assert error.value.code == 'INCOMPATIBLE_HISTORY'
    assert graph.dag_id == before
    assert not (tmp_path/'events').exists(), 'pure history validation must not start business actors'


def test_unversioned_history_is_not_reinterpreted():
    from types import SimpleNamespace
    from demiflow.execution.dataflow_shared import validate_shared_actor_binding
    # No new marker => no capture or interpretation of a new recipe at all.
    class Refuse:
        def __getattribute__(self, name):
            raise AssertionError('legacy validation attempted to inspect a new recipe')
    graph = SimpleNamespace(nodes=(SimpleNamespace(kernel=SimpleNamespace(configuration={})),))
    validate_shared_actor_binding(Refuse(), graph, bounds=None)


@pytest.mark.parametrize('source_kind', ['items', 'iterable', 'sql'])
@pytest.mark.parametrize('change_alias', [False, True])
def test_alias_admission_never_reads_source_or_constructs_factory(tmp_path, monkeypatch, source_kind, change_alias):
    from demiflow.data.api import DataAPI
    from demiflow.data.dataset import Dataset
    from demiflow.data.plan import LogicalPlan
    from demiflow.data.sources import ItemsSource, IterableSource, SqlSource
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.dataflow_api import _CompileContext
    from demiflow.execution.run_context import RunAction
    from demiflow.execution.dataflow_shared import validate_shared_actor_binding
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    calls = []
    def refuse(*args, **kwargs):
        calls.append('business-read')
        raise AssertionError('alias admission executed business source')
    class Factory:
        def __new__(cls):
            calls.append('construct')
            raise AssertionError('alias admission instantiated a class actor')
        def __call__(self, row):
            return row
    if source_kind == 'items':
        source = ItemsSource(({'id': 0},))
    elif source_kind == 'iterable':
        source = IterableSource(refuse)
    else:
        source = SqlSource('SELECT * FROM must_not_connect', refuse, {})
    executor = DataAPI()._executor
    action = RunAction(_CompileContext(), 'alias-passive', {})
    def request(shared):
        actor = Counter(tmp_path/'events')
        second = actor if shared else Counter(tmp_path/'events')
        ds = Dataset(source, LogicalPlan(), executor).map(Factory).map_async(actor).map_async(second)
        return ActionRequest(kind='take_all', source=ds._source, plan=ds._plan,
            arguments={}, sink=None, programs=(), provider='local', action=action)
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    graph = lower_action(request(True), bounds=bounds).graph
    # Guard every full-capture/row-schema path after the original plan exists.
    import demiflow.planning.dataflow_compile as compiler
    import demiflow.execution.dataflow_schema as schema
    monkeypatch.setattr(compiler, 'lower_action', refuse)
    monkeypatch.setattr(compiler, '_source_semantic_facts', refuse)
    monkeypatch.setattr(schema, 'literal_schema', refuse)
    if change_alias:
        with pytest.raises(DataflowError, match='Actor instance sharing differs'):
            validate_shared_actor_binding(request(False), graph, bounds=bounds)
    else:
        validate_shared_actor_binding(request(True), graph, bounds=bounds)
    assert calls == []
    assert not (tmp_path/'events').exists()


@pytest.mark.parametrize('shape', ['union', 'prepend'])
@pytest.mark.parametrize('changed', [False, True])
def test_passive_actor_relation_matches_multi_input_occurrences(tmp_path, monkeypatch, shape, changed):
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.dataflow_api import _CompileContext
    from demiflow.execution.run_context import RunAction
    from demiflow.execution.dataflow_shared import validate_shared_actor_binding
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    action = RunAction(_CompileContext(), 'alias-multi-input', {})
    def request(shared):
        first = SyncCounter(tmp_path/'events')
        second = first if shared else SyncCounter(tmp_path/'events')
        base = data.from_items([{'id': 0}])
        if shape == 'union':
            ds = base.map(first).union(base.map(second))
        else:
            ds = base.map(first).prepend(data.from_items([{'id': 1}]).map(second), max_rows=1).map(first)
        return ActionRequest(kind='take_all', source=ds._source, plan=ds._plan,
            arguments={}, sink=None, programs=(), provider='local', action=action)
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    graph = lower_action(request(True), bounds=bounds).graph
    import demiflow.execution.dataflow_schema as schema
    def refuse(*args, **kwargs):
        raise AssertionError('alias validation read business rows')
    monkeypatch.setattr(schema, 'literal_schema', refuse)
    if changed:
        with pytest.raises(DataflowError, match='Actor instance sharing differs'):
            validate_shared_actor_binding(request(False), graph, bounds=bounds)
    else:
        validate_shared_actor_binding(request(True), graph, bounds=bounds)
    assert not (tmp_path/'events').exists()
