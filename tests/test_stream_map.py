"""Public map streaming policy: scheduling, ownership and execution boundaries."""
import asyncio
import contextvars
import threading
import time
from collections import Counter
from functools import partial

import pyarrow as pa
import pytest

from demiflow.data.api import DataAPI
from demiflow.data.dataset import Dataset
from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import IterableSource
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.executors.local import LocalDatasetExecutor


@pytest.fixture
def data():
    executor = LocalDatasetExecutor(workers=2, block_size=2, materialize_memory_limit=128)
    try:
        yield DataAPI(executor)
    finally:
        executor.close()


def test_thread_map_only_overlaps_preserves_context_and_arguments(data):
    barrier = threading.Barrier(3, timeout=5)
    run = contextvars.ContextVar('map-run', default=None)
    run.set('example')
    threads = set()

    def transform(row, add, *, scale):
        assert run.get() == 'example'
        threads.add(threading.current_thread().name)
        barrier.wait()
        return {'i': row['i'], 'value': (row['i'] + add) * scale}

    result = (data.from_items([{'i': i} for i in range(6)])
              .map(transform, fn_args=[2], fn_kwargs={'scale': 3},
                   execution='thread', concurrency=3, queue_depth=1)
              .materialize().take_all())
    assert sorted(result, key=lambda row: row['i']) == [{'i': i, 'value': (i + 2) * 3} for i in range(6)]
    assert len(threads) == 3
    assert not any(t.name in threads for t in threading.enumerate())


def test_thread_map_allows_async_consumer_to_release_other_workers(data):
    barrier = threading.Barrier(3, timeout=5)
    consumed = threading.Event()

    def prepare(row):
        barrier.wait()
        if row['i']:
            assert consumed.wait(5), 'map blocked the downstream event loop'
        return row

    async def consume(row):
        consumed.set()
        return row

    stats = (data.from_items([{'i': i} for i in range(3)])
             .map(prepare, execution='thread', concurrency=3, queue_depth=1)
             .map_async(consume).run_stream())
    assert stats.emitted == 3


def test_default_map_remains_lazy_and_single_inline_stream_stage(data):
    def transform(row, *, value):
        return {'i': row['i'] + value}

    source = data.from_items([{'i': i} for i in range(4)])
    assert sorted(source.map(transform, fn_kwargs={'value': 2}).take_all(), key=lambda r: r['i']) == [
        {'i': i + 2} for i in range(4)]
    with pytest.raises(ValueError, match='explicit map'):
        source.map(transform, fn_kwargs={'value': 2}).run_stream()
    seen = []
    main_thread = threading.get_ident()
    def record(row):
        assert threading.get_ident() == main_thread
        seen.append(row['i'])
        return row
    stats = source.map_async(lambda row: row).map(record).run_stream()
    assert seen == list(range(4))
    assert next(q for q in stats.metrics['queues'] if q['stage'] == 'record')['capacity'] == 1


@pytest.mark.parametrize('scope,instances', [('stage', 1), ('worker', 3)])
def test_class_scope_is_lazy_recreated_per_action_and_closed(data, scope, instances):
    events = []
    barrier = threading.Barrier(3, timeout=5)

    class Stateful:
        def __init__(self, prefix, *, suffix):
            self.identity = len([event for event in events if event[0] == 'init'])
            self.count = 0
            self.prefix, self.suffix = prefix, suffix
            self.lock = threading.Lock()
            events.append(('init', self.identity))
        async def astart(self):
            await asyncio.sleep(0)
            events.append(('start', self.identity))
        def __call__(self, row, *, add):
            assert ('start', self.identity) in events
            with self.lock:
                self.count += 1
                count = self.count
            barrier.wait()
            return {**row, 'owner': self.identity, 'count': count,
                    'text': self.prefix + self.suffix, 'value': row['i'] + add}
        async def astop(self):
            events.append(('stop', self.identity))
        async def aclose(self):
            assert ('stop', self.identity) in events
            events.append(('close', self.identity))

    plan = data.from_items([{'i': i} for i in range(6)]).map(
        Stateful, fn_constructor_args=['a'], fn_constructor_kwargs={'suffix': 'b'},
        fn_kwargs={'add': 10}, callable_scope=scope, execution='thread', concurrency=3)
    assert events == []
    for action in range(2):
        result = plan.materialize().take_all()
        assert len({r['owner'] for r in result}) == instances
        assert all(r['text'] == 'ab' and r['value'] == r['i'] + 10 for r in result)
        if scope == 'worker':
            assert sorted(Counter(r['owner'] for r in result).values()) == [2, 2, 2]
            assert all(sorted(r['count'] for r in result if r['owner'] == owner) == [1, 2]
                       for owner in {r['owner'] for r in result})
        assert Counter(event[0] for event in events) == {
            name: instances * (action + 1) for name in ['init', 'start', 'stop', 'close']}


def test_empty_and_filtered_inputs_do_not_construct_worker_classes(data):
    class MustNotConstruct:
        def __init__(self):
            raise AssertionError('empty stage constructed an actor')
        def __call__(self, row):
            return row
    for rows in ([], [{'i': 1}]):
        stats = (data.from_items(rows).filter(lambda row: False)
                 .map(MustNotConstruct, concurrency=3, callable_scope='worker', execution='thread')
                 .run_stream())
        assert stats.emitted == 0


@pytest.mark.parametrize('execution', ['inline', 'thread'])
def test_partial_bound_inputs_and_outputs_keep_original_fields(data, execution):
    def calculate(value, *, add):
        return {'sum': value + add}
    result = (data.from_items([{'x': 3, 'keep': 'yes'}])
              .map(partial(calculate, add=4), inputs={'value': 'x'}, outputs={'sum': 'answer'},
                   execution=execution).materialize().take_all())
    assert result == [{'x': 3, 'keep': 'yes', 'answer': 7}]


def test_explicit_catch_and_drop_preserve_metrics_and_do_not_catch_timeouts(data):
    def transform(row):
        if row['i'] == 0:
            raise LookupError('expected absence')
        return row if row['i'] % 2 else None
    stats = (data.from_items([{'i': i} for i in range(6)])
             .map(transform, execution='thread', concurrency=2, catch=(LookupError,), label='prepare')
             .run_stream())
    assert stats.emitted == 3
    assert stats.miss == {'prepare:LookupError': 1, 'prepare:drop': 2}
    assert stats.metrics['stage_processing_latency']['prepare']['count'] == 6
    def timeout(row):
        raise TimeoutError('callback deadline')
    for execution in ['inline', 'thread']:
        with pytest.raises(TimeoutError, match='callback deadline'):
            data.from_items([{'i': 1}]).map(timeout, execution=execution).run_stream()
        caught = data.from_items([{'i': 1}]).map(timeout, execution=execution, catch=(TimeoutError,)).run_stream()
        assert caught.miss == {'timeout:TimeoutError': 1}


@pytest.mark.parametrize('failure', ['construct', 'start', 'call'])
def test_worker_failures_cleanup_even_partial_start(data, failure):
    events = []
    class Fail:
        def __init__(self):
            events.append('construct')
            if failure == 'construct':
                raise ValueError('construct failed')
        async def astart(self):
            events.append('start')
            if failure == 'start':
                raise ValueError('start failed')
        def __call__(self, row):
            raise ValueError('call failed')
        async def astop(self):
            events.append('stop')
        async def aclose(self):
            events.append('close')
    # Even a broad row catch must not suppress initialization failures.
    caught = (Exception,) if failure != 'call' else ()
    with pytest.raises(ValueError, match=failure + ' failed'):
        (data.from_items([{'i': 1}]).map(Fail, execution='thread', callable_scope='worker', catch=caught)
         .run_stream())
    assert events == (['construct'] if failure == 'construct' else ['construct', 'start', 'stop', 'close'])


def test_failure_drains_sibling_thread_before_all_instance_cleanup(data):
    started = threading.Barrier(2, timeout=5)
    finished = threading.Event()
    closed = []
    class Work:
        def __call__(self, row):
            started.wait()
            if row['i'] == 0:
                raise RuntimeError('row failed')
            time.sleep(.3)
            finished.set()
            return row
        async def astop(self):
            assert finished.is_set()
        async def aclose(self):
            assert finished.is_set()
            closed.append(self)
    with pytest.raises(RuntimeError, match='row failed'):
        (data.from_items([{'i': 0}, {'i': 1}])
         .map(Work, execution='thread', callable_scope='worker', concurrency=2).materialize())
    assert len(closed) == 2
    assert not data._executor._spill_paths


def test_stop_file_waits_for_thread_before_cleanup(data, tmp_path):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    closed = []
    class Work:
        def __call__(self, row):
            entered.set()
            assert release.wait(8)
            finished.set()
            return row
        async def aclose(self):
            assert finished.is_set()
            closed.append(True)
    plan = data.from_items([{'i': 0}]).map(Work, execution='thread', callable_scope='worker')
    from demiflow.execution.request_limits import ServiceStopped
    async def run():
        stop = tmp_path / 'STOP'
        task = asyncio.create_task(asyncio.to_thread(plan.run_stream, stop_file=stop))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            stop.touch()
            await asyncio.sleep(.35)
            assert not task.done() and not closed
        finally:
            release.set()
        with pytest.raises(ServiceStopped):
            await asyncio.wait_for(task, 8)
    asyncio.run(run())
    assert closed == [True]


def test_slow_consumer_bounds_wide_rows_and_inflight_calls(data):
    lock = threading.Lock()
    produced = active = peak = 0
    pause_snapshot = []
    def source():
        nonlocal produced
        for i in range(32):
            with lock:
                produced += 1
            yield {'i': i, 'payload': str(i) + 'x' * (128 * 1024)}
    def work(row):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(.005)
            return row
        finally:
            with lock:
                active -= 1
    async def consume(row):
        if not pause_snapshot:
            await asyncio.sleep(.1)
            pause_snapshot.append(produced)
        return row
    stats = (data.from_iter(source).map(work, execution='thread', concurrency=3, queue_depth=2)
             .map_async(consume, concurrency=1, queue_depth=1).run_stream(source_batch_size=1))
    # Consumer (1) + its queue (1) + workers (3) + input queue (2) + feeder (1).
    assert pause_snapshot[0] <= 8
    assert 1 < peak <= 3 and active == 0 and stats.emitted == 32
    assert all(q['peak'] <= q['capacity'] for q in stats.metrics['queues'])


@pytest.mark.parametrize('options', [
    {'concurrency': 0}, {'concurrency': True}, {'concurrency': 1.5},
    {'queue_depth': -1}, {'queue_depth': '2'}, {'execution': 'process'},
    {'catch': [ValueError]}, {'catch': (object,)}, {'catch': (BaseException,)},
    {'label': ''}, {'label': 2}, {'callable_scope': 'thread'}, {'callable_scope': 'worker'},
])
def test_invalid_policy_is_rejected_without_running_user_code(data, options):
    with pytest.raises((TypeError, ValueError)):
        data.from_items([]).map(lambda row: row, **options)


def test_async_functions_and_hidden_awaitables_are_rejected(data):
    async def async_fn(row):
        return row
    class AsyncActor:
        async def __call__(self, row):
            return row
    for fn in (async_fn, partial(async_fn), AsyncActor, AsyncActor()):
        with pytest.raises(TypeError, match='synchronous'):
            data.from_items([]).map(fn, execution='inline')
    for execution in ['inline', 'thread']:
        for bindings in ({}, {'inputs': {'value': 'i'}, 'output': 'answer'}):
            with pytest.raises(TypeError, match='awaitable'):
                (data.from_items([{'i': 1}]).map(lambda value: async_fn(value),
                     execution=execution, **bindings).run_stream())
    for unsupported in ({'hard_timeout': 1}, {'checkpoint': 'unused'}):
        with pytest.raises(TypeError):
            data.from_items([]).map(lambda row: row, execution='thread', **unsupported)


@pytest.mark.parametrize('options', [{'concurrency': 1}, {'queue_depth': 1},
    {'execution': 'inline'}, {'catch': ()}, {'label': 'x'}, {'callable_scope': 'stage'}])
@pytest.mark.parametrize('action', ['take', 'take_zero', 'take_all', 'count', 'write_lance'])
def test_every_explicit_option_supports_lazy_actions_without_eager_reads(data, options, action, tmp_path):
    from _worker_event_fixtures import emit, events
    import lance
    path = str(tmp_path / 'reads.jsonl')
    def source():
        emit(path, 'read')
        yield {'i': 1}
    plan = data.from_iter(source).map(lambda row: row, **options)
    assert events(path) == []  # Declaration stays lazy on every terminal path.
    if action == 'take':
        assert plan.take(1) == [{'i': 1}]
    elif action == 'take_zero':
        assert plan.take(0) == []
    elif action == 'write_lance':
        plan.write_lance(str(tmp_path / 'out.lance'), schema=pa.schema([('i', pa.int64())]))
        assert lance.dataset(str(tmp_path / 'out.lance')).to_table().to_pylist() == [{'i': 1}]
    elif action == 'take_all':
        assert plan.take_all() == [{'i': 1}]
    else:
        assert plan.count() == 1
    assert [event['event'] for event in events(path)] == ([] if action == 'take_zero' else ['read'])
    assert (tmp_path / 'out.lance').exists() == (action == 'write_lance')


def test_ray_explain_accepts_stream_options_and_rejects_legacy_tuning_before_io(data):
    # Compile-only preflight: Ray Core execution is tested by the real provider
    # suite. Valid stream options no longer mean an unsupported Ray Data path.
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    executor = RayDatasetExecutor()
    def forbidden():
        raise AssertionError('source was read during compile-only preflight')
        yield
    try:
        api = DataAPI(executor)
        plan = api.from_iter(forbidden).map(lambda row: row, execution='thread')
        for action in ('take_all', 'run_stream', 'materialize'):
            report = executor.plan(plan._source, plan._plan, action)
            assert report.supported
        with pytest.raises(ValueError, match='unsupported'):
            api.from_iter(forbidden).map(lambda row: row,
                backend_options={'ray': {'batch_size': 2}})
    finally:
        executor.close()


def test_prepend_rejects_configured_maps_but_accepts_plain_map(data):
    source = data.from_items([{'i': 1}])
    live = source.map(lambda row: row, execution='thread')
    with pytest.raises(ValueError, match='synchronous Dataset'):
        live.prepend(live, max_rows=1)
    stats = live.prepend(source.map(lambda row: row), max_rows=1).run_stream()
    assert stats.emitted == 2


@pytest.mark.parametrize('kind', ['json', 'lance'])
def test_retired_checkpoint_is_rejected_and_native_run_replays_without_calls(data, kind, tmp_path):
    from _worker_event_fixtures import emit, events
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.recovery_contract import RecoveryContractError
    import lance
    path = str(tmp_path / 'calls.jsonl')
    def transform(row):
        emit(path, 'call', i=row['i'])
        return row
    source = data.from_items([{'i': 1}, {'i': 2}]).map(transform, execution='thread', concurrency=2)
    with pytest.raises(RecoveryContractError, match='retired'):
        if kind == 'json':
            source.checkpoint(tmp_path / 'checkpoint.jsonl', version='v1')
        else:
            source.checkpoint_lance(str(tmp_path / 'checkpoint.lance'),
                                    schema=pa.schema([('i', pa.int64())]), fingerprint='v1')
    assert events(path) == []
    assert not (tmp_path / 'checkpoint.jsonl').exists() and not (tmp_path / 'checkpoint.lance').exists()
    # The same native journal now owns both result replay and table receipts.
    # JSON checkpoint was retired; its replacement is the committed row terminal.
    def save():
        if kind == 'json':
            return source.take_all()
        source.write_lance(str(tmp_path / 'native.lance'), mode='create', schema=pa.schema([('i', pa.int64())]))
        return lance.dataset(str(tmp_path / 'native.lance')).to_table().to_pylist()
    with pytest.raises(RuntimeError, match='after confirmed result'):
        with RunContext.start(tmp_path / 'run') as run:
            assert sorted(save(), key=lambda r: r['i']) == [{'i': 1}, {'i': 2}]
            ref = run.ref
            commits = run.journal.execution_commits('action-1')
            assert commits and all(item['status'] == 'acked' for item in commits)
            raise RuntimeError('after confirmed result')
    first = events(path)
    assert sorted(event['i'] for event in first) == [1, 2]
    with RunContext.resume(ref) as resumed:
        assert sorted(save(), key=lambda r: r['i']) == [{'i': 1}, {'i': 2}]
        assert resumed.journal.execution_commits('action-1') == commits
    assert events(path) == first
    if kind == 'lance':
        assert lance.dataset(str(tmp_path / 'native.lance')).version == 1


def test_stream_native_recovery_preserves_sealed_policy_and_new_run_can_append(data, tmp_path):
    from demiflow.execution.stream_checkpoint import StreamCheckpoint
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.recovery_contract import RecoveryContractError
    from _worker_event_fixtures import emit, events
    import lance
    with pytest.raises(RecoveryContractError, match='retired'):
        StreamCheckpoint(tmp_path / 'checkpoint.json', identity={'input': 'fixed'})
    schema = pa.schema([('i', pa.int64())])
    path = str(tmp_path / 'calls.jsonl')
    def transform(row):
        emit(path, 'call', i=row['i'])
        return row
    def build(rows, concurrency):
        return (data.from_items(rows).map(transform, execution='thread', concurrency=concurrency)
                .save_lance(str(tmp_path / 'out.lance'), schema=schema, key='i', stage='out', mode='append'))
    with pytest.raises(RecoveryContractError, match='retired'):
        build([{'i': 1}], 1).run_stream(checkpoint=object())
    assert events(path) == [] and not (tmp_path / 'out.lance').exists()
    with pytest.raises(RuntimeError, match='after confirmed result'):
        with RunContext.start(tmp_path / 'first') as run:
            first = build([{'i': 1}], 1).run_stream().outputs['out']
            ref = run.ref
            raise RuntimeError('after confirmed result')
    original = events(path)
    with RunContext.resume(ref):
        assert build([{'i': 1}], 1).run_stream().outputs['out'] == first
    assert events(path) == original
    # A new action may choose another concurrency; historical policy is not rewritten.
    with RunContext.start(tmp_path / 'next'):
        last = build([{'i': 2}], 2).run_stream().outputs['out']
    assert sorted(lance.dataset(**last).to_table().to_pylist(), key=lambda r: r['i']) == [{'i': 1}, {'i': 2}]
    assert sorted(event['i'] for event in events(path)) == [1, 2]


@pytest.mark.parametrize('mode', ['thread', 'process'])
def test_nested_relations_support_stream_options_without_eager_source_access(tmp_path, mode):
    from demiflow import data as public_data
    from _worker_event_fixtures import emit, events
    path = str(tmp_path / 'reads.jsonl')
    owned = tmp_path / 'owned'
    owned.mkdir()
    def source():
        emit(path, 'read')
        yield {'i': 1}
    with public_data.local_execution(workers=2, worker_mode=mode, temp_directory=str(owned)):
        plain = public_data.from_iter(source)
        streaming = plain.map(lambda row: row, concurrency=2, execution='thread')
        plans = [streaming.join(plain, on='i'), plain.join(streaming, on='i'),
                 streaming.reduce_by_key('i', lambda acc, row: row),
                 streaming.group_batches('i'), streaming.union(plain), plain.union(streaming)]
        assert events(path) == []
        expected = [[{'i': 1}], [{'i': 1}], [{'i': 1}],
                    [{'i': 1, 'items': [{'i': 1}], 'group_index': 0, 'group_last': True}],
                    [{'i': 1}, {'i': 1}], [{'i': 1}, {'i': 1}]]
        for plan, rows in zip(plans, expected):
            assert plan.take_all() == rows
        assert streaming.take_all() == [{'i': 1}]
        assert streaming.count() == 1
    # Reusing the same source in two ports does not execute a second scan.
    assert len(events(path)) == 8
    assert not list(owned.iterdir())


def test_whole_graph_preflight_precedes_construction_and_source_access(data, tmp_path):
    from _worker_event_fixtures import emit, events
    path = str(tmp_path / 'preflight.jsonl')
    class Actor:
        def __init__(self):
            emit(path, 'construct')
        def __call__(self, row):
            return row
    def source():
        emit(path, 'read')
        yield {'i': 1}
    plain = data.from_iter(source)
    native = {'ray': {'compute': {'kind': 'task_pool', 'size': 2}}}
    with pytest.raises(ValueError, match='backend_options'):
        plain.map(Actor, concurrency=2, backend_options=native)
    invalid = (plain.map(Actor).map(lambda row: row, backend_options=native)
               .map(lambda row: row, execution='thread'))
    with pytest.raises((ValueError, UnsupportedExecutionOptionError), match='backend_options|Ray Data|native option'):
        invalid.run_stream()
    assert events(path) == []
    # Limit is now supported; it cannot remain an old 'unsupported operation'
    # negative. Its true source and constructor laziness is checked here.
    valid = plain.map(Actor, execution='thread').limit(1)
    assert events(path) == []
    assert valid.run_stream().emitted == 1
    assert Counter(event['event'] for event in events(path)) == {'construct': 1, 'read': 1}


def test_policy_metrics_and_plan_summary_keep_duplicate_nodes_distinct(data):
    import json
    from demiflow.observability import plan_summary
    pipeline = (data.from_items([{'i': 1}])
                .map(lambda row: row, concurrency=3, execution='thread', catch=(LookupError,), label='prepare')
                .map(lambda row: row, concurrency=1, queue_depth=2, label='prepare'))
    policies = pipeline.run_stream().metrics['stage_policies']
    assert policies == {
        'prepare': dict(concurrency=3, queue_depth=3, execution='thread', callable_scope='stage',
                        catch=['builtins.LookupError']),
        'prepare#2': dict(concurrency=1, queue_depth=2, execution='inline', callable_scope='stage', catch=[])}
    summary = json.loads(json.dumps(plan_summary(pipeline._plan)))
    assert summary[0]['streaming'] == {**policies['prepare'], 'label': 'prepare'}


def test_bound_worker_class_and_none_output_are_regular_field_values(data):
    barrier = threading.Barrier(2, timeout=5)
    identities = []
    class Field:
        def __init__(self, add):
            self.add = add
            self.identity = len(identities)
            identities.append(self)
        def __call__(self, value):
            barrier.wait()
            return {'sum': value + self.add, 'owner': self.identity, 'nullable': None}
    actual = (data.from_items([{'i': i} for i in range(4)])
              .map(Field, fn_constructor_args=[10], inputs={'value': 'i'},
                   outputs={'sum': 'total', 'owner': 'owner', 'nullable': 'null'},
                   concurrency=2, execution='thread', callable_scope='worker')
              .materialize().take_all())
    assert len(identities) == 2 and len({r['owner'] for r in actual}) == 2
    assert all(r['total'] == r['i'] + 10 and r['null'] is None for r in actual)
    assert (data.from_items([{'i': 1}]).map(lambda value: None, inputs={'value': 'i'},
            output='null', execution='thread').materialize().take_all()) == [{'i': 1, 'null': None}]


def test_callable_instance_is_copied_per_action_and_list_results_expand(data):
    class CounterActor:
        def __init__(self):
            self.count = 0
        def __call__(self, row):
            self.count += 1
            return [row, {**row, 'i': self.count + 10}]
    actor = CounterActor()
    pipeline = data.from_items([{'i': 1}]).map(actor, execution='inline')
    for _ in range(2):
        assert pipeline.materialize().take_all() == [{'i': 1}, {'i': 11}]
    assert actor.count == 0


def test_concurrent_map_emits_fast_row_before_blocked_predecessor(data):
    fast_consumed = threading.Event()
    seen = []
    def work(row):
        if row['i'] == 0:
            assert fast_consumed.wait(5), 'stream reordered results behind a slow predecessor'
        return row
    async def consume(row):
        seen.append(row['i'])
        fast_consumed.set()
        return row
    stats = (data.from_items([{'i': 0}, {'i': 1}]).map(work, concurrency=2, execution='thread')
             .map_async(consume, concurrency=1).run_stream())
    assert seen == [1, 0] and stats.emitted == 2


@pytest.mark.parametrize('hook', ['astop', 'aclose'])
def test_cleanup_failure_still_closes_all_workers_and_native_actors(data, hook):
    barrier = threading.Barrier(2, timeout=5)
    events = []
    class Worker:
        def __call__(self, row):
            barrier.wait()
            return row
        async def astop(self):
            events.append('stop')
            if hook == 'astop':
                raise RuntimeError('stop failure')
        async def aclose(self):
            events.append('close')
            if hook == 'aclose':
                raise RuntimeError('close failure')
    class Native:
        concurrency = 1
        async def __call__(self, row):
            return row
        async def aclose(self):
            events.append('native_close')
    with pytest.raises(ExceptionGroup):
        (data.from_items([{'i': 0}, {'i': 1}]).map(Worker, concurrency=2, execution='thread',
               callable_scope='worker').map_async(Native()).materialize())
    assert Counter(events) == {'stop': 2, 'close': 2, 'native_close': 1}
    assert not data._executor._spill_paths


def test_external_cancel_drains_map_calls_before_action_cleanup(data):
    active, finished = threading.Event(), threading.Event()
    events = []
    class Work:
        def __call__(self, row):
            active.set()
            time.sleep(.15)
            finished.set()
            return row
        async def aclose(self):
            assert finished.is_set()
            events.append('closed')
    class CancelAction:
        concurrency = 1
        async def astart(self):
            task = asyncio.current_task()
            async def interrupt():
                await asyncio.to_thread(active.wait, 5)
                task.cancel()
            self.interrupt = asyncio.create_task(interrupt())
        async def __call__(self, row):
            return row
        async def aclose(self):
            await self.interrupt
    with pytest.raises(asyncio.CancelledError):
        (data.from_items([{'i': 1}]).map(Work, execution='thread')
         .map_async(CancelAction()).materialize())
    assert events == ['closed'] and not data._executor._spill_paths


@pytest.mark.parametrize('execution', ['inline', 'thread'])
@pytest.mark.parametrize('count', [1, 1000])
def test_callback_cancellation_never_returns_partial_success(data, execution, count):
    events = []
    class Cancel:
        def __call__(self, row):
            raise asyncio.CancelledError('callback cancelled')
        async def aclose(self):
            events.append('close')
    with pytest.raises(asyncio.CancelledError):
        (data.from_iter(lambda: ({'i': i} for i in range(count)))
         .map(Cancel, execution=execution, catch=(Exception,)).materialize())
    assert events == ['close'] and not data._executor._spill_paths


@pytest.mark.parametrize('failure', ['schema', 'writer'])
def test_lance_checkpoint_writer_failure_joins_stream_and_closes_workers(data, tmp_path, monkeypatch, failure):
    from demiflow.errors import InvalidLanceRequest
    import importlib
    checkpoint = importlib.import_module('demiflow.lance.checkpoint')
    events = []
    class Work:
        def __call__(self, row):
            time.sleep(.001)
            return {'i': row['i'], 'extra': True} if failure == 'schema' else row
        async def aclose(self):
            events.append('close')
    if failure == 'writer':
        class FailedWriter:
            def write_dataset(self, reader, *args, **kwargs):
                reader.read_next_batch()
                raise OSError('storage fixture full')
        monkeypatch.setattr(checkpoint, 'require_lance', lambda: FailedWriter())
    plan = (data.from_iter(lambda: ({'i': i} for i in range(400)))
            .map(Work, concurrency=2, execution='thread', callable_scope='worker'))
    uri = tmp_path / 'failed.lance'
    with pytest.raises(InvalidLanceRequest if failure == 'schema' else OSError):
        plan.checkpoint_lance(str(uri), schema=pa.schema([('i', pa.int64())]),
                              fingerprint='failure', max_rows_per_batch=1)
    assert events and len(events) <= 2
    assert not uri.exists() and checkpoint.read_checkpoint_record(str(uri)) is None
    assert not any(t.name == 'demiflow-lance-checkpoint-bridge' or t.name.startswith('demiflow-stream-')
                   for t in threading.enumerate())
