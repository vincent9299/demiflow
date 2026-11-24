"""Ordinary callback tasks are logical batches, independent of physical placement."""
import time

import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from _worker_event_fixtures import emit, events


class Number:
    def __init__(self, path=None):
        self.count = 0
        self.path = path
        if path:
            emit(path, 'construct')
    def __call__(self, row):
        self.count += 1
        if self.path:
            emit(self.path, 'call', row=row['i'], count=self.count)
        return {**row, 'within_task': self.count}
    def close(self):
        if self.path:
            emit(self.path, 'close', count=self.count)


def closure(path=None):
    state = [0]
    def number(row):
        state[0] += 1
        if path:
            emit(path, 'call', row=row['i'], count=state[0])
        return {**row, 'within_task': state[0]}
    return number


@pytest.mark.parametrize('form', ['class', 'closure', 'method'])
def test_public_task_batch_rebuilds_every_callable_shape(tmp_path, form):
    path = str(tmp_path / 'calls')
    callback = {'class': Number, 'closure': closure(path), 'method': Number(path).__call__}[form]
    # Initial driver instance construction is not a worker invocation.
    before = len(events(path))
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(workers=2, batch_rows=3):
        options = {'fn_constructor_args': [path]} if form == 'class' else {}
        actual = data.from_items([{'i': i} for i in range(7)]).map(callback, **options).take_all()
        assert [row['within_task'] for row in actual] == [1, 2, 3, 1, 2, 3, 1]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    observed = events(path)[before:]
    assert [item['row'] for item in observed if item['event'] == 'call'] == list(range(7))
    if form == 'class':
        # Two live-worker slots can park this three-stage pipeline. Resource
        # reconstruction is allowed; committed business prefixes never replay.
        owners = {}
        for item in observed:
            active = owners.get(item['pid'], False)
            if item['event'] == 'construct':
                assert not active
                owners[item['pid']] = True
            elif item['event'] == 'close':
                assert active
                owners[item['pid']] = False
            else:
                assert active and item['count'] == item['row'] % 3 + 1
        assert not any(owners.values())
        assert sum(item['event'] == 'construct' for item in observed) == sum(
            item['event'] == 'close' for item in observed) >= 3
    if form == 'method':
        assert callback.__self__.count == 0


@pytest.mark.parametrize('scope', ['stage', 'worker'])
def test_explicit_streaming_scope_does_not_become_task_scope(scope):
    with data.local_execution(batch_rows=3):
        result = data.from_items([{'i': i} for i in range(7)]).map(Number,
            execution='thread', callable_scope=scope, concurrency=1).take_all()
    assert [row['within_task'] for row in result] == list(range(1, 8))


@pytest.mark.parametrize('form', ['class', 'closure', 'method'])
@pytest.mark.parametrize('prefix', [2, 3])
def test_original_kernel_image_restores_partial_task_or_next_boundary(form, prefix):
    # A store-boundary component test: real DTO/kernel/state codec, no fake
    # worker/journal claim. The public driver recovery is tested separately.
    from test_runtime_v3_k import services, spec, program, InputPort, rec, rows_of
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.kernels.operator import OperatorKernel
    callback = {'class': Number, 'closure': closure(), 'method': Number().__call__}[form]
    svc = services(programs={'fn': program(CallableSpec.create(callback))})
    policy = spec('MapOp', {'program': 'fn', 'task_batch_rows': 3},
        inputs=[InputPort('input', 'incremental')], state='adapter')
    first = OperatorKernel(policy, services=svc, partition_id='p0')
    first.start()
    result = []
    for i in range(prefix):
        result.extend(rows_of(first.push('input', rec({'i': i}, sequence=i))))
    image = first.snapshot()
    first.close()
    second = OperatorKernel(policy, services=svc, partition_id='p0')
    second.restore(image)
    second.start()
    if prefix == 3:
        assert second._instance is None
    for i in range(prefix, 7):
        result.extend(rows_of(second.push('input', rec({'i': i}, sequence=i))))
    second.close()
    assert [row['within_task'] for row in result] == [1, 2, 3, 1, 2, 3, 1]


def test_task_state_rejects_aliases_mutable_globals_and_invalid_frontier():
    from demiflow.execution.kernels.task_scope import snapshot_function
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    shared = []
    state = [shared, shared]
    def aliased(row):
        state[0].append(1)
        return row
    assert snapshot_function(aliased, max_bytes=4096)['mode'] == 'nonrecoverable'
    namespace = {'counter': []}
    exec('def external(row):\n counter.append(1)\n return row', namespace)
    assert snapshot_function(namespace['external'], max_bytes=4096)['mode'] == 'nonrecoverable'
    from test_runtime_v3_k import services, spec, program, InputPort
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.kernels.operator import OperatorKernel, IMAGE_MEDIA_TYPE
    from demiflow.execution.contracts import state_codec
    svc = services(programs={'fn': program(CallableSpec.create(Number))})
    policy = spec('MapOp', {'program': 'fn', 'task_batch_rows': 3},
        inputs=[InputPort('input', 'incremental')], state='auto_json')
    before = OperatorKernel(policy, services=svc, partition_id='p0')
    before.start()
    ref = before.snapshot()
    raw = b''.join(svc.store.read(ref, max_bytes=ref.size_bytes))
    image = state_codec.loads(raw, max_bytes=svc.bounds.max_state_bytes)
    before.close()
    image['task']['input_frontier'] = -1
    raw = state_codec.dumps(image, max_bytes=svc.bounds.max_state_bytes)
    broken = svc.store.put(iter([raw]), max_bytes=len(raw), media_type=IMAGE_MEDIA_TYPE)
    after = OperatorKernel(policy, services=svc, partition_id='p0')
    after.restore(broken)
    with pytest.raises(DataflowError, match='task frontier'):
        after.start()


def test_filtered_and_expanded_chains_keep_original_task_boundaries():
    with data.local_execution(batch_rows=3):
        source = data.from_items([{'i': i} for i in range(7)])
        filtered = source.filter(lambda row: row['i'] % 2 == 0).select_columns(['i']).map(Number)
        assert [row['within_task'] for row in filtered.take_all()] == [1, 2, 1, 1]
        expanded = source.flat_map(lambda row: [row, row]).rename_columns({'i': 'value'}).map(
            lambda row: {'i': row['value']}).map(Number)
        assert [row['within_task'] for row in expanded.take_all()] == [1,2,3,4,5,6,1,2,3,4,5,6,1,2]
        # Limit deliberately ends the old task group and creates a new batch.
        limited = source.filter(lambda row: row['i'] % 2 == 0).limit(4).map(Number)
        assert [row['within_task'] for row in limited.take_all()] == [1, 2, 3, 1]


@pytest.mark.parametrize('form', ['class', 'closure'])
def test_actual_driver_resume_preserves_partial_task_and_committed_prefix(tmp_path, monkeypatch, form):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.contracts import state_codec
    class Interrupted(Exception):
        pass
    path = str(tmp_path / 'calls')
    class SlowNumber(Number):
        def __call__(self, row):
            time.sleep(.02)
            return super().__call__(row)
    if form == 'closure':
        state = [0]
        def callback(row):
            time.sleep(.02)
            state[0] += 1
            emit(path, 'call', row=row['i'], count=state[0])
            return {**row, 'within_task': state[0]}
        options = {}
    else:
        callback = SlowNumber
        options = {'fn_constructor_args': [path]}
    def build():
        return data.from_items([{'i': i} for i in range(23)]).map(callback, **options)
    original = DataflowCheckpointCoordinator.commit
    cut = {}
    def interrupt(coordinator, cut_id, *, sinks, **forwarded):
        # The driver also passes native_queue_state; forward whatever the real commit is given.
        image = original(coordinator, cut_id, sinks=sinks, **forwarded)
        if not image.cut.final and not cut:
            for contribution in image.contributions:
                for ref in contribution.operator_states.values():
                    if ref.media_type != 'application/x-demiflow-kernel-image-v3':
                        continue
                    raw = b''.join(coordinator._store.read(ref, max_bytes=ref.size_bytes))
                    state = state_codec.loads(raw, max_bytes=coordinator._physical.bounds.max_state_bytes)
                    task = state.get('task')
                    if task and task['input_frontier'] % 5 and task['input_frontier'] > 0:
                        cut.update(frontier=task['input_frontier'], commit=image.commit_id)
                        raise Interrupted('after native ACK')
        return image
    with monkeypatch.context() as patch:
        patch.setattr(DataflowCheckpointCoordinator, 'commit', interrupt)
        with pytest.raises(Interrupted, match='native ACK'):
            with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 1,
                    'checkpoint_interval_ms': 1, 'checkpoint_unconfirmed_rows': 2}) as run, data.local_execution(workers=4, batch_rows=5):
                ref = run.ref
                build().take_all()
    assert 0 < cut['frontier'] < 23
    before = events(path)
    committed_calls = [event for event in before if event['event'] == 'call' and event['row'] < cut['frontier']]
    assert [event['row'] for event in committed_calls] == list(range(cut['frontier']))
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.run_journal import RunJournal
    journal = RunJournal(ref.directory)
    commits_before = journal.execution_commits('action-1')
    started = time.monotonic()
    with pytest.raises(DataflowError, match='ready cohort exceeds admitted host or resident capacity') as rejected:
        with RunContext.resume(ref), data.local_execution(workers=2, batch_rows=999):
            # This seal has a 3-worker memory cohort. A tighter live envelope
            # cannot silently replace its exchanges or start a partial cohort.
            build().take_all()
    assert rejected.value.code == 'RESOURCE_INFEASIBLE'
    assert time.monotonic() - started < 30
    assert events(path) == before
    assert journal.execution_commits('action-1') == commits_before
    with RunContext.resume(ref) as resumed, data.local_execution(workers=4, batch_rows=999):
        # Feasible resources still use the original sealed batch size, never
        # today's executor default or a replay of the committed task prefix.
        rows = build().take_all()
        assert [row['within_task'] for row in rows] == [i % 5 + 1 for i in range(23)]
        assert any(fact['commit_id'] == cut['commit'] for fact in resumed.journal.execution_commits('action-1'))
    after = [event for event in events(path) if event['event'] == 'call' and event['row'] < cut['frontier']]
    assert after == committed_calls


def test_task_function_state_budget_and_rebound_dependency_fail_closed():
    from demiflow.execution.kernels.task_scope import snapshot_function, restore_function
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    value = b'initial'
    def replace_bytes(row):
        nonlocal value
        value += b'next'
        return row
    assert snapshot_function(replace_bytes, max_bytes=4096)['mode'] == 'nonrecoverable'
    function = closure()
    image = snapshot_function(function, max_bytes=4096)
    image['value']['cell:state'] = ['x' * 5000]
    with pytest.raises(DataflowError, match='state'):
        restore_function(function, image, max_bytes=4096)
    assert function({'i': 0})['within_task'] == 1


def test_task_provenance_is_separate_and_cannot_be_forged_at_a_follower():
    from test_runtime_v3_k import services, spec, program, InputPort, rec
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.kernels.operator import OperatorKernel
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    svc = services(programs={'fn': program(CallableSpec.create(Number))})
    policy = spec('MapOp', {'program': 'fn', 'task_batch_rows': 3, 'task_group': 'group-1', 'task_origin': False},
        inputs=[InputPort('input', 'incremental')], state='auto_json')
    follower = OperatorKernel(policy, services=svc, partition_id='p0')
    follower.start()
    for position in [{'index': 8}, {'index': 8, 'dataflow_task': {'group': 'wrong', 'ordinal': 0}},
                     {'index': 8, 'dataflow_task': {'group': 'group-1', 'ordinal': -1}}]:
        with pytest.raises(DataflowError, match='provenance'):
            list(follower.push('input', rec({'i': 0}, position=position)))
    record = list(follower.push('input', rec({'i': 0}, sequence=8,
        position={'index': 8, 'source': 'original', 'dataflow_task': {'group': 'group-1', 'ordinal': 2}})))[0]
    assert record.source_position['index'] == 8 and record.source_position['source'] == 'original'
    with pytest.raises(DataflowError, match='out of order'):
        list(follower.push('input', rec({'i': 1}, position={'dataflow_task': {'group': 'group-1', 'ordinal': 1}})))
    follower.close()


@pytest.mark.parametrize("form", ["function", "class"])
@pytest.mark.parametrize("measured", [False, True])
def test_private_task_rejects_oversize_mutable_checkpoint(form, measured):
    from dataclasses import replace
    from test_runtime_v3_k import services, spec, program, InputPort, rec
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.kernels.operator import OperatorKernel
    from demiflow.execution.contracts.dataflow_errors import DataflowError

    owned = []
    def grow(row):
        owned.append('x' * row['n'])
        return row

    class Grow:
        def __init__(self):
            self.value = ''

        def __call__(self, row):
            self.value = 'x' * row['n']
            return row

    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    target = grow if form == "function" else Grow
    svc = services(programs={'fn': program(CallableSpec.create(target))})
    svc.bounds = replace(svc.bounds, max_state_bytes=4096)
    options = {'program': 'fn', 'task_batch_rows': 2}
    if measured:
        options['codec_min_bytes'] = svc.bounds.max_resident_bytes
        svc.measured_workspace = True
    policy = spec('MapOp', options, state='adapter', inputs=[InputPort('input', 'incremental')])
    kernel = OperatorKernel(policy, services=svc, partition_id='p0')
    try:
        kernel.start()
        list(kernel.push('input', rec({'n': 16384})))
        error_type = DataflowError if form == 'function' else RecoveryContractError
        with pytest.raises(error_type, match='before encoding') as failed:
            kernel.snapshot()
        assert 'limit_bytes=4096' in str(failed.value) and 'occurrence_id=occ-MapOp' in str(failed.value)
        assert 'visited_python_bytes=' in str(failed.value) and 'actual_bytes' not in str(failed.value)
    finally:
        kernel.close()
    assert not owned


def test_private_task_reopens_original_whole_function_image(monkeypatch):
    from test_runtime_v3_k import services, spec, program, InputPort, rec, rows_of
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.kernels.operator import OperatorKernel
    from demiflow.execution.kernels.task_scope import snapshot_function

    owner = closure()
    svc = services(programs={'fn': program(CallableSpec.create(owner))})
    policy = spec('MapOp', {'program': 'fn', 'task_batch_rows': 3}, state='adapter',
                  inputs=[InputPort('input', 'incremental')])
    first = OperatorKernel(policy, services=svc, partition_id='p0')
    first.start()
    try:
        assert rows_of(first.push('input', rec({'i': 0}))) == [{'i': 0, 'within_task': 1}]
        # Exercise the original writer's image shape through the real reader.
        monkeypatch.setattr(first, '_snapshot_state', lambda instance=None: snapshot_function(
            first._state_owner(instance), max_bytes=svc.bounds.max_state_bytes))
        ref = first.snapshot()
    finally:
        first.close()
    second = OperatorKernel(policy, services=svc, partition_id='p0')
    try:
        second.restore(ref)
        second.start()
        assert rows_of(second.push('input', rec({'i': 1}, sequence=1))) == [{'i': 1, 'within_task': 2}]
    finally:
        second.close()

def test_task_checkpoint_reports_measured_bytes_and_owner():
    from dataclasses import replace
    from test_runtime_v3_k import services, spec, program, InputPort, rec
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.kernels.operator import OperatorKernel
    from demiflow.execution.contracts.dataflow_errors import DataflowError

    svc = services(programs={'fn': program(CallableSpec.create(closure()))})
    policy = spec('MapOp', {'program': 'fn', 'task_batch_rows': 3}, state='adapter',
                  inputs=[InputPort('input', 'incremental')])
    kernel = OperatorKernel(policy, services=svc, partition_id='p0')
    try:
        kernel.start()
        list(kernel.push('input', rec({'i': 0})))
        valid = kernel.snapshot()
        limit = valid.size_bytes - 1
        svc.bounds = replace(svc.bounds, max_state_bytes=limit)
        for operation in (kernel.snapshot, lambda: kernel.restore(valid)):
            with pytest.raises(DataflowError, match='actual_bytes=') as error:
                operation()
            assert error.value.context == {'limit_bytes': limit, 'actual_bytes': valid.size_bytes,
                                           'occurrence_id': 'occ-MapOp'}
    finally:
        kernel.close()


@pytest.mark.parametrize('form', ['class', 'function'])
def test_adaptive_task_checkpoint_restores_real_state_with_small_worker_grant(form):
    from dataclasses import replace
    from test_runtime_v3_k import services, spec, program, InputPort, rec, rows_of
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.kernels.operator import OperatorKernel

    class Counter:
        def __init__(self):
            self.count = 0
        def __call__(self, row):
            self.count += 1
            return {**row, 'count': self.count}

    count = [0]
    def counter(row):
        count[0] += 1
        return {**row, 'count': count[0]}

    target = Counter if form == 'class' else counter
    svc = services(programs={'fn': program(CallableSpec.create(target))})
    grant = 256 * 1024
    svc.bounds = replace(svc.bounds, max_resident_bytes=grant)
    svc.measured_workspace = True
    policy = spec('MapOp', {'program': 'fn', 'task_batch_rows': 3, 'codec_min_bytes': grant},
                  inputs=[InputPort('input', 'incremental')], state='adapter').with_memory_bytes(grant)
    first = OperatorKernel(policy, services=svc, partition_id='p0')
    first.start()
    assert rows_of(first.push('input', rec({'i': 0})))[0]['count'] == 1
    image = first.snapshot()
    first.close()
    second = OperatorKernel(policy, services=svc, partition_id='p0')
    second.restore(image)
    second.start()
    assert rows_of(second.push('input', rec({'i': 1}, sequence=1)))[0]['count'] == 2
    second.close()


def test_adaptive_task_rejects_growth_before_ack_of_filtered_row():
    from dataclasses import replace
    from test_runtime_v3_k import services, spec, program, InputPort, rec
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.kernels.operator import OperatorKernel

    class Grow:
        def __call__(self, row):
            self.value = 'x' * row['size']
            return False

    svc = services(programs={'fn': program(CallableSpec.create(Grow))})
    grant = 32 * 1024
    svc.bounds = replace(svc.bounds, max_resident_bytes=grant)
    svc.measured_workspace = True
    policy = spec('FilterOp', {'program': 'fn', 'task_batch_rows': 4, 'codec_min_bytes': grant},
                  inputs=[InputPort('input', 'incremental')], state='adapter').with_memory_bytes(grant)
    worker = OperatorKernel(policy, services=svc, partition_id='p0')
    worker.start()
    with pytest.raises(DataflowError) as failure:
        list(worker.push('input', rec({'size': grant + 1})))
    assert failure.value.code == 'RESOURCE_INFEASIBLE'
    assert failure.value.context['granted_bytes'] == grant
    assert failure.value.context['occurrence_id'] == 'occ-FilterOp'
    assert failure.value.context['required_bytes'] > grant
    worker.close()


def test_task_checkpoint_rejects_before_building_tagged_container_tree():
    import tracemalloc
    from demiflow.execution.contracts import state_codec

    values = list(range(10000))
    grant = 1 << 20

    class TooLarge(Exception):
        pass

    peak_at_admission = []
    def admit(required):
        peak_at_admission.append(tracemalloc.get_traced_memory()[1])
        assert required > grant
        raise TooLarge

    # Imports and the immutable input predate the working grant.
    from demiflow.execution.contracts.record_codec import json_encoding_shape
    json_encoding_shape(0)
    tracemalloc.start()
    try:
        with pytest.raises(TooLarge):
            state_codec.dumps(values, max_bytes=16 * grant, admit=admit)
    finally:
        tracemalloc.stop()
    assert len(peak_at_admission) == 1 and peak_at_admission[0] < grant
