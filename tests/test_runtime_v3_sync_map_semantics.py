"""Sync map, deferred factory construction, and worker cancellation are public semantics."""
import asyncio
import os

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from _worker_event_fixtures import emit, events


class NeverConstruct:
    def __init__(self):
        raise AssertionError('empty stage constructed an actor')
    def __call__(self, row):
        return row


class ObservedSyncActor:
    def __init__(self, path):
        self.path = str(path)
        self.count = 0
        emit(self.path, 'construct')
    async def astart(self):
        emit(self.path, 'start')
    def __call__(self, row):
        self.count += 1
        emit(self.path, 'call')
        return {**row, 'count': self.count}
    def snapshot_state(self):
        return {'count': self.count}
    def restore_state(self, state):
        self.count = state['count']
    async def astop(self):
        emit(self.path, 'stop')
    async def aclose(self):
        emit(self.path, 'close')


@pytest.mark.parametrize('rows', [[], [{'i': 1}]])
@pytest.mark.parametrize('scope', ['stage', 'worker'])
def test_empty_and_filtered_class_never_constructs(tmp_path, rows, scope):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        actual = data.from_items(rows).filter(lambda row: False).map(NeverConstruct,
            concurrency=3, callable_scope=scope, execution='thread').take_all()
        assert actual == []
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


def test_worker_factory_constructs_only_invoked_slot(tmp_path):
    path = tmp_path / 'events'
    with data.local_execution():
        actual = data.from_items([{'i': 1}]).map(ObservedSyncActor,
            fn_constructor_args=[str(path)], callable_scope='worker', execution='thread', concurrency=3).take_all()
    assert actual == [{'i': 1, 'count': 1}]
    observed = events(path)
    assert [item['event'] for item in observed] == ['construct', 'start', 'call', 'stop', 'close']
    assert all(item['pid'] != os.getpid() for item in observed)


def test_existing_instance_empty_action_still_starts_and_closes(tmp_path):
    path = tmp_path / 'events'
    actor = ObservedSyncActor(path)
    with data.local_execution():
        assert data.from_items([]).map(actor).take_all() == []
    observed = events(path)
    assert [item['event'] for item in observed] == ['construct', 'start', 'stop', 'close']
    assert observed[0]['pid'] == os.getpid()
    assert all(item['pid'] != os.getpid() for item in observed[1:])


@pytest.mark.parametrize('execution', [None, 'inline', 'thread'])
@pytest.mark.parametrize('bound', [False, True])
def test_sync_map_hidden_awaitable_is_rejected_without_running_it(tmp_path, execution, bound):
    path = tmp_path / 'async-called'
    async def hidden(value):
        emit(path, 'awaited')
        return value
    options = {} if execution is None else {'execution': execution}
    if bound:
        options.update(inputs={'value': 'i'}, output='answer')
    with pytest.raises(TypeError, match='awaitable'):
        with data.local_execution():
            data.from_items([{'i': 1}]).map(lambda value: hidden(value), **options).take_all()
    assert not path.exists()


@pytest.mark.parametrize('execution', ['inline', 'thread'])
@pytest.mark.parametrize('async_callback', [False, True])
def test_callback_cancel_preserves_exception_closes_actor_and_does_not_commit(tmp_path, execution, async_callback):
    path = tmp_path / 'events'
    async def cancel_async():
        raise asyncio.CancelledError('callback cancelled')
    class Cancel:
        def __call__(self, row):
            if async_callback:
                return cancel_async()
            raise asyncio.CancelledError('callback cancelled')
        async def aclose(self):
            emit(path, 'close')
    with pytest.raises(asyncio.CancelledError, match='callback cancelled'):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            ds = data.from_items([{'i': 1}])
            if async_callback:
                ds.map_async(Cancel(), execution=execution, catch=(Exception,)).take_all()
            else:
                ds.map(Cancel, execution=execution, catch=(Exception,)).take_all()
    assert [item['event'] for item in events(path)] == ['close']
    assert not run.journal.execution_commits('action-1')


@pytest.mark.parametrize('scope', ['stage', 'worker'])
@pytest.mark.parametrize('called', [False, True])
def test_factory_checkpoint_restores_only_previously_constructed_slots(tmp_path, scope, called):
    # Component boundary: use the real kernel, DTOs and state codec; the store
    # double is confined to image bytes. Public worker/journal behavior is above.
    from test_runtime_v3_k import services, spec, program, InputPort, rec, rows_of
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.kernels.operator import OperatorKernel
    path = tmp_path / 'events'
    declaration = CallableSpec.create(ObservedSyncActor, fn_constructor_args=[str(path)])
    svc = services(programs={'fn': program(declaration)})
    policy = spec('MapOp', {'program': 'fn', 'streaming_map': True, 'callable_scope': scope,
        'concurrency': 3 if scope == 'worker' else 1},
        inputs=[InputPort('input', 'incremental')], state='adapter')
    before = OperatorKernel(policy, services=svc, partition_id='p0')
    before.start()
    assert not events(path)
    if called:
        rows = list(before.push('input', rec({'i': 1}))) + list(before.finish('input'))
        assert rows_of(rows) == [{'i': 1, 'count': 1}]
    image = before.snapshot()
    before.close()
    restored = OperatorKernel(policy, services=svc, partition_id='p0')
    restored.restore(image)
    restored.start()
    assert sum(instance is not None for instance in restored._instances) == int(called)
    if called:
        assert restored._state_owner(restored._instances[0]).count == 1
    else:
        assert not events(path)
    restored.close()
    names = [item['event'] for item in events(path)]
    assert names.count('construct') == (2 if called else 0)
    assert names.count('close') == (2 if called else 0)


def test_cancel_wire_mapping_is_closed_and_preserves_cancellation_type():
    from demiflow.execution.dataflow_failures import failure_payload, restore_failure
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    error = restore_failure(failure_payload(asyncio.CancelledError('caller stopped')))
    assert type(error) is asyncio.CancelledError and str(error) == 'caller stopped'
    unknown = restore_failure({'error_type': 'arbitrary.module.ExecutableError', 'message': 'unknown'})
    assert type(unknown) is DataflowError
