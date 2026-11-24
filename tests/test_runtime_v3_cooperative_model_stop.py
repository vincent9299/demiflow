"""Cooperative effect drain uses original admission and lifecycle authorities."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import threading
from types import SimpleNamespace

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_codec import _thaw_json
from demiflow.execution.contracts.dataflow_codec import encode_artifact_reference as _ref_to_json
from demiflow.execution.contracts.dataflow_failures import failure_payload, restore_failure
from demiflow.execution.operator_bindings.dataflow_model_journal import _rpc_operation_id
from demiflow.execution.kernels.async_instance import AsyncInstance
from demiflow.services.request_limits import COOPERATIVE_STOP_REASON, COOPERATIVE_RUN_STOP_REASON, ServiceStopped, drain_on_service_stop
from test_runtime_v3_private_embedding_journal import (
    service, interrupted_run, private_graph, private_event, bind_private_embedding_contracts, charged)
from test_runtime_v3_native_rpc_replay import event
from test_embeddings import server


@pytest.mark.parametrize('reason,settled', [(COOPERATIVE_STOP_REASON, True), (COOPERATIVE_RUN_STOP_REASON, True), ('cancel', False),
                                          ('cooperative:other', False), (None, False)])
def test_async_owner_drains_only_fixed_service_reason(reason, settled):
    entered, finished, cancelled = threading.Event(), threading.Event(), threading.Event()
    async def exchange():
        entered.set()
        try:
            await asyncio.sleep(.15)
            finished.set()
        except asyncio.CancelledError:
            cancelled.set()
            raise
    async def invoke(_):
        await drain_on_service_stop(exchange(), enabled=True)
    services = SimpleNamespace(bounds=SimpleNamespace(timeout_ms=1000), check_stop=lambda: None)
    instance = AsyncInstance(invoke, services)
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(instance, None)
        assert entered.wait(2)
        instance.request_stop(reason)
        with pytest.raises((DataflowError, asyncio.CancelledError)):
            future.result(2)
        instance.close()
    assert finished.is_set() == settled
    assert cancelled.is_set() != settled
    assert not instance.thread.is_alive()


@pytest.mark.parametrize('private', [False, True])
@pytest.mark.parametrize('reason', [COOPERATIVE_STOP_REASON, COOPERATIVE_RUN_STOP_REASON])
def test_stop_closes_fresh_driver_reserve_but_allows_original_receipt_and_response(tmp_path, private, reason):
    from demiflow.execution.journal.dataflow_authority import bind_private_storage
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority, store, _):
        if private:
            authority._action.declaration['kind'] = 'take_all'
            bind_private_embedding_contracts(authority, private_graph(tmp_path/'calls.sqlite'))
            bind_private_storage(authority)
        make = private_event if private else event
        request = make(run, store, 'a')
        original = authority.dispatch(request)
        assert original.status == 'ok'
        authority.stop_model_admission(reason)
        assert authority.dispatch(request) == original  # Lost original ACK is not a second HTTP lease.
        with pytest.raises(ServiceStopped, match='operator_stop_file|run_cancelled'):
            authority.dispatch(make(run, store, 'b'))
        assert charged(run) == 1
        value = b'{"output":"confirmed"}'
        ref = store.put(iter([value]), max_bytes=len(value), media_type='application/json')
        payload = _thaw_json(request.payload)
        payload.update(operation='response', reservation=None, value_ref=_ref_to_json(ref))
        if private:
            response = private_event(run, store, operation='response', value_ref=ref)
            payload = _thaw_json(response.payload)
        else:
            payload['operation_id'] = _rpc_operation_id('action-1', 'node-1', payload['request_key'], 'response')
        response = replace(request, operation_id=payload['operation_id'], payload=payload)
        assert authority.dispatch(response).status == 'ok'
        assert charged(run) == 1


def test_service_stop_exception_remains_fixed_known_type():
    value = restore_failure(failure_payload(ServiceStopped('operator_stop_file')))
    assert type(value) is ServiceStopped and str(value) == 'operator_stop_file'


@pytest.mark.parametrize('mode', ['process', 'thread'])
@pytest.mark.parametrize('private', [False, True])
def test_real_native_and_private_admitted_responses_survive_cooperative_stop(tmp_path, server, mode, private):
    import sqlite3
    from demiflow import data
    from demiflow.execution.pipeline.run_context import RunContext
    stop = tmp_path/'STOP'
    server['delay'] = .2
    def on_request():
        if len(server['bodies']) == 2:
            stop.touch()
    server['on_request'] = on_request
    path = tmp_path/'private.sqlite'
    options = {'sqlite_journal': {'path': str(path)}} if private else {}
    with pytest.raises(ServiceStopped, match='operator_stop_file'):
        with RunContext.start(tmp_path/'run', limits={'max_requests': 2}) as run, \
                data.local_execution(workers=4, worker_mode=mode):
            data.from_items([{'text': str(i)} for i in range(10)]).map_embeddings(
                model=server['model'], inputs={'text': 'text'}, batch_size=1, concurrency=2,
                options=options).run_stream(stop_file=stop)
    assert len(server['bodies']) == 2
    if private:
        connection = sqlite3.connect(path)
    else:
        from demiflow.execution.journal.run_journal import RunJournal
        context = RunJournal(tmp_path/'run')._connection()
        connection = context.__enter__()
    try:
        assert connection.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 2
        assert connection.execute('SELECT count(*) FROM calls WHERE error_json IS NOT NULL').fetchone()[0] == 0
    finally:
        if private:
            connection.close()
        else:
            context.__exit__(None, None, None)


def test_cooperative_drain_remains_bounded_by_original_instance_deadline():
    entered, ended = threading.Event(), threading.Event()
    async def exchange():
        entered.set()
        try:
            await asyncio.sleep(10)
        finally:
            ended.set()
    async def invoke(_):
        await drain_on_service_stop(exchange(), enabled=True)
    instance = AsyncInstance(invoke, SimpleNamespace(bounds=SimpleNamespace(timeout_ms=100), check_stop=lambda: None))
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(instance, None)
        assert entered.wait(1)
        instance.request_stop(COOPERATIVE_STOP_REASON)
        with pytest.raises((DataflowError, asyncio.CancelledError)):
            future.result(1)
        with pytest.raises(DataflowError, match='DEADLINE_EXCEEDED'):
            instance.close()
        assert ended.wait(1)
        instance.close()
    assert not instance.thread.is_alive()


def test_native_paid_stop_without_cut_cannot_invent_recovery(tmp_path, server):
    from demiflow import data
    from demiflow.execution.pipeline.run_context import RunContext
    from demiflow.execution.journal.run_journal import RunJournal
    stop = tmp_path/'STOP'
    server['delay'] = .2
    def on_request():
        if len(server['bodies']) == 2:
            stop.touch()
    server['on_request'] = on_request
    def flow():
        return data.from_items([{'text': str(i)} for i in range(10)]).map_embeddings(
            model=server['model'], inputs={'text': 'text'}, batch_size=1, concurrency=2)
    with pytest.raises(ServiceStopped, match='operator_stop_file'):
        with RunContext.start(tmp_path/'run', limits={'max_requests': 2}) as run:
            reference = run.ref
            flow().run_stream(stop_file=stop)
    assert len(server['bodies']) == 2
    assert not RunJournal(tmp_path/'run').execution_commits('action-1')
    stop.unlink()
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    with pytest.raises(RecoveryContractError, match='P3 native sink adapter'):
        with RunContext.resume(reference):
            flow().run_stream(stop_file=stop)
    assert len(server['bodies']) == 2
    with RunJournal(tmp_path/'run')._connection() as db:
        assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 2
        assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 2


def test_handle_requires_original_run_cancel_fact_to_drain(tmp_path):
    from demiflow.execution.pipeline.run_context import RunContext
    from demiflow.execution.outputs.dataflow_handles import RuntimeActionHandle
    class EndInspection(Exception):pass
    with pytest.raises(EndInspection):
        with RunContext.start(tmp_path/'run') as run:
            reasons=[]
            def handle():
                execution=SimpleNamespace(request=SimpleNamespace(action=SimpleNamespace(context=run)),
                    subscribe_terminal=lambda:SimpleNamespace(close=lambda:None),
                    cancel=reasons.append,close=lambda:None)
                return RuntimeActionHandle(execution,bounds=SimpleNamespace(timeout_ms=1000))
            handle().close()
            assert reasons==['terminal handle closed before completion']
            deadline=handle()
            with pytest.raises(DataflowError,match='time budget'):
                deadline._raise_deadline()
            assert reasons[-1]=='terminal deadline exceeded'
            deadline.close()
            RunContext.cancel(run.ref)
            assert run.journal.execution_status()['status']=='stopping'
            handle().close()
            assert reasons[-1]==COOPERATIVE_RUN_STOP_REASON
            raise EndInspection()


@pytest.mark.parametrize('reason', [COOPERATIVE_RUN_STOP_REASON, COOPERATIVE_STOP_REASON,
                                    'terminal handle closed before completion', 'terminal deadline exceeded'])
def test_original_worker_cancel_reason_survives_cleanup_drain(reason):
    from demiflow.execution.worker.dataflow_execution import DataflowWorkerExecutor
    from demiflow.services.request_limits import cooperative_stop_error
    services=SimpleNamespace(bounds=SimpleNamespace(timeout_ms=1000,max_pending_events=8,
        max_row_bytes=1024,max_batch_bytes=4096),check_stop=lambda:None)
    executor=DataflowWorkerExecutor(services=services,mailbox=SimpleNamespace())
    instance=AsyncInstance(lambda value:value,services)
    executor._kernels={'owner':instance}
    executor.cancel(reason,deadline_ms=1000)
    executor.cancel('execution_close',deadline_ms=1000)
    executor.cancel(COOPERATIVE_RUN_STOP_REASON,deadline_ms=1000)
    assert executor._cancel_reason==reason
    expected=cooperative_stop_error(reason)
    actual=instance._cancellation_reason()
    assert (None if actual is None else str(actual))==(None if expected is None else str(expected))
    instance.close()
