"""Admitted prompt usage survives failure/cancellation and worker teardown."""
import threading
import pytest

from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.pipeline.local_kernel import LocalKernelOptions
from demiflow.operator_llm.errors import PromptBudgetExceededError, PromptResponseContractError
from demiflow.operator_llm.parser import parse_prompt_pack
from test_map_prompt_async import PACK, server


def suppress_periodic_progress(monkeypatch):
    from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
    threads = []
    def suppress(runner):
        threads.append(threading.get_ident())
    monkeypatch.setattr(WorkerGraphRunner, '_flush_metrics', suppress)
    return threads


@pytest.mark.parametrize('method', ['map_prompt', 'map_prompt_async'])
@pytest.mark.parametrize('exhausted', [False, True])
@pytest.mark.parametrize('action', ['materialize', 'run_stream'])
def test_final_prompt_usage_does_not_depend_on_progress_delivery(server, monkeypatch, method, exhausted, action):
    # No periodic progress can rescue the counters. Use the real worker Drain
    # response after calls finish, including budget failure before HTTP starts.
    progress_threads = suppress_periodic_progress(monkeypatch)
    api = DataAPI(LocalDatasetExecutor(workers=4, block_size=1,
                  local_kernel=LocalKernelOptions(workers=4, worker_mode='thread')))
    kwargs = {'concurrency': 4} if method == 'map_prompt_async' else {}
    ds = getattr(api.from_items([{'item': n} for n in range(8 if exhausted else 2)]), method)(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer',
        max_requests=2, **kwargs)
    if action == 'run_stream' and method == 'map_prompt':
        # run_stream needs an explicit streaming operation; retain the sync
        # prompt upstream while making the public action admissible.
        ds = ds.map_async(lambda row: row, concurrency=1)
    if exhausted:
        with pytest.raises(PromptBudgetExceededError):
            getattr(ds, action)()
    else:
        result = getattr(ds, action)()
        assert (result.emitted if action == 'run_stream' else len(result.take_all())) == 2
    usage = api.prompt_usage()
    assert usage['provider_requests_reserved'] == 2
    assert usage['provider_requests_started'] <= 2
    assert usage['provider_requests_completed'] + usage['provider_requests_failed'] == 2
    assert len(server['requests']) <= 2
    assert progress_threads and threading.get_ident() not in progress_threads
    # Reading an observational projection never applies it twice.
    assert api.prompt_usage() == usage


@pytest.mark.parametrize('method', ['map_prompt', 'map_prompt_async'])
@pytest.mark.parametrize('request_failed', [False, True])
@pytest.mark.parametrize('metadata_failed', [False, True])
def test_final_prompt_usage_survives_client_close_failure(server, monkeypatch, method, request_failed, metadata_failed):
    from demiflow.execution.journal import run_telemetry as dataflow_api
    from demiflow.operator_llm.client import AsyncOperatorLLMClient

    progress_threads = suppress_periodic_progress(monkeypatch)
    closed = []
    close = AsyncOperatorLLMClient.aclose

    async def fail_close(client):
        await close(client)
        closed.append(client)
        raise LookupError('actual prompt client close failure')

    monkeypatch.setattr(AsyncOperatorLLMClient, 'aclose', fail_close)
    if metadata_failed:
        record = dataflow_api.record_execution_metadata
        def fail_metadata(request, execution):
            record(request, execution)
            raise RuntimeError('actual final metadata failure')
        monkeypatch.setattr(dataflow_api, 'record_execution_metadata', fail_metadata)
    if request_failed:
        server['status'] = 503
    # Fault injection must reach the real worker's transport instance; spawned
    # Python processes would import a fresh, unpatched client class.
    api = DataAPI(LocalDatasetExecutor(workers=1, block_size=1,
                  local_kernel=LocalKernelOptions(workers=1, worker_mode='thread')))
    kwargs = {'concurrency': 1} if method == 'map_prompt_async' else {}
    ds = getattr(api.from_items([{'item': 1}]), method)(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer',
        max_requests=1, **kwargs)
    if request_failed:
        # Cleanup must not replace the original provider error or turn its
        # attempted request into a completed request.
        with pytest.raises(PromptResponseContractError, match='HTTP 503; full response saved') as failed:
            ds.materialize()
    else:
        with pytest.raises(ExceptionGroup, match='worker cleanup failed') as failed:
            ds.materialize()
        assert any(isinstance(error, LookupError) and str(error) == 'actual prompt client close failure'
                   for error in failed.value.exceptions)
    assert len(closed) == 1
    assert len(server['requests']) == 1
    assert progress_threads and threading.get_ident() not in progress_threads
    if request_failed:
        assert any('worker cleanup failed' in note for note in failed.value.__notes__)
    if metadata_failed:
        assert any('actual final metadata failure' in note for note in failed.value.__notes__)
    usage = api.prompt_usage()
    assert usage['provider_requests_reserved'] == 1
    assert usage['provider_requests_started'] == 1
    assert usage['provider_requests_completed'] == int(not request_failed)
    assert usage['provider_requests_failed'] == int(request_failed)
    assert api.prompt_usage() == usage


@pytest.mark.parametrize('handling_unrelated_error', [False, True])
@pytest.mark.parametrize('action', ['materialize', 'run_stream'])
def test_final_metadata_failure_is_not_suppressed_after_success(server, monkeypatch, handling_unrelated_error, action):
    from demiflow.execution.journal import run_telemetry as dataflow_api
    record = dataflow_api.record_execution_metadata
    def fail_metadata(request, execution):
        record(request, execution)
        raise RuntimeError('actual final metadata failure')
    monkeypatch.setattr(dataflow_api, 'record_execution_metadata', fail_metadata)
    api = DataAPI(workers=1, block_size=1)
    ds = api.from_items([{'item': 1}]).map_prompt(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer', max_requests=1)
    if action == 'run_stream':
        ds = ds.map_async(lambda row: row, concurrency=1)
    def execute():
        with pytest.raises(RuntimeError, match='actual final metadata failure'):
            getattr(ds, action)()
    if handling_unrelated_error:
        try:
            raise ValueError('unrelated caller error already being handled')
        except ValueError:
            execute()
    else:
        execute()
    assert len(server['requests']) == 1
    assert api.prompt_usage()['provider_requests_completed'] == 1
