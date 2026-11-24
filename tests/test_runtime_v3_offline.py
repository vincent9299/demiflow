"""Real Dataset offline transport and observed worker usage, with no paid service."""
import pytest

from demiflow.data.api import DataAPI
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.operator_llm.parser import parse_prompt_pack
from test_runtime_v3_native_execution import PACK, native_server


@pytest.mark.parametrize('storage', ['offline_dir', 'offline_store'])
def test_offline_worker_uses_driver_store_and_never_reserves_provider(tmp_path, storage, monkeypatch):
    import sqlite3
    import os
    original_connect = sqlite3.connect
    driver_pid = os.getpid()
    opened = []
    def record_connect(*args, **kwargs):
        assert os.getpid() == driver_pid
        opened.append(str(args[0]))
        return original_connect(*args, **kwargs)
    monkeypatch.setattr(sqlite3, 'connect', record_connect)
    ctx = DataAPI()
    options = ({storage: str(tmp_path / 'external')} if storage == 'offline_dir' else
               {storage: {'path': str(tmp_path / 'external.sqlite')}})
    def run(index):
        with RunContext.start(tmp_path / f'run-{index}') as scope:
            rows = ctx.from_items([{'item': 'question'}]).map_prompt_async(
                'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'},
                output='answer', call_output='call', error_output='error',
                max_requests=0, options=options).take_all()
            with scope.journal._connection() as db:
                assert not db.execute("SELECT 1 FROM runtime_budgets WHERE name='requests' AND consumed>0").fetchall()
            return rows[0]
    pending = run(0)
    assert pending['error']['type'] == 'PromptResponsePending'
    trace = pending['error']['call']
    if storage == 'offline_dir':
        from demiflow.operator_llm.offline import submit_response
        submit_response(trace['request_path'], trace['response_path'], {'result': 'offline'}, model='local-fixture')
    else:
        from demiflow.operator_llm.sqlite_offline import submit_response
        submit_response(None, trace['request_ref'], {'result': 'offline'}, model='local-fixture')
    first, replay = run(1), run(2)
    assert first['answer'] == replay['answer'] == 'offline'
    assert first['call']['request_sha256'] == replay['call']['request_sha256']
    assert first['call']['provider_called'] is replay['call']['provider_called'] is False
    usage = ctx.prompt_usage()
    assert usage['prompt_calls_attempted'] == 3
    assert usage['provider_requests_started'] == usage['provider_requests_reserved'] == 0
    assert usage['input_tokens'] == usage['output_tokens'] == 0
    if storage == 'offline_store':
        assert any('external.sqlite' in name for name in opened)


def test_public_usage_reads_actual_worker_and_is_cumulative(tmp_path, native_server):
    ctx = DataAPI()
    assert ctx.prompt_usage()['provider_requests_started'] == 0
    for index in range(2):
        with RunContext.start(tmp_path / f'run-{index}'):
            rows = ctx.from_items([{'item': str(index)}]).map_prompt_async(
                'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'},
                output='answer', max_requests=1).take_all()
        assert rows[0]['answer'] == 'ok'
        usage = ctx.prompt_usage()
        assert usage['provider_requests_started'] == index + 1
        assert usage['provider_requests_completed'] == index + 1
        assert usage['input_tokens'] == 3 * (index + 1)
        assert usage['output_tokens'] == index + 1
        assert usage['provider_requests_failed'] == 0
    assert len(native_server) == 2


@pytest.mark.parametrize('storage', ['offline_dir', 'offline_store'])
def test_offline_response_is_bounded_before_reading_into_python(tmp_path, storage):
    from demiflow.operator_llm.model import OperatorLLMRequest, PromptModel
    from demiflow.operator_llm.offline import PromptResponsePending, request_record
    from demiflow.operator_llm.client import create_async_operator_llm_client
    model = PromptModel('fixture', 'openai_compatible')
    options = ({storage: str(tmp_path / 'external')} if storage == 'offline_dir' else
               {storage: {'path': str(tmp_path / 'external.sqlite')}})
    client = create_async_operator_llm_client(model, options)
    record = request_record(OperatorLLMRequest('test', 'v1', model.name, ()))
    try:
        with pytest.raises(PromptResponsePending) as pending:
            client.lookup_record(record, max_bytes=1024)
        trace = pending.value.call
        if storage == 'offline_dir':
            from demiflow.operator_llm.offline import submit_response
            submit_response(trace['request_path'], trace['response_path'], 'x' * 4096, model=model.name)
        else:
            from demiflow.operator_llm.sqlite_offline import submit_response
            submit_response(None, trace['request_ref'], 'x' * 4096, model=model.name)
        with pytest.raises(ValueError, match='byte budget'):
            client.lookup_record(record, max_bytes=1024)
    finally:
        import asyncio
        asyncio.run(client.aclose())


def test_offline_worker_cannot_supply_an_unverified_port():
    from demiflow.operator_llm.offline import RemoteOfflinePromptClient
    with pytest.raises(ValueError, match='verified native'):
        RemoteOfflinePromptClient(None, {'offline_dir': '/unused'}, object())


def test_prompt_observation_replay_and_new_boot_do_not_duplicate_accounting():
    from types import SimpleNamespace
    from demiflow.execution.journal.dataflow_usage import empty_usage, observe_prompt_usage, publish_prompt_usage
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    execution, executor = SimpleNamespace(), SimpleNamespace()
    counts = {**empty_usage(), 'provider_requests_started': 2}
    payload = {'occurrence_id': 'model', 'partition_id': 'p', 'prompt_usage': counts}
    first = SimpleNamespace(boot_id='first')
    for _ in range(2):
        observe_prompt_usage(execution, first, payload)
        publish_prompt_usage(executor, execution)
    assert executor._dataflow_prompt_usage['provider_requests_started'] == 2
    payload['prompt_usage'] = {**counts, 'provider_requests_started': 1}
    with pytest.raises(DataflowError, match='backwards'):
        observe_prompt_usage(execution, first, payload)
    observe_prompt_usage(execution, SimpleNamespace(boot_id='second'), payload)
    publish_prompt_usage(executor, execution)
    assert executor._dataflow_prompt_usage['provider_requests_started'] == 3
    assert len(execution._prompt_usage_slots) == 1
