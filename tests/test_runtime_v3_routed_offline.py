"""Routed offline inputs use the sealed child store, never a provider or worker DB."""
from dataclasses import replace
import os
import sqlite3
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.operator_llm.parser import parse_prompt_pack
from test_runtime_v3_native_execution import PACK


def _routes(tmp_path, storage):
    return {name: {'options': {storage: str(tmp_path/name) if storage == 'offline_dir'
        else {'path': str(tmp_path/(name+'.sqlite'))}}, 'concurrency': 1, 'max_requests': 0}
        for name in ('left', 'right')}


def _pipeline(routes):
    # Both routes intentionally use the same model and request content. Only
    # their originally declared external input store is different.
    return data.from_items([{'item': 'same-question', 'route': name} for name in ('left', 'right')]).map_prompt_async(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer',
        call_output='call', error_output='error', routes=routes, route='route',
        concurrency=2, max_requests=0)


@pytest.mark.parametrize('storage', ['offline_dir', 'offline_store'])
@pytest.mark.parametrize('provider', ['local', 'ray'])
def test_routed_offline_same_model_request_uses_each_sealed_store(tmp_path, storage, monkeypatch, provider):
    from runtime_v3_residency_scenarios import _provider
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    if provider == 'ray':
        pytest.importorskip('ray')
    opened = []
    original_connect = sqlite3.connect
    driver_pid = os.getpid()
    def connect(*args, **kwargs):
        if any(str(tmp_path/name) in str(args[0]) for name in ('left', 'right')):
            assert os.getpid() == driver_pid
            opened.append(str(args[0]))
        return original_connect(*args, **kwargs)
    monkeypatch.setattr(sqlite3, 'connect', connect)
    routes = _routes(tmp_path, storage)
    fx = SimpleNamespace(root=tmp_path, provider=provider)
    with _provider(fx, workers=3), RunContext.start(tmp_path/'pending') as run:
        pending = _pipeline(routes).take_all()
        assert all(row['error']['type'] == 'PromptResponsePending' for row in pending)
    assert len(pending) == 2
    traces = {row['route']: row['error']['call'] for row in pending}
    assert traces['left']['request_sha256'] == traces['right']['request_sha256']
    for name, trace in traces.items():
        if storage == 'offline_dir':
            from demiflow.operator_llm.offline import submit_response
            submit_response(trace['request_path'], trace['response_path'], {'result': name}, model='local-fixture')
        else:
            from demiflow.operator_llm.sqlite_offline import submit_response
            submit_response(None, trace['request_ref'], {'result': name}, model='local-fixture')
    class Interrupted(Exception):
        pass
    pump = DataflowExecution.pump
    def interrupt(execution, **kwargs):
        status = pump(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            raise Interrupted()
        return status
    with _provider(fx, workers=3), pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'ready', limits={'checkpoint_rows': 1}) as run:
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                _pipeline(routes).take_all()
    prefix = run.journal.execution_commits('action-1')
    assert prefix and all(c['status'] == 'acked' for c in prefix)
    with _provider(fx, workers=3), RunContext.resume(ref) as run:
        rows = _pipeline(routes).take_all()
        assert sorted((row['route'], row['answer']) for row in rows) == [('left', 'left'), ('right', 'right')]
        assert all(row['call']['provider_called'] is False for row in rows)
        assert all(row['call']['request_sha256'] == traces[row['route']]['request_sha256'] for row in rows)
        with run.journal._connection() as db:
            assert not db.execute("SELECT 1 FROM runtime_budgets WHERE name LIKE 'requests%' AND consumed>0").fetchall()
        assert run.journal.execution_commits('action-1')[:len(prefix)] == prefix
    if storage == 'offline_store':
        assert any('left.sqlite' in path for path in opened)
        assert any('right.sqlite' in path for path in opened)


@pytest.mark.parametrize('injected', [{'route': 'absent'}, {'route': None}, {'path': '/tmp/not-the-sealed-store'}])
def test_routed_offline_rejects_rpc_store_or_route_override(tmp_path, monkeypatch, injected):
    from demiflow.execution import dataflow_offline
    original = dataflow_offline.offline_lookup
    seen = []
    def inject(service, command, record):
        # Adversarial wire options at the real driver boundary; the original
        # store authorization and worker/commit path are still executed.
        seen.append(command.options)
        return original(service, replace(command, options=injected), record)
    monkeypatch.setattr(dataflow_offline, 'offline_lookup', inject)
    with RunContext.start(tmp_path/'run'), data.local_execution():
        rows = _pipeline(_routes(tmp_path, 'offline_dir')).take_all()
    assert len(seen) == 2
    assert all(row.get('error') and 'answer' not in row for row in rows)
    assert all(row['error']['type'] == 'DataflowError' for row in rows)
    assert not (tmp_path/'left').exists() and not (tmp_path/'right').exists()
