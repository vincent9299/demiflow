"""Real original Prompt cache identities, bounded driver ownership and recovery refusal."""
import asyncio
from dataclasses import replace
import json
import os
from pathlib import Path
import sqlite3

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.journal import UncertainPromptCall
from test_runtime_v3_native_execution import PACK, native_server


def _pipeline(path, *, asynchronous=True, items=('a', 'b'), private_limit=None, maximum=None, error_output=None, read_only=False):
    ds = data.from_items([{'item': value} for value in items])
    method = ds.map_prompt_async if asynchronous else ds.map_prompt
    extra = {'concurrency': 1, 'call_output': 'call', 'error_output': error_output} if asynchronous else {}
    return method('enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer',
        options={'trust_env': False, 'max_response_bytes': 65536, 'max_event_bytes': 65536,
                 'sqlite_journal': {'path': str(path), 'max_requests': private_limit, 'read_only': read_only}},
        max_requests=maximum, **extra)


def _charged(run):
    with run.journal._connection() as db:
        value = db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='calls'").fetchone()
        assert not exists or db.execute('SELECT count(*) FROM calls').fetchone()[0] == 0
    return 0 if value is None else value[0]


def _rows(path):
    with sqlite3.connect(path) as db:
        return db.execute('SELECT request_key,request_json,response_json,error_json FROM calls ORDER BY request_key').fetchall()


@pytest.mark.parametrize('asynchronous', [False, True])
def test_original_private_prompt_keys_and_rows_survive_native_delivery(tmp_path, native_server, asynchronous):
    path = tmp_path/'original.sqlite'
    # Fill history using the actual original independent PromptActor/client,
    # before any unified worker or execution-native request namespace exists.
    actor = _pipeline(path)._plan.operations[-1].callable.target.__self__
    async def original():
        await actor.astart()
        try:
            for item in ('a', 'b'):
                assert (await actor({'item': item}))['answer'] == 'ok'
        finally:
            await actor.aclose()
    asyncio.run(original())
    before = _rows(path)
    assert len(before) == len(native_server) == 2
    assert all('native_occurrence' not in json.loads(row[1]) for row in before)
    with RunContext.start(tmp_path/'replay', limits={'max_requests': 1}) as run, data.local_execution():
        result = _pipeline(path, asynchronous=asynchronous).take_all()
        assert [row['answer'] for row in result] == ['ok', 'ok']
        assert _charged(run) == 0
        assert run.journal.source_binding('action-1-state-recovery-restriction')['recovery'] == 'attempt_only'
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert len(native_server) == 2 and _rows(path) == before


@pytest.mark.parametrize('scope', ['run', 'node', 'private'])
def test_private_prompt_all_budgets_precede_provider(tmp_path, native_server, scope):
    path = tmp_path/'private.sqlite'
    with RunContext.start(tmp_path/'run', limits={'max_requests': 1 if scope == 'run' else 5}) as run, data.local_execution():
        result = _pipeline(path, maximum=1 if scope == 'node' else 5,
            private_limit=1 if scope == 'private' else 5, error_output='error').take_all()
        assert len(result) == 2 and len(native_server) == 1
        assert sum(row.get('answer') == 'ok' for row in result) == 1
        assert sum(bool(row.get('error')) for row in result) == 1
        # Run admission intentionally precedes private SQLite. It is retained
        # if the latter refuses; there is no fictional cross-database refund.
        assert _charged(run) == (2 if scope == 'private' else 1)
    assert len(_rows(path)) == 1


def test_private_prompt_unknown_never_resends(tmp_path, native_server):
    path = tmp_path/'private.sqlite'
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match='response|RemoteProtocolError'), RunContext.start(tmp_path/'failed') as run, data.local_execution():
        ref = run.ref
        _pipeline(path, items=('__drop_response__',)).take_all()
    assert len(native_server) == 1
    assert _rows(path)[0][2] is None and _charged(run) == 1
    with pytest.raises(RecoveryContractError, match='attempt|recover|private'), RunContext.resume(ref):
        _pipeline(path, items=('__drop_response__',)).take_all()
    with pytest.raises(UncertainPromptCall), RunContext.start(tmp_path/'new'), data.local_execution():
        _pipeline(path, items=('__drop_response__',)).take_all()
    assert len(native_server) == 1


def test_private_prompt_rejects_materialization_before_database(tmp_path, native_server):
    path = tmp_path/'private.sqlite'
    with pytest.raises(RecoveryContractError, match='delivery-only'), RunContext.start(tmp_path/'run'), data.local_execution():
        _pipeline(path).materialize()
    assert not native_server and not path.exists()


def test_private_prompt_read_only_miss_does_not_send_or_charge(tmp_path, native_server):
    from demiflow.operator_llm.errors import PromptReplayMissError
    path = tmp_path/'private.sqlite'
    with RunContext.start(tmp_path/'seed'), data.local_execution():
        assert _pipeline(path, items=('a',)).take_all()[0]['answer'] == 'ok'
    before = _rows(path)
    with pytest.raises(PromptReplayMissError, match='Read-only'), RunContext.start(tmp_path/'run') as run, data.local_execution():
        _pipeline(path, items=('missing',), read_only=True).take_all()
    assert _charged(run) == 0
    assert len(native_server) == 1 and _rows(path) == before


def test_private_prompt_process_workers_never_open_cache(tmp_path, native_server):
    import psutil
    import threading
    from types import SimpleNamespace
    from runtime_v3_residency_scenarios import _provider
    path = tmp_path/'private.sqlite'
    observed = []
    def inspect_files():
        process = psutil.Process()
        assert any(Path(item.path) == path for item in process.open_files())
        children = process.children(recursive=True)
        assert children
        for child in children:
            try:
                assert all(str(path) not in item.path for item in child.open_files())
            except psutil.NoSuchProcess:
                pass
        observed.append(process.pid)
    native_server.barrier = threading.Barrier(1, action=inspect_files)
    with _provider(SimpleNamespace(root=tmp_path, provider='local'), workers=3), RunContext.start(tmp_path/'run') as run:
        assert [row['answer'] for row in _pipeline(path).take_all()] == ['ok', 'ok']
        assert _charged(run) == 2
    assert observed == [os.getpid(), os.getpid()]
    assert not any(Path(item.path) == path for item in psutil.Process().open_files())
    assert not any(thread.name == 'dataflow-private-journal-close' and thread.is_alive()
                   for thread in threading.enumerate())


def test_private_prompt_cannot_change_sealed_route_over_rpc(tmp_path, native_server, monkeypatch):
    from demiflow.execution import dataflow_prompt_journal as module
    original = module.private_prompt_request
    def attack(service, command, request):
        return original(service, replace(command, options={**dict(command.options), 'prompt_route': 'undeclared'}), request)
    monkeypatch.setattr(module, 'private_prompt_request', attack)
    path = tmp_path/'private.sqlite'
    with RunContext.start(tmp_path/'run'), data.local_execution():
        result = _pipeline(path, items=('a',), error_output='error').take_all()
    assert result[0]['error']['type'] == 'DataflowError'
    assert not path.exists() and not native_server
