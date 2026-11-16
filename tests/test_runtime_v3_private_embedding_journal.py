"""Legacy delivery cache stays driver-owned, bounded and explicitly attempt-only."""
from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_codec import _thaw_json
from demiflow.execution.dataflow_embedding_journal import bind_private_embedding_contracts
from demiflow.execution.dataflow_model_journal import _rpc_operation_id
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.operator_llm.errors import PromptBudgetExceededError
from demiflow.operator_llm.journal import UncertainPromptCall
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
from test_embeddings import server
from test_runtime_v3_native_rpc_replay import service, event, interrupted_run


def dataset(server, path, *, rows=None, maximum=None, read_only=False, concurrency=1):
    return data.from_items(rows or [{'text': 'a'}, {'text': 'b'}]).map_embeddings(
        model=server['model'], inputs={'text': 'text'}, batch_size=1, concurrency=concurrency,
        call_output='call', max_requests=maximum,
        options={'sqlite_journal': {'path': str(path), 'read_only': read_only}})


def charged(run):
    with run.journal._connection() as db:
        row = db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()
        return 0 if row is None else row[0]


def no_native_calls(run):
    with run.journal._connection() as db:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='calls'").fetchone()
        assert exists is None or db.execute('SELECT count(*) FROM calls').fetchone()[0] == 0


def test_real_workers_use_only_driver_cache_and_new_run_reuses_without_charge(tmp_path, server):
    path = tmp_path / 'private.sqlite'
    observed = []
    def inspect_descriptors():
        import psutil
        parent = psutil.Process()
        assert any(Path(file.path) == path for file in parent.open_files())
        children = parent.children(recursive=True)
        assert children
        for child in children:
            try:
                assert not any(str(path) in file.path for file in child.open_files())
            except psutil.NoSuchProcess:
                pass
        observed.append(parent.pid)
    server['on_request'] = inspect_descriptors
    for index in range(2):
        with RunContext.start(tmp_path / ('run-' + str(index)), limits={'max_requests': 2}) as run, \
                data.local_execution(workers=4):
            rows = dataset(server, path).take_all()
            assert len(rows) == 2 and all(row['call']['reused'] == bool(index) for row in rows)
            assert charged(run) == (2 if index == 0 else 0)
            no_native_calls(run)
            assert run.journal.source_binding('action-1-state-recovery-restriction')['recovery'] == 'attempt_only'
            assert run.journal.source_binding('action-1-private-embedding-journal')['admission'] == 'run_before_private'
        assert not any(thread.name == 'dataflow-private-journal-close' and thread.is_alive()
                       for thread in __import__('threading').enumerate())
    assert observed == [os.getpid(), os.getpid()] and len(server['bodies']) == 2
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 2


@pytest.mark.parametrize('ceiling', ['run', 'node'])
def test_private_and_run_node_budgets_precede_second_http(tmp_path, server, ceiling):
    path = tmp_path / 'private.sqlite'
    with pytest.raises(PromptBudgetExceededError):
        with RunContext.start(tmp_path / 'run', limits={'max_requests': 1 if ceiling == 'run' else 5}) as run, \
                data.local_execution(workers=4):
            dataset(server, path, maximum=1 if ceiling == 'node' else 5).take_all()
    assert len(server['bodies']) == 1
    with RunJournal(tmp_path / 'run')._connection() as db:
        assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 1


@pytest.mark.parametrize('after_private', [False, True])
def test_two_database_failure_keeps_admission_and_forbids_resume(tmp_path, server, monkeypatch, after_private):
    path = tmp_path / 'private.sqlite'
    original = SQLitePromptJournal.reserve
    def fail(journal, request):
        if after_private:
            assert original(journal, request)
        raise RuntimeError('injected-private-reserve-gap')
    monkeypatch.setattr(SQLitePromptJournal, 'reserve', fail)
    with pytest.raises(RuntimeError, match='injected-private-reserve-gap'):
        with RunContext.start(tmp_path / 'run', limits={'max_requests': 1}) as run, data.local_execution():
            reference = run.ref
            dataset(server, path, rows=[{'text': 'a'}]).take_all()
    assert not server['bodies']
    with RunJournal(reference.directory)._connection() as db:
        assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == int(after_private)
    monkeypatch.setattr(SQLitePromptJournal, 'reserve', original)
    with pytest.raises(RecoveryContractError, match='attempt-only'):
        with RunContext.resume(reference), data.local_execution():
            dataset(server, path, rows=[{'text': 'a'}]).take_all()
    if after_private:
        with pytest.raises(UncertainPromptCall):
            with RunContext.start(tmp_path / 'new-run'), data.local_execution():
                dataset(server, path, rows=[{'text': 'a'}]).take_all()
        assert not server['bodies']
    else:
        with RunContext.start(tmp_path / 'new-run'), data.local_execution():
            assert len(dataset(server, path, rows=[{'text': 'a'}]).take_all()) == 1
        assert len(server['bodies']) == 1


def test_materialize_private_still_rejected_before_http(tmp_path, server):
    with pytest.raises(RecoveryContractError, match='private SQLite journal'):
        dataset(server, tmp_path / 'private.sqlite').materialize()
    assert not server['bodies'] and not (tmp_path / 'private.sqlite').exists()


def test_managed_private_still_rejected_before_database(tmp_path, monkeypatch):
    from demiflow.execution import checked
    with interrupted_run(tmp_path / 'run') as run, service(run) as (authority, store, _):
        authority._action.declaration['kind'] = 'take_all'
        monkeypatch.setattr(checked, 'current', lambda: object())
        with pytest.raises(RecoveryContractError, match='Managed execution'):
            bind_private_embedding_contracts(authority, private_graph(tmp_path / 'private.sqlite'))
    assert not (tmp_path / 'private.sqlite').exists()


def private_graph(path):
    return SimpleNamespace(nodes=[SimpleNamespace(occurrence_id='node-1', kernel=SimpleNamespace(
        kind='embedding', configuration={'private_journal': {'path': str(path)}, 'max_requests': 2}))])


def private_event(run, store, seed='a'):
    original = event(run, store, seed)
    payload = _thaw_json(original.payload)
    payload['reservation'] = None
    op = _rpc_operation_id(payload['action_id'], payload['node_id'], payload['request_key'], 'reserve')
    payload['operation_id'] = op
    return replace(original, operation_id=op, payload=payload)


def test_duplicate_reserve_rpc_one_original_admission_and_private_intent(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    path = tmp_path / 'private.sqlite'
    with interrupted_run(tmp_path / 'run') as run, service(run, pending=1) as (authority, store, lease):
        authority._action.declaration['kind'] = 'take_all'
        bind_private_embedding_contracts(authority, private_graph(path))
        request = private_event(run, store)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = list(pool.map(authority.dispatch, (request, request)))
        assert first == second and first.payload['value'] is True
        assert charged(run) == 1
        no_native_calls(run)
        assert authority.dispatch(private_event(run, store, 'b')).status == 'ok'
        with pytest.raises(UncertainPromptCall):
            authority.dispatch(request)  # Evicted receipt cannot recreate intent.
        assert charged(run) == 2
        with sqlite3.connect(path) as db:
            assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 2
        stale = replace(request, attempt=run.attempt + 1, payload=_thaw_json(request.payload))
        with pytest.raises(Exception, match='FENCED'):
            authority.dispatch(stale)


def test_private_read_only_miss_has_no_admission_or_http(tmp_path, server):
    path = tmp_path / 'private.sqlite'
    with RunContext.start(tmp_path / 'write'), data.local_execution():
        dataset(server, path, rows=[{'text': 'a'}]).take_all()
    with pytest.raises(LookupError, match='read-only journal'):
        with RunContext.start(tmp_path / 'read') as run, data.local_execution():
            dataset(server, path, rows=[{'text': 'b'}], read_only=True).take_all()
    with RunJournal(tmp_path / 'read')._connection() as db:
        assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone() is None
    assert len(server['bodies']) == 1


@pytest.mark.parametrize('column', ['response_json', 'error_json', 'legacy_refs_json'])
def test_bounded_sqlite_read_counts_utf8_and_preserves_original_bytes(tmp_path, column):
    from demiflow.operator_llm.journal import RequestRecord
    request = RequestRecord({'body': {'input': 'same'}, 'model_contract': {'model': 'fixed'}})
    journal = SQLitePromptJournal(tmp_path / 'cache.sqlite')
    try:
        assert journal.reserve(request)
        journal.response(request, {'status_code': 200, 'body': 'ok', 'computed_at': 'fixed', 'elapsed_s': 1})
        # <1024 codepoints but >1024 UTF-8 bytes, admitted by the old path.
        large = json.dumps({'detail': '界' * 600}, ensure_ascii=False)
        with journal._connect() as db:
            db.execute('UPDATE calls SET ' + column + '=? WHERE request_key=?', (large, request.request_key))
        with pytest.raises(ValueError, match='before JSON decoding'):
            journal.lookup_bounded(request, max_bytes=1024)
        with journal._connect() as db:
            assert db.execute('SELECT ' + column + ' FROM calls WHERE request_key=?',
                              (request.request_key,)).fetchone()[0] == large
        if column == 'response_json':
            assert journal.lookup(request) == {'detail': '界' * 600}  # Default read contract unchanged.
    finally:
        journal.close()


@pytest.mark.parametrize('column', ['response_json', 'error_json'])
def test_large_historical_cache_is_rejected_before_http_and_not_rewritten(tmp_path, server, column):
    path = tmp_path / 'private.sqlite'
    with RunContext.start(tmp_path / 'first'), data.local_execution():
        dataset(server, path, rows=[{'text': 'a'}]).take_all()
    large = json.dumps({'message': '界' * 400000}, ensure_ascii=False)
    with sqlite3.connect(path) as db:
        db.execute('UPDATE calls SET ' + column + '=?', (large,))
        if column == 'error_json':
            db.execute('UPDATE calls SET response_json=NULL')
    with pytest.raises(ValueError, match='before JSON decoding'):
        with RunContext.start(tmp_path / 'second', limits={'max_control_bytes': 1024**2}), data.local_execution():
            dataset(server, path, rows=[{'text': 'a'}]).take_all()
    assert len(server['bodies']) == 1
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT ' + column + ' FROM calls').fetchone()[0] == large


@pytest.mark.parametrize('kind', ['acquisition', 'writer'])
def test_private_stream_rejects_native_acquisition_and_writes(tmp_path, kind):
    with interrupted_run(tmp_path / 'run') as run, service(run) as (authority, _, _):
        authority._action.declaration['kind'] = 'run_stream'
        graph = private_graph(tmp_path / 'private.sqlite')
        if kind == 'acquisition':
            authority._native = SimpleNamespace()
        else:
            graph.nodes.append(SimpleNamespace(occurrence_id='writer', kernel=SimpleNamespace(
                kind='SaveLanceOp', configuration={})))
        with pytest.raises(RecoveryContractError, match='acquisition/writes'):
            bind_private_embedding_contracts(authority, graph)
    assert not (tmp_path / 'private.sqlite').exists()
