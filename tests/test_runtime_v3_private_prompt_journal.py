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
from test_prompt_http_retry import retry_server
from test_runtime_v3_routed_prompt import routed_server


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


def test_private_prompt_materialization_reuses_original_committed_result(tmp_path, native_server):
    path = tmp_path/'private.sqlite'
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        saved = _pipeline(path).materialize()
        try:
            handle = saved._source.handle
            original = handle._manifest_ref
            expected = list(handle.rows())
            assert [row['answer'] for row in expected] == ['ok', 'ok']
            assert saved.take_all() == saved.take_all() == expected
            assert handle._manifest_ref == original
            assert len(native_server) == _charged(run) == 2
            commits = run.journal.execution_commits('action-1')
            assert commits and commits[-1]['status'] == 'acked'
            assert run.journal.execution_action('action-1')['result'] is not None
            assert run.journal.source_binding('action-1-state-recovery-restriction')['recovery'] == 'attempt_only'
        finally:
            saved.release()


def test_interrupted_private_materialization_never_becomes_a_resumable_result(tmp_path, native_server, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    class Interrupted(Exception):
        pass
    pump = DataflowExecution.pump
    def interrupt(execution, **kwargs):
        status = pump(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            raise Interrupted()
        return status
    with pytest.raises(Interrupted), RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 1}) as run, data.local_execution():
        ref = run.ref
        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, 'pump', interrupt)
            _pipeline(tmp_path/'private.sqlite', items=tuple(str(i) for i in range(8))).materialize()
    sent = len(native_server)
    assert 0 < sent < 8
    assert run.journal.execution_action('action-1')['result'] is None
    assert run.journal.execution_commits('action-1') and all(c['status'] == 'acked' for c in run.journal.execution_commits('action-1'))
    with pytest.raises(RecoveryContractError, match='attempt|recover|private'), RunContext.resume(ref):
        _pipeline(tmp_path/'private.sqlite', items=tuple(str(i) for i in range(8))).materialize()
    assert len(native_server) == sent


def test_private_prompt_ray_materializes_current_attempt_without_reissuing_http(tmp_path, native_server):
    pytest.importorskip('ray')
    from types import SimpleNamespace
    from runtime_v3_residency_scenarios import _provider
    with _provider(SimpleNamespace(root=tmp_path, provider='ray'), workers=3), RunContext.start(tmp_path/'run') as run:
        saved = _pipeline(tmp_path/'private.sqlite').materialize()
        try:
            original = saved._source.handle._manifest_ref
            first = saved.take_all()
            assert saved.take_all() == first and [row['answer'] for row in first] == ['ok', 'ok']
            assert len(native_server) == _charged(run) == 2
            assert saved._source.handle._manifest_ref == original
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
            assert run.journal.source_binding('action-1-state-recovery-restriction')['recovery'] == 'attempt_only'
        finally:
            saved.release()


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
    from demiflow.execution.journal import dataflow_prompt_journal as module
    original = module.private_prompt_request
    def attack(service, command, request):
        return original(service, replace(command, options={**dict(command.options), 'prompt_route': 'undeclared'}), request)
    monkeypatch.setattr(module, 'private_prompt_request', attack)
    path = tmp_path/'private.sqlite'
    with RunContext.start(tmp_path/'run'), data.local_execution():
        result = _pipeline(path, items=('a',), error_output='error').take_all()
    assert result[0]['error']['type'] == 'DataflowError'
    assert not path.exists() and not native_server


def test_private_prompt_original_retry_history_and_finite_charges(tmp_path, retry_server):
    from test_map_prompt_async import PACK as RETRY_PACK
    from test_prompt_http_retry import options
    from demiflow.operator_llm.call_ref import journal_totals, read_call
    path = tmp_path/'private.sqlite'
    def pipeline():
        return data.from_items([{'item': 'a'}, {'item': 'b'}]).map_prompt_async(
            'enrich', config=parse_prompt_pack(RETRY_PACK), inputs={'payload': 'item'},
            output='answer', call_output='call', concurrency=1, max_requests=4,
            options=options(path, limit=4))
    with RunContext.start(tmp_path/'first', limits={'max_requests': 4}) as run, data.local_execution():
        result = pipeline().take_all()
        assert [row['answer'] for row in result] == ['ok', 'ok']
        assert _charged(run) == 4
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    calls = result[0]['call']['attempts']
    assert [call['http_status'] for call in calls] == [401, 401, 200]
    assert [read_call(call['response_ref'])['status_code'] for call in calls] == [401, 401, 200]
    assert calls[-1]['response_ref']['attempt'] == 3
    assert len(retry_server['requests']) == 4
    assert retry_server['requests'][0] == retry_server['requests'][1] == retry_server['requests'][2]
    totals = journal_totals(path)
    assert (totals['requests'], totals['responses'], totals['http_errors'], totals['http_successes']) == (4, 4, 2, 2)
    before = _rows(path)
    with RunContext.start(tmp_path/'replay', limits={'max_requests': 1}) as replay, data.local_execution():
        again = pipeline().take_all()
        assert all(row['call']['reused'] for row in again) and _charged(replay) == 0
    assert len(retry_server['requests']) == 4 and _rows(path) == before


def _private_graph(path):
    # Component protocol fixture: the real original worker lease/store/journal
    # is used below; public graph/HTTP execution is separately tested above.
    from types import SimpleNamespace
    return SimpleNamespace(nodes=[SimpleNamespace(occurrence_id='node-1', kernel=SimpleNamespace(
        kind='prompt', configuration={'private_journal': {'path': str(path)},
        'max_requests': 5, 'options': {}}))])


def _private_event(run, store, seed='a', *, scope='first', operation='reserve', value_ref=None, options=None):
    from demiflow.execution.contracts.dataflow import NativeJournalCommand
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    from demiflow.execution.contracts.dataflow_codec import encode_artifact_reference as _ref_to_json, decode_artifact_reference as _ref_from_json
    from demiflow.execution.contracts.dataflow_codec import private_prompt_rpc_id as _rpc_id
    from demiflow.execution.recovery_contract import identity
    from test_runtime_v3_native_rpc_replay import event
    original = event(run, store, seed)
    payload = _thaw_json(original.payload)
    payload.update(reservation=None, operation=operation,
        value_ref=None if value_ref is None else _ref_to_json(value_ref),
        options={**(options or {}), 'prompt_route': None, 'row_scope': identity(['row', scope])})
    command = NativeJournalCommand(**{**payload, 'request_ref': _ref_from_json(payload['request_ref']), 'value_ref': value_ref})
    payload['operation_id'] = _rpc_id(command, payload['options']['row_scope'], None)
    return replace(original, operation_id=payload['operation_id'], payload=payload)


def test_duplicate_private_prompt_rpc_is_not_a_new_logical_invocation(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from demiflow.execution.journal.dataflow_prompt_journal import bind_private_prompt_contracts
    from demiflow.execution.journal.dataflow_authority import bind_private_storage
    from test_runtime_v3_native_rpc_replay import interrupted_run, service
    path = tmp_path/'private.sqlite'
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority, store, lease):
        authority._action.declaration['kind'] = 'take_all'
        bind_private_prompt_contracts(authority, _private_graph(path))
        bind_private_storage(authority)
        request = _private_event(run, store)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, retry = list(pool.map(authority.dispatch, (request, request)))
        assert first == retry and first.payload['value'] is True and _charged(run) == 1
        # Same HTTP key, distinct original row scope: do not replay the earlier
        # successful reserve lease into a second provider invocation.
        with pytest.raises(UncertainPromptCall):
            authority.dispatch(_private_event(run, store, scope='second'))
        assert _charged(run) == 1 and len(_rows(path)) == 1
        from demiflow.execution.contracts.dataflow_codec import _thaw_json
        stale = replace(request, attempt=run.attempt+1, payload=_thaw_json(request.payload))
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        with pytest.raises(DataflowError, match='FENCED'):
            authority.dispatch(stale)


def test_private_prompt_stop_keeps_receipt_and_response_but_refuses_new_reserve(tmp_path):
    from demiflow.execution.journal.dataflow_prompt_journal import bind_private_prompt_contracts
    from demiflow.execution.journal.dataflow_authority import bind_private_storage
    from demiflow.services.request_limits import COOPERATIVE_STOP_REASON, ServiceStopped
    from test_runtime_v3_native_rpc_replay import interrupted_run, service
    path = tmp_path/'private.sqlite'
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority, store, lease):
        authority._action.declaration['kind'] = 'take_all'
        bind_private_prompt_contracts(authority, _private_graph(path))
        bind_private_storage(authority)
        request = _private_event(run, store)
        admitted = authority.dispatch(request)
        authority.stop_model_admission(COOPERATIVE_STOP_REASON)
        assert authority.dispatch(request) == admitted
        with pytest.raises(ServiceStopped, match='operator_stop_file'):
            authority.dispatch(_private_event(run, store, seed='b'))
        value = json.dumps({'status_code': 200, 'body': 'ok', 'elapsed_s': 1, 'computed_at': 'fixed'}).encode()
        ref = store.put(iter([value]), max_bytes=len(value), media_type='application/json')
        receipt = authority.dispatch(_private_event(run, store, operation='response', value_ref=ref))
        assert receipt.status == 'ok' and json.loads(_rows(path)[0][2])['body'] == 'ok'
        assert _charged(run) == 1 and len(_rows(path)) == 1


def test_private_prompt_rpc_cannot_invent_http_retry_policy(tmp_path):
    from demiflow.execution.journal.dataflow_prompt_journal import bind_private_prompt_contracts
    from demiflow.execution.journal.dataflow_authority import bind_private_storage
    from test_runtime_v3_native_rpc_replay import interrupted_run, service
    path = tmp_path/'private.sqlite'
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority, store, lease):
        authority._action.declaration['kind'] = 'take_all'
        bind_private_prompt_contracts(authority, _private_graph(path))
        bind_private_storage(authority)
        forged = _private_event(run, store, operation='reserve_http_retry', options={
            'expected_attempt': 1, 'expected_status': 401, 'max_attempts': 3})
        reply = authority.dispatch(forged)
        assert reply.status == 'error' and reply.error_code == 'INVALID_CONTRACT'
        assert _charged(run) == 0 and not path.exists()


def test_old_classic_requeue_cannot_remove_active_private_driver_intent(tmp_path):
    from demiflow.execution.journal.dataflow_prompt_journal import bind_private_prompt_contracts
    from demiflow.execution.journal.dataflow_authority import bind_private_storage
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    from test_runtime_v3_native_rpc_replay import interrupted_run, service
    path = tmp_path/'private.sqlite'
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority, store, lease):
        authority._action.declaration['kind'] = 'take_all'
        bind_private_prompt_contracts(authority, _private_graph(path))
        bind_private_storage(authority)
        assert authority.dispatch(_private_event(run, store)).payload['value'] is True
        before = _rows(path)
        classic = SQLitePromptJournal(path)
        try:
            with pytest.raises(UncertainPromptCall, match='active writer'):
                classic.requeue_uncertain([before[0][0]], actor='separate-old-client', reason='must-not-repeat-paid-call')
        finally:
            classic.close()
        assert _rows(path) == before and _charged(run) == 1


def test_explicit_original_requeue_requires_fresh_admission_for_a_new_row(tmp_path):
    from demiflow.execution.journal.dataflow_prompt_journal import bind_private_prompt_contracts
    from demiflow.execution.journal.dataflow_authority import bind_private_storage
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    from test_runtime_v3_native_rpc_replay import interrupted_run, service
    path = tmp_path/'private.sqlite'
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority, store, lease):
        authority._action.declaration['kind'] = 'take_all'
        bind_private_prompt_contracts(authority, _private_graph(path))
        bind_private_storage(authority)
        first = _private_event(run, store)
        assert authority.dispatch(first).payload['value'] is True
        error = b'{"type":"InterruptedError","detail":"explicitly reviewed unanswered request"}'
        ref = store.put(iter([error]), max_bytes=len(error), media_type='application/json')
        assert authority.dispatch(_private_event(run, store, operation='failed', value_ref=ref)).status == 'ok'
        key = _rows(path)[0][0]
        original = SQLitePromptJournal(path)
        try:
            original.requeue_uncertain([key], actor='explicit-owner', reason='approved-another-attempt')
        finally:
            original.close()
        assert not _rows(path) and _charged(run) == 1
        second = _private_event(run, store, scope='new-original-business-row')
        assert authority.dispatch(second).payload['value'] is True
        assert authority.dispatch(second).payload['value'] is True  # Same RPC remains idempotent.
        assert _charged(run) == 2 and _rows(path)[0][0] == key
        with sqlite3.connect(path) as db:
            assert db.execute('SELECT count(*) FROM recovery_events').fetchone()[0] == 1


@pytest.mark.parametrize('after_private', [False, True])
def test_private_prompt_two_database_crash_retains_admission_and_refuses_resume(tmp_path, native_server, monkeypatch, after_private):
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    path = tmp_path/'private.sqlite'
    original = SQLitePromptJournal.reserve
    def fail(journal, request):
        if after_private:
            assert original(journal, request)
        raise RuntimeError('injected-private-prompt-reserve-gap')
    monkeypatch.setattr(SQLitePromptJournal, 'reserve', fail)
    with pytest.raises(RuntimeError, match='injected-private-prompt-reserve-gap'), RunContext.start(tmp_path/'run', limits={'max_requests': 1}) as run, data.local_execution():
        ref = run.ref
        _pipeline(path, items=('a',)).take_all()
    assert not native_server and _charged(run) == 1
    assert len(_rows(path)) == int(after_private)
    monkeypatch.setattr(SQLitePromptJournal, 'reserve', original)
    with pytest.raises(RecoveryContractError, match='attempt|recover|private'), RunContext.resume(ref):
        _pipeline(path, items=('a',)).take_all()
    if after_private:
        with pytest.raises(UncertainPromptCall), RunContext.start(tmp_path/'again'), data.local_execution():
            _pipeline(path, items=('a',)).take_all()
        assert not native_server
    else:
        with RunContext.start(tmp_path/'again'), data.local_execution():
            assert _pipeline(path, items=('a',)).take_all()[0]['answer'] == 'ok'
        assert len(native_server) == 1


@pytest.mark.parametrize('provider', ['local', 'ray'])
@pytest.mark.parametrize('routed', [False, True])
def test_private_prompt_real_provider_cache_reuse(tmp_path, routed_server, provider, routed):
    from types import SimpleNamespace
    from runtime_v3_residency_scenarios import _provider
    if provider == 'ray':
        pytest.importorskip('ray')
    path = tmp_path/'private.sqlite'
    from test_runtime_v3_routed_prompt import _pipeline as route_pipeline
    fixture = SimpleNamespace(root=tmp_path, provider=provider)
    with _provider(fixture, workers=3):
        before = None
        for replay in range(2):
            with RunContext.start(tmp_path/str(replay), limits={'max_requests': 2 if replay == 0 else 1}) as run:
                if routed:
                    graph = route_pipeline([{'item': 'same', 'route': name} for name in ('a', 'b')],
                        routes={name: {'concurrency': 1, 'max_requests': 2, 'options': {
                            'trust_env': False, 'sqlite_journal': {'path': str(path)}}} for name in ('a', 'b')})
                else:
                    graph = _pipeline(path)
                rows = graph.take_all()
                assert sorted(row['answer'] for row in rows) == (['a', 'b'] if routed else ['local-fixture']*2)
                assert all(row['call']['reused'] == bool(replay) for row in rows)
                assert _charged(run) == (0 if replay else 2)
                assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
                assert run.journal.source_binding('action-1-state-recovery-restriction')['recovery'] == 'attempt_only'
            if replay:
                assert _rows(path) == before
            else:
                before = _rows(path)
            assert len(routed_server.calls) == 2
    assert not any(routed_server.active.values())


@pytest.mark.parametrize('scope,total,route_limit,run_limit,expected', [
    ('node', 2, 3, 10, 2), ('route', 3, 1, 10, 2), ('run', 3, 3, 1, 1)])
def test_private_routed_prompt_keeps_all_original_budget_layers(tmp_path, routed_server, scope, total, route_limit, run_limit, expected):
    from test_runtime_v3_routed_prompt import _pipeline as route_pipeline
    path = tmp_path/'private.sqlite'
    routes = {name: {'concurrency': 1, 'max_requests': route_limit if name == 'a' else 3,
        # Isolate the run/node/route ceilings. Omitting this explicit original
        # option also applies each actor ceiling to the shared historical DB's
        # TOTAL request count, a distinct private budget verified above.
        'options': {'trust_env': False, 'sqlite_journal': {'path': str(path), 'max_requests': None}}} for name in ('a', 'b')}
    names = ['a', 'a', 'b'] if scope == 'route' else ['a', 'b', 'a']
    with RunContext.start(tmp_path/'run', limits={'max_requests': run_limit}) as run, data.local_execution():
        rows = route_pipeline([{'item': str(i), 'route': name} for i, name in enumerate(names)],
            routes=routes, total=total, concurrency=3).take_all()
        assert len(rows) == 3 and sum('answer' in row for row in rows) == expected
        assert sum(bool(row.get('error')) for row in rows) == 3-expected
        assert len(routed_server.calls) == _charged(run) == expected
        with run.journal._connection() as db:
            assert sum(row[0] for row in db.execute("SELECT consumed FROM runtime_budgets WHERE name LIKE 'requests%'")) == expected*3


def test_routed_private_and_native_children_keep_distinct_original_authorities(tmp_path, routed_server):
    from test_runtime_v3_routed_prompt import _pipeline as route_pipeline
    path = tmp_path/'private.sqlite'
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        rows = route_pipeline([{'item': 'same', 'route': name} for name in ('a', 'b')], routes={
            'a': {'options': {'trust_env': False, 'sqlite_journal': {'path': str(path)}}}, 'b': {}}).take_all()
        assert sorted(row['answer'] for row in rows) == ['a', 'b']
        with run.journal._connection() as db:
            calls = db.execute('SELECT request_json FROM calls').fetchall()
            assert len(calls) == 1 and 'native_occurrence' in json.loads(calls[0][0])
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 2
    assert len(_rows(path)) == 1 and 'native_occurrence' not in json.loads(_rows(path)[0][1])
    assert len(routed_server.calls) == 2
