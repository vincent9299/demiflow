"""Existing routes API, real native workers/journal/HTTP and original ACK recovery."""
from collections import Counter
import json
import threading
import time
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.operator_llm.parser import parse_prompt_pack
from test_runtime_v3_native_execution import PACK


@pytest.fixture
def routed_server(monkeypatch):
    value = SimpleNamespace(calls=[], finished=[], active=Counter(), peaks=Counter(), lock=threading.Lock())
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            model = body['model']
            with value.lock:
                value.calls.append(body)
                value.active[model] += 1
                value.peaks[model] = max(value.peaks[model], value.active[model])
            try:
                if '__drop_response__' in json.dumps(body):
                    self.close_connection = True
                    return
                time.sleep(.4 if model == 'slow' else .01)
                status = 401 if model == 'broken' else 200
                response = {'error': {'message': 'provider unavailable'}} if status == 401 else {
                    'choices': [{'message': {'content': json.dumps({'result': model})}}]}
                raw = json.dumps(response).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            finally:
                with value.lock:
                    value.active[model] -= 1
                    value.finished.append(model)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv('DF_V3_NATIVE_TEST_URL', f'http://127.0.0.1:{server.server_port}/v1')
    monkeypatch.setenv('DF_V3_NATIVE_TEST_KEY', 'routed-test')
    try:
        yield value
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _pipeline(rows, *, total=8, routes=None, concurrency=2, error_output='error'):
    if routes is None:
        routes = {name: {'concurrency': 1, 'max_requests': total} for name in ('a', 'b')}
    routes = {name: {'config': parse_prompt_pack(PACK.replace('local-fixture', name)), **settings}
              for name, settings in routes.items()}
    return data.from_items(rows).map_prompt_async('enrich', config=parse_prompt_pack(PACK),
        inputs={'payload': 'item'}, output='answer', error_output=error_output, call_output='call',
        routes=routes, route='route', isolate_route_failures=True, concurrency=concurrency,
        max_requests=total, queue_depth=1,
        options={'max_response_bytes': 65536, 'max_event_bytes': 65536, 'trust_env': False})


def _calls(run):
    with run.journal._connection() as db:
        calls = db.execute('SELECT request_json,response_json,error_json FROM calls ORDER BY request_key').fetchall()
        budgets = dict(db.execute('SELECT name,consumed FROM runtime_budgets WHERE name LIKE "requests%"').fetchall())
    return calls, budgets


def test_routed_native_overlap_and_provider_isolation(tmp_path, routed_server):
    rows = [{'item': i, 'route': name} for i, name in enumerate(['slow', 'broken', 'fast', 'fast', 'fast'])]
    routes = {name: {'concurrency': 1, 'max_requests': 5} for name in ('slow', 'broken', 'fast')}
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        pipeline = _pipeline(rows, total=5, routes=routes, concurrency=5)
        saved = pipeline.take_all()
        assert len(saved) == len(routed_server.calls) == 5
        assert routed_server.finished.index('fast') < routed_server.finished.index('slow')
        assert all(n == 1 for n in routed_server.peaks.values())
        assert sum(row.get('answer') == 'fast' for row in saved) == 3
        assert next(row['error'] for row in saved if row['route'] == 'broken')['route_unavailable'] is True
        assert all('reasoning_effort' not in call for call in routed_server.calls)
        calls, budgets = _calls(run)
        assert len(calls) == 5 and budgets['requests'] == 5
        assert sorted(budgets.values()) == [1, 1, 3, 5, 5]  # child, whole node, run
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
        usage = pipeline._executor.operator_llm_usage()
        assert usage['provider_requests_started'] == usage['provider_requests_reserved'] == 5
        assert usage['provider_requests_completed'] == 4 and usage['provider_requests_failed'] == 1
    assert not any(routed_server.active.values())


@pytest.mark.parametrize('scope,total,route_a,run_limit,expected', [
    ('node', 2, 3, 10, 2), ('route', 3, 1, 10, 2), ('run', 3, 3, 1, 1)])
@pytest.mark.parametrize('concurrency', [1, 3])
def test_routed_native_atomic_budget_layers(tmp_path, routed_server, scope, total, route_a, run_limit, expected, concurrency):
    names = ['a', 'a', 'b'] if scope == 'route' else ['a', 'b', 'a']
    rows = [{'item': i, 'route': name} for i, name in enumerate(names)]
    with RunContext.start(tmp_path/'run', limits={'max_requests': run_limit}) as run, data.local_execution():
        saved = _pipeline(rows, total=total, routes={
            'a': {'concurrency': 1, 'max_requests': route_a},
            'b': {'concurrency': 1, 'max_requests': 3}}, concurrency=concurrency).take_all()
        assert len(saved) == 3
        assert len(routed_server.calls) == expected
        assert sum('answer' in row for row in saved) == expected
        assert sum(bool(row.get('error')) for row in saved) == 3 - expected
        calls, budgets = _calls(run)
        assert len(calls) == expected == budgets['requests']
        assert all(row[1] is not None for row in calls)
        assert sum(budgets.values()) == expected * 3  # no failed reservation was charged
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


@pytest.mark.parametrize('provider', ['local', 'ray'])
def test_routed_native_original_ack_resume(tmp_path, routed_server, monkeypatch, provider):
    from runtime_v3_residency_scenarios import _provider
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    if provider == 'ray':
        pytest.importorskip('ray')
    class Interrupted(Exception):
        pass
    rows = [{'item': str(i), 'route': 'a' if i % 2 == 0 else 'b'} for i in range(8)]
    pump = DataflowExecution.pump
    def interrupt(execution, **kwargs):
        status = pump(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            raise Interrupted()
        return status
    fixture = SimpleNamespace(root=tmp_path, provider=provider)
    with _provider(fixture, workers=3), pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 2}) as run:
            ref, old_attempt = run.ref, run.attempt
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                _pipeline(rows).take_all()
    before = list(routed_server.calls)
    assert 0 < len(before) < 8
    committed = run.journal.execution_commits('action-1')
    assert committed and all(c['status'] == 'acked' for c in committed)
    with _provider(fixture, workers=3), RunContext.resume(ref) as run:
        saved = _pipeline(rows).take_all()
        assert run.attempt > old_attempt
        assert sorted((row['item'], row['route'], row['answer']) for row in saved) == [
            (str(i), 'a' if i % 2 == 0 else 'b', 'a' if i % 2 == 0 else 'b') for i in range(8)]
        calls, budgets = _calls(run)
        assert len(calls) == len(routed_server.calls) == 8
        assert budgets['requests'] == 8 and sorted(budgets.values()) == [4, 4, 8, 8]
        assert all(row[1] is not None for row in calls)
        assert run.journal.execution_commits('action-1')[:len(committed)] == committed
    assert routed_server.calls[:len(before)] == before


def test_routed_native_unknown_request_stays_unknown(tmp_path, routed_server):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.operator_llm.journal import UncertainPromptCall
    rows = [{'item': 'a', 'route': 'a'}, {'item': '__drop_response__', 'route': 'b'}]
    with pytest.raises(DataflowError), RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 1}) as run, data.local_execution():
        ref = run.ref
        _pipeline(rows, total=2, concurrency=1, error_output=None).take_all()
    assert len(routed_server.calls) == 2
    calls, budgets = _calls(run)
    assert sum(row[1] is None for row in calls) == 1 and budgets['requests'] == 2
    with pytest.raises(UncertainPromptCall), RunContext.resume(ref), data.local_execution():
        _pipeline(rows, total=2, concurrency=1, error_output=None).take_all()
    assert len(routed_server.calls) == 2


def test_unknown_route_never_selects_another_model(tmp_path, routed_server):
    with pytest.raises(KeyError, match='undeclared'), RunContext.start(tmp_path/'run') as run, data.local_execution():
        _pipeline([{'item': 'a', 'route': 'undeclared'}]).take_all()
    assert not routed_server.calls
    assert not run.journal.execution_commits('action-1')


class _StatefulPredicate:
    def __init__(self):
        self.count = 0

    def __call__(self, row):
        self.count += 1
        return True


def test_routed_mutable_predicate_cannot_claim_http_only_recovery(tmp_path, routed_server, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.recovery_contract import RecoveryContractError
    class Interrupted(Exception):
        pass
    def pipeline():
        value = _pipeline([{'item': str(i), 'route': 'a'} for i in range(4)], concurrency=1)
        actor = value._plan.operations[-1].callable.target.__self__
        for child in actor.actors.values():
            child.when = _StatefulPredicate()
        return value
    pump = DataflowExecution.pump
    def interrupt(execution, **kwargs):
        status = pump(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            raise Interrupted()
        return status
    with pytest.raises(Interrupted), RunContext.start(tmp_path/'run', limits={
            'checkpoint_rows': 1, 'max_state_bytes': 1024**2}) as run, data.local_execution():
        ref = run.ref
        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution, 'pump', interrupt)
            pipeline().take_all()
    before = len(routed_server.calls)
    assert 0 < before < 4
    with pytest.raises((DataflowError, RecoveryContractError), match='attempt|recover|state'), \
            RunContext.resume(ref), data.local_execution():
        pipeline().take_all()
    assert len(routed_server.calls) == before


@pytest.mark.parametrize('change', ['capacity', 'gates', 'external_gate'])
def test_routed_declaration_refuses_live_or_changed_children(change):
    from demiflow.execution.operator_bindings.dataflow_model_factory import native_model_declaration
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    pipeline = _pipeline([{'item': 1, 'route': 'a'}])
    operation = pipeline._plan.operations[-1]
    actor = operation.callable.target.__self__
    if change == 'capacity':
        actor.limits['a'] = 2
    elif change == 'gates':
        actor.gates['a'] = object()
    else:
        from demiflow.services.request_limits import RequestGate
        actor.actors['a']._runtime.request_gate = RequestGate(1)
    with pytest.raises((ValueError, DataflowError), match='declaration|capacity|copies differ'):
        native_model_declaration(operation)


@pytest.mark.parametrize('reason', ['cooperative:operator_stop_file','cooperative:run_cancelled'])
def test_routed_global_stop_is_not_provider_route_failure(tmp_path,routed_server,monkeypatch,reason):
    from demiflow.execution.journal.dataflow_authority import DriverJournalService
    from demiflow.services.request_limits import ServiceStopped
    original=DriverJournalService.native_request
    def stop_after_original_reservation(owner,command,**kwargs):
        reply=original(owner,command,**kwargs)
        if command.operation=='reserve' and reply.value is True:
            owner.stop_model_admission(reason)
        return reply
    monkeypatch.setattr(DriverJournalService,'native_request',stop_after_original_reservation)
    with pytest.raises(ServiceStopped):
        with RunContext.start(tmp_path/'run') as run, data.local_execution():
            _pipeline([{'item':0,'route':'a'},{'item':1,'route':'a'}],total=2,
                routes={'a':{'concurrency':1,'max_requests':2}},concurrency=1).take_all()
    assert len(routed_server.calls)==len(routed_server.finished)==1
    calls,budgets=_calls(run)
    assert len(calls)==1 and calls[0][1] is not None and budgets['requests']==1
    assert not any(fact['status']=='acked' for fact in run.journal.execution_commits('action-1'))
