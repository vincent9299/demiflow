"""Real V recovery scenarios. Each helper owns its RunContext and subprocesses.

No production module is replaced. Faults fire only inside reached native protocol
methods; SIGKILL is delivered to the actual Dataset driver, never a marker loop.
External model transport and network delivery faults are the only test substitutes.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

WORKTREE = Path(__file__).resolve().parents[1]
BOUNDARIES = {
    'before_intent': 'before_intent',
    'after_intent': 'after_intent',
    'after_prepare_before_record': 'after_prepare_before_record',
    'after_prepared_before_publish': 'after_prepared',
    'after_publish_before_receipt': 'after_sink_commit_before_receipt',
    'after_receipt_before_ack': 'after_sink_receipt',
}


def _write(path, value):
    raw = json.dumps(value, sort_keys=True, default=str).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _read(path):
    return json.loads(path.read_text())


def _child(fx, root, *, task, mode, fault=None, extra=None):
    root.mkdir(parents=True, exist_ok=True)
    config = {'root': str(root), 'task': task, 'mode': mode, 'fault': fault,
              'provider': fx.provider, **(extra or {})}
    request = root / (mode + '-request.json')
    _write(request, config)
    env = dict(os.environ, PYTHONPATH=str(WORKTREE) + ':' + str(WORKTREE / 'tests'),
               PYTHONDONTWRITEBYTECODE='1', DEMIFLOW_RESOURCE_ROOT=str(fx.root / 'capacity'))
    env.update(config.get('env', {}))
    stdout, stderr = root / (mode + '.stdout'), root / (mode + '.stderr')
    with stdout.open('wb') as out, stderr.open('wb') as err:
        process = subprocess.Popen([sys.executable, '-B', '-m', 'runtime_v3_recovery_scenarios', str(request)],
            cwd=WORKTREE, env=env, stdout=out, stderr=err, start_new_session=True)
        fx._children.append(process)
        try:
            process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
            raise AssertionError('Real Dataset driver exceeded scenario deadline: ' + str(root))
    return {'pid': process.pid, 'returncode': process.returncode,
            'stderr': stderr.read_text()[-12000:], 'stdout': stdout.read_text()[-2000:]}


def _journal(directory):
    from demiflow.execution.journal.run_journal import RunJournal
    return RunJournal(directory)


def _scope(directory):
    from demiflow.execution.pipeline.run_context import RunContext
    from demiflow.execution.contracts.recovery_contract import RunRef
    prior = _journal(directory).recovery_identity() if directory.exists() else None
    if prior:
        return RunContext.resume(RunRef(str(directory), prior['run_id']))
    return RunContext.start(directory, limits={'checkpoint_rows': 64, 'timeout_s': 300})


@contextmanager
def _provider(name):
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    ray = None
    if name == 'ray_core':
        import ray
        import tempfile
        # Ray adds session/socket suffixes; pytest's full case path exceeds the
        # AF_UNIX limit. Start an owned cluster under a short, unique /tmp root.
        ray.init(address='local', num_cpus=6, include_dashboard=False, object_store_memory=100 * 1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-vray-', dir='/tmp'))
    executor = LocalDatasetExecutor() if name == 'local' else RayDatasetExecutor()
    try:
        with _use_executor(executor):
            yield
    finally:
        executor.close()
        if ray is not None:
            ray.shutdown()


def _rows():
    return [{'id': i, 'value': i * 2} for i in range(6)]


def _double(row):
    return {'id': row['id'], 'value': row['value'] * 2}


def _commit_pipeline(target):
    from demiflow import data
    return data.from_items([{'id': i, 'value': i} for i in range(6)]).map(_double).write_lance(
        str(target), mode='create', return_receipt=True)


def _facts(journal):
    commits = journal.execution_commits('action-1')
    with journal._connection() as db:
        budgets = dict(db.execute('SELECT name,consumed FROM runtime_budgets'))
    return {'commits': commits, 'budgets': budgets, 'run_identity': journal.recovery_identity()}


def _run_child(config):
    if config['task'] in {'model_effect_recovery', 'remote_journal'}:
        return _run_model_child(config)
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE), demiflow.__file__
    from demiflow.execution.checkpoint import commit as protocol
    from demiflow.execution.journal.run_journal import RunJournal
    from demiflow.execution.journal.dataflow_authority import DriverJournalService
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.contracts.dataflow_codec import encode_wire, decode_wire
    from runtime_v3_support import _ExecuteActionRecorder, _RayDataBlocker
    root = Path(config['root'])
    directory, target = root / 'run', root / 'output.lance'
    fault, mode = config['fault'], config['mode']
    recorder, observed = _ExecuteActionRecorder(), {}
    dispatch = DriverJournalService.dispatch
    def audit_dispatch(service, event):
        reply = dispatch(service, event)
        if reply.status == 'ok' and 'worker_event' not in observed:
            observed['worker_event'] = json.loads(encode_wire(event, type_name='WorkerEvent', bounds=service._bounds))
        return reply
    DriverJournalService.dispatch = audit_dispatch
    run_holder = []
    def kill_at(boundary, commit_id=None):
        run = run_holder[0]
        marker = {'boundary': boundary, 'commit_id': commit_id, 'pid': os.getpid(),
                  'run_id': run.ref.run_id, 'attempt': run.attempt, 'fence': run.fence,
                  'facts': _facts(run.journal), 'execute_action_calls': recorder.calls,
                  'ray_data_import_attempts': list(blocked.attempts), **observed}
        _write(root / 'boundary.json', marker)
        os.kill(os.getpid(), signal.SIGKILL)
        raise AssertionError('SIGKILL did not terminate driver')
    if mode == 'crash':
        if fault == 'before_intent':
            begin = RunJournal.begin_commit
            def before_intent(journal, fence, action_id, sequence, intent):
                kill_at('before_intent')
                return begin(journal, fence, action_id, sequence, intent)
            RunJournal.begin_commit = before_intent
        else:
            def reached(name, commit_id):
                if name == BOUNDARIES[fault]:
                    kill_at(name, commit_id)
            protocol.fault_point = reached
    late = []
    if mode == 'resume' and config['task'] == 'driver_restart':
        previous = _read(root / 'boundary.json')
        prepare = DataflowExecution.prepare
        def reject_old(execution):
            result = prepare(execution)
            old = decode_wire(json.dumps(previous['worker_event']).encode(), type_name='WorkerEvent', bounds=execution._bounds)
            try:
                execution._services.journal_service.dispatch(old)
            except Exception as exc:
                late.append({'old_attempt': old.attempt, 'old_fence': old.fence,
                             'current_attempt': execution._attempt(), 'current_fence': execution._fence(),
                             'error_type': type(exc).__name__, 'error_code': getattr(exc, 'code', None), 'message': str(exc)})
            else:
                raise AssertionError('New owner accepted actual old worker event')
            return result
        DataflowExecution.prepare = reject_old
    with _scope(directory) as run, _provider(config['provider']), recorder, _RayDataBlocker() as blocked:
        run_holder.append(run)
        receipt = _commit_pipeline(target)
        import lance
        output = lance.dataset(str(target))
        result = {'rows': output.to_table().to_pylist(), 'version': output.version,
                  'receipt': asdict(receipt), 'attempt': run.attempt, 'fence': run.fence,
                  'pid': os.getpid(), 'facts': _facts(run.journal), 'late_events': late,
                  'execute_action_calls': recorder.calls, 'ray_data_import_attempts': list(blocked.attempts)}
        _write(root / (mode + '-result.json'), result)


def _crash_and_resume(fx, root, fault, *, task):
    if fault not in BOUNDARIES:
        raise ValueError('Unknown native commit boundary: ' + str(fault))
    first = _child(fx, root, task=task, mode='crash', fault=fault)
    assert first['returncode'] == -signal.SIGKILL, first
    boundary = _read(root / 'boundary.json')
    assert boundary['pid'] == first['pid'] and boundary['execute_action_calls']
    assert boundary['boundary'] == BOUNDARIES[fault]
    before = _facts(_journal(root / 'run'))
    version_before = None
    if (root / 'output.lance').exists():
        import lance
        try:
            version_before = lance.dataset(str(root / 'output.lance')).version
        except (OSError, ValueError):
            pass
    second = _child(fx, root, task=task, mode='resume', fault=fault)
    assert second['returncode'] == 0, second
    result = _read(root / 'resume-result.json')
    assert sorted(result['rows'], key=lambda r: r['id']) == _rows()
    assert result['attempt'] == boundary['attempt'] + 1 and result['fence'] != boundary['fence']
    commits = result['facts']['commits']
    assert commits and all(c['status'] == 'acked' for c in commits)
    assert len({c['commit_id'] for c in commits}) == len(commits)
    for old in before['commits']:
        assert any(c['commit_id'] == old['commit_id'] for c in commits)
    assert result['version'] == 1  # no repeated append/empty final publication
    assert result['facts']['budgets']['output_rows'] == 6
    assert result['facts']['budgets']['output_bytes'] == result['facts']['budgets']['row_bytes'] == 96
    if version_before is not None:
        assert version_before == result['version']
    attempted = boundary['ray_data_import_attempts'] + result['ray_data_import_attempts']
    assert attempted == []
    return {'crash': dict(first, killed=first['returncode'] == -signal.SIGKILL, boundary_hit=boundary),
            'before_resume': before, 'recovered': result, 'version_before_resume': version_before,
            'child_execute_action_calls': boundary['execute_action_calls'] + result['execute_action_calls'],
            'child_ray_data_import_attempts': attempted,
            'self_managed_run_context': True, 'scope': 'one host; real Dataset driver process replacement'}


def commit_crash_matrix(fx, fault):
    return _crash_and_resume(fx, fx.journal_dir / ('commit-' + str(fault)), fault, task='commit_crash_matrix')


def driver_restart(fx, fault):
    result = _crash_and_resume(fx, fx.journal_dir / 'driver-restart', fault or 'after_prepared_before_publish',
                               task='driver_restart')
    events = result['recovered']['late_events']
    assert events and all(e['error_code'] == 'FENCED' for e in events), events
    assert all(e['old_fence'] != e['current_fence'] for e in events)
    result['two_hosts_network_partition'] = 'not exercised; no multi-host claim'
    return result


MODEL_WINDOWS = ('before_send', 'after_send', 'after_confirm')


@contextmanager
def _http_fixture(root):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    requests, lock = [], threading.Lock()
    log = root / 'http-requests.jsonl'
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers['Content-Length'])
            if not 0 < length <= 128 * 1024:
                raise AssertionError('External fixture request exceeded bound')
            body = json.loads(self.rfile.read(length))
            with lock:
                entry = {'path': self.path, 'body': body, 'index': len(requests)}
                requests.append(entry)
                with log.open('ab') as stream:
                    stream.write(json.dumps(entry).encode() + b'\n')
                    stream.flush()
                    os.fsync(stream.fileno())
            if '/after_send/' in self.path:
                self.close_connection = True
                return  # provider accepted, but no complete HTTP response exists
            if self.path.endswith('/embeddings'):
                reply = {'data': [{'index': i, 'embedding': [1., 0., 0.]}
                                  for i in range(len(body['input']))], 'usage': {'prompt_tokens': 1}}
            else:
                result = {'api_calls': [], 'response': {'result': 'ok'}} if '/agent/' in self.path else {'result': 'ok'}
                reply = {'choices': [{'message': {'content': json.dumps(result)}}],
                         'usage': {'prompt_tokens': 3, 'completion_tokens': 1}}
            encoded = json.dumps(reply).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield requests, 'http://127.0.0.1:' + str(server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def _probe_worker(row):
    """Actual business worker counterexamples against process-wide guards."""
    import os
    import sqlite3
    from demiflow.execution.journal.run_journal import RunJournal
    from demiflow.execution.checkpoint.commit import CommitCoordinator
    checked = {}
    for name, operation in (
        ('sqlite_forbidden', lambda: sqlite3.connect(os.environ['DF_V3_RECOVERY_DB'])),
        ('journal_forbidden', lambda: RunJournal(Path(os.environ['DF_V3_RECOVERY_DB']).parent)),
        ('commit_forbidden', lambda: CommitCoordinator(None)),
    ):
        try:
            operation()
        except PermissionError as exc:
            assert 'V3 worker authority guard' in str(exc)
            checked[name] = True
        else:
            raise AssertionError('Worker opened a forbidden local authority: ' + name)
    return dict(row, **checked, worker_pid=os.getpid())


def _model_pipeline(kind, *, remote=False):
    from demiflow import data
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_runtime_v3_native_execution import PACK
    ds = data.from_items([{'item': 'one'}])
    if remote:
        ds = ds.map(_probe_worker)
    if kind == 'prompt':
        return ds.map_prompt_async('enrich', config=parse_prompt_pack(PACK),
            inputs={'payload': 'item'}, output='answer', concurrency=1, max_requests=1,
            options={'max_response_bytes': 65536, 'max_event_bytes': 65536})
    if kind == 'embedding':
        from demiflow.embeddings import EmbeddingModel
        model = EmbeddingModel('fixture', 'v1', 3, os.environ['DF_V3_NATIVE_TEST_URL'])
        return ds.map_embeddings(model=model, inputs={'text': 'item'}, batch_size=1, concurrency=1, max_requests=1)
    if kind == 'agent':
        from demiflow.agent import AgentConfig
        from demiflow.environment import OperatorEnvironment
        config = AgentConfig(parse_prompt_pack(PACK), OperatorEnvironment(operators=()), {}, 2)
        return ds.agentmap_async('enrich', config=config, inputs={'payload': 'item'}, output='answer')
    raise ValueError(kind)


def _native_calls(journal):
    with journal._connection() as db:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='calls'").fetchone()
        rows = db.execute('SELECT request_key,response_json,error_json FROM calls ORDER BY request_key').fetchall() if exists else []
        amount = db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()
    return {'keys': [r[0] for r in rows], 'confirmed': sum(r[1] is not None for r in rows),
            'unresolved': sum(r[1] is None for r in rows),
            'errors': [json.loads(r[2]) for r in rows if r[2]], 'requests_consumed': amount[0] if amount else 0}


def _run_model_child(config):
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE)
    from demiflow.execution.journal.prompt import RunPromptJournal
    from runtime_v3_support import _ExecuteActionRecorder, _RayDataBlocker
    root, mode, fault = Path(config['root']), config['mode'], config['fault']
    directory = root / 'run'
    recorder, run_holder = _ExecuteActionRecorder(), []
    def stop(boundary):
        run = run_holder[0]
        _write(root / 'boundary.json', {'boundary': boundary, 'pid': os.getpid(),
            'attempt': run.attempt, 'fence': run.fence, 'native': _native_calls(run.journal),
            'facts': _facts(run.journal), 'execute_action_calls': recorder.calls,
            'ray_data_import_attempts': list(blocker.attempts)})
        os.kill(os.getpid(), signal.SIGKILL)
    if mode == 'crash' and fault == 'before_send':
        original = RunPromptJournal.reserve
        def before_reserve(journal, *args, **kwargs):
            stop('native_reserve_before_send')
            return original(journal, *args, **kwargs)
        RunPromptJournal.reserve = before_reserve
    elif mode == 'crash' and fault == 'after_confirm':
        original = RunPromptJournal.response
        def after_response(journal, *args, **kwargs):
            answer = original(journal, *args, **kwargs)
            stop('native_response_confirmed_before_worker_reply')
            return answer
        RunPromptJournal.response = after_response
    rows, error = None, None
    try:
        with _scope(directory) as run, _provider(config['provider']), recorder, _RayDataBlocker() as blocker:
            run_holder.append(run)
            rows = _model_pipeline(config['model'], remote=config['task'] == 'remote_journal').take_all()
    except Exception as exc:
        if fault != 'after_send':
            raise
        error = {'type': type(exc).__name__, 'message': str(exc)}
        if mode == 'resume':
            assert type(exc).__name__ == 'UncertainPromptCall' and 'automatic regeneration is forbidden' in str(exc)
    run = run_holder[0]
    result = {'rows': rows, 'error': error, 'attempt': run.attempt, 'fence': run.fence, 'pid': os.getpid(),
              'native': _native_calls(run.journal), 'facts': _facts(run.journal),
              'execute_action_calls': recorder.calls, 'ray_data_import_attempts': list(blocker.attempts)}
    if fault == 'after_send':
        assert error is not None and result['native']['unresolved'] == 1
    else:
        assert error is None and result['native']['confirmed'] == 1
        assert result['native']['requests_consumed'] == 1
        assert len(rows) == 1 and 'error' not in rows[0]
        if config['model'] == 'embedding':
            assert rows[0]['embedding'] == [1., 0., 0.]
        else:
            assert rows[0]['answer'] == 'ok'
    _write(root / (mode + '-result.json'), result)


def model_effect_recovery(fx, fault):
    windows = (fault,) if fault is not None else MODEL_WINDOWS
    if any(w not in MODEL_WINDOWS for w in windows):
        raise ValueError('Unknown model recovery window')
    results, execute_calls, attempted = [], [], []
    with _http_fixture(fx.evidence_dir) as (requests, url):
        for model in ('prompt', 'embedding', 'agent'):
            for window in windows:
                root = fx.journal_dir / ('model-' + model + '-' + window)
                env = {'DF_V3_NATIVE_TEST_URL': url + '/' + model + '/' + window + '/v1',
                       'DF_V3_NATIVE_TEST_KEY': 'owned-test-only'}
                start = len(requests)
                first = _child(fx, root, task='model_effect_recovery', mode='crash', fault=window,
                               extra={'model': model, 'env': env})
                killed = window != 'after_send'
                assert first['returncode'] == (-signal.SIGKILL if killed else 0), first
                before = _read(root / ('boundary.json' if killed else 'crash-result.json'))
                observed_before = len(requests) - start
                assert observed_before == (0 if window == 'before_send' else 1)
                if window == 'after_confirm':
                    assert before['native']['confirmed'] == 1
                second = _child(fx, root, task='model_effect_recovery', mode='resume', fault=window,
                                extra={'model': model, 'env': env})
                assert second['returncode'] == 0, second
                resumed = _read(root / 'resume-result.json')
                assert len(requests) - start == 1
                assert resumed['native']['requests_consumed'] == 1
                assert resumed['fence'] != before['fence'] and resumed['attempt'] == before['attempt'] + 1
                if window != 'before_send':
                    assert resumed['native']['keys'] == before['native']['keys']
                if window == 'after_send':
                    assert resumed['native']['unresolved'] == 1 and resumed['error']['type'] == 'UncertainPromptCall'
                    assert resumed['facts']['commits'] == []
                else:
                    assert resumed['native']['confirmed'] == 1 and resumed['rows']
                    assert resumed['facts']['commits'][-1]['status'] == 'acked'
                execute_calls.extend(before['execute_action_calls'] + resumed['execute_action_calls'])
                attempted.extend(before['ray_data_import_attempts'] + resumed['ray_data_import_attempts'])
                assert attempted == []
                results.append({'model': model, 'window': window, 'first': first, 'before_resume': before,
                                'resumed': resumed, 'observed_http': requests[start:]})
    return {'transport': 'local_fixture', 'cases': results, 'child_execute_action_calls': execute_calls,
            'child_ray_data_import_attempts': attempted,
            'self_managed_run_context': True, 'scope': 'real PromptActor/Embedding/Agent with loopback external HTTP'}


_WORKER_GUARD = r'''
import os, sys
if os.environ.get('DEMIFLOW_WORKER_LAUNCH') or 'default_worker.py' in sys.argv[0]:
    import json
    def record(kind, **detail):
        row = dict(kind=kind, pid=os.getpid(), **detail)
        fd = os.open(os.environ['DF_V3_RECOVERY_AUDIT'], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, json.dumps(row).encode() + b'\n')
        finally:
            os.close(fd)
    try:
        import sqlite3
        from demiflow.execution.journal.run_journal import RunJournal
        from demiflow.execution.checkpoint.commit import CommitCoordinator
        from demiflow.execution.worker.runtime import WorkerJournalClient
        def reject(kind):
            def denied(*args, **kwargs):
                record(kind)
                raise PermissionError('V3 worker authority guard: ' + kind)
            return denied
        sqlite3.connect = reject('sqlite_denied')
        RunJournal.__init__ = reject('journal_denied')
        CommitCoordinator.__init__ = reject('commit_denied')
        original = WorkerJournalClient._request
        repeated = set()
        def request(self, kind, operation_id, payload):
            value = original(self, kind, operation_id, payload)
            record('rpc', command=kind, operation_id=operation_id, operation=payload.get('operation') or payload.get('kind'))
            retry = (kind == 'native_request' and payload.get('operation') == 'reserve'
                     or kind == 'artifact' and payload.get('kind') == 'settle_staging')
            if retry and operation_id not in repeated:
                repeated.add(operation_id)
                value = original(self, kind, operation_id, payload)
                record('lost_reply_retried', command=kind, operation_id=operation_id)
            return value
        WorkerJournalClient._request = request
        record('guard_installed')
    except BaseException as error:
        record('guard_install_failed', error=repr(error))
        os._exit(86)
'''


def remote_journal(fx, fault):
    if fault not in (None, 'lost_reply'):
        raise ValueError('Unknown remote-journal fault')
    root = fx.journal_dir / 'remote-journal'
    hooks = root / 'worker-hooks'
    hooks.mkdir(parents=True)
    (hooks / 'sitecustomize.py').write_text(_WORKER_GUARD)
    audit = root / 'worker-audit.jsonl'
    with _http_fixture(fx.evidence_dir) as (requests, url):
        env = {'DF_V3_NATIVE_TEST_URL': url + '/prompt/confirmed/v1',
               'DF_V3_NATIVE_TEST_KEY': 'owned-test-only', 'DF_V3_RECOVERY_AUDIT': str(audit),
               'DF_V3_RECOVERY_DB': str(root / 'run' / 'run.sqlite'),
               'PYTHONPATH': str(hooks) + ':' + str(WORKTREE) + ':' + str(WORKTREE / 'tests')}
        child = _child(fx, root, task='remote_journal', mode='run',
                       extra={'model': 'prompt', 'env': env})
        assert child['returncode'] == 0, child
        result = _read(root / 'run-result.json')
        records = [json.loads(line) for line in audit.read_text().splitlines()]
        installed = {r['pid'] for r in records if r['kind'] == 'guard_installed'}
        assert installed and child['pid'] not in installed
        assert result['rows'][0]['worker_pid'] in installed
        for guard in ('sqlite', 'journal', 'commit'):
            assert result['rows'][0][guard + '_forbidden'] is True
            assert any(r['kind'] == guard + '_denied' for r in records)
        rpc = [r for r in records if r['kind'] == 'rpc']
        assert {'native_request', 'artifact', 'check_grant'} <= {r['command'] for r in rpc}
        retries = [r for r in records if r['kind'] == 'lost_reply_retried']
        assert any(r['command'] == 'native_request' for r in retries)
        assert any(r['command'] == 'artifact' for r in retries)
        assert len(requests) == result['native']['confirmed'] == result['native']['requests_consumed'] == 1
        assert list(root.glob('**/*.sqlite')) == [root / 'run' / 'run.sqlite']
        assert result['facts']['commits'][-1]['status'] == 'acked'
        assert result['ray_data_import_attempts'] == []
    return {'worker_sqlite': 'forbidden', 'worker_pids': sorted(installed), 'driver_pid': child['pid'],
            'audit': records, 'native': result['native'], 'rows': result['rows'],
            'facts': result['facts'], 'child_execute_action_calls': result['execute_action_calls'],
            'self_managed_run_context': True, 'child_ray_data_import_attempts': result['ray_data_import_attempts'],
            'isolation_scope': 'worker-process SQLite/RunJournal/CommitCoordinator guards; not OS namespace isolation'}


if __name__ == '__main__':
    _run_child(_read(Path(sys.argv[1])))
