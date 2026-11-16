"""I09/V022 real business composition; only the external HTTP transport is fake."""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
PACK = '''
schema_version: demiflow_prompt_pack_v2
prompts:
  review:
    version: queue-business-v1
    model:
      name: local-contract-model
      transport: openai_compatible
      base_url_env: DEMIFLOW_QUEUE_TEST_URL
      api_key_env: DEMIFLOW_QUEUE_TEST_KEY
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [result]
      properties:
        result: {type: string}
    template: |
      QUEUE_PAYLOAD={{ payload | json }}
      QUEUE_PAYLOAD_END
'''


def source_evidence():
    files = sorted((ROOT/'demiflow').rglob('*.py'))
    files += [Path(__file__), Path(__file__).with_name('runtime_v3_queue_boundary_scenarios.py'),
              Path(__file__).with_name('runtime_v3_support.py')]
    assert len(files) <= 10000 and all(path.stat().st_size <= 2 * 1024**2 for path in files)
    return {'head': subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
            'dirty': subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True).strip(),
            'inputs': {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}}


def dependency_provenance():
    import importlib
    import psutil
    modules = {}
    for name in ('demiflow', 'ray', 'pyarrow', 'lance', 'babel', 'jinja2', 'aiohttp', 'httpx'):
        module = importlib.import_module(name)
        modules[name] = {'path': module.__file__, 'version': getattr(module, '__version__', None)}
    assert Path(modules['demiflow']['path']).is_relative_to(ROOT)
    return {'pid': os.getpid(), 'birth': psutil.Process().create_time(), 'modules': modules,
            'sys_path': sys.path}


def _body(key):
    return ('Evidence for ' + key + '.\n\nFull qualifying context.').encode()


@contextmanager
def external_transport():
    calls, model_calls, lock = Counter(), [], threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *args):
            pass
        def send(self, body, status=200):
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('ETag', '"queue-business-v1"')
            self.end_headers()
            self.wfile.write(body)
        def do_GET(self):
            parsed = urlsplit(self.path)
            with lock: calls[self.path] += 1
            if parsed.path == '/search':
                query = parse_qs(parsed.query)['q'][0]
                value = {'results': [{'url': base + '/objects/' + query + '-' + str(index),
                    'title': query + '-' + str(index), 'content': 'acquire and verify'} for index in range(2)]}
                self.send(json.dumps(value).encode())
            elif parsed.path.startswith('/objects/'):
                key = parsed.path.rsplit('/', 1)[1]
                self.send(b'' if key == 'pine-1' else _body(key), 404 if key == 'pine-1' else 200)
            else:
                self.send(b'{}', 404)
        def do_POST(self):
            length = int(self.headers.get('Content-Length', 0))
            assert 0 < length <= 256 * 1024
            body = json.loads(self.rfile.read(length))
            content = body['messages'][1]['content']
            if isinstance(content, list): content = ''.join(part.get('text', '') for part in content)
            payload = json.loads(content.split('QUEUE_PAYLOAD=', 1)[1].split('QUEUE_PAYLOAD_END', 1)[0])
            with lock:
                first = sum(call['item_id'] == payload['item_id'] for call in model_calls) % 2 == 0
            if first:
                answer = {'api_calls': [{'method': 'read_documents', 'arguments': {'request': {
                    'documents': [payload['resource']], 'questions': [{'id': 'q', 'text': 'qualifying'}],
                    'retained': [], 'requests': [], 'new_chars': 6000, 'total_chars': 6000}}}], 'response': None}
            else:
                assert 'Full qualifying context.' in json.dumps(body['messages'])
                answer = {'api_calls': [], 'response': {'result': 'reviewed:' + payload['item_id']}}
            with lock:
                assert len(model_calls) < 128
                model_calls.append({'item_id': payload['item_id'], 'turn': 1 if first else 2})
            self.send(json.dumps({'choices': [{'message': {'content': json.dumps(answer)}}],
                'usage': {'prompt_tokens': 10, 'completion_tokens': 2}}).encode())
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    base = f'http://127.0.0.1:{server.server_port}'
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    old = {key: os.environ.get(key) for key in ('DEMIFLOW_QUEUE_TEST_URL', 'DEMIFLOW_QUEUE_TEST_KEY')}
    os.environ.update(DEMIFLOW_QUEUE_TEST_URL=base + '/v1', DEMIFLOW_QUEUE_TEST_KEY='owned-test-key')
    try:
        yield base, calls, model_calls
    finally:
        server.shutdown(); server.server_close(); thread.join(5)
        assert not thread.is_alive()
        for key, value in old.items():
            if value is None: os.environ.pop(key, None)
            else: os.environ[key] = value


def _messages(row):
    from demiflow.execution.queue import task_message
    for result in row['searched']:
        assert result['status'] == 'ok'
        for candidate in result['candidates']:
            key = candidate['url'].rsplit('/', 1)[1]
            request = {'uri': candidate['url']}
            if key != 'pine-1':
                request.update(expected_size=len(_body(key)), expected_sha256=hashlib.sha256(_body(key)).hexdigest())
            yield {'messages': [task_message(key, {'key': key, 'request': request}, pool='p')]}


def _verified_annotation(row):
    assert row['result']['terminal'] is True
    if row['result']['status'] == 'ok':
        assert row['result']['checks']['sha256_verified'] and row['result']['checks']['size_verified']
    return {**row, 'business_accepted': row['result']['status'] == 'ok'}


def _expand_history(row, *, directory):
    from demiflow.collect.documents import store_document
    from demiflow.objects import ObjectRef
    assert row['verification']['status'] == 'verified'
    assert row['queue_record']['proof']['settled'] is True
    if row['result']['status'] != 'ok': return
    raw = ObjectRef(**row['result']['raw_ref']).read(max_bytes=4096)
    assert raw == _body(row['key'])
    document = store_document(Path(directory), raw, url=row['request']['uri'],
        final_url=row['request']['uri'], content_type='text/plain', retrieved_at='2026-11-16T00:00:00Z')
    resource = {'document_ref': document, 'url': row['request']['uri'], 'bindings': [], 'eligible': True}
    for part in range(2):
        item_id = row['key'] + ':' + str(part)
        yield {'item_id': item_id, 'source_sha256': row['result']['raw_ref']['sha256'],
               'item': {'item_id': item_id, 'resource': resource}, 'resources': {'D1': resource}}


def _model_result(row):
    assert 'error' not in row, row.get('error')
    assert row['answer'] == 'reviewed:' + row['item_id']
    assert len(row['call']['environment']['observations']) == 1
    return {'item_id': row['item_id'], 'answer': row['answer'],
            'source_sha256': row['source_sha256'], 'tool_observations': 1}


def _postprocess(row):
    return {**row, 'accepted': row['answer'] == 'reviewed:' + row['item_id']}


def _producer(channel, producer, root, base):
    from demiflow import data
    from demiflow.collect import SearchConfig
    from demiflow.collect.session import WebSession
    engine = {'name': 'owned', 'module': 'adapter', 'revision': 'fixture-v1',
              'base_url': base + '/search', 'enable_http': True}
    session = WebSession(cache_path=root/'search-cache.sqlite', object_directory=root/'search-objects',
        search=SearchConfig(engines=(engine,), language='en', host_interval_s=0,
            workers=1, request_concurrency=1, max_bytes=65536, max_results=8))
    return (data.from_items([{'queries': [{'query': query, 'request_id': query}]} for query in ('oak', 'pine')])
        .search_web(requests='queries', output='searched', session=session, max_candidates=2,
                    concurrency=1, queue_depth=1, request_concurrency=1)
        .flat_map(_messages).enqueue(channel.config, producer=producer).run_stream())


def _consumer(channel, root, *, explicit=False):
    from demiflow import data
    from demiflow.collect import FetchPolicy, HttpSource
    from demiflow.execution.queue_sinks import QueueResultSink
    from demiflow.objects import LocalObjectStore
    # A restart must wait out an unfinished native lease; it may not revoke a
    # live epoch merely because this Dataset cut was confirmed.
    ds = (data.read_queue(channel.config, pool='p', max_tasks=4, max_inflight=1, idle_timeout_s=35, lease_s=30,
        settlement='explicit' if explicit else 'sink')
        .fetch(store=LocalObjectStore(root/'raw'), policy=FetchPolicy(max_object_bytes=65536,
            max_attempts=1, http=HttpSource(allow_private=True)), concurrency=1)
        .map(_verified_annotation))
    if explicit: return ds.ack_queue(channel.config, keep_row=False).run_stream()
    return ds.write_datasink(QueueResultSink(channel.config, pool='p'))


class AfterNonfinalAck(RuntimeError):
    pass


def _one_business(root, base, calls, model_calls, *, explicit=False, recover=False):
    import pytest
    from demiflow import data
    from demiflow.agent import AgentConfig
    from demiflow.environment import OperatorEnvironment
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.execution.queue import QueueConfig, QueueChannel, ProducerSourceSpec
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.dataflow import DataflowExecution
    root.mkdir(parents=True)
    channel = QueueChannel(QueueConfig(backend='sqlite', queue_id='full-business', path=str(root/'queue.sqlite')))
    producer = channel.open('p', producer_id='search-producer', source=ProducerSourceSpec(
        kind='keywords', source_id='fixed-keywords', uri='memory:oak-pine', version=1))
    before_gets, before_models = dict(calls), len(model_calls)
    with RunContext.start(root/'producer', limits={'timeout_s': 90}) as run:
        stats = _producer(channel, producer, root, base)
    assert stats.emitted == 4
    assert channel.seal(producer)['actual_tasks'] == 4
    interrupted = []
    advance = DataflowExecution._advance_cut
    def observed(execution):
        result = advance(execution)
        image = execution._restore_image
        if image and not image.cut.final and not interrupted:
            with channel.database.connection() as db:
                settled = db.execute('SELECT count(*) FROM queue_completions').fetchone()[0]
            if 0 < settled < 4:
                fact = execution._action.context.journal.execution_commit(image.commit_id)
                assert fact['status'] == 'acked'
                interrupted.append({'commit_id': image.commit_id, 'settled': settled, 'final': image.cut.final,
                                    'plan_id': execution._physical.plan_id})
                raise AfterNonfinalAck('interrupt after real nonfinal Dataset ACK and native task settlement')
        return result
    limits = {'checkpoint_rows': 1, 'checkpoint_interval_ms': 60000, 'timeout_s': 120}
    if recover:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(DataflowExecution, '_advance_cut', observed)
            with pytest.raises(AfterNonfinalAck):
                with RunContext.start(root/'consumer', limits=limits) as run:
                    reference = run.ref
                    _consumer(channel, root, explicit=explicit)
        assert interrupted
        before = run.journal.execution_commits('action-1')
        sealed = run.journal.source_binding('action-1-dataflow-sealed')
        with RunContext.resume(reference) as run:
            _consumer(channel, root, explicit=explicit)
            assert run.journal.execution_commits('action-1')[:len(before)] == before
            assert run.journal.source_binding('action-1-dataflow-sealed') == sealed
    else:
        with RunContext.start(root/'consumer', limits=limits) as run:
            _consumer(channel, root, explicit=explicit)
    assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    state = channel.state('p')
    assert state['queue_drained'] and state['counts'] == {'done_ok': 3, 'done_failed': 1}, state
    snapshot = channel.snapshot('p')
    native = []
    for key in ('oak-0', 'oak-1', 'pine-0', 'pine-1'):
        proof = channel.completion(producer.generation, key)
        assert proof['settled']
        with channel.database.connection() as db:
            native.append({'task_id': key, 'final_commit': proof['final_commit'],
                'status': db.execute('SELECT status FROM runtime_commits WHERE commit_id=?', (proof['final_commit'],)).fetchone()[0]})
        assert native[-1]['status'] == 'acked'
        assert calls['/objects/' + key] - before_gets.get('/objects/' + key, 0) == 1
    with RunContext.start(root/'history-model', limits={'timeout_s': 120, 'max_requests': 12}) as run:
        history = data.read_queue_records(channel.config, pool='p', through=snapshot, results=True)
        config = AgentConfig(parse_prompt_pack(PACK), OperatorEnvironment(resources='resources'), {}, 12)
        transformed = (history.flat_map(_expand_history, fn_kwargs={'directory': str(root/'documents')})
            .agentmap_async('review', config=config, inputs={'payload': 'item'}, output='answer',
                call_output='call', error_output='error', concurrency=1, queue_depth=1)
            .map(_model_result))
        target = root/'reviewed.lance'
        transformed.write_lance(str(target), mode='create')
        assert run.journal.budget_consumed('requests') == 12
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    with RunContext.start(root/'postprocess'):
        rows = data.read_lance(str(target), version=1).map(_postprocess).take_all()
    expected = sorted(key + ':' + str(part) for key in ('oak-0', 'oak-1', 'pine-0') for part in range(2))
    assert sorted(row['item_id'] for row in rows) == expected and all(row['accepted'] for row in rows)
    assert len(model_calls) - before_models == 12
    assert Counter((call['item_id'], call['turn']) for call in model_calls[before_models:]) == Counter(
        (key, turn) for key in expected for turn in (1, 2))
    return {'rows': rows, 'native_settlements': native, 'snapshot': snapshot.__dict__,
            'nonfinal_ack_interruption': interrupted, 'model_requests': 12, 'explicit': explicit}


def _unknown_search(root, base, calls):
    import pytest
    from demiflow.execution.queue import QueueConfig, QueueChannel, ProducerSourceSpec
    from demiflow.execution.run_context import RunContext
    from demiflow.collect.search_contract import NativeSearchResultSink
    from demiflow.execution.recovery_contract import UnresolvedCommit
    root.mkdir(parents=True)
    channel = QueueChannel(QueueConfig(backend='sqlite', queue_id='unknown', path=str(root/'queue.sqlite')))
    producer = channel.open('p', producer_id='search', source=ProducerSourceSpec(
        kind='keywords', source_id='fixed-keywords', uri='memory:oak-pine', version=1))
    before = sum(calls.values())
    def interrupted(*args, **kwargs):
        raise RuntimeError('HTTP returned but native result not fixed')
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(NativeSearchResultSink, 'prepare', interrupted)
        with pytest.raises(RuntimeError, match='native result not fixed'):
            with RunContext.start(root/'first'):
                _producer(channel, producer, root, base)
    assert sum(calls.values()) == before + 1 and channel.state('p')['tasks'] == 0
    (root/'search-cache.sqlite').unlink()
    with pytest.raises(UnresolvedCommit):
        with RunContext.start(root/'replacement'):
            _producer(channel, producer, root, base)
    assert sum(calls.values()) == before + 1 and channel.state('p')['tasks'] == 0
    return {'http_calls': 1, 'second_attempt_calls': 0, 'tasks_published': 0, 'outcome': 'unknown_no_replay'}


def run_business(root, provider, *, cases=('normal', 'recovery', 'explicit', 'unknown', 'boundaries'), workers=4):
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from runtime_v3_support import _ExecuteActionRecorder, _RayDataBlocker, assert_tree_binding
    assert os.environ['DEMIFLOW_RESOURCE_ROOT'] == str(root/'capacity')
    assert_tree_binding()
    assert str(root).startswith('/tmp/demiflow-')
    fixture_path = str(ROOT/'tests/native_search_fixtures')
    sys.path.insert(0, fixture_path)
    report = {'cases': {}, 'provider': provider, 'workers': workers, 'passed': False,
              'interpreter': sys.executable, 'argv': sys.argv, 'resource_root': os.environ['DEMIFLOW_RESOURCE_ROOT'],
              'source_before': source_evidence()}
    executor = (LocalDatasetExecutor(workers=workers, resource_root=root) if provider == 'local'
                else RayDatasetExecutor(workers=workers, resource_root=root))
    try:
        with _use_executor(executor), _RayDataBlocker() as blocker, _ExecuteActionRecorder() as recorder:
            with external_transport() as (base, calls, model_calls):
                for case in cases:
                    if case == 'boundaries':
                        from runtime_v3_queue_boundary_scenarios import run_boundaries
                        report['cases'][case] = run_boundaries(root/case, base, calls)
                    elif case == 'unknown':
                        report['cases'][case] = _unknown_search(root/case, base, calls)
                    else:
                        report['cases'][case] = _one_business(root/case, base, calls, model_calls,
                            explicit=case == 'explicit', recover=case == 'recovery')
                report['transport_calls'] = dict(calls)
                report['model_calls'] = model_calls
            report['child_execute_action_calls'] = recorder.calls
            report['child_ray_data_import_attempts'] = blocker.attempts
            assert recorder.calls and not blocker.attempts
        report['passed'] = True
    except BaseException:
        report['error'] = traceback.format_exc()
        raise
    finally:
        executor.close()
        report['module_paths'] = assert_tree_binding()
        report['source_after'] = source_evidence()
        if report['source_before'] != report['source_after']:
            report['passed'] = False
            report['error'] = 'Source bytes changed during this run'
        (root/'business.json').write_text(json.dumps(report, indent=2, default=str))
        sys.path.remove(fixture_path)
        assert report['source_before'] == report['source_after'], 'source changed during verification'
    return report


def queue_full_business(fx, fault=None):
    if fault is not None: raise ValueError('queue business includes explicit nonfinal ACK and unknown-effect cases')
    # The outer generic fixture removes its own directory. Keep the original
    # journals/receipts in this separately owned /tmp evidence domain.
    requested = os.environ.get('DEMIFLOW_V3_QUEUE_OUTPUT_ROOT')
    if requested:
        root = Path(requested)
        assert root.parent == Path('/tmp') and root.name.startswith('demiflow-')
        root.mkdir()
    else:
        root = Path(tempfile.mkdtemp(prefix='demiflow-queue-business-'))
    config = {'root': str(root), 'provider': fx.provider}
    path = root/'config.json'; path.write_text(json.dumps(config))
    env = dict(os.environ, PYTHONPATH=str(ROOT)+':'+str(ROOT/'tests'), PYTHONDONTWRITEBYTECODE='1',
               DEMIFLOW_RESOURCE_ROOT=str(root/'capacity'))
    fallback = os.environ.get('DEMIFLOW_V3_QUEUE_FALLBACK_SITE')
    if fallback:
        # No install or shared environment edit. sitecustomize runs in the driver
        # and inherited worker environment, after the interpreter's own packages.
        directory = Path(fallback).resolve()
        assert directory.is_dir() and directory.name == 'site-packages'
        bootstrap = root/'dependency-bootstrap'; bootstrap.mkdir()
        (bootstrap/'sitecustomize.py').write_text('import sys\nsys.path.append(' + repr(str(directory)) + ')\n')
        env['PYTHONPATH'] += os.pathsep + str(bootstrap)
    command = [sys.executable, '-B', '-m', 'runtime_v3_queue_business_scenarios', str(path)]
    with (root/'child.log').open('w') as log:
        process = subprocess.Popen(command, env=env, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            process.wait(timeout=480)
        except subprocess.TimeoutExpired:
            process.send_signal(signal.SIGINT)  # original Dataset/Ray cleanup first
            try:
                process.wait(timeout=40)
            except subprocess.TimeoutExpired:
                import psutil
                owned = psutil.Process(process.pid).children(recursive=True)
                for child in owned:
                    try: child.kill()
                    except psutil.NoSuchProcess: pass
                process.kill(); process.wait(timeout=10)
                psutil.wait_procs(owned, timeout=10)
            raise TimeoutError('queue business deadline; original evidence retained at ' + str(root))
    report = json.loads((root/'business.json').read_text()) if (root/'business.json').exists() else {}
    assert process.returncode == 0 and report.get('passed'), report.get('error') or (root/'child.log').read_text()[-12000:]
    report['evidence_directory'] = str(root)
    return report


def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    root = Path(config['root']); root.mkdir(parents=True, exist_ok=True)
    assert os.environ['DEMIFLOW_RESOURCE_ROOT'] == str(root/'capacity')
    ray = None
    try:
        if config['provider'] == 'ray_core':
            import secrets
            os.environ['RAY_AUTH_MODE'] = 'token'; os.environ['RAY_AUTH_TOKEN'] = secrets.token_hex(32)
            # Ray's native module caches auth mode at import time.
            import ray
            ray.init(address='local', num_cpus=config.get('workers', 4), num_gpus=0, include_dashboard=False,
                object_store_memory=128*1024**2, _temp_dir='/tmp/demiflow-qb-'+secrets.token_hex(5))
            driver = dependency_provenance()
            worker = ray.get(ray.remote(max_retries=0)(dependency_provenance).remote(), timeout=30)
            for name in driver['modules']:
                assert driver['modules'][name] == worker['modules'][name], (name, driver, worker)
            assert worker['pid'] != driver['pid']
            (root/'dependency-provenance.json').write_text(json.dumps({'driver': driver, 'ray_worker': worker}, indent=2))
        run_business(root, config['provider'], cases=tuple(config.get('cases', [
            'normal', 'recovery', 'explicit', 'unknown', 'boundaries'])), workers=config.get('workers', 4))
    finally:
        if ray is not None: ray.shutdown()
        import psutil
        process = psutil.Process()
        deadline = time.monotonic() + 10
        children = process.children(recursive=True)
        while children and time.monotonic() < deadline:
            time.sleep(.05)
            children = process.children(recursive=True)
        allocations = list((root/'capacity').glob('capacity/allocations/*.json'))
        assert len(allocations) <= 4096
        records = [json.loads(path.read_text()) for path in allocations]
        active = [record['allocation_id'] for record in records if record['state'] != 'RELEASED']
        cleanup = {'children': [{'pid': child.pid, 'birth': child.create_time()} for child in children],
                   'allocations': len(records), 'active_allocations': active}
        (root/'cleanup.json').write_text(json.dumps(cleanup, indent=2))
        assert not children and not active, cleanup


if __name__ == '__main__':
    main()
