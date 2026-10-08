"""Native operator resources: real HTTP child processes, no GPU/model dependency."""
import asyncio
import os
from pathlib import Path
import socket
import sys

import pytest

from demiflow import data
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.services import VLLMService, ModelServiceError, vllm_config
from demiflow.services import vllm


@pytest.fixture
def managed(tmp_path, monkeypatch):
    # One real child serves readiness and deterministic chat replies. The test
    # only replaces argv; native locks/readiness/journaling/cleanup still run.
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    script = tmp_path / 'server.py'
    script.write_text('''import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b'{"data": [{"id": "fixture"}]}')
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.send_response(200); self.end_headers()
        self.wfile.write(json.dumps({'choices':[{'message':{'content':'{"result":"ok"}'},'finish_reason':'stop'}]}).encode())
    def log_message(self,*args): pass
ThreadingHTTPServer(('127.0.0.1', ''' + str(port) + '''), Handler).serve_forever()
''')
    monkeypatch.setattr(vllm, '_service_command', lambda *_: ([sys.executable, str(script)], os.environ.copy()))
    real_popen, processes = vllm.subprocess.Popen, []
    def popen(*args, **kwargs):
        p = real_popen(*args, **kwargs)
        processes.append(p)
        return p
    monkeypatch.setattr(vllm.subprocess, 'Popen', popen)
    monkeypatch.setenv('FIXTURE_KEY', 'test')
    pack = parse_prompt_pack('''schema_version: demiflow_prompt_pack_v2
prompts:
  test:
    version: v1
    model:
      name: fixture
      transport: openai_compatible
      base_url: http://127.0.0.1:''' + str(port) + '''/v1
      api_key_env: FIXTURE_KEY
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [result]
      properties:
        result: {type: string}
    template: '{{ value | json }}'
''')
    logs = []
    spec = VLLMService({'model_path': str(tmp_path), 'startup_timeout_s': 5,
                        'poll_interval_s': .01, 'shutdown_timeout_s': .1},
                       root=tmp_path, log_path=tmp_path / 'service.log', log=logs.append)
    def node(rows=None, **kwargs):
        return data.from_items(rows if rows is not None else [{'value': i} for i in range(8)]).map_prompt_async(
            'test', config=pack, inputs={'value': 'value'}, output='result',
            service=spec, concurrency=4, **kwargs)
    yield spec, node, processes, logs, port, pack
    # Even assertion failures must not leave test children behind.
    for p in processes:
        if p.poll() is None:
            import signal
            os.killpg(p.pid, signal.SIGKILL)
            p.wait()


def assert_reaped(processes):
    assert processes
    for p in processes:
        assert p.poll() is not None
        with pytest.raises(ProcessLookupError):
            os.kill(p.pid, 0)


def test_config_and_native_command(tmp_path):
    incoming = {'model_path': 'models/qwen', 'gpus': [1, 0], 'data_parallel_size': 2}
    spec = vllm_config(incoming)
    incoming['gpus'].clear()
    argv, env = vllm._service_command(spec, {'name': 'qwen', 'base_url': 'http://127.0.0.1:18463/v1'}, tmp_path)
    assert argv[argv.index('--data-parallel-size') + 1] == '2'
    assert argv[argv.index('--tensor-parallel-size') + 1] == '1'
    assert argv[argv.index('--port') + 1] == '18463'
    assert str(tmp_path / 'models/qwen') in argv
    assert env['CUDA_VISIBLE_DEVICES'] == '1,0'
    assert '--enforce-eager' not in argv
    assert '--enable-logging-iteration-details' not in argv
    observed = vllm_config({'model_path': 'models/qwen', 'enable_logging_iteration_details': True})
    observed_argv, _ = vllm._service_command(observed, {'name': 'qwen', 'base_url': 'http://127.0.0.1:18463/v1'}, tmp_path)
    assert '--enable-logging-iteration-details' in observed_argv
    assert vllm_config(None) is None
    for value in ({'model_path': 'qwen', 'gpus': [0, 1]}, {'model_path': 'qwen', 'gpus': [0, 0]},
                  {'model_path': 'qwen', 'unknown': 1}, {'model_path': 'qwen', 'startup_timeout_s': 0},
                  {'model_path': 'qwen', 'readiness_timeout_s': float('nan')},
                  {'model_path': 'qwen', 'resource_wait_timeout_s': -1},
                  {'model_path': 'qwen', 'resource_wait_timeout_s': float('inf')},
                  {'model_path': 'qwen', 'resource_wait_timeout_s': True},
                  {'model_path': 'qwen', 'enable_logging_iteration_details': 1}):
        with pytest.raises(ValueError):
            vllm_config(value)
    for url in ('https://127.0.0.1:8000/v1', 'http://example.com:8000/v1',
                'http://127.0.0.1/v1', 'http://user:pass@127.0.0.1:8000/v1'):
        with pytest.raises(ValueError):
            vllm._service_command(spec, {'name': 'qwen', 'base_url': url}, tmp_path)


def test_lazy_concurrent_start_replay_and_repeat_action(managed, tmp_path):
    spec, node, processes, logs, port, pack = managed
    plan = node(options={'journal_dir': str(tmp_path / 'calls')})
    assert not processes and not (tmp_path / '_demiflow').exists()
    assert plan.materialize().count() == 8
    assert len(processes) == 1 and sum('模型就绪' in msg for msg in logs) == 1
    assert_reaped(processes)
    # Both the same actor and a new node reuse full responses without loading.
    assert plan.materialize().count() == 8
    spec._config['gpus'] = [7]  # Placement is excluded from request identity.
    assert node(options={'journal_dir': str(tmp_path / 'calls')}, max_requests=0).materialize().count() == 8
    assert len(processes) == 1
    # A new event loop on the original plan gets a fresh execution owner.
    fresh = node([{'value': 'new'}])
    fresh.materialize(); fresh.materialize()
    assert len(processes) == 3
    assert_reaped(processes)


def test_empty_skipped_and_budget_exhausted_do_not_start(managed):
    _, node, processes, _, _, _ = managed
    assert node([]).materialize().count() == 0
    assert node(when=lambda row: False).materialize().count() == 8
    rows = node(max_requests=0, error_output='error').materialize().take_all()
    assert all(row['error']['type'] == 'PromptBudgetExceededError' for row in rows)
    assert not processes


@pytest.mark.parametrize('failure', ['exit', 'timeout', 'occupied'])
def test_start_failure_aborts_even_with_error_column(managed, monkeypatch, failure):
    _, node, processes, _, port, _ = managed
    listener = None
    if failure == 'occupied':
        listener = socket.socket(); listener.bind(('127.0.0.1', port)); listener.listen()
        pattern = 'occupied'
    else:
        command = 'raise SystemExit(3)' if failure == 'exit' else 'import time; time.sleep(60)'
        monkeypatch.setattr(vllm, '_service_command', lambda *_: ([sys.executable, '-c', command], os.environ.copy()))
        managed[0]._config['startup_timeout_s'] = .15
        pattern = 'exited' if failure == 'exit' else 'timed out'
    try:
        with pytest.raises(ModelServiceError, match=pattern):
            node(error_output='error').materialize()
    finally:
        if listener: listener.close()
    assert len(processes) == (0 if failure == 'occupied' else 1)
    if processes: assert_reaped(processes)


def test_downstream_failure_releases_and_logging_cannot_prevent_cleanup(managed):
    spec, node, processes, _, _, _ = managed
    spec.log = lambda _: (_ for _ in ()).throw(RuntimeError('bad logger'))
    def fail(row): raise RuntimeError('downstream fixture')
    with pytest.raises(RuntimeError, match='downstream fixture'):
        node().map(fail).materialize()
    assert_reaped(processes)


@pytest.mark.parametrize('during_start', [True, False])
def test_cancel_owner_cleans_process_and_releases_gpu_locks(managed, monkeypatch, during_start):
    spec, _, processes, _, _, pack = managed
    if during_start:
        monkeypatch.setattr(vllm, '_service_command', lambda *_: ([sys.executable, '-c', 'import time; time.sleep(60)'], os.environ.copy()))
    async def run():
        owner = spec.bind(pack.prompt_definitions['test'].model)
        row = asyncio.create_task(owner.ensure_ready())
        if during_start:
            while not processes: await asyncio.sleep(.01)
        else:
            await row
        row.cancel()
        try: await row
        except asyncio.CancelledError: pass
        await owner.aclose()
        await owner.aclose()
        import fcntl
        with (spec.root / '_demiflow/local_model_gpus/gpu_0.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    asyncio.run(run())
    assert_reaped(processes)


def test_managed_offline_is_rejected_at_declaration(managed, tmp_path):
    _, node, processes, _, _, _ = managed
    with pytest.raises(ValueError, match='online HTTP'):
        node(options={'offline_dir': str(tmp_path / 'offline')})
    assert not processes


def test_gpu_conflict_does_not_disturb_running_owner(managed):
    spec, _, processes, _, _, pack = managed
    async def run():
        model = pack.prompt_definitions['test'].model
        first, second = spec.bind(model), spec.bind(model)
        try:
            await first.ensure_ready()
            with pytest.raises(ModelServiceError, match='gpu_0 is occupied'):
                await second.ensure_ready()
            await second.aclose()
            assert len(processes) == 1 and processes[0].poll() is None
            await first.ensure_ready()
        finally:
            await first.aclose()
    asyncio.run(run())
    assert_reaped(processes)


@pytest.mark.parametrize('resource', ['gpu', 'port'])
def test_resource_wait_starts_only_after_release(managed, resource):
    import fcntl
    spec, _, processes, logs, port, pack = managed
    spec._config['resource_wait_timeout_s'] = 3
    control = spec.root / '_demiflow/local_model_gpus'
    control.mkdir(parents=True)
    if resource == 'gpu':
        occupied = (control / 'gpu_0.lock').open('a')
        fcntl.flock(occupied, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        occupied = socket.socket()
        occupied.bind(('127.0.0.1', port)); occupied.listen()

    async def run():
        owner = spec.bind(pack.prompt_definitions['test'].model)
        pending = asyncio.create_task(owner.ensure_ready())
        try:
            async with asyncio.timeout(2):
                while not logs: await asyncio.sleep(.01)
            assert any('等待模型资源' in msg for msg in logs)
            assert not processes and not pending.done()
            # A busy listener must not cause the waiting job to hold a GPU.
            if resource == 'port':
                with (control / 'gpu_0.lock').open('a') as probe:
                    fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            occupied.close()
            await pending
            assert len(processes) == 1 and processes[0].poll() is None
        finally:
            occupied.close()
            await owner.aclose()
    asyncio.run(run())
    assert_reaped(processes)


@pytest.mark.parametrize('outcome', ['timeout', 'cancel'])
def test_wait_timeout_cancel_release_partial_locks_without_touching_owner(managed, outcome):
    import fcntl
    spec, _, processes, logs, _, pack = managed
    spec._config.update(gpus=[0, 1], data_parallel_size=2,
                        resource_wait_timeout_s=.1 if outcome == 'timeout' else 3)
    control = spec.root / '_demiflow/local_model_gpus'
    control.mkdir(parents=True)
    with (control / 'gpu_1.lock').open('a') as occupied:
        fcntl.flock(occupied, fcntl.LOCK_EX | fcntl.LOCK_NB)
        async def run():
            owner = spec.bind(pack.prompt_definitions['test'].model)
            pending = asyncio.create_task(owner.ensure_ready())
            try:
                async with asyncio.timeout(2):
                    while not logs: await asyncio.sleep(.01)
                with (control / 'gpu_0.lock').open('a') as probe:
                    fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
                if outcome == 'timeout':
                    with pytest.raises(ModelServiceError, match='resource wait timed out'):
                        await pending
                else:
                    await owner.aclose()
                    with pytest.raises(asyncio.CancelledError):
                        await pending
            finally:
                await owner.aclose()
        asyncio.run(run())
        assert not processes
        with (control / 'gpu_1.lock').open('a') as probe:
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    for gpu in (0, 1):
        with (control / f'gpu_{gpu}.lock').open('a') as probe:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_prompt_actor_cancellation_closes_pending_startup(managed, monkeypatch):
    _, node, processes, _, _, _ = managed
    monkeypatch.setattr(vllm, '_service_command', lambda *_: (
        [sys.executable, '-c', 'import time; time.sleep(60)'], os.environ.copy()))
    actor = node()._stages[-1]
    async def run():
        task = asyncio.create_task(actor({'value': 1}))
        while not processes:
            await asyncio.sleep(.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await actor.aclose()
        assert actor._runtime._clients == {}
        assert actor._runtime.coordinator.usage().requests_failed == 1
    asyncio.run(run())
    assert_reaped(processes)


def test_multimodal_limit_command_and_validation(tmp_path):
    import json
    incoming = {'model_path': 'models/vision', 'limit_mm_per_prompt': {'image': 4}}
    spec = vllm_config(incoming)
    incoming['limit_mm_per_prompt']['image'] = 99
    argv, _ = vllm._service_command(spec, {'name': 'vision', 'base_url': 'http://127.0.0.1:8000/v1'}, tmp_path)
    assert json.loads(argv[argv.index('--limit-mm-per-prompt') + 1]) == {'image': 4}
    for value in ({}, {'image': -1}, {'image': True}, {'image': 1.5}, {'': 4}):
        with pytest.raises(ValueError, match='limit_mm_per_prompt'):
            vllm_config({'model_path': 'models/vision', 'limit_mm_per_prompt': value})


def test_multimodal_cache_budget_is_forwarded_and_validated(tmp_path):
    from demiflow.services import vllm
    for budget in [0, .5, 4]:
        cfg=vllm_config({'model_path':'model', 'mm_processor_cache_gb':budget})
        argv,_=vllm._service_command(cfg, {'name':'fixture','base_url':'http://127.0.0.1:18463/v1'},tmp_path)
        assert float(argv[argv.index('--mm-processor-cache-gb')+1])==budget
    for budget in [-1, True, '0', float('nan'), float('inf')]:
        with pytest.raises(ValueError,match='mm_processor_cache_gb'):
            vllm_config({'model_path':'model','mm_processor_cache_gb':budget})


def test_language_model_only_is_explicit_and_optional(tmp_path):
    for setting in (True, False):
        cfg = vllm_config({'model_path': 'model', 'language_model_only': setting})
        argv, _ = vllm._service_command(cfg, {'name': 'fixture', 'base_url': 'http://127.0.0.1:18463/v1'}, tmp_path)
        assert ('--language-model-only' in argv) is setting
    with pytest.raises(ValueError):
        vllm_config({'model_path': 'model', 'language_model_only': 1})


def test_explicit_cpu_threads_are_scoped_to_owned_service(tmp_path, monkeypatch):
    monkeypatch.setenv('OMP_NUM_THREADS', '4')
    spec = vllm_config({'model_path': 'model', 'omp_num_threads': 1})
    _, env = vllm._service_command(spec, {'name': 'fixture', 'base_url': 'http://127.0.0.1:18463/v1'}, tmp_path)
    assert env['OMP_NUM_THREADS'] == '1' and os.environ['OMP_NUM_THREADS'] == '4'
    for value in (0, -1, True, '1'):
        with pytest.raises(ValueError, match='omp_num_threads'):
            vllm_config({'model_path': 'model', 'omp_num_threads': value})


def test_prefix_cache_policy_is_explicit_and_optional(tmp_path):
    from demiflow.services import vllm
    for setting,flag in [(None,None),(True,'--enable-prefix-caching'),(False,'--no-enable-prefix-caching')]:
        cfg=vllm_config({'model_path':'model','enable_prefix_caching':setting})
        argv,_=vllm._service_command(cfg,{'name':'fixture','base_url':'http://127.0.0.1:18463/v1'},tmp_path)
        assert ([a for a in argv if 'enable-prefix-caching' in a] == ([] if flag is None else [flag]))
    with pytest.raises(ValueError,match='enable_prefix_caching'):
        vllm_config({'model_path':'model','enable_prefix_caching':0})
