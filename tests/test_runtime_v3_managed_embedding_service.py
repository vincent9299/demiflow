"""Passive native declarations and real V3-owned service process lifetime."""
import json
import threading
import time

import pytest

from demiflow import data
from demiflow.embeddings.flow_contract import actor_declaration, embedding_facts
from demiflow.execution.dataflow_managed_http import freeze_managed_http, restore_managed_http
from demiflow.execution.dataflow_model_factory import native_model_declaration
from demiflow.execution.run_context import RunContext
from runtime_v3_managed_http_scenarios import managed_fixture, assert_reaped, events


def dataset(fixture, root, *, cached=True, service=True):
    options = {'timeout_s': 5}
    if cached:
        options['sqlite_journal'] = {'path': str(root / 'cache.sqlite')}
    return data.from_items([{'text': 'a'}]).map_embeddings(model=fixture['model'],
        inputs={'text': 'text'}, batch_size=1, concurrency=1, call_output='call',
        options=options, service=fixture['service'] if service else None)


def test_passive_roundtrip_and_default_fact_identity(tmp_path, monkeypatch, request):
    fixture = managed_fixture(tmp_path, monkeypatch, request=request)
    frozen = freeze_managed_http(fixture['service'])
    assert freeze_managed_http(restore_managed_http(frozen)) == frozen
    plain = dataset(fixture, tmp_path, service=False)._plan.operations[-1]
    actor = plain.callable.target.__self__
    assert not hasattr(actor, '_sealed_service_declaration')
    original = json.dumps(actor_declaration(actor), sort_keys=True, separators=(',', ':'), allow_nan=False)
    assert actor._sealed_declaration == original
    assert embedding_facts(plain) == {'contract': 'demiflow.embeddings.v2', 'arguments': json.loads(original)}
    managed = dataset(fixture, tmp_path)._plan.operations[-1]
    assert embedding_facts(managed) == embedding_facts(plain)
    factory = native_model_declaration(managed)
    assert factory.instantiate().service.command == fixture['service'].command
    assert events(fixture) == []


@pytest.mark.parametrize('mutation', ['bind', 'environment', 'removed', 'live_actor', 'live_service'])
def test_mutated_or_live_declaration_is_not_silently_reconstructed(tmp_path, monkeypatch, request, mutation):
    fixture = managed_fixture(tmp_path, monkeypatch, request=request)
    operation = dataset(fixture, tmp_path)._plan.operations[-1]
    actor = operation.callable.target.__self__
    if mutation == 'bind': actor.service.bind = lambda model: None
    elif mutation == 'environment': actor.service.env['CHANGED'] = 'yes'
    elif mutation == 'removed': actor.service = None
    elif mutation == 'live_actor': actor._owner = object()
    elif mutation == 'live_service': actor.service._process = object()
    with pytest.raises(Exception, match='modified|differs|removed|passive|unknown'):
        native_model_declaration(operation)
    assert events(fixture) == []


def test_overridden_native_class_method_and_ungranted_gpu_are_rejected(tmp_path, monkeypatch, request):
    from demiflow.services import ManagedHTTPService
    fixture = managed_fixture(tmp_path, monkeypatch, request=request)
    with monkeypatch.context() as altered:
        altered.setattr(ManagedHTTPService, 'bind', lambda self, model=None: None)
        with pytest.raises(Exception, match='methods differ'):
            freeze_managed_http(fixture['service'])
    fixture['service'].gpus = (0,)
    # Numeric GPUs are now a passive declaration, never a runtime grant.
    operation = dataset(fixture, tmp_path)._plan.operations[-1]
    worker = native_model_declaration(operation).instantiate()
    import asyncio
    with pytest.raises(Exception, match='device grant'):
        asyncio.run(worker.service.bind(worker.model).ensure_ready())
    assert events(fixture) == []


@pytest.mark.parametrize('mode', ['failure', 'unready'])
def test_failure_reaps_real_service(tmp_path, monkeypatch, request, mode):
    fixture = managed_fixture(tmp_path, monkeypatch, request=request, mode=mode)
    with pytest.raises(Exception, match='503|ready|readiness'):
        dataset(fixture, tmp_path).take_all()
    assert_reaped(fixture, requests=1 if mode == 'failure' else 0)


def test_original_port_owner_is_not_attached_or_terminated(tmp_path, monkeypatch, request):
    from http.server import HTTPServer, BaseHTTPRequestHandler
    with HTTPServer(('127.0.0.1', 0), BaseHTTPRequestHandler) as occupied:
        fixture = managed_fixture(tmp_path, monkeypatch, request=request, port=occupied.server_port)
        with pytest.raises(Exception, match='occupied'):
            dataset(fixture, tmp_path).take_all()
        assert occupied.fileno() >= 0 and events(fixture) == []


def test_cancel_reaps_actual_worker_owned_service(tmp_path, monkeypatch, request):
    fixture = managed_fixture(tmp_path, monkeypatch, request=request, mode='slow')
    cancelled = threading.Event()
    with pytest.raises(Exception, match='[Cc]ancel'):
        with RunContext.start(tmp_path / 'run', limits={'timeout_s': 25}) as run:
            def cancel():
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if any(row['event'] == 'request' for row in events(fixture)):
                        RunContext.cancel(run.ref)
                        cancelled.set()
                        return
                    time.sleep(.01)
            thread = threading.Thread(target=cancel)
            thread.start()
            try:
                dataset(fixture, tmp_path, cached=False).take_all()
            finally:
                thread.join(16)
    assert cancelled.is_set() and not thread.is_alive()
    assert_reaped(fixture)


@pytest.mark.parametrize('mode', ['slow', 'detached'])
def test_worker_sigkill_retains_unit_until_actual_service_tree_exits(tmp_path, monkeypatch, request, mode):
    import os
    from pathlib import Path
    import psutil
    fixture = managed_fixture(tmp_path, monkeypatch, request=request, mode=mode)
    proof = {'premature_release': [], 'kill': None, 'charged_while_child_alive': False}

    def alive(item):
        try:
            proc = psutil.Process(item['pid'])
            return proc.create_time() == item['birth'] and proc.status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False

    def kill():
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            rows = events(fixture)
            if any(r['event'] == 'request' for r in rows) and (
                    mode != 'detached' or any(r['event'] == 'descendant' for r in rows)):
                service = next(r for r in rows if r['event'] == 'start')
                worker = psutil.Process(service['pid']).parent()
                for _ in range(3):
                    if 'demiflow.execution.worker.bootstrap' in worker.cmdline():
                        break
                    worker = worker.parent()
                assert 'demiflow.execution.worker.bootstrap' in worker.cmdline()
                proof['kill'] = {'pid': worker.pid, 'birth': worker.create_time()}
                worker.kill()
                records = Path(os.environ['DEMIFLOW_RESOURCE_ROOT']) / 'capacity/allocations'
                while time.monotonic() < deadline:
                    living = [r for r in rows if r['event'] in ('start', 'descendant') and alive(r)]
                    for path in records.glob('*.json'):
                        record = json.loads(path.read_text())
                        if (record.get('identity') or {}).get('pid') == worker.pid and living:
                            if record['state'] == 'RELEASED':
                                proof['premature_release'].append(record)
                            else:
                                proof['charged_while_child_alive'] = True
                    if not living:
                        return
                    time.sleep(.005)
                return
            time.sleep(.01)

    thread = threading.Thread(target=kill)
    thread.start()
    try:
        # SIGKILL can sever either the read or write half of the real socket.
        with pytest.raises(Exception, match='UNKNOWN_EFFECT|worker connection closed|Connection reset by peer|Broken pipe'):
            with RunContext.start(tmp_path / 'run', limits={'timeout_s': 25}):
                dataset(fixture, tmp_path, cached=False).take_all()
    finally:
        thread.join(16)
        (tmp_path / 'worker-kill-proof.json').write_text(json.dumps(proof, indent=2))
    assert proof['kill'] and not thread.is_alive()
    assert proof['charged_while_child_alive'] and not proof['premature_release']
    assert_reaped(fixture)
    # No early refund is only half the contract. Once all owned processes have
    # exited, the original close path must actually confirm and release them.
    import sqlite3
    with sqlite3.connect(f'file:{tmp_path / "run/run.sqlite"}?mode=ro', uri=True) as db:
        allocations = db.execute('SELECT allocation_id,cleanup FROM resource_allocations').fetchall()
    assert allocations and all(cleanup == 'CONFIRMED' for _, cleanup in allocations)
    records = Path(os.environ['DEMIFLOW_RESOURCE_ROOT']) / 'capacity/allocations'
    assert all(json.loads((records / (allocation + '.json')).read_text())['state'] == 'RELEASED'
               for allocation, _ in allocations)


def test_guardian_observer_exception_reaps_detached_closefd_descendant(tmp_path, monkeypatch, request):
    """Inject only an observer fault inside the real original managed unit."""
    import os
    import sys
    from demiflow.resources.process_unit import ManagedProcessUnit, birth_start
    fixture = managed_fixture(tmp_path, monkeypatch, request=request, mode='detached')
    declaration = {'parent': os.getpid(), 'birth': birth_start(os.getpid()),
        'command': list(fixture['service'].command), 'shutdown_timeout_s': .5}
    script = '''import json,sys
from pathlib import Path
from demiflow.resources import process_group as child
original=child.descendants
def failed_observer(owner):
    rows=original(owner)
    path=Path(sys.argv[2])
    if path.exists() and any(json.loads(line)['event']=='descendant' for line in path.read_text().splitlines()):
        raise RuntimeError('injected-guardian-observer-failure')
    return rows
child.descendants=failed_observer
child.main(sys.argv[1])
'''
    unit = ManagedProcessUnit(tmp_path / 'units')
    spawned = unit.spawn('service', 'guardian', 'a' * 64,
        [sys.executable, '-B', '-c', script, json.dumps(declaration), str(fixture['events'])],
        log_path=tmp_path / 'guardian.log', extra_env={'CUDA_VISIBLE_DEVICES': ''})
    process = spawned['process']
    try:
        assert process.wait(timeout=12) != 0
        assert 'injected-guardian-observer-failure' in (tmp_path / 'guardian.log').read_text()
        assert any(row['event'] == 'descendant' for row in events(fixture))
        assert_reaped(fixture, requests=0)
        assert unit.stop('service', 'guardian', terminate_deadline_ms=1000, process=process)['confirmed']
    finally:
        if process.poll() is None:
            unit.stop('service', 'guardian', terminate_deadline_ms=2000, process=process)
