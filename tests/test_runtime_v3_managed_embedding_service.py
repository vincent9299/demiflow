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
    with pytest.raises(Exception, match='device grant'):
        freeze_managed_http(fixture['service'])
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
