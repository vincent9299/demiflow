"""M007: _exchange scopes, reserves atomically, tracks inflight, and reuses."""
import asyncio
from types import SimpleNamespace

import httpx
import pytest

from demiflow.execution.run_context import RunContext
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.row_scope import RowScope, set_row_scope
from demiflow.embeddings.runtime import EmbeddingActor
from demiflow.embeddings.model import EmbeddingModel
from demiflow.execution.operator_bindings.dataflow_model_journal import NativeEmbeddingRecovery
from demiflow.operator_llm.journal import RequestRecord, canonical


def _actor(max_requests=None):
    model = EmbeddingModel(name='m', revision='r', dimensions=4, base_url='http://localhost:9/v1')
    return EmbeddingActor(model=model, inputs={'text': 'text'}, output='vector',
        call_output='call', error_output=None, concurrency=1, label='t',
        options={'timeout_s': 30, 'max_response_bytes': 4096},
        max_requests=max_requests, service=None, request_gate=None)


def _request(actor):
    body = {'input': ['hello'], 'model': 'm'}
    encoded = canonical(body).encode()
    return RequestRecord._from_canonical_body(protocol='embeddings-v1',
        model_contract=actor.model.contract(), body=body, encoded_body=encoded), encoded


def _response():
    return httpx.Response(200, json={'data': [{'index': 0, 'embedding': [0.1, 0.2, 0.3, 0.4]}],
                                    'model': 'm', 'usage': {}})


def test_exchange_reserves_persists_reuses_and_tracks_inflight(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = _actor()
        action = SimpleNamespace(context=run, action_id='act-1', object_sinks={})
        rec = actor._native_recovery = NativeEmbeddingRecovery(actor, action, 'node')
        sent, inflight_seen = [], []

        def handler(request):
            sent.append(request)
            inflight_seen.append(dict(rec.inflight))
            return _response()

        async def exercise():
            await actor.astart()
            actor._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            set_row_scope(RowScope('occ-1'))
            try:
                request, encoded = _request(actor)
                record, call = await actor._exchange(request, encoded, {})
                assert call['reused'] is False
                assert len(sent) == 1
                assert run.journal.budget_consumed('requests') == 1
                assert run.journal.budget_consumed('requests-' + identity('node')) == 1
                # The real fresh Task was registered while the request ran.
                assert len(inflight_seen[0]) == 1
                assert rec.inflight == {}  # removed after work finished
                # A persisted response is reused without fresh dispatch or quota.
                record2, call2 = await actor._exchange(request, encoded, {})
                assert call2['reused'] is True
                assert len(sent) == 1
                assert run.journal.budget_consumed('requests') == 1
            finally:
                set_row_scope(None)
                await actor.aclose()

        asyncio.run(exercise())


def test_exchange_native_budget_exhaustion_blocks_fresh(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = _actor(max_requests=1)
        action = SimpleNamespace(context=run, action_id='act-1', object_sinks={})
        actor._native_recovery = NativeEmbeddingRecovery(actor, action, 'node')
        sent = []

        def handler(request):
            sent.append(request)
            return _response()

        async def exercise():
            await actor.astart()
            actor._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            request, encoded = _request(actor)
            try:
                set_row_scope(RowScope('occ-1'))
                await actor._exchange(request, encoded, {})
                assert len(sent) == 1
                set_row_scope(RowScope('occ-2'))
                with pytest.raises(RecoveryContractError, match='Execution budget exhausted'):
                    await actor._exchange(request, encoded, {})
                assert len(sent) == 1  # second occurrence never dispatched
            finally:
                set_row_scope(None)
                await actor.aclose()

        asyncio.run(exercise())


def test_exchange_requires_row_scope(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = _actor()
        action = SimpleNamespace(context=run, action_id='act-1', object_sinks={})
        actor._native_recovery = NativeEmbeddingRecovery(actor, action, 'node')

        async def exercise():
            await actor.astart()
            actor._client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: _response()))
            request, encoded = _request(actor)
            set_row_scope(None)
            try:
                with pytest.raises(RecoveryContractError, match='source occurrence'):
                    await actor._exchange(request, encoded, {})
            finally:
                await actor.aclose()

        asyncio.run(exercise())
