"""M011: aclose drains native effects before releasing resources."""
import asyncio
from types import SimpleNamespace

import pytest

from demiflow.execution.run_context import RunContext
from demiflow.embeddings.runtime import EmbeddingActor
from demiflow.embeddings.model import EmbeddingModel


def _actor():
    model = EmbeddingModel(name='m', revision='r', dimensions=4, base_url='http://localhost:9/v1')
    return EmbeddingActor(model=model, inputs={'text': 'text'}, output='vector',
        call_output='call', error_output=None, concurrency=1, label='t',
        options={'timeout_s': 30, 'max_response_bytes': 4096},
        max_requests=None, service=None, request_gate=None)


def test_aclose_drains_before_closing_client(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = _actor()
        action = SimpleNamespace(context=run, action_id='act-1', object_sinks={})
        rec = actor.native_stream_recovery(action, 'node')

        async def exercise():
            await actor.astart()
            order = []
            orig_drain = rec.drain

            async def spy_drain(*, timeout_s):
                order.append('drain')
                await orig_drain(timeout_s=timeout_s)

            rec.drain = spy_drain
            client = actor._client
            orig_close = client.aclose

            async def spy_close():
                order.append('client_close')
                await orig_close()

            client.aclose = spy_close
            await actor.aclose()
            assert order == ['drain', 'client_close']

        asyncio.run(exercise())


def test_aclose_drain_timeout_leaves_client_open(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = _actor()
        action = SimpleNamespace(context=run, action_id='act-1', object_sinks={})
        rec = actor.native_stream_recovery(action, 'node')

        async def exercise():
            await actor.astart()
            closed = []
            client = actor._client
            orig_close = client.aclose

            async def spy_close():
                closed.append(True)
                await orig_close()

            client.aclose = spy_close

            async def timeout_drain(*, timeout_s):
                raise TimeoutError('still running')

            rec.drain = timeout_drain
            with pytest.raises(TimeoutError):
                await actor.aclose()
            assert closed == []  # client not closed while effects may be running
            # Release the partially started resources for test hygiene.
            actor._native_recovery = None
            await actor.aclose()

        asyncio.run(exercise())