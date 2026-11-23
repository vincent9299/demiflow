"""M006: astart binds the run-owned journal under native execution."""
import asyncio
from types import SimpleNamespace

import pytest

from demiflow.execution.run_context import RunContext
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.embeddings.runtime import EmbeddingActor
from demiflow.embeddings.model import EmbeddingModel
from demiflow.execution.operator_bindings.dataflow_model_journal import NativeEmbeddingRecovery
from demiflow.execution.operator_bindings.dataflow_model_journal import _BorrowedJournal
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal


def _actor(**options):
    model = EmbeddingModel(name='m', revision='r', dimensions=4, base_url='http://localhost:9/v1')
    return EmbeddingActor(model=model, inputs={'text': 'text'}, output='vector',
        call_output='call', error_output=None, concurrency=1, label='t',
        options={'timeout_s': 30, 'max_response_bytes': 4096, **options},
        max_requests=None, service=None, request_gate=None)


def _action(run):
    return SimpleNamespace(context=run, action_id='act-1', object_sinks={})


def test_astart_native_borrows_run_journal(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = _actor()
        rec = actor._native_recovery = NativeEmbeddingRecovery(actor, _action(run), 'node')

        async def exercise():
            await actor.astart()
            try:
                assert isinstance(actor._journal, _BorrowedJournal)
                assert actor._journal.journal is rec.journal
            finally:
                await actor.aclose()

        asyncio.run(exercise())
        # The run-owned journal survives the actor's borrowed close.
        assert rec.journal.lookup({'payload': 'fresh', 'native_response_bytes': 4096}) is None


def test_astart_native_rejects_private_journal(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = _actor(sqlite_journal={'path': str(tmp_path / 'private.sqlite')})
        with pytest.raises(RecoveryContractError, match='private SQLite journal'):
            actor._native_recovery = NativeEmbeddingRecovery(actor, _action(run), 'node')
        assert not (tmp_path / 'private.sqlite').exists()
        assert actor._client is None


def test_astart_non_native_keeps_private_journal(tmp_path):
    actor = _actor(sqlite_journal={'path': str(tmp_path / 'private.sqlite')})

    async def exercise():
        await actor.astart()
        try:
            assert isinstance(actor._journal, SQLitePromptJournal)
            assert actor._native_recovery is None
        finally:
            await actor.aclose()

    asyncio.run(exercise())


def test_astart_non_native_without_journal_has_none(tmp_path):
    actor = _actor()

    async def exercise():
        await actor.astart()
        try:
            assert actor._journal is None
        finally:
            await actor.aclose()

    asyncio.run(exercise())
