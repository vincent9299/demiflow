"""M003: NativeEmbeddingRecovery.__init__ borrows the run-owned journal."""
from types import SimpleNamespace

from demiflow.execution.run_context import RunContext
from demiflow.embeddings.stream_recovery import NativeEmbeddingRecovery
from demiflow.operator_llm.stream_recovery import RunPromptJournal


def _action(run):
    return SimpleNamespace(context=run, action_id='act-1', object_sinks={})


def test_init_borrows_single_run_journal(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = SimpleNamespace(_journal=None, options={'max_response_bytes': 4096})
        rec = NativeEmbeddingRecovery(actor, _action(run), 'node')
        assert rec.journal is run._native_prompt_journal
        assert isinstance(rec.journal, RunPromptJournal)
        assert rec.inflight == {}
        assert rec.semantic_owners == ()
        assert rec.row_local is True
        assert rec.copy_inputs is False
        assert rec.actor is actor and rec.node_id == 'node'


def test_init_reuses_existing_journal_without_second_writer(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = SimpleNamespace(_journal=None, options={'max_response_bytes': 4096})
        first = NativeEmbeddingRecovery(actor, _action(run), 'node-a')
        second = NativeEmbeddingRecovery(actor, _action(run), 'node-b')
        assert second.journal is first.journal
        assert run._native_prompt_journal is first.journal


def test_init_does_not_start_client_or_private_store(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor = SimpleNamespace(_journal=None, options={'max_response_bytes': 4096})
        rec = NativeEmbeddingRecovery(actor, _action(run), 'node')
        # No private success store and no client acquisition happened.
        assert not hasattr(rec, 'client')
        assert actor._journal is None
        assert rec._response_bound is None