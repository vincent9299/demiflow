"""M004: bind_actor borrows the run journal and refuses a second authority."""
from types import SimpleNamespace

import pytest

from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.operator_bindings.dataflow_model_journal import NativeEmbeddingRecovery
from demiflow.execution.operator_bindings.dataflow_model_journal import _BorrowedJournal


def _setup(run, *, max_response_bytes=4096, journal=None):
    actor = SimpleNamespace(_journal=journal, options={'max_response_bytes': max_response_bytes})
    action = SimpleNamespace(context=run, action_id='act-1', object_sinks={})
    return actor, NativeEmbeddingRecovery(actor, action, 'node')


def test_bind_installs_borrowed_journal_and_bound(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor, rec = _setup(run)
        rec.bind_actor()
        assert isinstance(actor._journal, _BorrowedJournal)
        assert actor._journal.journal is rec.journal
        assert rec._response_bound == 4096 + 64 * 1024


def test_borrowed_close_does_not_close_run_journal(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor, rec = _setup(run)
        rec.bind_actor()
        actor._journal.close()  # no-op wrapper
        # The run-owned writer is still usable after the borrowed close.
        assert rec.journal.lookup({'payload': 'fresh', 'native_response_bytes': 4096}) is None


def test_bind_rejects_coexisting_private_journal(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        actor, rec = _setup(run, journal=object())
        with pytest.raises(RecoveryContractError, match='private SQLite journal'):
            rec.bind_actor()
        assert not isinstance(actor._journal, _BorrowedJournal)


def test_bind_rejects_response_ceiling_over_record_budget(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        with pytest.raises(RecoveryContractError, match='record budget'):
            _setup(run, max_response_bytes=64 * 1024 ** 2)