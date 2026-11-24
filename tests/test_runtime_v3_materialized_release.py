"""A result handle outlives its driver scope; its pin is not a publication fence."""
import hashlib

import pytest

from demiflow import data
from demiflow.errors import ExecutionConflict
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.storage.objects import _pin_identity
from demiflow.execution.contracts.recovery_contract import RecoveryContractError


def test_materialized_release_after_scope_preserves_other_owner_and_history(tmp_path):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        materialized = data.from_items([{'v': 1}, {'v': 2}]).materialize()
        handle = materialized._source.handle
        refs = (handle._manifest_ref, *handle._blocks)
        commits = run.journal.execution_commits('action-1')
        for ref in refs:
            handle._store.retain(ref, owner='other-owner')
    assert materialized.take_all() == [{'v': 1}, {'v': 2}]
    # A regular stale worker/driver mutation remains fenced.
    with pytest.raises(ExecutionConflict):
        handle._store.release(refs[0], owner='other-owner')
    materialized.release()
    materialized.release()
    for ref in refs:
        owners, _ = run.journal.artifact_owners(ref, limit=100)
        assert 'other-owner' in owners and handle._owner not in owners
        assert handle._store.inspect(ref)
    assert run.journal.execution_commits('action-1') == commits
    with pytest.raises(RuntimeError, match='released'):
        materialized.take_all()


def test_release_requires_original_pin_and_unknown_reply_remains_retryable(tmp_path, monkeypatch):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        materialized = data.from_items([{'v': 3}]).materialize()
        handle = materialized._source.handle
    ref = handle._manifest_ref
    with pytest.raises(RecoveryContractError, match='original pin'):
        run.journal.release_materialized_owner(ref, owner=handle._owner, pin_operation_id='a' * 64)
    release = handle._store.release_materialized
    failed = []
    def unknown(ref, *, owner):
        if not failed:
            failed.append(True)
            raise OSError('unknown reply')
        return release(ref, owner=owner)
    monkeypatch.setattr(handle._store, 'release_materialized', unknown)
    with pytest.raises(OSError, match='unknown reply'):
        materialized.release()
    assert not handle._released
    materialized.release()
    assert handle._released
