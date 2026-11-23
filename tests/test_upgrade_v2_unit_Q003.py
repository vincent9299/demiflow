"""Q003 verify-preserving unit tests: prepare idempotent reservation (RECOVERY/BOUNDS).

prepare is unchanged production code. The commit-keyed reservation (commit/sink/
component/ordinal) must reuse the journal's charge when the same commit is prepared
again with identical bytes (window-3 re-prepare / resume), and must reject a changed
amount under the same reservation id rather than silently double-charging or refunding.
"""
import sqlite3

import pytest

from demiflow.execution.outputs.block_sink import BlockSink
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext


def _budgets(path):
    with sqlite3.connect(path / 'run.sqlite') as db:
        return dict(db.execute('SELECT name,consumed FROM runtime_budgets'))


def test_reprepare_same_commit_reuses_idempotent_charge(tmp_path):
    rows = [{'id': i, 'payload': b'p' * 100} for i in range(5)]
    with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 8192, 'max_batch_bytes': 8192}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'q003'}}) as action:
            context = CommitCoordinator(action)._context('commit-1')
            first = BlockSink(action).prepare(iter(rows), context)
            charged = _budgets(run.directory)
            # A fresh sink (new generation/paths) re-preparing the same commit must
            # reuse the commit-keyed charge, not add a second one.
            second = BlockSink(action).prepare(iter(rows), context)
            assert first['rows'] == second['rows'] == 5
            assert _budgets(run.directory) == charged
            assert charged.get('row_bytes', 0) > 0
            action.complete({'rows': 5})


def test_changed_amount_same_reservation_rejected(tmp_path):
    rows = [{'id': i, 'payload': b'p' * 100} for i in range(5)]
    wider = [{'id': i, 'payload': b'p' * 200} for i in range(5)]
    with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 8192, 'max_batch_bytes': 8192}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'q003'}}) as action:
            context = CommitCoordinator(action)._context('commit-1')
            BlockSink(action).prepare(iter(rows), context)
            with pytest.raises(RecoveryContractError, match='Budget reservation changed'):
                BlockSink(action).prepare(iter(wider), context)
            action.complete({'rows': 5})