"""Changing a pipeline creates a new run, never rewrites the old identity."""
import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.contracts.recovery_contract import RecoveryContractError


def test_explicit_identity_migration_rebinds_checkpoint(tmp_path):
    old = tmp_path / 'v1.lance'
    with pytest.raises(OSError, match='stop original'):
        with RunContext.start(tmp_path / 'v1-run', entry_identity={'contract': 'v1'}) as run:
            reference = run.ref
            receipt = data.from_items([{'value': 1}]).write_lance(str(old), mode='create', return_receipt=True)
            raise OSError('stop original')
    journal = RunJournal(reference.directory)
    identity, history = journal.recovery_identity(), journal.execution_commits('action-1')
    with pytest.raises(RecoveryContractError, match='identity'):
        with RunContext.resume(reference, entry_identity={'contract': 'v2'}):
            raise AssertionError('old history accepted a different pipeline identity')
    with RunContext.start(tmp_path / 'v2-run', entry_identity={'contract': 'v2'}) as migrated:
        result = data.read_lance(str(old), version=receipt.committed_version).map(
            lambda row: {'value': row['value'] + 1}).write_lance(
                str(tmp_path / 'v2.lance'), mode='create', return_receipt=True)
    assert migrated.ref.run_id != reference.run_id
    assert data.read_lance(str(tmp_path / 'v2.lance'), version=result.committed_version).take_all() == [{'value': 2}]
    assert data.read_lance(str(old), version=receipt.committed_version).take_all() == [{'value': 1}]
    assert journal.recovery_identity() == identity
    assert journal.execution_commits('action-1') == history
    assert not (tmp_path / 'checkpoint.json').exists()
