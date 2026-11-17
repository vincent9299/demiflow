"""Deep real action declarations stay in the journal, not worker control RPCs."""
import pytest

from demiflow.execution.contracts.dataflow import ControlReply, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_authority import DriverJournalService
from demiflow.execution.run_context import RunContext
from demiflow.execution.recovery_contract import RecoveryContractError


def test_deep_action_retains_original_journal_but_control_reply_is_narrow(tmp_path):
    source = {'leaf': 'original prompt schema'}
    for _ in range(30):
        source = {'source': source}
    declaration = {'kind': 'write_lance', 'source': source}
    # A control read must not complete the action; the run correctly remains
    # unconfirmed when this intentionally execution-free probe exits.
    with pytest.raises(RecoveryContractError, match='unconfirmed actions'), RunContext.start(tmp_path / 'run') as run:
        with run.action(declaration) as action:
            # Exercise the real projection against a real journal/action. No
            # workers/store are needed for this side-effect-free control read.
            authority = object.__new__(DriverJournalService)
            authority._action = action
            original = action.record['declaration']
            with pytest.raises(DataflowError, match='depth budget'):
                ControlReply(operation_id='0' * 64, status='ok', payload={
                    'action_id': action.action_id, 'declaration': original, 'limits': dict(run.limits)},
                    payload_ref=None, error_code=None)
            value = authority._read_action(action.action_id)
            reply = ControlReply(operation_id='0' * 64, status='ok', payload=value,
                                 payload_ref=None, error_code=None)
            assert thaw_record_value(reply.payload) == {'action_id': action.action_id, 'limits': dict(run.limits)}
            assert action.record['declaration'] == original == declaration
            with pytest.raises(DataflowError, match='FENCED'):
                authority._read_action('unrelated-action')
