"""Actual callbacks cannot hide platform failure behind catch=(Exception,)."""
import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.run_journal import RunJournal


async def fail_control(row):
    import asyncio
    from demiflow.data.runtime_contract import RuntimeContractError
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    from demiflow.execution.run_journal import ExecutionBudgetExceeded
    from demiflow.services.request_limits import ServiceStopped
    from demiflow.operator_llm.journal import UncertainPromptCall
    await asyncio.sleep(0)
    if row['kind'] == 'validation':
        raise RuntimeContractError('controlled validation failure')
    if row['kind'] == 'protocol':
        raise DataflowError('BUDGET_EXCEEDED', 'controlled protocol failure')
    if row['kind'] == 'recovery':
        raise RecoveryContractError('controlled recovery failure')
    if row['kind'] == 'budget':
        raise ExecutionBudgetExceeded('requests')
    if row['kind'] == 'stop':
        raise ServiceStopped('operator_stop_file')
    if row['kind'] == 'unknown':
        raise UncertainPromptCall('controlled unknown model response')
    raise ValueError('ordinary business miss')


@pytest.mark.parametrize('kind,expected', [
    ('validation', 'RuntimeContractError'), ('protocol', 'DataflowError'),
    ('recovery', 'RecoveryContractError'), ('budget', 'ExecutionBudgetExceeded'),
    ('stop', 'ServiceStopped'), ('unknown', 'UncertainPromptCall'),
])
def test_real_async_business_catch_cannot_ack_control_failure(tmp_path, kind, expected):
    with pytest.raises(Exception) as failure:
        with RunContext.start(tmp_path/'run'):
            data.from_items([{'kind': kind}]).map_async(fail_control, catch=(Exception,)).materialize()
    assert type(failure.value).__name__ == expected
    journal = RunJournal(tmp_path/'run')
    assert journal.execution_action('action-1')['result'] is None
    assert not journal.execution_commits('action-1')


def test_real_async_business_miss_is_still_a_dropped_row(tmp_path):
    with RunContext.start(tmp_path/'run'):
        assert data.from_items([{'kind': 'business'}]).map_async(
            fail_control, catch=(ValueError,)).materialize().take_all() == []


async def fail_batch_control(rows):
    await fail_control(rows[0])


@pytest.mark.parametrize('kind,expected', [
    ('validation', 'RuntimeContractError'), ('protocol', 'DataflowError'),
    ('recovery', 'RecoveryContractError'), ('budget', 'ExecutionBudgetExceeded'),
    ('stop', 'ServiceStopped'), ('unknown', 'UncertainPromptCall'),
])
def test_real_batch_business_catch_cannot_ack_control_failure(tmp_path, kind, expected):
    with pytest.raises(Exception) as failure:
        with RunContext.start(tmp_path/'run'):
            data.from_items([{'kind': kind}]).batch_map(
                fail_batch_control, max_batch=1, catch=(Exception,)).materialize()
    assert type(failure.value).__name__ == expected
    journal = RunJournal(tmp_path/'run')
    assert journal.execution_action('action-1')['result'] is None
    assert not journal.execution_commits('action-1')
