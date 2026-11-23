"""M013: native_request_budget stages the existing CALL_RESERVATION.

The helper only stages a reservation; RunPromptJournal applies it in the same
savepoint as the request intent. None inherits the run ceiling, an explicit 0
forbids fresh dispatch, and the ContextVar is always restored on exit.
"""
import pytest

from demiflow.execution.run_context import RunContext
from demiflow.operator_llm.errors import PromptBudgetExceededError
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.operator_llm.stream_recovery import native_request_budget, CALL_RESERVATION
from demiflow.execution.journal.prompt import RunPromptJournal


def _request():
    return {'payload': 'same occurrence', 'native_response_bytes': 4096}


def test_stages_and_clears_reservation(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        assert CALL_RESERVATION.get() is None
        with native_request_budget(run, node_id='node-a', max_requests=None) as reservation:
            rid, charges = CALL_RESERVATION.get()
            assert rid == reservation
            assert charges == {'requests': (1, 5),
                               'requests-' + identity('node-a'): (1, 5)}
        assert CALL_RESERVATION.get() is None


def test_none_inherits_run_ceiling_zero_and_explicit_are_distinct(tmp_path):
    node = 'requests-' + identity('n')
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 7}) as run:
        with native_request_budget(run, node_id='n', max_requests=None):
            assert CALL_RESERVATION.get()[1][node] == (1, 7)
        with native_request_budget(run, node_id='n', max_requests=0):
            assert CALL_RESERVATION.get()[1][node] == (1, 0)
        with native_request_budget(run, node_id='n', max_requests=3):
            assert CALL_RESERVATION.get()[1][node] == (1, 3)


def test_zero_ceiling_rejects_reserve_without_intent(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        journal = run._native_prompt_journal = RunPromptJournal(run)
        request = _request()
        with native_request_budget(run, node_id='n', max_requests=0):
            with pytest.raises(PromptBudgetExceededError, match='Operator LLM request budget exhausted'):
                journal.reserve(request)
        assert journal.lookup(request) is None
        assert run.journal.budget_consumed('requests') == 0


def test_reserve_applies_both_components_atomically(tmp_path):
    node = 'requests-' + identity('n')
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        journal = run._native_prompt_journal = RunPromptJournal(run)
        request = _request()
        with native_request_budget(run, node_id='n', max_requests=2):
            assert journal.reserve(request)
        assert run.journal.budget_consumed('requests') == 1
        assert run.journal.budget_consumed(node) == 1
        journal.response(request, {'body': 'ok'})
        assert journal.lookup(request)['body'] == 'ok'


def test_restores_prior_token_on_exception(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        sentinel = ('prior', {'requests': (1, 5)})
        token = CALL_RESERVATION.set(sentinel)
        try:
            with pytest.raises(RuntimeError, match='boom'):
                with native_request_budget(run, node_id='n', max_requests=1):
                    raise RuntimeError('boom')
            assert CALL_RESERVATION.get() is sentinel
        finally:
            CALL_RESERVATION.reset(token)


@pytest.mark.parametrize('bad', [-1, 1.5, '2', True, False])
def test_invalid_ceiling_rejected_before_staging(tmp_path, bad):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        with pytest.raises(RecoveryContractError):
            with native_request_budget(run, node_id='n', max_requests=bad):
                pass
        assert CALL_RESERVATION.get() is None


def test_same_reservation_replay_is_idempotent(tmp_path):
    node = 'requests-' + identity('n')
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        with native_request_budget(run, node_id='n', max_requests=2) as rid:
            charges = CALL_RESERVATION.get()[1]
        run.journal.reserve_budgets(run.fence, rid, charges)
        run.journal.reserve_budgets(run.fence, rid, charges)
        assert run.journal.budget_consumed('requests') == 1
        assert run.journal.budget_consumed(node) == 1