"""Q005 verify-preserving unit tests: payload_scope borrow/tighten/restore (BOUNDS/STREAM).

payload_scope is unchanged production code. It borrows the run limits into a
ContextVar-scoped effective ceiling, only tightens against owner_limits (never
loosens), restores the prior active limits on both normal exit and exception, is
scoped to the owning run, and does not swallow body exceptions. The ValueError ->
RecoveryContractError conversion for stream residency is exercised by the existing
test_execution_payload_limits.py::test_peak_scope_keeps_durable_cumulative_ceiling_across_resume.
"""
import pytest

from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.payload_limits import active_limits, payload_scope
from demiflow.execution.run_context import RunContext

MB = 1024 ** 2


def test_borrow_without_plan_and_restore_on_exit(tmp_path):
    base = {'sentinel': True}
    with RunContext.start(tmp_path / 'run') as run:
        with payload_scope(run, None) as limits:
            assert limits == dict(run.limits)
            assert active_limits(run, base) == limits
        assert active_limits(run, base) is base  # restored after normal exit


def test_owner_limits_only_tighten(tmp_path):
    with RunContext.start(tmp_path / 'run') as run:
        default_row = run.limits['max_row_bytes']
        with payload_scope(run, None, owner_limits={'max_row_bytes': 1024, 'max_batch_bytes': 2048}) as limits:
            assert limits['max_row_bytes'] == 1024
            assert limits['max_batch_bytes'] == 2048
        with payload_scope(run, None, owner_limits={'max_row_bytes': 999 * MB}) as limits:
            assert limits['max_row_bytes'] == default_row  # a larger owner ceiling cannot loosen


def test_restore_on_exception_and_body_error_propagates(tmp_path):
    base = {'sentinel': True}
    with RunContext.start(tmp_path / 'run') as run:
        with pytest.raises(RuntimeError, match='body'):
            with payload_scope(run, None):
                raise RuntimeError('body')
        assert active_limits(run, base) is base  # finally reset even on exception


def test_runtime_contract_error_not_swallowed(tmp_path):
    with RunContext.start(tmp_path / 'run') as run:
        with pytest.raises(RuntimeContractError, match='byte budget'):
            with payload_scope(run, None):
                raise RuntimeContractError('Value byte budget exceeded')


def test_scope_is_per_run(tmp_path):
    base = {'sentinel': True}
    other = object()
    with RunContext.start(tmp_path / 'run') as run:
        with payload_scope(run, None) as limits:
            assert active_limits(run, base) == limits
            assert active_limits(other, base) is base  # a different run sees its own base