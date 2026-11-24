"""Intercept the first Dataset action without starting execution.

The Pipeline entry owner runs trusted construction code inside capture_scope.
This scope owns interception state and unwinds at the first action boundary;
it never fabricates an action result or claims to know later branches.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from demiflow.execution.contracts.dataflow import Bounds, CompiledDataflow, RuntimePolicy

_CURRENT = ContextVar('demiflow_plan_capture', default=None)


def current_capture():
    return _CURRENT.get()


class _Captured(BaseException):
    """Unwind construction before data, workers, journals or sinks start."""


class CompileContext:
    fence = None
    attempt = 0
    ref = None

    def __init__(self, limits=None, explicit_limits=None):
        from demiflow.planning.dataflow_policy import execution_limits, declared_limit_keys
        self._limits = execution_limits(limits)
        self.explicit_limits = declared_limit_keys(limits, explicit_limits)

    @property
    def limits(self):
        from demiflow.execution.pipeline.run_context import active_limits
        return active_limits(self, self._limits)

    def check_stop(self):
        return None


@dataclass(frozen=True)
class CapturedAction:
    compiled: CompiledDataflow
    kind: str
    provider: str
    policy: RuntimePolicy
    bounds: Bounds
    entry_completed: bool = False
    submission: dict | None = None


class _Capture:
    def __init__(self, on_compiled=None, *, action_id='preview', limits=None, explicit_limits=None, submission=False):
        self.result = None
        self.context = CompileContext(limits, explicit_limits)
        self.on_compiled = on_compiled
        self.action_id = action_id
        self.submission = submission

    def intercept(self, request, *, policy, bounds):
        from demiflow.planning.dataflow_compile import lower_action
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        if self.result is not None:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'construction caught the planning boundary; later actions cannot be guessed')
        compiled = lower_action(request, bounds=bounds, on_compiled=self.on_compiled)
        contract = None
        if self.submission:
            from demiflow.execution.pipeline.managed_native import submission_contract
            contract = submission_contract(request, compiled, bounds=bounds)
        self.result = CapturedAction(compiled, request.kind, request.provider, policy, bounds, submission=contract)
        raise _Captured()


@contextmanager
def capture_scope(on_compiled=None, *, limits=None, explicit_limits=None, submission=False):
    """Bind one compile-only interception; the caller owns trusted entry execution."""
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.pipeline.run_context import current_run
    from demiflow.execution.pipeline.checked import current as current_checked
    if current_capture() is not None or current_run() is not None or current_checked() is not None:
        raise DataflowError('INVALID_CONTRACT', 'plan capture cannot nest inside execution or another capture')
    capture = _Capture(on_compiled, action_id='action-1' if submission else 'preview',
                       limits=limits, explicit_limits=explicit_limits, submission=submission)
    token = _CURRENT.set(capture)
    try:
        yield capture
    finally:
        _CURRENT.reset(token)
