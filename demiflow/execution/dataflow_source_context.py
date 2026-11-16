"""Original run identity for source callbacks, without driver write authority.

The shared artifact root and assignment are already authenticated by runtime
service assembly. This view opens no journal, claims no run and owns no action.
It is scoped to reader open/pull/close, including native callback threads that
use run_context.bind_iteration, rather than exposing the driver to a worker.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from .recovery_contract import RecoveryContractError, RunRef
from .run_context import _ACTION, _CURRENT


@dataclass(frozen=True)
class SourceRunView:
    ref: RunRef
    attempt: int
    fence: str
    limits: object
    check_stop: object

    def action(self, *args, **kwargs):
        raise RecoveryContractError("Source callbacks cannot create driver actions")


def source_run_view(services):
    """Derive immutable run facts from this worker's existing authenticated ports."""
    root = Path(services.store.root)
    if root.name != '.dataflow-store' or not root.is_absolute():
        raise RecoveryContractError("Source context requires the original run artifact root")
    assignment = services.assignment
    return SourceRunView(
        RunRef(str(root.parent), assignment.run_id), assignment.attempt,
        assignment.fence, MappingProxyType(dict(services.constants.get('native_limits', {}))),
        services.check_stop,
    )


@contextmanager
def source_run_scope(view):
    current_token = _CURRENT.set(view)
    action_token = _ACTION.set(None)
    try:
        yield view
    finally:
        _ACTION.reset(action_token)
        _CURRENT.reset(current_token)
