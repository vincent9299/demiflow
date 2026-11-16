"""Original run identity for source callbacks, without driver write authority.

The shared artifact root and assignment are already authenticated by runtime
service assembly. This view opens no journal, claims no run and owns no action.
Source and operator callbacks share this immutable view, including their
construction, async/thread calls, snapshots and cleanup. No driver is exposed.
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
        raise RecoveryContractError("Worker callbacks cannot create driver actions")


def source_run_view(services, *, limits=None):
    """Derive immutable run facts from this worker's existing authenticated ports."""
    root = Path(services.store.root)
    if root.name != '.dataflow-store' or not root.is_absolute():
        raise RecoveryContractError("Source context requires the original run artifact root")
    assignment = services.assignment
    # Native ceilings are optional declarations, not the worker's grant. Read
    # adapters still need the actual finite payload allowance when no optional
    # native policy was requested. Intersect both authenticated inputs; never
    # substitute a process-wide default or enlarge a declared ceiling.
    limits = dict(limits or {})
    for name, value in (services.constants.get('native_limits') or {}).items():
        limits[name] = min(limits.get(name, value), value)
    from .dataflow_record_frames import resident_limit
    for name, bound in (
        ('memory_bytes', resident_limit(services.bounds, services)),
        ('max_row_bytes', services.bounds.max_row_bytes),
        ('max_batch_bytes', services.bounds.max_batch_bytes),
    ):
        limits[name] = min(limits.get(name, bound), bound)
    return SourceRunView(
        RunRef(str(root.parent), assignment.run_id), assignment.attempt,
        assignment.fence, MappingProxyType(limits),
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
