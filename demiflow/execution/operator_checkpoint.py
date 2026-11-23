"""Explicitly retired operator checkpoint option."""
from pathlib import Path


class OperatorCheckpoint:
    """Retired facade; construction cannot create or recover an operator ledger."""

    def __init__(self, path: str | Path, *, operator: str, lease_s: float = 300.0,
                 payload_policy: str = 'immutable'):
        from .capabilities import require_action
        require_action('operator_checkpoint')
