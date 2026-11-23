"""Capabilities implementation."""
from __future__ import annotations

VERSION = "demiflow_execution_capabilities_v1"


def check_action(kind, arguments=None):
    """Shared declaration/runtime gate; no alternate checkpoint execution path."""
    arguments = arguments or {}
    retired = (kind in {'open_checkpoint', 'read_checkpoint', 'checkpoint',
                       'checkpoint_async', 'checkpoint_lance', 'checkpoint_lance_async',
                       'stream_checkpoint', 'operator_checkpoint'} or
               kind in {'run_stream', 'async_map', 'batch_map'}
               and (arguments.get('checkpoint') is not None
                    or arguments.get('checkpoint_operator') is not None
                    or arguments.get('checkpoint_lease_s', 300.0) != 300.0))
    return {'version': VERSION, 'supported': not retired,
            'reason': (f'Explicit checkpoint orchestration is retired: {kind}; native run recovery has one protocol; '
                       'stage outputs require their P3 adapters') if retired else None}

def require_action(kind, arguments=None):
    """Apply the same capability gate to native entry points before any work."""
    capability = check_action(kind, arguments)
    if not capability['supported']:
        from demiflow.execution.recovery_contract import RecoveryContractError
        raise RecoveryContractError(capability['reason'])
