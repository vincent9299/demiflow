"""Finite execution defaults; Forge may admit/tighten the same native budgets."""

DEFAULT_LIMITS = dict(timeout_s=600, admission_timeout_s=30, cancel_grace_s=10,
    memory_bytes=256 * 1024**2, max_rss_bytes=4 * 1024**3,
    max_scratch_bytes=4 * 1024**3, max_output_bytes=2 * 1024**3, max_evidence_bytes=64 * 1024**2,
    max_input_rows=100000, max_output_rows=200000, batch_rows=256, log_bytes=16 * 1024**2)

from .payload_limits import run_row_limit
DEFAULT_LIMITS.update(max_row_bytes=run_row_limit(DEFAULT_LIMITS),
                      max_batch_bytes=run_row_limit(DEFAULT_LIMITS))

RECOVERY_LIMITS = dict(max_state_bytes=16 * 1024**2, max_control_bytes=64 * 1024**2,
                       max_commits=16384, max_requests=100000, max_artifacts=65536,
                       checkpoint_rows=512, checkpoint_interval_ms=1000, checkpoint_timeout_s=60,
                       checkpoint_unconfirmed_rows=4096, checkpoint_unconfirmed_bytes=64 * 1024**2)


def declared_limit_keys(values, explicit):
    """A full admitted envelope may carry a smaller set of caller declarations."""
    from .recovery_contract import RecoveryContractError
    if values is not None and type(values) is not dict:
        raise RecoveryContractError('Unknown execution budget')
    keys = frozenset(values or ())
    if explicit is None:
        return keys
    declared = frozenset(explicit)
    if not declared <= keys:
        raise RecoveryContractError("Explicit execution budgets must have admitted values")
    return declared


def execution_limits(values=None):
    from .recovery_contract import RecoveryContractError
    result = {**DEFAULT_LIMITS, **RECOVERY_LIMITS}
    if values is not None:
        if type(values) is not dict or values.keys() - result.keys():
            raise RecoveryContractError("Unknown execution budget")
        result.update(values)
    if any(type(v) is not int or not 0 < v < 2**63 for v in result.values()):
        raise RecoveryContractError("Execution budgets must be finite positive integers")
    from .payload_limits import resolve_payload_bounds
    result.update(resolve_payload_bounds(result, values))
    if result["max_row_bytes"] > result["max_batch_bytes"]:
        raise RecoveryContractError("A row must fit the admitted batch budget")
    if result["max_commits"] > 16384 or result["max_artifacts"] > 65536:
        raise RecoveryContractError("Execution metadata cardinality exceeds protocol limits")
    return result
