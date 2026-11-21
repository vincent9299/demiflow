"""Pipeline budget policy: deployment ceiling → workspace → run tightening.

Defaults describe a small experiment, not a permanent workload size limit.
Native engine admission, available disk and process accounting still apply.
"""
from demiflow.forge.project import validate_budget_declaration
from demiflow.forge.schema import ForgeError

from demiflow.execution.host.control import DEFAULT_LIMITS

DEPLOYMENT_LIMITS = dict(timeout_s=7 * 86400, admission_timeout_s=600, cancel_grace_s=3600,
    memory_bytes=8 * 1024**3, max_rss_bytes=24 * 1024**3,
    max_scratch_bytes=128 * 1024**3, max_output_bytes=16 * 1024**4, max_evidence_bytes=128 * 1024**3,
    max_input_rows=2**40, max_output_rows=2**40, max_row_bytes=128 * 1024**2,
    max_batch_bytes=128 * 1024**2, batch_rows=8192, log_bytes=256 * 1024**2)


def _overlay(values, base, ceiling, label):
    if values is None:
        return dict(base)
    validate_budget_declaration(values, label=label)
    if any(v > ceiling[k] for k, v in values.items()):
        raise ForgeError(label + ' budgets must be positive integers within their admitted ceiling')
    return {**base, **values}


def limits_with(values=None, *, policy=None):
    from demiflow.execution.host.control import resolve_payload_bounds
    def effective(overrides, base, ceiling, label, inherited_explicit=()):
        result = _overlay(overrides, base, ceiling, label)
        result.update(resolve_payload_bounds(result, overrides,
            inherited_explicit=inherited_explicit, ceiling=ceiling))
        return result
    admitted = effective(policy, DEFAULT_LIMITS, DEPLOYMENT_LIMITS, 'Workspace')
    result = effective(values, admitted, admitted, 'Run', (policy or {}).keys())
    if result['memory_bytes'] < 64 * 1024**2 or result['max_rss_bytes'] < 1024**3:
        raise ForgeError('Insufficient native worker memory allowance')
    if result['memory_bytes'] >= result['max_rss_bytes'] // 2:
        raise ForgeError('RSS budget must leave room for the driver and native query process')
    return result
