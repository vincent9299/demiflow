"""Tighten the current run and sealed task ceilings without granting new capacity."""
from __future__ import annotations

from demiflow.execution.contracts.dataflow import validate_native_limits


def effective_native_limits(value, run_limits):
    from demiflow.planning.dataflow_policy import parse_native_resources
    requested = dict(parse_native_resources(value) or ())
    validate_native_limits(requested)
    # These remain upper bounds. A legacy declaration cannot enlarge the run.
    return {key: (max if key == 'cgroup_headroom_bytes' else min)(amount, run_limits[key]) if key in run_limits else amount
            for key, amount in requested.items()}

def intersect_native_limits(sealed, current):
    """A resume may only tighten ceilings (or increase required headroom)."""
    if sealed is None and current is None:
        return None
    result = dict(sealed or {})
    for name, value in (current or {}).items():
        result[name] = ((max if name == 'cgroup_headroom_bytes' else min)(result[name], value)
                        if name in result else value)
    validate_native_limits(result)
    return result

def intersect_bounds(sealed, current):
    from dataclasses import fields, replace
    return replace(current, **{f.name: min(getattr(sealed, f.name), getattr(current, f.name))
                               for f in fields(current)})

def limit_value(services, name, default):
    constants = getattr(services, 'constants', {}) or {}
    return min(default, (constants.get('native_limits') or {}).get(name, default))

def admission_timeout_ms(policy):
    value = (policy.native_limits or {}).get('admission_timeout_s')
    return min(policy.control_timeout_ms, max(1, int(value * 1000))) if value else policy.control_timeout_ms
