"""Tighten the current run and sealed task ceilings without granting new capacity."""
from __future__ import annotations

from demiflow.planning.dataflow_policy import resolve_runtime_policy

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


def resolve_action_limits(*, options, limits, explicit, ceilings, declared,
                          native_inherited, aggregate_sizes, provider):
    """Intersect declared per-action ceilings; never grant or acquire capacity."""
    if ceilings:
        if 'scratch_bytes' in ceilings:
            limits['max_scratch_bytes'] = min(limits['max_scratch_bytes'], ceilings['scratch_bytes'])
            explicit.add('max_scratch_bytes')
        if 'memory_bytes' in ceilings:
            options['memory_bytes'] = min(options.get('memory_bytes', limits['memory_bytes']), ceilings['memory_bytes'])
            explicit.add('memory_bytes')
        for key in ('inflight_bytes', 'output_bytes'):
            if key in ceilings:
                for name in ('max_row_bytes', 'max_batch_bytes'):
                    limits[name] = min(limits[name], ceilings[key])
                    explicit.add(name)
        if 'cpu_millis' in ceilings:
            count = ceilings['cpu_millis'] // 1000
            options['max_active_workers'] = min(options.get('max_active_workers', count), count)
    if 'inflight_rows' in ceilings:
        explicit.add('batch_rows')
    native = ({'batch_rows': min(ceilings['inflight_rows'], limits['batch_rows'])}
              if 'inflight_rows' in ceilings else None)
    if declared is not None:
        native = intersect_native_limits(native, effective_native_limits(declared, limits))
    if aggregate_sizes is not None:
        native = intersect_native_limits(native, {'batch_rows': min(aggregate_sizes)})
    if native is not None:
        for name in ('max_rss_bytes', 'max_scratch_bytes', 'max_row_bytes', 'batch_rows'):
            if name in native:
                limits[name] = native[name]
                if not native_inherited:
                    explicit.add(name)
        if 'memory_bytes' in native:
            # Keep the upstream run envelope intact. The policy compares it
            # with the effective local, graph and native ceiling exactly once.
            if 'memory_bytes' in ceilings:
                native['memory_bytes'] = min(native['memory_bytes'], ceilings['memory_bytes'])
            options['memory_bytes'] = min(options.get('memory_bytes', limits['memory_bytes']), native['memory_bytes'])
            if not native_inherited:
                explicit.add('memory_bytes')
    policy, bounds = resolve_runtime_policy(options, limits, provider=provider,
                                           explicit_limits=tuple(explicit))
    if native is not None:
        from dataclasses import replace
        policy = replace(policy, native_limits=native)
        if 'timeout_s' in native:
            bounds = replace(bounds, timeout_ms=min(bounds.timeout_ms, max(1, int(native['timeout_s'] * 1000))))
    return policy, bounds
