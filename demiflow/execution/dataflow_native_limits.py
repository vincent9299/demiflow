"""Explicit native-query ceilings on the existing unified execution authorities.

No capacity, scratch lease, effect or commit is owned here. The optional policy
is immutable execution configuration; the original bounds/store/allocator enforce
growth. RSS/headroom are sampled safeguards, not claims of OS isolation.
"""
import os
import time
from collections.abc import Mapping

from .contracts.dataflow_errors import DataflowError

EXECUTION_LIMITS = 'native_execution_limits'
DELIVERY_LIMIT = 'native_delivery_limit'


def execution_ceilings(value):
    """Small declaration borrowed from an old placement, never a new grant."""
    names = {'cpu_millis', 'memory_bytes', 'scratch_bytes', 'inflight_rows',
             'inflight_bytes', 'output_bytes'}
    if value is None:
        return {}
    if not isinstance(value, Mapping) or set(value) - names:
        raise DataflowError('INVALID_CONTRACT', 'invalid native execution ceilings')
    result = {key: amount for key, amount in value.items() if amount is not None}
    if any(type(amount) is not int or not 0 < amount < 2**63 for amount in result.values()):
        raise DataflowError('INVALID_CONTRACT', 'native execution ceilings must be bounded positive integers')
    if result.get('cpu_millis', 1000) < 1000:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'native execution requires one whole CPU worker')
    return result


def sealed_execution_ceilings(graph):
    values = [execution_ceilings(node.kernel.configuration.get('arguments', {}).get(EXECUTION_LIMITS))
              for node in graph.nodes if node.kind == 'terminal']
    return {key: min(value[key] for value in values if key in value)
            for key in set().union(*(value.keys() for value in values))}


def validate_execution_binding(request, graph):
    """An existing action may borrow its original sealed native adapter only.

    Older callers declared the source/plan before choosing a native placement.
    Their added ceilings/schema belong to this same graph seal, not a nested
    action or a mutable side ledger. Refuse drift before loading/starting work.
    """
    from .dataflow_schema import KEY
    keys = (EXECUTION_LIMITS, KEY, DELIVERY_LIMIT)
    expected = {key: request.arguments.get(key) for key in keys}
    for node in graph.nodes:
        if node.kind != 'terminal':
            continue
        actual = {key: node.kernel.configuration.get('arguments', {}).get(key) for key in keys}
        if actual != expected:
            raise DataflowError('INCOMPATIBLE_HISTORY',
                                'native execution limits or schema differ from the sealed binding')


def validate_native_limits(value):
    from .contracts.dataflow import _native_limits
    _native_limits(value)


def effective_native_limits(value, run_limits):
    from .native_resources import parse_native_resources
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


class NativeBudgetGuard:
    """Bounded local sampling; failures stop only this owned execution.

    Threads share their host process, so RSS is a conservative whole-process
    observation. Sampling cannot prevent a third-party allocation between polls.
    """
    def __init__(self, limits):
        self.limits = dict(limits or {})
        self._next = 0.0
        self.peak_rss_bytes = 0

    def check(self):
        if not {'max_rss_bytes', 'cgroup_headroom_bytes'} & self.limits.keys():
            return
        now = time.monotonic()
        if now < self._next:
            return
        self._next = now + .1
        if 'max_rss_bytes' in self.limits:
            from ..resources.process_observation import _read
            fields = _read('/proc/self/stat', maximum=8192).decode().rsplit(')', 1)[1].split()
            rss = int(fields[21]) * os.sysconf('SC_PAGE_SIZE')
            self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
            if rss > self.limits['max_rss_bytes']:
                raise DataflowError('BUDGET_EXCEEDED', 'native host sampled RSS exceeds max_rss_bytes')
        if 'cgroup_headroom_bytes' in self.limits:
            from demiflow.execution.hosts.datafusion import _cgroup_memory
            memory = _cgroup_memory()
            if memory and memory[0] + self.limits['cgroup_headroom_bytes'] >= memory[1]:
                raise DataflowError('RESOURCE_INFEASIBLE', 'native host lacks declared cgroup headroom')
