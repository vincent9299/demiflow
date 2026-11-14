"""Shared admission and budgets for Dataset native queries and Lance staging."""
import hashlib
import os
from collections.abc import Mapping
from pathlib import Path
import tempfile
from dataclasses import replace
import math
import time

from .datafusion import DataFusionOptions, DataFusionSession, _cpu_capacity, _cgroup_memory
from .payload_limits import native_row_limit
from ..resources.capacity import inherited_envelope


_LIMITS = {'memory_bytes', 'max_rss_bytes', 'max_scratch_bytes', 'max_row_bytes', 'batch_rows',
           'threads', 'partitions', 'timeout_s', 'admission_timeout_s', 'cgroup_headroom_bytes'}


def parse_native_resources(value):
    """Snapshot a small closed budget; never expose engine configuration."""
    if value is None:
        return None
    if isinstance(value, tuple):
        # LocalKernelOptions may be copied by dataclasses.replace.
        if len(value) > len(_LIMITS):
            raise ValueError('Too many native_resources fields')
        value = dict(value)
    if not isinstance(value, dict) or len(value) > len(_LIMITS) or set(value) - _LIMITS:
        raise ValueError('native_resources requires a dict of supported resource limits')
    for name, number in value.items():
        if name.endswith('_s'):
            valid = type(number) in (int, float) and math.isfinite(number) and number > 0
        else:
            valid = type(number) is int and number > 0
        if not valid:
            raise ValueError('native_resources ' + name + ' must be finite and positive')
    return tuple(sorted(value.items()))


def _sealed_ceilings(execution):
    """Read-only sealed resource ceilings borrowed from a bound ExecutionPlacement.

    Returns None when unbound. The profile carries cpu_millis/memory_bytes/
    scratch_bytes (no threads/rows); stage_limits carries per-stage
    inflight_rows/inflight_bytes/output_bytes. The tightest simultaneously-alive
    stage bounds the shared session. These ceilings only tighten the existing
    run/envelope/native authority; they never allocate a second host budget.
    """
    if execution is None:
        return None
    from .recovery_contract import RecoveryContractError
    profile = getattr(execution, 'profile', None)
    if not isinstance(profile, Mapping):
        raise RecoveryContractError('Bound execution requires a sealed resource profile')
    sealed = profile.get('limits') or profile.get('requests')
    if not isinstance(sealed, Mapping):
        raise RecoveryContractError('Bound execution profile requires requests or limits')
    memory = sealed.get('memory_bytes')
    if type(memory) is not int or memory <= 0:
        raise RecoveryContractError('Sealed profile memory_bytes must be a positive integer')
    scratch = sealed.get('scratch_bytes')
    if scratch is not None and (type(scratch) is not int or scratch <= 0):
        raise RecoveryContractError('Sealed profile scratch_bytes must be a positive integer')
    cpu = sealed.get('cpu_millis')
    if cpu is not None and (type(cpu) is not int or cpu <= 0):
        raise RecoveryContractError('Sealed profile cpu_millis must be a positive integer')
    stage_limits = getattr(execution, 'stage_limits', None) or {}
    if not isinstance(stage_limits, Mapping):
        raise RecoveryContractError('Bound execution stage_limits must be a mapping')
    stage_ids = tuple(getattr(execution, 'stage_ids', ()) or ())
    selected = [stage_limits[s] for s in stage_ids if s in stage_limits] if stage_ids else list(stage_limits.values())
    inflight_rows = inflight_bytes = output_bytes = None
    for budget in selected:
        if not isinstance(budget, Mapping):
            raise RecoveryContractError('Sealed stage budget must be a mapping')
        for name in ('inflight_rows', 'inflight_bytes', 'output_bytes'):
            value = budget.get(name)
            if value is None:
                continue
            if type(value) is not int or value <= 0:
                raise RecoveryContractError('Sealed stage ' + name + ' must be a positive integer')
        rows, bytes_, out = budget.get('inflight_rows'), budget.get('inflight_bytes'), budget.get('output_bytes')
        inflight_rows = rows if inflight_rows is None else min(inflight_rows, rows) if rows is not None else inflight_rows
        inflight_bytes = bytes_ if inflight_bytes is None else min(inflight_bytes, bytes_) if bytes_ is not None else inflight_bytes
        output_bytes = out if output_bytes is None else min(output_bytes, out) if out is not None else output_bytes
    return {'memory_bytes': memory, 'scratch_bytes': scratch, 'cpu_millis': cpu,
            'inflight_rows': inflight_rows, 'inflight_bytes': inflight_bytes, 'output_bytes': output_bytes}


def dataset_session(executor, directory, *, prefer_hash_join=False, lance_merge=False, execution=None):
    envelope = inherited_envelope()
    if envelope is None:
        cpus = _cpu_capacity()
        memory = _cgroup_memory()
    else:
        # A managed worker queries inside its parent allocation slice; defaults
        # come from the envelope, not the whole machine (single charge).
        cpus = max(1, envelope['cpu_millis'] // 1000)
        memory = (0, envelope['memory_bytes'])
    slots = min(2, max(1, cpus // 4))
    if memory and memory[1] < 52 * 2**30:
        slots = 1
    threads = min(8, max(1, cpus // slots))
    rss = min(24 * 2**30, memory[1] // 3 if memory else 24 * 2**30)
    # Lance's column writer shares its pool with sort-merge buffers and a
    # fragment update buffer. Reserve more of the same RSS slot for that job.
    pool = min((12 if lance_merge else 8) * 2**30, rss // 2)
    options = DataFusionOptions(memory_bytes=pool, max_rss_bytes=rss,
        max_row_bytes=native_row_limit(pool, rss),
        max_scratch_bytes=128 * 2**30, threads=threads, partitions=min(2, threads),
        prefer_hash_join=prefer_hash_join, timeout_s=3600, admission_timeout_s=3600,
        cgroup_headroom_bytes=min(2 * 2**30, rss // 4))
    budget = getattr(getattr(executor, '_local_kernel', None), 'native_resources', None)
    if budget is not None:
        limits = dict(parse_native_resources(budget))
        for name, number in limits.items():
            default = getattr(options, name)
            if name == 'cgroup_headroom_bytes':
                if number < default:
                    raise ValueError('native_resources cannot reduce cgroup headroom')
            elif number > default:
                raise ValueError('native_resources cannot increase default ' + name)
        requested_rss = limits.get('max_rss_bytes', rss)
        requested_pool = limits.get('memory_bytes', min(pool, requested_rss // 2))
        row_limit = native_row_limit(requested_pool, requested_rss, options.max_row_bytes)
        options = replace(options, **{**limits, 'memory_bytes': requested_pool,
            'max_row_bytes': limits.get('max_row_bytes', row_limit),
            'partitions': limits.get('partitions', min(options.partitions, limits.get('threads', threads)))})
    from .run_context import current_run
    run = current_run()
    if run is not None:
        limits = run.limits
        effective_rss = min(options.max_rss_bytes, max(1, limits['max_rss_bytes'] // 2))
        effective_pool = min(options.memory_bytes, limits['memory_bytes'], max(1, effective_rss // 2))
        row_ceiling = min(options.max_row_bytes, limits['max_row_bytes'], limits['max_batch_bytes'])
        explicit_row = 'max_row_bytes' in run.explicit_limits or 'max_row_bytes' in dict(budget or ())
        row_limit = row_ceiling if explicit_row else native_row_limit(effective_pool, effective_rss, row_ceiling)
        options = replace(options, memory_bytes=effective_pool, max_rss_bytes=effective_rss,
            max_row_bytes=row_limit,
            max_scratch_bytes=min(options.max_scratch_bytes, limits['max_scratch_bytes']),
            batch_rows=min(options.batch_rows, limits['batch_rows']) if 'batch_rows' in run.explicit_limits else options.batch_rows,
            timeout_s=min(options.timeout_s, max(0.001, run.deadline - time.time())),
            admission_timeout_s=min(options.admission_timeout_s, limits['admission_timeout_s'],
                                    max(0.001, run.deadline - time.time())))
    ceilings = _sealed_ceilings(execution)
    if ceilings is not None:
        # The sealed placement is an additional ceiling on the same authority,
        # not a new allocation. Intersect only downward; admission still charges
        # the tightened options.max_rss_bytes through the original slot ledger.
        sealed_memory = ceilings['memory_bytes']
        effective_rss = min(options.max_rss_bytes, sealed_memory)
        effective_pool = min(options.memory_bytes, max(1, effective_rss // 2), sealed_memory)
        row_ceiling = min(options.max_row_bytes, sealed_memory)
        if ceilings['inflight_bytes'] is not None:
            row_ceiling = min(row_ceiling, ceilings['inflight_bytes'])
        # Intersect the already admitted row ceiling with the sealed grant.
        # Re-running the default pool estimate would erase an explicit allowance.
        row_limit = min(row_ceiling, effective_pool, max(1, effective_rss // 2))
        scratch = options.max_scratch_bytes
        if ceilings['scratch_bytes'] is not None:
            scratch = min(scratch, ceilings['scratch_bytes'])
        batch_rows = options.batch_rows
        eff_threads = options.threads
        if ceilings['cpu_millis'] is not None:
            eff_threads = min(eff_threads, max(1, ceilings['cpu_millis'] // 1000))
        if ceilings['inflight_rows'] is not None:
            batch_rows = min(batch_rows, ceilings['inflight_rows'])
        if ceilings['inflight_bytes'] is not None:
            batch_rows = min(batch_rows, max(1, ceilings['inflight_bytes'] // max(1, row_limit)))
        options = replace(options, memory_bytes=effective_pool, max_rss_bytes=effective_rss,
            max_row_bytes=row_limit, max_scratch_bytes=scratch, batch_rows=batch_rows,
            threads=eff_threads, partitions=min(options.partitions, eff_threads))
    resources = executor.resource_root / '_demiflow' / 'dataset_native'
    managed = getattr(executor, 'diagnostics_root', None)
    if managed is None and run is not None:
        managed = run.directory / 'diagnostics' / 'native'
    if envelope is None:
        group_id = hashlib.sha256(Path('/proc/self/cgroup').read_bytes()).hexdigest()[:16]
        admission_root = Path(tempfile.gettempdir()) / f'demiflow-dataset-{os.getuid()}-{group_id}'
    else:
        # Private admission directory: the parent allocation already paid the
        # deployment-domain reservation, so queries must not charge it again.
        admission_root = Path(envelope['resource_directory'])
    return DataFusionSession(options=options, max_concurrent=slots, resource_directory=admission_root,
                             slot_capacity={'max_rss_bytes_per_slot': rss, 'threads_per_slot': threads},
                             diagnostics_directory=managed or resources / 'queries', temp_directory=str(directory),
                             managed_diagnostics=managed is not None)
