"""Shared admission and budgets for Dataset native queries and Lance staging."""
import hashlib
import os
from pathlib import Path
import tempfile
from dataclasses import replace
import math
import time

from .datafusion import DataFusionOptions, DataFusionSession, _cpu_capacity, _cgroup_memory
from .payload_limits import native_row_limit


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


def dataset_session(executor, directory, *, prefer_hash_join=False, lance_merge=False):
    cpus = _cpu_capacity()
    memory = _cgroup_memory()
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
    resources = executor.resource_root / '_demiflow' / 'dataset_native'
    managed = getattr(executor, 'diagnostics_root', None)
    group_id = hashlib.sha256(Path('/proc/self/cgroup').read_bytes()).hexdigest()[:16]
    shared_pool = Path(tempfile.gettempdir()) / f'demiflow-dataset-{os.getuid()}-{group_id}'
    return DataFusionSession(options=options, max_concurrent=slots, resource_directory=shared_pool,
                             slot_capacity={'max_rss_bytes_per_slot': rss, 'threads_per_slot': threads},
                             diagnostics_directory=managed or resources / 'queries', temp_directory=str(directory),
                             managed_diagnostics=managed is not None)
