"""Shared admission and budgets for Dataset native queries and Lance staging."""
import hashlib
import os
from pathlib import Path
import tempfile

from .datafusion import DataFusionOptions, DataFusionSession, _cpu_capacity, _cgroup_memory


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
        max_scratch_bytes=128 * 2**30, threads=threads, partitions=min(2, threads),
        prefer_hash_join=prefer_hash_join, timeout_s=3600, admission_timeout_s=3600,
        cgroup_headroom_bytes=min(2 * 2**30, rss // 4))
    resources = executor.resource_root / '_demiflow' / 'dataset_native'
    group_id = hashlib.sha256(Path('/proc/self/cgroup').read_bytes()).hexdigest()[:16]
    shared_pool = Path(tempfile.gettempdir()) / f'demiflow-dataset-{os.getuid()}-{group_id}'
    return DataFusionSession(options=options, max_concurrent=slots, resource_directory=shared_pool,
                             diagnostics_directory=resources / 'queries', temp_directory=str(directory))
