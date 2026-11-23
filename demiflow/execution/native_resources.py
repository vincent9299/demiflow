"""Parse Dataset native resource declarations for the unified execution policy."""
import math


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
