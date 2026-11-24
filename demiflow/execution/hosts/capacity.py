"""Local deployment capacity, shared by runs through the existing allocator.

The default domain is one user/cgroup/host, not one action. Deployments can
select a private domain with DEMIFLOW_RESOURCE_ROOT; tests must use /tmp roots.
This is logical admission, not a claim of hard process RSS/CPU enforcement.
Nested workers consume their parent's envelope instead of the whole machine.
"""
import hashlib
import os
from pathlib import Path
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar

from demiflow.resources.capacity import inherited_envelope

_DOMAIN = ContextVar('demiflow_local_capacity_domain', default=None)


_DEVICE_INVENTORY = ContextVar('demiflow_local_device_inventory', default=None)


def original_device_inventory():
    return _DEVICE_INVENTORY.get()


@contextmanager
def local_capacity_domain(directory, *, device_inventory=None):
    """Select a sealed host domain for maintenance without changing process env.

    This selects the existing allocator's directory, never an allocation or
    cleanup authority. Original provider identity and lease checks still apply.
    Child processes must receive the same directory through deployment env.
    """
    if type(directory) is not str or not 1 <= len(directory.encode()) <= 4096:
        raise ValueError('Invalid local capacity domain')
    root = Path(directory)
    if not root.is_absolute() or str(root.resolve()) != directory:
        raise ValueError('Local capacity domain must be canonical and absolute')
    token = _DOMAIN.set(root)
    inventory = _DEVICE_INVENTORY.set(device_inventory)
    try:
        yield root
    finally:
        _DEVICE_INVENTORY.reset(inventory)
        _DOMAIN.reset(token)


def local_capacity():
    envelope = inherited_envelope()
    if envelope is not None:
        return Path(envelope['resource_directory']) / 'dataflow', {
            key: envelope[key] for key in ('cpu_millis', 'memory_bytes')}
    cpus = len(os.sched_getaffinity(0)) * 1000
    memory = os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')
    cgroup = Path('/sys/fs/cgroup')
    try:
        quota, period = (cgroup / 'cpu.max').read_text().split()
        if quota != 'max':
            cpus = min(cpus, max(1, int(quota) * 1000 // int(period)))
    except (OSError, ValueError):
        pass
    try:
        limit = (cgroup / 'memory.max').read_text().strip()
        if limit != 'max':
            memory = min(memory, int(limit))
    except (OSError, ValueError):
        pass
    domain = (os.uname().nodename + ':' + str(os.getuid()) + ':' +
              Path('/proc/self/cgroup').read_text())
    default = Path(tempfile.gettempdir()) / ('demiflow-capacity-' +
              hashlib.sha256(domain.encode()).hexdigest()[:24])
    root = _DOMAIN.get() or Path(os.environ.get('DEMIFLOW_RESOURCE_ROOT', default)).resolve()
    return root, {'cpu_millis': cpus, 'memory_bytes': memory}
