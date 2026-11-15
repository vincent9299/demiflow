"""Explicit deployment GPU inventory wired into the existing device authority.

No hardware is auto-claimed. DEMIFLOW_DEVICE_INVENTORY names a bounded JSON file
using the existing DevicePool inventory schema. Local IDs must be CUDA-visible
UUIDs/indices for the deployment. Ray Core chooses actual device IDs itself;
this adapter currently admits one GPU host, avoiding invented cross-host device
memory ownership. The normal CPU-only configuration remains unchanged.
"""
import json
import os
import sys
from pathlib import Path

from .contracts.dataflow_errors import DataflowError


def device_pool(provider):
    path = os.environ.get('DEMIFLOW_DEVICE_INVENTORY')
    if not path:
        return None
    from ..resources.capacity import inherited_envelope
    if inherited_envelope() is not None:
        # Nested CPU work uses the parent's envelope. It cannot create a new
        # device ledger and advertise the same physical devices a second time.
        return None
    ray = sys.modules.get('ray')
    if ray is not None and ray.is_initialized() and ray.get_runtime_context().get_actor_id() is not None:
        return None  # A Ray child also cannot re-advertise its parent's GPU grant.
    from .dataflow_capacity import local_capacity
    from ..resources.device_pool import DevicePool
    try:
        with Path(path).open('rb') as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError('inventory exceeds 64 KiB')
        inventory = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise DataflowError('INVALID_CONTRACT', 'Invalid deployment device inventory: ' + str(exc)) from exc
    domain, _ = local_capacity()
    pool = DevicePool(domain / 'devices', inventory=inventory)
    if any(item['type'] != 'gpu' for item in pool.inventory):
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Unified device placement currently supports GPU inventories')
    if provider == 'ray_core':
        import ray
        gpu_hosts = [n for n in ray.nodes() if n.get('Alive') and n.get('Resources', {}).get('GPU', 0) > 0]
        if len(gpu_hosts) != 1:
            raise DataflowError('UNSUPPORTED_CAPABILITY',
                'Configured Ray device inventory requires exactly one GPU host; multi-host device binding is not proven')
        if len(pool.inventory) > gpu_hosts[0]['Resources']['GPU']:
            raise DataflowError('RESOURCE_INFEASIBLE', 'Device inventory exceeds actual Ray Core GPU capacity')
    return pool


def gpu_capacity(provider):
    pool = device_pool(provider)
    return 0 if pool is None else len(pool.inventory)
