"""Explicit deployment GPU inventory wired into the existing device authority.

No hardware is auto-claimed. DEMIFLOW_DEVICE_INVENTORY names a bounded JSON file
using the existing DevicePool inventory schema. Local IDs must be CUDA-visible
UUIDs/indices for the deployment. Ray Core chooses actual device IDs itself;
versioned host inventories are verified before its original DevicePool admits
the shell's actual GPU UUIDs. CPU-only configuration remains unchanged.
"""
import json
import os
import sys
from pathlib import Path

from demiflow.execution.contracts.dataflow_errors import DataflowError


def device_configuration(provider):
    from demiflow.execution.hosts.capacity import original_device_inventory
    path = original_device_inventory()
    if path is None:
        path = os.environ.get('DEMIFLOW_DEVICE_INVENTORY')
    if not path:
        return None, None
    from demiflow.resources.capacity import inherited_envelope
    if inherited_envelope() is not None:
        # Nested CPU work uses the parent's envelope. It cannot create a new
        # device ledger and advertise the same physical devices a second time.
        return None, None
    ray = sys.modules.get('ray')
    if ray is not None and ray.is_initialized() and ray.get_runtime_context().get_actor_id() is not None:
        return None, None  # A child cannot re-advertise its parent's GPU grant.
    from demiflow.execution.hosts.capacity import local_capacity
    from demiflow.resources.device_pool import DevicePool
    try:
        with Path(path).open('rb') as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError('inventory exceeds 64 KiB')
        inventory = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise DataflowError('INVALID_CONTRACT', 'Invalid deployment device inventory: ' + str(exc)) from exc
    domain, _ = local_capacity()
    topology = None
    if provider == 'ray_core':
        from demiflow.execution.hosts.ray_devices import load_ray_device_topology
        topology = load_ray_device_topology(inventory)
        inventory = {'devices': list(topology.pool_inventory)}
    pool = DevicePool(domain / 'devices', inventory=inventory)
    if any(item['type'] != 'gpu' for item in pool.inventory):
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Unified device placement currently supports GPU inventories')
    return pool, topology


def device_pool(provider):
    return device_configuration(provider)[0]


def gpu_capacity(provider):
    pool = device_pool(provider)
    return 0 if pool is None else len(pool.inventory)
