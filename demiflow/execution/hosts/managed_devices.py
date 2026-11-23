"""Managed devices implementation."""
from __future__ import annotations

from demiflow.execution.contracts.dataflow_kinds import MANAGED_HTTP_DEVICES_FIELD

import json
import os
import socket
import subprocess
from pathlib import Path
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import identity


def deployment_inventory():
    path = os.environ.get('DEMIFLOW_DEVICE_INVENTORY')
    if not path:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU service requires an explicit deployment inventory')
    with Path(path).open('rb') as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU deployment inventory exceeds 64 KiB')
    value = json.loads(raw)
    from demiflow.resources.device_pool import _validate_inventory
    return {'devices': _validate_inventory(value)}


def physical_devices():
    # NVIDIA's fixed metadata query does not initialize a model/CUDA context.
    result = subprocess.run(['nvidia-smi', '--query-gpu=index,uuid,pci.bus_id,memory.total,mig.mode.current',
        '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=5, check=True)
    if len(result.stdout.encode()) > 65536:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Physical GPU inventory exceeds its metadata bound')
    rows = []
    for line in result.stdout.splitlines():
        index, uuid, pci, memory, mig = (p.strip() for p in line.split(','))
        if mig not in ('Disabled', 'N/A', '[N/A]'):
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU service requires a non-MIG physical deployment')
        rows.append({'index': int(index), 'uuid': uuid, 'pci_bus_id': pci,
                     'memory_bytes': int(memory) * 1024**2})
    if not 1 <= len(rows) <= 64 or len({r['uuid'] for r in rows}) != len(rows):
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Physical GPU inventory is not bounded and unique')
    return sorted(rows, key=lambda row: row['pci_bus_id'])


def device_binding(constants, occurrence):
    value = constants.get('constants', {}).get(occurrence, {}).get(MANAGED_HTTP_DEVICES_FIELD)
    if not isinstance(value, dict) or value.get('schema') != 'demiflow.managed_http_devices.v1':
        # Runtime JSON maps are immutable MappingProxyTypes; thaw without
        # accepting arbitrary object encoders.
        from collections.abc import Mapping
        if not isinstance(value, Mapping) or value.get('schema') != 'demiflow.managed_http_devices.v1':
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU service has no sealed device binding')
    if value['hostname'] != socket.gethostname():
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU device binding belongs to another host')
    return value


def service_device_facts(declaration):
    order = declaration.get('device_order') or os.environ.get('CUDA_DEVICE_ORDER')
    if order != 'PCI_BUS_ID':
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Numeric managed GPU slots require explicit CUDA_DEVICE_ORDER=PCI_BUS_ID')
    inventory, physical = deployment_inventory(), physical_devices()
    declared = {d['device_id']: d for d in inventory['devices']}
    indices = tuple(declaration['indices'])
    if not indices or len(indices) > 64 or any(type(i) is not int or i < 0 or i >= len(physical) for i in indices):
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU slot is outside the physical local inventory')
    devices = [physical[i] for i in indices]
    for item in devices:
        configured = declared.get(item['uuid'])
        if configured is None or configured['type'] != 'gpu' or configured['memory_bytes'] > item['memory_bytes']:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU UUID is absent from the declared physical deployment allowance')
    return {'schema': 'demiflow.managed_http_devices.v1',
        'hostname': socket.gethostname(), 'inventory_digest': identity(inventory),
        'indices': list(indices), 'device_ids': [d['uuid'] for d in devices],
        'physical': devices}
