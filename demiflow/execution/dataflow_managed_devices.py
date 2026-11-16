"""Local managed-service placement through the original sealed plan/device pool.

Numeric slots are resolved only with explicit CUDA PCI ordering. Read-only
inventory is deployment evidence; neither this module nor the service allocates
devices, refunds grants, or owns another resource ledger.
"""
import json
import os
import socket
import subprocess
from dataclasses import replace
from pathlib import Path

from .contracts.dataflow_errors import DataflowError
from .recovery_contract import document, identity

_KEY = 'managed_http_devices'
_SCHEMA = 'demiflow.managed_http_devices.v1'


def _error(reason):
    return DataflowError('UNSUPPORTED_CAPABILITY', reason)


def _inventory():
    path = os.environ.get('DEMIFLOW_DEVICE_INVENTORY')
    if not path:
        raise _error('Managed GPU service requires an explicit deployment inventory')
    with Path(path).open('rb') as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise _error('Managed GPU deployment inventory exceeds 64 KiB')
    value = json.loads(raw)
    from ..resources.device_pool import _validate_inventory
    return {'devices': _validate_inventory(value)}


def _physical_devices():
    # NVIDIA's fixed metadata query does not initialize a model/CUDA context.
    result = subprocess.run(['nvidia-smi', '--query-gpu=index,uuid,pci.bus_id,memory.total,mig.mode.current',
        '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=5, check=True)
    if len(result.stdout.encode()) > 65536:
        raise _error('Physical GPU inventory exceeds its metadata bound')
    rows = []
    for line in result.stdout.splitlines():
        index, uuid, pci, memory, mig = (p.strip() for p in line.split(','))
        if mig not in ('Disabled', 'N/A', '[N/A]'):
            raise _error('Managed GPU service requires a non-MIG physical deployment')
        rows.append({'index': int(index), 'uuid': uuid, 'pci_bus_id': pci,
                     'memory_bytes': int(memory) * 1024**2})
    if not 1 <= len(rows) <= 64 or len({r['uuid'] for r in rows}) != len(rows):
        raise _error('Physical GPU inventory is not bounded and unique')
    return sorted(rows, key=lambda row: row['pci_bus_id'])


def service_profile(kernel, *, policy, bounds):
    if not kernel.configuration.get(_KEY):
        return kernel.profile
    if policy.provider != 'local' or policy.worker_mode == 'thread':
        raise _error('Legacy managed service GPU slots require a Local process host')
    rss = (policy.native_limits or {}).get('max_rss_bytes')
    if type(rss) is not int or rss <= 0:
        raise _error('Managed GPU service requires explicit native_resources.max_rss_bytes')
    if rss < kernel.profile.requests['memory_bytes'] or rss > bounds.max_resident_bytes:
        raise DataflowError('RESOURCE_INFEASIBLE', 'Managed GPU service RSS does not fit the original run envelope')
    return replace(kernel.profile, requests={**kernel.profile.requests, 'memory_bytes': rss})


def service_constants(graph, *, policy):
    result = {}
    for node in graph.nodes:
        declaration = node.kernel.configuration.get(_KEY)
        if not declaration:
            continue
        if policy is None or policy.provider != 'local' or policy.worker_mode == 'thread':
            raise _error('Legacy managed service GPU slots require a Local process host')
        rss = (policy.native_limits or {}).get('max_rss_bytes')
        if type(rss) is not int or rss <= 0:
            raise _error('Managed GPU service requires explicit native_resources.max_rss_bytes')
        order = declaration.get('device_order') or os.environ.get('CUDA_DEVICE_ORDER')
        if order != 'PCI_BUS_ID':
            raise _error('Numeric managed GPU slots require explicit CUDA_DEVICE_ORDER=PCI_BUS_ID')
        inventory, physical = _inventory(), _physical_devices()
        declared = {d['device_id']: d for d in inventory['devices']}
        indices = tuple(declaration['indices'])
        if not indices or len(indices) > 64 or any(type(i) is not int or i < 0 or i >= len(physical) for i in indices):
            raise _error('Managed GPU slot is outside the physical local inventory')
        devices = [physical[i] for i in indices]
        for item in devices:
            configured = declared.get(item['uuid'])
            if configured is None or configured['type'] != 'gpu' or configured['memory_bytes'] > item['memory_bytes']:
                raise _error('Managed GPU UUID is absent from the declared physical deployment allowance')
        result[node.occurrence_id] = {_KEY: {'schema': _SCHEMA,
            'hostname': socket.gethostname(), 'inventory_digest': identity(inventory),
            'indices': list(indices), 'device_ids': [d['uuid'] for d in devices],
            'physical': devices, 'max_rss_bytes': rss}}
    return result


def _binding(constants, occurrence):
    value = constants.get('constants', {}).get(occurrence, {}).get(_KEY)
    if not isinstance(value, dict) or value.get('schema') != _SCHEMA:
        # Runtime JSON maps are immutable MappingProxyTypes; thaw without
        # accepting arbitrary object encoders.
        from collections.abc import Mapping
        if not isinstance(value, Mapping) or value.get('schema') != _SCHEMA:
            raise _error('Managed GPU service has no sealed device binding')
    if value['hostname'] != socket.gethostname():
        raise _error('Managed GPU device binding belongs to another host')
    return value


def sealed_service_placements(graph, physical, *, store):
    occurrences = {n.occurrence_id: n for n in graph.nodes if n.kernel.configuration.get(_KEY)}
    if not occurrences:
        return None
    raw = b''.join(store.read(physical.constants_ref, max_bytes=physical.bounds.max_state_bytes))
    constants = json.loads(raw)
    if constants.get('dag_id') != graph.dag_id or physical.dag_id != graph.dag_id:
        raise _error('Managed service placement differs from the sealed graph')
    inventory_digest = identity(_inventory())
    placements = {}
    for stage in physical.stages:
        selected = set(stage.occurrences) & occurrences.keys()
        if not selected:
            continue
        if len(selected) != 1 or len(stage.occurrences) != 1:
            raise _error('Managed GPU service requires its original exclusive worker stage')
        occurrence = next(iter(selected)); binding = _binding(constants, occurrence)
        if binding['inventory_digest'] != inventory_digest:
            raise _error('Managed GPU deployment inventory changed since sealing')
        if stage.profile.requests['memory_bytes'] < binding['max_rss_bytes']:
            raise _error('Managed service worker profile lacks its sealed RSS grant')
        placements[stage.stage_id] = {'required': {'device_ids': list(binding['device_ids'])}}
    if len(placements) != len(occurrences):
        raise _error('Managed service placement does not cover every original occurrence')
    return placements


def bind_worker_service(actor, spec, *, services, node_id, service=None):
    if not spec.configuration.get(_KEY):
        return
    binding = _binding(services.constants, node_id)
    from ..resources.capacity import inherited_envelope

    def check():
        services.check_stop()
        services.journal.check_grant()
        envelope = inherited_envelope()
        if envelope is None or envelope['memory_bytes'] < binding['max_rss_bytes']:
            raise _error('Managed GPU service has no original worker memory grant')
        visible = os.environ.get('CUDA_VISIBLE_DEVICES', '').split(',')
        if set(visible) != set(binding['device_ids']) or len(visible) != len(binding['device_ids']):
            raise _error('Managed GPU service UUIDs differ from the actual worker grant')
        return binding

    check()
    service = actor.service if service is None else service
    if tuple(service.gpus) != tuple(binding['indices']):
        raise _error('Managed service GPU declaration differs from its original device binding')
    service._worker_device_binding = binding
    service._worker_device_check = check
