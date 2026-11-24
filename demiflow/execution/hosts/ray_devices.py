"""Read-only Ray/NVML binding evidence; all balances remain in DevicePool.

Ray owns physical allocation. The framework shell reports that allocation before
user activation; the original device pool then admits exactly those devices.
"""
from dataclasses import dataclass
import os
from types import MappingProxyType

from demiflow.execution.contracts.dataflow_errors import DataflowError

MAX_DEVICES = 64


def _error(message):
    return DataflowError('UNSUPPORTED_CAPABILITY', message)


def _nvml_inventory():
    # NVML queries do not initialize a CUDA context or allocate device memory.
    from ray._private.thirdparty import pynvml
    pynvml.nvmlInit()
    try:
        count = pynvml.nvmlDeviceGetCount()
        if not 0 <= count <= MAX_DEVICES:
            raise _error('GPU inventory exceeds its device bound')
        result = []
        for index in range(count):
            handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            uuid = pynvml.nvmlDeviceGetUUID(handle)
            if isinstance(uuid, bytes):
                uuid = uuid.decode('ascii')
            pci = pynvml.nvmlDeviceGetPciInfo(handle).busId
            if isinstance(pci, bytes):
                pci = pci.decode('ascii')
            try:
                mig = pynvml.nvmlDeviceGetMigMode(handle)[0]
            except pynvml.NVMLError_NotSupported:
                mig = 0
            result.append({'uuid': uuid, 'index': index, 'pci_bus_id': pci,
                           'memory_bytes': int(pynvml.nvmlDeviceGetMemoryInfo(handle).total),
                           'mig': bool(mig)})
        return result
    finally:
        pynvml.nvmlShutdown()


def _physical_device(identifier, inventory, *, device_order):
    text = str(identifier)
    exact = next((d for d in inventory if d['uuid'] == text), None)
    if exact is not None:
        if exact['mig']:
            raise _error('MIG device binding is not implemented')
        return exact
    # CUDA's default enumeration is not necessarily NVML index order. Do not
    # invent a UUID from an unproved numeric slot.
    if text.isdecimal() and device_order == 'PCI_BUS_ID':
        ordered = sorted(inventory, key=lambda d: d['pci_bus_id'])
        index = int(text)
        if index < len(ordered) and not any(d['mig'] for d in ordered):
            return ordered[index]
    raise _error('GPU identity requires UUID visibility or verified PCI_BUS_ID ordering')


def probe_ray_device_inventory():
    import ray
    import socket
    inventory = _nvml_inventory()
    original = ray._private.worker.global_worker.original_visible_accelerator_ids.get('GPU')
    visible = list(original) if original is not None else [str(i) for i in range(len(inventory))]
    if len(visible) > MAX_DEVICES:
        raise _error('Ray visible GPU domain exceeds its bound')
    order = os.environ.get('CUDA_DEVICE_ORDER')
    devices = []
    for identifier in visible:
        try:
            device = _physical_device(identifier, inventory, device_order=order)
        except DataflowError:
            if not str(identifier).isdecimal():
                raise
            device = None  # Actual granted shell resolves numeric CUDA order.
        devices.append({'ray_id': str(identifier), 'uuid': None if device is None else device['uuid']})
    return {'node_id': ray.get_runtime_context().get_node_id(),
            'hostname': socket.gethostname(), 'devices': devices, 'physical': inventory}


def _cuda_visible_uuids(expected_count):
    """Inspect only an already Ray-granted shell; no context/model allocation."""
    import ctypes
    class UUID(ctypes.Structure):
        _fields_ = [('bytes', ctypes.c_ubyte * 16)]
    try:
        driver = ctypes.CDLL('libcuda.so.1')
        init = driver.cuInit; init.argtypes = [ctypes.c_uint]; init.restype = ctypes.c_int
        count_fn = driver.cuDeviceGetCount; count_fn.argtypes = [ctypes.POINTER(ctypes.c_int)]
        count_fn.restype = ctypes.c_int
        uuid_fn = getattr(driver, 'cuDeviceGetUuid_v2', driver.cuDeviceGetUuid)
        uuid_fn.argtypes = [ctypes.POINTER(UUID), ctypes.c_int]; uuid_fn.restype = ctypes.c_int
        if init(0):
            raise _error('CUDA driver could not verify the granted GPU identity')
        count = ctypes.c_int()
        if count_fn(ctypes.byref(count)) or count.value != expected_count:
            raise _error('CUDA visible device count differs from the Ray grant')
        result = []
        for index in range(count.value):
            value = UUID()
            if uuid_fn(ctypes.byref(value), index):
                raise _error('CUDA driver could not resolve a granted GPU UUID')
            raw = bytes(value.bytes).hex()
            result.append('GPU-' + '-'.join((raw[:8], raw[8:12], raw[12:16], raw[16:20], raw[20:])))
        return result
    except (OSError, AttributeError) as exc:
        raise _error('CUDA driver identity verification is unavailable') from exc


def worker_device_binding():
    import ray
    context = ray.get_runtime_context()
    ids = [str(value) for value in context.get_accelerator_ids().get('GPU', [])]
    if len(ids) > MAX_DEVICES or len(set(ids)) != len(ids):
        raise _error('Ray GPU assignment exceeds its device bound')
    visible = os.environ.get('CUDA_VISIBLE_DEVICES')
    if visible is None:
        raise _error('Worker CUDA visibility is not restricted to its Ray grant')
    visible_ids = visible.split(',') if visible else []
    if sorted(visible_ids) != sorted(ids):
        raise _error('CUDA visibility differs from the actual Ray GPU assignment')
    inventory = _nvml_inventory() if ids else []
    devices = []
    uuids = (visible_ids if all(value.startswith('GPU-') for value in visible_ids)
             else _cuda_visible_uuids(len(visible_ids)))
    for identifier, uuid in zip(visible_ids, uuids):
        device = _physical_device(uuid, inventory, device_order=None)
        if identifier.startswith('GPU-') and identifier != uuid:
            raise _error('CUDA driver UUID differs from the Ray-visible UUID')
        devices.append({'ray_id': identifier, 'uuid': device['uuid'],
                        'memory_bytes': device['memory_bytes']})
    return {'schema': 'demiflow.ray_device_binding.v1',
            'node_id': context.get_node_id(), 'devices': devices,
            'ray_gpu_ids': ids, 'visible_gpu_ids': visible_ids}


@dataclass(frozen=True)
class RayDeviceTopology:
    # Entries are immutable deployment facts, not resource reservations.
    hosts: object
    pool_inventory: object
    allowed_ids: object
    physical: object = None

    def capacity(self, host_id):
        return min(len(self.hosts.get(host_id, ())), len(self.allowed_ids.get(host_id, ())))

    def feasible(self, host_id, profile):
        count = sum(x['count'] for x in profile.requests.get('accelerators', ())
                    if x['type'] == 'gpu')
        memory = (profile.requirements or {}).get('accelerator_memory_bytes')
        return min(self.capacity(host_id), sum(memory is None or memory <= d['memory_bytes']
                   for d in self.hosts.get(host_id, ()))) >= count

    def verify(self, binding, *, host_id, accelerators, device_memory_bytes=None):
        if (type(binding) is not dict or binding.get('schema') != 'demiflow.ray_device_binding.v1'
                or binding.get('node_id') != host_id.removeprefix('ray-')):
            raise _error('Worker GPU proof belongs to another Ray host')
        devices = binding.get('devices')
        count = sum(x['count'] for x in accelerators if x['type'] == 'gpu')
        if type(devices) is not list or len(devices) != count or count > MAX_DEVICES:
            raise _error('Worker GPU count differs from admitted accelerator demand')
        ids = [d.get('ray_id') for d in devices]
        uuids = [d.get('uuid') for d in devices]
        if (len(set(uuids)) != len(uuids) or sorted(binding.get('ray_gpu_ids', ())) != sorted(ids)
                or sorted(binding.get('visible_gpu_ids', ())) != sorted(ids)):
            raise _error('Worker GPU evidence is inconsistent')
        declared = {d['uuid']: d for d in self.hosts.get(host_id, ()) if d['uuid'] is not None}
        result = []
        for device in devices:
            claim = declared.get(device['uuid'])
            if claim is None:
                claim = next((d for d in self.hosts.get(host_id, ())
                              if d['uuid'] is None and d.get('ray_id') == device['ray_id']), None)
            actual_memory = (None if self.physical is None else
                             self.physical.get(host_id, {}).get(device['uuid']))
            if actual_memory is None and claim is not None:
                actual_memory = claim['actual_memory_bytes']
            if (claim is None or device['ray_id'] not in self.allowed_ids.get(host_id, ())
                    or actual_memory != device.get('memory_bytes')
                    or claim['memory_bytes'] > actual_memory
                    or device_memory_bytes is not None and device_memory_bytes > claim['memory_bytes']):
                raise _error('Ray assigned a GPU outside the verified deployment allowance')
            result.append(claim['device_id'])
        return tuple(result)


def load_ray_device_topology(declared, *, timeout_s=30):
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    from demiflow.resources.device_pool import _validate_inventory
    nodes = [n for n in ray.nodes() if n.get('Alive') and n.get('Resources', {}).get('GPU', 0) > 0]
    legacy = not (type(declared) is dict and declared.get('schema') == 'demiflow.device_inventory.v2')
    if not legacy:
        if set(declared) != {'schema', 'hosts'} or type(declared['hosts']) is not list:
            raise _error('Invalid versioned GPU deployment inventory')
        specs = declared['hosts']
    else:
        if len(nodes) != 1:
            raise _error('Legacy GPU inventory requires one GPU host; use explicit host inventory v2')
        specs = [{'hostname': nodes[0]['NodeManagerHostname'],
                  'devices': _validate_inventory(declared)}]
    if not 1 <= len(specs) <= MAX_DEVICES:
        raise _error('GPU host inventory exceeds its bound')
    hosts = {}; flattened = []; seen_hosts = set(); seen_uuids = set(); allowed_ids = {}; physical_hosts = {}
    for spec in specs:
        if type(spec) is not dict or set(spec) != {'hostname', 'devices'}:
            raise _error('GPU host entry requires hostname and devices')
        name = spec['hostname']
        if type(name) is not str or not 1 <= len(name) <= 256 or name in seen_hosts:
            raise _error('GPU host name must be bounded and unique')
        seen_hosts.add(name)
        matches = [n for n in nodes if n['NodeManagerHostname'] == name]
        if len(matches) != 1:
            raise _error('GPU host is not a unique alive Ray node')
        node = matches[0]; count = node['Resources']['GPU']
        if int(count) != count or not 1 <= count <= MAX_DEVICES:
            raise _error('Ray physical GPU inventory must have a bounded integral capacity')
        task = ray.remote(num_cpus=0, max_retries=0)(probe_ray_device_inventory).options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node['NodeID'], soft=False))
        future = task.remote()
        try:
            proof = ray.get(future, timeout=timeout_s)
        except BaseException:
            ray.cancel(future, force=True)
            raise
        if proof['node_id'] != node['NodeID'] or proof['hostname'] != name:
            raise _error('GPU inventory probe ran on another host')
        # NVML can see cards outside the Ray node's allowed slots. Only the
        # original visible domain, narrowed by its advertised GPU count, counts.
        allowed = proof['devices'][:int(count)]
        opaque = any(d['uuid'] is None for d in allowed)
        physical = {d['uuid']: d for d in proof['physical'] if not d['mig']}
        admitted_uuids = set(physical) if opaque else {d['uuid'] for d in allowed}
        host_id = 'ray-' + node['NodeID']
        allowed_ids[host_id] = tuple(d['ray_id'] for d in allowed)
        physical_hosts[host_id] = MappingProxyType({uuid:d['memory_bytes'] for uuid,d in physical.items()})
        entries = []
        for device in _validate_inventory({'devices': spec['devices']}):
            if device['type'] != 'gpu':
                raise _error('Ray device binding supports GPU only')
            matches = [d for d in physical.values() if d['uuid'] in admitted_uuids
                       and device['device_id'] == d['uuid']]
            if not matches:
                mapped = [d['uuid'] for d in allowed if d['ray_id'] == device['device_id'] and d['uuid']]
                matches = [physical[value] for value in mapped if value in physical]
            if (not matches and legacy and device['device_id'] in allowed_ids[host_id]
                    and any(d['ray_id'] == device['device_id'] and d['uuid'] is None for d in allowed)):
                if not physical or device['memory_bytes'] > max(d['memory_bytes'] for d in physical.values()):
                    raise _error('Legacy device memory exceeds every real Ray-visible card')
                entries.append(MappingProxyType({**device,'uuid':None,'ray_id':device['device_id'],
                                                'actual_memory_bytes':None}))
                flattened.append(device)
                continue
            if len(matches) != 1 or device['memory_bytes'] > matches[0]['memory_bytes']:
                raise _error('Declared GPU is outside the Ray-visible device/memory allowance')
            actual = matches[0]
            if actual['uuid'] in seen_uuids:
                raise _error('GPU UUID is duplicated across deployment entries')
            seen_uuids.add(actual['uuid'])
            entries.append(MappingProxyType({**device, 'uuid': actual['uuid'],
                            'memory_bytes': device['memory_bytes'],
                            'actual_memory_bytes': actual['memory_bytes']}))
            flattened.append(device)
        hosts[host_id] = tuple(entries)
    if len(flattened) > MAX_DEVICES:
        raise _error('Deployment GPU inventory exceeds its device bound')
    _validate_inventory({'devices': flattened})
    return RayDeviceTopology(MappingProxyType(hosts), tuple(flattened), MappingProxyType(allowed_ids),
                             MappingProxyType(physical_hosts))
