"""Sealed managed GPU deployment binding; allocation stays in DevicePool.

Only the trusted service environment supplies inventory and its shared resource
root. A flow declares resource requests, never a device path or an allocator.
The optional new manifest field is hashed by the existing manifest authority.
Old manifests have no field and retain their original CPU/thread behavior.
"""
import json
import os
from pathlib import Path

from demiflow.forge.platform import bounded_json
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.forge.io import no_symlinks

FORMAT = 'demiflow_managed_native_resources_v1'
INVENTORY = 'native-device-inventory.json'
MAX_BYTES = 65536


def capacity_domain(value=None):
    """Capture the trusted deployment domain before inspector TMPDIR changes."""
    from demiflow.execution.host.control import local_capacity, local_capacity_domain
    root = str(local_capacity()[0]) if value is None else value
    with local_capacity_domain(root):
        pass
    return root


def environment_names(value):
    """Names only; credential values never enter the run manifest or report."""
    import re
    if (type(value) is not list or len(value) > 32
            or any(type(name) is not str or not re.fullmatch(r'[A-Z_][A-Z0-9_]{0,127}', name)
                   or name in {'HOME', 'PATH', 'TMPDIR', 'LD_PRELOAD', 'LD_LIBRARY_PATH'}
                   or name.startswith(('PYTHON', 'DEMIFLOW_', 'CUDA_')) for name in value)
            or len(set(value)) != len(value)):
        raise ForgeError('Invalid deployment execution environment names')
    return sorted(value)


def executable_directories(value, *, workspace=None):
    if type(value) is not list or len(value) > 8 or any(type(p) is not str for p in value):
        raise ForgeError('Invalid deployment executable directories')
    result = []
    for item in value:
        path = no_symlinks(Path(item))
        if (not path.is_absolute() or str(path.resolve()) != item or not path.is_dir()
                or ':' in item or (workspace is not None and path.is_relative_to(Path(workspace).resolve()))):
            raise ForgeError('Executable directories must be canonical deployment directories outside the project')
        if item not in result:
            result.append(item)
    return result


def trusted_process_environment(*, workspace):
    """Deployment-owned per-project CLI credentials/settings, never request input."""
    path = os.environ.get('DEMIFLOW_WORKBENCH_RUN_ENVIRONMENT')
    if not path:
        return dict(environment_names=[], executable_dirs=[])
    path = no_symlinks(Path(path))
    root = Path(workspace).absolute()
    if not path.is_absolute() or path.is_relative_to(root):
        raise ForgeError('Execution environment declaration must be deployment-owned')
    value = json.loads(read_bounded(path, MAX_BYTES))
    if (type(value) is not dict or set(value) - {'executable_dirs'} != {'format', 'workspaces'}
            or value['format'] != 'demiflow_managed_environment_v1'
            or type(value['workspaces']) is not dict or len(value['workspaces']) > 128):
        raise ForgeError('Invalid deployment execution environment declaration')
    directories = value.get('executable_dirs', {})
    if type(directories) is not dict or len(directories) > 128:
        raise ForgeError('Invalid deployment executable directory declaration')
    return dict(environment_names=environment_names(value['workspaces'].get(str(root), [])),
                executable_dirs=executable_directories(directories.get(str(root), []), workspace=root))


def trusted_environment(*, workspace):
    return trusted_process_environment(workspace=workspace)['environment_names']


def validate(value):
    """Validate an already sealed/trusted declaration without allocating."""
    if type(value) is not dict or set(value) != {
            'format', 'worker_mode', 'resource_root', 'device_inventory', 'virtual_address_bytes'}:
        raise ForgeError('Invalid managed native resource declaration')
    if value['format'] != FORMAT or value['worker_mode'] != 'process':
        raise ForgeError('Unknown managed native resource protocol')
    root = value['resource_root']
    if type(root) is not str or len(root.encode()) > 4096 or not Path(root).is_absolute():
        raise ForgeError('Native resource root must be an explicit absolute deployment path')
    no_symlinks(Path(root))
    if str(Path(root).resolve()) != root:
        raise ForgeError('Native resource root must be canonical')
    ceiling = value['virtual_address_bytes']
    # This is virtual address space, NOT admitted RAM or device memory. It is
    # deployment supplied, finite, and independent of the unchanged RSS guard.
    if type(ceiling) is not int or not 1024**3 <= ceiling <= 128*1024**4:
        raise ForgeError('GPU virtual address ceiling must be finite in 1 GiB..128 TiB')
    from demiflow.resources.device_pool import validate_inventory
    from demiflow.resources.contracts import ResourceError
    try:
        devices = validate_inventory(value['device_inventory'])
    except ResourceError as exc:
        raise ForgeError('Invalid managed device inventory: ' + str(exc)) from exc
    if any(device['type'] != 'gpu' for device in devices):
        raise ForgeError('Managed native inventory requires GPU devices')
    canonical = {**value, 'device_inventory': {'devices': devices}}
    if canonical != value:
        raise ForgeError('Managed native inventory is not canonical')
    bounded_json(canonical, MAX_BYTES)
    return canonical


def trusted_deployment(*, workspace):
    """Capture service configuration before the isolated inspection worker."""
    path = os.environ.get('DEMIFLOW_DEVICE_INVENTORY')
    if not path:
        return None
    if not Path(path).is_absolute() or Path(path).resolve().is_relative_to(Path(workspace).resolve()):
        raise ForgeError('Device inventory must be an absolute deployment file outside the flow workspace')
    resource_root = os.environ.get('DEMIFLOW_RESOURCE_ROOT')
    ceiling = os.environ.get('DEMIFLOW_WORKBENCH_GPU_VIRTUAL_BYTES')
    if not resource_root or not ceiling or not ceiling.isdecimal():
        raise ForgeError('Managed GPU deployment requires an explicit shared resource root and finite virtual address ceiling')
    no_symlinks(Path(path))
    try:
        inventory = json.loads(read_bounded(Path(path), MAX_BYTES))
    except (OSError, ValueError) as exc:
        raise ForgeError('Invalid deployment device inventory') from exc
    return validate(dict(format=FORMAT, worker_mode='process', resource_root=resource_root,
                         device_inventory=inventory, virtual_address_bytes=int(ceiling)))


def select(report, deployment, *, workspace, training_deployment=None):
    """Bind declared GPU operators to the service's existing resource domain."""
    requested = any((n.get('capability_fact', {}).get('resource_declaration') or {}).get('gpu', 0)
                    for n in report['nodes'])
    if training_deployment and training_deployment['hosting'] == 'EngineManaged':
        requested |= any(node.get('analysis', {}).get('topology', {}).get('gpus_per_node', 0) > 0
                         for node in report['nodes'] if node.get('kind') == 'fit')
    if not requested:
        return None
    if deployment is None:
        raise ForgeError('GPU operators require an explicit managed deployment inventory')
    value = validate(deployment)
    if Path(value['resource_root']).is_relative_to(Path(workspace).resolve()):
        raise ForgeError('GPU resource root must be deployment-owned, outside the flow workspace')
    return value


def seal(target, value):
    if value is None:
        return
    value = validate(value)
    path = Path(target) / INVENTORY
    with path.open('xb') as stream:
        stream.write(bounded_json(value['device_inventory'], MAX_BYTES))
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(0o400)


def verify(target, value):
    if value is None:
        return None
    value = validate(value)
    path = Path(target) / INVENTORY
    no_symlinks(path)
    if read_bounded(path, MAX_BYTES) != bounded_json(value['device_inventory'], MAX_BYTES):
        raise ForgeError('Sealed deployment device inventory differs')
    return value


def environment(target, value):
    value = verify(target, value)
    if value is None:
        return {}
    # Both supervisor and executor use the same immutable inventory and the
    # same deployment root. Never create a separate per-run device ledger.
    return {'DEMIFLOW_DEVICE_INVENTORY': str(Path(target) / INVENTORY),
            'DEMIFLOW_RESOURCE_ROOT': value['resource_root']}
