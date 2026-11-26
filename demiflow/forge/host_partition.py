"""Optional fixed host envelope shared by run, development and analysis gates.

This is deployment configuration, not a second scheduler or a live usage ledger.
An administrator installs it only while participants are quiescent. External
processes and committed output retention require explicitly reserved headroom.
"""
from pathlib import Path
import os

from demiflow.forge.io import no_symlinks
from demiflow.forge.io import read_json
from demiflow.forge.schema import ForgeError
from demiflow.forge.schema import digest

MiB = 1024**2
REQUEST_HISTORY = 16 * MiB
DOMAIN_CONTROL = 4 * MiB
READONLY_RSS = 4 * 1024**3
OTHER_MEMORY_MINIMUM = READONLY_RSS + 1024**3
FORMAT = 'demiflow_forge_host_budget_v1'
FORMAT_V2 = 'demiflow_forge_host_budget_v2'

DEFAULTS = dict(
    max_active=1,
    max_rss_bytes=8 * 1024**3,
    cpu_threads=7,
    device_bytes=16 * 1024**3,
    history_bytes=4 * 1024**3,
)
PREPARATION_RSS = 768 * MiB


def location():
    # Deliberately independent of tempfile/TMPDIR, project and HTTP registry.
    path = Path(
        os.environ.get(
            "DEMIFLOW_FORGE_DOMAIN", f"/var/tmp/demiflow-forge-{os.getuid()}"
        )
    )
    if not path.is_absolute():
        raise ForgeError("Forge deployment domain must be absolute")
    no_symlinks(path)
    return path


def policy(path):
    value = (
        read_json(path / "policy.json", maximum=4096)
        if (path / "policy.json").exists()
        else {}
    )
    if type(value) is not dict or set(value) - DEFAULTS.keys():
        raise ForgeError("Invalid Forge deployment policy")
    result = {**DEFAULTS, **value}
    if (
        any(type(v) is not int or v <= 0 for v in result.values())
        or result["max_active"] > 16
    ):
        raise ForgeError("Invalid Forge deployment capacity")
    if result["history_bytes"] > 64 * 1024**3:
        raise ForgeError("History capacity exceeds 64 GiB")
    load(path, result)
    return result


def _path(value):
    if not isinstance(value, str) or len(value) > 2048 or not Path(value).is_absolute():
        raise ForgeError('Host partition paths must be absolute')
    path = Path(value)
    no_symlinks(path)
    return path


def _ancestor(path):
    for _ in range(32):
        if path.exists(): return path
        path = path.parent
    raise ForgeError('Host partition path exceeds ancestor budget')


_ANALYSIS_PROFILE_PROVIDER = None


def register_analysis_profile_provider(provider) -> None:
    """Allow the optional Workbench analysis module to register its v2 resource profile."""
    global _ANALYSIS_PROFILE_PROVIDER
    _ANALYSIS_PROFILE_PROVIDER = provider


def _analysis(value, development, workspaces, *, require_profile=False):
    """A v2 reservation in this same host authority, not a parallel budget."""
    fields = {'memory_bytes', 'state_root', 'state_disk_bytes', 'image_roots', 'profile_sha256'}
    if type(value) is not dict or set(value) != fields or type(value['profile_sha256']) is not str:
        raise ForgeError('Analysis host reservation requires this exact resource profile')
    for key in ('memory_bytes', 'state_disk_bytes'):
        if type(value[key]) is not int or not 0 < value[key] <= 2**60:
            raise ForgeError('Analysis host reservation requires positive bounded resources')
    if _ANALYSIS_PROFILE_PROVIDER is not None:
        resource, profile_sha = _ANALYSIS_PROFILE_PROVIDER()
        if value['profile_sha256'] != profile_sha:
            raise ForgeError('Analysis host reservation requires this exact resource profile')
        minimum_disk = resource['payload_limits']['global_artifact_bytes'] + resource['database']['region_reserved_bytes'] + 256*MiB + 64*MiB
        if value['memory_bytes'] < resource['memory']['analysis'] or value['state_disk_bytes'] < minimum_disk:
            raise ForgeError('Analysis host reservation is below its state/memory profile', code='resource_limit')
    elif require_profile:
        raise ForgeError('Analysis host reservation requires Workbench analysis resource profile', code='operation_rejected')
    state = _path(value['state_root'])
    if str(state) != value['state_root'] or '..' in state.parts:
        raise ForgeError('Analysis state must be a normalized physical path')
    protected = [_path(development), *[_path(item) for item in workspaces]]
    images = value['image_roots']
    if type(images) is not list or not 1 <= len(images) <= 4:
        raise ForgeError('Analysis host partition requires 1..4 explicit image/rollback roots')
    by_device = {_ancestor(state).stat().st_dev: value['state_disk_bytes']}
    owned = [state]
    for image in images:
        if type(image) is not dict or set(image) != {'path', 'bytes'} or type(image['bytes']) is not int or not 0 < image['bytes'] <= 2**60:
            raise ForgeError('Invalid analysis image reservation')
        path = _path(image['path'])
        if str(path) != image['path'] or '..' in path.parts:
            raise ForgeError('Analysis image root must be normalized')
        owned.append(path)
        device = _ancestor(path).stat().st_dev
        by_device[device] = by_device.get(device, 0) + image['bytes']
    for index, path in enumerate(owned):
        if any(path == other or path.is_relative_to(other) or other.is_relative_to(path) for other in protected + owned[index + 1:]):
            raise ForgeError('Analysis state/images must be disjoint from business/development roots and each other')
    return dict(value=value, memory_bytes=value['memory_bytes'], by_device=by_device,
                state_device=_ancestor(state).stat().st_dev)


def load(domain, capacity, *, required=False, root=None, devices=(), admit=False, require_analysis_profile=False):
    path = Path(domain) / 'host-budget.json'
    if not path.exists():
        if required: raise ForgeError('Combined development requires an explicit host-budget.json')
        return None
    value = read_json(path, maximum=32768)
    fields = {'format', 'memory_bytes', 'other_memory_bytes', 'development_memory_bytes',
              'development_disk_bytes', 'development_state_root', 'workspaces', 'devices'}
    if type(value) is not dict or type(value.get('format')) is not str or value['format'] not in {FORMAT, FORMAT_V2}:
        raise ForgeError('Invalid fixed host partition')
    v2 = value['format'] == FORMAT_V2
    if set(value) != fields | ({'analysis'} if v2 else set()):
        raise ForgeError('Invalid fixed host partition')
    for name in ('memory_bytes', 'other_memory_bytes', 'development_memory_bytes', 'development_disk_bytes'):
        if type(value[name]) is not int or not 0 < value[name] <= 2**60:
            raise ForgeError('Host partition requires positive bounded resource reservations')
    if value['other_memory_bytes'] < OTHER_MEMORY_MINIMUM:
        raise ForgeError('Host other-memory partition must cover the read worker and shared control headroom', code='resource_limit')
    roots = value['workspaces']
    if type(roots) is not list or not 1 <= len(roots) <= 128:
        raise ForgeError('Host partition requires 1..128 explicit runtime workspaces')
    identities = set()
    for item in roots:
        workspace = _path(item)
        if not workspace.is_dir(): raise ForgeError('Host partition workspace is unavailable')
        info = workspace.stat(); identity = (info.st_dev, info.st_ino)
        if identity in identities: raise ForgeError('Duplicate host partition workspace identity')
        identities.add(identity)
    if root is not None:
        candidate = _path(str(root)).stat()
        if (candidate.st_dev, candidate.st_ino) not in identities:
            raise ForgeError('Runtime workspace is outside the fixed host history partition', code='resource_limit')
    state = _path(value['development_state_root'])
    state_device = _ancestor(state).stat().st_dev
    analysis = _analysis(value['analysis'], value['development_state_root'], roots, require_profile=require_analysis_profile) if v2 else None
    run_memory = capacity['max_rss_bytes']
    # The domain RSS ceiling already includes its control/preparation trees.
    memory = run_memory + value['development_memory_bytes'] + value['other_memory_bytes'] + (analysis['memory_bytes'] if analysis else 0)
    if memory > value['memory_bytes']:
        raise ForgeError('Actual runtime and development RSS reservations exceed host partition', code='resource_limit')
    # History is per workspace, not per active slot. Charge its entire bound on
    # every declared device to cover separately located retained native objects.
    run_disk = capacity['device_bytes'] + len(roots) * (capacity['history_bytes'] + REQUEST_HISTORY) + DOMAIN_CONTROL
    rows = value['devices']
    if type(rows) is not list or not 1 <= len(rows) <= 16:
        raise ForgeError('Host partition requires 1..16 explicit devices')
    by_device = {}
    for row in rows:
        if type(row) is not dict or set(row) != {'path', 'bytes', 'other_bytes'}:
            raise ForgeError('Invalid host device partition')
        for key in ('bytes', 'other_bytes'):
            if type(row[key]) is not int or not 0 < row[key] <= 2**60:
                raise ForgeError('Host device reserves must be positive bounded bytes')
        device_path = _path(row['path'])
        device = device_path.stat().st_dev
        if device in by_device: raise ForgeError('Duplicate host device identity')
        chat = value['development_disk_bytes'] if device == state_device else 0
        analysis_disk = analysis['by_device'].get(device, 0) if analysis else 0
        total = run_disk + chat + analysis_disk + row['other_bytes']
        if total > row['bytes']:
            raise ForgeError('Actual runtime history/device and development reservations exceed host partition', code='resource_limit')
        space = os.statvfs(device_path)
        if row['bytes'] > space.f_blocks * space.f_frsize:
            raise ForgeError('Declared host partition exceeds physical device capacity', code='resource_limit')
        if admit and total > space.f_bavail * space.f_frsize:
            # Deliberately conservative: no credit for existing allocations.
            raise ForgeError('Insufficient free space for the combined host reservation', code='resource_limit')
        by_device[device] = dict(row, run_bytes=run_disk, development_bytes=chat, reserved_bytes=total)
        if analysis is not None:
            by_device[device]['analysis_bytes'] = analysis_disk
    if state_device not in by_device or any(int(d) not in by_device for d in devices):
        raise ForgeError('Runtime/development device is outside the fixed host partition', code='resource_limit')
    if analysis is not None and not set(analysis['by_device']) <= set(by_device):
        raise ForgeError('Analysis state/image device is outside the fixed host partition', code='resource_limit')
    if admit:
        with open('/proc/meminfo', 'rb') as stream: raw = stream.read(32769)
        if len(raw) > 32768: raise ForgeError('Host memory observation exceeds budget')
        memory_info = dict(line.split(b':', 1) for line in raw.splitlines())
        available = int(memory_info[b'MemAvailable'].split()[0]) * 1024
        if memory > available:
            raise ForgeError('Insufficient available memory for the combined host reservation', code='resource_limit')
    result = dict(digest=digest(value), value=value, run_memory_bytes=run_memory,
                run_disk_bytes=run_disk, memory_reserved_bytes=memory,
                devices=by_device, development_device=state_device)
    if analysis is not None:
        result.update(analysis_memory_bytes=analysis['memory_bytes'], analysis_device=analysis['state_device'])
    return result


def load_analysis(domain, capacity, *, state_root, image_root, admit=False):
    """Static/admission accounting only; no cgroup or execution authorization."""
    partition = load(domain, capacity, required=True, admit=admit, require_analysis_profile=True)
    if partition['value']['format'] != FORMAT_V2:
        raise ForgeError('Analysis requires an explicitly installed host-budget v2; v1 remains unchanged', code='operation_rejected')
    value = partition['value']['analysis']
    if str(state_root) != value['state_root'] or str(image_root) not in {item['path'] for item in value['image_roots']}:
        raise ForgeError('Analysis state/image differs from the installed shared host owner', code='conflict')
    return partition
