"""Workspace controls, immutable table references and snapshot-bound reports."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import stat
import tempfile
import uuid

from demiflow.workbench.projects.io import workspace_id
from demiflow.workbench.projects.io import identifier
from demiflow.workbench.projects.io import relative_path
from demiflow.workbench.projects.io import read_control
from demiflow.workbench.projects.io import control_lock
from demiflow.workbench.projects.io import write_control
from demiflow.forge.io import no_symlinks
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.platform_snapshot import build_snapshot
from demiflow.forge.platform_snapshot import check_snapshot
from demiflow.forge.platform_snapshot import load_snapshot
from demiflow.forge.platform_snapshot import write_snapshot
from demiflow.forge.schema import ForgeError

FORMAT = "demiflow_forge_workspace_v1"
FORMAT_V2 = "demiflow_forge_workspace_v2"
FORMAT_V3 = "demiflow_forge_workspace_v3"
MODERN_FORMATS = {FORMAT_V2, FORMAT_V3}
README_BYTES = 1024**2




def project_name(value):
    if (type(value) is not str or not value.strip() or value != value.strip()
            or len(value.encode('utf-8')) > 256 or any(ord(c) < 32 for c in value)):
        raise ForgeError('Project name requires 1..256 UTF-8 bytes without control characters', code='invalid_request')
    return value










def init_workspace(root, *, name="workspace", modules=("demiflow.data",), existing=False, extras=()):
    root = Path(root).absolute()
    no_symlinks(root)
    project_name(name)
    if not existing and root.exists() and any(root.iterdir()):
        raise ForgeError("Workspace initialization requires an empty directory")
    if (root / 'workspace.json').exists() or (root / '.forge').exists():
        raise ForgeError('Workspace controls already exist; refusing initialization')
    snapshot = build_snapshot(modules, extras=extras)
    snapshot_path = ".forge/platform/" + snapshot["digests"]["snapshot"] + ".json"
    value = {
        "format": FORMAT_V3,
        "audit": {"reports": [], "change_scope": None},
        "id": 'w' + uuid.uuid4().hex,
        "name": name,
        "entries": {},
        "locations": {},
        "platform": {"path": snapshot_path, "digest": snapshot["digests"]["snapshot"]},
    }
    created_dirs, created_files = [], []
    try:
        if not root.exists():
            root.mkdir(parents=True)
            created_dirs.append(root)
        # Exclusive control-directory creation also serializes competing initializers.
        (root / '.forge').mkdir()
        created_dirs.append(root / '.forge')
        for directory in ('.forge/platform', '.forge/inspections'):
            path = root / directory
            if not path.exists():
                path.mkdir()
                created_dirs.append(path)
        write_snapshot(root / snapshot_path, snapshot)
        created_files.append(root / snapshot_path)
        lease = root / '.forge/execution.lock'
        with lease.open('x') as stream:
            stream.write('{"format":"demiflow_forge_execution_owner_v1","run_id":null}')
            stream.flush()
            os.fsync(stream.fileno())
        created_files.append(lease)
        from demiflow.workbench.projects import index
        index._ensure(root)
        created_files.append(root / '.forge/index.sqlite')
        write_control(root / 'workspace.json', value, create=True)
    except BaseException:
        for path in reversed(created_files):
            path.unlink(missing_ok=True)
        for path in reversed(created_dirs):
            try:
                path.rmdir()
            except OSError:
                pass  # Never remove contents created by another actor.
        raise
    return {"status": "created", "workspace": str(root), "id": value['id'], "platform": value["platform"]}


def load_workspace(root):
    root = Path(root).absolute()
    no_symlinks(root)
    value = read_control(root / "workspace.json")
    modern = type(value) is dict and value.get('format') in MODERN_FORMATS
    audit_fields = {'audit'} if type(value) is dict and value.get('format') == FORMAT_V3 else set()
    if (type(value) is not dict or value.get('format') not in {FORMAT, *MODERN_FORMATS}
            or set(value) - ({'budgets'} if modern else set()) != {'format', 'name', 'locations', 'platform'} | ({'id', 'entries'} if modern else set()) | audit_fields):
        raise ForgeError("Unsupported workspace format")
    if audit_fields:
        from demiflow.forge.audit import validate_policy
        validate_policy(value['audit'])
    if modern:
        from demiflow.workbench.runs.limits import limits_with
        limits_with(policy=value.get('budgets'))
        if not isinstance(value['id'], str) or not re.fullmatch(r'w[0-9a-f]{32}', value['id']):
            raise ForgeError('Invalid workspace identity')
        if type(value['entries']) is not dict or len(value['entries']) > 256:
            raise ForgeError('Invalid registered entry index')
        for entry, parameters in value['entries'].items():
            validate_entry(root, entry)
            if type(parameters) is not list or not 1 <= len(parameters) <= 64 or any(type(p) is not str for p in parameters) or len(set(parameters)) != len(parameters):
                raise ForgeError('Entry requires 1..64 distinct parameter paths')
            for params in parameters:
                validate_parameter_path(root, params)
    project_name(value["name"])
    if type(value["locations"]) is not dict or not 0 <= len(value["locations"]) <= 16:
        raise ForgeError("Invalid location count")
    for key, location in value["locations"].items():
        identifier(key)
        if (
            set(location) not in ({"root", "writable"}, {"root", "writable", "write_modes"})
            or type(location["writable"]) is not bool
            or not isinstance(location["root"], str)
        ):
            raise ForgeError("Invalid data location")
        if 'write_modes' in location:
            modes = location['write_modes']
            if not modern or type(modes) is not list or len(modes) > 3 or len(set(modes)) != len(modes) or set(modes) - {'create', 'overwrite', 'checkpoint'} or bool(modes) != location['writable']:
                raise ForgeError('Invalid explicit location write modes')
        if not modern and location["writable"] and (key != "local" or location["root"] != "data"):
            raise ForgeError("Only workspace data/ may be writable")
        location_root(root, location)
    if set(value["platform"]) != {"path", "digest"}:
        raise ForgeError("Invalid platform binding")
    path = relative_path(root, value["platform"]["path"])
    if path.parent != root / ".forge/platform":
        raise ForgeError("Platform snapshot must be in .forge/platform")
    snapshot = load_snapshot(path)
    if (
        snapshot["digests"]["snapshot"] != value["platform"]["digest"]
        or snapshot["scope"]["discovery_version"] not in (2, 3)
    ):
        raise ForgeError("Workspace platform binding requires a matching supported snapshot")
    return root, value, snapshot



def _audit_workspace(root, expected):
    root, value, _ = load_workspace(root)
    if value['format'] != FORMAT_V3:
        raise ForgeError('Explicitly enable workspace v3 audit first')
    if content_digest(value) != expected:
        raise ForgeError('Workspace control changed', code='conflict')
    return root, value


def _publish_audit_policy(root, value, expected, **details):
    from demiflow.forge.audit import validate_policy
    validate_policy(value['audit'])
    with control_lock(root):
        write_control(root / 'workspace.json', value, expected=expected)
    return dict(status='updated', workspace_digest=content_digest(value), audit=value['audit'], **details)


def enable_audit(root, *, expected):
    root, value, _ = load_workspace(root)
    if content_digest(value) != expected:
        raise ForgeError('Workspace control changed', code='conflict')
    if value['format'] not in MODERN_FORMATS:
        raise ForgeError('Audit requires an explicitly registered modern workspace')
    value['format'] = FORMAT_V3
    value.setdefault('audit', dict(reports=[], change_scope=None))
    return _publish_audit_policy(root, value, expected)


def bind_audit_report(root, digest, *, expected):
    root, value = _audit_workspace(root, expected)
    result = report_status(root, digest)
    report = result['report']
    binding = report['binding']
    entry, params = binding['entry'], binding['parameter_path']
    if result['status'] != 'checked' or not params or params not in value['entries'].get(entry, []):
        raise ForgeError('Only a current checked registered entry/parameter report can be tracked')
    pins = []
    for old in value['audit']['reports']:
        original = load_report(root, old)['binding']
        if (original['entry'], original['parameter_path']) != (entry, params):
            pins.append(old)
    value['audit']['reports'] = pins + [digest]
    return _publish_audit_policy(root, value, expected)


def unbind_audit_report(root, digest, *, expected):
    from demiflow.forge.audit import digest as valid_digest
    valid_digest(digest)
    root, value = _audit_workspace(root, expected)
    if digest not in value['audit']['reports']:
        raise ForgeError('Report is not tracked', code='not_found')
    value['audit']['reports'].remove(digest)
    return _publish_audit_policy(root, value, expected)


def _author_roots(root, value, names):
    from demiflow.forge.audit import roots
    names = roots(names)
    locations = [location_root(root, item) for item in value['locations'].values()]
    for name in names:
        path = relative_path(root, name)
        if any(path.is_relative_to(p) or p.is_relative_to(path) for p in locations):
            raise ForgeError('Author inventory must not overlap a registered data location')
    return names


def _scope_baseline(root, value):
    from demiflow.forge import audit
    scope = value.get('audit', {}).get('change_scope')
    if scope is None:
        return None
    baseline = audit.validate_baseline(read_control(
        root / '.forge/audit/baselines' / (scope['baseline'] + '.json'), audit.BASELINE_BYTES))
    if baseline['digest'] != scope['baseline'] or baseline['workspace_id'] != value['id'] or baseline['root'] != str(root):
        raise ForgeError('Change baseline identity differs', code='conflict')
    _author_roots(root, value, baseline['watch_roots'])
    audit.writes(scope['writes'], baseline['watch_roots'])
    return baseline


def _capture_change_baseline(root, *, watch_roots, expected, limits=None):
    from demiflow.forge import audit
    import time
    root, value = _audit_workspace(root, expected)
    names = _author_roots(root, value, watch_roots)
    limits = audit.limits_with(limits)
    baseline = dict(format=audit.FORMAT, workspace_id=value['id'], root=str(root),
                    watch_roots=names, inventory_rules_version=1, limits=limits,
                    entries=audit.inventory(root, names, limits),
                    workspace_policy_sha256=audit.workspace_digest(value), created_at=time.time())
    baseline['digest'] = content_digest(baseline)
    if content_digest(read_control(root / 'workspace.json')) != expected:
        raise ForgeError('Workspace changed during baseline capture', code='conflict')
    return audit.validate_baseline(baseline)


def _store_scope(root, value, baseline, writes, expected):
    from demiflow.forge import audit
    import shutil
    if value['audit']['change_scope'] is not None:
        raise ForgeError('End the current scope explicitly before starting another', code='conflict')
    baseline = audit.validate_baseline(baseline)
    if baseline['workspace_id'] != value['id'] or baseline['root'] != str(root):
        raise ForgeError('Imported baseline belongs to another workspace', code='conflict')
    _author_roots(root, value, baseline['watch_roots'])
    allowed = audit.writes(writes, baseline['watch_roots'])
    size = len(bounded_json(baseline, audit.BASELINE_BYTES))
    directory = no_symlinks(root / '.forge/audit/baselines')
    with control_lock(root):
        if content_digest(read_control(root / 'workspace.json')) != expected:
            raise ForgeError('Workspace control changed', code='conflict')
        directory.mkdir(parents=True, exist_ok=True)
        count = total = 0
        with os.scandir(directory) as entries:
            for entry in entries:
                count += 1
                info = entry.stat(follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode):
                    raise ForgeError('Baseline storage contains nonregular entries')
                total += info.st_size
                if count > 1024 or total > 256*1024**2:
                    raise ForgeError('Baseline storage budget exceeded', code='resource_limit')
        target = directory / (baseline['digest'] + '.json')
        if not target.exists():
            if count >= 1024 or total + size > 256*1024**2 or shutil.disk_usage(directory).free < size*2 + 8*1024**2:
                raise ForgeError('Insufficient baseline storage budget', code='resource_limit')
            write_control(target, baseline, create=True)
        elif read_control(target, audit.BASELINE_BYTES) != baseline:
            raise ForgeError('Stored baseline differs', code='conflict')
        value['audit']['change_scope'] = dict(baseline=baseline['digest'], writes=allowed)
        write_control(root / 'workspace.json', value, expected=expected)
    return dict(status='updated', workspace_digest=content_digest(value), audit=value['audit'])


def begin_change_scope(root, *, writes, expected, watch_roots=None, limits=None):
    from demiflow.workbench.worker import operation
    root, value = _audit_workspace(root, expected)
    if value['audit']['change_scope'] is not None:
        raise ForgeError('A change scope is already active', code='conflict')
    names = author_roots(root, value) if watch_roots is None else watch_roots
    if not names:
        names = sorted({item['path'].split('/')[0] for item in writes if item['path'] != 'workspace.json'})
    baseline = operation('audit_capture', root, watch_roots=names,
                         expected=expected, limits=limits)
    return _store_scope(root, value, baseline, writes, expected)


def import_change_scope(root, baseline_path, *, writes, expected):
    from demiflow.forge.audit import BASELINE_BYTES
    root, value = _audit_workspace(root, expected)
    baseline = read_control(no_symlinks(Path(baseline_path).absolute()), BASELINE_BYTES)
    return _store_scope(root, value, baseline, writes, expected)


def amend_change_scope(root, *, writes, expected, reason):
    from demiflow.forge import audit
    from demiflow.forge.schema import required_text
    reason = required_text(reason, 'Scope change reason', maximum=2048)
    root, value = _audit_workspace(root, expected)
    baseline = _scope_baseline(root, value)
    if baseline is None:
        raise ForgeError('No active change scope')
    value['audit']['change_scope']['writes'] = audit.writes(writes, baseline['watch_roots'])
    return _publish_audit_policy(root, value, expected, reason=reason)


def end_change_scope(root, *, expected, reason):
    from demiflow.forge.schema import required_text
    reason = required_text(reason, 'Scope end reason', maximum=2048)
    root, value = _audit_workspace(root, expected)
    if value['audit']['change_scope'] is None:
        raise ForgeError('No active change scope')
    value['audit']['change_scope'] = None
    return _publish_audit_policy(root, value, expected, reason=reason)


def _check_change_scope(root):
    from demiflow.forge import audit
    root, value, _ = load_workspace(root)
    baseline = _scope_baseline(root, value)
    scope = value.get('audit', {}).get('change_scope')
    if baseline is None:
        return dict(status='unconfigured', complete=False, scope=None)
    current = audit.inventory(root, baseline['watch_roots'], baseline['limits'])
    if read_control(root / 'workspace.json') != value:
        raise ForgeError('Workspace changed during scope check', code='conflict')
    result = audit.compare(baseline, current, scope['writes'], workspace_policy_sha256=audit.workspace_digest(value))
    return dict(result, scope=scope, scope_digest=audit.scope_digest(scope))


@contextmanager
def audit_lease(root, kind):
    if kind not in {'watch', 'check'}:
        raise ForgeError('Unknown audit lease')
    directory = no_symlinks(Path(root).absolute() / '.forge/audit')
    directory.mkdir(exist_ok=True)
    path = no_symlinks(directory / (kind + '.lock'))
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ForgeError('Audit lease must be a regular file')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ForgeError('Workspace audit is busy', code='busy') from exc
        yield
    finally:
        os.close(fd)


def _audit_cache(root, value):
    from demiflow.forge.audit import STATUS_FORMAT
    from demiflow.forge.audit import STATUS_BYTES
    policy = content_digest(value)
    empty = dict(format=STATUS_FORMAT, workspace_id=workspace_id(root, value), policy_digest=policy,
                 revision='missing', observations={}, watcher=None, lightweight_observed_at=None)
    try:
        stored = read_control(root / '.forge/audit/status.json', STATUS_BYTES)
        if (type(stored) is not dict or set(stored) != set(empty) or stored['format'] != STATUS_FORMAT
                or stored['workspace_id'] != empty['workspace_id'] or stored['policy_digest'] != policy
                or type(stored['observations']) is not dict or len(stored['observations']) > 65):
            return empty
        if any(type(row) is not dict or row.get('identity') != key
               for key, row in stored['observations'].items()):
            return empty
        bounded_json(stored, STATUS_BYTES)
        for row in stored['observations'].values():
            result = row.get('result')
            if (type(result) is not dict or type(result.get('complete')) is not bool
                    or type(result.get('status')) is not str
                    or result.get('status') not in {'within_scope', 'violated', 'unconfigured', 'unknown',
                                                   'unverifiable', 'checked', 'stale', 'invalid', 'incomplete'}
                    or type(result.get('changes', [])) is not list):
                return empty
            for change in result.get('changes', []):
                if not (type(change) is str or (type(change) is dict and
                        set(change) == {'path', 'change', 'kind', 'allowed'} and
                        all(type(change[k]) is str for k in ('path', 'change', 'kind')) and
                        type(change['allowed']) is bool)):
                    return empty
        return stored
    except (OSError, ValueError):
        return empty


def _audit_publish(root, expected_policy, update):
    from demiflow.forge.audit import STATUS_BYTES
    with control_lock(root):
        _, value = _audit_workspace(root, expected_policy)
        cache = _audit_cache(root, value)
        update(cache)
        cache['revision'] = uuid.uuid4().hex
        raw = bounded_json(cache, STATUS_BYTES)
        directory = no_symlinks(root / '.forge/audit')
        directory.mkdir(exist_ok=True)
        path = directory / 'status.json'
        # This file is derived, unlike authority controls. A corrupt cache can
        # be replaced under the writer lock without parsing it as authority.
        no_symlinks(path)
        fd, temporary = tempfile.mkstemp(prefix='.status-', dir=directory)
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            Path(temporary).unlink(missing_ok=True)
    return cache


def audit_status(root):
    """Read derived observations; their age never grants execution admission."""
    from demiflow.forge.audit import scope_digest
    import time
    root, value, _ = load_workspace(root)
    cache = _audit_cache(root, value)
    now = time.time()
    policy = value.get('audit', dict(reports=[], change_scope=None))
    rows = []
    for kind, identity in [('scope', 'scope')] + [('report', d) for d in policy['reports']]:
        saved = cache['observations'].get(identity, {})
        if type(saved) is not dict:
            saved = {}
        stamp, due = saved.get('verified_at'), saved.get('due_at')
        valid_time = (type(stamp) in {int, float} and type(due) in {int, float}
                      and 0 < stamp <= now + 2 and stamp <= due <= stamp + 3600)
        valid = (saved.get('kind') == kind and saved.get('identity') == identity
                 and type(saved.get('result')) is dict and valid_time)
        if kind == 'scope' and policy['change_scope'] is None:
            result, fresh = dict(status='unconfigured', complete=False), 'unconfigured'
        elif not valid:
            result, fresh = dict(status='unknown', complete=False), 'unknown'
        else:
            result = saved['result']
            fresh = 'pending' if saved.get('pending') else 'overdue' if now > due else 'current'
        rows.append(dict(kind=kind, identity=identity, result=result, freshness=fresh,
                         verified_at=stamp if valid else None, due_at=due if valid else None))
    watcher = cache.get('watcher')
    if type(watcher) is not dict or set(watcher) != {'session', 'heartbeat'}:
        watcher = None
    heartbeat = watcher.get('heartbeat') if watcher else None
    live = type(heartbeat) in {int, float} and 0 <= now - heartbeat <= 180
    registered = sum(len(paths) for paths in value.get('entries', {}).values())
    return dict(format=cache['format'], workspace_id=cache['workspace_id'], policy_digest=cache['policy_digest'],
                observed_at=now,
                revision=cache['revision'], scope_digest=scope_digest(policy['change_scope']),
                enabled=value['format'] == FORMAT_V3, observations=rows,
                coverage=dict(tracked=len(policy['reports']), registered=registered),
                lightweight_observed_at=cache['lightweight_observed_at'],
                watcher=dict(status='observed' if live else 'unavailable', heartbeat=heartbeat))


def _audit_check(root, *, kind, report_digest=None, expected_policy):
    root, value = _audit_workspace(root, expected_policy)
    if kind == 'scope' and report_digest is None:
        result = _check_change_scope(root)
        result.pop('scope', None)
    elif kind == 'report' and report_digest in value['audit']['reports']:
        checked = _report_status(root, report_digest)
        result = dict(status=checked['status'] if checked['verification']['complete'] else 'unverifiable',
                      complete=checked['verification']['complete'], changes=checked['changes'][:256],
                      entry=checked['report']['binding']['entry'],
                      parameter_path=checked['report']['binding']['parameter_path'],
                      verification=checked['verification'])
    else:
        raise ForgeError('Audit requires scope or one currently tracked report', code='invalid_request')
    if content_digest(read_control(root / 'workspace.json')) != expected_policy:
        raise ForgeError('Audit policy changed during verification', code='conflict')
    return result


def audit_check(root, *, kind, report_digest=None, expected_policy, full_interval=300):
    """Verify one object, then publish only a bounded derived observation."""
    from demiflow.workbench.worker import operation
    import time
    root, value = _audit_workspace(root, expected_policy)
    if (type(kind) is not str or kind not in {'scope', 'report'} or (kind == 'scope' and report_digest is not None)
            or (kind == 'report' and report_digest not in value['audit']['reports'])
            or type(full_interval) is not int or not 1 <= full_interval <= 3600):
        raise ForgeError('Invalid audit target or interval', code='invalid_request')
    with audit_lease(root, 'check'):
        try:
            result = operation('audit_check', root, kind=kind, report_digest=report_digest, expected_policy=expected_policy)
        except ForgeError as exc:
            result = dict(status='unverifiable', complete=False, error=dict(code=exc.code, message=str(exc)[:2048]))
        stamp = time.time()
        identity = 'scope' if kind == 'scope' else report_digest
        observation = dict(kind=kind, identity=identity, result=result, verified_at=stamp,
                           due_at=stamp + full_interval, pending=False)
        _audit_publish(root, expected_policy, lambda cache: cache['observations'].__setitem__(identity, observation))
    return observation


def _audit_hint(root, *, expected_policy):
    from demiflow.forge import audit
    root, value = _audit_workspace(root, expected_policy)
    baseline = _scope_baseline(root, value)
    names = baseline['watch_roots'] if baseline else author_roots(root, value)
    limits = baseline['limits'] if baseline else None
    source = audit.metadata_hint(root, names, limits) if names else None
    stamps = []
    paths = table_files(root) + [root / '.forge/inspections' / (d + '.json') for d in value['audit']['reports']]
    for path in paths:
        no_symlinks(path)
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode):
                raise ForgeError('Audit references must be regular files')
            state = [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]
        except FileNotFoundError:
            state = None
        stamps.append([str(path.relative_to(root)), state])
    if content_digest(read_control(root / 'workspace.json')) != expected_policy:
        raise ForgeError('Audit policy changed during observation', code='conflict')
    return content_digest(dict(sources=source, controls=stamps, policy=expected_policy))


def audit_observe(root, *, expected_policy, session, previous_hint=None):
    """A cheap trigger cannot advance any completed-content timestamp."""
    from demiflow.workbench.worker import operation
    import time
    root, _ = _audit_workspace(root, expected_policy)
    if type(session) is not str or re.fullmatch('[0-9a-f]{32}', session) is None:
        raise ForgeError('Invalid watcher session', code='invalid_request')
    with audit_lease(root, 'check'):
        try:
            hint = operation('audit_hint', root, expected_policy=expected_policy)
        except ForgeError:
            def invalidate(cache):
                for row in cache['observations'].values():
                    row['pending'] = True
            _audit_publish(root, expected_policy, invalidate)
            raise
        changed = hint != previous_hint
        def update(cache):
            cache['lightweight_observed_at'] = time.time()
            cache['watcher'] = dict(session=session, heartbeat=time.time())
            if changed:
                for row in cache['observations'].values():
                    row['pending'] = True
        _audit_publish(root, expected_policy, update)
    return dict(hint=hint, changed=changed)


def set_budgets(root, limits, *, expected):
    from demiflow.workbench.runs.limits import limits_with
    root, value, _ = load_workspace(root)
    if value['format'] not in MODERN_FORMATS or content_digest(value) != expected:
        raise ForgeError('Budget policy requires the current workspace-v2 binding')
    effective = limits_with(policy=limits)
    value['budgets'] = dict(limits)
    with control_lock(root):
        write_control(root / 'workspace.json', value, expected=expected)
    return {'status': 'updated', 'workspace_digest': content_digest(value), 'limits': effective}


def validate_entry(root, entry):
    if not isinstance(entry, str) or entry.count(':') != 1:
        raise ForgeError('Entry requires project-relative file.py:function')
    path, symbol = entry.split(':')
    target = relative_path(root, path)
    if (target.suffix != '.py' or not symbol.isidentifier() or symbol.startswith('_')
            or any(not part.isidentifier() for part in path[:-3].split('/'))):
        raise ForgeError('Entry must name a local Python module and public function')
    return target, symbol


def validate_parameter_path(root, params):
    target = relative_path(root, params)
    if target.suffix != '.json' or any(part.startswith('.') for part in Path(params).parts):
        raise ForgeError('Parameters require a project-relative author JSON file', code='invalid_request')
    return target


def read_parameters(root, path):
    root, _, _ = load_workspace(root)
    return read_control(validate_parameter_path(root, path), 65536)


def source_text(root, path, *, expected_sha256=None):
    root, _, _ = load_workspace(root)
    target = relative_path(root, path)
    if target.suffix != '.py' or any(part.startswith('.') for part in Path(path).parts):
        raise ForgeError('Source view requires a project-relative author Python file', code='invalid_request')
    if expected_sha256 is not None and (type(expected_sha256) is not str
            or not re.fullmatch(r'[0-9a-f]{64}', expected_sha256)):
        raise ForgeError('Invalid source digest', code='invalid_request')
    raw = read_bounded(target, 1024**2)
    actual = hashlib.sha256(raw).hexdigest()
    if expected_sha256 is not None and actual != expected_sha256:
        raise ForgeError('Source changed since inspection; inspect the current working tree', code='conflict')
    return {'path': path, 'text': raw.decode('utf-8'), 'sha256': actual}


def _register_flow_value(root, value, entry, params, *, new_only=False):
    import ast
    if value['format'] not in MODERN_FORMATS:
        raise ForgeError('Explicit flow registration requires workspace v2')
    target, symbol = validate_entry(root, entry)
    tree = ast.parse(read_bounded(target, 1024**2))
    if sum(isinstance(n, ast.FunctionDef) and n.name == symbol for n in tree.body) != 1:
        raise ForgeError('Registered entry function is missing or ambiguous')
    read_control(validate_parameter_path(root, params), 65536)
    if new_only and entry in value['entries']:
        raise ForgeError('Managed registration can only add a new entry', code='conflict')
    paths = value['entries'].setdefault(entry, [])
    if params in paths:
        return dict(status='unchanged', entry=entry, params=params)
    if len(value['entries']) > 256 or len(paths) >= 64:
        raise ForgeError('Entry registration budget exceeded')
    paths.append(params)
    return dict(status='registered', entry=entry, params=params)


def plan_flow_registration(root, flows):
    """Validate an additive batch without publishing either project control."""
    root, value, _ = load_workspace(root)
    before = read_bounded(root / 'workspace.json', 1024**2).decode('utf-8')
    if content_digest(json.loads(before)) != content_digest(value):
        raise ForgeError('Workspace changed during registration planning', code='conflict')
    if not isinstance(flows, list) or not 1 <= len(flows) <= 8:
        raise ForgeError('Managed registration requires 1..8 new flows')
    for flow in flows:
        _register_flow_value(root, value, flow['path'] + ':' + flow['symbol'], flow['params'], new_only=True)
    after = bounded_json(value, 1024**2).decode('utf-8')
    return dict(before_raw=before, before_sha256=hashlib.sha256(before.encode()).hexdigest(),
                after_raw=after, after_sha256=hashlib.sha256(after.encode()).hexdigest())


def register_flow(root, entry, *, params):
    root, value, _ = load_workspace(root)
    expected = content_digest(value)
    result = _register_flow_value(root, value, entry, params)
    if result['status'] == 'unchanged': return result
    with control_lock(root):
        write_control(root / 'workspace.json', value, expected=expected)
    return result


def unregister_flow(root, entry, *, expected):
    root, value, _ = load_workspace(root)
    if value['format'] not in MODERN_FORMATS or entry not in value['entries']:
        raise ForgeError('Unknown registered entry')
    if content_digest(value) != expected:
        raise ForgeError('Workspace changed; refresh before removing entry')
    del value['entries'][entry]
    with control_lock(root):
        write_control(root / 'workspace.json', value, expected=expected)
    return dict(status='unregistered', entry=entry)


def location_root(root, location):
    path = Path(location["root"])
    result = path if path.is_absolute() else relative_path(root, location["root"])
    no_symlinks(result)
    if not result.is_dir():
        raise ForgeError("Data location must be an existing directory")
    return result.absolute()


def add_location(root, name, path, *, writable=False, write_modes=None):
    root, value, _ = load_workspace(root)
    if type(writable) is not bool or writable and value['format'] not in MODERN_FORMATS:
        raise ForgeError('Explicit writable locations require workspace v2')
    identifier(name)
    if name in value["locations"] or len(value["locations"]) >= 16:
        raise ForgeError("Location exists or budget exhausted")
    path = Path(path).absolute()
    no_symlinks(path)
    if not path.is_dir():
        raise ForgeError("Read-only location must exist")
    expected = content_digest(value)
    value["locations"][name] = {"root": str(path), "writable": writable}
    if write_modes is not None:
        if (not writable or type(write_modes) not in (list, tuple) or not write_modes
                or any(type(mode) is not str for mode in write_modes)
                or set(write_modes) - {'create', 'overwrite', 'checkpoint'} or len(set(write_modes)) != len(write_modes)):
            raise ForgeError('Write modes require an explicit writable location and create/overwrite/checkpoint permissions')
        value['locations'][name]['write_modes'] = sorted(write_modes)
    with control_lock(root):
        write_control(root / "workspace.json", value, expected=expected)
    return {"status": "created", "location": name, "root": str(path), "writable": writable}


def bind_platform(root, *, expected, extras=None):
    """Explicitly adopt current platform facts; existing reports stay historical."""
    root, value, previous = load_workspace(root)
    if value["platform"]["digest"] != expected:
        raise ForgeError("Platform binding changed; refresh before replacing")
    digest = content_digest(value)
    snapshot = build_snapshot(
        previous["scope"]["modules"], extras=previous["scope"]["extras"] if extras is None else extras
    )
    path = ".forge/platform/" + snapshot["digests"]["snapshot"] + ".json"
    with control_lock(root):
        if not (root / path).exists():
            write_snapshot(root / path, snapshot)
        value["platform"] = {"path": path, "digest": snapshot["digests"]["snapshot"]}
        write_control(root / "workspace.json", value, expected=digest)
    return {
        "status": "bound",
        "platform": value["platform"],
        "reports_revalidated": False,
    }


def table_files(root):
    directory = Path(root) / "tables"
    if not read_control(Path(root) / 'workspace.json').get('locations'):
        return []
    no_symlinks(directory)
    if not directory.exists():
        return []
    paths = []
    with os.scandir(directory) as entries:
        for entry in entries:
            if len(paths) >= 1024:
                raise ForgeError("Table reference budget exceeded")
            if (
                entry.is_symlink()
                or not entry.is_file(follow_symlinks=False)
                or not entry.name.endswith(".json")
            ):
                raise ForgeError("Unexpected table control entry")
            paths.append(Path(entry.path))
    return sorted(paths)


def table_binding(root, value, alias):
    from demiflow.lance.refs import DatasetRef

    if isinstance(alias, str) and alias.startswith("run:"):
        from demiflow.workbench.runs.manager import result_table
        parts = alias.split(":")
        if len(parts) != 3:
            raise ForgeError("Invalid run output reference")
        return result_table(root, parts[1], parts[2])
    identifier(alias)
    control = read_control(root / "tables" / f"{alias}.json", 16384)
    modern = control.get('format') == 'demiflow_forge_table_v2'
    if (
        set(control) != {"format", "alias", "location_id", "ref"} | ({'display', 'provenance'} if modern else set())
        or control["format"] not in {"demiflow_forge_table_v1", 'demiflow_forge_table_v2'}
        or control["alias"] != alias
    ):
        raise ForgeError("Invalid table reference")
    if control["location_id"] not in value["locations"]:
        raise ForgeError("Unknown table location")
    if modern:
        from demiflow.workbench.catalog import validate
        validate(control['display'], control['provenance'], value['locations'])
    ref = DatasetRef.from_dict(control["ref"])
    base = location_root(root, value["locations"][control["location_id"]])
    uri = relative_path(base, ref.relative_uri)
    if ref.store_id != control["location_id"]:
        raise ForgeError("Reference store and location differ")
    return control, ref, uri


def table_identity(control):
    """Presentation edits do not change a fixed Dataset binding or old v1 IDs."""
    return {key: 'demiflow_forge_table_v1' if key == 'format' else control[key]
            for key in ('format', 'alias', 'location_id', 'ref')}


def preview_binding(root, control, uri):
    """Identity of one resolved preview, distinct from the alias edit digest."""
    value = {"workspace": str(Path(root).absolute()), "uri": str(uri), "reference": table_identity(control)}
    bounded_json(value, 32 * 1024)
    return {**value, "alias": control["alias"], "digest": content_digest(value)}


def describe_table(root, alias, *, expected_binding=None, version=None, version_binding=None):
    from demiflow.workbench.worker import operation

    arguments = dict(alias=alias, expected_binding=expected_binding)
    if version is not None or version_binding is not None:
        arguments.update(version=version, version_binding=version_binding)
    return operation("table", root, **arguments)


def table_versions(root, alias, *, expected_binding=None, version=None):
    from demiflow.workbench.worker import operation

    return operation("table_versions", root, alias=alias, expected_binding=expected_binding, version=version)


def attach_table(
    root,
    alias,
    *,
    relative_uri,
    version,
    location_id="local",
    dataset_id=None,
    schema_name=None,
    schema_version=None,
    expected=None,
    expected_ref=None,
):
    from demiflow.workbench.worker import operation
    from demiflow.lance.refs import DatasetRef

    root, value, _ = load_workspace(root)
    identifier(alias)
    if location_id not in value["locations"]:
        raise ForgeError("Unknown location")
    base = location_root(root, value["locations"][location_id])
    uri = relative_path(base, relative_uri)
    description = operation("describe", uri, version=version)
    existing = operation(
        "catalog",
        base,
        relative_uri=relative_uri,
        version=description["version"],
        dataset_id=dataset_id,
    )
    if existing:
        ref = DatasetRef.from_dict(existing)
        if ref.store_id != location_id:
            raise ForgeError("Catalog store_id does not match selected location")
    else:
        if not all((dataset_id, schema_name, schema_version)):
            raise ForgeError(
                "Unregistered tables require dataset_id, schema_name and schema_version"
            )
        ref = DatasetRef(
            dataset_id,
            relative_uri,
            description["version"],
            schema_name,
            schema_version,
            description["schema_hash"],
            description["row_count"],
            store_id=location_id,
        )
    if (
        ref.schema_hash != description["schema_hash"]
        or ref.row_count != description["row_count"]
    ):
        raise ForgeError("Catalog reference differs from actual table")
    if expected_ref is not None and ref.to_dict() != expected_ref.to_dict():
        raise ForgeError('Resolved table differs from the expected fixed reference', code='conflict')
    control = {
        "format": "demiflow_forge_table_v1",
        "alias": alias,
        "location_id": location_id,
        "ref": ref.to_dict(),
    }
    with control_lock(root):
        if (
            len(table_files(root)) >= 1024
            and not (root / "tables" / f"{alias}.json").exists()
        ):
            raise ForgeError("Table reference budget exceeded")
        write_control(
            root / "tables" / f"{alias}.json",
            control,
            expected=expected,
            create=expected is None,
        )
    return {"status": "attached", "table": control, "digest": content_digest(control)}


def detach_table(root, alias, *, expected):
    root, value, _ = load_workspace(root)
    control, _, _ = table_binding(root, value, alias)
    with control_lock(root):
        path = root / "tables" / f"{alias}.json"
        if content_digest(read_control(path)) != expected:
            raise ForgeError("Reference changed before detach", code='conflict')
        path.unlink()
    return {"status": "detached", "alias": alias, "data_deleted": False}


def set_table_view(root, alias, *, display, provenance, expected):
    """CAS edit of presentation only; never writes the table or invents a run."""
    from demiflow.workbench.catalog import validate
    from demiflow.workbench.catalog import validate_fields
    from demiflow.workbench.worker import operation
    root, value, _ = load_workspace(root)
    identifier(alias)
    control, ref, uri = table_binding(root, value, alias)
    validate(display, provenance, value['locations'])
    validate_fields(display, operation('describe', uri, version=ref.lance_version)['fields'])
    updated = {**control, 'format': 'demiflow_forge_table_v2', 'display': display, 'provenance': provenance}
    with control_lock(root):
        write_control(root / 'tables' / (alias + '.json'), updated, expected=expected)
    return {'status': 'updated', 'table': updated, 'digest': content_digest(updated)}


def resolve_arguments(root, value, parameters):
    from demiflow.lance.preview import describe_table

    if (
        not isinstance(parameters, dict)
        or parameters.get("format") not in {"demiflow_forge_flow_parameters_v1", "demiflow_forge_flow_parameters_v2"}
        or set(parameters) != {"format", "arguments"}
        or not isinstance(parameters["arguments"], dict)
    ):
        raise ForgeError("Invalid flow parameter format")
    tables = {}
    targets = {}
    controls = {}

    def visit(item, depth=0):
        if depth > 16:
            raise ForgeError("Parameter nesting exceeds budget")
        if isinstance(item, dict):
            if item.get('kind') == 'location':
                if parameters['format'] != 'demiflow_forge_flow_parameters_v2' or set(item) != {'kind', 'location_id'}:
                    raise ForgeError('Location arguments require v2 and an explicit location_id')
                location = value['locations'].get(item['location_id'])
                if location is None:
                    raise ForgeError('Unknown declared location')
                return str(location_root(root, location))
            if item.get("kind") == "table":
                if set(item) != {"kind", "alias"}:
                    raise ForgeError("Invalid table argument")
                alias = item["alias"]
                control, ref, uri = table_binding(root, value, alias)
                description = describe_table(uri, ref.lance_version)
                if (
                    description["schema_hash"] != ref.schema_hash
                    or description["row_count"] != ref.row_count
                ):
                    raise ForgeError("Pinned table metadata changed")
                tables[alias] = {**description, "uri": str(uri), "reference": table_identity(control)}
                if not alias.startswith("run:"):
                    controls["tables/" + alias + ".json"] = content_digest(table_identity(control))
                return {"uri": str(uri), "version": ref.lance_version}
            if item.get("kind") == "output":
                modern = parameters['format'] == 'demiflow_forge_flow_parameters_v2'
                required = {'kind', 'location_id', 'relative_uri'} | ({'mode', 'expected_version'} if modern else set())
                if set(item) != required:
                    raise ForgeError("Invalid output argument")
                location = value["locations"].get(item["location_id"])
                if not location or not location["writable"]:
                    raise ForgeError("Output location must be workspace-owned")
                mode, expected = item.get('mode', 'create'), item.get('expected_version')
                if (mode not in location.get('write_modes', ['create']) or mode in {'create', 'checkpoint'} and expected is not None
                        or mode == 'overwrite' and (type(expected) is not int or expected < 1)):
                    raise ForgeError('Output mode requires location permission and a valid fixed CAS baseline')
                path = relative_path(
                    location_root(root, location), item["relative_uri"]
                )
                if path.suffix != ".lance":
                    raise ForgeError("Output must name a .lance table")
                if str(path) in targets:
                    raise ForgeError('Each output target must have one declaration')
                targets[str(path)] = {'mode': mode, 'expected_version': expected}
                return {'uri': str(path), **({'mode': mode, 'expected_version': expected} if modern else {})}
            return {k: visit(v, depth + 1) for k, v in item.items()}
        if isinstance(item, list):
            if len(item) > 1024:
                raise ForgeError("Parameter sequence exceeds budget")
            return [visit(v, depth + 1) for v in item]
        if item is None or type(item) in (str, int, float, bool):
            return item
        raise ForgeError("Only finite JSON parameters are accepted")

    args = visit(parameters["arguments"])
    bounded_json(args, 1024**2)
    return args, tables, targets, controls


def inspect_flow(root, entry, *, params, save=True, limits=None):
    from demiflow.workbench.worker import operation

    return operation(
        "inspect", root, entry=entry, params=params, save=save, limits=limits
    )


def check_project(root):
    from demiflow.workbench.worker import operation
    return operation('project_check', root)


def project_check_status(root):
    from demiflow.workbench.worker import operation
    return operation('project_status', root)


def sample_table(
    root, alias, *, columns=None, offset=0, limit=20, row_id=None, match=None,
    expected_binding=None, version=None, version_binding=None,
):
    from demiflow.workbench.worker import operation

    arguments = dict(
        alias=alias,
        columns=columns,
        offset=offset,
        limit=limit,
        row_id=row_id,
        match=match,
        expected_binding=expected_binding,
    )
    if version is not None or version_binding is not None:
        arguments.update(version=version, version_binding=version_binding)
    return operation("sample", root, **arguments)


def profile_table(root,alias,*,expected_binding,columns,version=None,version_binding=None):
    from demiflow.workbench.worker import operation
    arguments=dict(kind='workspace_table',alias=alias,expected_binding=expected_binding,columns=columns)
    if version is not None or version_binding is not None:
        arguments.update(version=version,version_binding=version_binding)
    return operation('table_profile',root,**arguments)


def project_profile(root,identity,*,expected,version,binding,columns):
    from demiflow.workbench.worker import operation
    return operation('table_profile',root,kind='project_table',identity=identity,expected=expected,version=version,binding=binding,columns=columns)


def table_media(root, alias, *, row_id, field=None, field_path=None, expected_binding=None, version=None, version_binding=None):
    from demiflow.workbench.worker import operation

    if (field is None) == (field_path is None):
        raise ForgeError('Media requires exactly one field or field_path', code='invalid_request')
    arguments = dict(alias=alias, row_id=row_id,
                     **({'field': field} if field is not None else {'field_path': field_path}),
                     expected_binding=expected_binding)
    if version is not None or version_binding is not None:
        arguments.update(version=version, version_binding=version_binding)
    return operation('media', root, **arguments)


def table_evidence(root, alias, *, row_id, field_path, expected_binding=None, version=None, version_binding=None):
    from demiflow.workbench.worker import operation

    arguments = dict(alias=alias, row_id=row_id, field_path=field_path, expected_binding=expected_binding)
    if version is not None or version_binding is not None:
        arguments.update(version=version, version_binding=version_binding)
    return operation('table_evidence', root, **arguments)


def _inspect_flow(root, entry, *, params, save=True, limits=None):
    from demiflow.forge.flow_check import FlowChecker

    root, value, snapshot = load_workspace(root)
    comparison = check_snapshot(root / value["platform"]["path"])
    if comparison["status"] != "current":
        return {
            "status": comparison["status"],
            "diagnostics": comparison["diagnostics"],
            "changes": comparison["changes"],
        }
    path = validate_parameter_path(root, params) if isinstance(params, str) else None
    parameters_value = read_control(path) if path else params
    args, tables, targets, controls = resolve_arguments(root, value, parameters_value)
    report = FlowChecker(
        root, snapshot["catalog"], tables, targets, limits=limits,
        resource_resolver=lambda tree, references, bindings: resolve_flow_resources(root, tree, references, bindings),
    ).inspect(entry, args)
    report["binding"].update(
        {
            "root": str(root),
            "platform": value["platform"]["digest"],
            "controls": controls,
            "parameters": parameters_value,
            "parameter_path": str(path.relative_to(root)) if path else None,
        }
    )
    report['binding']['workspace_policy'] = workspace_policy(value, report['binding'])
    report['binding']['workspace'] = content_digest(report['binding']['workspace_policy'])
    if (
        read_control(root / "workspace.json") != value
        or any(
            content_digest(read_control(root / p)) != digest
            for p, digest in controls.items()
        )
        or path
        and read_control(path) != parameters_value
    ):
        raise ForgeError("Workspace inputs changed during inspection")
    # Native implementation and environment identity are rechecked after the
    # candidate closure; no successful report spans a concurrent platform edit.
    final = check_snapshot(root / value["platform"]["path"])
    if final["status"] != "current":
        return {
            "status": "stale",
            "diagnostics": [
                {"rule": "FL-10", "detail": "Platform changed during flow inspection"}
            ],
        }
    if (
        read_control(root / "workspace.json") != value
        or any(
            content_digest(read_control(root / p)) != digest
            for p, digest in controls.items()
        )
        or path
        and read_control(path) != parameters_value
    ):
        raise ForgeError("Workspace inputs changed before report publication")
    for name, sha in report["binding"]["sources"].items():
        if (
            hashlib.sha256(read_bounded(relative_path(root, name), 1024**2)).hexdigest()
            != sha
        ):
            raise ForgeError("Candidate source changed before report publication")
    for binding in report["binding"].get("resources", {}).values():
        if binding["kind"] in {"config", "file"} and hashlib.sha256(read_bounded(relative_path(root, binding["path"]), binding.get('max_bytes', 1024**2))).hexdigest() != binding["sha256"]:
            raise ForgeError("Declared resource changed before report publication")
    report.pop("digest", None)
    report["digest"] = content_digest(report)
    if save:
        from demiflow.workbench.projects import index
        import shutil
        with control_lock(root):
            directory = root / ".forge/inspections"
            no_symlinks(directory)
            target = directory / (report["digest"] + ".json")
            marker = index.begin(root, 'report', report['digest'])
            if not target.exists():
                raw = bounded_json(report, 8 * 1024**2)
                if shutil.disk_usage(root).free < 2 * len(raw) + 8 * 1024**2:
                    raise ForgeError('Insufficient disk headroom for the report')
                write_control(target, report, create=True)
            index.publish(root, marker)
    return report


def resolve_flow_resources(root, tree, references, bindings):
    """Read declared files under the workspace's existing source budget.

    Analyzers receive configuration text or immutable file metadata, never a
    workspace, SourceTree, connection or unrestricted reader.
    """
    if type(references) is not list or len(references) > 256:
        raise ForgeError("Declared resource count exceeds budget")
    result = {}
    for reference in references:
        if type(reference) is not dict or type(reference.get('parameter')) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,63}', reference['parameter']) or reference['parameter'] in result:
            raise ForgeError('Invalid or duplicate resource parameter')
        if reference.get('kind') == 'objects':
            if set(reference)-{'access'} != {'kind', 'parameter', 'root', 'relocations', 'layout'}:
                raise ForgeError('Invalid object location reference')
            reference=dict(reference)
            from demiflow.objects import _local_path
            value=str(reference['root'])
            logical=_local_path(value) if value.startswith('file:') else Path(value)
            reference['root']=str(logical if logical.is_absolute() else Path(root)/logical)
            _, workspace, _ = load_workspace(root)
            matches = [key for key, location in workspace['locations'].items()
                       if str(location_root(root, location)) == reference['root']]
            if len(matches) != 1:
                raise ForgeError('Object root must match one registered workspace location')
            if reference.get('access') not in {None,'write'}:
                raise ForgeError('Unknown object resource access')
            if reference.get('access')=='write' and not workspace['locations'][matches[0]].get('writable',False):
                raise ForgeError('Image object destination is not writable')
            binding = {k: reference[k] for k in ('kind', 'root', 'relocations', 'layout')}
            binding['location_id'] = matches[0]
            if 'access' in reference:binding['access']=reference['access']
            key = 'objects:' + content_digest(binding)
            if key not in bindings and len(bindings) >= 256:
                raise ForgeError('Declared resource closure exceeds count budget')
            bindings[key] = binding
            result[reference['parameter']] = binding
            continue
        if type(reference) is dict and reference.get('kind') == 'ledger':
            from demiflow.operator_llm.sqlite_journal import MAX_DISK_BYTES
            if set(reference) != {'kind', 'parameter', 'path', 'format', 'max_bytes'}:
                raise ForgeError('Invalid native ledger reference')
            path = reference['path']
            if type(path) is not str or not re.fullmatch(r'evidence/[A-Za-z][A-Za-z0-9_-]{0,63}\.sqlite', path):
                raise ForgeError('Run ledger must use evidence/<name>.sqlite')
            if type(reference['max_bytes']) is not int or not 4 * 1024**2 <= reference['max_bytes'] <= MAX_DISK_BYTES:
                raise ForgeError('Run ledger requires a 4 MiB..32 GiB disk ceiling')
            if type(reference['format']) is not str or not re.fullmatch(r'[a-z][a-z0-9_]{0,95}', reference['format']):
                raise ForgeError('Invalid native ledger format')
            binding = {k: reference[k] for k in ('kind', 'path', 'format', 'max_bytes')}
            key = 'ledger:' + path
            if key in bindings and bindings[key] != binding:
                raise ForgeError('Shared ledger declarations must have identical limits and format')
            if key not in bindings and len(bindings) >= 256:
                raise ForgeError('Declared resource closure exceeds count budget')
            bindings[key] = binding
            result[reference['parameter']] = binding
            continue
        kind = reference.get('kind')
        required = {"kind", "parameter", "path"}
        if kind == 'file':
            required |= {'format', 'sha256', 'max_bytes'}
        if set(reference) != required:
            raise ForgeError("Invalid declared resource reference")
        if kind not in {'config', 'file'}:
            raise ForgeError("Declared resource kind has no workspace binding contract")
        maximum = 1024**2
        if kind == 'file':
            maximum = reference['max_bytes']
            if type(maximum) is not int or not 1 <= maximum <= 16 * 1024**2:
                raise ForgeError('Sealed file requires a byte ceiling <= 16 MiB')
            if type(reference['format']) is not str or not re.fullmatch(r'[a-z][a-z0-9_]{0,95}', reference['format']):
                raise ForgeError('Invalid sealed file format')
            if type(reference['sha256']) is not str or not re.fullmatch('[0-9a-f]{64}', reference['sha256']):
                raise ForgeError('Sealed file requires an exact SHA256')
        name, path = reference["parameter"], reference["path"]
        physical = relative_path(root, path)
        if any(part.startswith('.') for part in Path(path).parts):
            raise ForgeError("Declared resource must be an author file within the project")
        allowance = maximum if path in bindings else min(maximum, tree.limits["total_bytes"] - tree.bytes)
        raw = read_bounded(physical, allowance)
        digest = hashlib.sha256(raw).hexdigest()
        binding = {"kind": kind, "path": path, "size": len(raw), "sha256": digest}
        if kind == 'file':
            if digest != reference['sha256']:
                raise ForgeError('Declared file SHA256 mismatch')
            binding.update(format=reference['format'], max_bytes=maximum)
        if path in bindings and bindings[path] != binding:
            raise ForgeError("Declared resource changed during analysis")
        if path not in bindings:
            if len(bindings) >= 256:
                raise ForgeError("Declared resource closure exceeds count budget")
            tree.bytes += len(raw)
            bindings[path] = binding
        result[name] = {**binding, "text": raw.decode("utf-8")} if kind == 'config' else dict(binding)
    return result


def load_report(root, digest):
    if not isinstance(digest, str) or not re.fullmatch("[0-9a-f]{64}", digest):
        raise ForgeError("Invalid report identity")
    report = read_control(
        Path(root) / ".forge/inspections" / (digest + ".json"), 8 * 1024**2
    )
    if (
        report.get("format") not in {"demiflow_forge_flow_inspection_v1", "demiflow_forge_flow_inspection_v2", "demiflow_forge_flow_inspection_v3"}
        or report.get("digest") != digest
        or content_digest({k: v for k, v in report.items() if k != "digest"}) != digest
    ):
        raise ForgeError("Invalid or modified inspection report")
    return report


def workspace_policy(value, binding):
    """Bind only this Flow's registration and declared data access locations.

    Names, unrelated entries/configurations and unused locations are presentation
    or separate work. Legacy reports continue to bind the entire old control.
    """
    used = set()

    def visit(item):
        if type(item) is dict:
            if item.get('kind') in {'output', 'location'}:
                used.add(item['location_id'])
            for child in item.values():
                visit(child)
        elif type(item) is list:
            for child in item:
                visit(child)

    visit(binding['parameters'])
    for table in binding['tables'].values():
        used.add(table['reference']['location_id'])
    for resource in binding.get('resources', {}).values():
        if 'location_id' in resource:
            used.add(resource['location_id'])
    entry, params = binding['entry'], binding['parameter_path']
    return {'format': value['format'], 'id': value.get('id'), 'platform': value['platform'],
            **({'budgets': value['budgets']} if 'budgets' in value else {}),
            'entry_registered': entry in value.get('entries', {}),
            'parameters_registered': params in value.get('entries', {}).get(entry, []) if params else None,
            'locations': {key: value['locations'].get(key) for key in sorted(used)}}


def report_status(root, digest, *, platform_check=True):
    from demiflow.workbench.worker import operation

    return operation("status", root, digest=digest, platform_check=platform_check)


def _report_status(root, digest, *, platform_check=True):
    from demiflow.errors import DemiflowError
    from demiflow.lance.preview import describe_table

    root, value, _ = load_workspace(root)
    report = load_report(root, digest)
    binding = report["binding"]
    reasons, checks = [], []
    complete = bool(platform_check)
    total = 0

    def check(kind, subject, read, expected, reason, unavailable_reason=None):
        nonlocal complete, total
        total += 1
        try:
            actual = read()
            outcome = 'match' if actual == expected else 'mismatch'
            code = 'matches' if outcome == 'match' else 'content_differs'
        except (OSError, ValueError, RuntimeError, DemiflowError):
            outcome, code, complete = 'unavailable', 'cannot_verify', False
        if outcome != 'match':
            reasons.append(unavailable_reason if outcome == 'unavailable' and unavailable_reason else reason)
        if len(checks) < 256:
            checks.append(dict(kind=kind, subject=subject, outcome=outcome, reason_code=code))

    if report["format"] != "demiflow_forge_flow_inspection_v3":
        reasons.append("historical_contract_read_only")
        complete = False
    for resource in binding.get("resources", {}).values():
        if resource['kind'] in {'config', 'file'}:
            check('resource', resource['path'],
                  lambda r=resource: hashlib.sha256(read_bounded(relative_path(root, r['path']), r.get('max_bytes', 1024**2))).hexdigest(),
                  resource['sha256'], 'resource_changed:' + resource['path'])
    check('workspace', 'root', lambda: str(root), binding.get('root'), 'workspace_root_changed')
    current_policy = workspace_policy(value, binding) if report['format'] == 'demiflow_forge_flow_inspection_v3' else value
    check('workspace', 'policy', lambda: content_digest(current_policy), binding['workspace'], 'workspace_changed')
    for path, sha in binding['sources'].items():
        check('source', path, lambda p=path: hashlib.sha256(read_bounded(relative_path(root, p), 1024**2)).hexdigest(),
              sha, 'source_changed:' + path)
    for path, sha in binding['controls'].items():
        def read_binding(p=path):
            control = read_control(relative_path(root, p))
            return content_digest(table_identity(control) if p.startswith('tables/') else control)
        check('table_binding', path, read_binding, sha, 'table_binding_changed:' + path)
    for alias, table in binding['tables'].items():
        def read_table(a=alias):
            _, ref, uri = table_binding(root, value, a)
            metadata = describe_table(uri, ref.lance_version)
            return [str(uri), metadata['schema_hash'], metadata['version']]
        check('table', alias, read_table, [table['uri'], table['schema_hash'], table['version']],
              'table_identity_changed:' + alias, 'table_unavailable:' + alias)
    path = binding['parameter_path']
    if path:
        check('parameters', path, lambda: read_control(relative_path(root, path)), binding['parameters'], 'parameters_changed')
    if platform_check:
        check('platform', value['platform']['digest'], lambda: check_snapshot(root / value['platform']['path'])['status'],
              'current', 'platform_changed')
    return dict(status='stale' if reasons else report['status'], report=report, changes=reasons,
                platform_checked=platform_check,
                verification=dict(complete=complete, checks=checks, check_count=total, truncated=total > len(checks)))


def diff_reports(root, left, right):
    a, b = load_report(root, left), load_report(root, right)
    return {
        "status": "compared",
        "left": left,
        "right": right,
        "arguments": {
            "left": a["binding"]["arguments"],
            "right": b["binding"]["arguments"],
        },
        "nodes": {"left": a["nodes"], "right": b["nodes"]},
        "same_graph": a["nodes"] == b["nodes"] and a["edges"] == b["edges"],
    }


def author_roots(root, value):
    """Bounded author roots; no business directory names have special roles.

    Declared data locations and platform state are outside author inventories.
    Explicit task scopes remain the authority for complete change monitoring.
    """
    locations = [location_root(root, item) for item in value['locations'].values()]
    entries = value.get('entries', {})
    if entries:
        # Read saved dependency declarations, not author-file contents, for the
        # cheap observation hint. Fresh execution still resolves/checks source.
        paths = {e.split(':')[0] for e in entries} | {p for ps in entries.values() for p in ps}
        from demiflow.workbench.projects import index
        info = index.status(root)
        if info.get('reason') == 'index_not_initialized':
            recent = report_files(root)
        else:
            # This is a refresh hint, not a complete source closure or a gate.
            # Bound its cost independently of accumulated inspection history.
            recent = [relative_path(root, '.forge/inspections/' + item['identity'] + '.json')
                      for item in index.page(root, 'report', limit=32)['entries']]
        used = 0
        for report_path in recent:
            raw = read_bounded(report_path, min(8*1024**2, 16*1024**2 - used))
            used += len(raw)
            report = json.loads(raw)
            binding = report.get('binding', {})
            if binding.get('entry') in entries:
                paths.update(binding.get('sources', {}))
                paths.update(r['path'] for r in binding.get('resources', {}).values()
                             if r.get('kind') in {'config', 'file'})
        for path in paths:
            relative_path(root, path)
        names = sorted({p.split('/')[0] for p in paths})
        if len(names) > 64:
            raise ForgeError('Project author roots exceed budget', code='resource_limit')
        return names
    names = []
    with os.scandir(root) as children:
        for count, entry in enumerate(children):
            if count >= 4096:
                raise ForgeError('Project root inventory exceeds budget', code='resource_limit')
            path = Path(entry.path)
            if entry.name.startswith('.') or entry.name == '__pycache__' or entry.name == 'workspace.json':
                continue
            if any(path.is_relative_to(location) for location in locations):
                continue
            if entry.is_dir(follow_symlinks=False) or path.suffix in {'.py', '.json', '.yaml', '.yml', '.toml', '.md', '.ipynb'}:
                no_symlinks(path)
                names.append(entry.name)
    if len(names) > 64:
        raise ForgeError('Project author roots exceed budget', code='resource_limit')
    return sorted(names)


def source_files(root, name):
    """Bounded inventory shared by discovery and change hints; never import code."""
    if not (Path(root) / name).exists():
        return []
    if (Path(root) / name).is_file():
        no_symlinks(Path(root) / name)
        return [Path(root) / name]
    pending, paths, count = [Path(root) / name], [], 0
    while pending:
        directory = pending.pop()
        no_symlinks(directory)
        if len(directory.relative_to(root).parts) > 16:
            raise ForgeError('Source directory depth exceeded')
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.name.startswith('.') or entry.name == '__pycache__':
                    continue
                count += 1
                if count > 1024:
                    raise ForgeError('Workspace source index exceeds budget')
                if entry.is_symlink():
                    raise ForgeError('Symlink in source index')
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif not entry.is_file(follow_symlinks=False):
                    raise ForgeError('Unexpected source entry')
                else:
                    # Declared configs have no extension restriction. Discovery
                    # filters Python/parameter files; hints cover every saved file.
                    paths.append(Path(entry.path))
    return sorted(paths)


def source_inventory(root, value):
    """Best-effort browsing/refresh inventory, never inspection evidence.

    One oversized or unreadable subtree must not disable the whole console.
    Omitted scopes are explicit; execution still resolves its complete closure.
    """
    paths, diagnostics = [], []
    try:
        names = author_roots(root, value)
    except (ForgeError, OSError, ValueError) as exc:
        return [], [f'源码浏览范围未完成：{str(exc)[:1024]}；不能据此判断规范检查结果。']
    for name in names:
        try:
            found = source_files(root, name)
        except (ForgeError, OSError, ValueError) as exc:
            diagnostics.append(f'{name}：源码索引未完成（{str(exc)[:1024]}）；此范围不参与源码自动同步。')
            continue
        if len(paths) + len(found) > 4096:
            diagnostics.append('源码观察已达 4096 个文件上限；其余范围未扫描，不参与源码自动同步。')
            break
        paths.extend(found)
    return sorted(paths), diagnostics


def report_files(root):
    directory = Path(root) / '.forge/inspections'
    no_symlinks(directory)
    paths, total = [], 0
    with os.scandir(directory) as entries:
        for entry in entries:
            if len(paths) >= 128:
                raise ForgeError('Inspection report budget exceeded')
            if entry.is_symlink() or not entry.is_file(follow_symlinks=False) or not re.fullmatch(r'[0-9a-f]{64}\.json', entry.name):
                raise ForgeError('Unexpected report entry')
            size = entry.stat(follow_symlinks=False).st_size
            total += size
            if size > 8 * 1024**2 or total > 1024**3:
                raise ForgeError('Inspection report byte budget exceeded')
            paths.append(Path(entry.path))
    return sorted(paths)


def list_reports(root, *, offset=0, limit=20, cursor=None):
    from demiflow.workbench.projects import index
    info = index.status(root)
    if info.get('reason') == 'index_not_initialized':
        # Read-only compatibility for the old bounded catalogue. Explicit
        # rebuild adopts existing facts without rewriting those reports.
        paths = report_files(root)
        selected = [dict(identity=p.stem, digest=p.stem) for p in paths[offset:offset + limit]]
        page = dict(entries=selected, total=len(paths), next_cursor=None, index=info)
    else:
        page = index.page(root, 'report', offset=offset, limit=limit, cursor=cursor)
    reports, total_bytes = [], 0
    for item in page['entries']:
        path = relative_path(root, '.forge/inspections/' + item['identity'] + '.json')
        total_bytes += path.stat().st_size
        if total_bytes > 32 * 1024**2:
            raise ForgeError('Report page exceeds byte budget; request fewer reports')
        report = load_report(root, item['identity'])
        if report['digest'] != item['digest']:
            raise ForgeError('Indexed report differs from its source')
        reports.append(dict(digest=report['digest'], entry=report['entry'], status=report['status'],
                            parameter_path=report['binding']['parameter_path'], current_binding='not_rechecked'))
    return dict(reports=reports, total=page['total'], next_cursor=page['next_cursor'], index=page['index'])


def workspace_changes(root):
    """Metadata hints, not bindings or snapshots. Never read datasets or journals."""
    from demiflow.workbench.projects import index

    root = Path(root).absolute()
    no_symlinks(root)

    def stamp(paths):
        values = []
        for path in paths:
            no_symlinks(path)
            try:
                info = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                values.append((str(path.relative_to(root)), None))
                continue
            if not stat.S_ISREG(info.st_mode):
                raise ForgeError('Change inventory requires regular files')
            values.append((str(path.relative_to(root)), info.st_dev, info.st_ino,
                           info.st_size, info.st_mtime_ns, info.st_ctime_ns))
        return content_digest(values)

    info = index.status(root)
    active = info.get('active_runs', [])
    if info.get('reason') == 'index_not_initialized':
        from demiflow.workbench.runs.manager import _run_ids
        active = _run_ids(root, maximum=128)
    runs = {run_id: stamp([root / '.forge/runs' / run_id / name
                          for name in ('run.sqlite', 'run.sqlite-wal', 'run.sqlite-journal')])
            for run_id in active}
    value = read_control(root / 'workspace.json')
    source_paths, source_diagnostics = source_inventory(root, value)
    from demiflow.workbench.projects.discovery import discover
    return dict(workspace=str(root), workspace_id=workspace_id(root, value),
                discovery=discover(root, value)['digest'],
                audit=stamp([root / '.forge/audit/status.json']),
                project_check=stamp([root / '.forge/project-check.json']),
                workspace_stamp=stamp([root / 'workspace.json', root / 'README.md']),
                sources=content_digest(dict(files=stamp(source_paths), diagnostics=source_diagnostics)),
                source_diagnostics=source_diagnostics,
                tables=stamp(table_files(root)), reports=content_digest({k: info.get(k) for k in ('epoch', 'reports')}),
                runs=runs, run_revision=content_digest(info), index=info)


def workspace_context(root):
    import ast
    from demiflow.workbench.runs.limits import limits_with

    root, value, snapshot = load_workspace(root)
    from demiflow.workbench.projects.discovery import discover
    paths, source_diagnostics = source_inventory(root, value)
    source_paths = [str(path.relative_to(root)) for path in paths if path.suffix == '.py']
    if len(source_paths) > 256:
        source_diagnostics.append('源码列表只显示前 256 个 Python 文件；显示范围不代表规范检查覆盖范围。')
        source_paths = source_paths[:256]
    tables = []
    table_bytes = 0
    for path in table_files(root):
        control, ref, uri = table_binding(root, value, path.stem)
        table_bytes += len(bounded_json(control, 16384))
        if table_bytes > 2 * 1024**2:
            raise ForgeError("Table index byte budget exceeded")
        tables.append(
            {
                "alias": path.stem,
                "reference": control,
                "uri": str(uri),
                "digest": content_digest(control),
                "binding_digest": preview_binding(root, control, uri)["digest"],
            }
        )
    flows = []
    parameter_files = []
    total = 0
    modern = value['format'] in MODERN_FORMATS
    registered = value.get('entries', {})
    files = sorted({validate_entry(root, entry)[0] for entry in registered}) if modern else source_files(root, 'flows')
    if modern:
        parameter_files = sorted({p for paths in registered.values() for p in paths})
    found = set()
    for path in files:
        if path.suffix == '.json':
            parameter_files.append(str(path.relative_to(root)))
            continue
        if path.suffix != '.py':
            continue
        try:
            raw = read_bounded(path, 1024**2)
            total += len(raw)
            if total > 16 * 1024**2:
                raise ForgeError('Flow index source bytes exceeded')
            tree = ast.parse(raw)
        except (SyntaxError, OSError, ValueError) as exc:
            entries = [e for e in registered if e.split(':')[0] == str(path.relative_to(root))] if modern else [str(path.relative_to(root))]
            flows.extend({'entry': e, 'diagnostic': str(exc), 'parameters': registered.get(e, [])} for e in entries)
            found.update(entries)
            continue
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and not node.name.startswith('_'):
                entry = str(path.relative_to(root)) + ':' + node.name
                if modern and entry not in registered:
                    continue
                flows.append(dict(entry=entry, line=node.lineno, description=ast.get_docstring(node) or '',
                                  parameters=registered.get(entry, [])))
                found.add(entry)
                if len(flows) > 256:
                    raise ForgeError('Flow entry budget exceeded')
    if modern:
        flows.extend(dict(entry=e, parameters=registered[e], diagnostic='Registered function is missing') for e in sorted(set(registered) - found))
    report_page = list_reports(root)
    try:
        readme = (read_bounded(root / "README.md", README_BYTES).decode("utf-8")
                  if (root / "README.md").exists() else "")
    except (ValueError, OSError) as exc:
        readme = '项目说明无法读取：' + str(exc)[:1024]
    supported = [
        {"id": r["id"], "semantics": r["native_semantics"]}
        for r in snapshot["catalog"]["objects"]
        if r["native_semantics"].get("format") == 'demiflow_api_flow_v2'
    ]
    return {
        "status": "loaded",
        "name": value["name"],
        "workspace": str(root),
        "workspace_id": workspace_id(root, value),
        "readme": readme,
        "source_files": sorted(source_paths),
        "source_diagnostics": source_diagnostics,
        "project": discover(root, value),
        "locations": value["locations"],
        "platform": value["platform"],
        "tables": tables,
        "flows": sorted(flows, key=lambda f: f["entry"]),
        "parameter_files": sorted(parameter_files),
        "reports": report_page['reports'],
        "report_total": report_page['total'],
        "report_cursor": report_page['next_cursor'],
        "index": report_page['index'],
        "supported_native_apis": supported,
        "native_api_coverage": [{"id": r['id'], "role": r['native_semantics'].get('role'),
                                  "contract_status": 'declared' if r['native_semantics'].get('format') == 'demiflow_api_flow_v2' else 'unknown',
                                  "reason": None if r['native_semantics'].get('format') == 'demiflow_api_flow_v2' else 'No current native Flow contract; managed execution is blocked'}
                                 for r in snapshot['catalog']['objects']],
        "standard": snapshot["standard"],
        "execution": "local_checked_v3",
        "execution_enabled_by_default": False,
        "run_limits": limits_with(policy=value.get('budgets')),
    }


def discover_project(root):
    from demiflow.workbench.projects.discovery import discover
    root, control, _ = load_workspace(root)
    return discover(root, control)


def project_document(root, path, *, expected):
    from demiflow.workbench.projects.discovery import document
    root, _, _ = load_workspace(root)
    return document(root, path, expected=expected)


def project_table(root, identity, *, expected, version=None):
    from demiflow.workbench.worker import operation
    return operation('project_table', root, identity=identity, expected=expected, version=version)


def project_sample(root, identity, *, expected, version, binding, columns=None, offset=0, limit=20, row_id=None):
    from demiflow.workbench.worker import operation
    return operation('project_table', root, identity=identity, expected=expected, version=version, binding=binding,
                     sample=dict(columns=columns, offset=offset, limit=limit, row_id=row_id))


def verify_run_basis(root, manifest):
    from demiflow.forge import audit
    root, project, _ = load_workspace(root)
    if manifest['format'] == 'demiflow_forge_run_manifest_v3' and project['format'] == FORMAT_V3:
        raise ForgeError('Workspace audit contract changed; prepare a new run', code='conflict')
    audit.require_scope(_check_change_scope(root), expected_scope=manifest.get('change_scope'))
    report = _report_status(root, manifest['report_digest'])
    if report['status'] != 'checked' or not report['verification']['complete']:
        raise ForgeError('Prepared basis is stale: ' + ', '.join(report.get('changes', [])), code='conflict')
    if manifest['parameter_path'] and read_control(relative_path(root, manifest['parameter_path'])) != manifest['original_parameters']:
        raise ForgeError('Prepared parameters changed', code='conflict')
