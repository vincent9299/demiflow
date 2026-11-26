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
from demiflow.forge.project import (
    FORMAT, FORMAT_V2, FORMAT_V3, MODERN_FORMATS, load_project as load_workspace_control, load_project_snapshot as load_workspace,
    location_root, project_name, save_project as save_workspace_control, validate_entry, validate_parameter_path,
)
from demiflow.forge.project_io import identifier
from demiflow.forge.project_io import relative_path
from demiflow.forge.project_io import read_control
from demiflow.forge.project_io import control_lock
from demiflow.forge.project_io import write_control
from demiflow.forge.io import no_symlinks
from demiflow.forge.project_bindings import table_identity
from demiflow.forge.project_bindings import table_binding as local_table_binding
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.workbench.python_sources import source_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.platform_snapshot import build_snapshot
from demiflow.forge.platform_snapshot import check_snapshot
from demiflow.forge.platform_snapshot import write_snapshot
from demiflow.forge.schema import ForgeError

README_BYTES = 1024**2


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
        save_workspace_control(root, value, expected=expected)
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
    if content_digest(load_workspace_control(root)[1]) != expected:
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
        if content_digest(load_workspace_control(root)[1]) != expected:
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
        save_workspace_control(root, value, expected=expected)
    return dict(status='updated', workspace_digest=content_digest(value), audit=value['audit'])


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
    if load_workspace_control(root)[1] != value:
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


def _audit_check(root, *, table_resolver, kind, report_digest=None, expected_policy):
    root, value = _audit_workspace(root, expected_policy)
    if kind == 'scope' and report_digest is None:
        result = _check_change_scope(root)
        result.pop('scope', None)
    elif kind == 'report' and report_digest in value['audit']['reports']:
        checked = _report_status(root, report_digest, table_resolver=table_resolver, verify_dependencies=True)
        result = dict(status=checked['status'] if checked['verification']['complete'] else 'unverifiable',
                      complete=checked['verification']['complete'], changes=checked['changes'][:256],
                      entry=checked['report']['binding']['entry'],
                      parameter_path=checked['report']['binding']['parameter_path'],
                      verification=checked['verification'])
    else:
        raise ForgeError('Audit requires scope or one currently tracked report', code='invalid_request')
    if content_digest(load_workspace_control(root)[1]) != expected_policy:
        raise ForgeError('Audit policy changed during verification', code='conflict')
    return result


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
    if content_digest(load_workspace_control(root)[1]) != expected_policy:
        raise ForgeError('Audit policy changed during observation', code='conflict')
    return content_digest(dict(sources=source, controls=stamps, policy=expected_policy))


def set_budgets(root, limits, *, expected):
    from demiflow.workbench.pipeline_policy import limits_with
    root, value, _ = load_workspace(root)
    if value['format'] not in MODERN_FORMATS or content_digest(value) != expected:
        raise ForgeError('Budget policy requires the current workspace-v2 binding')
    effective = limits_with(policy=limits)
    value['budgets'] = dict(limits)
    with control_lock(root):
        save_workspace_control(root, value, expected=expected)
    return {'status': 'updated', 'workspace_digest': content_digest(value), 'limits': effective}


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


def unregister_flow(root, entry, *, expected):
    root, value, _ = load_workspace(root)
    if value['format'] not in MODERN_FORMATS or entry not in value['entries']:
        raise ForgeError('Unknown registered entry')
    if content_digest(value) != expected:
        raise ForgeError('Workspace changed; refresh before removing entry')
    del value['entries'][entry]
    with control_lock(root):
        save_workspace_control(root, value, expected=expected)
    return dict(status='unregistered', entry=entry)


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
        save_workspace_control(root, value, expected=expected)
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
        save_workspace_control(root, value, expected=digest)
    return {
        "status": "bound",
        "platform": value["platform"],
        "reports_revalidated": False,
    }


def table_files(root):
    directory = Path(root) / "tables"
    if not load_workspace_control(root)[1].get('locations'):
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
    control, ref, uri = local_table_binding(root, value, alias)
    if control['format'] == 'demiflow_forge_table_v2':
        from demiflow.workbench.catalog import validate
        validate(control['display'], control['provenance'], value['locations'])
    return control, ref, uri


def preview_binding(root, control, uri):
    """Identity of one resolved preview, distinct from the alias edit digest."""
    value = {"workspace": str(Path(root).absolute()), "uri": str(uri), "reference": table_identity(control)}
    bounded_json(value, 32 * 1024)
    return {**value, "alias": control["alias"], "digest": content_digest(value)}


def _attach_table_control(root, alias, *, relative_uri, location_id, dataset_id,
                          schema_name, schema_version, expected, expected_ref,
                          description, existing):
    """Validate actual metadata against a fixed reference and publish the control."""
    from demiflow.lance.refs import DatasetRef
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


def _table_view_inputs(root, alias, *, display, provenance):
    """Check the registered control and presentation before native metadata IO."""
    from demiflow.workbench.catalog import validate
    root, value, _ = load_workspace(root)
    identifier(alias)
    _, ref, uri = table_binding(root, value, alias)
    validate(display, provenance, value['locations'])
    return root, ref, uri


def _set_table_view_control(root, alias, *, display, provenance, expected, fields):
    """Validate presentation facts and atomically replace only its control."""
    from demiflow.workbench.catalog import validate, validate_fields
    root, value, _ = load_workspace(root)
    identifier(alias)
    control, _, _ = table_binding(root, value, alias)
    validate(display, provenance, value['locations'])
    validate_fields(display, fields)
    updated = {**control, 'format': 'demiflow_forge_table_v2', 'display': display, 'provenance': provenance}
    with control_lock(root):
        write_control(root / 'tables' / (alias + '.json'), updated, expected=expected)
    return {'status': 'updated', 'table': updated, 'digest': content_digest(updated)}


def current_platform_snapshot(snapshot):
    """Inspect the loaded checker; a workspace's historical snapshot is provenance."""
    return build_snapshot(snapshot['scope']['modules'], extras=snapshot['scope']['extras'],
                          limits=snapshot['limits'])


def _inspect_flow(root, entry, *, table_resolver, params, save=True, limits=None, shared_sources=None, dependencies=None):
    from demiflow.forge.pipeline_check import inspect_variant

    root, value, snapshot = load_workspace(root)
    if dependencies is None:
        from demiflow.forge.dependencies import binding as dependency_binding
        dependencies = dependency_binding(root)
    snapshot = current_platform_snapshot(snapshot)
    path = validate_parameter_path(root, params) if isinstance(params, str) else None
    parameters = read_control(path) if path else params
    report = inspect_variant(root, value, entry, parameters, snapshot['catalog'],
                             table_resolver=table_resolver, limits=limits)
    report['binding']['parameter_path'] = str(path.relative_to(root)) if path else None
    if shared_sources:
        report['binding']['shared_sources'] = shared_sources
    if dependencies is not None:
        report['binding']['project_dependencies'] = dependencies['digest']
    return _publish_inspection(root, value, snapshot, report, save=save)


def _publish_inspection(root, value, snapshot, report, *, save=True):
    binding = report['binding']
    path = validate_parameter_path(root, binding['parameter_path']) if binding['parameter_path'] else None
    parameters_value = binding['parameters']
    controls = binding['controls']
    binding.update(root=str(root), platform=snapshot['digests']['snapshot'])
    report['binding']['workspace_policy'] = workspace_policy(value, report['binding'])
    report['binding']['workspace'] = content_digest(report['binding']['workspace_policy'])
    if (
        load_workspace_control(root)[1] != value
        or any(
            content_digest(table_identity(read_control(root / p))) != digest
            for p, digest in controls.items()
        )
        or path
        and read_control(path) != parameters_value
    ):
        raise ForgeError("Workspace inputs changed during inspection")
    # Native implementation and environment identity are rechecked after the
    # candidate closure; no successful report spans a concurrent platform edit.
    final = current_platform_snapshot(snapshot)
    if final["digests"]["snapshot"] != snapshot["digests"]["snapshot"]:
        return {
            "status": "stale",
            "diagnostics": [
                {"rule": "FL-10", "detail": "Platform changed during flow inspection"}
            ],
        }
    if (
        load_workspace_control(root)[1] != value
        or any(
            content_digest(table_identity(read_control(root / p))) != digest
            for p, digest in controls.items()
        )
        or path
        and read_control(path) != parameters_value
    ):
        raise ForgeError("Workspace inputs changed before report publication")
    for name, sha in report["binding"]["sources"].items():
        if (
            source_digest(relative_path(root, name))
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
        from demiflow.workbench.projects.index import initialize_if_empty
        import shutil
        with control_lock(root):
            initialize_if_empty(root)
            directory = root / ".forge/inspections"
            no_symlinks(directory)
            directory.mkdir(exist_ok=True)
            target = directory / (report["digest"] + ".json")
            marker = index.begin(root, 'report', report['digest'])
            interrupted = False
            try:
                if not target.exists():
                    raw = bounded_json(report, 8 * 1024**2)
                    if shutil.disk_usage(root).free < 2 * len(raw) + 8 * 1024**2:
                        raise ForgeError('Insufficient disk headroom for the report')
                    write_control(target, report, create=True)
            except BaseException as exc:
                interrupted = not isinstance(exc, Exception)
                raise
            finally:
                if not interrupted:
                    publish_report_index(root, marker)
    return report


def report_index_entry(root, digest, *, deadline=None):
    from demiflow.forge import operation_budget
    operation_budget.check(deadline)
    from demiflow.forge.audit import digest as valid_digest
    valid_digest(digest)
    path = relative_path(root, '.forge/inspections/' + digest + '.json')
    if not path.exists():
        return None
    report = load_report(root, digest)
    return dict(kind='report', identity=digest, created_at=path.stat().st_mtime_ns / 1e9,
                digest=report['digest'], active=False)


def publish_report_index(root, marker, *, deadline=None):
    from demiflow.workbench.projects import index
    from demiflow.forge import operation_budget
    try:
        operation_budget.check(deadline)
        entry = report_index_entry(root, marker['identity'], deadline=deadline)
        operation_budget.check(deadline)
        error = index.publish(root, marker, entry)
    except Exception as exc:
        error = str(exc)[:1024]
    if error:
        try:
            import logging
            logging.getLogger(__name__).warning('Directory report %s: %s; demiflow workbench index ROOT --synchronize',
                                                marker['identity'], error[:1024])
        except Exception:
            pass
    return error


def report_index_entries(root, *, check, deadline):
    parent = relative_path(root, '.forge/inspections')
    if parent.exists():
        with os.scandir(parent) as entries:
            for entry in entries:
                check()
                if entry.is_symlink() or not entry.is_file(follow_symlinks=False) or not re.fullmatch(r'[0-9a-f]{64}\.json', entry.name):
                    raise ForgeError('Invalid report directory entry')
                value = report_index_entry(root, entry.name[:-5], deadline=deadline)
                if value is None:
                    raise ForgeError('Directory source is incomplete: ' + entry.name)
                yield value


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
            if item.get('kind') in {'output', 'location'} and 'location_id' in item:
                used.add(item['location_id'])
            for child in item.values():
                visit(child)
        elif type(item) is list:
            for child in item:
                visit(child)

    visit(binding['parameters'])
    for table in binding['tables'].values():
        ref = table.get('reference')
        if isinstance(ref, dict) and 'location_id' in ref:
            used.add(ref['location_id'])
    for resource in binding.get('resources', {}).values():
        if 'location_id' in resource:
            used.add(resource['location_id'])
    entry, params = binding['entry'], binding['parameter_path']
    return {'format': value['format'], 'id': value.get('id'), 'platform': value['platform'],
            **({'budgets': value['budgets']} if 'budgets' in value else {}),
            'entry_registered': entry in value.get('entries', {}),
            'parameters_registered': params in value.get('entries', {}).get(entry, []) if params else None,
            'locations': {key: value['locations'].get(key) for key in sorted(used)}}


def _report_status(root, digest, *, table_resolver, platform_check=True, verify_dependencies=False):
    from demiflow.errors import DemiflowError
    from demiflow.lance.preview import describe_table

    root, value, _ = load_workspace(root)
    report = load_report(root, digest)
    binding = report["binding"]
    reasons, checks = [], []
    complete = True  # Completeness concerns the business basis, not provenance.
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

    if verify_dependencies and 'project_dependencies' in binding:
        from demiflow.forge.dependencies import binding as dependency_binding

        def current_dependency_digest():
            locked = dependency_binding(root)
            return locked['digest'] if locked else None

        check('dependency', 'project_dependencies', current_dependency_digest, binding['project_dependencies'],
              'project_dependencies_changed', 'project_dependencies_unverifiable')

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
    original_policy = binding.get('workspace_policy')
    if type(original_policy) is dict:
        if content_digest(original_policy) != binding['workspace']:
            raise ForgeError('Original workspace policy digest differs', code='conflict')
        current_policy = {**current_policy, 'platform': original_policy.get('platform')}
    check('workspace', 'policy', lambda: content_digest(current_policy), binding['workspace'], 'workspace_changed')
    for path, sha in binding['sources'].items():
        check('source', path, lambda p=path: source_digest(relative_path(root, p)),
              sha, 'source_changed:' + path)
    for path, sha in binding['controls'].items():
        def read_binding(p=path):
            control = read_control(relative_path(root, p))
            return content_digest(table_identity(control) if p.startswith('tables/') else control)
        check('table_binding', path, read_binding, sha, 'table_binding_changed:' + path)
    for alias, table in binding['tables'].items():
        if isinstance(table.get('reference'), dict) and table['reference'].get('format') == 'demiflow_forge_table_uri_v1':
            def read_table(t=table):
                metadata = describe_table(Path(t['uri']), t['version'])
                return [t['uri'], metadata['schema_hash'], metadata['version']]
        else:
            def read_table(a=alias):
                _, ref, uri = table_resolver(root, value, a)
                metadata = describe_table(uri, ref.lance_version)
                return [str(uri), metadata['schema_hash'], metadata['version']]
        check('table', alias, read_table, [table['uri'], table['schema_hash'], table['version']],
              'table_identity_changed:' + alias, 'table_unavailable:' + alias)
    path = binding['parameter_path']
    if path:
        check('parameters', path, lambda: read_control(relative_path(root, path)), binding['parameters'], 'parameters_changed')
    provenance = check_snapshot(root / value['platform']['path']) if platform_check else None
    return dict(status='stale' if reasons else report['status'], report=report, changes=reasons,
                platform_checked=platform_check, platform_provenance=provenance,
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


def source_inventory(root, value, project):
    """Best-effort browsing/refresh inventory, never inspection evidence.

    One oversized or unreadable subtree must not disable the whole console.
    Omitted scopes are explicit; execution still resolves its complete closure.
    """
    paths, diagnostics = [], []
    try:
        names = author_roots(root, value)
    except (ForgeError, OSError, ValueError) as exc:
        names = ()
        diagnostics.append(f'源码浏览范围未完成：{str(exc)[:1024]}；不能据此判断规范检查结果。')
    definition_roots = {item['path'] for role in ('operators', 'prompts') for item in project[role]}
    for name in sorted(set(names) | definition_roots):
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
    if not directory.exists():
        return paths
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


def project_change_hints(root):
    """Metadata hints, not bindings or snapshots. Never read datasets or journals."""

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

    _, value = load_workspace_control(root)
    from demiflow.workbench.projects.discovery import discover
    project = discover(root, value)
    source_paths, source_diagnostics = source_inventory(root, value, project)
    return dict(workspace=str(root), workspace_id=workspace_id(root, value),
                discovery=project['digest'],
                audit=stamp([root / '.forge/audit/status.json']),
                project_check=stamp([root / '.forge/project-check.json']),
                workspace_stamp=stamp([root / 'workspace.json', root / '.forge/state.json', root / 'README.md']),
                sources=content_digest(dict(files=stamp(source_paths), diagnostics=source_diagnostics)),
                source_diagnostics=source_diagnostics,
                tables=stamp(table_files(root)))


def workspace_context(root):
    import ast
    from demiflow.workbench.pipeline_policy import limits_with

    root, value, snapshot = load_workspace(root)
    from demiflow.workbench.projects.discovery import discover
    project = discover(root, value)
    paths, source_diagnostics = source_inventory(root, value, project)
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
        "project": project,
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
