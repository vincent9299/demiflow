"""Single-project workspace controls, change-scope verification, and inspection publication.

Shared by Forge independent runs and Workbench without any Workbench imports.
"""
from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
import re
import shutil

from demiflow.forge import index
from demiflow.forge import operation_budget
from demiflow.forge.io import no_symlinks
from demiflow.forge.platform import bounded_json, content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.platform_snapshot import build_snapshot, check_snapshot
from demiflow.forge.project import (
    FORMAT,
    FORMAT_V2,
    FORMAT_V3,
    MODERN_FORMATS,
    load_project as load_workspace_control,
    load_project_snapshot as load_workspace,
    location_root,
    project_name,
    save_project as save_workspace_control,
    validate_entry,
    validate_parameter_path,
)
from demiflow.forge.project_bindings import table_binding as local_table_binding, table_identity
from demiflow.forge.project_io import control_lock, identifier, read_control, relative_path, write_control
from demiflow.forge.runtime import source_digest
from demiflow.forge.schema import ForgeError


def workspace_id(root, value):
    return value.get('id', 'l' + hashlib.sha256(str(Path(root).absolute()).encode()).hexdigest()[:32])


def scoped_data_location(root, path, *, private=()):
    """Admit registered data roots without granting project/host control ancestors."""
    path = Path(path).resolve()
    protected = (Path(root), Path.home(), *map(Path, private),
                 *map(Path, ('/etc', '/proc', '/sys', '/dev', '/run', '/root')))
    if any(path == parent or parent.is_relative_to(path) for parent in protected):
        return None
    if any(path.is_relative_to(parent) for parent in protected[2:]):
        return None
    if any(part.startswith('.') for part in path.parts[1:]):
        return None
    return path


def author_roots(root, value, names):
    from demiflow.forge.audit import roots
    names = roots(names)
    locations = [location_root(root, item) for item in value['locations'].values()]
    for name in names:
        path = relative_path(root, name)
        if any(path.is_relative_to(p) or p.is_relative_to(path) for p in locations):
            raise ForgeError('Author inventory must not overlap a registered data location')
    return names


def scope_baseline(root, value):
    from demiflow.forge import audit
    scope = value.get('audit', {}).get('change_scope')
    if scope is None:
        return None
    baseline = audit.validate_baseline(read_control(
        root / '.forge/audit/baselines' / (scope['baseline'] + '.json'), audit.BASELINE_BYTES))
    if baseline['digest'] != scope['baseline'] or baseline['workspace_id'] != value['id'] or baseline['root'] != str(root):
        raise ForgeError('Change baseline identity differs', code='conflict')
    author_roots(root, value, baseline['watch_roots'])
    audit.writes(scope['writes'], baseline['watch_roots'])
    return baseline


def check_change_scope(root):
    from demiflow.forge import audit
    root, value, _ = load_workspace(root)
    baseline = scope_baseline(root, value)
    scope = value.get('audit', {}).get('change_scope')
    if baseline is None:
        return dict(status='unconfigured', complete=False, scope=None)
    current = audit.inventory(root, baseline['watch_roots'], baseline['limits'])
    if load_workspace_control(root)[1] != value:
        raise ForgeError('Workspace changed during scope check', code='conflict')
    result = audit.compare(baseline, current, scope['writes'], workspace_policy_sha256=audit.workspace_digest(value))
    return dict(result, scope=scope, scope_digest=audit.scope_digest(scope))


def table_binding(root, value, alias):
    return local_table_binding(root, value, alias)


def dataset_ref(value):
    from demiflow.lance.refs import DatasetRef
    return DatasetRef.from_dict(value)


def current_platform_snapshot(snapshot):
    """Inspect the loaded checker; a workspace's historical snapshot is provenance."""
    return build_snapshot(snapshot['scope']['modules'], extras=snapshot['scope']['extras'],
                          limits=snapshot['limits'])


def load_report(root, digest):
    if not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest):
        raise ForgeError('Invalid report identity')
    report = read_control(Path(root) / '.forge/inspections' / (digest + '.json'), 8 * 1024**2)
    if (
        report.get('format') not in {'demiflow_forge_flow_inspection_v1', 'demiflow_forge_flow_inspection_v2', 'demiflow_forge_flow_inspection_v3'}
        or report.get('digest') != digest
        or content_digest({k: v for k, v in report.items() if k != 'digest'}) != digest
    ):
        raise ForgeError('Invalid or modified inspection report')
    return report


def report_index_entry(root, digest, *, deadline=None):
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
    try:
        operation_budget.check(deadline)
        entry = report_index_entry(root, marker['identity'], deadline=deadline)
        operation_budget.check(deadline)
        error = index.publish(root, marker, entry)
    except Exception as exc:
        error = str(exc)[:1024]
    if error:
        try:
            logging.getLogger(__name__).warning(
                'Directory report %s: %s; demiflow forge run rebuild-index ROOT',
                marker['identity'], error[:1024],
            )
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


def workspace_policy(value, binding):
    """Bind only this Flow's registration and declared data access locations."""
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


def publish_inspection(root, value, snapshot, report, *, save=True):
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
        raise ForgeError('Workspace inputs changed during inspection')
    final = current_platform_snapshot(snapshot)
    if final['digests']['snapshot'] != snapshot['digests']['snapshot']:
        return {
            'status': 'stale',
            'diagnostics': [
                {'rule': 'FL-10', 'detail': 'Platform changed during flow inspection'}
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
        raise ForgeError('Workspace inputs changed before report publication')
    for name, sha in report['binding']['sources'].items():
        if source_digest(relative_path(root, name)) != sha:
            raise ForgeError('Candidate source changed before report publication')
    for binding_item in report['binding'].get('resources', {}).values():
        if binding_item['kind'] in {'config', 'file'} and hashlib.sha256(read_bounded(relative_path(root, binding_item['path']), binding_item.get('max_bytes', 1024**2))).hexdigest() != binding_item['sha256']:
            raise ForgeError('Declared resource changed before report publication')
    report.pop('digest', None)
    report['digest'] = content_digest(report)
    if save:
        with control_lock(root):
            index.initialize_if_empty(root)
            directory = root / '.forge/inspections'
            no_symlinks(directory)
            directory.mkdir(exist_ok=True)
            target = directory / (report['digest'] + '.json')
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


DEPENDENCIES_UNSET = object()


def inspect_flow(root, entry, *, table_resolver, params, save=True, limits=None, shared_sources=None,
                 dependencies=DEPENDENCIES_UNSET, allowed_roots=None):
    from demiflow.forge.pipeline_check import inspect_variant

    root, value, snapshot = load_workspace(root)
    if dependencies is DEPENDENCIES_UNSET:
        from demiflow.forge.dependencies import binding as dependency_binding
        dependencies = dependency_binding(root, allowed_roots=allowed_roots)
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
    return publish_inspection(root, value, snapshot, report, save=save)


def verify_declared_entry(root, control, entry):
    """Confirm that a native flow entry is declared in workspace.json and resolves to a function."""
    import ast
    if entry not in (control.get('entries') or {}):
        raise ForgeError('Native entry must be declared by this project')
    target, symbol = validate_entry(root, entry)
    raw = read_bounded(target, 4 * 1024**2)
    try:
        tree = ast.parse(raw.decode('utf-8'), filename=str(target))
    except (SyntaxError, UnicodeDecodeError, RecursionError) as exc:
        raise ForgeError('Native entry source cannot be parsed') from exc
    if not any(isinstance(node, ast.FunctionDef) and node.name == symbol for node in tree.body):
        raise ForgeError('Native entry function not found in declared source')
    return target, symbol


def report_status(root, digest, *, table_resolver, platform_check=True, verify_dependencies=False, allowed_roots=None):
    from demiflow.errors import DemiflowError
    from demiflow.lance.preview import describe_table

    root, value, _ = load_workspace(root)
    report = load_report(root, digest)
    binding = report['binding']
    reasons, checks = [], []
    complete = True
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
            live = dependency_binding(root, allowed_roots=allowed_roots)
            return live['digest'] if live else None

        check('dependency', 'project_dependencies', current_dependency_digest, binding['project_dependencies'],
              'project_dependencies_changed', 'project_dependencies_unverifiable')

    if report['format'] != 'demiflow_forge_flow_inspection_v3':
        reasons.append('historical_contract_read_only')
        complete = False
    for resource in binding.get('resources', {}).values():
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

