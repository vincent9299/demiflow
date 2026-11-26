"""Project application operations: worker proxies and cross-domain composition."""
import re

from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs


def begin_change_scope(root, *, writes, expected, watch_roots=None, limits=None):
    from demiflow.workbench.worker import operation
    root, value = ws._audit_workspace(root, expected)
    if value['audit']['change_scope'] is not None:
        raise ForgeError('A change scope is already active', code='conflict')
    names = ws.author_roots(root, value) if watch_roots is None else watch_roots
    if not names:
        names = sorted({item['path'].split('/')[0] for item in writes if item['path'] != 'workspace.json'})
    baseline = operation('audit_capture', root, watch_roots=names,
                         expected=expected, limits=limits)
    return ws._store_scope(root, value, baseline, writes, expected)


def audit_check(root, *, kind, report_digest=None, expected_policy, full_interval=300, authority=None):
    """Verify one object, then publish only a bounded derived observation."""
    from demiflow.workbench.worker import operation
    import time
    root, value = ws._audit_workspace(root, expected_policy)
    if (type(kind) is not str or kind not in {'scope', 'report'} or (kind == 'scope' and report_digest is not None)
            or (kind == 'report' and report_digest not in value['audit']['reports'])
            or type(full_interval) is not int or not 1 <= full_interval <= 3600):
        raise ForgeError('Invalid audit target or interval', code='invalid_request')
    with ws.audit_lease(root, 'check'):
        try:
            result = operation('audit_check', root, kind=kind, report_digest=report_digest, expected_policy=expected_policy, _authority=authority)
        except ForgeError as exc:
            result = dict(status='unverifiable', complete=False, error=dict(code=exc.code, message=str(exc)[:2048]))
        stamp = time.time()
        identity = 'scope' if kind == 'scope' else report_digest
        observation = dict(kind=kind, identity=identity, result=result, verified_at=stamp,
                           due_at=stamp + full_interval, pending=False)
        ws._audit_publish(root, expected_policy, lambda cache: cache['observations'].__setitem__(identity, observation))
    return observation


def audit_observe(root, *, expected_policy, session, previous_hint=None):
    """A cheap trigger cannot advance any completed-content timestamp."""
    from demiflow.workbench.worker import operation
    import time
    root, _ = ws._audit_workspace(root, expected_policy)
    if type(session) is not str or re.fullmatch('[0-9a-f]{32}', session) is None:
        raise ForgeError('Invalid watcher session', code='invalid_request')
    with ws.audit_lease(root, 'check'):
        try:
            hint = operation('audit_hint', root, expected_policy=expected_policy)
        except ForgeError:
            def invalidate(cache):
                for row in cache['observations'].values():
                    row['pending'] = True
            ws._audit_publish(root, expected_policy, invalidate)
            raise
        changed = hint != previous_hint
        def update(cache):
            cache['lightweight_observed_at'] = time.time()
            cache['watcher'] = dict(session=session, heartbeat=time.time())
            if changed:
                for row in cache['observations'].values():
                    row['pending'] = True
        ws._audit_publish(root, expected_policy, update)
    return dict(hint=hint, changed=changed)


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

    root, value, _ = ws.load_workspace(root)
    ws.identifier(alias)
    if location_id not in value["locations"]:
        raise ForgeError("Unknown location")
    base = ws.location_root(root, value["locations"][location_id])
    uri = ws.relative_path(base, relative_uri)
    description = operation("describe", uri, version=version)
    existing = operation(
        "catalog",
        base,
        relative_uri=relative_uri,
        version=description["version"],
        dataset_id=dataset_id,
    )
    return ws._attach_table_control(root, alias, relative_uri=relative_uri,
        location_id=location_id, dataset_id=dataset_id, schema_name=schema_name,
        schema_version=schema_version, expected=expected, expected_ref=expected_ref,
        description=description, existing=existing)


def set_table_view(root, alias, *, display, provenance, expected):
    """Read fixed table metadata, then CAS the presentation control in core."""
    from demiflow.workbench.worker import operation
    root, ref, uri = ws._table_view_inputs(root, alias, display=display, provenance=provenance)
    description = operation('describe', uri, version=ref.lance_version)
    return ws._set_table_view_control(root, alias,
        display=display, provenance=provenance, expected=expected, fields=description['fields'])


def register_flow(root, entry, *, params=None, authorize, authority=None):
    """Register an authored flow using the deployment's current authority."""
    from demiflow.forge.project import register_flow as register
    from demiflow.workbench.projects.registry import dependency_scope, project_dependencies
    from demiflow.workbench.table_bindings import make_resolver

    authorize()
    expected = project_dependencies(root, authority=authority, verify_files=False)
    def check_authority():
        authorize()
        if project_dependencies(root, authority=authority, verify_files=False) != expected:
            raise ForgeError('Project dependency approval changed during registration', code='conflict')

    with dependency_scope(root, authority=authority):
        return register(root, entry, params=params, authorize=check_authority,
                        table_resolver=make_resolver(ws.table_binding, runs.result_table))


def inspect_flow(root, entry, *, params, save=True, limits=None, authority=None):
    from demiflow.workbench.worker import operation

    return operation(
        "inspect", root, entry=entry, params=params, save=save, limits=limits, _authority=authority
    )


def check_project(root, *, authority=None):
    from demiflow.workbench.worker import operation
    return operation('project_check', root, _authority=authority)


def project_check_status(root, *, authority=None):
    from demiflow.workbench.worker import operation
    return operation('project_status', root, _authority=authority)


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


def report_status(root, digest, *, platform_check=True, authority=None):
    from demiflow.workbench.worker import operation

    return operation("status", root, digest=digest, platform_check=platform_check, _authority=authority)


def project_table(root, identity, *, expected, version=None):
    from demiflow.workbench.worker import operation
    return operation('project_table', root, identity=identity, expected=expected, version=version)


def project_sample(root, identity, *, expected, version, binding, columns=None, offset=0, limit=20, row_id=None):
    from demiflow.workbench.worker import operation
    return operation('project_table', root, identity=identity, expected=expected, version=version, binding=binding,
                     sample=dict(columns=columns, offset=offset, limit=limit, row_id=row_id))


def bind_audit_report(root, digest, *, expected):
    root, value = ws._audit_workspace(root, expected)
    result = report_status(root, digest)
    report = result['report']
    binding = report['binding']
    entry, params = binding['entry'], binding['parameter_path']
    if result['status'] != 'checked' or not params or params not in value['entries'].get(entry, []):
        raise ForgeError('Only a current checked registered entry/parameter report can be tracked')
    pins = []
    for old in value['audit']['reports']:
        original = ws.load_report(root, old)['binding']
        if (original['entry'], original['parameter_path']) != (entry, params):
            pins.append(old)
    value['audit']['reports'] = pins + [digest]
    return ws._publish_audit_policy(root, value, expected)


def attach_result(root, run_id, output, alias, *, attempt, expected_result):
    """Name one observed, verified result without replacing an existing alias."""
    if attempt is None:
        runs._attempt(runs.show(root, run_id)['attempt'], None)
    elif type(attempt) is not int or not 1 <= attempt <= 32:
        raise ForgeError('Attempt must be an integer in 1..32', code='invalid_request')
    if type(expected_result) is not str or not re.fullmatch(r'[0-9a-f]{64}', expected_result):
        raise ForgeError('Invalid fixed result digest', code='invalid_request')
    _, ref, _ = runs.result_table(root, run_id, output, attempt=attempt)
    if content_digest(ref.to_dict()) != expected_result:
        raise ForgeError('Fixed result differs from the observed reference', code='conflict')
    return attach_table(root, alias, relative_uri=ref.relative_uri, version=ref.lance_version,
                           location_id=ref.store_id, dataset_id=ref.dataset_id,
                           schema_name=ref.schema_name, schema_version=ref.schema_version,
                           expected_ref=ref)


def workspace_changes(root):
    """Compose the existing refresh fields from their domain owners.

    The application reads directory status once for composition. The project's
    existing bounded author inventory separately reads recent report metadata;
    these hints do not constitute a transaction or an execution authorization.
    """
    from demiflow.workbench.projects import index
    info = index.status(root)
    return {**ws.project_change_hints(root), **runs.run_change_hints(root, info),
            'reports': content_digest({key: info.get(key) for key in ('epoch', 'reports')}),
            'index': info}
