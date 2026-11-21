"""Project-wide inspection; never imports, repairs, or executes business code."""
import hashlib
import time

from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.forge.platform_snapshot import compare_snapshots, snapshot_complete

FORMAT = 'demiflow_workbench_project_static_check_v3'


def check(root, *, table_resolver):
    from demiflow.workbench.projects import workspace as ws
    from demiflow.forge.api_reference import sdk_identity
    from demiflow.forge.pipeline_check import check_project, verify_check_inputs
    root, control, snapshot = ws.load_workspace(root)
    destination = root / '.forge/project-check.json'
    previous = content_digest(ws.read_control(destination, 2*1024**2)) if destination.exists() else None
    before = content_digest(control)
    saved_snapshot = snapshot
    snapshot = ws.current_platform_snapshot(saved_snapshot)
    if not snapshot_complete(snapshot):
        raise ForgeError('Cannot check a project with incomplete SDK evidence', code='inspection_incomplete')
    platform = compare_snapshots(saved_snapshot, snapshot)
    catalog = snapshot['catalog']
    reference = dict(catalog=catalog, catalog_id=content_digest(catalog), sdk_revision=sdk_identity(catalog))

    digests = {}
    def publish(report):
        saved = ws._publish_inspection(root, control, snapshot, report)
        if saved['status'] == 'stale':
            raise ForgeError('Checker platform changed during project inspection', code='conflict')
        digests[(report['entry'], report['binding']['parameter_path'])] = saved['digest']

    report = check_project(root, reference=reference, table_resolver=table_resolver, publish=publish,
                           max_variants=32, seconds=35, max_bytes=32*1024**2)
    if ws.current_platform_snapshot(saved_snapshot)['digests']['snapshot'] != snapshot['digests']['snapshot']:
        raise ForgeError('Checker platform changed during project inspection', code='conflict')
    results = [{**item, 'digest': digests.get((item['entry'], item['params']))} for item in report['entries']]
    for item in results:
        if item['digest'] and ws._report_status(root, item['digest'], platform_check=False, table_resolver=table_resolver)['status'] != item['status']:
            raise ForgeError('Inspection inputs changed before project publication', code='conflict')
    statuses = {'failed': 'invalid', 'incomplete': 'incomplete', 'not_applicable': 'incomplete', 'passed': 'checked'}
    diagnostics = list(report['diagnostics'])
    if report['status'] == 'not_applicable':
        diagnostics.append(dict(rule='FL-01', status='incomplete', path='', detail='没有已登记的可运行入口。'))
    result = dict(format=FORMAT, workspace_id=ws.workspace_id(root, control), control_digest=before,
        status=statuses[report['status']], coverage=dict(declared=report['coverage']['requested_variants'], inspected=len(results)),
        checker_platform_digest=platform['current_digest'], workspace_platform_status=platform['status'],
        native_flow=report['native_flow'], project_layout=report['project_layout'], check_digest=report['check_digest'],
        authoring_status=report['status'], basis=report['basis'],
        entries=results, sources=report['basis']['sources'], diagnostics=diagnostics,
        diagnostic_count=report['diagnostic_count'] + len(diagnostics) - len(report['diagnostics']), created_at=time.time())
    result['digest'] = content_digest(result)
    bounded_json(result, 2*1024**2)
    with ws.control_lock(root):
        verify_check_inputs(root, report['basis'], max_bytes=32*1024**2)
        ws.write_control(destination, result, expected=previous, create=previous is None)
    return result


def status(root, *, table_resolver):
    from demiflow.workbench.projects import workspace as ws
    from demiflow.forge.project_rules import RULES_ID
    root, control, saved_snapshot = ws.load_workspace(root)
    path = root / '.forge/project-check.json'
    workspace_id = ws.workspace_id(root, control)
    if not path.exists():
        return dict(status='missing', workspace_id=workspace_id)
    result = ws.read_control(path, 2*1024**2)
    if not isinstance(result, dict):
        raise ForgeError('Invalid project check record')
    if result.get('format') != FORMAT:
        return dict(status='missing', workspace_id=workspace_id)
    digest = result.pop('digest', None)
    if content_digest(result) != digest or result.get('workspace_id') != workspace_id:
        raise ForgeError('Invalid project check record')
    result['digest'] = digest
    reasons = []
    if result['control_digest'] != content_digest(control):
        reasons.append('project_declarations_changed')
    if result['basis']['rules_id'] != RULES_ID:
        reasons.append('authoring_rules_changed')
    try:
        current = ws.current_platform_snapshot(saved_snapshot)
    except (OSError, ValueError, RuntimeError, TypeError, ImportError) as exc:
        current = None
        reasons.append('checker_platform_unverifiable: ' + str(exc)[:256])
    complete = current is not None and snapshot_complete(current)
    if current is not None:
        if not complete:
            reasons.append('checker_platform_incomplete')
        elif result['checker_platform_digest'] != current['digests']['snapshot']:
            reasons.append('checker_platform_changed')

    # A legacy project's snapshot compares in its original protocol. The current
    # checker identity above always uses the protocol that produced this report.
    if current is not None and current['format'] == saved_snapshot['format']:
        platform = compare_snapshots(saved_snapshot, current)
    else:
        platform = ws.check_snapshot(root / control['platform']['path'])
    for item in result['entries']:
        try:
            if item['digest'] and ws._report_status(root, item['digest'], platform_check=False, table_resolver=table_resolver)['status'] != item['status']:
                reasons.append('inspection_changed:' + item['entry'])
        except (ForgeError, OSError, ValueError):
            reasons.append('inspection_unverifiable:' + item['entry'])
    total = 0
    evidence = {**result['sources'], 'workspace.json': result['basis']['manifest_sha256']}
    evidence.update(result['basis']['controls'])
    evidence.update(result['basis']['parameter_digests'])
    for path, expected in evidence.items():
        try:
            raw = read_bounded(ws.relative_path(root, path), 32*1024**2 - total)
            total += len(raw)
            if hashlib.sha256(raw).hexdigest() != expected:
                reasons.append('source_changed:' + path)
        except (ValueError, OSError):
            reasons.append('source_unverifiable:' + path)
    status = result['status'] if not reasons else 'stale' if complete else 'incomplete'
    return {**result, 'status': status, 'changes': reasons,
            'platform_provenance': platform}
