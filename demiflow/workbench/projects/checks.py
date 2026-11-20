"""Project-wide inspection; never imports, repairs, or executes business code."""
import hashlib
import time

from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError

FORMAT = 'demiflow_workbench_project_static_check_v2'
MAX_VARIANTS = 32


def check(root, *, table_resolver):
    from demiflow.workbench.projects import workspace as ws
    root, control, _ = ws.load_workspace(root)
    destination = root / '.forge/project-check.json'
    previous = content_digest(ws.read_control(destination, 2*1024**2)) if destination.exists() else None
    started, before = time.monotonic(), content_digest(control)
    platform = ws.check_snapshot(root / control['platform']['path'])
    if not platform.get('current_digest'):
        raise ForgeError('Current checker platform cannot be verified', code='operation_rejected')
    variants = [(e, p) for e, paths in control.get('entries', {}).items() for p in paths]
    results, diagnostics, sources = [], [], {}
    if not variants:
        diagnostics.append(dict(rule='FL-01', status='incomplete', path='',
            detail='没有已登记的原生流程与参数配置。'))
    for entry, params in variants[:MAX_VARIANTS]:
        if time.monotonic() - started > 35:
            break
        try:
            report = ws._inspect_flow(root, entry, params=params, save=True, table_resolver=table_resolver)
            results.append(dict(entry=entry, params=params, status=report['status'], digest=report.get('digest')))
            for name, sha in report.get('binding', {}).get('sources', {}).items():
                if name in sources and sources[name] != sha:
                    raise ForgeError('Source changed between inspections', code='conflict')
                sources[name] = sha
            diagnostics.extend({**d, 'entry': entry, 'params': params} for d in report.get('diagnostics', [])[:32])
        except (ForgeError, OSError, ValueError) as exc:
            # Conflict is not a candidate violation, and must not publish mixed evidence.
            if isinstance(exc, ForgeError) and exc.code == 'conflict':
                raise
            results.append(dict(entry=entry, params=params, status='incomplete', digest=None))
            diagnostics.append(dict(rule='FL-01', status='incomplete', path=entry.split(':')[0],
                entry=entry, params=params, detail=str(exc)[:2048]))
    if len(results) != len(variants):
        diagnostics.append(dict(rule='FL-10', status='incomplete', path='',
            detail='Inspection budget exhausted; remaining entries are not passes.'))
    if ws.check_snapshot(root / control['platform']['path']).get('current_digest') != platform['current_digest']:
        raise ForgeError('Checker platform changed during project inspection', code='conflict')
    if content_digest(ws.read_control(root / 'workspace.json')) != before:
        raise ForgeError('Project declarations changed during inspection', code='conflict')
    for item in results:
        if item['digest'] and ws._report_status(root, item['digest'], platform_check=False, table_resolver=table_resolver)['status'] != item['status']:
            raise ForgeError('Inspection inputs changed before project publication', code='conflict')
    total = 0
    for path, expected in sources.items():
        raw = read_bounded(ws.relative_path(root, path), min(1024**2, 32*1024**2 - total))
        total += len(raw)
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ForgeError('Source changed during inspection', code='conflict')
    if any(item['status'] == 'invalid' for item in results):
        outcome = 'invalid'
    elif not variants or len(results) != len(variants) or not all(item['status'] == 'checked' for item in results):
        outcome = 'incomplete'
    else:
        outcome = 'checked'
    result = dict(format=FORMAT, workspace_id=ws.workspace_id(root, control), control_digest=before,
        status=outcome, coverage=dict(declared=len(variants), inspected=len(results)),
        checker_platform_digest=platform['current_digest'], workspace_platform_status=platform['status'],
        entries=results, sources=sources, diagnostics=diagnostics[:256],
        diagnostic_count=len(diagnostics), created_at=time.time())
    result['digest'] = content_digest(result)
    bounded_json(result, 2*1024**2)
    with ws.control_lock(root):
        if content_digest(ws.read_control(root / 'workspace.json')) != before:
            raise ForgeError('Declarations changed before publication', code='conflict')
        ws.write_control(destination, result, expected=previous, create=previous is None)
    return result


def status(root, *, table_resolver):
    from demiflow.workbench.projects import workspace as ws
    root, control, _ = ws.load_workspace(root)
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
    platform = ws.check_snapshot(root / control['platform']['path'])
    for item in result['entries']:
        try:
            if item['digest'] and ws._report_status(root, item['digest'], platform_check=False, table_resolver=table_resolver)['status'] != item['status']:
                reasons.append('inspection_changed:' + item['entry'])
        except (ForgeError, OSError, ValueError):
            reasons.append('inspection_unverifiable:' + item['entry'])
    total = 0
    for path, expected in result.get('sources', {}).items():
        try:
            raw = read_bounded(ws.relative_path(root, path), min(1024**2, 32*1024**2 - total))
            total += len(raw)
            if hashlib.sha256(raw).hexdigest() != expected:
                reasons.append('source_changed:' + path)
        except (ValueError, OSError):
            reasons.append('source_unverifiable:' + path)
    return {**result, 'status': 'stale' if reasons else result['status'], 'changes': reasons,
            'platform_provenance': platform}
