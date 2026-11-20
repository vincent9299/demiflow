"""Project-wide inspection; never imports, repairs, or executes business code."""
import hashlib
import time

from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.forge.standards import engineering_standard
from demiflow.workbench.projects.discovery import discover
from demiflow.forge.source import InspectionError
from demiflow.forge.source import SourceTree

FORMAT = 'demiflow_forge_project_check_v1'
MAX_VARIANTS = 32


def check(root, *, table_resolver):
    from demiflow.workbench.projects import workspace as ws
    root, control, snapshot = ws.load_workspace(root)
    destination = root / '.forge/project-check.json'
    previous = content_digest(ws.read_control(destination, 2*1024**2)) if destination.exists() else None
    standard = engineering_standard()
    started, before = time.monotonic(), content_digest(control)
    platform = ws.check_snapshot(root / control['platform']['path'])
    if not platform.get('current_digest'):
        raise ForgeError('Current checker platform cannot be verified', code='operation_rejected')
    discovery = discover(root, control)
    variants = [(e, p) for e, paths in control.get('entries', {}).items() for p in paths]
    results, diagnostics, sources = [], [], {}
    if snapshot['standard']['digest'] != standard['digest']:
        diagnostics.append(dict(rule='FL-10', status='stale', path='workspace.json',
            detail='Historical standard binding: explicitly adopt the current platform before inspection.'))
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
    # Reuse the same source prerequisite checker for recognized entries that
    # have no native parameter binding. It cannot certify their Flow semantics.
    source_checks = []
    declared = {e for e, _ in variants}
    for item in discovery['entries']:
        if item['entry'] in declared or time.monotonic() - started > 35:
            continue
        tree = SourceTree(root, limits=dict(files=128, total_bytes=4*1024**2, seconds=10))
        outcome, detail = 'checked', None
        try:
            tree.load(item['path'][:-3].replace('/', '.'))
        except InspectionError as exc:
            outcome, detail = exc.diagnostic['status'], exc.diagnostic
        except (ValueError, OSError, SyntaxError, RecursionError) as exc:
            outcome = 'incomplete'
            detail = dict(rule='FL-01', status=outcome, path=item['path'], detail=str(exc)[:2048])
        if detail:
            diagnostics.append({**detail, 'entry': item['entry'], 'scope': 'source_prerequisites'})
        source_checks.append(dict(entry=item['entry'], status=outcome, sources=sorted(tree.files),
                                  coverage='source_prerequisites_only; flow_semantics_not_checked'))
        for path, sha in tree.files.items():
            if path in sources and sources[path] != sha:
                raise ForgeError('Source changed between inspections', code='conflict')
            sources[path] = sha
    if discovery['digest'] != discover(root, control)['digest']:
        raise ForgeError('Original project declarations changed during inspection', code='conflict')
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
    unbound = [e for e in discovery['entries'] if e['entry'] not in declared or e['protocol'] != 'native_flow']
    declaration_problem = any(d['code'] not in {'table_outside_project', 'table_missing', 'documentation_unreadable',
                                               'configuration_reference', 'configuration_unreadable'}
                              for d in discovery['diagnostics'])
    static = ('checked' if variants and len(results) == len(variants) and not diagnostics and not unbound and not declaration_problem
              and all(r['status'] == 'checked' for r in results) else
              'invalid' if any(r['status'] == 'invalid' for r in results + source_checks) else 'incomplete')
    diagnostics.extend(dict(rule='FL-01', status='incomplete', scope='discovery',
                            path=d['path'], detail=d['detail'], code=d['code'])
                       for d in discovery['diagnostics'])
    rules = []
    for rule in standard['rules']:
        implemented = rule['id'].startswith(('FL-', 'OP-'))
        # The checker can stop at the first violation. Do not label every
        # unrelated rule as failed (or passed) when its evidence is incomplete.
        failed = any(d.get('rule') == rule['id'] and d.get('status') == 'invalid' for d in diagnostics)
        evidence = 'passed' if static == 'checked' else 'invalid' if failed else 'incomplete'
        methods = {method: evidence
                   if method == 'static' and implemented else 'missing'
                   for method in rule['verification']}
        rules.append(dict(id=rule['id'], requirement=rule['requirement'], methods=methods, scope=rule['scope']))
    result = dict(format=FORMAT, workspace_id=ws.workspace_id(root, control),
        standard={k: standard[k] for k in ('id', 'version', 'digest')}, control_digest=before,
        static_status=static, status='invalid' if static == 'invalid' else 'incomplete',
        coverage=dict(scope='declared_entries_and_configurations', declared=len(variants),
                      inspected=len(results), recognized_entries=len(discovery['entries']),
                      source_inspected=len(source_checks), source_checks='prerequisites_only',
                      unbound_entries=len(unbound), undeclared_code='not_certified'),
        discovery_digest=discovery['digest'], source_checks=source_checks,
        checker_platform_digest=platform['current_digest'], workspace_platform_status=platform['status'],
        entries=results, sources=sources, rules=rules, diagnostics=diagnostics[:256],
        diagnostic_count=len(diagnostics), created_at=time.time())
    # Behavior/review obligations remain missing; static inspection is not certification.
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
    if not path.exists():
        return dict(status='missing', static_status='missing', workspace_id=ws.workspace_id(root, control))
    result = ws.read_control(path, 2*1024**2)
    digest = result.pop('digest', None)
    if result.get('format') != FORMAT or content_digest(result) != digest or result.get('workspace_id') != ws.workspace_id(root, control):
        raise ForgeError('Invalid project check record')
    result['digest'] = digest
    reasons = []
    if result['control_digest'] != content_digest(control):
        reasons.append('project_declarations_changed')
    if result.get('discovery_digest') and result['discovery_digest'] != discover(root, control)['digest']:
        reasons.append('original_project_evidence_changed')
    if result['standard']['digest'] != engineering_standard()['digest']:
        reasons.append('standard_changed')
    platform = ws.check_snapshot(root / control['platform']['path'])
    if (result.get('checker_platform_digest') != platform.get('current_digest') if result.get('checker_platform_digest')
            else platform['status'] != 'current'):
        reasons.append('platform_changed')
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
    return {**result, 'status': 'stale' if reasons else result['status'],
            'static_status': 'stale' if reasons else result['static_status'], 'changes': reasons}
