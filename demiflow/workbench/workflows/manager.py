"""Public Workbench operations for declarative multi-pipeline cross-project workflows."""
from __future__ import annotations

from collections import deque
import os
from pathlib import Path
import subprocess
import sys
import time

from demiflow.forge.project import load_project
from demiflow.forge.schema import ForgeError
from demiflow.forge.workflow import (
    discover_workflows,
    validate_workflow,
    validate_workflow_inputs,
    workflow_spec_digest,
)
from demiflow.forge.runs import manager as runs
from demiflow.workbench.workflows import store
from demiflow.workbench.workflows.resolver import (
    build_project_resolver,
    build_target_authority,
    check_workflow_permissions,
)

_NODE_TONE = {
    'pending': ('待执行', 'neutral'),
    'pending_resume': ('待续跑', 'neutral'),
    'waiting_project': ('排队中', 'running'),
    'preparing': ('准备中', 'running'),
    'running': ('运行中', 'running'),
    'succeeded': ('已完成', 'success'),
    'reused': ('已复用', 'success'),
    'failed': ('失败', 'failed'),
    'skipped': ('已跳过', 'neutral'),
    'cancelled': ('已停止', 'failed'),
    'interrupted': ('已中断', 'failed'),
}


def _public_projects(resolved_map: dict[str, dict], involved: list[str]) -> list[dict]:
    items = []
    for alias in involved:
        info = resolved_map.get(alias) or {}
        items.append({
            'alias': alias,
            'logical_ref': info.get('logical_ref', alias),
            'resolved': bool(info.get('resolved')),
            'workspace_id': info.get('workspace_id'),
            'name': info.get('name', alias),
            'root': info.get('root'),
            'is_self': bool(info.get('is_self', alias == 'self')),
            'permissions': info.get('permissions') or {},
            'error': info.get('error'),
        })
    return items


def _compact_resolved_projects(resolved_map: dict[str, dict], involved: list[str]) -> dict[str, dict]:
    out = {}
    for alias in involved:
        info = resolved_map.get(alias) or {}
        out[alias] = {
            'alias': alias,
            'logical_ref': info.get('logical_ref', alias),
            'workspace_id': info.get('workspace_id'),
            'name': info.get('name', alias),
            'root': info.get('root'),
            'is_self': bool(info.get('is_self', alias == 'self')),
            'permissions': info.get('permissions') or {},
        }
    return out


def build_workflow_graph_model(spec: dict, *, run_view: dict | None = None,
                               projects_info: list[dict] | None = None) -> dict:
    """Project a workflow DAG (and optional run snapshot) into ForgeWorkbenchGraph's GraphModel."""
    proj_by_alias = {p['alias']: p for p in (projects_info or ())}
    node_runs = (run_view or {}).get('nodes') or {}
    nodes = []
    for n in spec.get('nodes') or ():
        nid = n['id']
        nr = node_runs.get(nid)
        status = nr['status'] if nr else 'pending'
        label, tone = _NODE_TONE.get(status, (status, 'idle'))
        proj_meta = proj_by_alias.get(n['project']) or {}
        proj_label = proj_meta.get('name') or n.get('project_ref') or n['project']
        is_ext = n['project'] != 'self' and not proj_meta.get('is_self', False)
        kind_label = f'跨项目 · {proj_label}' if is_ext else f'本项目 · {proj_label}'
        summary_items = []
        if nr:
            art = nr.get('artifacts') or {}
            out_cnt = len(art.get('outputs') or {})
            mdl_cnt = len(art.get('models_list') or ())
            if out_cnt or mdl_cnt:
                bits = []
                if out_cnt:
                    bits.append(f'{out_cnt} 表')
                if mdl_cnt:
                    bits.append(f'{mdl_cnt} 模型')
                summary_items.append({'label': '产出', 'value': ' / '.join(bits)})
            elif nr.get('error'):
                summary_items.append({'label': '说明', 'value': str(nr['error'])[:80]})
        if not summary_items and n.get('params'):
            summary_items.append({'label': '参数', 'value': n['params']})
        nodes.append({
            'key': nid,
            'title': n.get('title') or nid,
            'kindLabel': kind_label,
            'apiExpr': n['entry'],
            'summary': summary_items,
            'state': {'label': label, 'tone': tone},
            'selection': {'key': nid, 'node_id': nid},
            'occurrences': [],
            'workflowNode': {
                'id': nid,
                'project': n['project'],
                'project_name': proj_label,
                'project_workspace_id': proj_meta.get('workspace_id'),
                'cross_project': is_ext,
                'entry': n['entry'],
                'params': n.get('params'),
                'concurrency_group': n.get('concurrency_group'),
                'status': status,
                'child_run_id': nr.get('child_run_id') if nr else None,
                'child_attempt': nr.get('child_attempt') if nr else None,
            },
        })
    edges = []
    for e in spec.get('edges') or ():
        raw_kind = e.get('kind', 'data')
        edge_kind = {
            'data': 'workflow_data',
            'model': 'workflow_model',
            'after': 'workflow_after',
        }.get(raw_kind, 'workflow_data')
        step_label = e.get('artifact') or ('顺序依赖' if raw_kind == 'after' else '')
        edges.append({
            'from': e['from'],
            'to': e['to'],
            'kind': edge_kind,
            'fromPort': e.get('artifact') or '',
            'toPort': e.get('target_param') or '',
            'step': step_label,
        })
    notices = [d['detail'] for d in (spec.get('diagnostics') or ())[:4]]
    return {
        'binding': spec.get('digest') or spec.get('path'),
        'choice': {'kind': 'workflow', 'label': '编排中尚无节点'},
        'execution': (run_view or {}).get('wf_run_id'),
        'nodes': nodes,
        'edges': edges,
        'notices': notices,
        'selected': None,
        'completeness': 'workflow_dag',
    }


def list_workflows(
    host_root: Path | str,
    *,
    registry_path: Path | str | None = None,
    registry_inventory: dict | None = None,
    project_map: dict[str, str | Path] | None = None,
    permission_ceiling: dict | None = None,
) -> dict:
    """List declared & discovered workflows in ``host_root`` with latest run summaries."""
    root = Path(host_root).absolute()
    _, control = load_project(root)
    resolver, resolved_map, _ = build_project_resolver(
        root,
        registry_path=registry_path,
        registry_inventory=registry_inventory,
        project_map=project_map,
        permission_ceiling=permission_ceiling,
    )
    items = discover_workflows(root, control=control, project_resolver=resolver)
    latest_by_path: dict[str, dict] = {}
    for run in list_workflow_runs(root, limit=50)['runs']:
        wf_path = run['workflow_path']
        if wf_path not in latest_by_path:
            latest_by_path[wf_path] = {
                'wf_run_id': run['wf_run_id'],
                'status': run['status'],
                'attempt': run['attempt'],
                'created_at': run['created_at'],
                'updated_at': run['updated_at'],
                'orchestrator_alive': run['orchestrator_alive'],
            }
    for item in items:
        item['latest_run'] = latest_by_path.get(item['path'])
        item['project_details'] = _public_projects(resolved_map, item['involved_projects'])
    return {
        'status': 'listed',
        'root': str(root),
        'total': len(items),
        'workflows': items,
    }


def inspect_workflow(
    host_root: Path | str,
    path: str,
    *,
    registry_path: Path | str | None = None,
    registry_inventory: dict | None = None,
    project_map: dict[str, str | Path] | None = None,
    permission_ceiling: dict | None = None,
) -> dict:
    """Validate one workflow including cross-project resolution, permissions, and GraphModel."""
    root = Path(host_root).absolute()
    _, control = load_project(root)
    resolver, resolved_map, _ = build_project_resolver(
        root,
        registry_path=registry_path,
        registry_inventory=registry_inventory,
        project_map=project_map,
        permission_ceiling=permission_ceiling,
    )
    spec = validate_workflow(root, path, control=control, project_resolver=resolver)
    perm_issues = check_workflow_permissions(spec, resolved_map, action='run_submit')
    projects_info = _public_projects(resolved_map, spec['involved_projects'])
    can_run = spec['status'] == 'passed' and not perm_issues
    return {
        'status': spec['status'],
        'can_run': can_run,
        'spec': spec,
        'projects': projects_info,
        'permission_diagnostics': perm_issues,
        'diagnostics': [*spec['diagnostics'], *perm_issues],
        'graph': build_workflow_graph_model(spec, projects_info=projects_info),
    }


def _validate_reuse_runs(spec: dict, resolved_map: dict[str, dict],
                         reuse_runs: dict[str, str] | None) -> dict[str, dict]:
    if reuse_runs is None:
        return {}
    if type(reuse_runs) is not dict or len(reuse_runs) > 64:
        raise ForgeError('reuse_runs must map node IDs to succeeded child run IDs', code='invalid_request')
    nodes_by_id = {n['id']: n for n in spec['nodes']}
    reused: dict[str, dict] = {}
    for node_id, run_id in reuse_runs.items():
        if node_id not in nodes_by_id:
            raise ForgeError(f'reuse_runs references unknown node: {node_id}', code='invalid_request')
        if type(run_id) is not str or not run_id:
            raise ForgeError(f'reuse_runs[{node_id}] must be a run ID string', code='invalid_request')
        node = nodes_by_id[node_id]
        proj = resolved_map[node['project']]
        target_root = proj['root']
        art = runs.run_artifacts(target_root, run_id)
        if art['status'] != 'succeeded':
            raise ForgeError(
                f'节点 {node_id} 指定的复用运行 {run_id} 状态为 {art["status"]}，仅允许复用已成功（succeeded）的运行。',
                code='conflict',
            )
        if art['entry'] != node['entry']:
            raise ForgeError(
                f'节点 {node_id} 指定的复用运行 {run_id} 入口为 {art["entry"]}，与节点入口 {node["entry"]} 不一致。',
                code='conflict',
            )
        reused[node_id] = art
    return reused


def _spawn_orchestrator(host_root: Path, wf_run_id: str, attempt: int) -> None:
    run_dir = store.run_directory(host_root, wf_run_id)
    script = Path(__file__).resolve().with_name('orchestrator.py')
    log_file = store.log_path(run_dir, attempt)
    env = dict(os.environ)
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    with open(log_file, 'ab') as out:
        subprocess.Popen(
            [sys.executable, '-I', '-B', str(script), 'run', str(host_root), wf_run_id, str(attempt)],
            cwd=str(host_root),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=out,
            start_new_session=True,
            close_fds=True,
        )


def start_workflow(
    host_root: Path | str,
    path: str,
    *,
    inputs: dict | None = None,
    reuse_runs: dict[str, str] | None = None,
    registry_path: Path | str | None = None,
    registry_inventory: dict | None = None,
    project_map: dict[str, str | Path] | None = None,
    permission_ceiling: dict | None = None,
) -> dict:
    """Validate, verify cross-project permissions & reuse targets, seal manifest, and spawn detached orchestrator."""
    root = Path(host_root).absolute()
    _, control = load_project(root)
    resolver, resolved_map, meta = build_project_resolver(
        root,
        registry_path=registry_path,
        registry_inventory=registry_inventory,
        project_map=project_map,
        permission_ceiling=permission_ceiling,
    )
    spec = validate_workflow(root, path, control=control, project_resolver=resolver)
    if spec['status'] != 'passed':
        detail = '; '.join(d['detail'] for d in spec['diagnostics'][:4])
        raise ForgeError(f'编排静态校验未通过：{detail}', code='invalid_request')

    perm_issues = check_workflow_permissions(spec, resolved_map, action='run_submit')
    if perm_issues:
        raise PermissionError('; '.join(d['detail'] for d in perm_issues[:4]))

    resolved_inputs = validate_workflow_inputs(spec, inputs)
    reused = _validate_reuse_runs(spec, resolved_map, reuse_runs)

    wf_run_id = store.new_workflow_run_id()
    run_dir = store.run_directory(root, wf_run_id)
    now = time.time()
    manifest = {
        'format': store.MANIFEST_FORMAT,
        'wf_run_id': wf_run_id,
        'host_root': str(root),
        'host_workspace_id': meta['host_workspace_id'],
        'workflow_path': spec['path'],
        'workflow_name': spec['name'],
        'workflow_digest': spec['digest'],
        'spec_digest': workflow_spec_digest(spec),
        'spec': spec,
        'inputs': resolved_inputs,
        'resolved_projects': _compact_resolved_projects(resolved_map, spec['involved_projects']),
        'authority_mode': meta['authority_mode'],
        'registry_path': meta['registry_path'],
        'permission_ceiling': meta['permission_ceiling'],
        'created_at': now,
    }
    store.write_manifest(run_dir, manifest)

    nodes_init = []
    for n in spec['nodes']:
        nid = n['id']
        proj = resolved_map[n['project']]
        if nid in reused:
            art = reused[nid]
            nodes_init.append({
                'node_id': nid,
                'status': 'reused',
                'project_alias': n['project'],
                'project_root': proj['root'],
                'project_workspace_id': proj.get('workspace_id'),
                'entry': n['entry'],
                'params': n.get('params'),
                'child_run_id': art['run_id'],
                'child_attempt': art['attempt'],
                'artifacts': art,
                'started_at': now,
                'finished_at': now,
            })
        else:
            nodes_init.append({
                'node_id': nid,
                'status': 'pending',
                'project_alias': n['project'],
                'project_root': proj['root'],
                'project_workspace_id': proj.get('workspace_id'),
                'entry': n['entry'],
                'params': n.get('params'),
            })
    store.initialize_ledger(run_dir, wf_run_id=wf_run_id, nodes_init=nodes_init, created_at=now)
    for nid, art in reused.items():
        store.append_event(run_dir, 1, 'node.reused', node_id=nid, detail={'child_run_id': art['run_id']})

    _spawn_orchestrator(root, wf_run_id, 1)
    return show_workflow_run(root, wf_run_id)


def _format_run_view(run_dir: Path, manifest: dict, snap: dict) -> dict:
    alive = store.is_orchestrator_alive(run_dir)
    spec = manifest['spec']
    proj_map = manifest.get('resolved_projects') or {}
    projects_info = [
        {**proj_map[alias], 'resolved': True, 'error': None}
        for alias in (spec.get('involved_projects') or ('self',))
        if alias in proj_map
    ]
    needs_reconcile = (
        snap['attempt'] == snap['current_attempt']
        and snap['status'] in store.ACTIVE_WORKFLOW_STATUSES
        and not alive
        and (time.time() - snap['updated_at']) > 2.0
    )
    node_list = []
    for n in spec.get('nodes') or ():
        nid = n['id']
        nr = snap['nodes'].get(nid) or {}
        node_list.append({
            'id': nid,
            'title': n.get('title') or nid,
            'description': n.get('description', ''),
            'project': n['project'],
            'project_name': (proj_map.get(n['project']) or {}).get('name', n['project']),
            'project_root': nr.get('project_root') or (proj_map.get(n['project']) or {}).get('root'),
            'project_workspace_id': nr.get('project_workspace_id') or (proj_map.get(n['project']) or {}).get('workspace_id'),
            'entry': n['entry'],
            'params': n.get('params'),
            'deps': n.get('deps') or [],
            'concurrency_group': n.get('concurrency_group'),
            'status': nr.get('status', 'pending'),
            'child_run_id': nr.get('child_run_id'),
            'child_attempt': nr.get('child_attempt'),
            'resolved_overrides': nr.get('resolved_overrides'),
            'bindings': nr.get('bindings'),
            'artifacts': nr.get('artifacts'),
            'error': nr.get('error'),
            'started_at': nr.get('started_at'),
            'finished_at': nr.get('finished_at'),
            'updated_at': nr.get('updated_at'),
        })
    counts: dict[str, int] = {}
    for item in node_list:
        counts[item['status']] = counts.get(item['status'], 0) + 1
    result = {
        'format': store.MANIFEST_FORMAT,
        'wf_run_id': snap['wf_run_id'],
        'workflow_path': manifest['workflow_path'],
        'workflow_name': manifest['workflow_name'],
        'workflow_digest': manifest['workflow_digest'],
        'host_root': manifest['host_root'],
        'host_workspace_id': manifest.get('host_workspace_id'),
        'attempt': snap['attempt'],
        'current_attempt': snap['current_attempt'],
        'status': snap['status'],
        'current_status': snap['current_status'],
        'stop_requested': snap['stop_requested'],
        'orchestrator_alive': alive,
        'needs_reconcile': needs_reconcile,
        'created_at': snap['created_at'],
        'started_at': snap['started_at'],
        'updated_at': snap['updated_at'],
        'finished_at': snap['finished_at'],
        'error': snap['error'],
        'inputs': manifest.get('inputs') or {},
        'policy': spec.get('policy') or {},
        'projects': projects_info,
        'counts': counts,
        'nodes': snap['nodes'],
        'node_list': node_list,
        'attempts': snap['attempts'],
        'events': snap['events'],
        'spec': spec,
    }
    result['graph'] = build_workflow_graph_model(spec, run_view=result, projects_info=projects_info)
    return result


def show_workflow_run(host_root: Path | str, wf_run_id: str, *, attempt: int | None = None,
                      event_limit: int = 80) -> dict:
    """Read-only bounded view of a workflow run (no implicit mutation on GET)."""
    run_dir = store.run_directory(host_root, wf_run_id)
    manifest = store.read_manifest(run_dir)
    snap = store.read_run_snapshot(run_dir, attempt=attempt, event_limit=event_limit)
    return _format_run_view(run_dir, manifest, snap)


def list_workflow_runs(host_root: Path | str, *, workflow_path: str | None = None,
                       offset: int = 0, limit: int = 20) -> dict:
    """List workflow runs stored under ``host_root/.forge/workflow-runs`` newest first."""
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
        raise ForgeError('Invalid workflow run page bounds', code='invalid_request')
    root = Path(host_root).absolute()
    entries = []
    for wf_run_id in store.scan_run_ids(root):
        run_dir = store.run_directory(root, wf_run_id)
        try:
            manifest = store.read_manifest(run_dir)
            if workflow_path is not None and manifest.get('workflow_path') != workflow_path:
                continue
            snap = store.read_run_snapshot(run_dir, event_limit=1)
        except (ForgeError, OSError):
            continue
        counts: dict[str, int] = {}
        for nr in snap['nodes'].values():
            st = nr['status']
            counts[st] = counts.get(st, 0) + 1
        entries.append({
            'wf_run_id': wf_run_id,
            'workflow_path': manifest['workflow_path'],
            'workflow_name': manifest['workflow_name'],
            'status': snap['current_status'],
            'attempt': snap['current_attempt'],
            'created_at': snap['created_at'],
            'updated_at': snap['updated_at'],
            'finished_at': snap['finished_at'],
            'error': snap['error'],
            'orchestrator_alive': store.is_orchestrator_alive(run_dir),
            'counts': counts,
            'node_total': len(snap['nodes']),
            'cross_project': bool((manifest.get('spec') or {}).get('cross_project')),
        })
    entries.sort(key=lambda r: (r['created_at'], r['wf_run_id']), reverse=True)
    return {
        'status': 'listed',
        'root': str(root),
        'total': len(entries),
        'offset': offset,
        'limit': limit,
        'runs': entries[offset:offset + limit],
    }


def cancel_workflow_run(
    host_root: Path | str,
    wf_run_id: str,
    *,
    expected_attempt: int | None = None,
    registry_path: Path | str | None = None,
    registry_inventory: dict | None = None,
    permission_ceiling: dict | None = None,
) -> dict:
    """Stop a running workflow and propagate cancellation to every active child run."""
    root = Path(host_root).absolute()
    run_dir = store.run_directory(root, wf_run_id)
    manifest = store.read_manifest(run_dir)
    if registry_path is not None or registry_inventory is not None or permission_ceiling is not None:
        resolver, resolved_map, _ = build_project_resolver(
            root,
            registry_path=registry_path or manifest.get('registry_path'),
            registry_inventory=registry_inventory,
            permission_ceiling=permission_ceiling if permission_ceiling is not None else manifest.get('permission_ceiling'),
        )
        for alias, ref in (manifest['spec'].get('projects') or {}).items():
            resolver(alias, ref)
        perm_issues = check_workflow_permissions(manifest['spec'], resolved_map, action='run_control')
        if perm_issues:
            raise PermissionError('; '.join(d['detail'] for d in perm_issues[:4]))

    attempt, status = store.request_stop(run_dir, expected_attempt=expected_attempt)
    if status == 'stopping':
        snap = store.read_run_snapshot(run_dir, attempt=attempt)
        for nid, nr in snap['nodes'].items():
            if nr['status'] in {'running', 'preparing', 'waiting_project'} and nr.get('child_run_id'):
                try:
                    auth = build_target_authority(manifest, nr['project_alias'], 'run_control')
                    runs.cancel(nr['project_root'], nr['child_run_id'], expected_attempt=nr.get('child_attempt'), authority=auth)
                except Exception:
                    pass
        if not store.is_orchestrator_alive(run_dir):
            # Orchestrator already died before stop; settle cleanly right away.
            reconcile_workflow_run(root, wf_run_id)
    return show_workflow_run(root, wf_run_id, attempt=attempt)


def reconcile_workflow_run(host_root: Path | str, wf_run_id: str) -> dict:
    """Reconcile a workflow run whose orchestrator stopped unexpectedly against real child run journals."""
    root = Path(host_root).absolute()
    run_dir = store.run_directory(root, wf_run_id)
    manifest = store.read_manifest(run_dir)
    if store.is_orchestrator_alive(run_dir):
        raise ForgeError('编排调度器仍在运行，无需对账恢复。', code='busy')

    snap = store.read_run_snapshot(run_dir)
    attempt = snap['current_attempt']
    nodes = snap['nodes']
    still_running = []

    for nid, nr in nodes.items():
        if nr['status'] not in {'preparing', 'running', 'waiting_project'}:
            continue
        child_run_id = nr.get('child_run_id')
        target_root = nr['project_root']
        if not child_run_id:
            store.update_node(run_dir, attempt, nid, status='interrupted', error='编排器意外中断（子节点未启动）', finished_at=time.time())
            nr['status'] = 'interrupted'
            continue
        try:
            child = runs.show(target_root, child_run_id, attempt=nr.get('child_attempt'))
        except Exception as exc:
            err_msg = f'对账查询子管线失败：{exc}'[:1024]
            store.update_node(run_dir, attempt, nid, status='interrupted', error=err_msg, finished_at=time.time())
            nr['status'] = 'interrupted'
            continue

        c_status = child.get('status')
        if c_status == 'succeeded':
            art = runs.run_artifacts(target_root, child_run_id, attempt=child.get('attempt'))
            store.update_node(
                run_dir, attempt, nid,
                status='succeeded',
                child_attempt=child.get('attempt'),
                artifacts=art,
                error=None,
                finished_at=time.time(),
            )
            store.append_event(run_dir, attempt, 'node.reconciled_succeeded', node_id=nid, detail={'child_run_id': child_run_id})
            nr['status'] = 'succeeded'
            nr['artifacts'] = art
        elif c_status in {'starting', 'running', 'stopping'}:
            if child.get('supervisor_alive') is True:
                still_running.append(nid)
                store.update_node(run_dir, attempt, nid, status='running', child_attempt=child.get('attempt'))
                nr['status'] = 'running'
            else:
                try:
                    rec = runs.reconcile(target_root, child_run_id)
                    c_status = rec.get('status', 'interrupted')
                except Exception:
                    c_status = 'interrupted'
                if c_status == 'succeeded':
                    art = runs.run_artifacts(target_root, child_run_id, attempt=child.get('attempt'))
                    store.update_node(run_dir, attempt, nid, status='succeeded', artifacts=art, error=None, finished_at=time.time())
                    nr['status'] = 'succeeded'
                    nr['artifacts'] = art
                else:
                    store.update_node(run_dir, attempt, nid, status=c_status, error=f'对账发现子管线处于 {c_status}', finished_at=time.time())
                    nr['status'] = c_status
        elif c_status == 'prepared':
            store.update_node(run_dir, attempt, nid, status='interrupted', error='子管线已准备但尚未启动', finished_at=time.time())
            nr['status'] = 'interrupted'
        else:
            store.update_node(run_dir, attempt, nid, status=c_status, error=f'子管线结束状态：{c_status}', finished_at=time.time())
            nr['status'] = c_status

    if snap['status'] in store.ACTIVE_WORKFLOW_STATUSES:
        statuses = {nr['status'] for nr in nodes.values()}
        if still_running:
            new_wf_status = 'interrupted'
            wf_err = f'编排调度器已退出，但子管线 {", ".join(still_running)} 仍在执行，可直接续跑接管。'
        elif snap['stop_requested']:
            new_wf_status = 'succeeded' if statuses <= {'succeeded', 'reused'} else 'cancelled'
            wf_err = None if new_wf_status == 'succeeded' else '编排已停止'
        elif statuses <= {'succeeded', 'reused'}:
            new_wf_status = 'succeeded'
            wf_err = None
        elif 'failed' in statuses:
            new_wf_status = 'failed'
            wf_err = '编排存在失败节点'
        else:
            new_wf_status = 'interrupted'
            wf_err = '编排调度器意外退出，已完成对账，支持断点续跑。'
        store.update_workflow_status(run_dir, attempt, new_wf_status, error=wf_err, finished=True)
        store.append_event(run_dir, attempt, 'workflow.reconciled', detail={'status': new_wf_status, 'still_running': still_running})

    return show_workflow_run(root, wf_run_id, attempt=attempt)


def _downstream_closure(spec: dict, start_node: str) -> set[str]:
    children: dict[str, list[str]] = {n['id']: [] for n in spec['nodes']}
    for n in spec['nodes']:
        for dep in n['deps']:
            children[dep].append(n['id'])
    visited = {start_node}
    q = deque([start_node])
    while q:
        cur = q.popleft()
        for nxt in children.get(cur, ()):
            if nxt not in visited:
                visited.add(nxt)
                q.append(nxt)
    return visited


def resume_workflow_run(
    host_root: Path | str,
    wf_run_id: str,
    *,
    from_node: str | None = None,
    reuse_runs: dict[str, str] | None = None,
    registry_path: Path | str | None = None,
    registry_inventory: dict | None = None,
    project_map: dict[str, str | Path] | None = None,
    permission_ceiling: dict | None = None,
) -> dict:
    """Create a new workflow attempt skipping verified succeeded/reused nodes and spawn a fresh orchestrator."""
    root = Path(host_root).absolute()
    run_dir = store.run_directory(root, wf_run_id)
    if store.is_orchestrator_alive(run_dir):
        raise ForgeError('编排仍在运行中，无法续跑。', code='busy')

    # Always reconcile first so completed/running children are accurately recorded.
    reconciled = reconcile_workflow_run(root, wf_run_id)
    manifest = store.read_manifest(run_dir)
    spec = manifest['spec']
    nodes_by_id = {n['id']: n for n in spec['nodes']}

    if from_node is not None and from_node not in nodes_by_id:
        raise ForgeError(f'指定的起始重跑节点不存在：{from_node}', code='invalid_request')

    # Re-verify current cross-project permissions before launching the new attempt.
    eff_reg = registry_path or manifest.get('registry_path')
    eff_ceiling = permission_ceiling if permission_ceiling is not None else manifest.get('permission_ceiling')
    eff_map = project_map
    if eff_map is None and manifest.get('authority_mode') == 'trusted_local':
        eff_map = {
            alias: info['root']
            for alias, info in (manifest.get('resolved_projects') or {}).items()
            if alias != 'self' and info.get('root')
        }
    resolver, resolved_map, _ = build_project_resolver(
        root,
        registry_path=eff_reg,
        registry_inventory=registry_inventory,
        project_map=eff_map,
        permission_ceiling=eff_ceiling,
    )
    for alias, ref in (spec.get('projects') or {}).items():
        resolver(alias, ref)
    perm_issues = check_workflow_permissions(spec, resolved_map, action='run_submit')
    if perm_issues:
        raise PermissionError('; '.join(d['detail'] for d in perm_issues[:4]))

    explicit_reused = _validate_reuse_runs(spec, resolved_map, reuse_runs)
    rerun_set = _downstream_closure(spec, from_node) if from_node is not None else set()

    prev_attempt = reconciled['current_attempt']
    new_attempt = prev_attempt + 1
    prev_nodes = reconciled['nodes']
    now = time.time()
    next_nodes = []

    for n in spec['nodes']:
        nid = n['id']
        prev = prev_nodes[nid]
        base = {
            'node_id': nid,
            'project_alias': prev['project_alias'],
            'project_root': prev['project_root'],
            'project_workspace_id': prev.get('project_workspace_id'),
            'entry': prev['entry'],
            'params': prev.get('params'),
        }
        if nid in explicit_reused:
            art = explicit_reused[nid]
            next_nodes.append({
                **base,
                'status': 'reused',
                'child_run_id': art['run_id'],
                'child_attempt': art['attempt'],
                'artifacts': art,
                'started_at': now,
                'finished_at': now,
            })
        elif nid in rerun_set:
            next_nodes.append({**base, 'status': 'pending'})
        elif prev['status'] in {'succeeded', 'reused'} and prev.get('artifacts') is not None:
            next_nodes.append({
                **base,
                'status': prev['status'],
                'child_run_id': prev.get('child_run_id'),
                'child_attempt': prev.get('child_attempt'),
                'resolved_overrides': prev.get('resolved_overrides'),
                'bindings': prev.get('bindings'),
                'artifacts': prev.get('artifacts'),
                'started_at': prev.get('started_at'),
                'finished_at': prev.get('finished_at'),
            })
        elif prev['status'] == 'running' and prev.get('child_run_id'):
            # Re-adopt still-running child job cleanly.
            next_nodes.append({
                **base,
                'status': 'running',
                'child_run_id': prev['child_run_id'],
                'child_attempt': prev.get('child_attempt'),
                'resolved_overrides': prev.get('resolved_overrides'),
                'bindings': prev.get('bindings'),
                'started_at': prev.get('started_at'),
            })
        elif prev['status'] == 'interrupted' and prev.get('child_run_id'):
            # Prefer resuming the interrupted child run to preserve P2/P4 checkpoints.
            next_nodes.append({
                **base,
                'status': 'pending_resume',
                'child_run_id': prev['child_run_id'],
                'child_attempt': prev.get('child_attempt'),
                'resolved_overrides': prev.get('resolved_overrides'),
                'bindings': prev.get('bindings'),
                'started_at': prev.get('started_at'),
            })
        else:
            next_nodes.append({**base, 'status': 'pending'})

    store.create_attempt(run_dir, attempt=new_attempt, from_node=from_node, nodes_init=next_nodes)
    _spawn_orchestrator(root, wf_run_id, new_attempt)
    return show_workflow_run(root, wf_run_id, attempt=new_attempt)
