"""Cross-project alias resolution and explicit per-project permission checks for workflows."""
from __future__ import annotations

from pathlib import Path

from demiflow.forge.project import load_project
from demiflow.forge.project_rules import diagnostic
from demiflow.forge.schema import ForgeError
from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs.authority import LocalAuthority, WebAuthority


def _effective_permissions(item: dict, ceiling: dict | None) -> dict[str, bool]:
    base = console.permissions(item)
    if ceiling is None:
        return base
    return {k: bool(base.get(k) and ceiling.get(k, True)) for k in console.PERMISSIONS}


def _find_in_inventory(inventory: dict, logical_ref: str) -> tuple[str, dict] | None:
    projects = inventory.get('projects') or {}
    # 1. Exact workspace ID match.
    if logical_ref in projects:
        return logical_ref, projects[logical_ref]
    # 2. Exact unique project name match.
    matches = [(ws_id, item) for ws_id, item in projects.items() if item.get('name') == logical_ref]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ForgeError(
            f'项目名称 {logical_ref!r} 在注册表中有多个同名项目，请改用唯一的工作区 ID 引用。',
            code='conflict',
        )
    return None


def _host_inventory_entry(inventory: dict | None, host_root: Path, host_ws_id: str) -> tuple[str | None, dict | None]:
    if not inventory:
        return None, None
    projects = inventory.get('projects') or {}
    if host_ws_id in projects:
        return host_ws_id, projects[host_ws_id]
    host_str = str(host_root)
    for ws_id, item in projects.items():
        if item.get('root') == host_str:
            return ws_id, item
    return None, None


def build_project_resolver(
    host_root: Path | str,
    *,
    registry_path: Path | str | None = None,
    registry_inventory: dict | None = None,
    project_map: dict[str, str | Path] | None = None,
    permission_ceiling: dict | None = None,
):
    """Build a resolver callback compatible with ``forge.workflow.validate_workflow`` plus full metadata."""
    host = Path(host_root).absolute()
    _, host_control = load_project(host)
    host_ws_id = ws.workspace_id(host, host_control)

    inventory = registry_inventory
    reg_path_str: str | None = None
    if registry_path is not None:
        reg_path_str = str(Path(registry_path).absolute())
        if inventory is None:
            inventory = console.load_registry(reg_path_str)

    host_reg_id, host_item = _host_inventory_entry(inventory, host, host_ws_id)
    if host_item is not None:
        host_perms = _effective_permissions(host_item, permission_ceiling)
    elif permission_ceiling is not None:
        host_perms = {k: bool(permission_ceiling.get(k, True)) for k in console.PERMISSIONS}
    else:
        host_perms = {k: True for k in console.PERMISSIONS}

    resolved_map: dict[str, dict] = {
        'self': {
            'alias': 'self',
            'logical_ref': host_control['name'],
            'resolved': True,
            'workspace_id': host_reg_id or host_ws_id,
            'name': host_control['name'],
            'root': str(host),
            'control': host_control,
            'permissions': host_perms,
            'is_self': True,
            'error': None,
        }
    }

    explicit_map = {}
    if project_map:
        for k, v in project_map.items():
            explicit_map[str(k)] = Path(v).absolute()

    def resolve(alias: str, logical_ref: str) -> dict:
        if alias in resolved_map:
            return resolved_map[alias]
        # 1. Explicit CLI --project alias=/path mapping (trusted local CLI mode only).
        if alias in explicit_map or logical_ref in explicit_map:
            target_root = explicit_map.get(alias) or explicit_map[logical_ref]
            try:
                ws.no_symlinks(target_root)
                _, ctrl = load_project(target_root)
                ws_id = ws.workspace_id(target_root, ctrl)
                info = {
                    'alias': alias,
                    'logical_ref': logical_ref,
                    'resolved': True,
                    'workspace_id': ws_id,
                    'name': ctrl['name'],
                    'root': str(target_root),
                    'control': ctrl,
                    'permissions': {k: True for k in console.PERMISSIONS},
                    'is_self': target_root == host,
                    'error': None,
                }
            except (ForgeError, OSError) as exc:
                info = {
                    'alias': alias,
                    'logical_ref': logical_ref,
                    'resolved': False,
                    'workspace_id': None,
                    'name': logical_ref,
                    'root': str(target_root),
                    'control': None,
                    'permissions': {k: False for k in console.PERMISSIONS},
                    'is_self': False,
                    'error': str(exc),
                }
            resolved_map[alias] = info
            return info

        # 2. Match host project itself by name or workspace ID.
        if logical_ref in {host_control['name'], host_ws_id}:
            info = {**resolved_map['self'], 'alias': alias, 'logical_ref': logical_ref}
            resolved_map[alias] = info
            return info

        # 3. Resolve through Workbench project registry.
        if inventory is not None:
            try:
                found = _find_in_inventory(inventory, logical_ref)
            except ForgeError as exc:
                info = {
                    'alias': alias,
                    'logical_ref': logical_ref,
                    'resolved': False,
                    'workspace_id': None,
                    'name': logical_ref,
                    'root': None,
                    'control': None,
                    'permissions': {k: False for k in console.PERMISSIONS},
                    'is_self': False,
                    'error': str(exc),
                }
                resolved_map[alias] = info
                return info
            if found is not None:
                ws_id, item = found
                target_root = Path(item['root']).absolute()
                try:
                    ws.no_symlinks(target_root)
                    _, ctrl = load_project(target_root)
                    if ws.workspace_id(target_root, ctrl) != ws_id:
                        raise ForgeError('目标项目身份与注册表记录不一致')
                    perms = _effective_permissions(item, permission_ceiling)
                    info = {
                        'alias': alias,
                        'logical_ref': logical_ref,
                        'resolved': True,
                        'workspace_id': ws_id,
                        'name': ctrl['name'],
                        'root': str(target_root),
                        'control': ctrl,
                        'permissions': perms,
                        'is_self': target_root == host,
                        'error': None,
                    }
                except (ForgeError, OSError) as exc:
                    info = {
                        'alias': alias,
                        'logical_ref': logical_ref,
                        'resolved': False,
                        'workspace_id': ws_id,
                        'name': item.get('name', logical_ref),
                        'root': str(target_root),
                        'control': None,
                        'permissions': {k: False for k in console.PERMISSIONS},
                        'is_self': False,
                        'error': str(exc),
                    }
                resolved_map[alias] = info
                return info

        info = {
            'alias': alias,
            'logical_ref': logical_ref,
            'resolved': False,
            'workspace_id': None,
            'name': logical_ref,
            'root': None,
            'control': None,
            'permissions': {k: False for k in console.PERMISSIONS},
            'is_self': False,
            'error': f'未在工作台项目注册表中找到项目 {logical_ref!r}（CLI 模式可通过 --project {alias}=/abs/path 指定）',
        }
        resolved_map[alias] = info
        return info

    return resolve, resolved_map, {
        'host_workspace_id': host_reg_id or host_ws_id,
        'registry_path': reg_path_str,
        'permission_ceiling': dict(permission_ceiling) if permission_ceiling is not None else None,
        'authority_mode': 'web' if (registry_path is not None or permission_ceiling is not None) else 'trusted_local',
    }


def check_workflow_permissions(spec: dict, resolved_map: dict[str, dict], *,
                               action: str = 'run_submit') -> list[dict]:
    """Verify every project referenced by ``spec`` is resolved and grants ``action``."""
    issues: list[dict] = []
    label = {'run_submit': '管线运行 (run_submit)', 'run_control': '管线停止与控制 (run_control)'}.get(action, action)
    seen: set[str] = set()
    for alias in spec.get('involved_projects') or ('self',):
        if alias in seen:
            continue
        seen.add(alias)
        info = resolved_map.get(alias)
        if info is None or not info.get('resolved'):
            issues.append(diagnostic(
                'WF-03', spec.get('path', ''),
                f'项目 {alias} 未解析：{info.get("error") if info else "未知项目别名"}。',
            ))
            continue
        perms = info.get('permissions') or {}
        if not perms.get(action):
            issues.append(diagnostic(
                'WF-05', spec.get('path', ''),
                f'跨项目权限不足：项目「{info["name"]}」（{alias}）未开启「{label}」权限，请先在该项目的项目设置中授权。',
            ))
    return issues


def build_target_authority(manifest: dict, project_alias: str, action: str):
    """Construct the exact LocalAuthority or WebAuthority for a target project in a workflow run."""
    proj = (manifest.get('resolved_projects') or {}).get(project_alias)
    if proj is None or not proj.get('root'):
        raise ForgeError(f'Workflow manifest missing target project: {project_alias}', code='conflict')
    target_root = str(Path(proj['root']).absolute())
    mode = manifest.get('authority_mode', 'trusted_local')
    if mode != 'web':
        return LocalAuthority(target_root)
    reg_path = manifest.get('registry_path')
    ceiling_map = manifest.get('permission_ceiling') or {}
    ceiling = action == 'read' or bool(ceiling_map.get(action, False))
    ws_id = proj.get('workspace_id')
    if reg_path is not None:
        inventory = console.load_registry(reg_path)
        item = (inventory.get('projects') or {}).get(ws_id)
        grant = action == 'read' or (item is not None and console.permissions(item).get(action) is True)
        return WebAuthority(target_root, ws_id, reg_path, action, ceiling, grant)
    single_grant = action == 'read' or bool((proj.get('permissions') or {}).get(action, False))
    return WebAuthority(target_root, ws_id, None, action, ceiling, single_grant)
