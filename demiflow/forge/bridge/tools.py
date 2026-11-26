"""Forge-owned Agent MCP tool schemas and controlled project layout mutations."""
from __future__ import annotations

from pathlib import Path
import re
import shutil

from .common import reject, safe_path

CATALOG_TOOL = dict(
    name='list_registered_projects',
    description='List visible Workbench projects, or inspect one logical name/workspace ID for registered Flows and bound parameter keys. Use offset to page.',
    inputSchema=dict(type='object', additionalProperties=False, properties={
        'project': dict(type='string'), 'entry': dict(type='string'), 'params': dict(type='string'),
        'offset': dict(type='integer', minimum=0),
        'limit': dict(type='integer', minimum=1, maximum=16),
    }),
)

INPUT_TOOL = dict(
    name='read_input',
    description='Discover and read bounded input data.',
    inputSchema=dict(type='object', additionalProperties=False, properties={
        'action': dict(type='string', enum=['list', 'find', 'read', 'inspect']),
        'path': dict(type='string'), 'pattern': dict(type='string'),
        'depth': dict(type='integer'), 'offset': dict(type='integer'), 'bytes': dict(type='integer'),
        'limit': dict(type='integer'), 'columns': dict(type='array', items=dict(type='string')),
    }),
)

REVIEW_TOOL = dict(
    name='request_pipeline_review',
    description='Request an independent read-only delivery review after this author turn finishes. '
                'By default review the source changes observed in this development session. '
                'Supply an entry (and optional params) to review a particular flow.',
    inputSchema=dict(type='object', additionalProperties=False, properties={
        'entry': dict(type='string', description='Project-relative flow.py:function'),
        'params': dict(type='string', description='Project-relative parameter file'),
    }),
)

_STANDARD_PROJECT_DIRS = frozenset({
    'flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks', 'workflows', 'data', 'logs', 'tests', 'docs',
})
_MOVE_DEST_DIRS = frozenset({
    'flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks', 'workflows', 'tests', 'docs',
})
_PROTECTED_TOP_DIRS = frozenset({'.forge', '.git', '.venv', 'runs', 'data', 'logs', 'packages'})
_PROTECTED_ROOT_FILES = frozenset({'workspace.json', 'AGENTS.md', 'PIPELINE_SPEC.md', 'README.md'})
_PROTECTED_DATA_SUFFIXES = frozenset({'.lance', '.parquet', '.arrow', '.db', '.sqlite', '.sqlite3'})

PROJECT_LAYOUT_TOOL = dict(
    name='manage_project_layout',
    description=(
        'Safely manage controlled project structure, file organization and workspace.json registrations on the host: '
        'ensure_dirs creates missing standard directories (flows/operators/prompts/configs/data_tools/notebooks/workflows/data/logs/tests/docs); '
        'register_flow checks and registers a flow entry in workspace.json (defaulting to configs/<flow>.json); '
        'register_workflow validates and registers a declarative workflows/<name>.json file in workspace.json; '
        'bind_params updates an existing registered entry parameter binding under configs/*.json; '
        'move_path moves an existing project file or directory into a standard role directory (e.g. root configs to configs/, root *.ipynb to notebooks/, review tools to data_tools/) without losing bytes or overwriting existing files; '
        'remove_path deletes obsolete unreferenced non-data files or directories (e.g. non-standard directories or unused orphan flows/configs/prompts/scripts), strictly blocking notebooks, business data, runs/, and registered entries; '
        'sync_env_example generates/updates root .env.example (empty-value template ONLY, never real secrets) from prompts/*.yaml api_key_env/base_url_env declarations (plus optional extra variable names) and ensures .gitignore ignores .env and .forge/; '
        'slim_manifest normalizes workspace.json to the minimal human-readable project manifest (name/entries/budgets) while storing Forge internal state in .forge/state.json.'
    ),
    inputSchema=dict(type='object', additionalProperties=False, required=['action'], properties={
        'action': dict(type='string', enum=[
            'ensure_dirs', 'register_flow', 'register_workflow', 'bind_params', 'move_path', 'remove_path', 'sync_env_example', 'slim_manifest',
        ]),
        'dirs': dict(type='array', items=dict(type='string')),
        'entry': dict(type='string'),
        'params': dict(type='string'),
        'old_params': dict(type='string'),
        'path': dict(type='string'),
        'src': dict(type='string'),
        'dst': dict(type='string'),
        'paths': dict(type='array', items=dict(type='string')),
        'variables': dict(type='array', items=dict(type='string')),
    }),
)


def _registered_control_paths(root: Path) -> set[str]:
    from demiflow.forge import project as forge_project
    try:
        _, value = forge_project.load_project(root)
    except Exception:
        return set()
    entries = value.get('entries') or {}
    used = {e.partition(':')[0] for e in entries}
    for plist in entries.values():
        used.update(plist)
    for wf in value.get('workflows') or ():
        used.add(wf)
    return used


def _validate_tree_mutable(root: Path, rel: str, target: Path, *, allow_notebooks: bool, registered: set[str]):
    parts = Path(rel).parts
    if not parts or any(p.startswith('.') for p in parts):
        reject(f'Protected or hidden path cannot be modified: {rel}', 'operation_rejected')
    top = parts[0]
    if top in _PROTECTED_TOP_DIRS or (not allow_notebooks and top == 'notebooks'):
        reject(f'Protected project directory cannot be modified: {rel}', 'operation_rejected')
    if len(parts) == 1 and top in _PROTECTED_ROOT_FILES:
        reject(f'Protected root file cannot be modified: {rel}', 'operation_rejected')
    if rel in registered:
        reject(f'Registered entry or bound parameter cannot be moved/removed directly: {rel}', 'conflict')
    if target.is_symlink():
        reject(f'Symbolic link cannot be moved/removed: {rel}', 'operation_rejected')
    if target.is_file():
        suf = target.suffix.lower()
        if suf in _PROTECTED_DATA_SUFFIXES or (not allow_notebooks and suf == '.ipynb'):
            reject(f'Protected data or notebook file cannot be deleted: {rel}', 'operation_rejected')
        return
    if target.is_dir():
        count = 0
        stack = [target]
        while stack:
            cur = stack.pop()
            for child in cur.iterdir():
                count += 1
                if count > 4096:
                    reject(f'Directory tree exceeds modification budget: {rel}', 'resource_limit')
                if child.is_symlink():
                    reject(f'Directory contains symbolic link: {rel}', 'operation_rejected')
                child_rel = child.relative_to(root).as_posix()
                if child_rel in registered:
                    reject(f'Directory contains registered entry or parameter: {child_rel}', 'conflict')
                suf = child.suffix.lower()
                if suf in _PROTECTED_DATA_SUFFIXES or (not allow_notebooks and suf == '.ipynb'):
                    reject(f'Directory contains protected data or notebook file: {child_rel}', 'operation_rejected')
                if child.is_dir():
                    stack.append(child)


def run_project_layout(project_root, arguments):
    from demiflow.forge import project as forge_project
    from demiflow.forge.io import atomic_write_text, ensure_directory
    from demiflow.forge.project_io import control_lock
    if type(arguments) is not dict:
        reject('Invalid manage_project_layout arguments', 'invalid_request')
    action = arguments.get('action')
    root = Path(project_root).absolute()
    if action == 'ensure_dirs':
        dirs = arguments.get('dirs')
        if not isinstance(dirs, list) or not dirs or len(dirs) > 16:
            reject('ensure_dirs requires a non-empty list of standard directory names', 'invalid_request')
        created, existing = [], []
        for item in dirs:
            if not isinstance(item, str) or item not in _STANDARD_PROJECT_DIRS:
                reject(f'Directory not in standard Forge layout: {item!r}', 'invalid_request')
            target = safe_path(root, item, missing=True)
            if target.exists():
                if not target.is_dir():
                    reject(f'Path exists and is not a directory: {item}', 'conflict')
                existing.append(item)
            else:
                ensure_directory(target)
                created.append(item)
        return dict(status='ok', action='ensure_dirs', created=created, existing=existing)
    if action == 'register_flow':
        entry = arguments.get('entry')
        params = arguments.get('params')
        if not isinstance(entry, str) or not entry:
            reject('register_flow requires entry (e.g. flows/demo.py:run)', 'invalid_request')
        if params is not None and not isinstance(params, str):
            reject('params must be a relative JSON path', 'invalid_request')
        return forge_project.register_flow(root, entry, params=params)
    if action == 'register_workflow':
        wf_path = arguments.get('path')
        if not isinstance(wf_path, str) or not wf_path:
            reject('register_workflow requires path (e.g. workflows/demo.json)', 'invalid_request')
        return forge_project.register_workflow(root, wf_path)
    if action == 'bind_params':
        entry = arguments.get('entry')
        params = arguments.get('params')
        old_params = arguments.get('old_params')
        if not isinstance(entry, str) or not isinstance(params, str):
            reject('bind_params requires entry and params', 'invalid_request')
        forge_project.validate_entry(root, entry)
        param_file = forge_project.validate_parameter_path(root, params)
        if not param_file.is_file():
            reject(f'Parameter file does not exist: {params}', 'not_found')
        with control_lock(root):
            _, value = forge_project.load_project(root)
            expected = forge_project.content_digest(value)
            entries = value.get('entries') or {}
            if entry not in entries:
                reject(f'Entry is not registered in workspace.json: {entry}', 'not_found')
            bound = list(entries[entry])
            if old_params is not None:
                if old_params not in bound:
                    reject(f'old_params not bound to {entry}: {old_params}', 'not_found')
                bound = [params if p == old_params else p for p in bound]
                bound = list(dict.fromkeys(bound))
            else:
                bound = [params]
            value['entries'][entry] = bound
            forge_project.save_project(root, value, expected=expected)
        return dict(status='updated', entry=entry, params=bound)
    if action == 'move_path':
        src_raw = arguments.get('src')
        dst_raw = arguments.get('dst')
        if not isinstance(src_raw, str) or not src_raw or not isinstance(dst_raw, str) or not dst_raw:
            reject('move_path requires non-empty relative src and dst paths', 'invalid_request')
        src_path = safe_path(root, src_raw, missing=False)
        dst_path = safe_path(root, dst_raw, missing=True)
        src_rel = src_path.relative_to(root).as_posix()
        dst_rel = dst_path.relative_to(root).as_posix()
        src_parts = Path(src_rel).parts
        dst_parts = Path(dst_rel).parts
        if len(dst_parts) < 2 or dst_parts[0] not in _MOVE_DEST_DIRS or any(p.startswith('.') for p in dst_parts):
            reject(f'move_path destination must be inside a standard role directory: {dst_rel}', 'invalid_request')
        if src_parts and src_parts[0] == 'notebooks':
            reject(f'Files already inside notebooks/ cannot be moved out: {src_rel}', 'operation_rejected')
        has_nb = src_path.suffix.lower() == '.ipynb' or (
            src_path.is_dir() and any(p.suffix.lower() == '.ipynb' for p in src_path.rglob('*.ipynb'))
        )
        if has_nb and dst_parts[0] != 'notebooks':
            reject(f'Notebook files may only be moved into notebooks/: {src_rel} -> {dst_rel}', 'operation_rejected')
        if dst_path.exists() or dst_path.is_symlink():
            reject(f'move_path destination already exists: {dst_rel}', 'conflict')
        if src_path == dst_path or src_path in dst_path.parents:
            reject('move_path source and destination overlap', 'invalid_request')
        with control_lock(root):
            registered = _registered_control_paths(root)
            _validate_tree_mutable(root, src_rel, src_path, allow_notebooks=True, registered=registered)
            ensure_directory(dst_path.parent)
            shutil.move(str(src_path), str(dst_path))
        return dict(status='moved', src=src_rel, dst=dst_rel)
    if action == 'remove_path':
        raw_paths = arguments.get('paths')
        if not isinstance(raw_paths, list) or not raw_paths or len(raw_paths) > 64:
            reject('remove_path requires a non-empty list of 1..64 relative paths', 'invalid_request')
        targets = []
        for item in raw_paths:
            if not isinstance(item, str) or not item:
                reject('remove_path entries must be non-empty relative path strings', 'invalid_request')
            target = safe_path(root, item, missing=False)
            targets.append((target.relative_to(root).as_posix(), target))
        removed = []
        with control_lock(root):
            registered = _registered_control_paths(root)
            for rel, target in targets:
                _validate_tree_mutable(root, rel, target, allow_notebooks=False, registered=registered)
            for rel, target in targets:
                if target.is_dir():
                    shutil.rmtree(target)
                elif target.exists():
                    target.unlink()
                removed.append(rel)
        return dict(status='removed', removed=removed)
    if action == 'sync_env_example':
        from demiflow.forge.project_env import declared_prompt_env_names, ensure_gitignore_env, load_project_env
        extra = arguments.get('variables')
        names = set(declared_prompt_env_names(root))
        if extra is not None:
            if not isinstance(extra, list) or len(extra) > 64:
                reject('sync_env_example variables must be a list of at most 64 env variable names', 'invalid_request')
            for item in extra:
                if not isinstance(item, str) or not re.fullmatch(r'[A-Z_][A-Z0-9_]{0,127}', item.strip()):
                    reject(f'Invalid environment variable name: {item!r}', 'invalid_request')
                names.add(item.strip())
        example_path = root / '.env.example'
        if example_path.is_symlink():
            reject('.env.example must be a regular file, not a symlink', 'operation_rejected')
        if example_path.is_file():
            names.update(load_project_env(root, filename='.env.example', allow_empty=True).keys())
        sorted_names = sorted(names)
        lines = [
            '# Project runtime environment template (copy to .env locally; never commit real secrets)',
            *(f'{name}=' for name in sorted_names),
        ]
        content = '\n'.join(lines) + '\n'
        with control_lock(root):
            atomic_write_text(example_path, content)
            gi_updated = ensure_gitignore_env(root)
        return dict(status='synced', path='.env.example', variables=sorted_names, gitignore_updated=gi_updated)
    if action == 'slim_manifest':
        return forge_project.slim_project_manifest(root)
    reject('Unsupported manage_project_layout action', 'invalid_request')
