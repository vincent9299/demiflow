"""Prepare current coding guidance and serve turn-bound read-only input."""
from __future__ import annotations

import hashlib
import hmac
import os
from pathlib import Path
import secrets
import socket
import sys
import tempfile
import threading
import time

from demiflow.workbench.conversations.common import digest
from demiflow.workbench.conversations.common import read_file
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import safe_path
from demiflow.workbench.conversations.common import wire


def authoritative_source_paths(project):
    root = Path(project['root'])
    paths = []
    parents = reversed((root, *root.parents)) if project['kind'] == 'platform' else (root,)
    for parent in parents:
        candidate = parent / 'AGENTS.md'
        if candidate.is_file(): paths.append(candidate)
    for relative in project['writes']:
        candidate = safe_path(root, relative, missing=True)
        if not candidate.is_dir(): candidate = candidate.parent
        while candidate != root and root in candidate.parents:
            agents = candidate / 'AGENTS.md'
            if agents.is_file() and agents not in paths: paths.append(agents)
            candidate = candidate.parent
    paths.extend(safe_path(root, p) for p in project['sources'])
    return list(dict.fromkeys(paths))


def assemble(project, requirement, limits, *, execution_mode='trusted_local', input_access=False,
             coding=None, delivery_review=False, previous_review=None):
    root = Path(project['root'])
    sources, pages, used = [], [], 0

    def append(source_id, path, raw, *, binding=None, sha=None, origin="authoritative_file"):
        nonlocal used
        used += len(raw.encode())
        if used > limits.context_bytes or len(pages) >= 32:
            reject('Required context exceeds initial byte/page budget; split the task explicitly', 'resource_limit')
        sources.append(dict(kind=project['kind'], source_id=source_id, canonical_path=str(path),
            sha256=sha or hashlib.sha256(raw.encode()).hexdigest(), version=binding, required=True,
            bytes=len(raw.encode()), delivery='prepared', origin=origin))
        pages.append(raw)

    for path in authoritative_source_paths(project):
        raw = read_file(path).decode('utf-8')
        append(str(path), path, '# Source: ' + str(path) + '\n' + raw,
               sha=hashlib.sha256(raw.encode()).hexdigest())
    guidance = None
    if coding is not None:
        from importlib.resources import files as resource_files
        from demiflow.forge.context import build_context

        facts = build_context(root, reference=coding, exclude=project.get('protected', ()))
        if facts['status'] != 'ready':
            reject('Forge coding context incomplete: ' + '; '.join(
                item['detail'] for item in facts['diagnostics']), 'resource_limit')
        sdk = facts['sdk']
        guidance = dict(sdk_revision=sdk['sdk_revision'], catalog_id=sdk['catalog_id'],
                        rules_id=facts['rules_id'], symbols=[item['id'] for item in sdk['apis']],
                        sources=[{key: item[key] for key in ('origin', 'path', 'sha256')}
                                 for item in facts['sources']])
        skill_text = resource_files('demiflow.forge').joinpath('skills', 'demiforge', 'SKILL.md').read_text(encoding='utf-8')
        append('forge-skill', root, skill_text, binding=digest(facts),
               origin='forge_skill')
        if previous_review:
            append('forge-pipeline-review', root,
                   '本会话上次独立评审，当时的源码及报告仅供本轮复核：\n'
                   + wire(previous_review).decode(), origin='forge_review_result')
    grant = {key: project[key] for key in ('kind', 'phase', 'writes', 'watch', 'non_goals', 'authorization')}
    manifest = dict(kind=project['kind'], grant=grant, requirement=requirement, sources=sources, bytes=used)
    if guidance is not None:
        manifest['coding_guidance'] = guidance
    if input_access: manifest['input_access'] = 'conversation_readonly'
    # Strict sandbox enforces boundaries in the OS; domain guidance lives solely in
    # the attached Forge Skill (and any explicitly declared project sources).
    prompt = '\n\n'.join(pages) if pages else ('实际项目目录：' + str(root) + '\n')
    if len(prompt.encode()) > limits.context_bytes:
        reject('Context with trusted header exceeds initial budget', 'resource_limit')
    manifest['digest'] = digest(manifest)
    return manifest, prompt


INPUT_TOOL = dict(name='read_input', description='Discover and read bounded input data.',
    inputSchema=dict(type='object', additionalProperties=False, properties={
        'action': dict(type='string', enum=['list', 'find', 'read', 'inspect']),
        'path': dict(type='string'), 'pattern': dict(type='string'),
        'depth': dict(type='integer'), 'offset': dict(type='integer'), 'bytes': dict(type='integer'),
        'limit': dict(type='integer'), 'columns': dict(type='array', items=dict(type='string'))}))

_STANDARD_PROJECT_DIRS = frozenset({
    'flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks', 'data', 'logs', 'tests', 'docs',
})
_MOVE_DEST_DIRS = frozenset({
    'flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks', 'tests', 'docs',
})
_PROTECTED_TOP_DIRS = frozenset({'.forge', '.git', '.venv', 'runs', 'data', 'logs'})
_PROTECTED_ROOT_FILES = frozenset({'workspace.json', 'AGENTS.md', 'PIPELINE_SPEC.md', 'README.md'})
_PROTECTED_DATA_SUFFIXES = frozenset({'.lance', '.parquet', '.arrow', '.db', '.sqlite', '.sqlite3'})

PROJECT_LAYOUT_TOOL = dict(
    name='manage_project_layout',
    description=(
        'Safely manage controlled project structure, file organization and workspace.json registrations on the host: '
        'ensure_dirs creates missing standard directories (flows/operators/prompts/configs/data_tools/notebooks/data/logs/tests/docs); '
        'register_flow checks and registers a flow entry in workspace.json (defaulting to configs/<flow>.json); '
        'bind_params updates an existing registered entry parameter binding under configs/*.json; '
        'move_path moves an existing project file or directory into a standard role directory (e.g. root configs to configs/, root *.ipynb to notebooks/, review tools to data_tools/) without losing bytes or overwriting existing files; '
        'remove_path deletes obsolete unreferenced non-data files or directories (e.g. non-standard directories or unused orphan flows/configs/prompts/scripts), strictly blocking notebooks, business data, runs/, and registered entries; '
        'sync_env_example generates/updates root .env.example (empty-value template ONLY, never real secrets) from prompts/*.yaml api_key_env/base_url_env declarations (plus optional extra variable names) and ensures .gitignore ignores .env and .forge/; '
        'slim_manifest normalizes workspace.json to the minimal human-readable project manifest (name/entries/budgets) while storing Forge internal state in .forge/state.json.'
    ),
    inputSchema=dict(type='object', additionalProperties=False, required=['action'], properties={
        'action': dict(type='string', enum=[
            'ensure_dirs', 'register_flow', 'bind_params', 'move_path', 'remove_path', 'sync_env_example', 'slim_manifest',
        ]),
        'dirs': dict(type='array', items=dict(type='string')),
        'entry': dict(type='string'),
        'params': dict(type='string'),
        'old_params': dict(type='string'),
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


def _run_project_layout(project_root, arguments):
    from demiflow.forge import project as forge_project
    from demiflow.forge.io import ensure_directory
    from demiflow.forge.project_io import control_lock, write_control
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
        import shutil
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
        # Protect read-only notebooks/ from move-out -> edit -> move-back bypasses.
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
        import shutil
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
        import re
        from demiflow.forge.io import atomic_write_text
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
        # Preserve any existing declared variable names already in .env.example (values are always cleared).
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


class BridgeServer:
    """Bounded input IPC owned by the trusted worker.

    The MCP subprocess receives only an endpoint and one-turn capability token.
    B1 may relay this Unix endpoint into its isolated network namespace; the same
    token, epoch, active-turn and fixed-operation checks apply on this side.
    """
    def __init__(self, input_access, *, delivery_review=None, project_root=None):
        self.lock = threading.Lock()
        self.input_access = input_access
        self.delivery_review = delivery_review
        self.project_root = Path(project_root).absolute() if project_root is not None else None
        self.bound_identity = (input_access.tid, input_access.epoch) if input_access is not None else None
        self.token = secrets.token_urlsafe(32)
        self.directory = Path(tempfile.mkdtemp(prefix='demiflow-dev-control-'))
        self.path = self.directory / 'bridge.sock'
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(str(self.path)); os.chmod(self.path, 0o600)
        self.socket.listen(4); self.socket.settimeout(.2)
        self.stopping = threading.Event()
        self.count = 0
        self.authorized_layout_paths: set[str] = set()
        self.thread = threading.Thread(target=self._run, name='development-control', daemon=True)
        self.thread.start()

    def take_layout_paths(self) -> list[str]:
        with self.lock:
            paths = sorted(self.authorized_layout_paths)
            self.authorized_layout_paths.clear()
            return paths

    def _record_layout_paths(self, action, answer):
        if not isinstance(answer, dict):
            return
        recorded: set[str] = set()
        if action == 'ensure_dirs':
            for d in answer.get('created') or ():
                if isinstance(d, str) and d:
                    recorded.add(d)
        elif action == 'move_path':
            for key in ('src', 'dst'):
                val = answer.get(key)
                if isinstance(val, str) and val:
                    recorded.add(val)
        elif action == 'remove_path':
            for p in answer.get('removed') or ():
                if isinstance(p, str) and p:
                    recorded.add(p)
        elif action in ('register_flow', 'bind_params'):
            recorded.update(('workspace.json', '.forge/state.json'))
            params = answer.get('params')
            if isinstance(params, str) and params:
                recorded.add(params)
            elif isinstance(params, list):
                recorded.update(p for p in params if isinstance(p, str) and p)
        elif action == 'sync_env_example':
            recorded.add('.env.example')
            if answer.get('gitignore_updated'):
                recorded.add('.gitignore')
        elif action == 'slim_manifest':
            recorded.update(('workspace.json', '.forge/state.json'))
        if recorded:
            with self.lock:
                self.authorized_layout_paths.update(recorded)

    def set_turn(self, input_access, *, delivery_review=None, project_root=None):
        with self.lock:
            self.input_access = input_access
            self.delivery_review = delivery_review
            self.project_root = Path(project_root).absolute() if project_root is not None else None
            self.count = 0
            if input_access is not None:
                self.authorized_layout_paths.clear()
            if input_access is not None and self.bound_identity is None:
                self.bound_identity = (input_access.tid, input_access.epoch)

    def _answer(self, request):
        with self.lock:
            active = self.input_access
            if (active is None or type(request) is not dict or set(request) != {'token', 'turn_id', 'epoch', 'method', 'params'}
                    or type(request['token']) is not str or not hmac.compare_digest(request['token'], self.token)
                    or (request['turn_id'], request['epoch']) not in {(active.tid, active.epoch), self.bound_identity}):
                reject('Development control capability does not match this turn', 'not_found')
            active.require_active()
        return self.dispatch(request['method'], request['params'], active)

    def dispatch(self, method, params, active):
        from .pipeline_review import REVIEW_TOOL
        with self.lock:
            if active is not self.input_access:
                reject('Development control capability does not match this turn', 'not_found')
            if method == 'tools/list' and params == {}:
                tools = [INPUT_TOOL]
                if self.delivery_review is not None:
                    tools.append(REVIEW_TOOL)
                if self.project_root is not None:
                    tools.append(PROJECT_LAYOUT_TOOL)
                return dict(tools=tools)
            if method != 'tools/call' or type(params) is not dict or set(params) != {'name', 'arguments'}:
                reject('Unsupported development control operation')
            name = params['name']
            layout_root = self.project_root
            if name == REVIEW_TOOL['name'] and self.delivery_review is not None:
                # End-of-turn detachment serializes with this short durable admission.
                answer = self.delivery_review.request(params['arguments'])
            elif name == PROJECT_LAYOUT_TOOL['name'] and layout_root is not None:
                answer = None
            elif name != INPUT_TOOL['name']:
                reject('Unsupported development control operation')
        if name == INPUT_TOOL['name']:
            answer = active.invoke(params['arguments'])
        elif name == PROJECT_LAYOUT_TOOL['name']:
            answer = _run_project_layout(layout_root, params['arguments'])
            self._record_layout_paths(params['arguments'].get('action') if isinstance(params.get('arguments'), dict) else None, answer)
        return dict(content=[dict(type='text', text=wire(answer, 60000).decode())], isError=False)

    def _run(self):
        from demiflow.workbench.conversations.transport import decode
        while not self.stopping.is_set():
            try: connection, _ = self.socket.accept()
            except socket.timeout: continue
            except OSError: return
            with connection:
                connection.settimeout(2)
                try:
                    with self.lock:
                        if self.count >= 256:
                            reject('Development control request budget exceeded', 'resource_limit')
                        self.count += 1
                    with connection.makefile('rb') as stream:
                        raw = stream.readline(65537)
                    if len(raw) > 65536 or not raw.endswith(b'\n'): reject('Development control frame exceeds budget')
                    result = dict(result=self._answer(decode(raw)))
                except Exception as exc:
                    result = dict(error=dict(code=-32602, message=str(exc)[:2048]))
                try: connection.sendall(wire(result, 65536) + b'\n')
                except OSError: pass

    def close(self):
        self.stopping.set()
        self.socket.close()
        self.thread.join(timeout=3)
        try: self.path.unlink()
        except FileNotFoundError: pass
        try: self.directory.rmdir()
        except OSError: pass
