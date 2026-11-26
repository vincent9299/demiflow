"""Explicit console access inventory. Workspace controls remain authoritative."""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import re

from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.forge.project import initialize_project

FORMAT = 'demiflow_forge_console_v1'
FORMAT_V2 = 'demiflow_forge_console_v2'
FORMAT_V3 = 'demiflow_forge_console_v3'
PERMISSIONS = {'run_submit', 'run_control', 'attach'}
CAPABILITIES = {'config_write', 'analysis_execute', 'analysis_write', 'case_annotate'}


def identity(value):
    if not isinstance(value, str) or not re.fullmatch(r'[wl][0-9a-f]{32}', value):
        raise ForgeError('Invalid registered workspace ID', code='invalid_request')
    return value


def validate_python(value):
    """Validate optional per-project read-only Python interpreter path."""
    if value is None or value == '':
        return value
    if type(value) is not str or len(value.encode('utf-8')) > 2048 or any(ord(c) < 32 for c in value):
        raise ForgeError('Python 路径格式不正确。', code='invalid_request')
    raw = value.strip()
    if len(raw) > 1:
        raw = raw.rstrip('/')
    p = Path(raw)
    if not p.is_absolute() or str(p) != raw or '..' in p.parts:
        raise ForgeError('Python 路径必须是规范的绝对路径。', code='invalid_request')
    if p.is_dir():
        for rel in ('bin/python', 'bin/python3', 'python', 'python3'):
            candidate = p / rel
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate)
        raise ForgeError('该目录下未找到可执行的 bin/python 或 bin/python3：' + raw, code='invalid_request')
    if not p.is_file() or not os.access(p, os.X_OK):
        raise ForgeError('指定的 Python 解释器不存在或不可执行：' + raw, code='invalid_request')
    return raw


PROTECTED_SANDBOX_ROOTS = {'.git', '.forge', 'flows', 'operators', 'prompts', 'data_tools', 'tests', 'src', 'lib'}
DEFAULT_NOTEBOOK_WRITE_PATHS = ['data', 'notebooks']


def validate_notebook_sandbox(value):
    """Validate optional per-project Notebook Kernel sandbox policy."""
    if value is None:
        return None
    if type(value) is not dict or not set(value) <= {'mode', 'write_paths'}:
        raise ForgeError('Notebook 沙箱配置格式不正确。', code='invalid_request')
    mode = value.get('mode', 'readonly')
    if mode not in {'readonly', 'off'}:
        raise ForgeError('Notebook 沙箱模式仅支持 readonly 或 off。', code='invalid_request')
    raw_writes = value.get('write_paths', [])
    if type(raw_writes) not in (list, tuple) or len(raw_writes) > 16:
        raise ForgeError('允许写入目录最多 16 项。', code='invalid_request')
    cleaned = []
    for item in raw_writes:
        if type(item) is not str or not item.strip() or len(item.encode('utf-8')) > 512:
            raise ForgeError('允许写入目录必须是非空相对路径。', code='invalid_request')
        rel = item.strip().strip('/')
        parts = Path(rel).parts
        if Path(item).is_absolute() or not parts or '..' in parts or '.' in parts:
            raise ForgeError('允许写入目录必须是项目内的相对子目录路径：' + item, code='invalid_request')
        if parts[0] in PROTECTED_SANDBOX_ROOTS or rel.endswith('.ipynb'):
            raise ForgeError('不允许向受保护源码目录开放内核写入：' + item, code='invalid_request')
        norm = '/'.join(parts)
        if norm not in cleaned:
            cleaned.append(norm)
    return {'mode': mode, 'write_paths': cleaned}


def notebook_sandbox(item):
    raw = item.get('notebook_sandbox') if isinstance(item, dict) else None
    if isinstance(raw, dict):
        return {'mode': raw.get('mode', 'off'), 'write_paths': list(raw.get('write_paths', []))}
    return {'mode': 'off', 'write_paths': []}


def dev_network(item):
    return bool(item.get('dev_network', False)) if isinstance(item, dict) else False


def load_registry(path):
    path = ws.no_symlinks(Path(path).absolute())
    value = ws.read_control(path, 256*1024)
    if type(value) is not dict or set(value) != {'format', 'projects'} or value['format'] not in {FORMAT, FORMAT_V2, FORMAT_V3}:
        raise ForgeError('Unsupported console registry')
    if type(value['projects']) is not dict or len(value['projects']) > 128:
        raise ForgeError('Console project budget exceeded')
    roots = set()
    for key, item in value['projects'].items():
        identity(key)
        fields = {'root','name','allow_runs'} if value['format']==FORMAT else {'root','name','permissions'}
        if type(item) is not dict or set(item) - {'python', 'notebook_sandbox', 'dev_network', 'dependency_lock'} != fields:
            raise ForgeError('Invalid console project')
        if 'python' in item:
            py = item['python']
            if py != '' and (type(py) is not str or not Path(py).is_absolute() or str(Path(py)) != py or '..' in Path(py).parts):
                raise ForgeError('Invalid project python configuration')
        if 'dependency_lock' in item and (
                type(item['dependency_lock']) is not str or not re.fullmatch(r'[0-9a-f]{64}', item['dependency_lock'])):
            raise ForgeError('Invalid approved dependency lock digest')
        if 'notebook_sandbox' in item:
            validate_notebook_sandbox(item['notebook_sandbox'])
        if 'dev_network' in item and type(item['dev_network']) is not bool:
            raise ForgeError('Invalid project dev_network configuration')
        if value['format']==FORMAT:
            if type(item['allow_runs']) is not bool:
                raise ForgeError('Invalid run permission')
        elif (type(item['permissions']) is not dict
              or set(item['permissions']) != (PERMISSIONS | CAPABILITIES if value['format'] == FORMAT_V3 else PERMISSIONS)
              or any(type(v) is not bool for v in item['permissions'].values())):
            raise ForgeError('Invalid explicit run permissions')
        ws.project_name(item['name'])
        if type(item['root']) is not str or not Path(item['root']).is_absolute() or str(Path(item['root'])) != item['root'] or '..' in Path(item['root']).parts:
            raise ForgeError('Project root must be a normalized absolute path')
        if item['root'] in roots:
            raise ForgeError('Duplicate console project root')
        roots.add(item['root'])
    return value


@contextmanager
def _locked(path):
    path = ws.no_symlinks(Path(path).absolute())
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = ws.no_symlinks(path.with_name(path.name + '.lock'))
    fd = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield path
    except BlockingIOError as exc:
        raise ForgeError('Console registry is being updated; retry') from exc
    finally:
        os.close(fd)


def create(path, root, *, name):
    """Initialize an empty project, then publish its console reference.

    If publication fails, the initialized project stays recoverable by add;
    registration never deletes project files as rollback.
    """
    if '..' in Path(root).parts:
        raise ForgeError('Project directory must be normalized', code='invalid_request')
    initialize_project(root, name=name)
    return add(path, root)


def add(path, root, *, allow_runs=False, name=None):
    if '..' in Path(root).parts:
        raise ForgeError('Project directory must be normalized', code='invalid_request')
    root = ws.no_symlinks(Path(root).absolute())
    if not root.is_dir():
        raise ForgeError('Existing project directory is required', code='invalid_request')
    if type(allow_runs) is not bool:
        raise ForgeError('Invalid run permission')
    if not (root / 'workspace.json').exists():
        initialize_project(root, name=name or root.name, existing=True)
    root, workspace, _ = ws.load_workspace(root)
    if name is not None and name != workspace['name']:
        raise ForgeError('Existing project has a different name', code='conflict')
    key = ws.workspace_id(root, workspace)
    item = dict(root=str(root), name=workspace['name'], allow_runs=allow_runs)
    with _locked(path) as path:
        exists = path.exists()
        value = load_registry(path) if exists else dict(format=FORMAT, projects={})
        expected = content_digest(value) if exists else None
        if value['format'] in {FORMAT_V2, FORMAT_V3}:
            item.pop('allow_runs')
            item['permissions']={key:allow_runs for key in PERMISSIONS}
            if value['format'] == FORMAT_V3:
                item['permissions'].update({key:False for key in CAPABILITIES})
        if key in value['projects']:
            if value['projects'][key] != item:
                raise ForgeError('Workspace is already registered with a different root or permission')
            return dict(status='unchanged', workspace_id=key)
        if len(value['projects']) >= 128 or any(p['root'] == str(root) for p in value['projects'].values()):
            raise ForgeError('Duplicate root or project budget exhausted')
        value['projects'][key] = item
        ws.write_control(path, value, expected=expected, create=not exists)
    return dict(status='registered', workspace_id=key, **item)


def remove(path, workspace_id, *, expected):
    """Revoke this registry's membership; preserve all project files and work."""
    identity(workspace_id)
    with _locked(path) as path:
        value = load_registry(path)
        if content_digest(value) != expected:
            raise ForgeError('Console registry changed; refresh before removing', code='conflict')
        if workspace_id not in value['projects']:
            raise ForgeError('Workspace is not registered', code='not_found')
        del value['projects'][workspace_id]
        ws.write_control(path, value, expected=expected)
    return dict(status='removed', workspace_id=workspace_id)


def resolve_workspace(value, workspace_id):
    identity(workspace_id)
    item = value['projects'].get(workspace_id)
    if item is None:
        raise ForgeError('Workspace is not registered', code='not_found')
    root = ws.no_symlinks(Path(item['root']))
    _, workspace = ws.load_workspace_control(root)
    if ws.workspace_id(root, workspace) != workspace_id:
        raise ForgeError('Registered workspace identity changed')
    return root, permissions(item)['run_submit']


def projects(value):
    result = []
    for key, item in value['projects'].items():
        row = dict(workspace_id=key, **item, **({'allow_runs':permissions(item)['run_submit']} if 'allow_runs' not in item else {}))
        row['permissions']=permissions(item)
        row['capabilities']=capabilities(item)
        row['python'] = item.get('python')
        row['notebook_sandbox'] = notebook_sandbox(item)
        row['dev_network'] = dev_network(item)
        try:
            resolve_workspace(value, key)
            row['status'] = 'available'
        except (ValueError, OSError) as exc:
            row.update(status='unavailable', error=str(exc))
        result.append(row)
    return dict(projects=result, digest=content_digest(value))


def permissions(item):
    return {k:item['permissions'][k] for k in PERMISSIONS} if 'permissions' in item else {k:item['allow_runs'] for k in PERMISSIONS}


def capabilities(item):
    """Projection of the same registry grants; legacy reads never grant or write."""
    return {key:item.get('permissions', {}).get(key, False) for key in CAPABILITIES}


def migrate(path, *, expected, format_name=FORMAT_V3):
    if format_name != FORMAT_V3:
        raise ForgeError('Only explicit console v3 migration is supported')
    with _locked(path) as path:
        value=load_registry(path)
        if content_digest(value)!=expected:
            raise ForgeError('Registry changed before migration',code='conflict')
        if value['format']==FORMAT_V3:
            return dict(status='unchanged',digest=expected)
        backup=path.with_name(path.name+'.before-v3-'+expected[:16]+'.json')
        if backup.exists():
            if load_registry(backup)!=value:
                raise ForgeError('Migration backup differs',code='conflict')
        else:
            ws.write_control(backup,value,create=True)
        for item in value['projects'].values():
            item['permissions']={**permissions(item),**capabilities(item)}
            item.pop('allow_runs',None)
        value['format']=FORMAT_V3
        ws.write_control(path,value,expected=expected)
    return dict(status='migrated',digest=content_digest(value),backup=str(backup))


def set_capabilities(path, workspace_id, *, expected, capabilities, analysis_service=None):
    identity(workspace_id)
    if (type(capabilities) is not dict or set(capabilities)!=CAPABILITIES
            or any(type(v) is not bool for v in capabilities.values())):
        raise ForgeError('Capabilities require four explicit booleans')
    observed=load_registry(path)
    if content_digest(observed)!=expected:
        raise ForgeError('Registry changed before capability update',code='conflict')
    if workspace_id not in observed['projects']:
        raise ForgeError('Workspace is not registered',code='not_found')
    configured=observed['projects'][workspace_id].get('permissions',{})
    # Preserving/revoking configured grants remains possible while a service
    # is offline; an unavailable deployment never acquires a new grant.
    added={key for key in CAPABILITIES-{'config_write'} if capabilities[key] and not configured.get(key,False)}
    if added:
        from demiflow.workbench.analysis.web_service import WebService
        from demiflow.workbench.notebooks.service import Notebooks
        if not isinstance(analysis_service,(WebService,Notebooks)):
            raise ForgeError('Analysis deployment is not connected',code='operation_rejected')
        ceiling=analysis_service.ceiling()
        if any(not ceiling[key] for key in added):
            raise ForgeError('Analysis capability exceeds the reviewed service ceiling',code='operation_rejected')
        analysis_service.assert_ready()
    with _locked(path) as path:
        value=load_registry(path)
        if content_digest(value)!=expected:
            raise ForgeError('Registry changed before capability update',code='conflict')
        if value['format']!=FORMAT_V3:
            raise ForgeError('Explicit console v3 migration is required',code='operation_rejected')
        if workspace_id not in value['projects']:
            raise ForgeError('Workspace is not registered',code='not_found')
        value['projects'][workspace_id]['permissions'].update(capabilities)
        ws.write_control(path,value,expected=expected)
    return dict(status='updated',workspace_id=workspace_id,capabilities=capabilities,digest=content_digest(value))


def set_permissions(path, workspace_id, *, expected, run_submit, run_control, attach):
    """Explicit version upgrade; listing/resolution never rewrite a registry."""
    identity(workspace_id)
    requested=dict(run_submit=run_submit,run_control=run_control,attach=attach)
    if any(type(v) is not bool for v in requested.values()):
        raise ForgeError('Permissions must be booleans')
    with _locked(path) as path:
        value=load_registry(path)
        if content_digest(value)!=expected:
            raise ForgeError('Registry changed before permission update',code='conflict')
        if workspace_id not in value['projects']:
            raise ForgeError('Workspace is not registered',code='not_found')
        for item in value['projects'].values():
            item['permissions']={**permissions(item),**(capabilities(item) if value['format']==FORMAT_V3 else {})}
            item.pop('allow_runs',None)
        if value['format']!=FORMAT_V3:
            value['format']=FORMAT_V2
        value['projects'][workspace_id]['permissions'].update(requested)
        ws.write_control(path,value,expected=expected)
    return dict(status='updated',workspace_id=workspace_id,permissions=requested,digest=content_digest(value))


def set_python(path, workspace_id, *, expected, python):
    """Configure optional per-project read-only Python interpreter for dev sandbox testing."""
    identity(workspace_id)
    cleaned = validate_python(python)
    with _locked(path) as path:
        value = load_registry(path)
        if content_digest(value) != expected:
            raise ForgeError('Registry changed before python environment update', code='conflict')
        if workspace_id not in value['projects']:
            raise ForgeError('Workspace is not registered', code='not_found')
        if cleaned != value['projects'][workspace_id].get('python'):
            value['projects'][workspace_id].pop('dependency_lock', None)
        if cleaned is None:
            value['projects'][workspace_id].pop('python', None)
        else:
            value['projects'][workspace_id]['python'] = cleaned
        ws.write_control(path, value, expected=expected)
    return dict(status='updated', workspace_id=workspace_id, python=cleaned, digest=content_digest(value))


def approve_dependencies(path, workspace_id, *, expected, digest):
    """Approve the supplied candidate digest for the registered project Python."""
    from demiflow.forge.dependencies import binding
    identity(workspace_id)
    value = load_registry(path)
    item = value['projects'].get(workspace_id)
    if not item or not item.get('python'):
        raise ForgeError('An explicit registered project Python is required')
    locked = binding(item['root'], approval=digest, python=item['python'])
    if locked is None:
        raise ForgeError('The project has no dependency lock to approve')
    with _locked(path) as path:
        value = load_registry(path)
        if content_digest(value) != expected:
            raise ForgeError('Registry changed before dependency approval', code='conflict')
        current = value['projects'][workspace_id]
        if current['root'] != item['root'] or current.get('python') != item['python']:
            raise ForgeError('Project dependency authority changed', code='conflict')
        current['dependency_lock'] = locked['digest']
        ws.write_control(path, value, expected=expected)
    return dict(status='approved', workspace_id=workspace_id, dependency_lock=digest, digest=content_digest(value))


def set_notebook_sandbox(path, workspace_id, *, expected, mode, write_paths=()):
    """Configure optional per-project Notebook Kernel Landlock sandbox policy."""
    identity(workspace_id)
    cleaned = validate_notebook_sandbox({'mode': mode, 'write_paths': list(write_paths or ())})
    with _locked(path) as path:
        value = load_registry(path)
        if content_digest(value) != expected:
            raise ForgeError('Registry changed before notebook sandbox update', code='conflict')
        if workspace_id not in value['projects']:
            raise ForgeError('Workspace is not registered', code='not_found')
        if cleaned == {'mode': 'readonly', 'write_paths': []}:
            value['projects'][workspace_id].pop('notebook_sandbox', None)
        else:
            value['projects'][workspace_id]['notebook_sandbox'] = cleaned
        ws.write_control(path, value, expected=expected)
    return dict(status='updated', workspace_id=workspace_id, notebook_sandbox=cleaned, digest=content_digest(value))


def set_dev_network(path, workspace_id, *, expected, enabled):
    """Configure optional per-project outbound network access for the dev assistant sandbox."""
    identity(workspace_id)
    if type(enabled) is not bool:
        raise ForgeError('dev_network enabled must be boolean', code='invalid_request')
    with _locked(path) as path:
        value = load_registry(path)
        if content_digest(value) != expected:
            raise ForgeError('Registry changed before dev_network update', code='conflict')
        if workspace_id not in value['projects']:
            raise ForgeError('Workspace is not registered', code='not_found')
        if enabled:
            value['projects'][workspace_id]['dev_network'] = True
        else:
            value['projects'][workspace_id].pop('dev_network', None)
        ws.write_control(path, value, expected=expected)
    return dict(status='updated', workspace_id=workspace_id, dev_network=enabled, digest=content_digest(value))


def register(forge):
    import click
    from demiflow.forge.platform_cli import output

    @forge.group('console')
    @click.option('--registry', required=True, type=click.Path(path_type=Path, dir_okay=False))
    @click.pass_context
    def console(ctx, registry):
        """Serve explicitly registered workspaces through one loopback console."""
        ctx.obj = registry

    @console.command('create')
    @click.argument('root', type=click.Path(path_type=Path, file_okay=False))
    @click.option('--name', required=True)
    @click.pass_obj
    def create_project(registry, root, name):
        output(lambda: create(registry, root, name=name), True)

    @console.command('add')
    @click.argument('root', type=click.Path(path_type=Path, file_okay=False))
    @click.option('--allow-runs', is_flag=True)
    @click.option('--name', default=None)
    @click.pass_obj
    def add_project(registry, root, allow_runs, name):
        output(lambda: add(registry, root, allow_runs=allow_runs, name=name), True)

    @console.command('list')
    @click.pass_obj
    def list_projects(registry):
        output(lambda: projects(load_registry(registry)), True)

    @console.command('remove')
    @click.argument('workspace_id')
    @click.option('--expected', required=True)
    @click.pass_obj
    def remove_project(registry, workspace_id, expected):
        output(lambda: remove(registry, workspace_id, expected=expected), True)

    @console.command('permissions')
    @click.argument('workspace_id')
    @click.option('--expected', required=True)
    @click.option('--submit/--no-submit', default=False)
    @click.option('--control/--no-control', default=False)
    @click.option('--attach/--no-attach', default=False)
    @click.pass_obj
    def update_permissions(registry, workspace_id, expected, submit, control, attach):
        output(lambda:set_permissions(registry,workspace_id,expected=expected,run_submit=submit,run_control=control,attach=attach),True)

    @console.command('approve-dependencies')
    @click.argument('workspace_id')
    @click.option('--expected', required=True)
    @click.option('--digest', required=True)
    @click.pass_obj
    def approve_project_dependencies(registry, workspace_id, expected, digest):
        output(lambda: approve_dependencies(registry, workspace_id, expected=expected, digest=digest), True)

    @console.command('serve')
    @click.option('--port', type=click.IntRange(0, 65535), default=0)
    @click.option('--enable-runs', is_flag=True)
    @click.option('--enable-config-write', is_flag=True, help='Deployment ceiling; still requires an explicit project grant.')
    @click.option('--control-only', is_flag=True, help='Permit stopping/reconciliation; no new submissions or attach.')
    @click.option('--api-only', is_flag=True, help='Serve HTTP capabilities without Web assets.')
    @click.option('--token-file', type=click.Path(path_type=Path, dir_okay=False))
    @click.pass_obj
    @click.option("--development-config", type=click.Path(exists=True, dir_okay=False, path_type=Path), help="Explicit project development deployment; disabled by default")
    @click.option("--analysis-config", type=click.Path(exists=True, dir_okay=False, path_type=Path), help="Explicit reviewed production analysis adapter; disabled by default")
    @click.option('--notebook-python', type=click.Path(exists=True, dir_okay=False, path_type=Path), help='Python environment with native Jupyter Notebook 7 installed.')
    def serve_projects(registry, port, enable_runs, enable_config_write, control_only, api_only, token_file, development_config, analysis_config, notebook_python):
        from demiflow.workbench.api import serve
        serve(None, registry=registry, port=port, enable_runs=enable_runs, enable_config_write=enable_config_write, control_only=control_only, api_only=api_only, token_file=token_file, development_config=development_config, analysis_config=analysis_config, notebook_python=notebook_python)

    @console.command('migrate')
    @click.option('--expected', required=True)
    @click.option('--format', 'format_name', required=True, type=click.Choice([FORMAT_V3]))
    @click.pass_obj
    def migrate_registry(registry, expected, format_name):
        output(lambda:migrate(registry,expected=expected,format_name=format_name),True)

    @console.command('capabilities')
    @click.argument('workspace_id')
    @click.option('--expected', required=True)
    @click.option('--config-write/--no-config-write', default=False)
    @click.pass_obj
    def update_capabilities(registry, workspace_id, expected, config_write):
        output(lambda:set_capabilities(registry,workspace_id,expected=expected,
            capabilities={key:config_write if key=='config_write' else False for key in CAPABILITIES}),True)

    @console.command('watch')
    @click.option('--interval', type=click.IntRange(1, 300), default=30)
    @click.option('--full-interval', type=click.IntRange(1, 3600), default=300)
    @click.pass_obj
    def watch_projects(registry, interval, full_interval):
        from demiflow.workbench.projects.watch import watch
        watch(registry, interval=interval, full_interval=full_interval)
