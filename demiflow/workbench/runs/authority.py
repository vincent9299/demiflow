"""Workbench registry-backed WebAuthority built on Forge's run authority protocol."""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from demiflow.forge.runs.authority import (
    ACTIONS,
    LocalAuthority,
    VERSION,
    from_bootstrap as _forge_from_bootstrap,
    register_bootstrap_kind,
)
from demiflow.forge.schema import ForgeError


@dataclass(frozen=True)
class WorkbenchLocalAuthority(LocalAuthority):
    """Trusted-local Workbench CLI authority that uses Workbench's worker for graph source materials."""

    def inspector_operation(self, name, root, *, _pass_fds=(), _authority=None, _source_modules=None, _dependencies=None, **arguments):
        from demiflow.workbench.worker import operation
        return operation(
            name, root, _pass_fds=_pass_fds, _authority=_authority,
            _source_modules=_source_modules, _dependencies=_dependencies, **arguments,
        )


def local_authority(root: Path, python: Path | None = None, read_roots: tuple[Path, ...] = ()):
    py = str(python) if python is not None else None
    rr = tuple(str(Path(p).resolve()) for p in read_roots) if read_roots else None
    return WorkbenchLocalAuthority(str(Path(root).absolute()), python=py, read_roots=rr)


@dataclass(frozen=True)
class WebAuthority:
    root: str
    workspace_id: str
    registry: str | None
    action: str
    ceiling: bool
    single_grant: bool
    read_roots: tuple[str, ...] | None = None
    denied_paths: tuple[str, ...] = ()

    def require(self, root, action):
        from demiflow.workbench.projects import registry as console
        from demiflow.workbench.projects import workspace
        if str(Path(root).absolute()) != self.root or action not in {self.action, 'read'}:
            raise PermissionError('Run authority target or action differs')
        if self.registry is not None:
            inventory = console.load_registry(self.registry)
            item = inventory['projects'].get(self.workspace_id)
            if item is None or item['root'] != self.root:
                raise PermissionError('Project membership changed')
            allowed = action == 'read' or console.permissions(item).get(action) is True
        else:
            allowed = action == 'read' or self.single_grant
        _, project, _ = workspace.load_workspace(root)
        if workspace.workspace_id(root, project) != self.workspace_id:
            raise PermissionError('Project identity changed')
        if action != 'read' and not (allowed and self.ceiling):
            label = {'run_submit': '管线运行', 'run_control': '管线停止与控制', 'attach': '保留结果为输入'}.get(action, action)
            raise PermissionError(f'当前项目未开启「{label}」权限，请在右上角「项目设置」中开启。')

    @contextmanager
    def guard(self, root, action):
        if self.registry is None:
            self.require(root, action)
            yield
        else:
            from demiflow.workbench.projects.registry import _locked
            with _locked(self.registry):
                self.require(root, action)
                yield

    def resolve_dependencies(self, root, *, python=None, verify_files=True, require_environment=False):
        from demiflow.workbench.projects.registry import project_dependencies
        return project_dependencies(
            root, authority=self.bootstrap(), python=python,
            verify_files=verify_files, require_environment=require_environment,
        )

    def inspector_operation(self, name, root, *, _pass_fds=(), _authority=None, _source_modules=None, _dependencies=None, **arguments):
        from demiflow.workbench.worker import operation
        return operation(
            name, root, _pass_fds=_pass_fds, _authority=_authority,
            _source_modules=_source_modules, _dependencies=_dependencies, **arguments,
        )

    def bootstrap(self):
        value = dict(format=VERSION, kind='web', root=self.root, workspace_id=self.workspace_id,
                     registry=self.registry, action=self.action, ceiling=self.ceiling, single_grant=self.single_grant)
        if self.read_roots is not None:
            value['read_roots'] = list(self.read_roots)
        if self.denied_paths:
            value['denied_paths'] = list(self.denied_paths)
        return value


def _from_web_bootstrap(value, root):
    keys = {'format', 'kind', 'root', 'workspace_id', 'registry', 'action', 'ceiling', 'single_grant'}
    paths = {}
    for field in ('read_roots', 'denied_paths'):
        if field not in value:
            continue
        entries = value[field]
        if (type(entries) is not list or len(entries) > 1024 or any(type(p) is not str
                or not Path(p).is_absolute() or '..' in Path(p).parts for p in entries)):
            raise ForgeError('Invalid trusted project source scope', code='operation_rejected')
        paths[field] = tuple(entries)
    if (set(value) - paths.keys() != keys or value['kind'] != 'web' or value['action'] not in ACTIONS
            or type(value['ceiling']) is not bool or type(value['single_grant']) is not bool
            or type(value['workspace_id']) is not str
            or value['registry'] is not None and (type(value['registry']) is not str or not Path(value['registry']).is_absolute())):
        raise ForgeError('Invalid trusted run bootstrap', code='operation_rejected')
    from demiflow.workbench.projects.registry import identity
    identity(value['workspace_id'])
    return WebAuthority(**{key: value[key] for key in keys - {'format', 'kind'}}, **paths)


register_bootstrap_kind('web', _from_web_bootstrap)


def from_bootstrap(value, root):
    auth = _forge_from_bootstrap(value, root)
    if type(auth) is LocalAuthority:
        return WorkbenchLocalAuthority(auth.root, python=auth.python, read_roots=auth.read_roots)
    return auth


def for_http(server, path, root, action):
    key = path.split('/')[3] if path.startswith('/api/workspaces/') else next(iter(server.single['projects']))
    ceiling = action == 'read' or server.permission_ceiling()[action]
    grant = action == 'read' or server.effective_permissions(server.inventory()['projects'][key])[action]
    allowed, denied = server.source_scope(key, root)
    return WebAuthority(str(root), key, str(server.registry) if server.registry is not None else None,
                        action, ceiling, grant, allowed, tuple(map(str, denied)))
