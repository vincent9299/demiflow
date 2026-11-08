"""Trusted local and registry-backed run authorities, without WebService types.

Registry mutation and admission commit use the same nonblocking lock. Runtime
locks precede this guard; a parent must never hold it while waiting on an
inspector. Bootstrap data is host-generated, separate from business arguments.
It is not an authentication mechanism against another process of the same OS
user. Single-workspace deployments retain their fixed service configuration.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from demiflow.forge.schema import ForgeError

VERSION = 'demiflow_run_authority_v1'
ACTIONS = {'read', 'run_submit', 'run_control', 'attach'}


@dataclass(frozen=True)
class LocalAuthority:
    root: str

    def require(self, root, action):
        if str(Path(root).absolute()) != self.root or action not in ACTIONS:
            raise PermissionError('Run authority target or action differs')

    @contextmanager
    def guard(self, root, action):
        self.require(root, action)
        yield

    def bootstrap(self):
        return dict(format=VERSION, kind='trusted_local', root=self.root)


@dataclass(frozen=True)
class WebAuthority:
    root: str
    workspace_id: str
    registry: str | None
    action: str
    ceiling: bool
    single_grant: bool

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
            raise PermissionError('Required run permission is disabled: ' + action)

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

    def bootstrap(self):
        return dict(format=VERSION, kind='web', root=self.root, workspace_id=self.workspace_id,
                    registry=self.registry, action=self.action, ceiling=self.ceiling, single_grant=self.single_grant)


def from_bootstrap(value, root):
    """Closed internal protocol. No callable, import path or client grant flag."""
    if type(value) is not dict or value.get('format') != VERSION or value.get('root') != str(Path(root).absolute()):
        raise ForgeError('Missing or invalid trusted run bootstrap', code='operation_rejected')
    if value.get('kind') == 'trusted_local' and set(value) == {'format', 'kind', 'root'}:
        return LocalAuthority(value['root'])
    keys = {'format', 'kind', 'root', 'workspace_id', 'registry', 'action', 'ceiling', 'single_grant'}
    if (set(value) != keys or value['kind'] != 'web' or value['action'] not in ACTIONS
            or type(value['ceiling']) is not bool or type(value['single_grant']) is not bool
            or type(value['workspace_id']) is not str
            or value['registry'] is not None and (type(value['registry']) is not str or not Path(value['registry']).is_absolute())):
        raise ForgeError('Invalid trusted run bootstrap', code='operation_rejected')
    from demiflow.workbench.projects.registry import identity
    identity(value['workspace_id'])
    return WebAuthority(**{key: value[key] for key in keys - {'format', 'kind'}})


def for_http(server, path, root, action):
    key = path.split('/')[3] if path.startswith('/api/workspaces/') else next(iter(server.single['projects']))
    ceiling = action == 'read' or server.permission_ceiling()[action]
    grant = action == 'read' or server.effective_permissions(server.inventory()['projects'][key])[action]
    return WebAuthority(str(root), key, str(server.registry) if server.registry is not None else None,
                        action, ceiling, grant)
