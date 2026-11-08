"""Explicit same-account development binding for the common analysis service.

This is a distinct admitted binding, never a fabricated OCI Deployment. The
ordinary-process backend alone validates its fixed interpreter/dependencies;
the common service owns these auth/IO paths and registered project cwd binding.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import resource
import stat
import sys

from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.deployment import Deployment
from demiflow.workbench.analysis.deployment import _read
from demiflow.workbench.analysis import execution_facts

FORMAT = 'demiflow_analysis_trusted_binding_v1'
WEB_FORMAT = 'demiflow_analysis_trusted_web_config_v1'


def _path(value, label, *, socket=False):
    c.text(value, label, 2048)
    result = Path(value)
    if not result.is_absolute() or '..' in result.parts or str(result) != value:
        c.reject(label+' requires a fixed normalized absolute path')
    if socket and len(os.fsencode(result)) > 107:
        c.reject('Fixed analysis socket exceeds the Unix path bound')
    # Socket files may not yet exist. Validate the actual parent hierarchy.
    no_symlinks(result.parent if socket else result)
    return result


def _directory(path, *, mode=None):
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        c.reject('Trusted service directory must belong to its current account')
    if mode is not None and stat.S_IMODE(info.st_mode) != mode:
        c.reject('Trusted private/shared directory mode differs')


@dataclass(frozen=True)
class LocalPaths:
    state: Path
    helper_uid: int
    helper_gid: int


@dataclass(frozen=True)
class TrustedBinding:
    local_site: object
    deployment_id: str
    principal: str
    helper_socket: Path
    auth_socket: Path
    transfer_root: Path
    service_capabilities: dict

    execution_mode = 'trusted_local'
    # This is the service admission state, distinct from execution_mode.
    mode = 'production'

    @property
    def paths(self):
        return LocalPaths(self.local_site.state, os.geteuid(), os.getegid())

    @property
    def value(self):
        # These common IO/authority names are also consumed by the original
        # transfer code. No OCI release/cgroup/profile facts are synthesized.
        return dict(site=dict(allowed_web_uids=[os.geteuid()], helper_uid=os.geteuid(),
            helper_gid=os.getegid(), helper_socket_path=str(self.helper_socket),
            web_auth_socket_path=str(self.auth_socket), host_transfer_root=str(self.transfer_root)))

    @classmethod
    def load(cls, path):
        from demiflow.workbench.analysis.trusted_local import LocalSite
        path = Path(path)
        value = c.decode(_read(path))
        info = path.stat()
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
            c.reject('Trusted execution config must be private and owned by this account')
        required = {'format', 'deployment_id', 'principal', 'workspace', 'state',
            'python', 'python_sha256', 'code_root', 'code_manifest', 'code_sha256',
            'environment_manifest', 'environment_sha256', 'helper_socket',
            'auth_socket', 'transfer_root', 'limits', 'media_tool', 'service_capabilities'}
        c.fields(value, required, label='trusted local service binding')
        if value['format'] != FORMAT:
            c.reject('Unsupported trusted service binding edition')
        c.identity(value['deployment_id']); c.identity(value['principal'])
        paths = {key:_path(value[key], key, socket=key in {'helper_socket','auth_socket'})
            for key in ('workspace','state','python','code_root','code_manifest',
                'environment_manifest','helper_socket','auth_socket','transfer_root')}
        if paths['helper_socket'] == paths['auth_socket']:
            c.reject('Helper control and original Web authority need distinct sockets')
        for key in ('helper_socket','auth_socket'):
            _directory(paths[key].parent, mode=0o700)
        _directory(paths['state'], mode=0o700)
        _directory(paths['transfer_root'], mode=0o2770)
        if paths['transfer_root'] != paths['state']/'transfer/web-outbox':
            c.reject('Trusted source transfer must use its fixed owned outbox')
        if (paths['state'].is_relative_to(paths['workspace'])
                or paths['workspace'].is_relative_to(paths['state'])):
            c.reject('Analysis state and the configured project must be separate')
        if paths['python'] != Path(sys.executable).absolute():
            c.reject('Controller must use the same fixed interpreter as its workers')
        if paths['code_root'] != Path(__file__).absolute().parents[3]:
            c.reject('Controller and workers must load the same fixed platform root')
        # Capture this before Controller installs its SQLite file-size guard.
        inherited = resource.getrlimit(resource.RLIMIT_FSIZE)
        site = LocalSite(state=paths['state'], python=paths['python'], code_root=paths['code_root'],
            code_manifest=paths['code_manifest'], code_digest=value['code_sha256'],
            python_digest=value['python_sha256'], environment_digest=value['environment_sha256'],
            environment_manifest=paths['environment_manifest'], workspace=paths['workspace'],
            original_fsize=inherited, limits=value['limits'], media_tool=value['media_tool']).check()
        execution_facts.origin(site.origin())
        capabilities = value['service_capabilities']
        c.fields(capabilities, {'analysis_execute','analysis_write','case_annotate'},
            label='trusted service capability ceiling')
        if any(type(flag) is not bool for flag in capabilities.values()):
            c.reject('Trusted service capability ceilings must be explicit booleans')
        return cls(site,value['deployment_id'],value['principal'],paths['helper_socket'],
            paths['auth_socket'],paths['transfer_root'],dict(capabilities))

    def environment(self, authority):
        """Process-free provider inside the existing deferred metadata route."""
        value = authority.call('project-context', {})
        c.fields(value, {'workspace'}, label='authorized project execution context')
        c.text(value['workspace'],'authorized project directory',2048)
        # The root comes only from the original authenticated Web registry.
        # LocalSite validates it; create receives this recorded cwd internally.
        return execution_facts.origin(self.local_site.origin(Path(value['workspace'])))

    def web_config(self, helper_identity):
        c.fields(helper_identity, {'pid','boot','start'}, label='original helper process')
        c.integer(helper_identity['pid'], 'original helper PID', 2**31-1, 1)
        c.text(helper_identity['boot'],'original helper boot',64)
        c.text(helper_identity['start'],'original helper start',32)
        origin = self.local_site.origin()
        return dict(format=WEB_FORMAT, deployment_id=self.deployment_id, principal=self.principal,
            helper_uid=os.geteuid(),helper_identity=dict(helper_identity),
            helper_socket=str(self.helper_socket),auth_socket=str(self.auth_socket),
            transfer_root=str(self.transfer_root),code_digest=origin['code_digest'],
            python_digest=origin['python_digest'],environment_digest=origin['environment_digest'],
            policy_digest=c.digest(origin['limits']),service_capabilities=dict(self.service_capabilities))


def admitted(value):
    return isinstance(value,(Deployment,TrustedBinding))
