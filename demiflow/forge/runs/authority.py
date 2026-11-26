"""Trusted local run authority owned by Forge, with extensible host bootstrap hooks.

Forge standalone CLI uses LocalAuthority with explicit project Python and read roots.
Optional host layers (such as Workbench WebAuthority) register their own bootstrap
deserializer via register_bootstrap_kind without introducing reverse imports.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from demiflow.forge.schema import ForgeError

VERSION = 'demiflow_run_authority_v1'
ACTIONS = {'read', 'run_submit', 'run_control', 'attach'}

_BOOTSTRAP_KINDS: dict[str, object] = {}


def register_bootstrap_kind(kind: str, loader) -> None:
    """Register a host-owned bootstrap deserializer (e.g. Workbench WebAuthority)."""
    if not isinstance(kind, str) or not kind or kind == 'trusted_local':
        raise ValueError('Invalid bootstrap authority kind')
    _BOOTSTRAP_KINDS[kind] = loader


@dataclass(frozen=True)
class LocalAuthority:
    root: str
    python: str | None = None
    read_roots: tuple[str, ...] | None = None

    def require(self, root, action):
        if str(Path(root).absolute()) != self.root or action not in ACTIONS:
            raise PermissionError('Run authority target or action differs')

    @contextmanager
    def guard(self, root, action):
        self.require(root, action)
        yield

    def resolve_dependencies(self, root, *, python=None, verify_files=True, require_environment=False):
        from demiflow.forge.dependencies import binding, declaration
        if declaration(root) is None:
            return None
        effective_py = str(python) if python else self.python
        if not effective_py:
            raise ForgeError('Configure an explicit project Python before admitting dependencies', code='operation_rejected')
        return binding(
            root,
            python=effective_py,
            allowed_roots=self.read_roots,
            verify_files=verify_files,
            require_python=True,
            require_environment=require_environment,
        )

    def inspector_operation(self, name, root, *, _pass_fds=(), _authority=None, _source_modules=None, _dependencies=None, **arguments):
        from demiflow.forge.runs.inspector import run_operation
        return run_operation(
            name, root, _pass_fds=_pass_fds, _authority=_authority,
            _source_modules=_source_modules, _dependencies=_dependencies, **arguments,
        )

    def bootstrap(self):
        value = dict(format=VERSION, kind='trusted_local', root=self.root)
        if self.python:
            value['python'] = self.python
        if self.read_roots is not None:
            value['read_roots'] = list(self.read_roots)
        return value


def from_bootstrap(value, root):
    """Closed internal protocol. No callable, import path or client grant flag."""
    if type(value) is not dict or value.get('format') != VERSION or value.get('root') != str(Path(root).absolute()):
        raise ForgeError('Missing or invalid trusted run bootstrap', code='operation_rejected')
    kind = value.get('kind')
    if kind == 'trusted_local' and set(value) <= {'format', 'kind', 'root', 'python', 'read_roots'}:
        py = value.get('python')
        if py is not None and (type(py) is not str or not py or not Path(py).is_absolute()):
            raise ForgeError('Invalid trusted run bootstrap', code='operation_rejected')
        rr = value.get('read_roots')
        if rr is not None:
            if type(rr) is not list or len(rr) > 64 or any(type(p) is not str or not p or not Path(p).is_absolute() for p in rr):
                raise ForgeError('Invalid trusted run bootstrap', code='operation_rejected')
            rr = tuple(rr)
        return LocalAuthority(value['root'], python=py, read_roots=rr)
    loader = _BOOTSTRAP_KINDS.get(kind)
    if loader is not None:
        return loader(value, root)
    raise ForgeError('Invalid trusted run bootstrap', code='operation_rejected')
