"""Canonical Forge OS sandbox policies shared by terminal `forge develop` and Workbench.

Single source of truth for:
  - host-granted read scope (`authorized_read_roots`),
  - restricted developer sandbox policy (`build_development_sandbox_spec`), and
  - strictly read-only independent review sandbox policy (`build_review_sandbox_spec`).
"""
from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
import sys

from demiflow.forge.dependencies import declaration as dependency_declaration
from demiflow.forge.project_workspace import scoped_data_location
from demiflow.forge.sandbox.sandbox import SandboxSpec

PROTECTED_PROJECT_NAMES = (
    '.git', '.forge', '.venv', '.env', '.env.local',
    'AGENTS.md', 'workspace.json', 'PIPELINE_SPEC.md', 'data_tools', 'packages',
)
FORBIDDEN_DIRECT_WRITE_ROOTS = frozenset({('data_tools',), ('packages',), ('.forge',)})
DIRECT_NETWORK_READ_PATHS = (
    '/etc/resolv.conf', '/etc/hosts', '/etc/nsswitch.conf', '/etc/gai.conf',
    '/etc/ssl', '/etc/ca-certificates', '/usr/share/ca-certificates', '/etc/ld.so.cache',
)


def _declared_readonly_locations(root: Path, denied=(), private_home: Path | None = None) -> list[Path]:
    from demiflow.forge.project import load_project
    try:
        _, workspace = load_project(root)
    except Exception:
        return []
    private = [*(Path(p) for p in denied), *([Path(private_home)] if private_home is not None else [])]
    extra: list[Path] = []
    for loc in (workspace.get('locations') or {}).values():
        if not isinstance(loc, dict) or loc.get('writable', False) or not loc.get('root'):
            continue
        scoped = scoped_data_location(root, loc['root'], private=private)
        if scoped is not None and scoped.is_dir() and scoped not in extra:
            extra.append(scoped)
    return extra


def authorized_read_roots(
    root: str | Path,
    *,
    read_roots=(),
    denied_paths=(),
    private_home: str | Path | None = None,
) -> tuple[str, ...]:
    """Collect host-granted read directories (project root, explicit read_roots, readonly locations)."""
    root_path = Path(root).absolute()
    granted: list[Path] = [root_path.resolve()]
    private = [*(Path(p) for p in denied_paths), *([Path(private_home)] if private_home is not None else [])]
    for raw in (read_roots or ()):
        if not raw:
            continue
        scoped = scoped_data_location(root_path, raw, private=private)
        if scoped is not None and scoped not in granted:
            granted.append(scoped)
    for loc_path in _declared_readonly_locations(root_path, denied_paths, Path(private_home) if private_home else None):
        resolved = loc_path.resolve()
        if resolved not in granted:
            granted.append(resolved)
    return tuple(str(p) for p in granted)


def default_runtime_paths(*, executable: str | Path | None = None, project_python: str | Path | None = None,
                          extra_paths=()) -> tuple[str, ...]:
    """Minimal read-only runtime directories required to run Python/Node tools inside the sandbox."""
    from demiflow.forge.runtime import _probe_runtime_dirs, validate_python_executable

    dirs: set[Path] = {
        Path(__file__).resolve().parents[3],  # demiflow repo / installed root
        Path(sys.prefix).resolve(),
        Path(sys.base_prefix).resolve(),
        Path(sys.executable).resolve().parent,
    }
    for sys_dir in ('/usr', '/lib', '/lib64', '/bin'):
        p = Path(sys_dir)
        if p.exists():
            dirs.add(p.resolve())
    if executable:
        exe = Path(executable).resolve()
        if exe.exists():
            if exe.name == 'bin.js' or exe.suffix == '.js':
                if exe.parent.parent != Path('/'):
                    dirs.add(exe.parent.parent)
            else:
                dirs.add(exe)
    if project_python:
        try:
            interp = validate_python_executable(project_python)
            dirs.update(_probe_runtime_dirs(interp))
        except Exception:
            p = Path(project_python)
            if p.exists():
                dirs.add(p.parent.resolve())
    for raw in (extra_paths or ()):
        p = Path(raw).resolve()
        if p.exists() and p != Path('/'):
            dirs.add(p)
    return tuple(sorted(str(p) for p in dirs if p != Path('/')))


def _unique_abs(paths) -> tuple[str, ...]:
    return tuple(sorted({str(Path(path).absolute()) for path in paths}))


def build_development_sandbox_spec(
    root: str | Path,
    *,
    writes=(),
    protected=(),
    read_roots=(),
    extra_read_paths=(),
    runtime_paths=(),
    denied_paths=(),
    private_home: str | Path,
    kind: str = 'pipeline',
    phase: str = 'implementation',
    readonly: bool = False,
    notebook_op: bool = False,
    direct_network: bool = False,
    active_credential_path: str | Path | None = None,
    extra_protected=(),
    seconds: float = 120,
    rss_bytes: int = 2 * 1024**3,
    processes: int = 32,
    file_bytes: int = 64 * 1024**2,
    private_bytes: int = 256 * 1024**2,
    ensure_pyproject_placeholder: bool = False,
) -> SandboxSpec:
    """Build the single canonical Forge OS SandboxSpec for developer / readonly sessions.

    Enforces:
      - `packages/`, `.forge/`, `data_tools/` are ALWAYS protected against direct writes.
      - Pipeline implementation turns may create/edit/save `pyproject.toml` (single-file grant).
      - External `local_ref` / vendor roots require explicit host authorization via `read_roots`.
    """
    root_path = Path(root).absolute()
    read: list[Path] = []
    if not notebook_op:
        read.append(root_path)
        for p in extra_read_paths:
            cand = Path(p)
            if cand.exists():
                read.append(cand)
    openssl = Path('/etc/ssl/openssl.cnf')
    if openssl.is_file():
        read.append(openssl)
    active_cred = None
    if direct_network:
        for net_path in DIRECT_NETWORK_READ_PATHS:
            cand = Path(net_path)
            if cand.exists():
                read.append(cand)
        if active_credential_path:
            cred_path = Path(active_credential_path).resolve()
            if cred_path.is_file():
                active_cred = cred_path
                read.append(cred_path)

    has_deps = False
    if not notebook_op:
        try:
            has_deps = dependency_declaration(root_path) is not None
        except Exception:
            has_deps = (root_path / 'pyproject.toml').is_file()

    write_list: list[Path] = []
    if not readonly:
        for name in writes:
            if Path(name).parts[:1] in FORBIDDEN_DIRECT_WRITE_ROOTS:
                continue
            target = root_path / name
            if target.exists():
                write_list.append(target)

    if not readonly and not notebook_op and kind == 'pipeline' and phase == 'implementation':
        pyproject = root_path / 'pyproject.toml'
        if ensure_pyproject_placeholder and not pyproject.exists() and not pyproject.is_symlink():
            pyproject.write_text('', encoding='utf-8')
        if (pyproject.is_file() and not pyproject.is_symlink()
                and 'pyproject.toml' not in (protected or ())
                and pyproject not in write_list):
            write_list.append(pyproject)

    protected_list: list[Path] = []
    if not notebook_op:
        protected_list.extend(root_path / name for name in PROTECTED_PROJECT_NAMES)
        if root_path.is_dir():
            try:
                with os.scandir(root_path) as root_entries:
                    for entry in root_entries:
                        if entry.name.startswith('.env.') and entry.name not in {'.env.example', '.env.sample'}:
                            protected_list.append(root_path / entry.name)
            except OSError:
                pass
        protected_list.extend(root_path / name for name in (protected or ()))
    protected_list.extend(Path(p) for p in (extra_protected or ()))
    if active_cred is not None:
        protected_list.append(active_cred)

    denied_list = [Path(p) for p in (denied_paths or ())]
    if not notebook_op:
        for granted_str in authorized_read_roots(
            root_path, read_roots=read_roots, denied_paths=denied_list, private_home=private_home,
        ):
            granted_path = Path(granted_str)
            if granted_path.exists():
                read.append(granted_path)

    scoped_reads = bool(readonly or has_deps or read_roots)
    return SandboxSpec(
        read_paths=_unique_abs(read),
        write_paths=_unique_abs(write_list),
        runtime_paths=tuple(sorted({str(Path(p).absolute()) for p in runtime_paths if Path(p).exists()})),
        protected_paths=_unique_abs(protected_list),
        denied_paths=_unique_abs(denied_list),
        private_home=str(Path(private_home).absolute()),
        role='readonly' if readonly else 'developer',
        seconds=seconds,
        rss_bytes=rss_bytes,
        processes=processes,
        file_bytes=file_bytes,
        private_bytes=private_bytes,
        direct_network=direct_network,
        read_policy='scoped' if scoped_reads else 'inherit_os',
        write_policy='scoped',
    )


def build_review_sandbox_spec(
    author_spec: SandboxSpec | None = None,
    *,
    private_home: str | Path,
    seconds: float = 120,
    extra_runtime_paths=(),
    extra_read_paths=(),
    denied_paths=(),
    direct_network: bool = False,
    rss_bytes: int = 2 * 1024**3,
    processes: int = 32,
    file_bytes: int = 64 * 1024**2,
    private_bytes: int = 256 * 1024**2,
) -> SandboxSpec:
    """Build the single canonical strictly read-only Forge SandboxSpec for independent review."""
    home_str = str(Path(private_home).absolute())
    if author_spec is not None:
        review_runtime = tuple(dict.fromkeys((
            *author_spec.runtime_paths,
            *(str(Path(p).absolute()) for p in extra_runtime_paths if Path(p).exists()),
        )))
        read_list = [p for p in author_spec.read_paths if p == '/etc/ssl/openssl.cnf']
        for p in extra_read_paths:
            if Path(p).exists():
                read_list.append(str(Path(p).absolute()))
        if direct_network:
            for net_path in DIRECT_NETWORK_READ_PATHS:
                if Path(net_path).exists():
                    read_list.append(net_path)
        return replace(
            author_spec,
            role='readonly',
            read_policy='scoped',
            write_policy='scoped',
            direct_network=bool(direct_network),
            seconds=seconds,
            private_home=home_str,
            write_paths=(),
            runtime_paths=tuple(p for p in review_runtime if Path(p).exists()),
            denied_paths=tuple(dict.fromkeys((*author_spec.denied_paths, author_spec.private_home,
                                              *(str(Path(p).absolute()) for p in denied_paths)))),
            read_paths=tuple(sorted(set(read_list))),
        )
    read_list = [str(Path(p).absolute()) for p in extra_read_paths if Path(p).exists()]
    openssl = Path('/etc/ssl/openssl.cnf')
    if openssl.is_file():
        read_list.append(str(openssl))
    if direct_network:
        for net_path in DIRECT_NETWORK_READ_PATHS:
            if Path(net_path).exists():
                read_list.append(net_path)
    return SandboxSpec(
        read_paths=tuple(sorted(set(read_list))),
        write_paths=(),
        runtime_paths=tuple(sorted({str(Path(p).absolute()) for p in extra_runtime_paths if Path(p).exists()})),
        protected_paths=(),
        denied_paths=_unique_abs(denied_paths),
        private_home=home_str,
        role='readonly',
        seconds=seconds,
        rss_bytes=rss_bytes,
        processes=processes,
        file_bytes=file_bytes,
        private_bytes=private_bytes,
        direct_network=bool(direct_network),
        read_policy='scoped',
        write_policy='scoped',
    )
