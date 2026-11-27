"""Canonical Forge OS sandbox policies shared by terminal `forge develop` and Workbench.

Single source of truth for:
  - host-granted read scope (`authorized_read_roots`),
  - restricted developer sandbox policy (`build_development_sandbox_spec`), and
  - strictly read-only independent review sandbox policy (`build_review_sandbox_spec`).
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import os
from pathlib import Path
import sys

from demiflow.forge.project_io import SYSTEM_PRIVATE_PATHS
from demiflow.forge.project_workspace import scoped_data_location
from demiflow.forge.sandbox.sandbox import SandboxSpec

PROTECTED_PROJECT_NAMES = (
    '.git', '.forge', '.venv', '.env', '.env.local',
    'AGENTS.md', 'workspace.json', 'PIPELINE_SPEC.md', 'data_tools', 'packages',
)
FORBIDDEN_DIRECT_WRITE_ROOTS = frozenset({('data_tools',), ('packages',), ('.forge',)})
DIRECT_NETWORK_READ_PATHS = (
    '/etc/resolv.conf', '/etc/hosts', '/etc/nsswitch.conf', '/etc/gai.conf',
    '/etc/ssl/certs', '/etc/ssl/cert.pem', '/etc/ssl/openssl.cnf',
    '/etc/ca-certificates', '/usr/share/ca-certificates', '/etc/ld.so.cache',
)
ACCOUNT_PRIVATE_PATHS = (
    '.ssh', '.aws', '.azure', '.kube', '.gnupg', '.config/gcloud', '.docker/config.json',
    '.netrc', '.npmrc', '.pypirc', '.git-credentials', '.codex',
)


def default_private_read_paths():
    """Resolve private OS and account paths in the trusted host environment."""
    paths = []
    account = Path.home()
    paths.extend(account / name for name in ACCOUNT_PRIVATE_PATHS)
    codex_home = os.environ.get('CODEX_HOME')
    if codex_home and Path(codex_home).is_absolute():
        paths.append(Path(codex_home))
    dsh_home = Path(os.environ.get('DSH_HOME') or account / '.dsh')
    if dsh_home.is_absolute():
        paths.extend((dsh_home / '.credentials.yaml', dsh_home / 'profiles'))
    if os.name == 'posix':
        paths.extend(SYSTEM_PRIVATE_PATHS)
    return tuple(paths)


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


def development_read_scope(root, *, read_roots=(), denied_paths=(), private_home=None):
    """Return the developer's explicit source scope, or None to inherit ordinary OS reads."""
    if not read_roots:
        return None
    return authorized_read_roots(root, read_roots=read_roots, denied_paths=denied_paths, private_home=private_home)


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


def declaration_identity(root):
    """Identify the directory entry to which the author's single-file grant was bound."""
    try:
        entry = (Path(root) / 'pyproject.toml').lstat()
    except FileNotFoundError:
        return None
    return entry.st_dev, entry.st_ino


@contextmanager
def writable_project_declaration(root):
    """Prepare the single-file grant only while an author is running, between source snapshots."""
    path = Path(root) / 'pyproject.toml'
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        yield
        return
    try:
        # Distinguish an intentional empty save even on filesystems with coarse timestamps.
        os.utime(fd, ns=(os.fstat(fd).st_atime_ns, 0))
        original = os.fstat(fd)

        def untouched():
            try:
                current = path.lstat()
            except FileNotFoundError:
                return False
            return (os.path.samestat(original, current) and current.st_size == 0 and current.st_mtime_ns == 0
                    and current.st_ctime_ns == original.st_ctime_ns)

        try:
            yield untouched
        finally:
            if untouched():
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
    finally:
        os.close(fd)


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
    host_state_root: str | Path | None = None,
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
) -> SandboxSpec:
    """Build the single canonical Forge OS SandboxSpec for developer / readonly sessions.

    Visibility (`read_policy`) and dependencies are strictly decoupled:
      - `packages/`, `.forge/`, `data_tools/` are ALWAYS protected against direct writes.
      - Pipeline implementation turns may create/edit/save `pyproject.toml` (single-file grant).
      - Read visibility is controlled solely by the sandbox (`readonly` turns or explicit
        sandbox-level `read_roots` use `scoped`; normal developer sessions use `inherit_os`
        regardless of whether `pyproject.toml` declares dependencies).
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
        if (pyproject.is_file() and not pyproject.is_symlink()
                and 'pyproject.toml' not in (protected or ())
                and pyproject not in write_list):
            write_list.append(pyproject)

    protected_list: list[Path] = []
    private_files: list[Path] = []
    if not notebook_op:
        protected_list.extend(root_path / name for name in PROTECTED_PROJECT_NAMES)
        if root_path.is_dir():
            try:
                with os.scandir(root_path) as root_entries:
                    for entry in root_entries:
                        if (entry.name == '.env' or entry.name.startswith('.env.')) and entry.name not in {'.env.example', '.env.sample'}:
                            private_files.append(root_path / entry.name)
            except OSError:
                pass
        protected_list.extend(private_files)
        protected_list.extend(root_path / name for name in (protected or ()))
    protected_list.extend(Path(p) for p in (extra_protected or ()))
    if active_cred is not None:
        protected_list.append(active_cred)

    denied_list = [Path(p) for p in (denied_paths or ())]
    if active_cred is not None and any(active_cred.is_relative_to(path.resolve()) and not (
            host_state_root is not None and path.resolve() == Path(host_state_root).resolve()
            and active_cred.is_relative_to(Path(private_home).resolve())) for path in denied_list):
        raise ValueError('Selected model credential overlaps a host-denied path')
    denied_list.extend(default_private_read_paths())
    denied_list.extend(private_files)
    if not notebook_op:
        for granted_str in authorized_read_roots(
            root_path, read_roots=read_roots, denied_paths=denied_list, private_home=private_home,
        ):
            granted_path = Path(granted_str)
            if granted_path.exists():
                read.append(granted_path)

    scoped_reads = bool(readonly or read_roots)
    return SandboxSpec(
        read_paths=_unique_abs(read),
        write_paths=_unique_abs(write_list),
        runtime_paths=tuple(sorted({str(Path(p).absolute()) for p in runtime_paths if Path(p).exists()})),
        protected_paths=_unique_abs(protected_list),
        denied_paths=_unique_abs(denied_list),
        credential_read_path=str(active_cred) if active_cred is not None else None,
        host_state_root=str(host_state_root) if host_state_root is not None else None,
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
            credential_read_path=None,
            host_state_root=(author_spec.host_state_root if author_spec.host_state_root is not None
                             and Path(home_str).is_relative_to(Path(author_spec.host_state_root)) else None),
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
