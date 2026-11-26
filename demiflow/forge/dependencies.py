"""Project dependency declaration (pyproject.toml), unified single-package rules, and dep commands."""
from __future__ import annotations

import ast
import hashlib
import keyword
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import sys

from packaging.requirements import InvalidRequirement, Requirement

from . import operation_budget
from .development_state import physical_root
from .io import atomic_write_text, ensure_directory, no_symlinks, publish_bytes
from .platform import content_digest
from .platform_environment import read_bounded
from .project_io import control_lock
from .schema import ForgeError

try:
    import tomllib
except ImportError:
    import tomli as tomllib


FORMAT = 'demiflow_project_dependencies_v2'
MAX_DECLARATION_BYTES = 128 * 1024
MAX_PACKAGES = 16
MAX_PKG_FILES = 256
MAX_PKG_BYTES = 8 * 1024**2
MAX_FILES = 512
MAX_BYTES = 16 * 1024**2
FILE_BYTES = 512 * 1024
MAX_REQUIREMENTS = 256

_RESERVED = frozenset({
    'demiflow', 'flows', 'operators', 'data_tools', 'tests', 'prompts', 'configs',
    'packages', 'notebooks', 'workflows', 'sitecustomize', 'usercustomize',
})
_IGNORED_DIRS = frozenset({'__pycache__'})
_PRIVATE_SEGMENTS = frozenset({
    '.ssh', '.codex', '.codex-home', '.config', '.aws', '.azure', '.kube',
    '.git', '.gnupg', '.netrc', '.npmrc', '.pypirc', '.git-credentials',
    '.env', 'access.token', 'auth.json', 'credentials.json',
})
_SYSTEM_DENIED = tuple(Path(p) for p in (
    '/proc', '/sys', '/dev', '/run', '/etc/shadow', '/etc/gshadow',
    '/etc/ssh', '/etc/ssl/private', '/etc/sudoers', '/etc/sudoers.d',
))
_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,63}\Z')
_DEPENDENCY_URL_RE = re.compile(
    r"""(?<![A-Za-z0-9+.-])([A-Za-z][A-Za-z0-9+.-]*://)"""
    r"""([^/?#\s"'\\]*)([^?#\s"'\\]*)([?#][^\s"'\\]*)?"""
)


def _is_within(path: Path, root: Path) -> bool:
    return path == root or path.is_relative_to(root)


def _has_private_segment(path: Path) -> bool:
    return any(
        part in _PRIVATE_SEGMENTS or part.startswith('.env.')
        or (part.startswith('demiflow-') and '-preview-' in part)
        for part in path.parts
    )


def _canonical_roots(roots) -> tuple[Path, ...]:
    out: list[Path] = []
    for raw in roots or ():
        if raw is None or raw == '':
            continue
        p = Path(raw)
        if not p.is_absolute() or '..' in p.parts:
            continue
        try:
            resolved = p.resolve(strict=False)
        except OSError:
            continue
        if resolved == Path('/'):
            continue
        if resolved not in out:
            out.append(resolved)
    return tuple(out)


def authorize_external_path(project_root: Path, target: Path, *, allowed_roots=None, denied_paths=()) -> Path:
    """Verify caller-granted directory scope BEFORE any stat/scandir/read/hash of an external source."""
    root = Path(project_root).absolute()
    target = Path(target)
    if not target.is_absolute() or '..' in target.parts or '\x00' in str(target):
        raise ForgeError('External dependency path must be a clean absolute path', code='operation_rejected')
    if target == Path('/') or target == root or root.is_relative_to(target) or target.is_relative_to(root):
        raise ForgeError('External dependency path must be a specific directory outside the project', code='operation_rejected')
    if _has_private_segment(target) or any(_is_within(target, sys_dir) for sys_dir in _SYSTEM_DENIED):
        raise ForgeError('External dependency path enters a private or system directory', code='operation_rejected')
    for denied in _canonical_roots(denied_paths):
        if _is_within(target, denied):
            raise ForgeError('External dependency path enters a denied directory', code='operation_rejected')
    if allowed_roots is not None:
        granted = _canonical_roots(allowed_roots)
        if not any(_is_within(target, allowed) for allowed in granted):
            raise ForgeError(
                f'External dependency path is outside the host-authorized read scope: {target}',
                code='operation_rejected',
            )
    # Reject symlinks anywhere along the target path after scope check.
    physical = no_symlinks(target)
    if _has_private_segment(physical) or any(_is_within(physical, sys_dir) for sys_dir in _SYSTEM_DENIED):
        raise ForgeError('External dependency path resolves into a private or system directory', code='operation_rejected')
    for denied in _canonical_roots(denied_paths):
        if _is_within(physical, denied):
            raise ForgeError('External dependency path resolves into a denied directory', code='operation_rejected')
    if allowed_roots is not None:
        granted = _canonical_roots(allowed_roots)
        if not any(_is_within(physical, allowed) for allowed in granted):
            raise ForgeError(
                f'External dependency path resolves outside the host-authorized read scope: {physical}',
                code='operation_rejected',
            )
    return physical


def declaration(root) -> dict | None:
    """Read the opted-in dependency declaration from pyproject.toml.

    Projects without a ``[tool.demiflow.packages]`` table keep their legacy behavior
    (returns ``None``). Projects using only A-class installed dependencies can opt in
    with an empty ``[tool.demiflow.packages]`` table.
    """
    root = no_symlinks(Path(root).absolute())
    pyproject = root / 'pyproject.toml'
    try:
        raw = read_bounded(pyproject, MAX_DECLARATION_BYTES)
    except FileNotFoundError:
        return None
    except ForgeError as error:
        if error.code == 'resource_limit' or pyproject.is_symlink():
            if not (root / 'packages').exists():
                return None
        raise
    if not raw.strip():
        return None
    try:
        text = raw.decode('utf-8')
        value = tomllib.loads(text)
    except (UnicodeError, ValueError, RecursionError) as error:
        if b'tool.demiflow.packages' not in raw and not (root / 'packages').exists():
            return None
        raise ForgeError('Invalid dependency declaration in pyproject.toml') from error
    tools = value.get('tool', {})
    forge = tools.get('demiflow', {}) if isinstance(tools, dict) else {}
    if not isinstance(forge, dict) or 'packages' not in forge:
        return None
    packages = forge['packages']
    if type(packages) is not dict or len(packages) > MAX_PACKAGES:
        raise ForgeError(f'Expected at most {MAX_PACKAGES} packages in [tool.demiflow.packages]')
    project = value.get('project', {})
    requirements = project.get('dependencies', []) if isinstance(project, dict) else []
    if type(requirements) is not list or len(requirements) > MAX_REQUIREMENTS:
        raise ForgeError('Invalid [project].dependencies')
    normalized_reqs = []
    raw_reqs = []
    try:
        for item in requirements:
            if type(item) is not str or len(item) > 4096:
                raise ValueError('Requirement must be a bounded string')
            raw_reqs.append(item)
            normalized_reqs.append(str(Requirement(item)))
    except (ValueError, InvalidRequirement) as error:
        raise ForgeError('Invalid PEP 508 project dependency') from error
    clean_packages = {}
    for name, item in packages.items():
        if (not isinstance(name, str) or not _NAME.fullmatch(name) or keyword.iskeyword(name)
                or name in _RESERVED or name in sys.stdlib_module_names):
            raise ForgeError('Invalid or reserved dependency package: ' + str(name)[:64])
        if type(item) is not dict or set(item) != {'source', 'path'} or type(item['path']) is not str:
            raise ForgeError('A dependency package requires source and path: ' + name)
        raw_path = item['path']
        path = Path(raw_path)
        if len(raw_path) > 4096 or '\0' in raw_path or str(path) != raw_path or '..' in path.parts:
            raise ForgeError('Invalid dependency package path: ' + name)
        if item['source'] == 'vendored':
            if path != Path('packages') / name:
                raise ForgeError('Vendored packages must use packages/' + name)
        elif item['source'] == 'local_ref':
            if not path.is_absolute() or path == root or root.is_relative_to(path):
                raise ForgeError('A local_ref must name a specific outside package directory: ' + name)
            if path.is_relative_to(root):
                raise ForgeError('Project-owned package source must use the vendored declaration: ' + name)
        else:
            raise ForgeError('Dependency package source must be vendored or local_ref: ' + name)
        clean_packages[name] = {'source': item['source'], 'path': raw_path}
    return dict(
        sha256=hashlib.sha256(raw).hexdigest(),
        raw=text,
        packages=clean_packages,
        requirements=normalized_reqs,
        raw_requirements=raw_reqs,
    )


def declaration_line_map(raw_text: str) -> dict:
    """Best-effort line lookup for [tool.demiflow.packages] entries and [project].dependencies."""
    lines = raw_text.splitlines()
    result: dict = {'packages_table': None, 'dependencies': None, 'packages': {}, 'requirements': {}}
    section = ''
    in_deps = False
    for lineno, line in enumerate(lines, 1):
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        if stripped.startswith('[') and not stripped.startswith('[['):
            header = stripped.split('#', 1)[0].strip()
            if header.startswith('[') and header.endswith(']'):
                section = header[1:-1].strip()
                in_deps = False
                if section == 'tool.demiflow.packages' and result['packages_table'] is None:
                    result['packages_table'] = lineno
                elif section.startswith('tool.demiflow.packages.'):
                    pkg = section[len('tool.demiflow.packages.'):].strip().strip('"').strip("'")
                    if pkg and pkg not in result['packages']:
                        result['packages'][pkg] = lineno
                continue
        if section == 'project' and re.match(r'dependencies\s*=', stripped):
            result['dependencies'] = lineno
            in_deps = '[' in stripped and ']' not in stripped.split('#', 1)[0]
            for m in re.finditer(r'["\']([^"\']+)["\']', stripped):
                result['requirements'].setdefault(m.group(1), lineno)
            continue
        if in_deps:
            for m in re.finditer(r'["\']([^"\']+)["\']', stripped):
                result['requirements'].setdefault(m.group(1), lineno)
            if ']' in stripped.split('#', 1)[0]:
                in_deps = False
            continue
        if section == 'tool.demiflow.packages':
            m = re.match(r'([A-Za-z_][A-Za-z0-9_]*)\s*=', stripped)
            if m:
                result['packages'].setdefault(m.group(1), lineno)
    return result


def redact_dependency_urls(text: str) -> str:
    """Keep a dependency's origin and path while concealing credentials and signed URL parameters."""
    def public_url(match):
        scheme, authority, path, parameters = match.groups()
        host = authority.rsplit('@', 1)[-1]
        return scheme + host + path + (parameters[0] + '<redacted>' if parameters else '')

    return _DEPENDENCY_URL_RE.sub(public_url, text)


def redact_declaration_text(raw_text: str) -> str:
    """Redact credentials and host-private local_ref absolute paths while preserving exact line numbers."""
    out_lines = []
    section = ''
    current_pkg = None
    in_local_ref_table = False
    for line in redact_dependency_urls(raw_text).splitlines(keepends=True):
        ending = ''
        body = line
        if body.endswith('\r\n'):
            body, ending = body[:-2], '\r\n'
        elif body.endswith('\n'):
            body, ending = body[:-1], '\n'
        stripped = body.strip()
        header = stripped.split('#', 1)[0].strip()
        if header.startswith('[') and header.endswith(']') and not header.startswith('[['):
            section = header[1:-1].strip()
            if section.startswith('tool.demiflow.packages.'):
                current_pkg = section[len('tool.demiflow.packages.'):].strip().strip('"').strip("'")
                in_local_ref_table = True
            else:
                current_pkg = None
                in_local_ref_table = False
            out_lines.append(body + ending)
            continue
        redacted = body
        if section == 'tool.demiflow.packages':
            m = re.match(r'^(\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*=\s*\{.*)$', redacted)
            if m and re.search(r'source\s*=\s*["\']local_ref["\']', m.group(3)):
                pkg_name = m.group(2)
                rest = re.sub(
                    r'(path\s*=\s*)(["\'])(?:\\.|(?!\2).)*\2',
                    lambda pm: f'{pm.group(1)}"<local_ref:{pkg_name}>"',
                    m.group(3),
                )
                redacted = m.group(1) + pkg_name + rest
        elif in_local_ref_table and current_pkg:
            # Redact absolute path lines under [tool.demiflow.packages.<name>]
            redacted = re.sub(
                r'^(\s*path\s*=\s*)(["\'])(/[^"\']*)\2',
                lambda pm: f'{pm.group(1)}"<local_ref:{current_pkg}>"',
                redacted,
            )
        out_lines.append(redacted + ending)
    return ''.join(out_lines)


def _check_package_imports(raw: bytes, relative: Path, package: str, root: Path) -> None:
    try:
        tree = ast.parse(raw, filename=str(relative))
    except (SyntaxError, ValueError, RecursionError) as error:
        raise ForgeError(f'Invalid Python in dependency package {package}/{relative}: {error}') from error
    reverse_roots = _RESERVED - {'sitecustomize', 'usercustomize'}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names = [node.module, *(node.module + '.' + alias.name for alias in node.names)]
        else:
            continue
        for name in names:
            parts = name.split('.')
            qualified_project = any(
                tuple(parts[:depth]) == root.parts[-depth:] and parts[depth] in reverse_roots
                for depth in range(1, min(8, len(parts) - 1) + 1)
            )
            if parts[0] in reverse_roots or qualified_project:
                raise ForgeError(
                    f'Dependency {package}/{relative}:{node.lineno} imports a project role or platform module'
                )


def validate_package_source(
    name: str,
    source: Path,
    *,
    project_root: Path,
    files_left: int = MAX_FILES,
    bytes_left: int = MAX_BYTES,
    check_imports: bool = True,
    allowed_roots=None,
    denied_paths=(),
    is_external: bool = False,
) -> dict:
    """Unified single-package validation rule shared by `forge check`, `forge dep vendor`, `verify`, `runtime`, and `review`."""
    root = Path(project_root).absolute()
    if (not isinstance(name, str) or not _NAME.fullmatch(name) or keyword.iskeyword(name)
            or name in _RESERVED or name in sys.stdlib_module_names):
        raise ForgeError('Invalid or reserved dependency package: ' + str(name)[:64])
    if is_external:
        source = authorize_external_path(root, Path(source), allowed_roots=allowed_roots, denied_paths=denied_paths)
    else:
        source = no_symlinks(Path(source).absolute())
    if not source.is_dir() or source.is_symlink() or not (source / '__init__.py').is_file() or (source / '__init__.py').is_symlink():
        raise ForgeError('Expected a regular Python package with __init__.py: ' + name, code='not_found')
    pending = [source]
    count = 0
    total = 0
    files: dict[str, dict] = {}
    pkg_file_cap = min(MAX_PKG_FILES, files_left)
    pkg_byte_cap = min(MAX_PKG_BYTES, bytes_left)
    while pending:
        folder = pending.pop()
        operation_budget.check()
        before = folder.lstat()
        if not stat.S_ISDIR(before.st_mode):
            raise ForgeError('Non-directory in dependency package: ' + name)
        with os.scandir(folder) as entries:
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                if entry.name in _IGNORED_DIRS:
                    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                        raise ForgeError('Invalid __pycache__ entry in dependency package: ' + name)
                    continue
                count += 1
                rel = Path(entry.path).relative_to(source)
                if count > 512 or len(rel.parts) > 12:
                    raise ForgeError('Package tree exceeds its directory budget: ' + name, code='resource_limit')
                if stat.S_ISDIR(info.st_mode):
                    if not _NAME.fullmatch(entry.name):
                        raise ForgeError('Invalid directory in dependency package: ' + str(rel))
                    pending.append(Path(entry.path))
                    continue
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or rel.suffix != '.py' or not _NAME.fullmatch(rel.stem)):
                    raise ForgeError('Dependency packages accept only direct Python source files: ' + str(rel))
                if len(files) >= pkg_file_cap or total + info.st_size > pkg_byte_cap:
                    raise ForgeError('Dependency package exceeds its file or byte budget: ' + name, code='resource_limit')
                raw = read_bounded(Path(entry.path), min(FILE_BYTES, pkg_byte_cap - total))
                total += len(raw)
                if check_imports:
                    _check_package_imports(raw, rel, name, root)
                files[rel.as_posix()] = dict(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
        if before.st_mtime_ns != folder.lstat().st_mtime_ns:
            raise ForgeError('Dependency package changed while scanning: ' + name, code='inputs_changed')
    if '__init__.py' not in files:
        raise ForgeError('Dependency package initializer disappeared: ' + name, code='inputs_changed')
    for relative in files:
        parent = Path(relative).parent
        while parent != Path('.'):
            if (parent / '__init__.py').as_posix() not in files:
                raise ForgeError('Dependency subpackage needs __init__.py: ' + str(parent))
            parent = parent.parent
    return dict(files=files, bytes=total, file_count=len(files), tree_sha256=content_digest(files))


def find_orphan_packages(root: Path, config: dict | None) -> list[dict]:
    """Find undeclared or invalid entries directly under packages/."""
    root = Path(root).absolute()
    vendor = root / 'packages'
    if not (vendor.exists() or vendor.is_symlink()):
        return []
    if vendor.is_symlink() or not vendor.is_dir():
        return [dict(path='packages', detail='packages 必须是项目内的普通目录，不能是软链或文件。')]
    allowed = {
        name for name, item in ((config or {}).get('packages') or {}).items()
        if item.get('source') == 'vendored'
    }
    orphans: list[dict] = []
    with os.scandir(no_symlinks(vendor)) as entries:
        for index, entry in enumerate(entries):
            if entry.name.startswith('.vendor-'):
                continue
            if index >= MAX_PACKAGES + 16:
                orphans.append(dict(path='packages', detail='packages/ 下条目数超过预算上限。'))
                break
            rel = f'packages/{entry.name}'
            if entry.name not in allowed:
                orphans.append(dict(path=rel, detail=f'packages/ 下存在未在 pyproject.toml 声明的孤儿条目：{entry.name}。'))
            elif not entry.is_dir(follow_symlinks=False):
                orphans.append(dict(path=rel, detail=f'packages/{entry.name} 必须是普通目录，不能是文件或软链。'))
    return sorted(orphans, key=lambda item: item['path'])


def _requirements_with_platform(config: dict) -> list[str]:
    from .platform_environment import platform_declaration
    platform = platform_declaration(Path(__file__).resolve().parents[1], {'distributions': 256, 'metadata_file_bytes': 4 * 1024**2, 'metadata_bytes': 16 * 1024**2})
    return list(dict.fromkeys([*platform['requires'], *config['requirements']]))


def module_files(root, binding_or_packages) -> dict[str, dict]:
    """Project validated B-class package inventory onto exact qualified module names and file paths."""
    result: dict[str, dict] = {}
    if not binding_or_packages:
        return result
    packages = binding_or_packages.get('packages', binding_or_packages) if isinstance(binding_or_packages, dict) else {}
    root_path = Path(root).absolute()
    for name, item in sorted(packages.items()):
        if not isinstance(item, dict) or 'files' not in item:
            continue
        base = Path(item['path']) if Path(item['path']).is_absolute() else (root_path / item['path'])
        for relative, proof in sorted(item['files'].items()):
            path = Path(relative)
            parts = path.parent.parts if path.name == '__init__.py' else path.with_suffix('').parts
            qualified = '.'.join((name, *parts))
            if qualified in result:
                raise ForgeError('Two files claim the same dependency module: ' + qualified, code='conflict')
            result[qualified] = dict(path=str(base / relative), package=name, **proof)
    return result


def inspect_dependencies(
    root,
    *,
    python=None,
    allowed_roots=None,
    denied_paths=(),
    check_imports: bool = True,
    check_environment: bool = False,
    probe_imports=(),
    allow_partial: bool = True,
) -> dict:
    """Live inspection of pyproject.toml, B-class packages, orphans, and optional A-class Python environment."""
    root = no_symlinks(Path(root).absolute())
    try:
        config = declaration(root)
    except ForgeError as exc:
        if not allow_partial:
            raise
        orphans = find_orphan_packages(root, None)
        return dict(
            declared=True,
            config=None,
            declaration_error=str(exc),
            packages={},
            missing_packages=[],
            invalid_packages=[],
            orphan_packages=orphans,
            module_collisions=[],
            environment=None,
            environment_skipped=False,
            environment_error=None,
            missing_requirements=[],
            shadowed_packages=[],
            import_probe=None,
            digest=None,
        )
    orphans = find_orphan_packages(root, config)
    if config is None:
        return dict(
            declared=False,
            config=None,
            declaration_error=None,
            packages={},
            missing_packages=[],
            invalid_packages=[],
            orphan_packages=orphans,
            module_collisions=[],
            environment=None,
            environment_skipped=False,
            environment_error=None,
            missing_requirements=[],
            shadowed_packages=[],
            import_probe=None,
            digest=None,
        )
    line_map = declaration_line_map(config['raw'])
    valid_packages: dict[str, dict] = {}
    missing_packages: list[dict] = []
    invalid_packages: list[dict] = []
    left_files, left_bytes = MAX_FILES, MAX_BYTES
    for name, item in sorted(config['packages'].items()):
        is_external = item['source'] == 'local_ref'
        source_path = Path(item['path']) if is_external else (root / item['path'])
        line = line_map['packages'].get(name) or line_map.get('packages_table')
        try:
            if is_external:
                source_path = authorize_external_path(
                    root, source_path, allowed_roots=allowed_roots, denied_paths=denied_paths,
                )
            if not source_path.exists():
                entry = dict(name=name, source=item['source'], path=item['path'], line=line,
                             detail=f'已声明的依赖包 {name} 缺失（期望路径：{item["path"]}）。')
                if not allow_partial:
                    raise ForgeError(entry['detail'], code='not_found')
                missing_packages.append(entry)
                continue
            scanned = validate_package_source(
                name,
                source_path,
                project_root=root,
                files_left=left_files,
                bytes_left=left_bytes,
                check_imports=check_imports,
                allowed_roots=allowed_roots,
                denied_paths=denied_paths,
                is_external=is_external,
            )
            left_files -= scanned['file_count']
            left_bytes -= scanned['bytes']
            valid_packages[name] = dict(source=item['source'], path=item['path'], **scanned)
        except ForgeError as exc:
            if not allow_partial:
                raise
            entry = dict(name=name, source=item['source'], path=item['path'], line=line, code=exc.code, detail=str(exc))
            if exc.code == 'not_found':
                missing_packages.append(entry)
            else:
                invalid_packages.append(entry)
    module_collisions: list[dict] = []
    try:
        module_files(root, valid_packages)
    except ForgeError as exc:
        if not allow_partial:
            raise
        module_collisions.append(dict(detail=str(exc), line=line_map.get('packages_table')))
    if not allow_partial and orphans:
        raise ForgeError(orphans[0]['detail'], code='conflict')

    environment = None
    environment_skipped = False
    environment_error = None
    missing_requirements: list[dict] = []
    shadowed_packages: list[dict] = []
    import_probe = None

    needs_env_check = bool(config['requirements'] or config['packages'] or probe_imports)
    if check_environment:
        if python:
            from .runtime import probe_environment
            probe_cfg = dict(
                requirements=config['requirements'],
                platform_requirements=_requirements_with_platform(config),
                packages=sorted(config['packages']),
            )
            try:
                probed = probe_environment(
                    python,
                    probe_cfg,
                    strict=not allow_partial,
                    check_conflicts=True,
                    probe_imports=tuple(sorted(set(probe_imports))),
                )
                if 'environment' in probed and 'interpreter' not in probed:
                    environment = probed['environment']
                    for item in probed.get('missing_requirements', ()):
                        req_line = line_map['requirements'].get(item.get('spec')) or line_map.get('dependencies')
                        missing_requirements.append({**item, 'line': req_line})
                    for pkg in probed.get('shadowed_packages', ()):
                        shadowed_packages.append(dict(
                            name=pkg,
                            line=line_map['packages'].get(pkg) or line_map.get('packages_table'),
                            detail=f'B 类依赖包 {pkg} 与项目 Python 环境已安装或内置模块同名冲突。',
                        ))
                    import_probe = probed.get('import_probe')
                else:
                    environment = probed
            except ForgeError as exc:
                if not allow_partial:
                    raise
                environment_error = str(exc)
        elif needs_env_check:
            environment_skipped = True

    digest_payload = dict(
        format=FORMAT,
        project=physical_root(root),
        declaration=config['sha256'],
        packages={
            name: dict(source=item['source'], path=item['path'] if item['source'] == 'vendored' else '<local_ref>',
                       tree_sha256=item['tree_sha256'], file_count=item['file_count'], bytes=item['bytes'])
            for name, item in sorted(valid_packages.items())
        },
        requirements=config['requirements'],
    )
    return dict(
        declared=True,
        config=config,
        declaration_error=None,
        packages=valid_packages,
        missing_packages=missing_packages,
        invalid_packages=invalid_packages,
        orphan_packages=orphans,
        module_collisions=module_collisions,
        environment=environment,
        environment_skipped=environment_skipped,
        environment_error=environment_error,
        missing_requirements=missing_requirements,
        shadowed_packages=shadowed_packages,
        import_probe=import_probe,
        digest=content_digest(digest_payload),
    )


def binding(
    root,
    *,
    python=None,
    allowed_roots=None,
    denied_paths=(),
    verify_files: bool = True,
    check_imports: bool = False,
    require_python: bool = False,
    require_environment: bool = False,
) -> dict | None:
    """Resolve live validated dependency binding directly from pyproject.toml (no lock file)."""
    root = no_symlinks(Path(root).absolute())
    config = declaration(root)
    if config is None:
        return None
    if require_python and not python:
        raise ForgeError('Configure an explicit project Python before admitting dependencies', code='operation_rejected')
    if verify_files:
        inspected = inspect_dependencies(
            root,
            python=python,
            allowed_roots=allowed_roots,
            denied_paths=denied_paths,
            check_imports=check_imports,
            check_environment=bool(python and require_environment),
            allow_partial=False,
        )
        packages = inspected['packages']
        environment = inspected['environment']
    else:
        packages = {
            name: dict(source=item['source'], path=item['path'])
            for name, item in sorted(config['packages'].items())
        }
        environment = None
        if python and require_environment:
            from .runtime import probe_environment
            probed = probe_environment(
                python,
                dict(requirements=config['requirements'], platform_requirements=_requirements_with_platform(config),
                     packages=sorted(config['packages'])),
                strict=True,
                check_conflicts=True,
            )
            environment = probed
    requirements = _requirements_with_platform(config)
    record = dict(
        format=FORMAT,
        project=physical_root(root),
        declaration=config['sha256'],
        packages=packages,
        requirements=requirements,
        declared_requirements=config['requirements'],
        environment=environment,
    )
    if verify_files:
        module_files(root, record)
    digest_basis = {
        k: v for k, v in record.items()
        if k != 'packages'
    }
    digest_basis['packages'] = {
        name: {k: item[k] for k in ('source', 'path', 'tree_sha256', 'file_count', 'bytes') if k in item}
        for name, item in sorted(packages.items())
    }
    record['digest'] = content_digest(digest_basis)
    return record


def facts(bound: dict | None, *, include_paths: bool = False) -> dict | None:
    if bound is None:
        return None
    packages = {}
    for name, item in sorted((bound.get('packages') or {}).items()):
        entry = {'source': item['source']}
        for field in ('file_count', 'bytes', 'tree_sha256'):
            if field in item:
                entry[field] = item[field]
        if include_paths and item.get('source') == 'vendored':
            entry['path'] = item['path']
        packages[name] = entry
    env = bound.get('environment')
    distributions = [row['name'] for row in env['inventory']['dependencies']] if isinstance(env, dict) else []
    return dict(
        digest=bound['digest'],
        declaration=bound['declaration'],
        requirements=list(bound.get('declared_requirements', bound.get('requirements', ()))),
        distributions=distributions,
        packages=packages,
    )


def vendor(root, name: str, source, *, allowed_roots=None, denied_paths=()) -> dict:
    """Validate and vendor one declared B-class package into packages/<name>.

    Allows declaring multiple missing packages and vendoring them one by one,
    and succeeds even when A-class installed dependencies are not yet installed.
    Preserves existing content on failure or concurrent edit.
    """
    root = no_symlinks(Path(root).absolute())
    config = declaration(root)
    if config is None:
        raise ForgeError('Declare [tool.demiflow.packages] in pyproject.toml first')
    if name not in config['packages']:
        raise ForgeError(f'Package {name!r} is not declared in [tool.demiflow.packages]', code='not_found')
    item = config['packages'][name]
    if item['source'] != 'vendored':
        raise ForgeError(f'Package {name!r} is declared as {item["source"]!r}; vendor requires source = "vendored"')

    source_path = authorize_external_path(root, Path(source).absolute(), allowed_roots=allowed_roots, denied_paths=denied_paths)
    # Reuse the single unified package validation rule BEFORE writing anything.
    scanned = validate_package_source(
        name,
        source_path,
        project_root=root,
        files_left=MAX_FILES,
        bytes_left=MAX_BYTES,
        check_imports=True,
        allowed_roots=allowed_roots,
        denied_paths=denied_paths,
        is_external=True,
    )
    # Also ensure total project budget across existing vendored/local_ref packages is respected.
    existing_files = 0
    existing_bytes = 0
    for other_name, other_item in sorted(config['packages'].items()):
        if other_name == name:
            continue
        other_is_ext = other_item['source'] == 'local_ref'
        other_path = Path(other_item['path']) if other_is_ext else (root / other_item['path'])
        if not other_path.exists():
            continue
        other_scanned = validate_package_source(
            other_name,
            other_path,
            project_root=root,
            files_left=MAX_FILES - existing_files,
            bytes_left=MAX_BYTES - existing_bytes,
            check_imports=False,
            allowed_roots=allowed_roots,
            denied_paths=denied_paths,
            is_external=other_is_ext,
        )
        existing_files += other_scanned['file_count']
        existing_bytes += other_scanned['bytes']
    if existing_files + scanned['file_count'] > MAX_FILES or existing_bytes + scanned['bytes'] > MAX_BYTES:
        raise ForgeError('Project dependency packages exceed the project file or byte budget', code='resource_limit')
    # Check module/package basename collisions within this package.
    module_files(root, {name: dict(path=item['path'], files=scanned['files'])})

    with control_lock(root):
        current = declaration(root)
        if current is None or current['sha256'] != config['sha256']:
            raise ForgeError('pyproject.toml changed concurrently during vendor; preserving existing content', code='inputs_changed')
        packages_dir = root / 'packages'
        if packages_dir.is_symlink():
            raise ForgeError('packages/ must be a regular directory, not a symlink', code='operation_rejected')
        ensure_directory(packages_dir)
        target = packages_dir / name
        if target.is_symlink():
            raise ForgeError(f'packages/{name} must not be a symlink', code='operation_rejected')
        staging = packages_dir / f'.vendor-{name}-{secrets.token_hex(6)}'
        backup = packages_dir / f'.vendor-backup-{name}-{secrets.token_hex(6)}'
        moved_existing = False
        try:
            staging.mkdir(mode=0o700)
            for rel, proof in sorted(scanned['files'].items()):
                raw = read_bounded(source_path / rel, FILE_BYTES)
                if hashlib.sha256(raw).hexdigest() != proof['sha256']:
                    raise ForgeError('Vendor source changed during copy', code='inputs_changed')
                ensure_directory((staging / rel).parent)
                publish_bytes(staging / rel, raw)
            # Re-verify declaration did not change before publishing into place.
            after_copy = declaration(root)
            if after_copy is None or after_copy['sha256'] != config['sha256']:
                raise ForgeError('pyproject.toml changed concurrently during vendor; preserving existing content', code='inputs_changed')
            if target.exists():
                os.replace(target, backup)
                moved_existing = True
            os.replace(staging, target)
            if moved_existing:
                shutil.rmtree(backup, ignore_errors=True)
            from .project_guard import record_authorized_tree_write
            record_authorized_tree_write(root, 'packages')
            record_authorized_tree_write(root, f'packages/{name}')
        except BaseException:
            if moved_existing and not target.exists() and backup.exists():
                try:
                    os.replace(backup, target)
                except OSError:
                    pass
            shutil.rmtree(staging, ignore_errors=True)
            raise
    return dict(
        status='vendored',
        package=name,
        path=item['path'],
        file_count=scanned['file_count'],
        bytes=scanned['bytes'],
        tree_sha256=scanned['tree_sha256'],
    )


def _remove_package_from_toml(raw_text: str, name: str) -> tuple[str, bool]:
    """Surgically remove one package entry from [tool.demiflow.packages] while preserving other sections."""
    lines = raw_text.splitlines(keepends=True)
    out = []
    section = ''
    skipping_subtable = False
    skipping_braces = 0
    removed = False
    entry_re = re.compile(rf'^\s*{re.escape(name)}\s*=')
    for line in lines:
        if skipping_braces > 0:
            code_part = line.split('#', 1)[0]
            skipping_braces += code_part.count('{') - code_part.count('}')
            if skipping_braces < 0:
                skipping_braces = 0
            continue
        stripped = line.strip()
        header = stripped.split('#', 1)[0].strip()
        if header.startswith('[') and header.endswith(']') and not header.startswith('[['):
            section = header[1:-1].strip()
            if section == f'tool.demiflow.packages.{name}':
                skipping_subtable = True
                removed = True
                continue
            skipping_subtable = False
            out.append(line)
            continue
        if skipping_subtable:
            continue
        if section == 'tool.demiflow.packages' and entry_re.match(line):
            removed = True
            code_part = line.split('#', 1)[0]
            delta = code_part.count('{') - code_part.count('}')
            if delta > 0:
                skipping_braces = delta
            continue
        out.append(line)
    updated = ''.join(out)
    if removed:
        parsed = tomllib.loads(updated)
        pkgs = (parsed.get('tool') or {}).get('demiflow', {}).get('packages')
        if isinstance(pkgs, dict) and name in pkgs:
            raise ForgeError(f'Unable to cleanly remove {name} from pyproject.toml', code='conflict')
    return updated, removed


def remove(root, name: str) -> dict:
    """Remove a single declared package by name and delete ONLY its project-owned vendored copy."""
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        raise ForgeError('Invalid package name: ' + str(name)[:64], code='invalid_request')
    root = no_symlinks(Path(root).absolute())
    with control_lock(root):
        pyproject = root / 'pyproject.toml'
        config = declaration(root)
        declared_source = None
        removed_declaration = False
        if config is not None and name in config['packages']:
            declared_source = config['packages'][name]['source']
            updated_text, removed_declaration = _remove_package_from_toml(config['raw'], name)
            # Re-verify file bytes did not change concurrently before writing.
            current_raw = read_bounded(pyproject, MAX_DECLARATION_BYTES)
            if hashlib.sha256(current_raw).hexdigest() != config['sha256']:
                raise ForgeError('pyproject.toml changed concurrently during remove; preserving existing content', code='inputs_changed')
            if removed_declaration:
                atomic_write_text(pyproject, updated_text)
        removed_files = False
        # Never touch external local_ref directories — only project-owned packages/<name>.
        if declared_source != 'local_ref':
            vendor_dir = root / 'packages'
            target = vendor_dir / name
            if vendor_dir.is_dir() and not vendor_dir.is_symlink() and target.exists():
                if target.is_symlink():
                    raise ForgeError(f'Refusing to remove symlink under packages/: {name}', code='operation_rejected')
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
                removed_files = True
                from .project_guard import record_authorized_tree_write
                record_authorized_tree_write(root, 'packages')
                record_authorized_tree_write(root, f'packages/{name}')
        if not removed_declaration and not removed_files:
            raise ForgeError(f'Package {name!r} is neither declared nor present under packages/', code='not_found')
    return dict(
        status='removed',
        package=name,
        source=declared_source,
        removed_declaration=removed_declaration,
        removed_files=removed_files,
    )


def list_dependencies(root, *, python=None, allowed_roots=None, denied_paths=()) -> dict:
    """List declared A-class and B-class dependencies along with missing/invalid status."""
    inspected = inspect_dependencies(
        root,
        python=python,
        allowed_roots=allowed_roots,
        denied_paths=denied_paths,
        check_imports=False,
        check_environment=bool(python),
        allow_partial=True,
    )
    if not inspected['declared']:
        return dict(status='not_applicable', declared=False, dependencies=None)
    if inspected['declaration_error']:
        return dict(
            status='failed',
            declared=True,
            diagnostics=[dict(code='invalid_declaration', path='pyproject.toml', detail=inspected['declaration_error'])],
        )
    packages = {}
    for name, item in sorted(inspected['packages'].items()):
        packages[name] = dict(
            source=item['source'],
            status='present',
            file_count=item['file_count'],
            bytes=item['bytes'],
            tree_sha256=item['tree_sha256'],
        )
    for item in inspected['missing_packages']:
        packages[item['name']] = dict(source=item['source'], status='missing', detail=item['detail'])
    for item in inspected['invalid_packages']:
        packages[item['name']] = dict(source=item['source'], status='invalid', detail=item['detail'])
    return dict(
        status='passed',
        declared=True,
        dependencies=dict(
            digest=inspected['digest'],
            declaration=inspected['config']['sha256'],
            requirements=list(inspected['config']['requirements']),
            packages=packages,
            missing_packages=inspected['missing_packages'],
            invalid_packages=inspected['invalid_packages'],
            orphan_packages=inspected['orphan_packages'],
            missing_requirements=inspected['missing_requirements'],
            shadowed_packages=inspected['shadowed_packages'],
            environment_skipped=inspected['environment_skipped'],
            environment_error=inspected['environment_error'],
        ),
    )


def verify(root, *, python=None, allowed_roots=None, denied_paths=()) -> dict:
    """Verify pyproject.toml, all B-class packages, packages/ hygiene, and optional explicit Python environment."""
    inspected = inspect_dependencies(
        root,
        python=python,
        allowed_roots=allowed_roots,
        denied_paths=denied_paths,
        check_imports=True,
        check_environment=bool(python),
        allow_partial=True,
    )
    if not inspected['declared']:
        return dict(status='not_applicable', dependencies=None)
    diagnostics: list[dict] = []
    if inspected['declaration_error']:
        diagnostics.append(dict(code='invalid_declaration', path='pyproject.toml', detail=inspected['declaration_error']))
    for item in inspected['missing_packages']:
        diagnostics.append(dict(code='missing_package', path='pyproject.toml', line=item.get('line'), detail=item['detail']))
    for item in inspected['invalid_packages']:
        diagnostics.append(dict(code=item.get('code') or 'invalid_package', path='pyproject.toml', line=item.get('line'), detail=item['detail']))
    for item in inspected['orphan_packages']:
        diagnostics.append(dict(code='orphan_package', path=item['path'], detail=item['detail']))
    for item in inspected['module_collisions']:
        diagnostics.append(dict(code='module_collision', path='pyproject.toml', line=item.get('line'), detail=item['detail']))
    for item in inspected['missing_requirements']:
        diagnostics.append(dict(code='missing_requirement', path='pyproject.toml', line=item.get('line'), detail=item['detail']))
    for item in inspected['shadowed_packages']:
        diagnostics.append(dict(code='shadowed_package', path='pyproject.toml', line=item.get('line'), detail=item['detail']))
    if inspected['environment_error']:
        diagnostics.append(dict(code='environment_error', path='pyproject.toml', detail=inspected['environment_error']))
    status = 'failed' if diagnostics else 'passed'
    summary = None
    if inspected['config'] is not None:
        summary = dict(
            digest=inspected['digest'],
            declaration=inspected['config']['sha256'],
            requirements=list(inspected['config']['requirements']),
            packages={
                name: {k: item[k] for k in ('source', 'file_count', 'bytes', 'tree_sha256')}
                for name, item in sorted(inspected['packages'].items())
            },
            environment_skipped=inspected['environment_skipped'],
        )
    result = dict(status=status, dependencies=summary)
    if diagnostics:
        result['diagnostics'] = diagnostics
    if inspected['environment_skipped']:
        result['skipped_checks'] = ['未提供 --python，已跳过 A 类环境包版本与已安装模块撞名核验。']
    return result


def install(root, python) -> dict:
    """Install declared A-class requirements into an explicit separate user-managed Python environment."""
    from .runtime import probe_environment
    root = no_symlinks(Path(root).absolute())
    config = declaration(root)
    if config is None:
        raise ForgeError('Declare [tool.demiflow.packages] in pyproject.toml first')
    with control_lock(root):
        identity = probe_environment(python, config, identity_only=True)
        prefix = Path(identity['prefix']).resolve()
        if prefix in {Path(sys.prefix).resolve(), Path(sys.base_prefix).resolve()}:
            raise ForgeError('Dependency installation requires a separate user-managed environment')
        current = declaration(root)
        if current is None or current['sha256'] != config['sha256']:
            raise ForgeError('Dependency declaration changed before installation', code='inputs_changed')
        if config['requirements']:
            index_options = {
                'PIP_INDEX_URL', 'PIP_EXTRA_INDEX_URL', 'PIP_NO_INDEX', 'PIP_FIND_LINKS',
                'PIP_TRUSTED_HOST', 'PIP_CERT',
            }
            environment = {
                key: value for key, value in os.environ.items()
                if not key.startswith('PIP_') or key in index_options
            }
            environment['PIP_CONFIG_FILE'] = os.devnull
            try:
                subprocess.run(
                    [str(python), '-I', '-m', 'pip', '--disable-pip-version-check',
                     'install', '--no-input', '--', *config['requirements']],
                    env=environment,
                    timeout=300,
                    check=True,
                    stdout=2,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                raise ForgeError('Project dependency installation failed', code='operation_failed') from error
    return dict(status='installed', requirements=list(config['requirements']), python=str(python))
