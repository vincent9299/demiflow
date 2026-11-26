"""Project dependency declaration, bounded inventory and trusted control writes."""

import ast
from contextlib import contextmanager
from contextvars import ContextVar
import hashlib
import json
import keyword
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile

from packaging.requirements import InvalidRequirement, Requirement

from . import operation_budget
from .development_state import physical_root
from .io import ensure_directory, no_symlinks, publish_bytes
from .platform import bounded_json, content_digest
from .platform_environment import environment_inventory, read_bounded
from .project_io import control_lock, read_control
from .schema import ForgeError

try:
    import tomllib
except ImportError:
    import tomli as tomllib


LOCK = '.forge/deps.lock.json'
FORMAT = 'demiflow_project_dependencies_v1'
MAX_LOCK = 512 * 1024
MAX_FILES = 512
MAX_BYTES = 16 * 1024**2
FILE_BYTES = 512 * 1024
_RESERVED = frozenset({
    'demiflow', 'flows', 'operators', 'data_tools', 'tests', 'prompts', 'configs',
    'packages', 'notebooks', 'sitecustomize', 'usercustomize',
})
_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,63}\Z')
_HASH = re.compile(r'[0-9a-f]{64}\Z')
_HOST_APPROVAL = ContextVar('project_dependency_approval', default=None)
_ENV_LIMITS = dict(distributions=256, metadata_file_bytes=4 * 1024**2, metadata_bytes=16 * 1024**2)


def declaration(root):
    """Read the opted-in declaration. Other pyproject files keep their old meaning."""
    root = no_symlinks(Path(root).absolute())
    try:
        raw = read_bounded(root / 'pyproject.toml', 128 * 1024)
    except FileNotFoundError:
        return None
    try:
        value = tomllib.loads(raw.decode('utf-8'))
    except (UnicodeError, ValueError, RecursionError) as error:
        # A standalone project may use pyproject.toml for unrelated tooling.
        if b'tool.demiflow.packages' not in raw and not (root / LOCK).exists():
            return None
        raise ForgeError('Invalid dependency declaration in pyproject.toml') from error
    tools = value.get('tool', {})
    forge = tools.get('demiflow', {}) if isinstance(tools, dict) else {}
    if not isinstance(forge, dict) or 'packages' not in forge:
        return None
    packages = forge['packages']
    if type(packages) is not dict or len(packages) > 16:
        raise ForgeError('Expected at most 16 packages in [tool.demiflow.packages]')
    project = value.get('project', {})
    requirements = project.get('dependencies', []) if isinstance(project, dict) else None
    if type(requirements) is not list or len(requirements) > 256:
        raise ForgeError('Invalid [project].dependencies')
    try:
        if any(type(item) is not str or len(item) > 4096 for item in requirements):
            raise ValueError
        requirements = [str(Requirement(item)) for item in requirements]
    except (ValueError, InvalidRequirement) as error:
        raise ForgeError('Invalid PEP 508 project dependency') from error
    for name, item in packages.items():
        if (not _NAME.fullmatch(name) or keyword.iskeyword(name)
                or name in _RESERVED or name in sys.stdlib_module_names):
            raise ForgeError('Invalid or reserved dependency package: ' + name[:64])
        if type(item) is not dict or set(item) != {'source', 'path'} or type(item['path']) is not str:
            raise ForgeError('A dependency package requires source and path: ' + name)
        raw_path = item['path']
        path = Path(raw_path)
        if len(raw_path) > 4096 or '\0' in raw_path or str(path) != raw_path or '..' in path.parts:
            raise ForgeError('Invalid dependency package path: ' + name)
        if item['source'] == 'vendored':
            if path != Path('packages') / name:
                raise ForgeError('Vendored packages must use packages/' + name)
        elif item['source'] != 'local_ref' or not path.is_absolute() or path == root or root.is_relative_to(path):
            raise ForgeError('A local_ref must name a specific outside package directory: ' + name)
        elif path.is_relative_to(root):
            raise ForgeError('Project-owned package source must use the vendored declaration: ' + name)
    return dict(sha256=hashlib.sha256(raw).hexdigest(), packages=packages, requirements=requirements)


def _source(root, item):
    return no_symlinks(root / item['path'])


def _scan(name, source, *, files_left, bytes_left, check_imports=False):
    if not source.is_dir() or not (source / '__init__.py').is_file():
        raise ForgeError('Expected a regular Python package with __init__.py: ' + name)
    pending = [source]
    count = total = 0
    files = {}
    while pending:
        folder = pending.pop()
        operation_budget.check()
        before = folder.lstat()
        if not stat.S_ISDIR(before.st_mode):
            raise ForgeError('Non-directory in dependency package: ' + name)
        with os.scandir(folder) as entries:
            for entry in entries:
                count += 1
                rel = Path(entry.path).relative_to(source)
                if count > 512 or len(rel.parts) > 12:
                    raise ForgeError('Package tree exceeds its directory budget: ' + name, code='resource_limit')
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    if not _NAME.fullmatch(entry.name):
                        raise ForgeError('Invalid directory in dependency package: ' + str(rel))
                    pending.append(Path(entry.path))
                    continue
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or rel.suffix != '.py' or not _NAME.fullmatch(rel.stem):
                    raise ForgeError('Dependency packages accept only direct Python source files: ' + str(rel))
                if len(files) >= min(256, files_left) or total + info.st_size > min(8 * 1024**2, bytes_left):
                    raise ForgeError('Dependency package exceeds its file or byte budget: ' + name, code='resource_limit')
                raw = read_bounded(Path(entry.path), min(FILE_BYTES, min(8 * 1024**2, bytes_left) - total))
                total += len(raw)
                if check_imports:
                    _check_imports(raw, rel, name)
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
    return files, total


def _check_imports(raw, relative, package):
    try:
        tree = ast.parse(raw, filename=str(relative))
    except (SyntaxError, ValueError, RecursionError) as error:
        raise ForgeError('Invalid Python in dependency package: ' + str(relative)) from error
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names = [node.module]
        else:
            continue
        if any(name.partition('.')[0] in _RESERVED - {'sitecustomize', 'usercustomize'} for name in names):
            raise ForgeError(f'Dependency {package}/{relative}:{node.lineno} imports a project role or platform module')


def _inventory_packages(root, config, *, check_imports=False):
    result = {}
    left_files, left_bytes = MAX_FILES, MAX_BYTES
    for name, item in sorted(config['packages'].items()):
        path = _source(root, item)
        files, size = _scan(name, path, files_left=left_files, bytes_left=left_bytes, check_imports=check_imports)
        left_files -= len(files)
        left_bytes -= size
        result[name] = dict(**item, files=files, tree_sha256=content_digest(files))
    allowed = {name for name, item in config['packages'].items() if item['source'] == 'vendored'}
    vendor = root / 'packages'
    if vendor.exists() or vendor.is_symlink():
        with os.scandir(no_symlinks(vendor)) as entries:
            for index, entry in enumerate(entries):
                if index >= 16 or entry.name not in allowed or not entry.is_dir(follow_symlinks=False):
                    raise ForgeError('Unregistered content under packages/: ' + entry.name[:80])
    return result


def _interpreter(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or not path.is_file() or not os.access(path, os.X_OK):
        raise ForgeError('Configure an explicit executable project Python', code='invalid_request')
    info = path.stat()
    return dict(path=str(path), binary=str(path.resolve()), device=info.st_dev, inode=info.st_ino,
                bytes=info.st_size, mtime_ns=info.st_mtime_ns)


def interpreter_identity():
    return dict(**_interpreter(sys.executable), prefix=str(Path(sys.prefix).resolve()),
                base_prefix=str(Path(sys.base_prefix).resolve()))


def current_environment(config, *, check_conflicts=False):
    """Measure the current approved process environment, without importing business code."""
    from importlib.machinery import PathFinder
    descriptor = dict(mode='source', name='project', version='0', extras={},
                      requires=config['requirements'], project_sha256=content_digest(config['requirements']))
    inventory = environment_inventory(descriptor, _ENV_LIMITS)
    conflicts = [name for name in config['packages'] if check_conflicts
                 if name in sys.stdlib_module_names or name in sys.modules or PathFinder.find_spec(name) is not None]
    if conflicts:
        raise ForgeError('Dependency source shadows an installed or standard module: ' + conflicts[0], code='conflict')
    problems = [row for row in inventory['diagnostics'] if row['code'] != 'direct_requirement_not_attested']
    if problems:
        raise ForgeError('Project Python dependency verification failed: ' + problems[0]['name'], code='conflict')
    return dict(interpreter=interpreter_identity(), inventory=inventory)


def probe_environment(python, config, *, identity_only=False):
    """Inspect an explicitly trusted interpreter in a bounded independent process."""
    interpreter = _interpreter(python)
    script = (
        'import json,sys;data={"prefix":sys.prefix};'
        'print(json.dumps({"format":"dep_probe","data":data}))'
        if identity_only else
        'import json,sys;sys.path.insert(0,sys.argv[1]);'
        'from demiflow.forge.dependencies import current_environment;'
        'data=current_environment(json.load(sys.stdin),check_conflicts=True);'
        'print(json.dumps({"format":"dep_probe","data":data}))'
    )
    # This explicit project interpreter receives no controller channels or credentials.
    from .platform_worker import capture
    body = {'requirements': config['requirements'], 'packages': sorted(config['packages'])}
    result = capture(
        [interpreter['path'], '-I', '-B', '-c', script, str(Path(__file__).resolve().parents[2])],
        body, {'timeout_seconds': 30, 'catalog_bytes': MAX_LOCK, 'stderr_bytes': 8192},
        response_formats={'dep_probe'}, input_limit=128 * 1024, tree_rss_limit=512 * 1024**2,
        inherit_authority=False,
    )['data']
    if not identity_only and any(result['interpreter'].get(key) != val for key, val in interpreter.items()):
        raise ForgeError('Project interpreter supply differs from the inspected binary', code='conflict')
    return result


def _lock_view(root, config, approval):
    locked = read_control(root / LOCK, MAX_LOCK)
    if type(locked) is not dict or set(locked) != {'format', 'project', 'declaration', 'packages', 'environment', 'requirements', 'digest'}:
        raise ForgeError('Invalid dependency lock')
    digest = content_digest({k: v for k, v in locked.items() if k != 'digest'})
    if locked['format'] != FORMAT or digest != locked['digest'] or (approval is not None and digest != approval):
        raise ForgeError('Project dependency lock is unapproved or differs from the approved identity', code='conflict')
    if locked['project'] != physical_root(root) or locked['declaration'] != config['sha256']:
        raise ForgeError('Dependency declaration or project changed; refresh the lock', code='conflict')
    return locked


@contextmanager
def host_approval(root, digest, python):
    """Scope a trusted host's read admission to this physical project."""
    token = _HOST_APPROVAL.set((Path(root).absolute(), digest or '', python))
    try:
        yield
    finally:
        _HOST_APPROVAL.reset(token)


def binding(root, *, approval=None, python=None, verify_files=True):
    """Read the dependency binding, checking host approval before external files."""
    scoped = _HOST_APPROVAL.get()
    if scoped is not None:
        if Path(root).absolute() != scoped[0]:
            raise ForgeError('Dependency approval does not cover this project', code='operation_rejected')
        approval, python = scoped[1:]
    config = declaration(root)
    if config is None:
        if approval:
            raise ForgeError('Approved dependency declaration is absent', code='conflict')
        return None
    if approval is not None and (not isinstance(approval, str) or not _HASH.fullmatch(approval)):
        raise ForgeError('Project dependency approval is required', code='operation_rejected')
    root = Path(root).absolute()
    locked = _lock_view(root, config, approval)
    runtime = locked['environment']['interpreter']
    if python is not None and python != runtime['path']:
        raise ForgeError('Configured project Python differs from the approved dependency lock', code='conflict')
    if verify_files and locked['packages'] != _inventory_packages(root, config):
        raise ForgeError('Dependency source differs from its approved bytes', code='conflict')
    return locked


def facts(locked):
    if locked is None:
        return None
    return dict(digest=locked['digest'],
                distributions=[row['name'] for row in locked['environment']['inventory']['dependencies']],
                packages={name: {field: item[field] for field in ('source', 'tree_sha256')}
                          for name, item in locked['packages'].items()})


def module_files(root, locked):
    """Project an approved Python inventory onto exact module names and file paths."""
    result = {}
    if locked:
        for name, item in locked['packages'].items():
            for relative, proof in item['files'].items():
                path = Path(relative)
                parts = path.parent.parts if path.name == '__init__.py' else path.with_suffix('').parts
                qualified = '.'.join((name, *parts))
                if qualified in result:
                    raise ForgeError('Two files claim the same dependency module: ' + qualified, code='conflict')
                result[qualified] = dict(path=str(Path(root) / item['path'] / relative), **proof)
    return result


def update(root, python, *, name=None, vendor=None):
    """Verify the declaration and atomically publish its approved local inventory."""
    root = no_symlinks(Path(root).absolute())
    config = declaration(root)
    if config is None:
        raise ForgeError('Declare [tool.demiflow.packages] in pyproject.toml first')
    if name is not None and name not in config['packages']:
        raise ForgeError('Package is absent from the dependency declaration: ' + name)
    from .platform_environment import platform_declaration
    platform = platform_declaration(Path(__file__).resolve().parents[1], _ENV_LIMITS)
    requirements = list(dict.fromkeys([*platform['requires'], *config['requirements']]))
    environment = probe_environment(python, {**config, 'requirements': requirements})
    with control_lock(root):
        if declaration(root) != config:
            raise ForgeError('Dependency declaration changed during the update', code='inputs_changed')
        created = None
        try:
            if vendor is not None:
                item = config['packages'][name]
                if item['source'] != 'vendored':
                    raise ForgeError('Vendor requires a declared vendored package')
                source = no_symlinks(Path(vendor).absolute())
                files, _ = _scan(name, source, files_left=MAX_FILES, bytes_left=MAX_BYTES, check_imports=True)
                target = _source(root, item)
                ensure_directory(target.parent)
                target.mkdir(mode=0o700)
                created = target
                for rel, record in files.items():
                    raw = read_bounded(source / rel, FILE_BYTES)
                    if hashlib.sha256(raw).hexdigest() != record['sha256']:
                        raise ForgeError('Vendor source changed during the copy', code='inputs_changed')
                    publish_bytes(target / rel, raw)
            packages = _inventory_packages(root, config, check_imports=True)
            record = dict(format=FORMAT, project=physical_root(root), declaration=config['sha256'],
                          packages=packages, environment=environment, requirements=requirements)
            # Module-name collisions also include module/package basename pairs.
            module_files(root, record)
            record['digest'] = content_digest(record)
            if declaration(root) != config:
                raise ForgeError('Dependency declaration changed before publication', code='inputs_changed')
            lock = root / LOCK
            publish_bytes(lock, bounded_json(record, MAX_LOCK), create=not lock.exists())
        except BaseException:
            if created is not None:
                shutil.rmtree(created)
            raise
    return dict(status='updated', **facts(record))


def remove(root):
    """Revoke the local lock. Leave source, installations, and history in place."""
    root = no_symlinks(Path(root).absolute())
    from .io import directory_handle
    with control_lock(root), directory_handle(root / '.forge') as directory:
        try:
            os.unlink('deps.lock.json', dir_fd=directory)
        except FileNotFoundError:
            pass
        os.fsync(directory)
    return {'status': 'removed'}


def install(root, python):
    """Install approved A requirements in the explicit user environment."""
    config = declaration(root)
    if config is None:
        raise ForgeError('Declare [tool.demiflow.packages] in pyproject.toml first')
    with control_lock(root):
        identity = probe_environment(python, config, identity_only=True)
        prefix = Path(identity['prefix']).resolve()
        if prefix in {Path(sys.prefix).resolve(), Path(sys.base_prefix).resolve()}:
            raise ForgeError('Dependency installation requires a separate user-managed environment')
        if declaration(root) != config:
            raise ForgeError('Dependency declaration changed before installation', code='inputs_changed')
        if config['requirements']:
            subprocess.run([str(python), '-I', '-m', 'pip', '--disable-pip-version-check',
                            'install', '--no-input', '--', *config['requirements']], timeout=300, check=True)
    return {'status': 'installed'}
