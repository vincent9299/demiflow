"""Collect bounded, immutable review evidence without importing business code."""
from __future__ import annotations

import ast
from collections import deque
import hashlib
import os
from pathlib import Path
import re
import select
import subprocess
import time

from .io import no_symlinks
from .platform_environment import read_bounded
from .project import discover_project_sources, validate_entry, validate_parameter_path
from .project_io import relative_path
from .project_rules import ROLE_DIRS
from .schema import ForgeError, digest

MAX_FILES = 256
FILE_BYTES = 96 * 1024
TOTAL_BYTES = 8 * 1024**2
UNIT_FILES = 48
UNIT_BYTES = 224 * 1024
MAX_UNITS = 32
SCAN_BYTES = 16 * 1024**2
INDEX_BYTES = 256 * 1024
TEXT_SUFFIXES = {'.py', '.yaml', '.yml', '.json', '.md', '.txt', '.toml', '.rst'}
BULK_ROOTS = {'runs', 'outputs', 'artifacts', 'data', 'datasets', 'build', 'dist', 'venv'}
SKIP_DIRS = {'__pycache__', 'node_modules'}
PRIVATE_PART = re.compile(r'(?:^|[_.-])(?:auth|credentials?|secrets?|passwords?|private[_-]?key|api[_-]?key|tokens?)(?:$|[_.-])', re.I)


def _permitted(name, excluded):
    parts = Path(name).parts
    return (bool(parts) and parts[0] not in BULK_ROOTS and
            all(not part.startswith('.') and part not in SKIP_DIRS and not PRIVATE_PART.search(part)
                for part in parts)
            and not any(name == item or name.startswith(item + '/') for item in excluded))


def _check_path(root, name, excluded):
    path = relative_path(root, name)
    if (len(name.encode()) > 1024 or any(ord(char) < 32 for char in name)
            or path.suffix.lower() not in TEXT_SUFFIXES or not _permitted(name, excluded)):
        raise ForgeError('Selected evidence is outside the reviewable source scope: ' + name,
                         code='invalid_request')
    return path


def _git(root, *args, maximum=INDEX_BYTES, optional=False):
    """Read fixed Git metadata/blob commands; bound pipe output before accumulation."""
    command = ['git', '--no-optional-locks', '-c', 'core.fsmonitor=false', '-c', 'core.hooksPath=/dev/null',
               '-C', str(root), *args]
    env = dict(PATH=os.defpath, HOME='/nonexistent', GIT_CONFIG_NOSYSTEM='1',
               GIT_CONFIG_GLOBAL=os.devnull, GIT_NO_REPLACE_OBJECTS='1', LC_ALL='C')
    chunks = bytearray()
    deadline = time.monotonic() + 5
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL,
                          env=env) as proc:
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not select.select([proc.stdout], [], [], remaining)[0]:
                    raise ForgeError('Git evidence timed out', code='resource_limit')
                part = os.read(proc.stdout.fileno(), min(16384, maximum - len(chunks) + 1))
                if not part:
                    break
                chunks.extend(part)
                if len(chunks) > maximum:
                    raise ForgeError('Git evidence exceeds its byte limit; narrow the review scope',
                                     code='resource_limit')
            status = proc.wait(timeout=max(.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired as exc:
            raise ForgeError('Git evidence timed out', code='resource_limit') from exc
        finally:
            if proc.poll() is None:
                proc.kill()
    if status == 0:
        return bytes(chunks)
    if optional:
        return None
    raise ForgeError('Git could not read the selected review basis', code='inspection_incomplete')


def _revision(root, base):
    if not isinstance(base, str) or not base or len(base) > 256:
        raise ForgeError('Invalid Git review base', code='invalid_request')
    result = _git(root, 'rev-parse', '--verify', '--end-of-options', base + '^{commit}',
                  maximum=128, optional=True)
    if result is None or not re.fullmatch(rb'(?:[0-9a-f]{40}|[0-9a-f]{64})\n', result):
        raise ForgeError('Cannot resolve the Git review base; select a valid commit or an explicit scope',
                         code='invalid_request')
    return result.decode().strip()


def _baseline(root, base, excluded, selected):
    """Read blob identities only. Tree entries are relative to the selected project."""
    def tree(*args):
        raw = _git(root, '--literal-pathspecs', 'ls-tree', '-z', *args)
        for record in raw.split(b'\0'):
            if record:
                metadata, name = record.split(b'\t', 1)
                mode, kind, oid = metadata.split(b' ')
                yield name.decode(), mode, kind, oid.decode()

    top = list(selected) if selected else [name for name, _, _, _ in tree(base) if _permitted(name, excluded)]
    if not top:
        return {}
    if len(top) > 256:
        raise ForgeError('Git project inventory exceeds the review budget', code='resource_limit')
    return {name: oid for name, mode, kind, oid in tree('-r', base, '--', *top)
            if kind == b'blob' and mode in (b'100644', b'100755') and Path(name).suffix.lower() in TEXT_SUFFIXES
            and _permitted(name, excluded)}


def _changed_files(root, base, excluded):
    """Compare the visible working bytes with Git blobs. Never run clean/smudge filters."""
    prior = _baseline(root, base, excluded, ())
    omit = [':(exclude,literal)' + name for name in sorted(BULK_ROOTS | SKIP_DIRS | set(excluded))]
    listing = _git(root, 'ls-files', '-z', '--cached', '--others', '--exclude-standard',
                   '--', '.', ':(exclude,glob)**/.*', ':(exclude,glob)**/.*/**', *omit)
    names = set(prior)
    for raw in listing.split(b'\0'):
        name = raw.decode()
        if Path(name).suffix.lower() in TEXT_SUFFIXES and _permitted(name, excluded):
            names.add(name)
    if len(names) > 2048:
        raise ForgeError('Git source inventory exceeds the review budget', code='resource_limit')
    changed = []
    remaining = SCAN_BYTES
    for name in sorted(names):
        oid = prior.get(name)
        try:
            path = _check_path(root, name, excluded)
        except ForgeError:
            if any(part.is_symlink() for part in ((root / name), *(root / name).parents)):
                if oid:
                    raise ForgeError('Tracked file now resolves through a symlink: ' + name,
                                     code='inspection_incomplete')
                continue
            raise
        if oid is None or not path.exists():
            if oid or path.exists():
                changed.append(name)
            continue
        raw = read_bounded(path, min(remaining, 2 * 1024**2))
        remaining -= len(raw)
        hasher = hashlib.new('sha1' if len(oid) == 40 else 'sha256')
        hasher.update(f'blob {len(raw)}\0'.encode())
        hasher.update(raw)
        if hasher.hexdigest() != oid:
            changed.append(name)
    return changed, prior


class _ProjectEvidence:
    def __init__(self, root, excluded, deleted):
        self.deleted = deleted
        self.root = root
        self.excluded = excluded
        self.text = {}
        self.edges = {}
        self.gaps = {}
        self.scanned = 0
        self.lookups = 0

    def read(self, name):
        if name not in self.text:
            if len(self.text) >= 256:
                raise ForgeError('Source exploration exceeds its file limit', code='resource_limit')
            raw = read_bounded(_check_path(self.root, name, self.excluded), min(FILE_BYTES, SCAN_BYTES - self.scanned))
            self.scanned += len(raw)
            self.text[name] = raw.decode('utf-8')
        return self.text[name]

    def _note(self, file, detail):
        messages = self.gaps.setdefault(file, [])
        if len(messages) < 16 and detail not in messages:
            messages.append(detail)

    def _lookup(self, name):
        """Return a visible path, counting attempts independently of successful reads."""
        self.lookups += 1
        if self.lookups > 8192:
            raise ForgeError('Source lookup exceeds the review budget', code='resource_limit')
        if not _permitted(name, self.excluded):
            return None
        try:
            return relative_path(self.root, name)
        except ForgeError:
            # An import/literal is an untrusted hint. Never follow a linked path.
            return None

    def _import(self, file, line, parts, names, *, relative):
        """Resolve local modules without importing them or treating symbols as missing files."""
        if self.root.name in parts:
            end = parts.index(self.root.name) + 1
            if self.root.parts[-end:] == tuple(parts[:end]):
                relative = True
                parts = parts[end:]
        if not parts:
            return set()
        first = parts[0]
        path = '/'.join(parts)
        module_file = path + '.py'
        package_file = path + '/__init__.py'
        local = relative or first in ROLE_DIRS
        if not local:
            # Only inspect an external-looking import if the project visibly owns its root.
            local = any(candidate is not None and candidate.exists()
                        for candidate in (self._lookup(first), self._lookup(first + '.py')))
        if not local:
            return set()
        found = set()
        module = self._lookup(module_file)
        if module is not None and (module.is_file() or module_file in self.deleted):
            return found | {module_file}
        folder = self._lookup(path)
        init = self._lookup(package_file)
        if folder is None or not folder.is_dir():
            self._note(file, f'{file}:{line} referenced local module unavailable: {path}')
            return found
        if init is None:
            self._note(file, f'{file}:{line} local package initializer is outside the review scope: {package_file}')
        package_init = init is not None and init.is_file()
        if package_init:
            found.add(package_file)
        for name in names:
            if name == '*':
                continue
            child = path + '/' + name
            implementations = (child + '.py', child + '/__init__.py')
            matched = False
            inaccessible = False
            for implementation in implementations:
                candidate = self._lookup(implementation)
                if candidate is None:
                    inaccessible = True
                elif candidate.is_file() or implementation in self.deleted:
                    found.add(implementation)
                    matched = True
            # A package initializer may export an ordinary value with this name.
            namespace = self._lookup(child)
            if not matched and (inaccessible or not package_init) and (namespace is None or not namespace.is_dir()):
                self._note(file, f'{file}:{line} referenced local package member unavailable: {child}')
        return found

    def dependencies(self, name):
        if name in self.edges:
            return self.edges[name]
        found = set()
        path = Path(name)
        if name in self.deleted or path.suffix != '.py':
            return ()
        source = self.read(name)
        try:
            tree = ast.parse(source, name)
        except (SyntaxError, RecursionError, ValueError):
            self._note(name, 'Unable to parse imports in ' + name)
            self.edges[name] = []
            return ()
        for directory in path.parents:
            init = (directory / '__init__.py').as_posix()
            candidate = self._lookup(init)
            if candidate is not None and candidate.is_file():
                found.add(init)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    found.update(self._import(name, node.lineno, alias.name.split('.'), [], relative=False))
            elif isinstance(node, ast.ImportFrom):
                parents = list(path.parent.parts)
                if node.level > len(parents) + 1:
                    self._note(name, f'{name}:{node.lineno} relative import extends outside the project')
                    continue
                package = parents[:len(parents) + 1 - node.level] if node.level else []
                prefix = package + (node.module.split('.') if node.module else [])
                names = [item.name for item in node.names]
                if prefix:
                    found.update(self._import(name, node.lineno, prefix, names, relative=bool(node.level)))
                else:
                    # from . import x in a module at the project root
                    for member in names:
                        if member != '*':
                            found.update(self._import(name, node.lineno, [member], [], relative=True))
            elif isinstance(node, ast.Constant) and isinstance(node.value, str) and len(node.value) < 256 and node.value.isprintable():
                value = node.value
                if Path(value).suffix.lower() not in {'.yaml', '.yml', '.json'} or value.split('/') != list(Path(value).parts):
                    continue
                names = {value}
                if '/' not in value:
                    names.update((str(path.parent / value), 'prompts/' + value))
                for possible in names:
                    candidate = self._lookup(possible)
                    if candidate is not None and candidate.is_file():
                        found.add(possible)
        self.edges[name] = sorted(found - {name})
        return self.edges[name]

    def closure(self, names):
        seen = set()
        queue = deque(names)
        while queue:
            name = queue.popleft()
            if name not in seen:
                seen.add(name)
                queue.extend(self.dependencies(name))
        return seen


def capture_evidence(root, *, entry=None, params=None, paths=(), base=None, evidence=(), exclude=(),
                     prior_sources=None):
    """Capture actual candidate bytes, their preimages and explicitly attached evidence."""
    root = no_symlinks(Path(root).absolute())
    if '..' in root.parts or not root.is_dir() or entry and paths or params and not entry:
        raise ForgeError('Select a project directory and one valid review scope', code='invalid_request')
    if len(paths) + len(evidence) > MAX_FILES or len(exclude) > 256:
        raise ForgeError('Review path list exceeds its limit', code='resource_limit')
    excluded = tuple(exclude)
    for name in (*paths, *evidence):
        _check_path(root, name, excluded)
    if prior_sources is not None:
        if not paths or base or not isinstance(prior_sources, dict) or set(prior_sources) != set(paths):
            raise ForgeError('Observed changes require a before/after basis for every selected path',
                             code='invalid_request')
        total = 0
        for row in prior_sources.values():
            if not isinstance(row, dict) or set(row) != {'before', 'after_sha256'}:
                raise ForgeError('Invalid observed source basis', code='invalid_request')
            before, after = row['before'], row['after_sha256']
            if before is not None:
                if not isinstance(before, str) or len(before.encode()) > FILE_BYTES:
                    raise ForgeError('Observed source preimage exceeds budget', code='resource_limit')
                total += len(before.encode())
            if after is not None and (not isinstance(after, str) or not re.fullmatch('[0-9a-f]{64}', after)):
                raise ForgeError('Invalid observed source identity', code='invalid_request')
        if total > TOTAL_BYTES:
            raise ForgeError('Observed source basis exceeds total review budget', code='resource_limit')
    if entry:
        target, symbol = validate_entry(root, entry)
        filename = target.relative_to(root).as_posix()
        _check_path(root, filename, excluded)
    if params:
        validate_parameter_path(root, params)
        _check_path(root, params, excluded)
    mode = 'entry' if entry else 'paths' if paths else 'changes'
    revision = _revision(root, base or 'HEAD') if base is not None or mode == 'changes' else None
    if prior_sources is not None:
        mode = 'observed_changes'
    if not revision and mode == 'paths' and any(not (root / name).exists() for name in paths):
        revision = _revision(root, 'HEAD')
    explicit = [filename] if entry else list(dict.fromkeys(paths))
    if mode == 'changes':
        targets, history = _changed_files(root, revision, excluded)
        if not targets:
            raise ForgeError('No source changes; select an entry, path or another base', code='invalid_request')
    else:
        targets = explicit
        history = _baseline(root, revision, excluded, targets) if revision else {}

    view = discover_project_sources(root, exclude=excluded, include_roles=False)
    entries = view['entries']
    known_before = history.keys() | (prior_sources or {}).keys()
    deleted = {name for name in targets if name in known_before and not (root / name).exists()}
    project = _ProjectEvidence(root, excluded, deleted)
    related = set()
    groups = []
    for name in targets:
        if (root / name).exists():
            closure = project.closure([name])
            related.update(closure)
            groups.append(sorted(closure))
        elif name not in known_before:
            raise ForgeError('Review target not found: ' + name, code='invalid_request')
    if entry:
        try:
            tree = ast.parse(project.read(filename), filename)
        except (SyntaxError, RecursionError, ValueError) as exc:
            raise ForgeError('Cannot parse selected entry: ' + entry, code='inspection_incomplete') from exc
        if not any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == symbol
                   for node in tree.body):
            raise ForgeError('Review entry symbol not found: ' + entry, code='invalid_request')
        selected_entries = [entry]
        groups = [sorted(related | set(targets) | ({params} if params else set(entries.get(entry, ()))))]
    else:
        selected_entries = []
        for declared, bindings in entries.items():
            flow = declared.partition(':')[0]
            if not (root / flow).is_file():
                continue
            dependencies = project.closure([flow])
            if dependencies.intersection(targets) or set(bindings).intersection(targets):
                selected_entries.append(declared)
                related.update(dependencies)
                groups.insert(0, sorted(dependencies | set(bindings)))
    selected_params = {params} if params else {p for key in selected_entries for p in entries.get(key, ())}
    related.update(selected_params)
    for name in targets:
        if name.endswith('.py'):
            test = 'tests/test_' + Path(name).stem + '.py'
            if _permitted(test, excluded) and relative_path(root, test).is_file():
                related.add(test)
    roles = dict.fromkeys(sorted(related), 'dependency')
    roles.update(dict.fromkeys(evidence, 'evidence'))
    roles.update(dict.fromkeys(targets, 'target'))
    if params:
        roles[params] = 'target'
    if len(roles) > MAX_FILES:
        raise ForgeError('Selected evidence exceeds its file budget; narrow the review scope', code='resource_limit')
    if revision:
        history.update(_baseline(root, revision, excluded, roles))
    files = {}
    remaining = TOTAL_BYTES
    changed = []
    for name, role in sorted(roles.items()):
        path = _check_path(root, name, excluded)
        after = project.read(name) if path.exists() else None
        before = None
        if revision and name in history:
            before = _git(root, 'cat-file', 'blob', history[name], maximum=FILE_BYTES).decode('utf-8')
        observed = (prior_sources or {}).get(name)
        if observed is not None:
            current = hashlib.sha256(after.encode()).hexdigest() if after is not None else None
            if current != observed['after_sha256']:
                raise ForgeError('Source changed since the observed edit: ' + name, code='inputs_changed')
            before = observed['before']
        if (revision or observed is not None) and after != before:
            changed.append(name)
        if before == after:
            before = None
        if after is None and before is None:
            raise ForgeError('Selected material is missing: ' + name, code='invalid_request')
        remaining -= sum(len(value.encode()) for value in (before, after) if value is not None)
        if remaining < 0:
            raise ForgeError('Evidence exceeds its total byte limit; narrow the review scope', code='resource_limit')
        files[name] = dict(role=role, after=after, before=before)
    scope = dict(mode=mode, base=revision, entry=entry, params=params, paths=list(paths),
                 entries=selected_entries, changed=changed, evidence=list(evidence))
    result = dict(scope=scope, files=files, groups=groups,
                  gaps=[detail for name in files for detail in project.gaps.get(name, ())])
    result['identity'] = digest(result)
    return result


def verify_evidence(root, captured, *, exclude=(), prior_sources=None):
    """Recheck the fixed baseline and every selected working file before returning findings."""
    scope = captured['scope']
    try:
        current = capture_evidence(root, entry=scope['entry'], params=scope['params'], paths=scope['paths'],
                                   base=scope['base'], evidence=scope['evidence'], exclude=exclude,
                                   prior_sources=prior_sources)
    except (OSError, ValueError, RecursionError):
        return False
    return current['identity'] == captured['identity']


def review_units(captured):
    """Partition a fixed candidate without dropping a target or splitting its dependency closure."""
    files = captured['files']
    targets = {name for name, item in files.items() if item['role'] == 'target'}
    attachments = {name for name, item in files.items() if item['role'] == 'evidence'}
    grouped = {name for group in captured.get('groups', ()) for name in group}
    attachments.update(files.keys() - grouped - targets)
    units, covered = [], set()

    def fits(names):
        return len(names) <= UNIT_FILES and sum(
            len(value.encode()) for name in names for side in ('before', 'after')
            if (value := files[name].get(side)) is not None) <= UNIT_BYTES

    def add(names):
        names = set(names) & files.keys() | attachments
        if not fits(names):
            raise ForgeError('A Flow dependency closure exceeds one review unit; narrow or simplify the source: '
                             + ', '.join(sorted(names)[:4]), code='resource_limit')
        units.append(names)
        covered.update(names & targets)

    # Flow groups precede individual target closures. Shared sources remain in
    # every affected Flow; unregistered sources and deleted files remain targets.
    for index, group in enumerate(captured.get('groups', ())):
        names = set(group) & files.keys()
        if index >= len(captured['scope']['entries']) and not (names & (targets - covered)):
            continue
        add(names)
    for name in sorted(targets - covered):
        add({name})
    # Pack independent tiny groups while retaining every dependency and an
    # achievable number of cited targets. Large Flow groups stay intact.
    packed = []
    for names in units:
        if packed and fits(packed[-1] | names) and len((packed[-1] | names) & targets) <= 12:
            packed[-1].update(names)
        else:
            packed.append(set(names))
    if len(packed) > MAX_UNITS:
        raise ForgeError('Review unit count exceeds the delivery budget', code='resource_limit')
    for names in packed:
        yield dict(scope=captured['scope'], files={name: files[name] for name in sorted(names)},
                   gaps=captured['gaps'], identity=captured['identity'])
