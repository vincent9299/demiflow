"""Bounded local static facts used by candidate.9 snapshots and checks.

No candidate imports, installed-module probing, execution or persistent cache.
Only declared local paths and statically named project-local dependencies enter
this observation. External imports are explicitly outside the proof.
"""
from __future__ import annotations

import ast
from collections import deque
from dataclasses import dataclass
import hashlib
import io
import os
from pathlib import Path
import stat
import time
import tokenize

from .project import (ForgeError, MAX_FILE, MAX_TOTAL, PROJECT_LOCAL_STANDARDS,
                      SKIP, file_identity, no_symlinks, relative, snapshot_limits, within)

RUNTIME_NAMES = frozenset({'runs', 'datasets'})
PROTECTED_NAMES = SKIP - RUNTIME_NAMES
AST_LIMIT = 200000
METADATA_LIMIT = 16 * 1024**2


def bounded_bytes(path, maximum, *, expected=None, deadline=None):
    no_symlinks(path)
    before = path.stat()
    identity = file_identity(before)
    if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
        raise ForgeError(f'Source exceeds byte budget or is not a regular file: {path}')
    if expected is not None and identity != expected:
        raise ForgeError(f'Source changed before context/inspection: {path}')
    if deadline is not None and time.monotonic() > deadline:
        raise ForgeError('Source inspection time budget exceeded')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        if file_identity(os.fstat(stream.fileno())) != identity:
            raise ForgeError(f'Source changed before reading: {path}')
        data = stream.read(before.st_size + 1)
        if len(data) != before.st_size or file_identity(os.fstat(stream.fileno())) != identity:
            raise ForgeError(f'Source changed during reading: {path}')
    no_symlinks(path)
    if file_identity(path.stat()) != identity:
        raise ForgeError(f'Source changed during reading: {path}')
    return data, identity


def module_name(path):
    parts = path.removesuffix('.py').split('/')
    return '.'.join(parts[:-1] if parts[-1] == '__init__' else parts)


@dataclass
class Observation:
    tree: dict
    readonly: dict
    sources: dict
    coverage: dict | None = None
    facts: object = None
    identities: dict | None = None

    def stable(self):
        if self.facts is not None:
            self.facts.stable()
        for path, expected in (self.identities or {}).items():
            if self.facts is not None and time.monotonic() > self.facts.deadline:
                raise ForgeError('Source observation time budget exceeded')
            if isinstance(expected, tuple) and len(expected) == 2 and expected[0] == 'symlink':
                no_symlinks(path.parent)
                actual = file_identity(path.lstat()) if path.is_symlink() else None
                if actual != expected[1]:
                    raise ForgeError(f'Symlink changed during observation: {path}')
                continue
            no_symlinks(path)
            actual = file_identity(path.stat()) if path.exists() else None
            if actual != expected:
                raise ForgeError(f'Source changed during observation: {path}')


class SourceFacts:
    def __init__(self, project, *, deadline=None):
        self.project, self.root = project, project.root
        self.limits = snapshot_limits(project.config.get('snapshot_limits'))
        self.maximum = min(20000, self.limits['entries'])
        self.deadline = deadline or time.monotonic() + self.limits['seconds']
        self.inspection_deadline = min(self.deadline, time.monotonic() + 120)
        self.units, self.metadata_bytes = set(), 0
        self.meta, self.parsed, self.modules = {}, {}, {}
        self.pending, self.scheduled = deque(), set()
        self.required, self.subtrees, self.hashes = set(), set(), {}
        self.seeds, self.entry_owners, self.operators = [], {}, []
        self.problems, self.external, self.unwatched = set(), set(), set()
        self.bytes = self.references = 0
        self.symbol_cache = {}
        self.locals, self.modern, self.operator_set = set(), [], set()
        for name, pipeline in project.config['pipelines'].items():
            self.charge(('registration', name))
            entry = project.entry_path(name)
            self.charge(('entry', entry))
            self.entry_owners[module_name(entry)] = pipeline['path']
            self.locals.add(pipeline['path'].split('/')[0])
            self.locals.add(entry.split('/')[0])
            for operator in pipeline.get('operators', [pipeline['path'] + '/operators']):
                self.charge(('operator', operator))
                self.locals.add(operator.split('/')[0])
                if operator not in self.operator_set:
                    self.operator_set.add(operator)
                    self.operators.append(operator)
            if project.standard(name) not in PROJECT_LOCAL_STANDARDS:
                continue
            self.modern.append(name)
            self.seed(entry, False, module_name(entry))
            for path in pipeline['operators']:
                self.seed(path, True, module_name(entry), subtree=True)
            for path in pipeline['notebooks']:
                self.seed(path, False, module_name(entry))
        while self.pending:
            path = self.pending.popleft()
            record = self.parse(path)
            if record is None:
                continue
            for edge in record['imports']:
                self.resolve_edge(record, edge)
        for name in self.modern:
            record = self.parsed.get(project.entry_path(name))
            if record is not None and record['functions'].count(project.pipeline(name)['entry']['symbol']) != 1:
                self.problem('DF-02 entry requires exactly one registered top-level function')
        self.directions()
        self.stable()

    def check(self):
        if time.monotonic() > self.inspection_deadline:
            raise ForgeError('Registered source inspection time budget exceeded')

    def charge(self, key):
        self.check()
        if key in self.units:
            return
        if len(self.units) >= self.maximum:
            raise ForgeError('Registered source inspection entry budget exceeded')
        size = sum(len(str(part).encode('utf-8')) for part in key)
        if self.metadata_bytes + size > METADATA_LIMIT:
            raise ForgeError('Source coverage metadata byte budget exceeded')
        self.units.add(key)
        self.metadata_bytes += size

    def problem(self, message):
        if len(self.problems) >= 256 and message not in self.problems:
            raise ForgeError('Registered source diagnostic budget exceeded')
        self.problems.add(message)

    def watched(self, path):
        return any(within(path, scope) for scope in self.project.config['watch'])

    def need(self, path):
        relative(path, modern=True)
        if self.watched(path):
            self.charge(('required', path))
            self.required.add(path)

    def probe(self, path):
        relative(path, modern=True)
        if path not in self.meta:
            self.charge(('path', path))
            target = no_symlinks(self.root / path)
            try:
                info = target.stat()
                kind = 'directory' if stat.S_ISDIR(info.st_mode) else 'file' if stat.S_ISREG(info.st_mode) else 'special'
                self.meta[path] = (kind, file_identity(info))
            except (FileNotFoundError, NotADirectoryError):
                self.meta[path] = (None, None)
        return self.meta[path][0]

    def schedule(self, path):
        self.need(path)
        if path not in self.scheduled:
            self.charge(('pending', path))
            self.scheduled.add(path)
            self.pending.append(path)

    def seed(self, path, operator, entry, *, subtree=False):
        relative(path, modern=True)
        self.need(path)
        kind = self.probe(path)
        if kind == 'directory':
            if subtree:
                self.subtrees.add(path)
            self.walk_author(path, operator, entry)
        else:
            self.charge(('seed', path, operator, entry))
            self.seeds.append((path, operator, entry))
            self.schedule(path)

    def walk_author(self, directory, operator, entry):
        if len(Path(directory).parts) > 64:
            raise ForgeError('Registered source depth budget exceeded')
        self.charge(('author-directory', directory))
        self.need(directory)
        before = self.meta[directory][1] if directory in self.meta else None
        if before is None:
            self.probe(directory)
            before = self.meta[directory][1]
        with os.scandir(self.root / directory) as children:
            for child in children:
                self.charge(('author-child', directory, child.name))
                if child.name in PROTECTED_NAMES:
                    continue
                path = directory + '/' + child.name
                kind = self.probe(path)
                if kind == 'directory':
                    self.walk_author(path, operator, entry)
                elif kind == 'special':
                    raise ForgeError(f'Special file in registered source subtree: {path}')
                elif path.endswith('.py'):
                    self.charge(('seed', path, operator, entry))
                    self.seeds.append((path, operator, entry))
                    self.schedule(path)
        if file_identity((self.root / directory).stat()) != before:
            raise ForgeError(f'Source directory changed during inspection: {directory}')

    def ast_facts(self, text, path, record, *, cell=None):
        # A token gate precedes ast.parse's allocations; the AST gate remains.
        for count, _ in enumerate(tokenize.generate_tokens(io.StringIO(text).readline), 1):
            self.check()
            if count > AST_LIMIT:
                raise ForgeError('Registered Python token/AST budget exceeded')
        tree = ast.parse(text, filename=path)
        for count, node in enumerate(ast.walk(tree), 1):
            self.check()
            if count > AST_LIMIT:
                raise ForgeError('Registered Python AST budget exceeded')
            if cell is not None and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self.problem(f'DF-02 registered notebook {path} cell {cell} defines execution code')
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                self.references += len(node.names) + 1
                if self.references > self.maximum:
                    raise ForgeError('Registered import reference budget exceeded')
                edge = {'module': node.module or '' if isinstance(node, ast.ImportFrom) else '',
                        'level': node.level if isinstance(node, ast.ImportFrom) else 0,
                        'kind': 'from' if isinstance(node, ast.ImportFrom) else 'import',
                        'names': [(a.name, a.asname) for a in node.names], 'line': node.lineno}
                record['imports'].append(edge)
        for node in tree.body:
            names = []
            binding = ('static',)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names = [node.name]
                if isinstance(node, ast.FunctionDef):
                    record['functions'].append(node.name)
                if node.name == '__getattr__':
                    record['dynamic'] = True
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                if isinstance(node, ast.AnnAssign) and node.value is None:
                    continue
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                # Attribute/subscript mutation does not bind its receiver at module scope.
                stack = list(targets)
                while stack:
                    target = stack.pop()
                    if isinstance(target, ast.Name):
                        names.append(target.id)
                    elif isinstance(target, (ast.Tuple, ast.List)):
                        stack.extend(target.elts)
                    elif isinstance(target, ast.Starred):
                        stack.append(target.value)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    key = alias.asname or alias.name.split('.')[0]
                    self.charge(('symbol', path, key))
                    record['symbols'][key] = ('module', alias.name if alias.asname else key)
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name == '*':
                        record['dynamic'] = True
                        continue
                    key = alias.asname or alias.name
                    self.charge(('symbol', path, key))
                    record['symbols'][key] = ('from', node.module or '', node.level, alias.name)
            for name in names:
                self.charge(('symbol', path, name))
                record['symbols'][name] = binding
        del tree

    def parse(self, path):
        if path in self.parsed:
            return self.parsed[path]
        kind = self.probe(path)
        if not self.watched(path):
            self.unwatched.add(path)
            self.problem(f'DF-01 imported source is not watched: {path}')
            return None
        if kind != 'file' or not path.endswith(('.py', '.ipynb')):
            self.problem(f'DF-02 expected Python source: {path}')
            return None
        record = dict(path=path, imports=[], symbols={}, functions=[], dynamic=False,
                      dependencies=set(), targets=set())
        self.parsed[path] = record
        try:
            if path.endswith('.py'):
                if self.bytes + self.meta[path][1][2] > min(MAX_TOTAL, self.limits['total_bytes']):
                    raise ForgeError('Registered Python source byte budget exceeded')
                raw, identity = bounded_bytes(self.root / path, min(MAX_FILE, self.limits['file_bytes']),
                                                expected=self.meta[path][1], deadline=self.inspection_deadline)
                if self.bytes + len(raw) > min(MAX_TOTAL, self.limits['total_bytes']):
                    raise ForgeError('Registered Python source byte budget exceeded')
                self.bytes += len(raw)
                self.hashes[path] = hashlib.sha256(raw).hexdigest()
                self.ast_facts(raw.decode('utf-8'), path, record)
                del raw
                parts = module_name(path).split('.')
                if all(p.isidentifier() for p in parts):
                    module = self.module('.'.join(parts), force=True)
                    if module:
                        record['dependencies'].update(p for p in module['files'] if p != path)
            else:
                from .notebook import read_cells
                remaining = min(MAX_TOTAL, self.limits['total_bytes']) - self.bytes
                if remaining <= 0:
                    raise ForgeError('Registered Python/notebook source byte budget exceeded')
                cells = read_cells(self.root / path, max_bytes=self.limits['notebook_bytes'],
                                   code_bytes=min(1024**2, remaining))
                for index, cell in enumerate(cells):
                    if cell['cell_type'] != 'code':
                        continue
                    code = '\n'.join('' if line.lstrip().startswith(('%', '!')) else line
                                     for line in ''.join(cell['source']).splitlines())
                    self.bytes += len(code.encode())
                    if self.bytes > min(MAX_TOTAL, self.limits['total_bytes']):
                        raise ForgeError('Registered notebook/Python source byte budget exceeded')
                    self.ast_facts(code, path, record, cell=index)
                if file_identity((self.root / path).stat()) != self.meta[path][1]:
                    raise ForgeError(f'Notebook changed during inspection: {path}')
        except (SyntaxError, UnicodeError, tokenize.TokenError) as error:
            self.problem(f'DF-02 invalid Python {path}: {error}')
        return record

    def local_evidence(self, name):
        parts = name.split('.')
        if parts[0] in self.locals:
            return True
        # Classify from safe, bounded prefixes before rejecting a local target.
        # A restricted/deep suffix cannot erase an already known local identity.
        for index, part in enumerate(parts[:64], 1):
            if not part.isidentifier() or part in PROTECTED_NAMES:
                break
            stem = '/'.join(parts[:index])
            if self.probe(stem + '.py') == 'file':
                return True
            if self.probe(stem) == 'directory' and self.probe(stem + '/__init__.py') == 'file':
                return True
        return False

    def module(self, name, *, force=False):
        if name in self.modules:
            return self.modules[name]
        if not force and not self.local_evidence(name):
            return None
        self.charge(('module', name))
        parts = name.split('.')
        reason = ('local module depth budget exceeded' if len(parts) > 64 else
                  'protected local module path' if any(p in PROTECTED_NAMES for p in parts) else
                  'unsupported local module identity' if any(not p.isidentifier() for p in parts) else None)
        if reason:
            self.problem(f'DF-03 {reason}: {name}')
            self.modules[name] = None
            return None
        files = []
        for index in range(1, len(parts) + 1):
            stem = '/'.join(parts[:index])
            file = stem + '.py'
            file_kind, directory_kind = self.probe(file), self.probe(stem)
            initializer = stem + '/__init__.py'
            init_kind = self.probe(initializer) if directory_kind == 'directory' else None
            if file_kind == 'special' or directory_kind == 'special' or init_kind == 'special':
                raise ForgeError(f'Special file in module path: {name}')
            if file_kind == 'file' and init_kind == 'file':
                self.problem(f'DF-03 ambiguous local module/package: {name}')
                for item in (file, initializer):
                    self.schedule(item)
                self.modules[name] = None
                return None
            if index < len(parts):
                if file_kind == 'file':
                    self.schedule(file)
                    self.problem(f'DF-03 local module is not a package: {name}')
                    self.need('/'.join(parts) + '.py')
                    self.modules[name] = None
                    return None
                if directory_kind != 'directory':
                    self.need(stem)
                    self.need('/'.join(parts) + '.py')
                    self.problem(f'DF-03 missing local import module: {name}')
                    self.modules[name] = None
                    return None
                self.need(stem)
                if init_kind == 'file':
                    files.append(initializer)
                    self.schedule(initializer)
                continue
            if init_kind == 'file':
                self.need(stem)
                files.append(initializer)
                self.schedule(initializer)
                result = dict(name=name, kind='package', path=initializer, files=files)
            elif file_kind == 'file':
                files.append(file)
                self.schedule(file)
                result = dict(name=name, kind='module', path=file, files=files)
            elif directory_kind == 'directory':
                self.need(stem)
                result = dict(name=name, kind='namespace', path=None, files=files)
            else:
                self.need(file)
                self.problem(f'DF-03 missing local import module: {name}')
                result = None
            self.modules[name] = result
            return result

    def prefix(self, record, module, level):
        if not level:
            return module
        if record['path'].endswith('.ipynb'):
            self.problem(f'DF-03 unsupported relative notebook import: {record["path"]}')
            return None
        package = record['path'].split('/')[:-1]
        if level > len(package):
            self.problem(f'DF-03 relative import escapes project: {record["path"]}')
            return None
        return '.'.join(package[:len(package) - level + 1] + ([module] if module else []))

    def from_symbol(self, module, name, chain=()):
        key = (module['name'], name)
        if key in self.symbol_cache:
            return self.symbol_cache[key]
        if key in chain or len(chain) >= 64:
            self.problem(f'DF-03 unresolved local re-export cycle: {module["name"]}.{name}')
            return False
        self.charge(('resolution', *key))
        record = self.parse(module['path']) if module['path'] else None
        binding = record['symbols'].get(name) if record else None
        if binding and binding[0] in {'static', 'module'}:
            value = True
        elif binding:
            base = self.prefix(record, binding[1], binding[2])
            if base == module['name'] and binding[3] == name and module['kind'] == 'package':
                value = self.module(base + '.' + name, force=True) is not None
            else:
                provider = self.module(base, force=bool(binding[2])) if base else None
                value = self.from_symbol(provider, binding[3], (*chain, key)) if provider else base is not None and not self.local_evidence(base)
        elif record and record['dynamic']:
            self.problem(f'DF-03 unsupported dynamic local export: {module["name"]}.{name}')
            value = False
        elif module['kind'] in {'package', 'namespace'}:
            value = self.module(module['name'] + '.' + name, force=True) is not None
        else:
            self.problem(f'DF-03 missing local import symbol: {module["name"]}.{name}')
            value = False
        self.symbol_cache[key] = value
        return value

    def target(self, record, module, line):
        if not module:
            return
        self.charge(('edge', record['path'], module['name'], line))
        record['dependencies'].update(module['files'])
        record['targets'].add((module['name'], line))

    def resolve_edge(self, record, edge):
        line = edge['line']
        if edge['kind'] == 'import':
            for name, _ in edge['names']:
                if any(name == e or name.startswith(e + '.') for e in self.entry_owners):
                    self.charge(('syntactic-entry-edge', record['path'], name, line))
                    record['targets'].add((name, line))
                module = self.module(name)
                if module:
                    self.target(record, module, line)
                elif name in self.modules:
                    self.problem(f'DF-03 unresolved local import in {record["path"]}:{line}: {name}')
                else:
                    self.external.add((record['path'], line, name))
            return
        prefix = self.prefix(record, edge['module'], edge['level'])
        if prefix is None:
            return
        if any(prefix == e or prefix.startswith(e + '.') for e in self.entry_owners):
            self.charge(('syntactic-entry-edge', record['path'], prefix, line))
            record['targets'].add((prefix, line))
        for name, _ in edge['names']:
            target = prefix + '.' + name
            if name != '*' and any(target == e or target.startswith(e + '.') for e in self.entry_owners):
                self.charge(('syntactic-entry-edge', record['path'], target, line))
                record['targets'].add((target, line))
        local = bool(edge['level']) or self.local_evidence(prefix)
        if not local:
            local = any(name != '*' and self.local_evidence(prefix + '.' + name) for name, _ in edge['names'])
        module = self.module(prefix, force=local) if prefix else None
        if module is None:
            if local:
                self.problem(f'DF-03 unresolved local import in {record["path"]}:{line}: {prefix}')
            else:
                self.external.add((record['path'], line, prefix))
            return
        self.target(record, module, line)
        for name, _ in edge['names']:
            if name == '*':
                continue
            self.from_symbol(module, name)
            child = self.modules.get(prefix + '.' + name)
            if child:
                self.target(record, child, line)

    def directions(self):
        pending, visited = deque(self.seeds), set()
        queued = set(self.seeds)
        while pending:
            path, operator, own_entry = pending.popleft()
            operator = operator or any(within(path, root) for root in self.operators)
            key = (path, operator, own_entry)
            if key in visited:
                continue
            self.charge(('direction', *key))
            visited.add(key)
            record = self.parsed.get(path)
            if record is None:
                continue
            owner = self.entry_owners[own_entry]
            ancestor_init = path.endswith('/__init__.py') and any(within(p, str(Path(path).parent))
                            for p in [owner, *self.operators])
            if not within(path, owner) and not any(within(path, p) for p in self.operators) and not ancestor_init:
                self.problem(f'DF-02 local dependency has no declared operator owner: {path}')
            for module, line in record['targets']:
                entries = {e for e in self.entry_owners if module == e or module.startswith(e + '.')}
                if entries - {own_entry}:
                    self.problem(f'DF-03 cross-entry import in {path}:{line}')
                if operator and entries:
                    self.problem(f'DF-03 operator imports entry in {path}:{line}')
            for dependency in sorted(record['dependencies']):
                target_entry = module_name(dependency) if dependency.endswith('.py') else ''
                if target_entry in self.entry_owners:
                    if operator:
                        self.problem(f'DF-03 operator imports entry in {path}: package initialization/dependency')
                    if target_entry != own_entry:
                        self.problem(f'DF-03 cross-entry import in {path}: package initialization/dependency')
                context = (dependency, operator, own_entry)
                if context not in queued:
                    self.charge(('direction-pending', *context))
                    queued.add(context)
                    pending.append(context)

    def stable(self):
        if time.monotonic() > self.deadline:
            raise ForgeError('Source observation time budget exceeded')
        for path, (_, expected) in self.meta.items():
            if time.monotonic() > self.deadline:
                raise ForgeError('Source observation time budget exceeded')
            target = no_symlinks(self.root / path)
            actual = file_identity(target.stat()) if target.exists() else None
            if actual != expected:
                raise ForgeError(f'Source changed during static observation: {path}')

    def summary(self):
        return {'policy': 'candidate.9-local-source-v1', 'scope': 'declared-local-and-supported-static-imports',
                'exact_sources': sorted(self.required), 'author_subtrees': sorted(self.subtrees),
                'unwatched_local_sources': sorted(self.unwatched), 'static_diagnostics': sorted(self.problems),
                'external_imports_uninspected': [dict(source=p, line=n, module=m) for p, n, m in sorted(self.external)],
                'limitations': ['No dynamic import, reflection, arbitrary file-read or runtime-effect proof']}
