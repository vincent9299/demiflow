"""Bounded, offline descriptions of two immutable Git source trees. No thresholds.

Import resolution/kinds and declaration closure adapt the original graph.py/facts.py:
  graph.py sha256 20946bdb810d96ec04e5e4f151e13229b9c7442b53d0c0833e6be929fa4d06f0
  facts.py sha256 3766c4112f227dd778679364fbeddb723335ff3723bb34b679d6cb25d78767c9
Their source location is recorded in method(); there is no runtime dependency on them.
Unlike those scripts this tool reads Git objects, rejects incomplete scans, uses an
iterative SCC walk and explicitly describes missing/acyclic anchors and unknowns.
Only this CLI process applies OS backup limits; importing its functions changes no limits.
"""
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time


VERSION = 'demiflow_architecture_diagnostics_v1'
REPORTS = ('scc-membership.json', 'declaration-upward-imports.json')
SHA = re.compile(r'(?:[0-9a-f]{40}|[0-9a-f]{64})\Z')
SYMBOL = re.compile(r'(demiflow(?:\.[A-Za-z_][A-Za-z0-9_]*)+)(?::([A-Za-z_][A-Za-z0-9_.]*))?\Z')
RUN_KINDS = {'top', 'guarded', 'lazy'}
ANCHORS = {'base': ('demiflow.execution.action_driver.dataflow', 'demiflow.execution.dataflow'),
           'workbench': ('demiflow.workbench.worker',)}
FIXED_SEEDS = ('demiflow.execution.contracts', 'demiflow.execution.recovery_contract', *(
    'demiflow.data.' + name for name in ('plan', 'sources', 'native_options', 'constraints',
        'dataflow_operation', 'runtime_contract', 'datasink', 'datasource', 'callable_contract',
        'records', 'execution_contract')))


@dataclass(frozen=True)
class Limits:
    modules: int = 4096
    blob_bytes: int = 2 * 1024**2
    source_bytes: int = 64 * 1024**2
    path_bytes: int = 1024
    metadata_entries: int = 20000
    metadata_bytes: int = 4 * 1024**2
    rows: int = 50000
    report_bytes: int = 8 * 1024**2
    stderr_bytes: int = 65536
    seconds: int = 60


class DiagnosticError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def error_record(exc):
    return {'code': getattr(exc, 'code', 'analysis_error'),
            'message': str(exc).encode('utf-8', 'replace')[:4096].decode('utf-8', 'replace')}


def requested_value(value):
    """Invalid input is evidence too, but must not expand a bounded failure receipt."""
    if not isinstance(value, str):
        return {'invalid_type': type(value).__name__}
    return value if len(value) <= 64 else {'prefix': value[:64], 'characters': len(value)}


class Budget:
    def __init__(self, limits=Limits(), clock=time.monotonic):
        if any(type(v) is not int or not 0 < v <= asdict(Limits())[k]
               for k, v in asdict(limits).items()):
            raise DiagnosticError('invalid_limits', 'Limits must be positive and at most the published defaults')
        self.limits, self.clock = limits, clock
        self.deadline = clock() + limits.seconds

    def check(self):
        if self.clock() >= self.deadline:
            raise DiagnosticError('deadline', 'Analysis software deadline exceeded')

    def admit(self, key, count):
        self.check()
        if count > getattr(self.limits, key):
            raise DiagnosticError('budget_' + key, f'{key}: {count} exceeds {getattr(self.limits, key)}')


def git_environment():
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    env.update(GIT_OPTIONAL_LOCKS='0', GIT_NO_REPLACE_OBJECTS='1',
               GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull)
    return env


class GitReader:
    """One serial Git child. All pipe reads, including headers/stderr, are bounded."""
    def __init__(self, repository, arguments, budget):
        self.budget = budget
        self.child = subprocess.Popen(['git', '--no-replace-objects', '-C', str(repository),
            *arguments], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=git_environment())
        self.selector = selectors.DefaultSelector()
        for stream, label in ((self.child.stdout, 'out'), (self.child.stderr, 'err')):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, label)
        self.buffer = bytearray()
        self.stderr = bytearray()

    def fill(self):
        self.budget.check()
        if not self.selector.get_map():
            raise DiagnosticError('git_error', 'Unexpected end of Git output: ' + self.stderr.decode(errors='replace'))
        for key, _ in self.selector.select(min(0.2, max(0, self.budget.deadline-self.budget.clock()))):
            chunk = os.read(key.fileobj.fileno(), 65536)
            if not chunk:
                self.selector.unregister(key.fileobj)
            elif key.data == 'out':
                self.buffer.extend(chunk)
            else:
                self.budget.admit('stderr_bytes', len(self.stderr)+len(chunk))
                self.stderr.extend(chunk)

    def exact(self, size):
        value = bytearray()
        while len(value) < size:
            self.budget.check()
            if not self.buffer:
                self.fill()
                continue
            count = min(size-len(value), len(self.buffer))
            value.extend(self.buffer[:count])
            del self.buffer[:count]
        return bytes(value)

    def line(self):
        while b'\n' not in self.buffer:
            if len(self.buffer) > 256:
                raise DiagnosticError('git_protocol', 'Git object header is too long')
            self.fill()
        end = self.buffer.index(b'\n') + 1
        if end > 256:
            raise DiagnosticError('git_protocol', 'Git object header is too long')
        return self.exact(end).decode('ascii').strip()

    def object(self, oid, maximum):
        self.budget.check()
        self.child.stdin.write((oid+'\n').encode('ascii'))
        self.child.stdin.flush()
        fields = self.line().split()
        if len(fields) != 3 or fields[0] != oid or not fields[2].isdigit():
            raise DiagnosticError('git_object', f'Cannot read fixed Git object {oid}: {fields}')
        size = int(fields[2])
        if size > maximum:
            raise DiagnosticError('object_size', f'Object {oid} declared {size} bytes exceeds {maximum}; body not admitted')
        value = self.exact(size)
        if self.exact(1) != b'\n':
            raise DiagnosticError('git_protocol', 'Missing object terminator')
        return fields[1], value

    def output(self, maximum):
        self.child.stdin.close()
        output = bytearray()
        while self.selector.get_map():
            self.budget.check()
            if self.buffer:
                if len(output)+len(self.buffer) > maximum:
                    raise DiagnosticError('budget_metadata_bytes', 'Git tree metadata exceeds byte limit')
                output.extend(self.buffer)
                self.buffer.clear()
            self.fill()
        if len(output)+len(self.buffer) > maximum:
            raise DiagnosticError('budget_metadata_bytes', 'Git tree metadata exceeds byte limit')
        output.extend(self.buffer)
        self.buffer.clear()
        if self.child.wait(timeout=1):
            raise DiagnosticError('git_error', self.stderr.decode(errors='replace'))
        return bytes(output)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.selector.close()
        if self.child.poll() is None:
            self.child.terminate()
            try:
                self.child.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait(timeout=1)
        for stream in (self.child.stdin, self.child.stdout, self.child.stderr):
            stream.close()


def fixed_inputs(repository, base, candidate, budget, inputs=None):
    inputs = {} if inputs is None else inputs
    with GitReader(repository, ['cat-file', '--batch'], budget) as reader:
        for label, oid in (('base', base), ('candidate', candidate)):
            if not isinstance(oid, str) or len(oid) not in (40, 64) or not SHA.fullmatch(oid):
                raise DiagnosticError('invalid_sha', f'{label} requires a full immutable lowercase object SHA')
            kind, raw = reader.object(oid, budget.limits.metadata_bytes)
            inputs[label] = {'requested_sha': oid, 'input_kind': kind,
                             'commit_sha': None, 'tree_sha': None}
            if kind not in ('commit', 'tree') or (label == 'base' and kind != 'commit'):
                raise DiagnosticError('object_type', f'{label} must be '+('commit' if label == 'base' else 'commit or tree'))
            tree = oid
            if kind == 'commit':
                first = raw.split(b'\n', 1)[0].decode('ascii')
                if not first.startswith('tree ') or not SHA.fullmatch(first[5:]):
                    raise DiagnosticError('git_protocol', 'Invalid commit tree header')
                tree = first[5:]
            inputs[label] = {'requested_sha': oid, 'input_kind': kind,
                'commit_sha': oid if kind == 'commit' else None, 'tree_sha': tree}
    return inputs


def modules_at(repository, identity, budget):
    with GitReader(repository, ['ls-tree', '-r', '-z', '-l', identity['tree_sha'], '--', 'demiflow'], budget) as reader:
        raw = reader.output(budget.limits.metadata_bytes)
    entries = raw.split(b'\0')
    budget.admit('metadata_entries', len(entries)-1)
    modules = {}
    total = excluded = 0
    for item in filter(None, entries):
        budget.check()
        meta, rawpath = item.split(b'\t', 1)
        budget.admit('path_bytes', len(rawpath))
        path = rawpath.decode('utf-8')
        if not path.endswith('.py'):
            continue
        mode, kind, oid, size = meta.decode('ascii').split()
        if mode not in ('100644', '100755') or kind != 'blob' or not size.isdigit():
            raise DiagnosticError('source_mode', f'Python source is not a regular Git blob: {path} ({mode}/{kind})')
        if '_vendor' in path.split('/'):
            excluded += 1
            continue
        count = int(size)
        budget.admit('blob_bytes', count)
        total += count
        budget.admit('source_bytes', total)
        is_package = path.endswith('/__init__.py')
        module = path[:-3].replace('/', '.')
        if is_package:
            module = module[:-len('.__init__')]
        if module in modules:
            raise DiagnosticError('ambiguous_module', f'Duplicate file/package module: {module}')
        modules[module] = {'module': module, 'path': path, 'oid': oid,
                           'bytes': count, 'is_package': is_package}
        budget.admit('modules', len(modules))
    if not modules:
        raise DiagnosticError('missing_module_tree', 'No Python source in fixed demiflow/ tree')
    return dict(sorted(modules.items())), {'source_bytes': total, 'excluded_vendor_python_files': excluded,
                                          'metadata_entries': len(entries)-1, 'metadata_bytes': len(raw)}


def resolve(name, modules):
    if name != 'demiflow' and not name.startswith('demiflow.'):
        return None
    if '_vendor' in name.split('.'):
        return None
    while name:
        if name in modules:
            return name
        name = name.rpartition('.')[0]
    return None


def relative_name(module, level, package):
    if not level:
        return module or ''
    parts = package.split('.')
    if level > len(parts):
        return ''
    return '.'.join(parts[:len(parts)-level+1] + ([module] if module else []))


def scan_source(meta, raw, modules, rows, budget):
    try:
        tree = ast.parse(raw, filename=meta['path'])
    except (SyntaxError, ValueError, RecursionError) as exc:
        raise DiagnosticError('syntax', f"{meta['path']}: {exc}") from exc
    package = meta['module'] if meta['is_package'] else meta['module'].rpartition('.')[0]
    stack = [(tree, 'top')]

    def add(category, node, kind, **fields):
        row = dict(src=meta['module'], path=meta['path'], line=node.lineno, kind=kind, **fields)
        rows[category].append(row)
        budget.admit('rows', sum(len(value) for value in rows.values()))

    while stack:
        budget.check()
        node, kind = stack.pop()
        if isinstance(node, ast.Import):
            for alias in node.names:
                target = resolve(alias.name, modules)
                if target:
                    add('static', node, kind, dst=target, names=[], name=alias.name, raw=alias.name)
        elif isinstance(node, ast.ImportFrom):
            origin = relative_name(node.module, node.level, package)
            for alias in node.names:
                full = origin+'.'+alias.name
                target = full if full in modules else resolve(origin, modules)
                if target:
                    add('static', node, kind, dst=target,
                        names=[] if full in modules else [alias.name], name=alias.name, raw=full if full in modules else origin)
        elif isinstance(node, ast.Call):
            called = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, 'attr', '')
            if called in ('import_module', '_import_module', '__import__'):
                argument = node.args[0] if node.args else None
                if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                    name = argument.value
                    target_package = package
                    if name.startswith('.'):
                        second = node.args[1] if len(node.args) > 1 else next((k.value for k in node.keywords if k.arg == 'package'), None)
                        if isinstance(second, ast.Constant) and isinstance(second.value, str):
                            target_package = second.value
                        elif second is not None and not (isinstance(second, ast.Name) and second.id == '__package__'):
                            add('unresolved_dynamic', node, kind, called=called, reason='computed relative package')
                            target_package = ''
                        name = relative_name(name.lstrip('.'), len(name)-len(name.lstrip('.')), target_package) if target_package else ''
                    add('literal_dynamic', node, kind, called=called, raw=argument.value, dst=resolve(name, modules))
                else:
                    add('unresolved_dynamic', node, kind, called=called, reason='nonliteral or absent first argument')
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            match = SYMBOL.fullmatch(node.value)
            if match:
                add('symbols', node, kind, raw=node.value, dst=resolve(match[1], modules),
                    names=[match[2]] if match[2] else [])
        if isinstance(node, ast.If) and (
                isinstance(node.test, ast.Name) and node.test.id == 'TYPE_CHECKING' or
                isinstance(node.test, ast.Attribute) and node.test.attr == 'TYPE_CHECKING'):
            stack.extend((child, kind) for child in reversed(node.orelse))
            stack.extend((child, 'typing') for child in reversed(node.body))
            stack.append((node.test, kind))
        else:
            child_kind = kind
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)) and kind != 'typing':
                child_kind = 'lazy'
            elif isinstance(node, (ast.Try, ast.If, ast.With)) and kind == 'top':
                child_kind = 'guarded'
            stack.extend((child, child_kind) for child in reversed(list(ast.iter_child_nodes(node))))
    del tree


def grouped(edges):
    groups = {}
    for row in edges:
        key = (row['src'], row['dst'], tuple(row['names']), row['kind'])
        value = groups.setdefault(key, {'src': row['src'], 'dst': row['dst'],
            'names': row['names'], 'kind': row['kind'], 'locations': []})
        location = {k: row[k] for k in ('path', 'line', 'name', 'raw')}
        if location not in value['locations']:
            value['locations'].append(location)
    for row in groups.values():
        row['locations'].sort(key=lambda v: (v['path'], v['line'], v['name'], v['raw']))
    return {key: groups[key] for key in sorted(groups)}


def edge_delta(before, after):
    old, new = grouped(before), grouped(after)
    return {'added': [new[k] for k in sorted(new.keys()-old.keys())],
            'removed': [old[k] for k in sorted(old.keys()-new.keys())],
            'relocated': [dict(new[k], before_locations=old[k]['locations'])
                          for k in sorted(old.keys() & new.keys()) if old[k]['locations'] != new[k]['locations']]}


def components(nodes, edges, budget):
    """Iterative finish-order/reverse reachability; same SCC membership as old Tarjan."""
    adjacency, reverse = {n: set() for n in nodes}, {n: set() for n in nodes}
    for row in edges:
        adjacency[row['src']].add(row['dst'])
        reverse[row['dst']].add(row['src'])
    seen, order = set(), []
    for start in sorted(nodes):
        if start in seen:
            continue
        seen.add(start)
        todo = [(start, iter(sorted(adjacency[start])))]
        while todo:
            budget.check()
            node, children = todo[-1]
            child = next(children, None)
            if child is None:
                order.append(node)
                todo.pop()
            elif child not in seen:
                seen.add(child)
                todo.append((child, iter(sorted(adjacency[child]))))
    seen, output = set(), []
    for start in reversed(order):
        if start in seen:
            continue
        todo, members = [start], []
        seen.add(start)
        while todo:
            budget.check()
            node = todo.pop()
            members.append(node)
            for child in sorted(reverse[node]):
                if child not in seen:
                    seen.add(child)
                    todo.append(child)
        if len(members) > 1:
            output.append(sorted(members))
    return sorted(output)


def analyze_snapshot(repository, identity, budget):
    modules, counts = modules_at(repository, identity, budget)
    rows = {kind: [] for kind in ('static', 'literal_dynamic', 'symbols', 'unresolved_dynamic')}
    with GitReader(repository, ['cat-file', '--batch'], budget) as reader:
        for module, meta in modules.items():
            kind, raw = reader.object(meta['oid'], budget.limits.blob_bytes)
            if kind != 'blob' or len(raw) != meta['bytes']:
                raise DiagnosticError('git_protocol', 'Source object does not match fixed tree')
            meta['lines'] = raw.count(b'\n')
            scan_source(meta, raw, modules, rows, budget)
            del raw
    nodes = set(modules)-{'demiflow'}
    run = [e for e in rows['static'] if e['kind'] in RUN_KINDS and e['src'] in nodes
           and e['dst'] in nodes and e['src'] != e['dst']]
    cycles = components(nodes, run, budget)
    return {'modules': modules, 'rows': rows, 'run': run, 'cycles': cycles, 'counts': counts}


def member(module, snapshot):
    return {'module': module, 'path': snapshot['modules'][module]['path']}


def component_view(members, snapshot):
    return {'id': hashlib.sha256('\n'.join(members).encode()).hexdigest(),
            'members': [member(m, snapshot) for m in members]}


def anchor_path(snapshot, paths):
    return next((path for path in paths if path in snapshot['modules']), paths[0])


def anchor_view(snapshot, paths):
    module = anchor_path(snapshot, paths)
    if module not in snapshot['modules']:
        return {'module': module, 'status': 'missing', 'members': []}
    cycle = next((c for c in snapshot['cycles'] if module in c), None)
    return {'module': module, 'status': 'cyclic' if cycle else 'acyclic',
            'members': [member(m, snapshot) for m in cycle or [module]]}


def scc_view(snapshot):
    return {'inventory': list(snapshot['modules'].values()), 'counts': snapshot['counts'],
        'components': [component_view(c, snapshot) for c in snapshot['cycles']],
        'anchors': {name: anchor_view(snapshot, module) for name, module in ANCHORS.items()},
        'typing': list(grouped(e for e in snapshot['rows']['static'] if e['kind'] == 'typing').values()),
        'observations': {k: snapshot['rows'][k] for k in ('literal_dynamic', 'symbols', 'unresolved_dynamic')}}


def membership_delta(before, after):
    old, new = set(before['modules'])-{'demiflow'}, set(after['modules'])-{'demiflow'}
    oldcycles = {m for c in before['cycles'] for m in c}
    newcycles = {m for c in after['cycles'] for m in c}
    oldcomponents = {tuple(c) for c in before['cycles']}
    newcomponents = {tuple(c) for c in after['cycles']}
    oldchanged = [component_view(c, before) for c in sorted(oldcomponents-newcomponents)]
    newchanged = [component_view(c, after) for c in sorted(newcomponents-oldcomponents)]
    owners_old = {m['module']: c['id'] for c in oldchanged for m in c['members']}
    owners_new = {m['module']: c['id'] for c in newchanged for m in c['members']}
    links = defaultdict(list)
    for m in sorted(owners_old.keys() & owners_new.keys()):
        links[(owners_old[m], owners_new[m])].append(m)
    outgoing, incoming = defaultdict(set), defaultdict(set)
    for a, b in links:
        outgoing[a].add(b)
        incoming[b].add(a)
    overlap = [{'before_id': a, 'after_id': b, 'shared_members': links[(a, b)],
                'split': len(outgoing[a]) > 1, 'merge': len(incoming[b]) > 1}
               for a, b in sorted(links)]
    return {'modules_added': [member(m, after) for m in sorted(new-old)],
        'modules_deleted': [member(m, before) for m in sorted(old-new)],
        'cyclic_entered_new': [member(m, after) for m in sorted((newcycles-oldcycles)-old)],
        'cyclic_entered_existing': [member(m, after) for m in sorted((newcycles-oldcycles) & old)],
        'cyclic_exited_existing': [member(m, before) for m in sorted((oldcycles-newcycles) & new)],
        'cyclic_deleted_modules': [member(m, before) for m in sorted((oldcycles-newcycles)-new)],
        'before_changed_components': oldchanged, 'after_changed_components': newchanged,
        'overlap_links': overlap, 'dependencies': edge_delta(before['run'], after['run'])}


def package_of(module, snapshot):
    parts = module.split('.')
    return '(root)' if len(parts) == 1 or len(parts) == 2 and not snapshot['modules'][module]['is_package'] else parts[1]


def declaration(snapshot, budget):
    modules, run = snapshot['modules'], snapshot['run']
    seeds = {m for m in modules if m == 'demiflow.execution.contracts' or m.startswith('demiflow.execution.contracts.')}
    seeds.update(set(FIXED_SEEDS) & modules.keys())
    top = defaultdict(set)
    for e in run:
        if e['kind'] != 'lazy':
            top[e['src']].add(e['dst'])
    closure, todo = set(seeds), list(sorted(seeds))
    while todo:
        budget.check()
        for child in sorted(top[todo.pop()]):
            if child not in closure:
                closure.add(child)
                todo.append(child)
    module = anchor_path(snapshot, ANCHORS['base'])
    base = next((set(c) for c in snapshot['cycles'] if module in c), set())
    top_candidates = [e for e in run if e['src'] in seeds and e['kind'] != 'lazy'
        and package_of(e['dst'], snapshot) not in ('execution', 'data', '_compat', 'resources') and e['dst'] != 'demiflow.errors']
    leaving = [e for e in run if e['kind'] == 'lazy' and e['src'] in closure and e['dst'] not in closure]
    groups = {'seed_top_candidates': top_candidates,
        'lazy_into_base_scc': [e for e in leaving if e['dst'] in base],
        'lazy_outside_base_scc': [e for e in leaving if e['dst'] not in base]}
    view = {'seeds': [member(m, snapshot) for m in sorted(seeds)],
        'missing_fixed_seeds': sorted(set(FIXED_SEEDS)-modules.keys()),
        'closure': [member(m, snapshot) for m in sorted(closure)],
        'counts': {name: {'imported_name_sites': len(edges),
                         'statements': len({(e['src'], e['line'], e['dst']) for e in edges})}
                   for name, edges in groups.items()},
        'reason': 'Candidates under the declared legacy seed/closure definition; not an approved layering rule or violation verdict.',
        'groups': {name: list(grouped(edges).values()) for name, edges in groups.items()},
        'typing_from_closure': list(grouped(e for e in snapshot['rows']['static'] if e['src'] in closure and e['kind'] == 'typing').values()),
        'observations_from_closure': {k: [e for e in snapshot['rows'][k] if e['src'] in closure]
            for k in ('literal_dynamic', 'symbols', 'unresolved_dynamic')}}
    return view, groups


def method(limits):
    return {'version': VERSION, 'tool_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'algorithm_sources': {'location': '_demiflow/shared-review/demiflow-architecture-20261119-data/',
            'graph.py': '20946bdb810d96ec04e5e4f151e13229b9c7442b53d0c0833e6be929fa4d06f0',
            'facts.py': '3766c4112f227dd778679364fbeddb723335ff3723bb34b679d6cb25d78767c9'},
        'scope': 'Fixed Git demiflow/**/*.py regular blobs; _vendor subtrees excluded; root facade inventoried but excluded from SCC/closure edges; other package initializers retained.',
        'scc_kinds': sorted(RUN_KINDS), 'excluded_from_scc': ['self edges', 'typing', 'literal_dynamic', 'symbols', 'unresolved_dynamic'],
        'delta_key': ['src', 'dst', 'names', 'kind'],
        'seeds': {'fixed': list(FIXED_SEEDS), 'subtree': 'demiflow.execution.contracts.*'},
        'seed_top_candidate_exclusions': ['data', 'execution', '_compat', 'resources', 'demiflow.errors'],
        'limits': asdict(limits),
        'limitations': [
            'Static source graph, not a runtime call graph or complete import initialization graph. No source execution or package imports.',
            'TYPE_CHECKING recognizes only direct Name/Attribute tests; compound tests, aliases, reflection and runtime reachability are not resolved.',
            'Function/lambda subtrees use the original lazy convention, including defaults/decorators/annotations; this is not evaluation-order proof.',
            'Dynamic calls are lexical import_module/_import_module/__import__ observations; aliases and shadowing are not inferred. Nonliteral arguments are unresolved.',
            'Literal relative dynamic calls use a literal package, __package__, or the source package. Runtime caller/package overrides may differ.',
            'String symbol observations use the original whole-string demiflow dotted/colon pattern. They do not prove loading or registered reachability.',
            'Longest existing module prefix resolves imports to source modules, not the final runtime object or reexport provider.',
            'Resource bounds are input/row/output/deadline admission limits with CLI OS backups; they are not a whole-host RSS guarantee.']}


def reports(repository, base, candidate, budget, inputs=None):
    inputs = fixed_inputs(repository, base, candidate, budget) if inputs is None else inputs
    snapshots = {name: analyze_snapshot(repository, identity, budget) for name, identity in inputs.items()}
    old, new = snapshots['base'], snapshots['candidate']
    common = {'method': method(budget.limits), 'inputs': inputs}
    scc = dict(common, format='demiflow_architecture_scc_report_v1',
        snapshots={name: scc_view(value) for name, value in snapshots.items()}, delta=membership_delta(old, new))
    views, groups = {}, {}
    for name, snapshot in snapshots.items():
        views[name], groups[name] = declaration(snapshot, budget)
    delta = {name: edge_delta(groups['base'][name], groups['candidate'][name]) for name in groups['base']}
    for label in ('seeds', 'closure'):
        before = {m['module'] for m in views['base'][label]}
        after = {m['module'] for m in views['candidate'][label]}
        delta[label+'_added'] = [member(m, new) for m in sorted(after-before)]
        delta[label+'_deleted'] = [member(m, old) for m in sorted(before-after)]
    decl = dict(common, format='demiflow_architecture_declaration_report_v1', snapshots=views, delta=delta)
    return inputs, (scc, decl)


def write_json(path, value, maximum, budget):
    size, digest = 0, hashlib.sha256()
    with path.open('xb') as stream:
        for chunk in json.JSONEncoder(sort_keys=True, ensure_ascii=False, indent=2).iterencode(value):
            budget.check()
            raw = chunk.encode('utf-8')
            size += len(raw)
            if size+1 > maximum:
                raise DiagnosticError('budget_report_bytes', 'Encoded JSON exceeds output byte limit')
            stream.write(raw)
            digest.update(raw)
        stream.write(b'\n')
        digest.update(b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    return {'path': str(path), 'bytes': size+1, 'sha256': digest.hexdigest()}


def generate(repository, base, candidate, output_dir, *, limits=Limits(), backups=None):
    started = time.monotonic()
    receipt = {'status': 'failed', 'requested_inputs': {'base': requested_value(base), 'candidate': requested_value(candidate)},
        'limits': asdict(limits), 'backups': backups or {'status': 'not_applied_in_process_api'}, 'files': []}
    owned, created = False, []
    try:
        for path in (repository, output_dir):
            value = os.fspath(path)
            if not isinstance(value, str) or len(value) > 4096 or len(os.fsencode(value)) > 4096:
                raise DiagnosticError('invalid_path', 'Repository/output path exceeds 4096 bytes')
        repository, output = Path(repository).resolve(), Path(output_dir).absolute()
        if output.resolve().is_relative_to(repository):
            raise DiagnosticError('output_inside_repository', 'Output must be outside the repository')
        output.mkdir()  # Exclusive new stage; never use or overwrite existing evidence.
        owned = True
        budget = Budget(limits)
        receipt['inputs'] = {}
        fixed_inputs(repository, base, candidate, budget, receipt['inputs'])
        inputs, values = reports(repository, base, candidate, budget, receipt['inputs'])
        prepared = []
        for name, value in zip(REPORTS, values):
            temporary = output / ('.'+name+'.tmp')
            created.append(temporary)
            record = write_json(temporary, value, limits.report_bytes, budget)
            prepared.append((temporary, output/name, record))
        for temporary, final, record in prepared:
            os.link(temporary, final)
            created.append(final)
            temporary.unlink()
            record['path'] = str(final)
            receipt['files'].append(record)
        receipt['status'] = 'complete'
    except Exception as exc:
        receipt.update(status='failed', error=error_record(exc), files=[])
        for path in reversed(created):
            path.unlink(missing_ok=True)
    receipt['elapsed_seconds'] = time.monotonic()-started
    if owned:
        try:
            # The failure receipt has a fresh independent write deadline, not a fake report.
            write_json(output/'receipt.json', receipt, 65536, Budget())
        except Exception as exc:
            receipt.update(status='failed', error=error_record(exc), files=[])
            for path in reversed(created):
                path.unlink(missing_ok=True)
    return receipt


def apply_backups():
    state = {'address_space_bytes': 512*1024**2, 'cpu_seconds': 60, 'wall_seconds': 90}
    try:
        import resource
        for key, limit in ((resource.RLIMIT_AS, state['address_space_bytes']), (resource.RLIMIT_CPU, 60)):
            soft, hard = resource.getrlimit(key)
            value = limit if hard == resource.RLIM_INFINITY else min(hard, limit)
            resource.setrlimit(key, (value, value))
        state['resource_limits'] = 'applied'
    except (ImportError, ValueError, OSError) as exc:
        state['resource_limits'] = 'unsupported: '+str(exc)[:200]
    def stopped(number, frame):
        raise DiagnosticError('cancelled' if number != getattr(signal, 'SIGALRM', None) else 'wall_timeout',
                              'Diagnostic process interrupted or wall deadline exceeded')
    for name in ('SIGTERM', 'SIGINT', 'SIGALRM'):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), stopped)
    if hasattr(signal, 'setitimer'):
        signal.setitimer(signal.ITIMER_REAL, 90)
        state['wall_timer'] = 'applied'
    else:
        state['wall_timer'] = 'unsupported; caller must supervise process'
    return state


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--repository', required=True)
    parser.add_argument('--base', required=True)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--output-dir', required=True)
    options = parser.parse_args(argv)
    receipt = generate(options.repository, options.base, options.candidate, options.output_dir,
                       backups=apply_backups())
    print(json.dumps(receipt, ensure_ascii=True, sort_keys=True))
    return 0 if receipt['status'] == 'complete' else 1


if __name__ == '__main__':
    raise SystemExit(main())
