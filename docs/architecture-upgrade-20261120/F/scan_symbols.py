"""Finite, source-only F1/F2 inventory. Does not import analyzed modules.

Uses the relative-import and kind conventions of the published architecture
graph.py (20946bdb810d96ec04e5e4f151e13229b9c7442b53d0c0833e6be929fa4d06f0).
This is a design probe, not a production guard or a complete Python call graph.
"""
import argparse
import ast
import collections
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

FIVE = {'demiflow.forge.' + name for name in (
    'schema', 'io', 'platform', 'platform_environment', 'operation_budget')}
LIBS = {'operator_llm', 'embeddings', 'collect', 'services', 'lance'}
MAX_FILES, MAX_FILE, MAX_BYTES, MAX_ROWS = 4096, 2*1024**2, 64*1024**2, 100000
SYMBOL = re.compile(r'(demiflow(?:\.[A-Za-z_]\w*)+)(?::([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*))?\Z')


def module_name(path):
    parts = path.with_suffix('').parts
    return '.'.join(parts[:-1] if parts[-1] == '__init__' else parts)


def scan(root):
    start = time.monotonic()
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    source_roots = ['demiflow', 'tests', 'tools', 'examples']
    if subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', *source_roots], cwd=root).returncode:
        raise ValueError('analyzed tracked sources differ from the recorded HEAD')
    names = subprocess.check_output(['git', 'ls-files', '-z', '--', 'demiflow', 'tests', 'tools', 'examples'], cwd=root)
    paths = sorted(Path(name.decode()) for name in names.split(b'\0') if name.endswith(b'.py') and b'/_vendor/' not in name)
    if len(paths) > MAX_FILES:
        raise ValueError('source inventory exceeds file budget')
    mods = {module_name(path): path for path in paths}
    imports, uses, strings, dynamic, sources = [], [], [], [], []
    total = 0

    def checked(row_list, row):
        if sum(map(len, (imports, uses, strings, dynamic))) >= MAX_ROWS:
            raise ValueError('scan row budget exceeded')
        row_list.append(row)

    def relevant(module):
        return module in FIVE or module.startswith(('demiflow.execution.', 'demiflow.planning.'))

    def split_symbol(target):
        parts = target.split('.')
        for count in range(len(parts), 0, -1):
            prefix = '.'.join(parts[:count])
            if prefix in mods:
                return prefix, '.'.join(parts[count:]) or '<module>'
        return target, '<unresolved>'

    for module, relative in sorted(mods.items()):
        if time.monotonic() - start > 60:
            raise ValueError('scan deadline exceeded')
        path = root / relative
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_FILE:
            raise ValueError('invalid/oversize source: ' + str(relative))
        total += path.stat().st_size
        if total > MAX_BYTES:
            raise ValueError('source inventory exceeds byte budget')
        raw = path.read_bytes()
        sources.append({'path': str(relative), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
        tree = ast.parse(raw, filename=str(relative))
        package = module if relative.name == '__init__.py' else module.rsplit('.', 1)[0]

        def row(node, context, kind, **more):
            return {'consumer': module, 'path': str(relative), 'line': node.lineno, 'context': context, 'kind': kind, **more}

        def resolved(node, env):
            if isinstance(node, ast.Name):
                return env.get(node.id)
            if isinstance(node, ast.Attribute):
                parent = resolved(node.value, env)
                if parent:
                    return parent + '.' + node.attr
            return None

        def walk(node, env, context, kind, parent=None):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                for dec in node.decorator_list:
                    walk(dec, env, context, kind, node)
                child_env = dict(env)
                if not isinstance(node, ast.ClassDef):
                    for arg in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
                        child_env.pop(arg.arg, None)
                child_kind = kind if kind == 'typing' else ('lazy' if not isinstance(node, ast.ClassDef) else kind)
                for item in node.body:
                    walk(item, child_env, context + '.' + node.name, child_kind, node)
                return
            if isinstance(node, ast.If) and ((isinstance(node.test, ast.Name) and node.test.id == 'TYPE_CHECKING') or
                                            (isinstance(node.test, ast.Attribute) and node.test.attr == 'TYPE_CHECKING')):
                for item in node.body:
                    walk(item, dict(env), context, 'typing', node)
                for item in node.orelse:
                    walk(item, env, context, kind, node)
                return
            if isinstance(node, (ast.If, ast.Try, ast.With)) and kind == 'top':
                kind = 'guarded'
            if isinstance(node, ast.ImportFrom):
                if node.level:
                    base = package.split('.')
                    base = base[:len(base) - (node.level - 1)]
                    target = '.'.join(base + ([node.module] if node.module else []))
                else:
                    target = node.module or ''
                for alias in node.names:
                    full = target + '.' + alias.name
                    provider, symbol = split_symbol(full)
                    if provider.startswith('demiflow'):
                        checked(imports, row(node, context, kind, provider=provider, symbol=symbol,
                                             binding=alias.asname or alias.name, raw=full))
                    env[alias.asname or alias.name] = full
                return
            if isinstance(node, ast.Import):
                for alias in node.names:
                    provider, symbol = split_symbol(alias.name)
                    if provider.startswith('demiflow'):
                        checked(imports, row(node, context, kind, provider=provider, symbol=symbol,
                                             binding=alias.asname or alias.name.split('.')[0], raw=alias.name))
                    env[alias.asname or alias.name.split('.')[0]] = alias.name if alias.asname else alias.name.split('.')[0]
                return
            if isinstance(node, (ast.Name, ast.Attribute)) and isinstance(node.ctx, ast.Load):
                if not (isinstance(parent, ast.Attribute) and parent.value is node):
                    target = resolved(node, env)
                    if target:
                        provider, symbol = split_symbol(target)
                        if relevant(provider):
                            checked(uses, row(node, context, kind, provider=provider, symbol=symbol,
                                              role='call' if isinstance(parent, ast.Call) and parent.func is node else 'reference',
                                              expression=ast.unparse(parent if isinstance(parent, ast.Call) and parent.func is node else node)[:300]))
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                match = SYMBOL.fullmatch(node.value)
                if match:
                    provider, symbol = split_symbol(match[1])
                    if relevant(provider):
                        checked(strings, row(node, context, kind, provider=provider,
                                             symbol=match[2] or symbol, raw=node.value))
            if isinstance(node, ast.Call):
                function = node.func
                name = function.id if isinstance(function, ast.Name) else (function.attr if isinstance(function, ast.Attribute) else '')
                if name in ('import_module', '_import_module', '__import__'):
                    checked(dynamic, row(node, context, kind, expression=ast.unparse(node)[:300],
                                         literal=bool(node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str))))
            for child in ast.iter_child_nodes(node):
                walk(child, env, context, kind, node)

        walk(tree, {}, module, 'top')

    f1 = [r for r in imports if r['consumer'].startswith('demiflow.workbench.') and r['provider'] in FIVE]
    f2 = [r for r in imports if r['consumer'].split('.')[1:2] and r['consumer'].split('.')[0] == 'demiflow'
          and r['consumer'].split('.')[1] in LIBS and r['provider'].startswith(('demiflow.execution.', 'demiflow.planning.'))]
    keys = {(r['provider'], r['symbol']) for r in f1 + f2 if r['symbol'] != '<module>'}
    module_targets = {r['provider'] for r in f1 + f2 if r['symbol'] == '<module>'}
    for r in uses:
        if r['consumer'].startswith('demiflow.workbench.') and r['provider'] in module_targets:
            keys.add((r['provider'], r['symbol'].split('.')[0]))
    declarations = []
    for provider, symbol in sorted(keys):
        path = mods.get(provider)
        if path is None:
            continue
        tree = ast.parse((root / path).read_bytes())
        found = []
        for node in tree.body:
            named = (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name == symbol)
            assigned = isinstance(node, (ast.Assign, ast.AnnAssign)) and any(
                isinstance(t, ast.Name) and t.id == symbol for t in (node.targets if isinstance(node, ast.Assign) else [node.target]))
            if named or assigned:
                methods = []
                if isinstance(node, ast.ClassDef):
                    methods = [{'name': n.name, 'line': n.lineno, 'signature': ast.unparse(n.args),
                                'returns': ast.unparse(n.returns) if n.returns else None}
                               for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
                found.append({'path': str(path), 'line': node.lineno, 'end_line': node.end_lineno,
                              'type': type(node).__name__, 'signature': ast.unparse(node.args) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) else None,
                              'returns': ast.unparse(node.returns) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.returns else None,
                              'doc': ast.get_docstring(node) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) else None,
                              'methods': methods})
        declarations.append({'provider': provider, 'symbol': symbol, 'definitions': found})
    def needed(record):
        key = (record.get('provider'), record.get('symbol', '').split('.')[0])
        return key in keys or (record.get('provider') in module_targets and record.get('symbol') == '<module>')

    all_counts = {'imports': len(imports), 'uses': len(uses), 'strings': len(strings), 'dynamic': len(dynamic)}
    if subprocess.run(['git', 'diff', '--quiet', revision, '--', *source_roots], cwd=root).returncode:
        raise ValueError('analyzed tracked sources changed during scan')
    return {'format': 'task_f_source_scan_v1', 'source_sha': revision, 'roots': ['demiflow', 'tests', 'tools', 'examples'],
            'limits': {'files': MAX_FILES, 'file_bytes': MAX_FILE, 'total_bytes': MAX_BYTES, 'rows': MAX_ROWS, 'seconds': 60},
            'sources': sources, 'imports': [r for r in imports if needed(r)], 'uses': [r for r in uses if needed(r)],
            'string_references': [r for r in strings if needed(r)], 'dynamic_calls': dynamic,
            'f1_imports': f1, 'f2_imports': f2, 'target_declarations': declarations,
            'summary': {'files': len(paths), 'bytes': total, 'f1_sites': len(f1), 'f1_kinds': dict(collections.Counter(r['kind'] for r in f1)),
                        'f2_sites': len(f2), 'f2_kinds': dict(collections.Counter(r['kind'] for r in f2)), 'target_symbols': len(keys),
                        'pre_filter_rows': all_counts},
            'limitations': ['Static lexical traversal, not a complete call graph or proof of external consumers.',
                            'Assignment aliases, branch-dependent shadowing, reflection and computed dynamic strings are not fully resolved.',
                            'Call/reference excerpts and imports are evidence separately; imported does not mean reached at runtime.',
                            'Only literal TYPE_CHECKING tests classified; other conditions stay guarded. No business code executed.']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    options = parser.parse_args()
    output = scan(options.root.resolve())
    raw = json.dumps(output, ensure_ascii=False, indent=2) + '\n'
    if len(raw.encode()) > 16*1024**2:
        raise ValueError('output budget exceeded')
    with options.output.open('x') as handle:
        handle.write(raw)
    print(json.dumps(output['summary'], sort_keys=True))
