"""Source-only facts for the pre-v3 retention ledger: who imports a module, who names a symbol.

Nothing is imported or executed. A module's importers come from the syntax tree (imports
inside functions included). A symbol's users are the package modules in which its name occurs
as a code token outside comments and strings, so ``chain.run_local_chain`` counts as well as
``from .chain import run_local_chain``.
"""
from __future__ import annotations

import ast
import io
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / 'demiflow'


def modules(package=PACKAGE):
    out = {}
    for path in sorted(package.rglob('*.py')):
        if '_vendor' in path.parts:
            continue
        name = '.'.join(path.relative_to(package.parent).with_suffix('').parts)
        if name.endswith('.__init__'):
            name = name[:-9]
        out[name] = path
    return out


def importers(known):
    """{imported module: {importing module}} over the package."""
    out = {}
    for name, path in known.items():
        tree = ast.parse(path.read_text())
        base = name.split('.') if path.name == '__init__.py' else name.split('.')[:-1]
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                prefix = base[:len(base) - (node.level - 1)] if node.level > 1 else base
                module = '.'.join((prefix if node.level else []) + ([node.module] if node.module else []))
                targets = [module] + [module + '.' + alias.name for alias in node.names]
            for target in targets:
                if target in known and target != name:
                    out.setdefault(target, set()).add(name)
    return out


def name_tokens(path):
    """Names used as code in one file (comments and strings do not count)."""
    out = set()
    try:
        for token in tokenize.generate_tokens(io.StringIO(path.read_text()).readline):
            if token.type == tokenize.NAME:
                out.add(token.string)
    except (tokenize.TokenError, IndentationError):
        pass
    return out


def top_level(path):
    """{name: kind} for functions, classes and simple assignments at module level."""
    out = {}
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out[node.name] = 'function'
        elif isinstance(node, ast.ClassDef):
            out[node.name] = 'class'
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    out[target.id] = 'constant'
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            out[node.target.id] = 'constant'
    return out


def function_source(path, name):
    source = path.read_text()
    for node in ast.parse(source).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return ast.get_source_segment(source, node) or ''
    return ''


def symbol_users(known, module, names, tokens=None):
    """{symbol: {module using it}} outside the defining module."""
    tokens = tokens if tokens is not None else {name: name_tokens(path) for name, path in known.items()}
    return {symbol: {name for name, used in tokens.items() if name != module and symbol in used} for symbol in names}
