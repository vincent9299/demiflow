"""Read direct callable bindings in registered flow source, without importing project code."""
import ast
import hashlib
from pathlib import Path

from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.forge.project_io import relative_path


def _api(snapshot):
    result = {}
    for item in snapshot['catalog']['objects']:
        name = item['id'].replace(':', '.')
        if not name.startswith('demiflow.data.'):
            continue
        spec = item.get('native_semantics', {})
        produces = (spec.get('role') in {'source', 'transform', 'bridge'}
                    or (item['signature'].get('text') or '').endswith(("-> 'Dataset'", "-> 'MaterializedDataset'")))
        ports = [p['name'] for p in spec.get('callable_ports', [])]
        if not ports and spec.get('callable'):
            ports.append(spec['callable'])
        if produces or ports:
            positional = [p['name'] for p in item['signature'].get('parameters', [])
                          if p['kind'] in {'POSITIONAL_ONLY', 'POSITIONAL_OR_KEYWORD'} and p['name'] != 'self']
            result[name] = produces, ports, positional
    return result


_DATASET = 'demiflow.data.Dataset'


class _Entry(ast.NodeVisitor):
    """Track only project imports, simple aliases and known Dataset receivers."""
    def __init__(self, entry, definitions, api, uses):
        self.entry = entry
        self.names = {}
        self.definitions = definitions
        self.api = api
        self.uses = uses

    def visit_Import(self, node):
        for item in node.names:
            first = item.name.split('.')[0]
            self.names[item.asname or first] = item.name if item.asname else first

    def visit_ImportFrom(self, node):
        package = Path(self.entry['path']).parent.parts
        if node.level > len(package) + 1:
            return
        prefix = list(package[:len(package) - node.level + 1]) if node.level else []
        if node.module:
            prefix.append(node.module)
        for item in node.names:
            self.names[item.asname or item.name] = '.'.join([*prefix, item.name])

    def visit_Name(self, node):
        return self.names.get(node.id)

    def visit_Attribute(self, node):
        base = self.visit(node.value)
        if base:
            return base + '.' + node.attr

    def visit_Call(self, node):
        name = self.visit(node.func)
        args = [self.visit(a) for a in node.args]
        kwargs = {k.arg: self.visit(k.value) for k in node.keywords}
        if name in self.definitions and self.definitions[name]['definition_type'] == 'class':
            return name
        if name not in self.api:
            return None
        produces, ports, positional = self.api[name]
        bound = dict(zip(positional, args))
        bound.update(kwargs)
        for port in ports:
            definition = self.definitions.get(bound.get(port))
            if definition:
                # Keep one concrete source location per entry and definition.
                self.uses.setdefault(definition['id'], {}).setdefault(self.entry['entry'], dict(
                    entry=self.entry['entry'], line=node.lineno, api=name.rsplit('.', 1)[-1], port=port))
        return _DATASET if produces else None

    def assign(self, targets, value):
        for target in targets:
            for name in ast.walk(target):
                if isinstance(name, ast.Name) and isinstance(name.ctx, ast.Store):
                    self.names[name.id] = value if target is name else None

    def visit_Assign(self, node):
        self.assign(node.targets, self.visit(node.value))

    def visit_AnnAssign(self, node):
        self.assign([node.target], self.visit(node.value) if node.value else None)

    def visit_If(self, node):
        self.visit(node.test)
        before = self.names.copy()
        for stmt in node.body:
            self.visit(stmt)
        left, self.names = self.names, before
        for stmt in node.orelse:
            self.visit(stmt)
        self.names = {k:v for k,v in left.items() if self.names.get(k) == v}

    def visit_For(self, node):
        self.visit(node.iter)
        before = self.names.copy()
        self.assign([node.target], None)
        for stmt in node.body:
            self.visit(stmt)
        self.names = {k:v for k,v in before.items() if self.names.get(k) == v}
        for stmt in node.orelse:
            self.visit(stmt)

    def visit_FunctionDef(self, node):
        self.names.pop(node.name, None)

    visit_AsyncFunctionDef = visit_ClassDef = visit_FunctionDef

    def visit_Lambda(self, node):
        return None


def direct_references(root, entries, items, snapshot):
    names, uses = {}, {}
    for item in items:
        if item['kind'] != 'code':
            continue
        module = list(Path(item['path']).with_suffix('').parts)
        if module[-1] == '__init__':
            module.pop()
        parts = [*module, item['symbol']]
        names['.'.join(parts)] = item
        # Monorepo flows can use a fully qualified project package.
        for parent in reversed(root.parts):
            if not parent.isidentifier():
                break
            parts.insert(0, parent)
            names['.'.join(parts)] = item
    api = _api(snapshot)
    for entry in entries:
        if entry['status'] != 'recognized':
            continue
        raw = read_bounded(relative_path(root, entry['path']), 1024**2)
        if hashlib.sha256(raw).hexdigest() != entry['sha256']:
            raise ForgeError('Flow changed while reading operator bindings', code='conflict')
        tree = ast.parse(raw)  # Discovery already bounded and checked this exact source.
        reader = _Entry(entry, names, api, uses)
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                reader.visit(node)
        function = next(n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and n.name == entry['symbol'])
        for arg in ast.walk(function.args):
            if isinstance(arg, ast.arg):
                reader.names.pop(arg.arg, None)
        for stmt in function.body:
            reader.visit(stmt)
    return {key: list(by_entry.values()) for key, by_entry in uses.items()}
