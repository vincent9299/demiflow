"""Read project-authored public definitions without importing business modules."""
import ast


OPERATOR_EXPORT = '__demiflow_operators__'
DATA_TOOL_EXPORT = '__demiflow_data_tools__'
EXPORT_FIELDS = {'operators': OPERATOR_EXPORT, 'data_tools': DATA_TOOL_EXPORT}
DEFINITIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


DATA_TOOL_ROLES = ('tool', 'analysis', 'view', 'annotation', 'annotation_component')
DATA_TOOL_SEMANTIC_KEYS = {'kind', 'title', 'summary', 'when_to_use'}


def _parse_data_tool_entry(node):
    if isinstance(node, ast.Constant) and node.value in DATA_TOOL_ROLES:
        return {'kind': node.value}
    if isinstance(node, ast.Dict):
        meta = {}
        for k, v in zip(node.keys, node.values):
            if not isinstance(k, ast.Constant) or k.value not in DATA_TOOL_SEMANTIC_KEYS:
                raise ValueError(f'{DATA_TOOL_EXPORT} 语义声明仅支持字段：{", ".join(sorted(DATA_TOOL_SEMANTIC_KEYS))}。')
            if not isinstance(v, ast.Constant) or type(v.value) is not str:
                raise ValueError(f'{DATA_TOOL_EXPORT} 的 {k.value} 需要是字符串常量。')
            meta[k.value] = v.value.strip()[:256]
        if meta.get('kind') not in DATA_TOOL_ROLES:
            raise ValueError(f'{DATA_TOOL_EXPORT} 的类别应为 tool、analysis、view、annotation 或 annotation_component。')
        return meta
    raise ValueError(f'{DATA_TOOL_EXPORT} 的类别应为 tool、analysis、view、annotation 或 annotation_component，或包含 kind/title/summary/when_to_use 的静态字典。')


def _parse_exports(tree, field):
    names = None
    semantics = {}
    symbols = {}
    for statement in tree.body:
        if isinstance(statement, DEFINITIONS):
            symbols.setdefault(statement.name, []).append(statement)
        if not isinstance(statement, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            continue
        targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
        if not any(isinstance(t, ast.Name) and t.id == field for t in targets):
            continue
        if names is not None or len(targets) != 1 or isinstance(statement, ast.AugAssign):
            raise ValueError(f'{field} 必须在模块顶层一次性声明。')
        value = statement.value
        if field == DATA_TOOL_EXPORT and isinstance(value, ast.Dict):
            members = [(key, _parse_data_tool_entry(role)) for key, role in zip(value.keys, value.values)]
        elif isinstance(value, (ast.List, ast.Tuple)):
            members = [(entry, {'kind': 'view' if field == DATA_TOOL_EXPORT else 'operator'}) for entry in value.elts]
        else:
            raise ValueError(f'{field} 需要使用静态声明列出公开名称。')
        names = {}
        for entry, meta in members:
            if not isinstance(entry, ast.Constant) or type(entry.value) is not str:
                raise ValueError(f'{field} 的成员需要是名称字符串。')
            name = entry.value
            if not name.isidentifier() or name.startswith('_') or name in names:
                raise ValueError(f'{field} 的名称无效或重复：{name[:80]}')
            names[name] = meta['kind']
            semantics[name] = meta
    for name in sorted(names or ()):
        if len(symbols.get(name, [])) != 1:
            raise ValueError(f'{field} 找不到唯一本地函数或类：{name}')
        node = symbols[name][0]
        if (field == OPERATOR_EXPORT and isinstance(node, ast.ClassDef)
                and not any(isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name == '__call__' for m in node.body)
                and not any(not isinstance(base, ast.Name) or base.id != 'object' for base in node.bases)):
            raise ValueError(f'算子类 {name} 需要实现或继承 __call__。')
    return names, semantics


def exports(tree, field):
    """An absent declaration is legacy. An explicit empty list exports nothing."""
    names, _ = _parse_exports(tree, field)
    return names


def data_tool_semantics(tree):
    """Return optional semantic metadata (title/summary/when_to_use) declared in __demiflow_data_tools__."""
    _, semantics = _parse_exports(tree, DATA_TOOL_EXPORT)
    return semantics
