"""Read project-authored public definitions without importing business modules."""
import ast


OPERATOR_EXPORT = '__demiflow_operators__'
DATA_TOOL_EXPORT = '__demiflow_data_tools__'
EXPORT_FIELDS = {'operators': OPERATOR_EXPORT, 'data_tools': DATA_TOOL_EXPORT}
DEFINITIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def exports(tree, field):
    """An absent declaration is legacy. An explicit empty list exports nothing."""
    names = None
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
            members = []
            for key, role in zip(value.keys, value.values):
                if not isinstance(role, ast.Constant) or role.value not in ('analysis', 'view', 'annotation', 'annotation_component'):
                    raise ValueError(f'{field} 的类别应为 analysis、view、annotation 或 annotation_component。')
                members.append((key, role.value))
        elif isinstance(value, (ast.List, ast.Tuple)):
            members = [(entry, 'view' if field == DATA_TOOL_EXPORT else 'operator') for entry in value.elts]
        else:
            raise ValueError(f'{field} 需要使用静态声明列出公开名称。')
        names = {}
        for entry, kind in members:
            if not isinstance(entry, ast.Constant) or type(entry.value) is not str:
                raise ValueError(f'{field} 的成员需要是名称字符串。')
            name = entry.value
            if not name.isidentifier() or name.startswith('_') or name in names:
                raise ValueError(f'{field} 的名称无效或重复：{name[:80]}')
            names[name] = kind
    for name in sorted(names or ()):
        if len(symbols.get(name, [])) != 1:
            raise ValueError(f'{field} 找不到唯一本地函数或类：{name}')
        node = symbols[name][0]
        if (field == OPERATOR_EXPORT and isinstance(node, ast.ClassDef)
                and not any(isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name == '__call__' for m in node.body)
                and not any(not isinstance(base, ast.Name) or base.id != 'object' for base in node.bases)):
            raise ValueError(f'算子类 {name} 需要实现或继承 __call__。')
    return names
