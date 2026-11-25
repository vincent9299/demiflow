"""Passive definition projections shared by current and snapshot readers."""
import ast
import copy
from demiflow.forge.platform import bounded_json
from demiflow.forge.project_rules import parse_docstring_example


def _str_const(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value:
        return node.value
    return None


def extract_field_flow(node, *, max_fields: int = 24):
    """Passively extract read/written row field names from an operator function or Actor AST."""
    entry = node
    if isinstance(node, ast.ClassDef):
        entry = next((n for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == '__call__'), None)
    if not isinstance(entry, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return None

    pos_args = [*entry.args.posonlyargs, *entry.args.args]
    if pos_args and pos_args[0].arg in {'self', 'cls'}:
        pos_args = pos_args[1:]
    if not pos_args:
        return None
    row_param = pos_args[0].arg
    row_vars = {row_param}
    out_vars = set()
    passes_through = False
    reads: list[str] = []
    writes: list[str] = []

    def add_read(key):
        if key and key not in reads and len(reads) < max_fields:
            reads.append(key)

    def add_write(key):
        if key and key not in writes and len(writes) < max_fields:
            writes.append(key)

    def is_row_expr(expr) -> bool:
        return isinstance(expr, ast.Name) and expr.id in row_vars

    def collect_dict_keys(expr):
        nonlocal passes_through
        if isinstance(expr, ast.Dict):
            for k, v in zip(expr.keys, expr.values):
                if k is None:
                    if is_row_expr(v):
                        passes_through = True
                else:
                    add_write(_str_const(k))
        elif isinstance(expr, (ast.List, ast.Tuple)):
            for elt in expr.elts:
                collect_dict_keys(elt)

    # First pass: discover loop aliases of row/batch and output dict containers.
    for sub in ast.walk(entry):
        if isinstance(sub, (ast.For, ast.AsyncFor)):
            if isinstance(sub.target, ast.Name) and is_row_expr(sub.iter):
                row_vars.add(sub.target.id)
        elif isinstance(sub, ast.Assign) and len(sub.targets) == 1 and isinstance(sub.targets[0], ast.Name):
            tgt = sub.targets[0].id
            val = sub.value
            if isinstance(val, ast.Call):
                if isinstance(val.func, ast.Name) and val.func.id == 'dict' and val.args and is_row_expr(val.args[0]):
                    out_vars.add(tgt)
                    passes_through = True
                elif isinstance(val.func, ast.Attribute) and val.func.attr == 'copy' and is_row_expr(val.func.value):
                    out_vars.add(tgt)
                    passes_through = True
            elif isinstance(val, ast.Dict):
                if any(k is None and is_row_expr(v) for k, v in zip(val.keys, val.values)):
                    out_vars.add(tgt)
                    passes_through = True
        elif isinstance(sub, (ast.Return, ast.Yield)) and isinstance(sub.value, ast.Name):
            if sub.value.id in row_vars:
                passes_through = True
            else:
                out_vars.add(sub.value.id)

    # Second pass: collect read keys and written keys in source order.
    for sub in ast.walk(entry):
        if isinstance(sub, ast.Subscript) and isinstance(sub.value, ast.Name):
            key = _str_const(sub.slice)
            if not key:
                continue
            if isinstance(sub.ctx, ast.Load) and sub.value.id in row_vars:
                add_read(key)
            elif isinstance(sub.ctx, ast.Store) and (sub.value.id in out_vars or sub.value.id in row_vars):
                add_write(key)
        elif isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute) and isinstance(sub.func.value, ast.Name):
            recv = sub.func.value.id
            attr = sub.func.attr
            if recv in row_vars and attr in {'get', 'pop'} and sub.args:
                add_read(_str_const(sub.args[0]))
            elif (recv in out_vars or recv in row_vars) and attr == 'update':
                if sub.args and isinstance(sub.args[0], ast.Dict):
                    collect_dict_keys(sub.args[0])
                for kw in sub.keywords:
                    if kw.arg:
                        add_write(kw.arg)
            elif attr == 'append' and sub.args and isinstance(sub.args[0], ast.Dict):
                collect_dict_keys(sub.args[0])
        elif isinstance(sub, ast.Compare) and len(sub.ops) == 1 and isinstance(sub.ops[0], (ast.In, ast.NotIn)):
            if len(sub.comparators) == 1 and is_row_expr(sub.comparators[0]):
                add_read(_str_const(sub.left))
        elif isinstance(sub, (ast.Return, ast.Yield)) and sub.value is not None:
            collect_dict_keys(sub.value)
        elif isinstance(sub, ast.Assign) and len(sub.targets) == 1 and isinstance(sub.targets[0], ast.Name):
            if sub.targets[0].id in out_vars and isinstance(sub.value, ast.Dict):
                collect_dict_keys(sub.value)

    if not reads and not writes:
        return None
    return dict(reads=reads, writes=writes, passes_through=passes_through)


def signature(node, *, variadic=False):
    args = node.args
    positional = [*args.posonlyargs, *args.args]
    defaults = [None] * (len(positional) - len(args.defaults)) + args.defaults
    rows = []
    pairs = [*zip(positional, defaults), *zip(args.kwonlyargs, args.kw_defaults)]
    for arg, default in pairs:
        if arg.arg in {'self', 'cls'}:
            continue
        row = dict(name=arg.arg, annotation=ast.unparse(arg.annotation) if arg.annotation else None,
                   required=default is None, positional_only=arg in args.posonlyargs, default_known=False)
        if default is not None:
            row['default_expression'] = ast.unparse(default)
            try:
                value = ast.literal_eval(default)
                bounded_json(value, 16384)
                if type(value) in {str, int, float, bool, list, dict, type(None)}:
                    row.update(default=value, default_known=True)
            except (ValueError, TypeError, RecursionError):
                pass
        rows.append(row)
    if variadic:
        for arg, prefix, label in ((args.vararg, '*', '可变位置参数'), (args.kwarg, '**', '可变关键字参数')):
            if arg is not None:
                rows.append(dict(name=prefix + arg.arg, annotation=ast.unparse(arg.annotation) if arg.annotation else None,
                                 required=False, default_known=False, default_expression=label))
    return rows


def code_definition(node, source, *, kind='code', name=None):
    entry = node
    if isinstance(node, ast.ClassDef):
        entry = next((n for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == '__call__'), None)
    value = dict(kind=kind, symbol=name or node.name, source=source,
                 description=ast.get_docstring(node) or '', parameters=signature(entry, variadic=True) if entry else [],
                 returns=ast.unparse(entry.returns) if entry and entry.returns else None)
    args = copy.deepcopy(entry.args) if entry else None
    if args and (kind == 'api' or isinstance(node, ast.ClassDef)):
        if args.posonlyargs and args.posonlyargs[0].arg in {'self', 'cls'}:
            args.posonlyargs.pop(0)
        elif args.args and args.args[0].arg in {'self', 'cls'}:
            args.args.pop(0)
    value['signature'] = ((name or node.name) + '(' + ast.unparse(args) + ')'
                          + (' -> ' + value['returns'] if value['returns'] else '')) if entry else None
    if kind == 'code':
        field_flow = extract_field_flow(node)
        if field_flow:
            value['field_flow'] = field_flow
        example = parse_docstring_example(value['description'])
        if example is None and entry is not None and entry is not node:
            example = parse_docstring_example(ast.get_docstring(entry) or '')
        if example:
            value['example'] = example
    return value


def prompt_definition(name, item, *, arguments=None):
    """Describe template variables with the native compiler, never render them."""
    variables, diagnostic = [], None
    if arguments is None:
        if item.get('input_mode') == 'messages':
            arguments = {'messages': 'json'}
        elif type(item.get('template')) is str:
            from demiflow.operator_llm.template import inspect_template
            compiled, issues = inspect_template(item['template'])
            if compiled is not None:
                arguments = {key: value.value for key, value in compiled.arguments.items()}
            else:
                diagnostic = '模板变量未通过原生模板解析，输入契约未知。'
        systemone = item.get('systemone', {})
        each = systemone.get('for_each', {}) if type(systemone) is dict else {}
        if type(each) is dict and type(each.get('input')) is str:
            arguments = dict(arguments or {}, **{each['input']: 'json'})
    for key, value in (arguments or {}).items():
        variables.append(dict(name=key, type=value, required=True))
    model = item.get('model')
    if type(model) is dict:
        model = {key: value for key, value in model.items() if key in {'name', 'id', 'model', 'transport'}}
    return dict(kind='prompt', symbol=name, declared_version=item.get('version'),
                template=item.get('template'), messages=item.get('messages'), description=item.get('description', ''),
                input_variables=variables, input_diagnostic=diagnostic,
                output_schema=item.get('response_schema', item.get('json_schema', item.get('schema'))),
                response_format=item.get('response_format'), model=model,
                defaults={key: item[key] for key in ('schema_retries', 'input_mode', 'message_limits') if key in item})
