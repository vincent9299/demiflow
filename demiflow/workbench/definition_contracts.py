"""Passive definition projections shared by current and snapshot readers."""
import ast
import copy
from demiflow.forge.platform import bounded_json


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
