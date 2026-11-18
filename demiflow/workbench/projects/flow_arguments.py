"""One current business argument document per flow, using existing workspace I/O."""
import ast
import hashlib

from demiflow.forge.platform import bounded_json, content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.workbench.graph.parameters import _decode, MAX_BYTES
from . import workspace as ws
from .definitions import signature, _safe_tree


def read(root, *, entry):
    root, control, _ = ws.load_workspace(root)
    target, symbol = ws.validate_entry(root, entry)
    paths = control.get('entries', {}).get(entry, [])
    if len(paths) > 1:
        raise ForgeError('此流程仍登记了多份参数。请在开发中明确唯一当前参数，页面不会猜选。', code='conflict')
    raw = read_bounded(target, 512 * 1024)
    nodes = [n for n in _safe_tree(raw).body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == symbol]
    if len(nodes) != 1:
        raise ForgeError('Flow entry is missing or ambiguous', code='invalid_request')
    fields = signature(nodes[0])
    if paths:
        path = paths[0]
        saved = read_bounded(ws.validate_parameter_path(root, path), MAX_BYTES)
        document = _decode(saved)
        arguments = document['arguments']
        revision = hashlib.sha256(saved).hexdigest()
    else:
        path, document, arguments, revision = None, None, {}, None
    names = {f['name'] for f in fields}
    unknown = sorted(set(arguments) - names)
    if unknown and nodes[0].args.kwarg is None:
        raise ForgeError('保存参数与入口不匹配：' + ', '.join(unknown), code='conflict')
    for field in fields:
        name = field['name']
        field['present'] = name in arguments or field['default_known']
        field['value'] = arguments.get(name, field.get('default'))
        field['origin'] = 'current' if name in arguments else 'default' if field['default_known'] else 'missing'
    result = dict(entry=entry, path=path, revision=revision,
                  source_sha256=hashlib.sha256(raw).hexdigest(), fields=fields,
                  writable=path is not None and not unknown and not any(f['positional_only'] for f in fields),
                  arguments=arguments, description=ast.get_docstring(nodes[0]) or '',
                  reason=None if path else '尚未绑定唯一的可保存参数；有默认值的流程仍可解析。')
    bounded_json(result, 128 * 1024)
    return result


def save(root, *, entry, revision, source_sha256, arguments, authorize):
    if type(arguments) is not dict:
        raise ForgeError('Arguments must be a JSON object', code='invalid_request')
    root, _, _ = ws.load_workspace(root)
    with ws.control_lock(root):
        authorize()
        state = read(root, entry=entry)
        if not state['writable']:
            raise ForgeError('此流程没有唯一可写的参数绑定。', code='operation_rejected')
        if state['revision'] != revision or state['source_sha256'] != source_sha256:
            raise ForgeError('流程或参数已变化，请核对当前值；你的草稿应保留。', code='conflict')
        fields = {f['name']: f for f in state['fields']}
        if set(arguments) - fields.keys():
            raise ForgeError('Unknown flow business parameter', code='invalid_request')
        for name, field in fields.items():
            if name not in arguments:
                if field['required']: raise ForgeError('缺少业务参数：' + name, code='invalid_request')
                continue
            actual = arguments[name]
            annotation = {'int': int, 'float': (int, float), 'str': str, 'bool': bool, 'dict': dict, 'list': list}.get(field['annotation'])
            expected = annotation or (type(field['value']) if field['present'] and field['value'] is not None else None)
            if expected and (type(actual) not in (expected if isinstance(expected, tuple) else (expected,))):
                raise ForgeError('参数类型不匹配：' + name, code='invalid_request')
        path = ws.validate_parameter_path(root, state['path'])
        previous_raw = read_bounded(path, MAX_BYTES)
        if hashlib.sha256(previous_raw).hexdigest() != revision:
            raise ForgeError('参数已被其他编辑更新；你的草稿应保留。', code='conflict')
        previous = _decode(previous_raw)
        document = dict(previous, arguments=arguments)
        _decode(bounded_json(document, MAX_BYTES))
        authorize()
        ws.write_control(path, document, expected=content_digest(previous))
    return read(root, entry=entry)
