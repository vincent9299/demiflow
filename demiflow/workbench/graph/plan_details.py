"""Read presentation material from passive compiler declarations, never actors."""
import ast
from collections.abc import Mapping
import hashlib
import json
from pathlib import Path

from demiflow.forge.platform_environment import read_bounded
from demiflow.workbench.projects import workspace as ws

KINDS = ('callsite', 'configuration', 'schema', 'implementation', 'prompt')
TEXT_BYTES = 64 * 1024


def unavailable(reason, *, referenced=False):
    return dict(referenced=referenced, availability='unavailable', reason=reason, source_basis='compiled_snapshot',
                path=None, sha256=None, start_line=None, end_line=None,
                language=None, text=None, truncated=False)


def material(text, *, language, path=None, sha256=None, start_line=None, end_line=None, metadata=None):
    raw = text.encode('utf-8')
    truncated = len(raw) > TEXT_BYTES
    text = raw[:TEXT_BYTES].decode('utf-8', errors='ignore') if truncated else text
    value = dict(availability='available', reason=None, source_basis='compiled_snapshot',
                 path=path, sha256=sha256 or hashlib.sha256(raw).hexdigest(),
                 start_line=start_line, end_line=end_line, language=language, text=text, truncated=truncated)
    if metadata is not None:
        value['metadata'] = metadata
    return value


def safe_json(value, *, depth=0, budget=None):
    """Only strict passive JSON; never stringify objects or resolve credentials."""
    budget = [8192] if budget is None else budget
    budget[0] -= 1
    if depth > 16 or budget[0] < 0:
        raise ValueError('材料超过结构预算')
    if type(value) is str:
        # URLs also occur in lists (paths, inputs), without a named URL field.
        # Apply this to passive configuration only, never the exact source text.
        import re
        if re.match(r'^[A-Za-z][A-Za-z0-9+.-]*://', value):
            from urllib.parse import urlsplit, urlunsplit
            parts = urlsplit(value)
            return urlunsplit((parts.scheme, parts.netloc.rsplit('@', 1)[-1], parts.path, '', ''))
        return value
    if value is None or type(value) in (bool, int, float):
        return value
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError('配置含非文本键')
            normalized = key.lower().replace('-', '_')
            secret = (normalized in {'api_key', 'token', 'password', 'secret', 'authorization', 'headers', 'env', 'environment',
                                     'credentials', 'storage_options', 'access_key', 'secret_key', 'private_key'}
                      or normalized.endswith(('_token', '_password', '_secret', '_api_key', '_access_key', '_secret_key', '_private_key')))
            # Endpoint query/userinfo may embed credentials. Names of environment
            # bindings remain useful; their values are never looked up.
            if secret:
                result[key] = '[不展示凭据]'
            else:
                result[key] = safe_json(item, depth=depth + 1, budget=budget)
        return result
    if type(value) in (tuple, list):
        return [safe_json(item, depth=depth + 1, budget=budget) for item in value]
    raise ValueError('配置不是可直接呈现的 JSON')


def _implementation(reference, root, sources, shared):
    if reference.availability != 'available':
        return unavailable('函数身份与编译声明不一致。' if reference.reason == 'identity_mismatch'
                           else '本节点没有已绑定的业务函数源码；原生算子可查看 API 调用与入参。')
    method = reference.function
    is_class = reference.kind == 'callable_class'
    path = Path(method.__code__.co_filename)
    if path.is_relative_to(root):
        relative = path.relative_to(root).as_posix()
        expected = sources.get(relative)
    elif path in shared:
        relative, expected = shared[path]
    else:
        return unavailable('函数不在本次项目源码范围内。')
    if not expected or (reference.kind == 'function' and reference.source_hash != expected):
        return unavailable('函数源码未绑定本次解析的文件摘要。')
    ws.no_symlinks(path)
    raw = read_bounded(path, 4 * 1024**2)
    if hashlib.sha256(raw).hexdigest() != expected:
        return unavailable('函数源码在解析期间发生变化。')
    # Parsing locates an already authenticated function; it never invents DAG
    # nodes or dataflow edges from syntax.
    tree = ast.parse(raw, filename=str(path))
    line = method.__code__.co_firstlineno
    candidates = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            start = min([node.lineno] + [d.lineno for d in node.decorator_list])
            if not is_class and start == line and node.name == reference.name:
                candidates.append((node, start))
        elif is_class and isinstance(node, ast.ClassDef) and node.name == reference.name:
            calls = [n for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == '__call__']
            if len(calls) == 1 and min([calls[0].lineno, *(d.lineno for d in calls[0].decorator_list)]) == line:
                candidates.append((node, min([node.lineno, *(d.lineno for d in node.decorator_list)])))
    if len(candidates) != 1:
        return unavailable('无法唯一定位编译函数的源码范围。')
    node, start = candidates[0]
    lines = raw.decode('utf-8-sig').splitlines(keepends=True)
    text = ''.join(lines[start - 1:node.end_lineno])
    from demiflow.workbench.definition_contracts import code_definition
    definition = code_definition(node, '')
    return material(text, language='python', path=relative,
                    sha256=expected, start_line=start, end_line=node.end_lineno, metadata=dict(definition=definition))


def _prompt_detail(prompts):
    if prompts.state == 'unavailable':
        return unavailable(prompts.reason)
    if prompts.state == 'none':
        return unavailable('这个节点没有声明 Prompt。')
    texts, metadata = [], []
    for item in prompts.prompts:
        name, definition = item.name, item.definition
        text = definition.template.source
        texts.append(text if len(prompts.prompts) == 1 else f'### {name}\n\n{text}')
        from demiflow.workbench.definition_contracts import prompt_definition
        contract = prompt_definition(name, dict(version=definition.version, template=text,
            response_schema=definition.response_schema, response_format=definition.response_format,
            input_mode=definition.input_mode, schema_retries=definition.schema_retries,
            message_limits=definition.message_limits, model=definition.model.name),
            arguments={key: kind.value for key, kind in definition.arguments.items()})
        metadata.append(safe_json(dict(name=name, version=definition.version, definition=contract,
            model=definition.model.name, transport=definition.model.transport,
            response_format=definition.response_format, response_schema=definition.response_schema,
            inputs=item.inputs, output=item.output, outputs=item.outputs,
            max_requests=item.max_requests, template_sha256=hashlib.sha256(text.encode()).hexdigest())))
    return material('\n\n'.join(texts), language='text',
                    metadata=metadata[0] if len(metadata) == 1 else {'routes': metadata})


def _configuration_detail(projected, callsite=None):
    if projected.availability != 'available':
        return unavailable(projected.reason)
    configuration = safe_json(projected.engine_configuration)
    public = safe_json(projected.arguments)
    title = '读取入参' if projected.argument_kind == 'read' else '算子入参'
    call = callsite.get('metadata', {}) if callsite and callsite.get('availability') == 'available' else {}
    if call:
        public = {p['name']: p['value'] for p in call['arguments']}
        title = 'API 入参'
    return material(json.dumps(public, ensure_ascii=False, indent=2, allow_nan=False), language='json',
        metadata=dict(display_title=title,
            note='入参可以来自常量、函数、上游 Dataset 或外层配置字典。这里展示解析时绑定的值；不读取数据或展开任意对象。' if call else '未取得公开 API 入参绑定；下方仅为编译参数，包含默认值，不等同于调用源码。',
            arguments=call.get('arguments'), expansions=call.get('expansions', []), api=call.get('api'),
            engine_configuration=configuration))


def build_plan_details(compiled, *, sources, root, callsites=None, bounds=None, shared_sources=None):
    root = Path(root)
    from demiflow.workbench.python_sources import bound_files
    shared = {origin: (path.as_posix(), digest)
              for _, origin, path, digest in bound_files({'shared_sources': shared_sources or {}})}
    if len(compiled.graph.nodes) + len(compiled.graph.jobs) > 256:
        raise ValueError('Plan materials exceed node budget')
    if callsites is not None:
        callsites.check_compilation(compiled.graph)
    from demiflow.execution.host.inspection import compiled_node_materials
    projected = compiled_node_materials(compiled, bounds=bounds)
    from .plan_schema import build_schema_details
    schemas = build_schema_details(projected) if bounds is not None else {}
    result, total = {}, 0
    for occurrence, native in projected.items():
        details = {kind: unavailable('本节点没有已核验的这类材料。') for kind in KINDS}
        details['implementation'] = _implementation(native.implementation, root, sources, shared)
        details['implementation']['referenced'] = native.implementation.referenced
        prompts = native.prompts
        if prompts is not None:
            try:
                details['prompt'] = _prompt_detail(prompts)
            except ValueError as exc:
                # Presentation cannot turn an accepted compilation into failure.
                details['prompt'] = unavailable(str(exc))
            details['prompt']['referenced'] = prompts.state != 'none'
        details['callsite'] = (callsites.detail(occurrence) if callsites is not None
                               else unavailable('这份计划未记录精确 API 调用；不会按函数名猜测。'))
        details['schema'] = schemas.get(occurrence, unavailable('这份计划未提供 Schema 材料。'))
        call_meta = details['callsite'].get('metadata', {})
        schema_meta = details['schema'].get('metadata')
        if schema_meta is not None:
            schema_meta['declarations'] = [dict(parameter=p['name'], expression=p['expression'], **p['value'])
                for p in call_meta.get('arguments', []) if type(p.get('value')) is dict
                and p['value'].get('type') == 'Schema' and 'fields' in p['value']]
            details['schema'] = material(json.dumps(schema_meta, ensure_ascii=False, indent=2), language='json', metadata=schema_meta)

        try:
            details['configuration'] = _configuration_detail(native.configuration, details['callsite'])
        except ValueError as exc:
            details['configuration'] = unavailable(str(exc))
        for kind, detail in details.items():
            size = len(json.dumps(detail, ensure_ascii=False).encode())
            if size > 128 * 1024 or total + size > 4 * 1024**2 - 256 * 1024:
                details[kind] = unavailable('材料超过展示预算。', referenced=detail.get('referenced', False))
                size = len(json.dumps(details[kind], ensure_ascii=False).encode())
            total += size
        result[occurrence] = details
    return result
