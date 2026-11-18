"""Read presentation material from passive compiler declarations, never actors."""
import ast
from collections.abc import Mapping
import hashlib
import json
from pathlib import Path
from types import FunctionType

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


def _implementation(declaration, root, sources):
    spec = declaration.callable_spec
    target = spec.target if spec is not None else None
    identity = declaration.code_identity.get('target', {})
    prompts = _prompts(spec)
    if prompts:
        predicates = [settings['when'] for _, _, _, settings in prompts if type(settings.get('when')) is FunctionType]
        if len(predicates) == 1:
            target = predicates[0]
            path = Path(target.__code__.co_filename)
            relative = path.relative_to(root).as_posix() if path.is_relative_to(root) else None
            identity = dict(module=target.__module__, name=target.__qualname__, source_hash=sources.get(relative))
    is_class = type(target) is type
    method = vars(target).get('__call__') if is_class else target
    if type(method) is not FunctionType or not isinstance(identity, Mapping):
        return unavailable('本节点没有已绑定的业务函数源码；原生算子可查看 API 调用与入参。')
    if identity.get('module') != target.__module__ or identity.get('name') != target.__qualname__:
        return unavailable('函数身份与编译声明不一致。')
    path = Path(method.__code__.co_filename)
    if not path.is_absolute() or not path.is_relative_to(root):
        return unavailable('函数不在本次项目源码范围内。')
    relative = path.relative_to(root).as_posix()
    expected = sources.get(relative)
    if not expected or (not is_class and identity.get('source_hash') != expected):
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
            if not is_class and start == line and node.name == target.__name__:
                candidates.append((node, start))
        elif is_class and isinstance(node, ast.ClassDef) and node.name == target.__name__:
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


def _prompts(spec, *, depth=0):
    from demiflow.data.plan import CallableSpec, OperatorLLMMapOp
    from demiflow.execution.dataflow_model_factory import PromptFactory, CacheFactory, RoutedPromptFactory
    from demiflow.operator_llm.model import PromptPack, PromptDefinition
    if type(spec) is not CallableSpec or depth > 4:
        return []
    args = spec.constructor_args
    if spec.target is CacheFactory and args:
        return _prompts(args[0], depth=depth + 1)
    if spec.target is RoutedPromptFactory and args and type(args[0]) is dict and len(args[0]) <= 32:
        return [(route + '/' + name, definition, operation, settings)
                for route, child in args[0].items()
                for name, definition, operation, settings in _prompts(child, depth=depth + 1)]
    if spec.target is PromptFactory and args:
        operation = args[0]
        if type(operation) is OperatorLLMMapOp and type(operation.config) is PromptPack:
            definition = operation.config.prompt_definitions.get(operation.prompt_name)
            if type(definition) is PromptDefinition:
                return [(operation.prompt_name, definition, operation, args[1] if len(args) > 1 and type(args[1]) is dict else {})]
    return []


def _prompt_detail(declaration):
    definitions = _prompts(declaration.callable_spec)
    if not definitions:
        return unavailable('这个节点没有声明 Prompt。')
    texts, metadata = [], []
    for name, definition, operation, settings in definitions:
        text = definition.template.source
        texts.append(text if len(definitions) == 1 else f'### {name}\n\n{text}')
        from demiflow.workbench.definition_contracts import prompt_definition
        contract = prompt_definition(name, dict(version=definition.version, template=text,
            response_schema=definition.response_schema, response_format=definition.response_format,
            input_mode=definition.input_mode, schema_retries=definition.schema_retries,
            message_limits=definition.message_limits, model=definition.model.name),
            arguments={key: kind.value for key, kind in definition.arguments.items()})
        metadata.append(safe_json(dict(name=name, version=definition.version, definition=contract,
            model=definition.model.name, transport=definition.model.transport,
            response_format=definition.response_format, response_schema=definition.response_schema,
            inputs=operation.inputs, output=operation.output, outputs=operation.outputs,
            max_requests=operation.max_requests, template_sha256=hashlib.sha256(text.encode()).hexdigest())))
    return material('\n\n'.join(texts), language='text',
                    metadata=metadata[0] if len(metadata) == 1 else {'routes': metadata})


def _configuration_detail(node, callsite=None):
    configuration = safe_json(node.kernel.configuration)
    public = {k: v for k, v in configuration.items() if k not in {'occurrence_id', 'program'}}
    title = '算子入参'
    # This is the public Lance API's immutable scan declaration, not a
    # pipeline-specific mapping or another input to execution.
    query = configuration.get('query')
    if node.kernel.kind == 'lance' and type(query) is dict and type(query.get('fields')) is dict:
        public = query['fields']
        title = '读取入参'
    call = callsite.get('metadata', {}) if callsite and callsite.get('availability') == 'available' else {}
    if call:
        public = {p['name']: p['value'] for p in call['arguments']}
        title = 'API 入参'
    return material(json.dumps(public, ensure_ascii=False, indent=2, allow_nan=False), language='json',
        metadata=dict(display_title=title,
            note='入参可以来自常量、函数、上游 Dataset 或外层配置字典。这里展示解析时绑定的值；不读取数据或展开任意对象。' if call else '未取得公开 API 入参绑定；下方仅为编译参数，包含默认值，不等同于调用源码。',
            arguments=call.get('arguments'), expansions=call.get('expansions', []), api=call.get('api'),
            engine_configuration=configuration))


def build_plan_details(compiled, *, sources, root, callsites=None, bounds=None):
    root = Path(root)
    if len(compiled.graph.nodes) > 256:
        raise ValueError('Plan materials exceed node budget')
    declarations = {d.occurrence_id: d for d in compiled.declarations}
    from .plan_schema import build_schema_details
    schemas = build_schema_details(compiled, bounds=bounds) if bounds is not None else {}
    result, total = {}, 0
    for node in compiled.graph.nodes:
        details = {kind: unavailable('本节点没有已核验的这类材料。') for kind in KINDS}
        declaration = declarations.get(node.occurrence_id)
        if declaration is not None:
            details['implementation'] = _implementation(declaration, root, sources)
            spec = declaration.callable_spec
            prompt_refs = _prompts(spec)
            target = spec.target if spec is not None else None
            details['implementation']['referenced'] = (type(target) is FunctionType
                or type(target) is type and not target.__module__.startswith('demiflow.')
                or any(type(settings.get('when')) is FunctionType for _, _, _, settings in prompt_refs))
            try:
                details['prompt'] = _prompt_detail(declaration)
            except ValueError as exc:
                # Presentation has a narrower budget than legal execution.
                # A missing material must not discard the actual compiled DAG.
                details['prompt'] = unavailable(str(exc))
            details['prompt']['referenced'] = bool(prompt_refs)
        details['callsite'] = (callsites.detail(node.occurrence_id) if callsites is not None
                               else unavailable('这份计划未记录精确 API 调用；不会按函数名猜测。'))
        details['schema'] = schemas.get(node.occurrence_id, unavailable('这份计划未提供 Schema 材料。'))
        call_meta = details['callsite'].get('metadata', {})
        schema_meta = details['schema'].get('metadata')
        if schema_meta is not None:
            schema_meta['declarations'] = [dict(parameter=p['name'], expression=p['expression'], **p['value'])
                for p in call_meta.get('arguments', []) if type(p.get('value')) is dict
                and p['value'].get('type') == 'Schema' and 'fields' in p['value']]
            details['schema'] = material(json.dumps(schema_meta, ensure_ascii=False, indent=2), language='json', metadata=schema_meta)

        try:
            details['configuration'] = _configuration_detail(node, details['callsite'])
        except ValueError as exc:
            details['configuration'] = unavailable(str(exc))
        for kind, detail in details.items():
            size = len(json.dumps(detail, ensure_ascii=False).encode())
            if size > 128 * 1024 or total + size > 4 * 1024**2 - 256 * 1024:
                details[kind] = unavailable('材料超过展示预算。', referenced=detail.get('referenced', False))
                size = len(json.dumps(details[kind], ensure_ascii=False).encode())
            total += size
        result[node.occurrence_id] = details
    return result
