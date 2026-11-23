"""Pure prompt contracts and independent facts of the native prompt actor.

No file, environment, network, service or candidate callback is read here.
"""

import math
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from urllib.parse import urlsplit

from ..data.flow_contract import scalar, field, union_type, require_fields
from .parser import parse_prompt_pack, resolve_prompt


def _value(value):
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {f.name: _value(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, Mapping):
        return {k: _value(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_value(v) for v in value]
    return value


def normalize_prompt(arguments):
    from ..data.plan import normalize_bound_inputs, normalize_outputs
    result = dict(arguments)
    result['prompt'] = str(result['prompt'] or '').strip()
    result['inputs'] = normalize_bound_inputs(result['inputs'])
    result['outputs'] = normalize_outputs(result['outputs'])
    if type(result['concurrency']) is not int or result['queue_depth'] is not None and type(result['queue_depth']) is not int:
        raise ValueError('Prompt concurrency and queue depth must be integers')
    if result['concurrency'] < 1 or result['queue_depth'] is not None and result['queue_depth'] < 1:
        raise ValueError('Prompt concurrency and queue depth must be positive')
    result['options'] = dict(result['options'] or {})
    if not result['prompt'] or type(result['config']) is not str:
        raise ValueError('Managed prompt requires a named prompt and sealed configuration file')
    if (result['output'] is None) == (result['outputs'] is None):
        raise ValueError('Prompt requires exactly one of output or outputs')
    if any(result.get(k) is not None for k in ('service', 'request_gate', 'routes', 'route')) or result['isolate_route_failures']:
        raise ValueError('This managed prompt contract requires a fixed external HTTP endpoint without routing or service construction')
    from ..inference import request_admission_config
    result['request_policy'] = request_admission_config(result.get('request_policy'), concurrency=result['concurrency'])
    if type(result['max_requests']) is not int or not 0 <= result['max_requests'] <= 2**40:
        raise ValueError('Managed prompt requires a finite nonnegative request budget <= 2**40')
    from .tokens import budget_declaration
    result['token_budget'] = budget_declaration(result['token_budget'])
    if result['catch']:
        raise ValueError('Managed prompt errors use the native error_output contract')
    return result


def prompt_resources(arguments):
    resources = [{'kind': 'config', 'parameter': 'config', 'path': arguments['config']}]
    if (arguments.get('options') or {}).get('object_store'):
        resources.append(dict(kind='objects',parameter='objects',root=arguments['options']['object_store'],
            relocations=[],layout='sha256_prefix2',access='write'))
    budget = arguments.get('token_budget')
    if budget and budget['unit'] == 'tokens':
        profile = budget['profile']
        if 'encoding_file' not in profile:
            raise ValueError('Managed token counting requires a local, hash-bound encoding file; implicit downloads are not allowed')
        resources.append({'kind': 'file', 'parameter': 'tokenizer', 'path': profile['encoding_file'],
                          'format': 'tiktoken_bpe_v1', 'sha256': profile['encoding_sha256'], 'max_bytes': 16 * 1024**2})
    return resources


def _schema_type(schema, depth=0):
    if depth > 16:
        raise ValueError('Prompt response schema exceeds type depth')
    kind = schema.get('type')
    if isinstance(kind, list):
        return union_type(*(_schema_type({**schema, 'type': k}, depth + 1) for k in kind))
    if kind == 'object':
        additional = schema.get('additionalProperties')
        if type(additional) is dict and not schema.get('properties'):
            return {**scalar('dict'), 'key': scalar('str'), 'value': _schema_type(additional, depth + 1)}
        if not schema.get('properties') and additional is not False:
            raise ValueError('Open prompt mappings require an explicit value schema')
        return {**scalar('record'), 'fields': {k: field(_schema_type(v, depth + 1), required=k in schema.get('required', [])) for k, v in schema.get('properties', {}).items()}}
    if kind == 'array':
        return {**scalar('list'), 'item': _schema_type(schema['items'], depth + 1)}
    if kind == 'number':
        return union_type(scalar('float'), scalar('int'))
    types = {'string': 'str', 'integer': 'int', 'boolean': 'bool', 'null': 'none'}
    if kind not in types:
        raise ValueError('Prompt response field requires a supported explicit JSON type')
    return scalar(types[kind])


def _native(prompt, inputs, output, outputs, options, maximum, when, call_output, error_output, budget, request_policy=None):
    from .tokens import budget_declaration
    result = {'contract': 'demiflow.prompt.http', 'prompt': _value(prompt), 'inputs': dict(inputs),
            'output': output, 'outputs': dict(outputs) if outputs is not None else None,
            'options': _value(options), 'max_requests': maximum, 'when': when,
            'call_output': call_output, 'error_output': error_output, 'token_budget': budget_declaration(budget)}
    # No-policy facts retain their historical bytes and identity.
    if request_policy is not None:
        result['request_policy'] = dict(request_policy)
    return result


def _systemone_result(prompt):
    """Known protocol fields; native response parsing also validates values/IDs."""
    number = union_type(scalar('float'), scalar('int'))
    if prompt.systemone['type'] == 'noul':
        answer = {**scalar('record'), 'fields': {'type': field(scalar('str')), 'noul': field(number)}}
    else:
        answer = {**scalar('record'), 'fields': {'type': field(scalar('str')), 'choice': field(scalar('str')),
            'probabilities': field({**scalar('dict'), 'key': scalar('str'), 'value': number})}}
    if 'for_each' in prompt.systemone:
        answer = {**scalar('record'), 'fields': {
            'answers': field({**scalar('dict'), 'key': scalar('str'), 'value': answer}),
            'errors': field({**scalar('dict'), 'key': scalar('str'), 'value': scalar('str')})}}
    return {'result': field(answer)}


def validate_prompt_inputs(prompt, schema, bindings):
    """One structural input contract for measurement and actual completion."""
    require_fields(schema, list(bindings.values()))
    if set(bindings) != set(prompt.arguments):
        raise ValueError('Prompt inputs must exactly cover template placeholders')
    for placeholder in prompt.template.placeholders:
        kind = schema[bindings[placeholder.name]]['type']
        if placeholder.kind.value == 'text' and (kind['kind'] not in {'str', 'int', 'float', 'bool'} or kind.get('nullable')):
            raise ValueError('Text placeholders require a non-null scalar field')
        if placeholder.kind.value in {'image', 'numbered_image'}:
            if prompt.response_format=='image' and (kind['kind']!='list' or kind['item']['kind']!='bytes'
                    or kind.get('nullable') or kind['item'].get('nullable')):
                raise ValueError('Image output placeholders require non-null list[bytes] fields')
            item = kind['item'] if kind['kind'] == 'list' else kind
            if item['kind'] not in {'str', 'bytes'}:
                raise ValueError('Image placeholder requires bounded data-URL/bytes fields')
    if prompt.model.transport == 'systemone' and 'for_each' in prompt.systemone:
        policy = prompt.systemone['for_each']
        items = schema[bindings[policy['input']]]['type']
        if items['kind'] != 'list' or items['item']['kind'] != 'record':
            raise ValueError('SystemOne questions require a typed list of records')
        from .template import compile_template
        template = compile_template(prompt.systemone['instructions'])
        require_fields(items['item']['fields'], [policy['key'], *template.arguments])
        if items['item']['fields'][policy['key']]['type']['kind'] != 'str':
            raise ValueError('SystemOne question identity must be a string')


def _trace_type():
    fields_ = {name: field(scalar('str'), required=False) for name in ('request_id', 'journal_path', 'kind', 'state')}
    fields_.update(reused=field(scalar('bool'), required=False), attempt=field(scalar('int'), required=False))
    return {**scalar('record'), 'fields': fields_}


def analyze_prompt(arguments, *, inputs, callables, resources):
    from .http_options import validate_http_options
    a = arguments
    pack = parse_prompt_pack(resources['config']['text'])
    prompt = resolve_prompt(pack, a['prompt'])
    model = prompt.model
    image=prompt.response_format=='image'
    a={**a,'queue_depth':(1 if image else a['concurrency']) if a['queue_depth'] is None else a['queue_depth']}
    if model.transport not in {'openai_compatible', 'azure_openai', 'systemone'} or model.deployment is not None or model.base_url_env:
        raise ValueError('Managed model requires an explicit HTTP endpoint; environment endpoints and owned services have no binding yet')
    url = urlsplit(model.base_url)
    if url.scheme not in {'http', 'https'} or not url.hostname or url.username or url.password or url.fragment or url.query:
        raise ValueError('Model endpoint must be a normalized HTTP(S) URL without embedded credentials')
    if prompt.response_format not in {'json','image'} or prompt.input_mode != 'template' or set(prompt.input_modalities) - {'text', 'image'}:
        raise ValueError('Managed prompt requires JSON/image responses and a text/image template')
    if len(prompt.template.placeholders) > 64 or len(prompt.template.source.encode()) > 65536:
        raise ValueError('Prompt template exceeds the managed rendering budget')
    allowed = {'timeout_s', 'max_request_bytes', 'max_response_bytes', 'max_event_bytes', 'trust_env',
               'collect_journal_totals', 'max_connections', 'max_keepalive_connections', 'model_revision', 'input_limits',
               'connect_timeout_s', 'read_timeout_s', 'write_timeout_s', 'pool_timeout_s', 'http_error_retry'}
    options = a['options']
    if image:
        from .media import execution_options, reserved_media_bytes, media_limits
        if type(model.provider.get('revision')) is not str or not model.provider['revision'].strip():
            raise ValueError('Managed image requires an explicit provider revision')
        if a['token_budget'] or not a['call_output'] or not a['error_output']:
            raise ValueError('Image output requires call/error fields and no token budget')
        if not 1<=a['concurrency']<=8 or not 1<=a['queue_depth']<=8:
            raise ValueError('Image concurrency/queue depth must be 1..8')
        options=execution_options(prompt,options,a['max_requests'])
        options['object_store']=resources['objects']['root']
    elif set(options) - allowed:
        raise ValueError('Prompt option has no bounded managed contract')
    validate_http_options(options)
    if options.get('trust_env') is not False:
        raise ValueError('Managed HTTP requires trust_env=False')
    deadline = options.get('timeout_s')
    if type(deadline) not in (int, float) or not 0 < deadline <= 3600:
        raise ValueError('HTTP deadline must be finite within deployment ceilings')
    if not image and (model.transport == 'systemone' or 'image' in prompt.input_modalities) and not options.get('model_revision'):
        raise ValueError('SystemOne/image input requires an explicit model_revision binding')
    if not image and 'image' in prompt.input_modalities and 'input_limits' not in options:
        raise ValueError('Image input requires explicit count and byte limits')
    budget = a['token_budget']
    if budget and budget['unit'] == 'tokens' and (model.name not in budget['profile']['models'] or 'image' in prompt.input_modalities):
        raise ValueError('Token profile must bind this text-only model; image tokens require their own provider contract')
    for name in ('max_request_bytes', 'max_response_bytes'):
        if type(options.get(name)) is not int or not 1024 <= options[name] <= 64 * 1024**2:
            raise ValueError(name + ' must be 1 KiB..64 MiB')
    if options.get('max_connections', a['concurrency']) > a['concurrency'] or options.get('max_keepalive_connections', a['concurrency']) > a['concurrency']:
        raise ValueError('HTTP pool must fit node concurrency')
    schema = inputs[0]
    validate_prompt_inputs(prompt, schema, a['inputs'])
    response = _systemone_result(prompt) if model.transport == 'systemone' else _schema_type(prompt.response_schema)['fields']
    keys = prompt.response_keys
    destinations = {keys[0]: a['output']} if a['output'] is not None and len(keys) == 1 else a['outputs']
    if destinations is None or set(destinations) != set(keys):
        raise ValueError('Prompt outputs must map every required response field')
    names = list(destinations.values()) + [a[n] for n in ('call_output', 'error_output') if a[n] is not None]
    if any(type(n) is not str or not n for n in names) or len(set(names)) != len(names):
        raise ValueError('Prompt destinations must be distinct nonempty fields')
    when = any(c['name'] == 'when' for c in callables)
    conditional = when if image else when or a['error_output'] is not None
    result = dict(schema)
    for source, target in destinations.items():
        typ = response[source]['type']
        if image and a['error_output'] is not None:
            typ={**typ,'nullable':True}
        if conditional and target in schema:
            typ = union_type(typ, schema[target]['type'])
        result[target] = field(typ, required=not conditional or target in schema and schema[target]['required'])
    for name in ('call_output', 'error_output'):
        target = a[name]
        if target is not None:
            if target in schema:
                raise ValueError('Prompt evidence/error destination cannot overwrite an input field')
            typ = _trace_type() if name == 'call_output' else {**scalar('record'), 'fields': {
                'category': field(scalar('str')), 'type': field(scalar('str')), 'detail': field(scalar('str')),
                'call': field(dict(_trace_type(), nullable=True))}}
            if image:
                typ=dict(_trace_type(),nullable=True) if name=='call_output' else scalar('str',nullable=True)
            result[target] = field(typ, required=False)
    return {'fields': result, 'cardinality': 'one', 'order': 'unspecified', 'driver': 'stream', 'kind': 'async_map',
            'callable_form': 'awaitable', 'placement': 'inline', 'scope': 'stage',
            'native': _native(prompt, a['inputs'], a['output'], a['outputs'], options, a['max_requests'], when, a['call_output'], a['error_output'], budget, a.get('request_policy')),
            'stream_arguments': dict(concurrency=a['concurrency'], queue_depth=a['queue_depth'], catch=[], label=a['label'] or a['prompt'], hard_timeout=None, execution='inline'),
            'reserved_bytes': (reserved_media_bytes(options,a['concurrency']) if image else a['concurrency'] * 8 * (options['max_request_bytes'] + options['max_response_bytes']))
                              + (64 * resources['tokenizer']['size'] + 64 * 1024**2 if 'tokenizer' in resources else 0),
            'resources': [{'kind': 'network', 'endpoint': model.base_url}, {'kind': 'credential', 'name': model.api_key_env}]
                         +([resources['objects'],{'kind':'network','source':'provider_image_url',
                             'schemes':['http','https'],'max_bytes':media_limits(options)['max_image_bytes'],
                             'credentials':False,'redirects':False}] if image else []),
            'drain_timeout_s': deadline + 1,
            'required_distributions': ['httpx', 'requests'] + (['pillow'] if image else []) + (['tiktoken'] if budget and budget['unit'] == 'tokens' else []),
            'question_budget': {'per_request': prompt.systemone.get('for_each', {}).get('max_questions', 1),
                                'scope': 'each_request_including_retries'} if model.transport == 'systemone' else None,
            'request_budget': {'scope': 'execution', 'maximum': a['max_requests']}}


def prompt_facts(actor):
    op, runtime = actor._operation, actor._runtime
    prompt = resolve_prompt(op.config, op.prompt_name)
    from ..execution.dataflow_prompt_service import prompt_service_declaration
    prompt_service_declaration(actor)
    from ..execution.adaptive_requests import AdaptiveRequestGate
    from ..inference import request_admission_config
    declaration = getattr(actor, '_request_policy', None)
    policy = request_admission_config(dict(declaration), concurrency=runtime.concurrency) if declaration is not None else None
    gate = runtime.request_gate
    gate_matches = gate is None if policy is None else (
        type(gate) is AdaptiveRequestGate
        and gate is getattr(actor, '_owned_request_gate', None)
        and gate.policy == policy and gate.concurrency == policy['max_concurrency']
        and gate.state_path is None and gate.failures == 5 and gate.interval_s == 0)
    if (_value(prompt) != _value(resolve_prompt(runtime.config, op.prompt_name)) or _value(prompt) != _value(actor._prompt)
            or op.options != runtime.options or runtime.coordinator.max_requests != op.max_requests
            or actor.environment is not None
            or not gate_matches
            or runtime._clients or runtime._service_owner is not None or runtime._service_owners):
        raise ValueError('Native prompt lowering/configuration copies differ')
    return _native(prompt, op.inputs, op.output, op.outputs, runtime.options, op.max_requests,
                   actor.when is not None, actor.call_output, actor.error_output, runtime.token_budget, policy)


def agent_facts(actor, environment):
    """Agent declaration: guarded prompt copies plus the exact native environment.

    prompt_facts keeps rejecting an environment on a prompt-only declaration.
    This separate domain validates and seals the environment explicitly.
    """
    from copy import copy
    from ..environment import OperatorEnvironment
    if type(environment) is not OperatorEnvironment or actor.environment is not environment:
        raise ValueError('Native agent environment differs from its declaration')
    environment.prompt(actor._prompt)
    projection = copy(actor)
    projection.environment = None
    result = prompt_facts(projection)
    return {**result, 'contract': 'demiflow.agent.node.v3',
            'environment': _value(environment.identity())}


def private_prompt_declaration(options):
    value = (options or {}).get('sqlite_journal')
    if value is None:
        return None
    if (type(value) is not dict or set(value) - {'path', 'max_requests', 'read_only', 'timeout_s'}
            or type(value.get('path')) is not str or not value['path']):
        raise ValueError('Private Prompt cache requires an explicit bounded SQLite declaration')
    limit = value.get('max_requests')
    if limit is not None and (type(limit) is not int or not 0 <= limit <= 2**40):
        raise ValueError('Invalid private Prompt request ceiling')
    if type(value.get('read_only', False)) is not bool:
        raise ValueError('Private Prompt read_only must be boolean')
    timeout = value.get('timeout_s', 30)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 3600:
        raise ValueError('Private Prompt lock deadline must be finite')
    if any(key in options for key in ('journal_dir', 'offline_dir', 'offline_store', 'codex_exec', 'codex_agent')):
        raise ValueError('Private Prompt cache requires a single explicit HTTP journal')
    return dict(value)
