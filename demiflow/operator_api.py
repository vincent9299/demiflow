"""Parse trusted YAML operator declarations; execute ordinary functions/actors.

There is no application registration catalog. Standard names are compatibility
shorthands for native contracts; custom implementations live under operators/.
Model arguments never select Python implementations.
"""
from copy import deepcopy
from importlib import import_module
import inspect
import json
from pathlib import Path
import re

from .schema import compile_schema, validate_instance


class OperatorCallError(ValueError):
    """A model-originated API error that can be returned for correction."""
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


# Native implementations own their contracts. No mutable registry or import-time
# registration; historical standard names keep the same resolved identity.
_STANDARD = {
    'read_documents': ('demiflow.collect.reading:read_documents', 'demiflow.collect.reading_api'),
    'map_embeddings': ('demiflow.embeddings.api:map_embeddings', 'demiflow.embeddings.api'),
    'search_vectors': ('demiflow.lance.search_api:search_vectors', 'demiflow.lance.search_api'),
}
_EMPTY = {'type': 'object', 'properties': {}, 'additionalProperties': False}
_FIELDS = {'fn', 'actor', 'init', 'description', 'version', 'arguments', 'fixed_schema',
           'fixed_arguments', 'bindings', 'validate_resources', 'validate_arguments',
           'requires_resources', 'execution', 'replay', 'result_images'}


def _json_budget(value, limit, label):
    size = 0
    for chunk in json.JSONEncoder(ensure_ascii=False, allow_nan=False).iterencode(value):
        size += len(chunk.encode('utf-8'))
        if size > limit:
            raise ValueError(label + ' exceeds ' + str(limit) + ' bytes')


def _load(path):
    if not isinstance(path, str) or not re.fullmatch(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*', path):
        raise ValueError('Operator entry must be a trusted module:name declaration')
    module, name = path.split(':')
    return getattr(import_module(module), name)


def _entry(path, *, standard=False):
    module = path.split(':', 1)[0] if isinstance(path, str) else ''
    if not standard and 'operators' not in module.split('.'):
        raise ValueError('Custom fn/actor and validators must be defined under operators/')
    target = _load(path)
    if not callable(target):
        raise TypeError('Operator entry must resolve to a callable')
    if not standard:
        # Reject re-exporting an unrelated function through an allowed module.
        if (target.__module__ != module or 'operators' not in
                Path(inspect.getfile(target)).resolve().parts):
            raise ValueError('Custom operator implementation must be defined in its operators/ module')
    return target


def definitions(operators):
    """Resolve a bounded mapping; legacy lists may select standard names only."""
    if isinstance(operators, (tuple, list)):
        if any(not isinstance(name, str) for name in operators) or len(set(operators)) != len(operators):
            raise ValueError('Operators must be distinct standard names')
        operators = {name: {} for name in operators}
    if not isinstance(operators, dict) or len(operators) > 64:
        raise ValueError('operators must declare at most 64 tools')
    _json_budget(operators, 256 * 1024, 'Operator declarations')
    resolved, settings = {}, {}
    for name, value in operators.items():
        if not isinstance(name, str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', name):
            raise ValueError('Invalid operator name')
        if not isinstance(value, dict) or set(value) - _FIELDS:
            raise ValueError('Unknown operator declaration fields: ' + name)
        selected = value.get('fn')
        standard_name = next((key for key, (entry, _) in _STANDARD.items() if entry == selected), None)
        if not value.get('actor') and selected is None and name in _STANDARD:
            standard_name = name
        base = deepcopy(import_module(_STANDARD[standard_name][1]).DEFINITION) if standard_name else {}
        if not base and not (set(value) & {'fn', 'actor'}):
            raise ValueError('Unknown standard operator; custom operators require fn or actor: ' + name)
        if 'fn' in value and 'actor' in value:
            raise ValueError('Choose exactly one of fn and actor')
        definition = {**base, **{k: deepcopy(v) for k, v in value.items()
                               if k not in {'fn', 'actor', 'fixed_arguments'}}, 'name': name}
        if 'actor' in value:
            definition['actor'] = value['actor']
            definition['function'] = None
        elif selected is not None:
            definition['function'] = selected
        for key, default in {'version': '1', 'bindings': {}, 'fixed_schema': _EMPTY,
                             'validate_resources': None, 'validate_arguments': None,
                             'requires_resources': False, 'replay': 'recorded', 'result_images': None}.items():
            definition.setdefault(key, deepcopy(default))
        is_actor = 'actor' in definition
        entry = definition['actor'] if is_actor else definition.get('function')
        target = _entry(entry, standard=bool(standard_name))
        if is_actor != inspect.isclass(target):
            raise TypeError('actor must name a callable class; fn must name a function')
        if not is_actor and 'init' in definition:
            raise ValueError('init is only valid for actor')
        if is_actor:
            initial = definition.setdefault('init', {})
            if not isinstance(initial, dict):
                raise ValueError('Actor init must be an object')
            _json_budget(initial, 65536, 'Actor init')
            inspect.signature(target).bind(**initial)  # No construction during parsing.
            target = target.__call__
        asynchronous = inspect.iscoroutinefunction(target)
        definition.setdefault('execution', 'async' if asynchronous else 'thread')
        if definition['execution'] not in ('async', 'thread') or (definition['execution'] == 'async') != asynchronous:
            raise ValueError('execution must match async fn/actor or synchronous thread execution')
        for key in ('description', 'version'):
            if not isinstance(definition.get(key), str) or not definition[key]:
                raise ValueError('Operator requires ' + key)
        if type(definition['requires_resources']) is not bool or definition['replay'] not in ('recorded', 'verify'):
            raise ValueError('Invalid resources/replay declaration')
        for key in ('arguments', 'fixed_schema'):
            schema = definition.get(key)
            if not isinstance(schema, dict) or schema.get('type') != 'object' or schema.get('additionalProperties') is not False:
                raise ValueError('Operator ' + key + ' requires a closed object schema')
            compile_schema(schema)
        bindings = definition['bindings']
        if not isinstance(bindings, dict) or any(not isinstance(k, str) or not isinstance(v, str) or
                v not in ('context', 'resources') and not re.fullmatch(r'limits\.[a-z_]+', v)
                for k, v in bindings.items()):
            raise ValueError('Invalid operator execution bindings')
        groups = [set(definition[k].get('properties', {})) for k in ('arguments', 'fixed_schema')] + [set(bindings)]
        if any(groups[i] & groups[j] for i in range(3) for j in range(i)):
            raise ValueError('Dynamic, fixed and platform-bound arguments must be disjoint')
        for key in ('validate_resources', 'validate_arguments'):
            if definition[key] is not None:
                # Only the unchanged native validator gets the native exemption.
                _entry(definition[key], standard=bool(base and definition[key] == base.get(key)))
        if definition['result_images'] is not None:
            from .operator_media import image_fields
            definition['result_images'] = image_fields(definition['result_images'])
        setting = {}
        if 'fixed_arguments' in value:
            setting['arguments'] = deepcopy(value['fixed_arguments'])
        if setting:
            settings[name] = setting
        resolved[name] = definition
    return resolved, settings


def tool_definition(definition):
    return {'name': definition['name'], 'description': definition['description'],
            'parameters': deepcopy(definition['arguments'])}


def validate_fixed(definition, fixed):
    validate_instance(fixed, definition['fixed_schema'], label=definition['name'] + ' fixed arguments')


def check_resources(definition, resources, limits):
    if definition['validate_resources'] is not None:
        _load(definition['validate_resources'])(resources, limits)


def prepare(definition, arguments, *, fixed, context, resources, limits):
    validate_instance(arguments, definition['arguments'], label=definition['name'] + ' arguments')
    validate_fixed(definition, fixed)
    if definition['validate_arguments'] is not None:
        _load(definition['validate_arguments'])(arguments, resources, limits)
    supplied = {'context': context, 'resources': resources}
    bound = {key: (getattr(limits, source[7:]) if source.startswith('limits.') else supplied[source])
             for key, source in definition['bindings'].items()}
    target = _load(definition.get('actor') or definition['function'])
    signature = inspect.signature(target.__call__ if 'actor' in definition else target)
    if 'actor' in definition:
        signature = signature.replace(parameters=list(signature.parameters.values())[1:])
    arguments = signature.bind(**arguments, **fixed, **bound)
    return target, arguments


def catalog_identity(resolved):
    """Preserve the original standard function identities and request cache keys."""
    return json.dumps([{k: v for k, v in d.items() if k != "description"} for d in resolved.values()],
                      ensure_ascii=False, sort_keys=True,
                      allow_nan=False, separators=(',', ':'))
