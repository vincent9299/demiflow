"""Passive presentation of the actual compiled native program declaration.

Factory layout belongs to execution. Hosts receive selected PromptDefinition
values and bindings, never factory arguments, services or live actors. References
live only for this compilation; callers must not mutate or persist these views.
"""
from __future__ import annotations

from dataclasses import dataclass
from types import FunctionType
from typing import Any, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from demiflow.execution.contracts.dataflow import ProgramDeclaration
    from demiflow.operator_llm.model import PromptDefinition


@dataclass(frozen=True)
class PromptMaterial:
    name: str
    definition: PromptDefinition
    # Preserve the actual declaration shape; do not normalize/copy bindings here.
    inputs: Any
    output: str | None
    outputs: Any
    max_requests: int | None
    predicate: FunctionType | None


@dataclass(frozen=True)
class ProgramPromptMaterials:
    occurrence_id: str
    state: Literal['none', 'available', 'unavailable']
    prompts: tuple[PromptMaterial, ...] = ()
    reason: str | None = None


def program_prompt_materials(declaration: ProgramDeclaration, *, max_depth=4, max_prompts=32):
    """Read selected compiled prompts without loading files or invoking user code.

    Bounds apply before traversal/allocation: depth <= 4, emitted prompts <= 32,
    total visited wrappers <= 161, route/name <= 2048 characters, and at most 256
    definitions per pack. Large template/schema values are retained by reference;
    the host still owns text/JSON budgets and redaction. Oversize/unknown native
    layouts report unavailable materials without rejecting the compiled graph.
    """
    from demiflow.execution.contracts.dataflow import ProgramDeclaration
    from demiflow.data.plan import CallableSpec, OperatorLLMMapOp
    from demiflow.operator_llm.model import PromptPack, PromptDefinition
    from demiflow.execution.compiler_support.dataflow_native import model_variant, CACHE, ROUTED_PROMPT, PROMPT

    if type(declaration) is not ProgramDeclaration:
        raise TypeError('program_prompt_materials requires a compiled ProgramDeclaration')
    if type(max_depth) is not int or not 0 <= max_depth <= 4:
        raise ValueError('Prompt material max_depth must be in 0..4')
    if type(max_prompts) is not int or not 1 <= max_prompts <= 32:
        raise ValueError('Prompt material max_prompts must be in 1..32')

    prompts = []
    visits = 0

    def visit(spec, depth=0, prefix=''):
        nonlocal visits
        visits += 1
        if depth > max_depth or visits > max_prompts * (max_depth + 1) + 1:
            raise ValueError('Prompt 声明超过展示遍历预算。')
        if type(spec) is not CallableSpec:
            return
        variant = model_variant(spec)
        if variant is None:
            return
        args = spec.constructor_args
        if type(args) is not tuple or not args:
            raise ValueError('Prompt 原生声明材料不可用。')
        if variant == CACHE:
            visit(args[0], depth + 1, prefix)
        elif variant == ROUTED_PROMPT:
            children = args[0]
            if type(children) is not dict or len(children) > max_prompts:
                raise ValueError('Prompt 路由超过展示预算或声明不可用。')
            for route, child in children.items():
                if type(route) is not str or len(prefix) + len(route) + 1 > 2048:
                    raise ValueError('Prompt 路由名称超过展示预算。')
                visit(child, depth + 1, prefix + route + '/')
        elif variant == PROMPT:
            operation = args[0]
            if type(operation) is not OperatorLLMMapOp or type(operation.config) is not PromptPack:
                raise ValueError('Prompt 原生声明材料不可用。')
            definitions = operation.config.prompts
            if type(definitions) is not tuple or len(definitions) > 256 or len(prompts) >= max_prompts:
                raise ValueError('Prompt 定义数量超过展示预算。')
            name = operation.prompt_name
            if type(name) is not str or len(prefix) + len(name) > 2048:
                raise ValueError('Prompt 名称超过展示预算。')
            # Avoid building the whole pack's prompt_definitions mapping. This
            # selects from the exact pack the compiler already validated.
            selected = None
            for definition in definitions:
                if type(definition) is not PromptDefinition:
                    raise ValueError('Prompt 编译定义材料不可用。')
                if definition.name == name:
                    selected = definition
            if selected is None:
                raise ValueError('Prompt 编译定义材料不可用。')
            settings = args[1] if len(args) > 1 and type(args[1]) is dict else {}
            predicate = settings.get('when')
            prompts.append(PromptMaterial(prefix + name, selected, operation.inputs, operation.output,
                operation.outputs, operation.max_requests, predicate if type(predicate) is FunctionType else None))

    try:
        visit(declaration.callable_spec)
    except ValueError as exc:
        return ProgramPromptMaterials(declaration.occurrence_id, 'unavailable', reason=str(exc))
    return ProgramPromptMaterials(declaration.occurrence_id, 'available' if prompts else 'none', tuple(prompts))


@dataclass(frozen=True)
class NodeSchemaMaterial:
    availability: Literal['available', 'unavailable']
    reason: str | None
    output: dict | None
    inputs: tuple[dict, ...] = ()
    bindings: Any = None
    read_fields: tuple[str, ...] | None = None


@dataclass(frozen=True)
class NodeConfigurationMaterial:
    availability: Literal['available', 'unavailable']
    reason: str | None
    engine_configuration: Any = None
    arguments: Any = None
    argument_kind: Literal['read', 'kernel', 'fit'] | None = None


@dataclass(frozen=True)
class NodeImplementationMaterial:
    availability: Literal['available', 'unavailable']
    reason: str | None
    referenced: bool = False
    kind: Literal['function', 'callable_class', 'predicate'] | None = None
    function: FunctionType | None = None
    name: str | None = None
    source_hash: str | None = None


@dataclass(frozen=True)
class CompiledNodeMaterials:
    occurrence_id: str
    kind: str
    schema: NodeSchemaMaterial
    configuration: NodeConfigurationMaterial
    implementation: NodeImplementationMaterial
    prompts: ProgramPromptMaterials | None


_MATERIAL_BYTES = 128 * 1024
_TOTAL_BYTES = 4 * 1024**2 - 256 * 1024


def _display_json(value, total, *, remaining=None, visits=None, depth=0):
    """Bounded passive JSON copy; the host still owns redaction and UI policy."""
    import json
    import math
    from types import MappingProxyType
    remaining = [_MATERIAL_BYTES] if remaining is None else remaining
    visits = [8192] if visits is None else visits
    visits[0] -= 1
    if depth > 16 or visits[0] < 0:
        raise ValueError('Material exceeds structural display budget')

    def charge(size):
        if size > remaining[0] or size > total[0]:
            raise ValueError('Material exceeds byte display budget')
        remaining[0] -= size
        total[0] -= size

    if value is None or type(value) in (bool, int, float, str):
        if type(value) is float and not math.isfinite(value):
            raise ValueError('Material JSON is not finite')
        if type(value) is str and len(value) > min(remaining[0], total[0]):
            raise ValueError('Material exceeds byte display budget')
        charge(len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')))
        return value
    if type(value) in (dict, MappingProxyType):
        if len(value) > visits[0]:
            raise ValueError('Material exceeds structural display budget')
        charge(2 + 2 * len(value))
        result = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError('Material JSON requires text keys')
            _display_json(key, total, remaining=remaining, visits=visits, depth=depth + 1)
            result[key] = _display_json(item, total, remaining=remaining, visits=visits, depth=depth + 1)
        return result
    if type(value) in (tuple, list):
        if len(value) > visits[0]:
            raise ValueError('Material exceeds structural display budget')
        charge(2 + len(value))
        return [_display_json(item, total, remaining=remaining, visits=visits, depth=depth + 1)
                for item in value]
    raise ValueError('Material is not passive JSON')


def _implementation_material(declaration, prompts):
    if declaration is None:
        return NodeImplementationMaterial('unavailable', 'no_business_source')
    from demiflow.data.plan import CallableSpec
    from collections.abc import Mapping
    spec = declaration.callable_spec
    target = spec.target if type(spec) is CallableSpec else None
    identity = declaration.code_identity.get('target', {})
    predicates = tuple(item.predicate for item in prompts.prompts if item.predicate is not None)
    is_class = type(target) is type
    module = target.__module__ if type(target) in (FunctionType, type) else None
    referenced = (type(target) is FunctionType
        or is_class and type(module) is str and not module.startswith('demiflow.')
        or bool(predicates))
    if len(predicates) == 1:
        function = predicates[0]
        return NodeImplementationMaterial('available', None, True, 'predicate', function,
                                          function.__name__)
    function = vars(target).get('__call__') if is_class else target
    if type(function) is not FunctionType or not isinstance(identity, Mapping):
        return NodeImplementationMaterial('unavailable', 'no_business_source', referenced)
    if identity.get('module') != module or identity.get('name') != target.__qualname__:
        return NodeImplementationMaterial('unavailable', 'identity_mismatch', referenced)
    digest = identity.get('source_hash')
    return NodeImplementationMaterial('available', None, referenced,
        'callable_class' if is_class else 'function', function, target.__name__,
        digest if type(digest) is str else None)


def compiled_node_materials(compiled, *, bounds):
    """Project real compiled materials without executing or exposing native layouts.

    Function/Prompt references are borrowed for this compilation only. Config
    JSON is opaque display content, never a second execution/schema contract.
    Missing material bounds disable schema only; they do not create fake limits.
    """
    from demiflow.execution.contracts.dataflow import Bounds, CompiledDataflow
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.compiler_support.dataflow_schema import node_schemas
    from types import MappingProxyType
    from itertools import islice
    if type(compiled) is not CompiledDataflow:
        raise TypeError('compiled_node_materials requires CompiledDataflow')
    if bounds is not None and type(bounds) is not Bounds:
        raise TypeError('compiled_node_materials bounds must be Bounds or None')
    graph = compiled.graph
    if len(graph.nodes) + len(graph.jobs) > 256:
        raise ValueError('Plan materials exceed node budget')
    nodes = {node.occurrence_id for node in graph.nodes}
    jobs = {job.occurrence_id for job in graph.jobs}
    declarations = {d.occurrence_id: d for d in compiled.declarations}
    if (len(nodes | jobs) != len(graph.nodes) + len(graph.jobs)
            or len(declarations) != len(compiled.declarations) or not set(declarations) <= nodes | jobs):
        raise ValueError('Material compilation identities differ')
    reason = 'schema_bounds_absent' if bounds is None else None
    schemas = {}
    if bounds is not None:
        try:
            schemas = node_schemas(graph, bounds=bounds)
        except DataflowError as exc:
            if exc.code != 'BUDGET_EXCEEDED':
                raise
            reason = 'schema_budget'
    total = [_TOTAL_BYTES]
    incoming = {key: [] for key in nodes}
    if reason is None:
        for edge in graph.edges:
            incoming[edge.consumer].append(edge)
    facts, reads, schema_reasons = {}, {}, {}
    for node in graph.nodes:
        key, config = node.occurrence_id, node.kernel.configuration
        query = config.get('query', {}) if node.kind == 'source' else {}
        fields = query.get('fields') if type(query) in (dict, MappingProxyType) else None
        columns = fields.get('columns') if type(fields) in (dict, MappingProxyType) else None
        columns = columns if type(columns) in (tuple, list) else None
        # Names are presentation facts, not a scan or proof of types.
        read = tuple(columns[:256]) if columns is not None and all(type(c) is str for c in columns[:256]) else None
        reads[key] = read
        if reason is not None:
            schema_reasons[key] = reason
            continue
        schema = schemas.get(key)
        if schema is not None:
            value = dict(status='declared', fields=[dict(name=f.name, type=str(f.type)[:1024], nullable=f.nullable)
                for f in islice(schema, 256)], truncated=len(schema) > 256)
        elif read is not None:
            value = dict(status='fields_only', fields=[dict(name=c, type=None, nullable=None) for c in read],
                         truncated=len(columns) > 256)
        else:
            value = dict(status='unknown', fields=[], truncated=False)
        try:
            facts[key] = _display_json(value, total)
        except ValueError as exc:
            schema_reasons[key] = str(exc)
    result = {}
    for node in graph.nodes:
        key, config = node.occurrence_id, node.kernel.configuration
        unavailable_reason = schema_reasons.get(key)
        if unavailable_reason is None:
            unavailable_reason = next((schema_reasons[e.producer] for e in incoming[key]
                                       if e.producer in schema_reasons), None)
        try:
            bindings = _display_json({k: config[k] for k in ('inputs', 'output', 'outputs') if k in config}, total)
        except ValueError as exc:
            bindings = None
            unavailable_reason = unavailable_reason or str(exc)
        schema_material = (NodeSchemaMaterial('unavailable', unavailable_reason, None, bindings=bindings)
            if unavailable_reason else NodeSchemaMaterial('available', None, facts[key],
                tuple(dict(port=e.input_port, producer=e.producer, schema=facts[e.producer]) for e in incoming[key]),
                bindings, reads[key]))
        try:
            remaining = [_MATERIAL_BYTES]
            configuration = _display_json(config, total, remaining=remaining)
            arguments = {k: v for k, v in configuration.items() if k not in ('occurrence_id', 'program')}
            kind = 'kernel'
            query = configuration.get('query')
            if node.kernel.kind == 'lance' and type(query) is dict and type(query.get('fields')) is dict:
                arguments, kind = query['fields'], 'read'
            config_material = NodeConfigurationMaterial('available', None, configuration, arguments, kind)
        except ValueError as exc:
            config_material = NodeConfigurationMaterial('unavailable', str(exc))
        declaration = declarations.get(key)
        prompts = program_prompt_materials(declaration) if declaration is not None else None
        implementation = _implementation_material(declaration, prompts)
        result[key] = CompiledNodeMaterials(key, node.kind, schema_material, config_material, implementation, prompts)
    for job in graph.jobs:
        key = job.occurrence_id
        try:
            config = _display_json(job.configuration, total)
            config_material = NodeConfigurationMaterial('available', None, config, config, 'fit')
        except ValueError as exc:
            config_material = NodeConfigurationMaterial('unavailable', str(exc))
        declaration = declarations.get(key)
        prompts = program_prompt_materials(declaration) if declaration is not None else None
        result[key] = CompiledNodeMaterials(key, 'fit', NodeSchemaMaterial(
            'unavailable', 'model_manifest_value', None), config_material,
            _implementation_material(declaration, prompts), prompts)
    return result
