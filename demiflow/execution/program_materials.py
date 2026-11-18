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
    from .contracts.dataflow import ProgramDeclaration
    from ..operator_llm.model import PromptDefinition


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
    from .contracts.dataflow import ProgramDeclaration
    from ..data.plan import CallableSpec, OperatorLLMMapOp
    from ..operator_llm.model import PromptPack, PromptDefinition
    from .dataflow_native import model_variant, CACHE, ROUTED_PROMPT, PROMPT

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
