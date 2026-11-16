"""Ephemeral, declaration-selected credentials for a Ray allocation.

Only authenticated native factory declarations supply names. Values are passed
straight to the actor runtime environment, never to a sealed artifact or journal.
No user instance, property or lifecycle hook is consulted.
"""
import re

from .contracts.dataflow_errors import DataflowError


def native_environment_names(spec, *, depth=0):
    from ..data.plan import CallableSpec, OperatorLLMMapOp
    from ..operator_llm.model import PromptPack, PromptDefinition, PromptModel
    from ..embeddings.model import EmbeddingModel
    from .dataflow_model_factory import PromptFactory, EmbeddingFactory, CacheFactory, RoutedPromptFactory
    if type(spec) is not CallableSpec:
        return ()
    if depth > 4:
        raise DataflowError('BUDGET_EXCEEDED', 'Native environment declaration exceeds depth bound')
    args = spec.constructor_args
    if spec.target is CacheFactory:
        return native_environment_names(args[0], depth=depth + 1)
    if spec.target is RoutedPromptFactory:
        # Read only the exact passive declarations authenticated by the native
        # program pin. Route actors/clients are never instantiated here, and
        # values remain ephemeral allocation data rather than sealed identity.
        children = args[0] if len(args) == 5 else None
        if (type(children) is not dict or not 1 <= len(children) <= 32
                or any(type(name) is not str or not name
                       or type(child) is not CallableSpec or child.target is not PromptFactory
                       for name, child in children.items())):
            raise DataflowError('INVALID_CONTRACT', 'Native routed prompt environment requires original passive child declarations')
        names = tuple(sorted({name for child in children.values()
            for name in native_environment_names(child, depth=depth + 1)}))
    elif spec.target is PromptFactory:
        operation = args[0]
        if type(operation) is not OperatorLLMMapOp or type(operation.config) is not PromptPack:
            raise DataflowError('INVALID_CONTRACT', 'Native prompt environment requires the original passive declaration')
        pack = operation.config
        if (any(type(prompt) is not PromptDefinition or type(prompt.model) is not PromptModel
                for prompt in pack.prompts)):
            raise DataflowError('INVALID_CONTRACT', 'Native prompt environment requires platform model declarations')
        names = pack.required_environment_names
    elif spec.target is EmbeddingFactory:
        declaration = args[0]
        model = declaration.get('model') if type(declaration) is dict else None
        if type(model) is not EmbeddingModel:
            raise DataflowError('INVALID_CONTRACT', 'Native embedding environment requires the original model declaration')
        names = (model.api_key_env,) if model.api_key_env else ()
    else:
        return ()
    if len(names) > 128 or any(type(name) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,255}', name) for name in names):
        raise DataflowError('INVALID_CONTRACT', 'Native environment names must be bounded identifiers')
    return tuple(sorted(set(names)))


def allocation_environment(assignment, *, store, bounds):
    from .dataflow_seal import load_sealed_dataflow, verify_dataflow_program
    from .environment import select_pipeline_environment
    from .recovery_contract import document
    graph, physical, pin = load_sealed_dataflow(assignment.sealed,
        action_id=assignment.action_id, store=store, bounds=bounds)
    if physical.plan_id != assignment.plan_id or physical.plan_revision != assignment.plan_revision:
        raise DataflowError('FENCED', 'Native environment assignment differs from its sealed plan')
    owned = {occ for stage in assignment.stages for occ in stage.occurrences}
    names = set()
    for node in graph.nodes:
        # Do not deserialize arbitrary user source/callable objects as an env
        # preflight. These native kinds are produced by the platform compiler.
        if node.occurrence_id not in owned or node.kernel.kind not in {'prompt', 'agent', 'embedding'} or node.program_ref is None:
            continue
        program = verify_dataflow_program(node.program_ref,
            expected_pin=pin['programs'][node.occurrence_id], store=store, bounds=bounds)
        if program.kind == 'callable':
            names.update(native_environment_names(program.value))
    if len(names) > 128:
        raise DataflowError('BUDGET_EXCEEDED', 'Native environment name count exceeds allocation bound')
    values = select_pipeline_environment(sorted(names), required_names=names)
    try:
        document(values, min(bounds.max_control_bytes, 65536))
    except Exception as exc:
        raise DataflowError('BUDGET_EXCEEDED', 'Native worker environment exceeds control budget') from None
    return values
