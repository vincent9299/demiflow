"""Environment implementation."""
from __future__ import annotations

import re
from demiflow.execution.contracts.dataflow_errors import DataflowError

import os
from typing import Mapping
from demiflow.execution.seal.pipeline_sources import uses_lance
from demiflow.operator_llm import load_referenced_prompt_packs
from demiflow.operator_llm.errors import PromptProviderUnavailableError
from demiflow.pipeline import discover_pipeline_definition


LANCE_STORAGE_CREDENTIAL_ENV_NAMES=frozenset({
    "AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY","AWS_SESSION_TOKEN","AWS_REGION","AWS_DEFAULT_REGION","AWS_PROFILE","AWS_SHARED_CREDENTIALS_FILE","AWS_CONFIG_FILE",
    "GOOGLE_APPLICATION_CREDENTIALS","GOOGLE_CLOUD_PROJECT",
    "AZURE_STORAGE_ACCOUNT_NAME","AZURE_STORAGE_ACCOUNT_KEY","AZURE_STORAGE_SAS_TOKEN","AZURE_CLIENT_ID","AZURE_TENANT_ID","AZURE_CLIENT_SECRET",
})

def prompt_environment_names(bundle_root)->tuple[str,...]:
    packs=load_referenced_prompt_packs(bundle_root)
    return tuple(sorted({name for pack in packs.values() for name in pack.required_environment_names}))

def pipeline_environment_names(bundle_root)->tuple[str,...]:
    definition=discover_pipeline_definition(__import__('pathlib').Path(bundle_root))
    names=set(prompt_environment_names(bundle_root))
    if uses_lance(bundle_root,definition.entrypoint): names.update(LANCE_STORAGE_CREDENTIAL_ENV_NAMES)
    return tuple(sorted(names))

def missing_required_environment_names(required_names,environment:Mapping[str,str]|None=None)->tuple[str,...]:
    source=os.environ if environment is None else environment
    return tuple(sorted(name for name in required_names if not str(source.get(name) or "").strip()))

def select_pipeline_environment(names,environment:Mapping[str,str]|None=None,*,required_names=())->dict[str,str]:
    source=os.environ if environment is None else environment
    missing=missing_required_environment_names(required_names, source)
    if missing:
        raise PromptProviderUnavailableError(
            "Operator LLM requires environment variables: "+", ".join(sorted(missing))
        )
    return {name:str(source[name]) for name in names if str(source.get(name) or "").strip()}

__all__=["LANCE_STORAGE_CREDENTIAL_ENV_NAMES","missing_required_environment_names","pipeline_environment_names","prompt_environment_names","select_pipeline_environment"]


def native_environment_names(spec, *, depth=0):
    from demiflow.data.plan import CallableSpec, OperatorLLMMapOp
    from demiflow.operator_llm.model import PromptPack, PromptDefinition, PromptModel
    from demiflow.embeddings.model import EmbeddingModel
    from demiflow.execution.dataflow_model_factory import PromptFactory, EmbeddingFactory, CacheFactory, RoutedPromptFactory
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
        # Offline inputs use the driver's sealed store and never construct an
        # HTTP provider. Match transport selection rather than treating a zero
        # request budget (which can also describe online replay) as offline.
        options = operation.options or {}
        names = () if any(key in options for key in ('offline_dir', 'offline_store')) else pack.required_environment_names
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
    from demiflow.execution.seal.plan import load_sealed_dataflow, verify_dataflow_program
    from demiflow.execution.recovery_contract import document
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
