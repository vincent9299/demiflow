"""Environment implementation."""
from __future__ import annotations

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


def allocation_environment(assignment, *, store, bounds):
    from demiflow.execution.operator_bindings.dataflow_model_factory import native_environment_names
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
