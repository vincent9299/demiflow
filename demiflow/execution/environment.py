"""Minimal environment projection for isolated Pipeline execution."""
from __future__ import annotations
import os
from typing import Mapping
from .pipeline_sources import uses_lance
from ..operator_llm import load_referenced_prompt_packs
from ..operator_llm.errors import PromptProviderUnavailableError
from ..pipeline import discover_pipeline_definition

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
