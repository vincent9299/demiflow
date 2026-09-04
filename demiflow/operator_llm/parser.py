"""Closed parser for Candidate-owned Operator LLM prompt packs."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import yaml

from .errors import (
    PromptContractDiagnostic, PromptDefinitionError, PromptPackError,
    PromptPackVersionError, PromptRoleNotFoundError,
)
from .model import PromptDefinition, PromptPack, PromptPackVersion, inspect_prompt_model
from .template import inspect_template
from demiflow.schema import inspect_schema



@dataclass(frozen=True)
class PromptPackInspection:
    schema_version: PromptPackVersion | None
    prompt_definitions: Mapping[str, PromptDefinition]
    diagnostics: tuple[PromptContractDiagnostic, ...]
    content_hash: str


def inspect_prompt_pack(text: str) -> PromptPackInspection:
    content_hash = "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()
    issues: list[PromptContractDiagnostic] = []
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return PromptPackInspection(None, MappingProxyType({}), (
            PromptContractDiagnostic(
                "prompt_pack_yaml_invalid", f"invalid prompt-pack YAML: {exc}", "$",
            ),
        ), content_hash)
    if not isinstance(raw, Mapping):
        return PromptPackInspection(None, MappingProxyType({}), (
            PromptContractDiagnostic(
                "prompt_pack_not_mapping", "prompt pack must be a YAML mapping", "$",
            ),
        ), content_hash)
    version = None
    try:
        version = PromptPackVersion(str(raw.get("schema_version") or ""))
    except ValueError:
        issues.append(PromptContractDiagnostic(
            "prompt_pack_version_invalid", "unsupported prompt-pack schema",
            "$.schema_version",
        ))
    for name in sorted(set(raw) - {"schema_version", "prompts"}):
        issues.append(PromptContractDiagnostic(
            "prompt_pack_field_unsupported",
            f"prompt pack has unsupported field: {name}", f"$.{name}",
        ))
    prompts_raw = raw.get("prompts")
    if not isinstance(prompts_raw, Mapping) or not prompts_raw:
        issues.append(PromptContractDiagnostic(
            "prompt_pack_prompts_invalid",
            "prompt pack requires non-empty prompts", "$.prompts",
        ))
        return PromptPackInspection(
            version, MappingProxyType({}), tuple(issues), content_hash,
        )
    definitions: dict[str, PromptDefinition] = {}
    for raw_name, value in prompts_raw.items():
        name = str(raw_name)
        definition, prompt_issues = _inspect_prompt(name, value)
        issues.extend(prompt_issues)
        if definition is not None:
            definitions[name] = definition
    return PromptPackInspection(
        version, MappingProxyType(dict(sorted(definitions.items()))),
        tuple(issues), content_hash,
    )


def parse_prompt_pack(text: str) -> PromptPack:
    inspection = inspect_prompt_pack(text)
    if inspection.diagnostics or inspection.schema_version is None:
        error_type = (
            PromptPackVersionError
            if any(item.code == "prompt_pack_version_invalid" for item in inspection.diagnostics)
            else PromptDefinitionError
            if any(item.prompt for item in inspection.diagnostics)
            else PromptPackError
        )
        raise error_type(
            "; ".join(item.message for item in inspection.diagnostics),
            inspection.diagnostics,
        )
    return PromptPack(
        inspection.schema_version,
        tuple(inspection.prompt_definitions.values()),
        inspection.content_hash,
    )


def _inspect_prompt(name: str, raw: Any) -> tuple[PromptDefinition | None, tuple[PromptContractDiagnostic, ...]]:
    issues: list[PromptContractDiagnostic] = []
    if not name or not isinstance(raw, Mapping):
        return None, (PromptContractDiagnostic(
            "prompt_definition_invalid", f"invalid prompt: {name!r}",
            f"$.prompts.{name}", name,
        ),)
    for field in sorted(set(raw) - {"version", "model", "response_schema", "schema_retries", "template"}):
        issues.append(PromptContractDiagnostic(
            "prompt_field_unsupported",
            f"prompt {name!r} has unsupported field: {field}",
            f"$.prompts.{name}.{field}", name,
        ))
    version = str(raw.get("version") or "").strip()
    if not version:
        issues.append(PromptContractDiagnostic(
            "prompt_version_required", f"{name!r} requires non-empty version",
            f"$.prompts.{name}.version", name,
        ))
    model, model_issues = inspect_prompt_model(
        raw.get("model"), label=f"prompt {name!r} model", prompt=name,
    )
    issues.extend(model_issues)
    template, template_issues = inspect_template(raw.get("template"), prompt=name)
    issues.extend(template_issues)
    schema, schema_issues = inspect_schema(raw.get("response_schema"))
    issues.extend(PromptContractDiagnostic(
        "prompt_response_schema_invalid", f"prompt {name!r} response_schema invalid: {message}",
        f"$.prompts.{name}.response_schema{path.removeprefix('$')}", name,
    ) for path, message in schema_issues)
    retries = raw.get("schema_retries", 0)
    if isinstance(retries, bool) or not isinstance(retries, int) or retries not in (0, 1):
        issues.append(PromptContractDiagnostic(
            "prompt_schema_retries_invalid",
            f"prompt {name!r} schema_retries must be 0 or 1",
            f"$.prompts.{name}.schema_retries", name,
        ))
    if issues or model is None or template is None or schema is None:
        return None, tuple(issues)
    return PromptDefinition(name, version, model, template, schema, retries), ()

def load_prompt_pack(path: str | Path) -> PromptPack:
    target = Path(path)
    if not target.is_file():
        raise PromptPackError(f"prompt pack not found: {target}")
    return parse_prompt_pack(target.read_text(encoding="utf-8"))


def resolve_prompt(pack: PromptPack, name: str) -> PromptDefinition:
    try:
        return pack.prompt_definitions[str(name)]
    except KeyError as exc:
        raise PromptRoleNotFoundError(f"prompt not found: {name}") from exc


def load_referenced_prompt_packs(bundle_root: str | Path) -> dict[str, PromptPack]:
    """Load prompt configs referenced by the entrypoint source closure."""
    import ast
    from demiflow.pipeline import discover_pipeline_definition
    from demiflow.execution.pipeline_sources import reachable_pipeline_sources
    root=Path(bundle_root).resolve(); pipeline=root/"pipeline"
    entrypoint=discover_pipeline_definition(root).entrypoint
    values=set()
    for source_path in reachable_pipeline_sources(root,entrypoint):
        tree=ast.parse(source_path.read_text(encoding="utf-8"),filename=str(source_path))
        for node in ast.walk(tree):
            if not isinstance(node,ast.Call) or not isinstance(node.func,ast.Attribute) or node.func.attr!="map_prompt": continue
            keywords={item.arg:item.value for item in node.keywords if item.arg}; value=keywords.get("config")
            if not isinstance(value,ast.Constant) or not isinstance(value.value,str): raise PromptPackError("map_prompt config must be a string literal")
            name=value.value; path=Path(name)
            if path.is_absolute() or len(path.parts)!=1 or path.suffix not in {".yaml",".yml"}: raise PromptPackError("map_prompt config must be a top-level pipeline YAML file")
            values.add(name)
    result={}
    for name in sorted(values):
        target=pipeline/name
        if target.is_symlink(): raise PromptPackError("map_prompt config symlinks are forbidden")
        result[name]=load_prompt_pack(target)
    return result
