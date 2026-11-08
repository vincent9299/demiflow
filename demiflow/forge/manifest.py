"""Generated Pipeline manifest and semantic identity."""
from __future__ import annotations

from pathlib import Path

from .definition import inspect_operators, load_definition
from .io import atomic_write_json, read_json
from .schema import ForgeError, digest, validate_data


def build_manifest(project: Path) -> dict:
    context = read_json(project / "contract.lock.json")
    definition = load_definition(project).to_dict()
    operators = inspect_operators(project)
    map_node = definition["nodes"][1]
    selected = [item for item in operators if item["name"] == map_node["operator"]]
    if len(selected) != 1 or len(operators) != 1:
        raise ForgeError("Contract v1 requires exactly the operator referenced by the map node")
    implementation = selected[0]
    if implementation["input_schema"] != definition["inputs"][0]["schema"]:
        raise ForgeError("Operator input schema differs from Pipeline input")
    if implementation["output_schema"] != definition["outputs"][0]["schema"]:
        raise ForgeError("Operator output schema differs from Pipeline output")
    validate_data(map_node["config"], implementation["config_schema"], "$.nodes[1].config")
    semantic = {
        "context_digest": context["context_digest"],
        "definition": definition,
        "operator": {
            key: value for key, value in implementation.items()
            if key not in {"source_sha256", "module", "function"}
        },
    }
    payload = {
        "schema_version": "demiflow_forge_manifest_v1",
        "context_digest": context["context_digest"],
        "definition": definition,
        "operators": operators,
        "implementation": {
            "pipeline_sha256": _file_hash(project / "pipeline.py"),
            "operator_sha256": implementation["source_sha256"],
        },
        "semantic_digest": digest(semantic),
    }
    return {**payload, "manifest_digest": digest(payload)}


def write_manifest(project: Path) -> dict:
    value = build_manifest(project)
    atomic_write_json(project / "manifest.json", value)
    return value


def _file_hash(path: Path) -> str:
    import hashlib

    result = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            result.update(chunk)
    return result.hexdigest()


__all__ = ["build_manifest", "write_manifest"]
