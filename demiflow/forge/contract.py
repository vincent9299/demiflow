"""Built-in engineering contract and current demiflow platform context."""
from __future__ import annotations

import hashlib
import importlib
import inspect
from pathlib import Path

from .io import read_json
from .schema import ForgeError, copy_json, digest, exact_fields, positive_int, required_text


CONTRACT_PATH = Path(__file__).resolve().parent / "contracts" / "pipeline-v1.json"


def load_contract() -> dict:
    value = read_json(CONTRACT_PATH)
    exact_fields(value, {
        "schema_version", "contract_id", "version", "nodes", "layout",
        "platform_objects", "operator_imports", "limits",
    }, "ContractBundle")
    if value["schema_version"] != "demiflow_forge_contract_v1":
        raise ForgeError("Unsupported ContractBundle schema")
    required_text(value["contract_id"], "contract_id")
    required_text(value["version"], "version")
    if value["nodes"] != ["read_lance", "map", "write_lance"]:
        raise ForgeError("Contract v1 requires read_lance, map, write_lance")
    layout = exact_fields(value["layout"], {"root_files", "source_dirs", "runtime_dirs", "ignored_dirs"}, "layout")
    for field in layout:
        items = layout[field]
        if not isinstance(items, list) or not items or any(not isinstance(item, str) or not item for item in items):
            raise ForgeError(f"layout.{field} must be non-empty strings")
        if len(items) != len(set(items)):
            raise ForgeError(f"layout.{field} contains duplicates")
    if layout["source_dirs"] != ["operators", "tests"] or layout["runtime_dirs"] != ["datasets", "runs"]:
        raise ForgeError("Contract v1 source/runtime layout is invalid")
    objects = value["platform_objects"]
    if not isinstance(objects, list) or not objects:
        raise ForgeError("platform_objects must be non-empty")
    ids = set()
    for item in objects:
        exact_fields(item, {"module", "path"}, "platform object")
        required_text(item["module"], "platform module")
        required_text(item["path"], "platform path")
        identifier = f"{item['module']}:{item['path']}"
        if identifier in ids:
            raise ForgeError("Duplicate platform contract")
        ids.add(identifier)
    imports = value["operator_imports"]
    if not isinstance(imports, list) or any(not isinstance(item, str) or not item.isidentifier() for item in imports):
        raise ForgeError("operator_imports must contain simple module names")
    if imports != sorted(set(imports)):
        raise ForgeError("operator_imports must be sorted and unique")
    limits = exact_fields(value["limits"], {
        "max_files", "max_file_bytes", "max_total_bytes", "max_timeout_seconds",
        "output_tail_bytes", "output_hard_limit_bytes",
    }, "limits")
    for name, item in limits.items():
        positive_int(item, f"limits.{name}", maximum=1024 * 1024 * 1024)
    if limits["output_tail_bytes"] > limits["output_hard_limit_bytes"]:
        raise ForgeError("Output tail exceeds hard limit")
    return copy_json(value)


def build_context() -> dict:
    contract = load_contract()
    platform = []
    for item in contract["platform_objects"]:
        module_name = item["module"]
        path = item["path"]
        try:
            value = importlib.import_module(module_name)
            for part in path.split("."):
                value = getattr(value, part)
        except (ImportError, AttributeError) as exc:
            raise ForgeError(f"Platform contract is unavailable: {module_name}:{path}") from exc
        if path.split(".")[0] not in getattr(importlib.import_module(module_name), "__all__", [path.split(".")[0]]):
            raise ForgeError(f"Platform contract is not publicly exported: {module_name}:{path}")
        doc = inspect.getdoc(value) or ""
        if not doc.strip():
            raise ForgeError(f"Platform contract has no public documentation: {module_name}:{path}")
        try:
            signature = str(inspect.signature(value))
        except (TypeError, ValueError) as exc:
            raise ForgeError(f"Platform contract has no inspectable signature: {module_name}:{path}") from exc
        source = inspect.getsourcefile(value)
        if not source:
            raise ForgeError(f"Platform contract has no Python source: {module_name}:{path}")
        source_path = Path(source).resolve()
        platform.append({
            "id": f"{module_name}:{path}",
            "module": module_name,
            "path": path,
            "signature": signature,
            "doc": doc,
            "source_sha256": _file_hash(source_path),
        })
    payload = {
        "schema_version": "demiflow_forge_context_v1",
        "contract": contract,
        "platform": platform,
    }
    return {**payload, "context_digest": digest(payload)}


def _file_hash(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            result.update(chunk)
    return result.hexdigest()


__all__ = ["CONTRACT_PATH", "build_context", "load_contract"]
