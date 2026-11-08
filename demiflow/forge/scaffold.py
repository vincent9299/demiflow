"""Transactional project generation from a closed Pipeline request."""
from __future__ import annotations

import os
from pathlib import Path
import pprint
import shutil
import tempfile

from .contract import build_context
from .definition import PipelineDefinition, definition_from_request, normalize_request, normalize_run_config
from .io import atomic_write_json, no_symlinks, read_json, write_exclusive
from .manifest import build_manifest
from .schema import ForgeError


def new_project(request_path: Path, destination: Path) -> dict:
    request = normalize_request(read_json(request_path))
    target = destination.absolute()
    no_symlinks(target.parent)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and (target.is_symlink() or not target.is_dir() or any(target.iterdir())):
        raise ForgeError("Forge project destination must be absent or empty")
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.forge-", dir=target.parent))
    try:
        _write_project(staging, request)
        if target.exists():
            target.rmdir()
        os.replace(staging, target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    manifest = read_json(target / "manifest.json")
    context = read_json(target / "contract.lock.json")
    return {
        "schema_version": "demiflow_forge_new_result_v1",
        "project": str(target),
        "status": "initialized",
        "context_digest": context["context_digest"],
        "manifest_digest": manifest["manifest_digest"],
    }


def _write_project(root: Path, request: dict) -> None:
    context = build_context()
    definition = definition_from_request(request)
    (root / "operators").mkdir()
    (root / "tests").mkdir()
    write_exclusive(root / "request.json", _json(request))
    write_exclusive(root / "contract.lock.json", _json(context))
    write_exclusive(root / "project.json", _json({
        "schema_version": "demiflow_forge_project_v1",
        "name": request["name"],
        "contract_id": context["contract"]["contract_id"],
        "contract_version": context["contract"]["version"],
        "context_digest": context["context_digest"],
    }))
    write_exclusive(root / "pipeline.py", "from demiflow.forge import PipelineDefinition\n\n\nPIPELINE = PipelineDefinition(" + pprint.pformat(definition, sort_dicts=False, width=100) + ")\n")
    write_exclusive(root / "operators" / "__init__.py", "")
    map_value = request["map"]
    write_exclusive(root / "operators" / f"{map_value['name']}.py", _operator_source(map_value, definition))
    write_exclusive(root / "tests" / "test_pipeline.py", "import pytest\n\n\ndef test_pipeline_contract():\n    pytest.fail('TODO: implement business behavior tests')\n")
    run_config = {
        "schema_version": "demiflow_forge_run_v1",
        "inputs": {definition["inputs"][0]["name"]: {"uri": "datasets/input.lance", "version": 1}},
        "outputs": {definition["outputs"][0]["name"]: {"uri": "datasets/output.lance"}},
        "max_rows": definition["budgets"]["max_rows"],
    }
    normalize_run_config(run_config, PipelineDefinition(definition))
    write_exclusive(root / "run.example.json", _json(run_config))
    write_exclusive(root / "README.md", _readme(request))
    atomic_write_json(root / "manifest.json", build_manifest(root))


def _operator_source(map_value: dict, definition: dict) -> str:
    config = map_value["config"]
    parameters = []
    for name in map_value["config_schema"]["properties"]:
        has_default = name not in map_value["config_schema"]["required"]
        parameters.append(f"{name}={config[name]!r}" if has_default and name in config else name)
    signature = "row"
    if parameters:
        signature += ", *, " + ", ".join(parameters)
    values = {
        "name": map_value["name"],
        "version": map_value["version"],
        "description": map_value["description"],
        "input_schema": definition["inputs"][0]["schema"],
        "output_schema": definition["outputs"][0]["schema"],
        "config_schema": map_value["config_schema"],
    }
    decorator = "\n    " + ",\n".join(
        f"{name}={_format_python(value)}" for name, value in values.items()
    ) + ",\n"
    return (
        "from demiflow.forge import operator\n\n\n"
        f"@operator({decorator})\n"
        f"def {map_value['name']}({signature}):\n"
        f"    raise NotImplementedError('TODO: implement {map_value['name']}')\n"
    )


def _format_python(value) -> str:
    return pprint.pformat(value, sort_dicts=False, width=88).replace("\n", "\n    ")


def _json(value) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"


def _readme(request: dict) -> str:
    return f"""# {request['name']}

{request['goal']}

## Contract

This project is bound to the built-in demiflow Forge contract in `contract.lock.json`.
Only `pipeline.py`, `operators/`, `tests/`, `run.example.json`, and this README are
candidate-owned. `project.json`, `request.json`, `contract.lock.json`, and
`manifest.json` are controlled bindings or generated files.

## Input and output

- Row grain: {request['row_grain']}
- Input: one Lance table declared in `pipeline.py` and bound by RunConfig.
- Output: one local Lance table under `datasets/`.
- Completion: {request['completion']}

## Development

Implement the marked operator and behavior tests, then run `demiflow forge manifest`,
`check`, and `verify`. The initial scaffold intentionally fails verification.

## Verification and execution

Review and Freeze must match the current source revision. `forge run` executes only
frozen code and only reads or writes Lance paths below this project's `datasets/`.
Model calls, network access, and external writes are outside contract v1.
"""


__all__ = ["new_project"]
