"""Dry-run and frozen local execution over the existing Dataset API."""
from __future__ import annotations

import importlib.util
from pathlib import Path

from .checks import check_project, current_revision
from .definition import inspect_operators, load_definition, normalize_run_config
from .io import no_symlinks, read_json
from .schema import ForgeError, arrow_schema, validate_data


def dry_run(project: Path, config_path: Path) -> dict:
    definition = load_definition(project)
    config = normalize_run_config(read_json(config_path), definition)
    payload = definition.to_dict()
    return {
        "schema_version": "demiflow_forge_dry_run_v1",
        "project": str(project.absolute()),
        "definition": payload["name"],
        "nodes": payload["nodes"],
        "inputs": config["inputs"],
        "outputs": config["outputs"],
        "max_rows": config["max_rows"],
        "side_effects": payload["side_effects"],
        "execution": "local_dataset",
        "performed_io": False,
    }


def run_project(project: Path, config_path: Path) -> dict:
    from .store import locked

    with locked(project):
        return _run_frozen(project, config_path)


def _run_frozen(project: Path, config_path: Path) -> dict:
    from .lifecycle import require_frozen

    project = project.absolute()
    require_frozen(project)
    report = check_project(project)
    if not report["passed"]:
        raise ForgeError("Frozen project no longer passes contract checks")
    definition = load_definition(project)
    config = normalize_run_config(read_json(config_path), definition)
    payload = definition.to_dict()
    implementation = inspect_operators(project)[0]
    function = _load_operator(project, implementation, report["revision"])
    map_node = payload["nodes"][1]
    defaults = {item["name"]: item["default"] for item in implementation["parameters"] if not item["required"]}
    effective_config = {**defaults, **map_node["config"]}
    validate_data(effective_config, implementation["config_schema"], "$.map.config")

    def apply(row):
        checked = validate_data(dict(row), implementation["input_schema"], "$.row")
        try:
            output = function(checked, **effective_config)
        except Exception as exc:
            raise ForgeError(f"Operator {implementation['name']} failed: {type(exc).__name__}: {exc}") from exc
        return validate_data(output, implementation["output_schema"], "$.output")

    input_name = payload["inputs"][0]["name"]
    output_name = payload["outputs"][0]["name"]
    source = _dataset_path(project, config["inputs"][input_name]["uri"], must_exist=True)
    target = _dataset_path(project, config["outputs"][output_name]["uri"], must_exist=False)
    from demiflow import data

    receipt = (
        data.read_lance(
            str(source), version=config["inputs"][input_name]["version"],
            limit=config["max_rows"],
        )
        .map(apply)
        .write_lance(
            str(target), mode=payload["outputs"][0]["mode"],
            schema=arrow_schema(payload["outputs"][0]["schema"]),
            return_receipt=True,
        )
    )
    return {
        "schema_version": "demiflow_forge_run_result_v1",
        "project": str(project),
        "revision": current_revision(project),
        "status": "completed",
        "receipt": receipt.to_dict(),
    }


def _load_operator(project: Path, implementation: dict, revision: str):
    path = project / implementation["module"]
    no_symlinks(path)
    name = f"_demiflow_forge_{revision}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ForgeError(f"Cannot load operator: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    function = getattr(module, implementation["function"], None)
    contract = getattr(function, "__demiflow_operator__", None)
    public_fields = ("name", "version", "description", "input_schema", "output_schema", "config_schema")
    if contract is None or any(getattr(contract, field) != implementation[field] for field in public_fields):
        raise ForgeError("Loaded operator contract differs from Manifest")
    return function


def _dataset_path(project: Path, uri: str, *, must_exist: bool) -> Path:
    path = project / uri
    no_symlinks(project)
    no_symlinks(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    no_symlinks(path)
    resolved = path.resolve()
    resolved.relative_to((project / "datasets").resolve())
    if must_exist and not path.is_dir():
        raise ForgeError(f"Input Lance dataset is unavailable: {uri}")
    return resolved


__all__ = ["dry_run", "run_project"]
