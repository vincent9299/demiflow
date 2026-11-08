"""Static Pipeline and operator authoring contracts."""
from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from .io import no_symlinks, read_json
from .schema import (
    ForgeError, copy_json, exact_fields, normalize_schema, positive_int,
    required_text, safe_name, validate_data,
)


@dataclass(frozen=True)
class PipelineDefinition:
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", normalize_definition(self.payload))

    def to_dict(self) -> dict:
        return copy_json(self.payload)


@dataclass(frozen=True)
class OperatorDefinition:
    name: str
    version: str
    description: str
    input_schema: Mapping[str, Any]
    output_schema: Mapping[str, Any]
    config_schema: Mapping[str, Any]

    def __post_init__(self) -> None:
        safe_name(self.name, "operator.name")
        required_text(self.version, "operator.version")
        required_text(self.description, "operator.description")
        for name in ("input_schema", "output_schema", "config_schema"):
            object.__setattr__(self, name, normalize_schema(getattr(self, name)))

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "input_schema": copy_json(self.input_schema),
            "output_schema": copy_json(self.output_schema),
            "config_schema": copy_json(self.config_schema),
        }


def operator(**contract):
    if set(contract) != {"name", "version", "description", "input_schema", "output_schema", "config_schema"}:
        raise ForgeError("operator contract has unsupported fields")
    definition = OperatorDefinition(
        contract["name"], contract["version"], contract["description"],
        contract["input_schema"], contract["output_schema"], contract["config_schema"],
    )

    def decorate(fn):
        fn.__demiflow_operator__ = definition
        return fn

    return decorate


def normalize_request(value: Any) -> dict:
    exact_fields(value, {
        "schema_version", "name", "version", "source_text", "goal", "row_grain",
        "input", "output", "map", "side_effects", "budgets", "completion",
        "unresolved_questions",
    }, "PipelineRequest")
    if value["schema_version"] != "demiflow_forge_request_v1":
        raise ForgeError("Unsupported PipelineRequest schema")
    required_text(value["source_text"], "source_text")
    if value["unresolved_questions"] != []:
        raise ForgeError("PipelineRequest has unresolved questions")
    exact_fields(value["input"], {"name", "schema"}, "PipelineRequest.input")
    exact_fields(value["output"], {"name", "mode", "schema"}, "PipelineRequest.output")
    map_value = exact_fields(value["map"], {"name", "version", "description", "config", "config_schema"}, "PipelineRequest.map")
    required_text(map_value["version"], "map.version")
    required_text(map_value["description"], "map.description")
    if not isinstance(map_value["name"], str) or not map_value["name"].isidentifier():
        raise ForgeError("map.name must be a valid Python function identifier")
    config_schema = normalize_schema(map_value["config_schema"])
    if any(not name.isidentifier() for name in config_schema["properties"]):
        raise ForgeError("config_schema property names must be valid Python identifiers")
    validate_data(map_value["config"], config_schema, "$.map.config")
    optional = set(config_schema["properties"]) - set(config_schema["required"])
    if optional - set(map_value["config"]):
        raise ForgeError("Every optional config field requires an explicit default in map.config")
    payload = _definition_from_request_payload(value)
    normalize_definition(payload)
    result = copy_json(value)
    result["input"]["schema"] = normalize_schema(result["input"]["schema"])
    result["output"]["schema"] = normalize_schema(result["output"]["schema"])
    result["map"]["config_schema"] = config_schema
    return result


def definition_from_request(value: Any) -> dict:
    request = normalize_request(value)
    return _definition_from_request_payload(request)


def _definition_from_request_payload(request: Mapping[str, Any]) -> dict:
    input_value = request["input"]
    output_value = request["output"]
    map_value = request["map"]
    return {
        "schema_version": "demiflow_forge_definition_v1",
        "name": request["name"],
        "version": request["version"],
        "goal": request["goal"],
        "row_grain": request["row_grain"],
        "inputs": [{"name": input_value["name"], "kind": "lance", "schema": input_value["schema"]}],
        "nodes": [
            {"id": "read_source", "kind": "read_lance", "input": input_value["name"]},
            {"id": map_value["name"], "kind": "map", "operator": map_value["name"], "config": map_value["config"]},
            {"id": "write_result", "kind": "write_lance", "output": output_value["name"]},
        ],
        "outputs": [{
            "name": output_value["name"], "kind": "lance", "mode": output_value["mode"],
            "schema": output_value["schema"],
        }],
        "side_effects": request["side_effects"],
        "budgets": request["budgets"],
        "completion": request["completion"],
    }


def normalize_definition(value: Any) -> dict:
    exact_fields(value, {
        "schema_version", "name", "version", "goal", "row_grain", "inputs",
        "nodes", "outputs", "side_effects", "budgets", "completion",
    }, "PipelineDefinition")
    if value["schema_version"] != "demiflow_forge_definition_v1":
        raise ForgeError("Unsupported PipelineDefinition schema")
    safe_name(value["name"], "name")
    for field in ("version", "goal", "row_grain", "completion"):
        required_text(value[field], field)
    inputs = value["inputs"]
    outputs = value["outputs"]
    if not isinstance(inputs, list) or len(inputs) != 1 or not isinstance(outputs, list) or len(outputs) != 1:
        raise ForgeError("Contract v1 requires exactly one input and output")
    input_value = exact_fields(inputs[0], {"name", "kind", "schema"}, "input")
    safe_name(input_value["name"], "input.name")
    if input_value["kind"] != "lance":
        raise ForgeError("Contract v1 input must be Lance")
    input_schema = normalize_schema(input_value["schema"])
    output_value = exact_fields(outputs[0], {"name", "kind", "mode", "schema"}, "output")
    safe_name(output_value["name"], "output.name")
    if output_value["kind"] != "lance" or output_value["mode"] not in {"create", "append", "overwrite"}:
        raise ForgeError("Output kind/mode is invalid")
    output_schema = normalize_schema(output_value["schema"])
    nodes = value["nodes"]
    if not isinstance(nodes, list) or len(nodes) != 3:
        raise ForgeError("Contract v1 requires three linear nodes")
    read = exact_fields(nodes[0], {"id", "kind", "input"}, "read node")
    mapping = exact_fields(nodes[1], {"id", "kind", "operator", "config"}, "map node")
    write = exact_fields(nodes[2], {"id", "kind", "output"}, "write node")
    if read["kind"] != "read_lance" or read["input"] != input_value["name"]:
        raise ForgeError("read_lance node input is invalid")
    if mapping["kind"] != "map" or write["kind"] != "write_lance" or write["output"] != output_value["name"]:
        raise ForgeError("Linear node references are invalid")
    for node in nodes:
        safe_name(node["id"], "node.id")
    safe_name(mapping["operator"], "map.operator")
    side_effects = exact_fields(value["side_effects"], {"model_calls", "network", "external_writes"}, "side_effects")
    if any(side_effects[name] is not False for name in side_effects):
        raise ForgeError("Contract v1 does not support model, network or external writes")
    budgets = exact_fields(value["budgets"], {"max_rows", "test_timeout_seconds"}, "budgets")
    positive_int(budgets["max_rows"], "budgets.max_rows", maximum=1_000_000)
    positive_int(budgets["test_timeout_seconds"], "budgets.test_timeout_seconds", maximum=600)
    result = copy_json(value)
    result["inputs"][0]["schema"] = input_schema
    result["outputs"][0]["schema"] = output_schema
    return result


def load_request(project: Path) -> dict:
    return normalize_request(read_json(project / "request.json"))


def load_definition(project: Path) -> PipelineDefinition:
    path = project / "pipeline.py"
    no_symlinks(path)
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename="pipeline.py")
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise ForgeError(f"Cannot parse pipeline.py: {exc}") from exc
    assignments = []
    for statement in tree.body:
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant) and isinstance(statement.value.value, str):
            continue
        if isinstance(statement, ast.ImportFrom):
            if statement.module != "demiflow.forge" or statement.level or len(statement.names) != 1 or statement.names[0].name != "PipelineDefinition":
                raise ForgeError("pipeline.py may only import PipelineDefinition from demiflow.forge")
            continue
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1 and isinstance(statement.targets[0], ast.Name) and statement.targets[0].id == "PIPELINE":
            assignments.append(statement.value)
            continue
        raise ForgeError("pipeline.py contains code outside the static PipelineDefinition")
    if len(assignments) != 1 or not isinstance(assignments[0], ast.Call) or not isinstance(assignments[0].func, ast.Name) or assignments[0].func.id != "PipelineDefinition":
        raise ForgeError("pipeline.py requires one PIPELINE = PipelineDefinition(...)")
    call = assignments[0]
    if len(call.args) != 1 or call.keywords:
        raise ForgeError("PipelineDefinition requires one literal mapping")
    try:
        payload = ast.literal_eval(call.args[0])
    except (TypeError, ValueError) as exc:
        raise ForgeError("PipelineDefinition must be a static literal") from exc
    return PipelineDefinition(payload)


def inspect_operators(project: Path) -> list[dict]:
    root = project / "operators"
    no_symlinks(root)
    if not root.is_dir():
        raise ForgeError("operators/ must be a directory")
    result = []
    names = set()
    for path in sorted(root.glob("*.py")):
        no_symlinks(path)
        if path.name == "__init__.py":
            continue
        try:
            source = path.read_bytes()
            tree = ast.parse(source.decode("utf-8"), filename=path.name)
        except (OSError, UnicodeError, SyntaxError) as exc:
            raise ForgeError(f"Cannot parse operator {path.name}: {exc}") from exc
        found = []
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            decorators = [item for item in node.decorator_list if isinstance(item, ast.Call) and isinstance(item.func, ast.Name) and item.func.id == "operator"]
            if decorators:
                found.append((node, decorators))
        if len(found) != 1 or len(found[0][1]) != 1:
            raise ForgeError(f"{path.name} must define exactly one @operator function")
        node, decorators = found[0]
        call = decorators[0]
        if call.args or any(item.arg is None for item in call.keywords):
            raise ForgeError(f"{path.name} @operator requires keyword literals")
        try:
            raw = {item.arg: ast.literal_eval(item.value) for item in call.keywords}
        except (TypeError, ValueError) as exc:
            raise ForgeError(f"{path.name} @operator must use static literals") from exc
        definition = OperatorDefinition(
            raw.get("name"), raw.get("version"), raw.get("description"),
            raw.get("input_schema"), raw.get("output_schema"), raw.get("config_schema"),
        ).to_dict()
        if set(raw) != set(definition):
            raise ForgeError(f"{path.name} @operator has unsupported fields")
        if definition["name"] != node.name or not node.name.isidentifier():
            raise ForgeError(f"{path.name} operator name must equal its Python function name")
        if definition["name"] in names:
            raise ForgeError(f"Duplicate operator: {definition['name']}")
        names.add(definition["name"])
        parameters = _operator_parameters(node, definition["config_schema"], path.name)
        result.append({
            **definition,
            "module": f"operators/{path.name}",
            "function": node.name,
            "parameters": parameters,
            "source_sha256": hashlib.sha256(source).hexdigest(),
        })
    if not result:
        raise ForgeError("No operator implementation found")
    return result


def _operator_parameters(node: ast.FunctionDef, config_schema: Mapping[str, Any], filename: str) -> list[dict]:
    args = node.args
    if (
        args.posonlyargs or len(args.args) != 1 or args.args[0].arg != "row"
        or args.vararg is not None or args.kwarg is not None
        or len(args.defaults) != 0 or len(args.kw_defaults) != len(args.kwonlyargs)
    ):
        raise ForgeError(f"{filename} operator must be fn(row, *, <config>)")
    properties = config_schema["properties"]
    if {item.arg for item in args.kwonlyargs} != set(properties):
        raise ForgeError(f"{filename} config parameters do not match config_schema")
    parameters = []
    for item, default in zip(args.kwonlyargs, args.kw_defaults):
        required = default is None
        value = None
        if not required:
            try:
                value = ast.literal_eval(default)
                validate_data({item.arg: value}, {"type": "object", "properties": {item.arg: properties[item.arg]}, "required": [item.arg], "additionalProperties": False})
            except (TypeError, ValueError) as exc:
                raise ForgeError(f"{filename} default for {item.arg} is invalid") from exc
        parameters.append({"name": item.arg, "required": required, "default": value})
    required_names = {item["name"] for item in parameters if item["required"]}
    if required_names != set(config_schema["required"]):
        raise ForgeError(f"{filename} required config does not match function defaults")
    return parameters


def normalize_run_config(value: Any, definition: PipelineDefinition) -> dict:
    exact_fields(value, {"schema_version", "inputs", "outputs", "max_rows"}, "RunConfig")
    if value["schema_version"] != "demiflow_forge_run_v1":
        raise ForgeError("Unsupported RunConfig schema")
    payload = definition.to_dict()
    input_name = payload["inputs"][0]["name"]
    output_name = payload["outputs"][0]["name"]
    inputs = exact_fields(value["inputs"], {input_name}, "RunConfig.inputs")
    outputs = exact_fields(value["outputs"], {output_name}, "RunConfig.outputs")
    source = exact_fields(inputs[input_name], {"uri", "version"}, "RunConfig input")
    target = exact_fields(outputs[output_name], {"uri"}, "RunConfig output")
    source_uri = dataset_uri(source["uri"])
    target_uri = dataset_uri(target["uri"])
    if source_uri == target_uri:
        raise ForgeError("Input and output Lance paths must differ")
    positive_int(source["version"], "RunConfig input version", maximum=2**63 - 1)
    positive_int(value["max_rows"], "RunConfig.max_rows", maximum=payload["budgets"]["max_rows"])
    return copy_json(value)


def dataset_uri(value: Any) -> str:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        raise ForgeError("Lance path must be a project-relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or len(path.parts) < 2 or path.parts[0] != "datasets" or any(part in {"", ".", ".."} for part in path.parts) or path.suffix != ".lance" or str(path) != value:
        raise ForgeError("Lance path must stay under datasets/ and end in .lance")
    return value


__all__ = [
    "OperatorDefinition", "PipelineDefinition", "dataset_uri", "definition_from_request",
    "inspect_operators", "load_definition", "load_request", "normalize_definition",
    "normalize_request", "normalize_run_config", "operator",
]
