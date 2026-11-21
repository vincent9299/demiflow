"""Declarative, project-provided training recipes and model selections."""
from __future__ import annotations

from dataclasses import dataclass, field
import inspect
import math
import re
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence

from ..schema import compile_schema, validate_instance
from .execution_contract import normalize_batch_format
from .flow_contract import flow_api, field as flow_field, scalar, require_fields
from .native_options import parse_native_options, validate_stream_native_options
from .plan import FitDeclaration, PredictOp
from .training_schema import validate_columns


def _freeze(value):
    if isinstance(value, Mapping) and all(type(k) is str for k in value):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if value is None or type(value) in (bool, int, str) or type(value) is float and math.isfinite(value):
        return value
    raise TypeError("recipe fields require finite JSON values")


def _plain(value):
    if isinstance(value, Mapping):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_plain(v) for v in value]
    return value


def _mapping(value, name):
    if not isinstance(value, Mapping):
        raise TypeError(name + " must be a mapping")
    return _freeze(value)


def _schema(value, name):
    result = _mapping(value, name)
    if any(not k or not isinstance(v, str) or not v for k, v in result.items()):
        raise ValueError(name + " requires nonempty field names and types")
    return result


def _same_scope(left, right):
    if left is right:
        return True
    left_scope = getattr(left, "declaration_scope", None)
    return left_scope is not None and left_scope == getattr(right, "declaration_scope", None)


def _steps(values):
    if (not isinstance(values, Sequence) or isinstance(values, (bytes, str)) or not 1 <= len(values) <= 1024
            or any(type(i) is not int or not 0 < i < 2**63 for i in values)
            or len(set(values)) != len(values)):
        raise ValueError("steps requires 1–1024 unique positive optimizer steps")
    return tuple(values)


def _parameters(arguments):
    declared = arguments.get("parameter_schema")
    schema = compile_schema({"type": "object"} if declared is None else _plain(_mapping(declared, "parameter schema")))
    values = _mapping(arguments["parameters"], "parameters")
    validate_instance(_plain(values), schema, label="recipe parameters")
    return {"parameter_schema": _freeze(schema), "parameters": values}


def predictor_values(arguments):
    if type(arguments["batch_size"]) is not int or arguments["batch_size"] < 1:
        raise ValueError("batch_size must be positive")
    fields = _schema(arguments["input_schema"], "predictor input schema")
    if set(fields) != set(arguments["inputs"]):
        raise ValueError("Every predictor input requires its declared Arrow type")
    formats = tuple(arguments.get("model_formats", ()))
    if isinstance(arguments.get("model_formats"), str) or any(type(v) is not str or not v for v in formats):
        raise ValueError("model_formats requires a sequence of format names")
    options = arguments.get("native_options")
    if options is not None:
        options = _mapping(options, "native_options")
        validate_stream_native_options(parse_native_options(_plain(options), family="batch_transform"))
    return {**_parameters(arguments), "input_schema": fields, "model_formats": formats, "inputs": _schema(arguments["inputs"], "predictor inputs"),
            "output_schema": _schema(arguments["output_schema"], "predictor outputs"),
            "batch_format": normalize_batch_format(arguments["batch_format"]), "native_options": options}


def recipe_values(arguments):
    for key in ("base_model_digest", "environment_digest"):
        if type(arguments[key]) is not str or not re.fullmatch("[0-9a-f]{64}", arguments[key]):
            raise ValueError("base model and environment require SHA-256 identities")
    if not isinstance(arguments["model_format"], str) or not arguments["model_format"]:
        raise ValueError("model_format must be nonempty")
    monitors = _mapping(arguments["monitor_schemas"], "monitor_schemas")
    if len(monitors) >= 64:
        raise ValueError("training accepts at most 63 monitor ports")
    for name, schema in monitors.items():
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,63}", name) or name in {"train", "parent"}:
            raise ValueError("invalid monitor port " + name)
        _schema(schema, name)
    media = _mapping(arguments.get('media_fields', {}), 'media_fields')
    schemas = {'train': arguments['train_schema'], **monitors}
    for role, fields in media.items():
        if (role not in schemas or not isinstance(fields, tuple)
                or any(type(name) is not str or name not in schemas[role] for name in fields)
                or len(fields) != len(set(fields))):
            raise ValueError("media_fields requires distinct declared columns for each named dataset")
    saved = _steps(arguments["save_steps"])
    if tuple(sorted(saved)) != saved:
        raise ValueError("save_steps must be increasing")
    budget = _mapping(arguments["budget"], "budget")
    updates = budget.get("optimizer_updates")
    if type(updates) is not int or updates < max(saved):
        raise ValueError("optimizer_updates must cover every saved step")
    return {**_parameters(arguments), "train_schema": _schema(arguments["train_schema"], "training schema"),
            "monitor_schemas": monitors, "media_fields": media, "save_steps": saved, "budget": budget,
            "topology": _mapping(arguments["topology"], "topology")}


def prediction_arguments(spec, inputs, parameters):
    bindings = dict(spec["inputs"])
    if inputs is not None:
        overrides = _schema(inputs, "predict inputs")
        if set(overrides) - bindings.keys():
            raise ValueError("unknown predictor input")
        bindings.update(overrides)
    effective = {**spec["parameters"], **_mapping({} if parameters is None else parameters, "parameters")}
    validate_instance(_plain(effective), _plain(spec["parameter_schema"]), label="prediction parameters")
    return bindings, effective


# The static inspector calls these pure analyzers with source-level descriptors.
# The inspector and runtime validate the same recipe parameters without importing project code.
def analyze_fit(arguments, *, inputs, callables, resources):
    recipe = arguments["recipe"]
    expected = [recipe["train_schema"], *(recipe["monitor_schemas"][name] for name in sorted(recipe["monitor_schemas"]))]
    if len(inputs) != len(expected):
        raise ValueError("fit requires all named inputs")
    for columns, schema in zip(inputs, expected):
        require_fields(columns, tuple(schema))
    return {"fields": {}, "driver": "pull", "cardinality": "one", "order": "unspecified"}


def analyze_prediction(arguments, *, inputs, callables, resources):
    recipe = arguments["self"]["recipe"]
    spec = recipe["predictor"]
    bound, _ = prediction_arguments(spec, arguments["inputs"], arguments["parameters"])
    require_fields(inputs[0], tuple(bound.values()))
    types = {"string": "str", "double": "float", "float64": "float", "float32": "float",
             "int64": "int", "int32": "int", "boolean": "bool", "binary": "bytes"}
    outputs = {key: flow_field(scalar(types.get(kind, kind))) for key, kind in spec["output_schema"].items()}
    return {"fields": outputs, "driver": "pull", "cardinality": "zero_or_more", "order": "unspecified"}


@flow_api("predictor_spec", role="value")
@dataclass(frozen=True, kw_only=True)
class PredictorSpec:
    """A batch actor factory. The worker supplies its sealed model binding."""
    factory: type
    inputs: Mapping[str, str]
    input_schema: Mapping[str, str]
    model_formats: tuple[str, ...] = ()
    output_schema: Mapping[str, str]
    parameter_schema: Mapping[str, Any] | None = None
    parameters: Mapping[str, Any] = field(default_factory=dict)
    batch_size: int = 1
    batch_format: str = "default"
    native_options: Mapping[str, Any] | None = None

    def __post_init__(self):
        if not inspect.isclass(self.factory):
            raise TypeError("predictor factory must be a batch actor class")
        inspect.signature(self.factory).bind(model=None, inputs={}, parameters={})
        for name, value in predictor_values(vars(self)).items():
            object.__setattr__(self, name, value)


@flow_api("training_recipe", role="value")
@dataclass(frozen=True, kw_only=True)
class TrainingRecipe:
    trainer: Callable
    train_schema: Mapping[str, str]
    base_model_digest: str
    environment_digest: str
    budget: Mapping[str, Any]
    save_steps: tuple[int, ...]
    model_format: str
    predictor: PredictorSpec
    topology: Mapping[str, Any]
    media_fields: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    monitor_schemas: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    parameter_schema: Mapping[str, Any] | None = None
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.predictor, PredictorSpec) and self.predictor.model_formats and self.model_format not in self.predictor.model_formats:
            raise ValueError("Predictor is incompatible with the training model format")
        if not callable(self.trainer) or not isinstance(self.predictor, PredictorSpec):
            raise TypeError("training needs a trainer and a PredictorSpec")
        for name, value in recipe_values(vars(self)).items():
            object.__setattr__(self, name, value)


@dataclass(frozen=True)
class FitRef:
    declaration: FitDeclaration

    @property
    def checkpoints(self):
        return self

    @flow_api("select_model", role="value")
    def select(self, *, steps):
        return ModelSelection(self.declaration, _steps(steps))


@dataclass(frozen=True)
class ModelSelection:
    declaration: FitDeclaration
    steps: tuple[int, ...]

    @flow_api("predict", datasets=("on",), analyze=analyze_prediction)
    def predict(self, *, on, inputs=None, parameters=None):
        from .dataset import Dataset
        if not isinstance(on, Dataset) or not _same_scope(on._executor, self.declaration.executor):
            raise ValueError("predict requires a Dataset on the training execution port")
        spec = self.declaration.recipe.predictor
        bindings, effective = prediction_arguments(vars(spec), inputs, parameters)
        op = PredictOp(self.declaration, self.steps, _freeze(bindings), _freeze(effective))
        return on.replace_plan(on._source, on._plan.append(op))


def declare_fit(dataset, recipe, monitors, retrain_id, resume_from):
    from .dataset import Dataset
    if not isinstance(recipe, TrainingRecipe):
        raise TypeError("fit requires a TrainingRecipe")
    if monitors is None:
        monitors = {}
    if not isinstance(monitors, Mapping) or monitors.keys() != recipe.monitor_schemas.keys():
        raise ValueError("monitor datasets must match the recipe's monitor ports")
    sources = []
    for name, ds in (("train", dataset), *sorted(monitors.items())):
        if not isinstance(ds, Dataset) or not _same_scope(ds._executor, dataset._executor):
            raise ValueError("training inputs require the same execution port")
        sources.append((name, ds._source, ds._plan))
    if retrain_id is not None and (type(retrain_id) is not str or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", retrain_id)):
        raise ValueError("retrain_id must be a stable operation identifier")
    if isinstance(resume_from, ModelSelection):
        if len(resume_from.steps) != 1 or not _same_scope(resume_from.declaration.executor, dataset._executor):
            raise ValueError("resume_from requires a single model on the same execution port")
    elif resume_from is not None:
        resume_from = _mapping(resume_from, "published parent")
        if set(resume_from) != {"manifest_digest", "step"}:
            raise ValueError("published parent requires manifest_digest and step")
        if not re.fullmatch("[0-9a-f]{64}", str(resume_from["manifest_digest"])):
            raise ValueError("invalid published parent digest")
        _steps((resume_from["step"],))
    return FitRef(FitDeclaration(tuple(sources), recipe, dataset._executor, retrain_id, resume_from))
