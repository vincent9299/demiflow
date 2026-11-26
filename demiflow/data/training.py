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
from .training_schema import prediction_columns, validate_columns


def _freeze(value):
    if isinstance(value, Mapping) and all(type(k) is str for k in value):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if value is None or type(value) in (bool, int, str) or type(value) is float and math.isfinite(value):
        return value
    raise TypeError("recipe fields require finite JSON values")


def training_json(value):
    if isinstance(value, Mapping):
        return {k: training_json(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [training_json(v) for v in value]
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


def training_steps(values):
    if (not isinstance(values, Sequence) or isinstance(values, (bytes, str)) or not 1 <= len(values) <= 1024
            or any(type(i) is not int or not 0 < i < 2**63 for i in values)
            or len(set(values)) != len(values)):
        raise ValueError("steps requires 1–1024 unique positive optimizer steps")
    return tuple(values)


def _parameters(arguments):
    declared = arguments.get("parameter_schema")
    schema = compile_schema({"type": "object"} if declared is None else training_json(_mapping(declared, "parameter schema")))
    values = _mapping(arguments["parameters"], "parameters")
    validate_instance(training_json(values), schema, label="recipe parameters")
    return {"parameter_schema": _freeze(schema), "parameters": values}


def predictor_values(arguments):
    if type(arguments["batch_size"]) is not int or arguments["batch_size"] < 1:
        raise ValueError("batch_size must be positive")
    fields = _schema(arguments["input_schema"], "predictor input schema")
    if set(fields) != set(arguments["inputs"]):
        raise ValueError("Every predictor input requires its declared Arrow type")
    inputs = _schema(arguments["inputs"], "predictor inputs")
    prediction_columns(inputs, fields)
    formats = tuple(arguments.get("model_formats", ()))
    if isinstance(arguments.get("model_formats"), str) or any(type(v) is not str or not v for v in formats):
        raise ValueError("model_formats requires a sequence of format names")
    options = arguments.get("native_options")
    if options is not None:
        options = _mapping(options, "native_options")
        validate_stream_native_options(parse_native_options(training_json(options), family="batch_transform"))
    return {**_parameters(arguments), "input_schema": fields, "model_formats": formats, "inputs": inputs,
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
    saved = training_steps(arguments["save_steps"])
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
    prediction_columns(bindings, spec["input_schema"])
    effective = {**spec["parameters"], **_mapping({} if parameters is None else parameters, "parameters")}
    validate_instance(training_json(effective), training_json(spec["parameter_schema"]), label="prediction parameters")
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
    return {"fields": {}, "driver": "pull", "cardinality": "one", "order": "unspecified",
            "topology": recipe["topology"]}


def analyze_prediction(arguments, *, inputs, callables, resources):
    recipe = arguments["self"]["recipe"]
    spec = recipe["predictor"]
    bound, effective = prediction_arguments(spec, arguments["inputs"], arguments["parameters"])
    require_fields(inputs[0], tuple(bound.values()))
    types = {"string": "str", "double": "float", "float64": "float", "float32": "float",
             "int64": "int", "int32": "int", "boolean": "bool", "binary": "bytes"}
    outputs = {key: flow_field(scalar(types.get(kind, kind))) for key, kind in spec["output_schema"].items()}
    return {"fields": outputs, "driver": "pull", "cardinality": "zero_or_more", "order": "unspecified",
            "stream_arguments": {"steps": arguments["self"]["steps"], "inputs": bound,
                                 "parameters": training_json(effective), "model_format": recipe["model_format"]}}


@flow_api("predictor_spec", role="value")
@dataclass(frozen=True, kw_only=True)
class PredictorSpec:
    """Declarative batch actor specification used by ``ModelSelection.predict``.

    The worker instantiates ``factory(model=..., inputs=..., parameters=...)`` with its
    sealed model binding and runs batched inference over the target Dataset.
    ``model`` is a plain mapping with ``model["step"]`` (int), ``model["predictor_digest"]``,
    and ``model["snapshot"]`` containing ``"step"``, ``"model_format"``, and
    ``"files": {logical_name: {"uri": "file://...", "sha256": str, "size_bytes": int}}``
    pointing to the sealed files in the managed model store.

    Fields:
        factory: Batch actor class whose constructor accepts ``(model, inputs, parameters)``.
        inputs: Mapping ``{predictor_port: dataset_column}`` naming required input columns.
        input_schema: Mapping ``{predictor_port: arrow_type}`` whose keys must equal ``inputs``.
        output_schema: Mapping ``{output_column: arrow_type}`` (e.g. ``"string"``, ``"binary"``).
        model_formats: Optional tuple of compatible model format identifiers.
        parameter_schema: Optional JSON Schema (object) validating ``parameters``.
        parameters: Default finite JSON prediction parameters.
        batch_size: Positive integer inference batch size (default ``1``).
        batch_format: Batch payload format passed to the actor (default ``"default"``).
        native_options: Optional stream ``batch_transform`` execution options.
    """
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
    """Immutable declarative training contract passed to ``Dataset.fit(recipe=...)``.

    Fields:
        trainer: Synchronous callable ``trainer(request, context)`` executed by the training worker.
            Write checkpoint files inside ``context.model_dir`` (no internal DemiFlow imports needed)
            and deliver them by returning ``{step: {name: path}}`` (or ``{step: {"files": ...,
            "resume_files": ...}}`` when supplying optimizer resume state) or
            ``{"checkpoints": {step: ...}, "metrics": {...}}``. Optionally call
            ``context.report_snapshot(step, files=..., resume_files=...)`` during training for
            declared ``save_steps`` (keep reported file contents unchanged until import); if all
            required steps including ``budget["optimizer_updates"]`` were reported, ``trainer`` may
            return ``None`` or ``{"metrics": {...}}``. Optional ``trainer.reconcile(request, *,
            model_dir, work_dir)`` may return ``{"step": int, "files": ..., "resume_files": ...}``
            or ``None``.
        train_schema: Mapping ``{column_name: arrow_type}`` required on the training Dataset.
        base_model_digest: 64-char lowercase hex SHA-256 digest identifying the base model.
        environment_digest: 64-char lowercase hex SHA-256 digest identifying the runtime environment.
        budget: Finite JSON mapping containing ``optimizer_updates`` (int ``>= max(save_steps)``).
        save_steps: Strictly increasing sequence of 1..1024 unique positive optimizer step ints.
        model_format: Nonempty artifact format name compatible with ``predictor.model_formats``.
        predictor: ``PredictorSpec`` used for downstream inference on checkpoints of this run.
        topology: Finite JSON mapping declaring training worker/device topology.
        media_fields: Optional mapping ``{role: (column_name, ...)}`` for media columns in train/monitors.
        monitor_schemas: Optional mapping of up to 63 named monitor Dataset schemas (excluding ``train``/``parent``).
        parameter_schema: Optional JSON Schema (object) validating ``parameters``.
        parameters: Finite JSON training hyperparameters validated against ``parameter_schema``.
    """
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
    """Handle returned by ``Dataset.fit`` referencing a declared managed training job.

    Use ``fit_ref.select(steps=...)`` (or ``fit_ref.checkpoints.select(steps=...)``)
    to bind one or more saved optimizer steps into a ``ModelSelection``.
    """
    declaration: FitDeclaration

    @property
    def checkpoints(self):
        """Return this FitRef as a checkpoint collection for step selection."""
        return self

    @flow_api("select_model", role="value")
    def select(self, *, steps):
        """Select 1..1024 unique positive saved optimizer steps and return a ``ModelSelection``."""
        return ModelSelection(self.declaration, training_steps(steps))


@dataclass(frozen=True)
class ModelSelection:
    """One or more selected checkpoint steps from a ``FitRef`` ready for inference or resume.

    Call ``.predict(on=dataset, inputs=..., parameters=...)`` to apply the recipe's
    ``PredictorSpec`` over a Dataset, or pass a single-step selection to ``Dataset.fit(resume_from=...)``.
    """
    declaration: FitDeclaration
    steps: tuple[int, ...]

    @flow_api("predict", datasets=("on",), analyze=analyze_prediction)
    def predict(self, *, on, inputs=None, parameters=None):
        """Apply the recipe's ``PredictorSpec`` to ``on`` Dataset using the selected checkpoint steps.

        Args:
            on: Target ``Dataset`` sharing the training execution port.
            inputs: Optional mapping overriding predictor input port column bindings.
            parameters: Optional finite JSON overrides merged over ``PredictorSpec.parameters``
                and validated against ``PredictorSpec.parameter_schema``.

        Returns:
            A new ``Dataset`` with the columns declared in ``PredictorSpec.output_schema``.
        """
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
        training_steps((resume_from["step"],))
    return FitRef(FitDeclaration(tuple(sources), recipe, dataset._executor, retrain_id, resume_from))
