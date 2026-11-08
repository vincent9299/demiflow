"""Demiflow public API: one Driver program, Dataset, and Local/Ray execution."""

from importlib import import_module as _import_module

__all__ = [
    "Pipeline",
    "PipelineProgram",
    "ResourceAPI",
    "StreamCheckpoint",
    "AdmissionQuotaReader",
    "data",
    "Dataset",
    "MaterializedDataset",
    "Datasource",
    "Datasink",
    "ReadTask",
    "BlockMetadata",
    "WriteResult",
    "AggregateFnV2",
    "AbsMax",
    "Count",
    "Max",
    "Mean",
    "Min",
    "Std",
    "Sum",
    "PipelineBackend",
    "PipelineBundleRef",
    "PipelineExecutionEnvironment",
    "PipelineRunObservation",
    "PipelineRunReadiness",
    "PipelineRunRequest",
    "PipelineRunResult",
    "PipelineExecutionTarget",
    "PlatformRuntimeIdentity",
]

# Keep lightweight tooling usable with the standard library. Public objects are
# resolved from their original modules and cached, preserving object identity.
_EXPORTS = {
    **{name: (".pipeline", name) for name in ("Pipeline", "PipelineProgram", "ResourceAPI")},
    "StreamCheckpoint": (".execution.stream_checkpoint", "StreamCheckpoint"),
    "AdmissionQuotaReader": (".execution.stream_admission", "AdmissionQuotaReader"),
    "data": (".data", None),
    **{name: (".data", name) for name in (
        "Dataset", "MaterializedDataset", "Datasource", "Datasink", "ReadTask",
        "BlockMetadata", "WriteResult", "AggregateFnV2", "AbsMax", "Count", "Max",
        "Mean", "Min", "Std", "Sum",
    )},
    **{name: (".execution", name) for name in (
        "PipelineBackend", "PipelineBundleRef", "PipelineExecutionEnvironment",
        "PipelineRunObservation", "PipelineRunReadiness", "PipelineRunRequest",
        "PipelineRunResult", "PipelineExecutionTarget", "PlatformRuntimeIdentity",
    )},
}


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = _EXPORTS[name]
    module = _import_module(module_name, __name__)
    value = module if attribute is None else getattr(module, attribute)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
