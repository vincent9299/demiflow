"""Lazy Dataset readers/types facade; contracts can load without the runtime."""
from importlib import import_module as _import_module

__all__ = [
    'require',
    'schema',
    'open_checkpoint',
    'read_checkpoint',
    'read_queue',
    'read_queue_records',
    'read_legacy_queue_records',
    'add_lance_columns',
    'ensure_lance_vector_index',
    'local_execution',
    'from_items',
    'from_iter',
    'read_records',
    'range',
    'from_arrow',
    'from_numpy',
    'from_pandas',
    'read_parquet',
    'read_json',
    'read_csv',
    'read_text',
    'read_binary_files',
    'read_images',
    'read_sql',
    'read_datasource',
    'read_lance',
    'read_document_receipts',
    'vector_search_lance',
    "FitRef",
    "ModelSelection",
    "TrainingRecipe",
    "PredictorSpec",
    "Dataset",
    "MaterializedDataset",
    "BlockMetadata",
    "Datasource",
    "ReadTask",
    "Datasink",
    "WriteResult",
    "AbsMax",
    "AggregateFnV2",
    "Count",
    "Max",
    "Mean",
    "Min",
    "Std",
    "Sum",
]


_EXPORTS = {
    **{name: (".training", name) for name in ("TrainingRecipe", "PredictorSpec", "FitRef", "ModelSelection")},
    **{name: (".dataset", name) for name in ("Dataset", "MaterializedDataset")},
    **{name: (".datasource", name) for name in ("BlockMetadata", "Datasource", "ReadTask")},
    **{name: (".datasink", name) for name in ("Datasink", "WriteResult")},
    **{name: (".aggregate", name) for name in (
        "AbsMax", "AggregateFnV2", "Count", "Max", "Mean", "Min", "Std", "Sum",
    )},
    "require": (".guards", "require"),
    "schema": ("..lance.schema", "schema"),
    **{name: ("..execution.stream_checkpoint", name) for name in ("open_checkpoint", "read_checkpoint")},
    "local_execution": ("..execution.local_kernel", "local_execution"),
    **{name: ("..lance", name) for name in ("add_lance_columns", "ensure_lance_vector_index")},
    **{name: (".read_api", name) for name in (
        "read_queue", "read_queue_records", "read_legacy_queue_records", "from_items", "from_iter", "read_records", "range",
        "from_arrow", "from_numpy", "from_pandas", "read_parquet", "read_json", "read_csv",
        "read_text", "read_binary_files", "read_images", "read_sql", "read_datasource",
        "read_lance", "read_document_receipts", "vector_search_lance",
    )},
}


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = _EXPORTS[name]
    value = getattr(_import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
