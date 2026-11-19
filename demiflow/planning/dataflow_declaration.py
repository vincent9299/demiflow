"""Pure declaration checks shared by SDK authoring and the engine facade.

Validation retains the original NativeOperation rules but never returns its
throwaway preflight marker as an executable plan or constructs a runtime owner.
"""
from __future__ import annotations

import json
from collections.abc import Mapping

from ..data.dataflow_operation import NativeOperation
from ..data.plan import CallableSpec
from ..execution.contracts.dataflow_errors import DataflowError
from ..execution.contracts.dataflow_kinds import (
    NATIVE_DECLARATION_ALIASES, NATIVE_DECLARATION_ONLY,
)
from .dataflow_registry import operator_registry

_MAX_DECLARATION_BYTES = 1024 * 1024


def normalize_json_mapping(value: object, label: str) -> dict:
    """Preserve the original mapping/encoded-budget check and error ordering."""
    if not isinstance(value, Mapping):
        raise DataflowError("INVALID_CONTRACT", label + " must be a mapping")
    try:
        raw = json.dumps(dict(value), sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise DataflowError("INVALID_CONTRACT", label + " must be bounded Json") from exc
    if len(raw.encode("utf-8")) > _MAX_DECLARATION_BYTES:
        raise DataflowError("BUDGET_EXCEEDED", label + " exceeds declaration budget")
    return dict(value)


def normalize_native_fields(kind: str, configuration: Mapping[str, object], *,
                            bindings: Mapping[str, object]) -> tuple[dict, dict]:
    """The checks that originally preceded NativeOperation construction."""
    if type(kind) is not str or not kind:
        raise DataflowError("INVALID_CONTRACT", "native operation kind must be a non-empty str")
    config = normalize_json_mapping(configuration, "configuration")
    binds = normalize_json_mapping(bindings, "bindings")
    return config, binds


def validate_native_kind(kind: str) -> None:
    """The registry check that follows strict NativeOperation validation."""
    if (NATIVE_DECLARATION_ALIASES.get(kind, kind) not in operator_registry()
            and kind not in NATIVE_DECLARATION_ONLY):
        raise DataflowError("UNSUPPORTED_CAPABILITY", "unknown native operation: " + kind)


def _preflight_factory(*args: object, **kwargs: object) -> object:
    raise TypeError('A declaration preflight marker cannot execute')


def validate_native_operation(kind: str, configuration: Mapping[str, object], *,
                              bindings: Mapping[str, object]) -> None:
    """Check the full original declaration contract without returning a node."""
    config, binds = normalize_native_fields(kind, configuration, bindings=bindings)
    NativeOperation(kind=kind, configuration=config, bindings=binds,
                    factory=CallableSpec.create(target=_preflight_factory))
    validate_native_kind(kind)


def validate_relational_inputs(source, expected_inputs: int) -> None:
    """Preserve the original keyed-source expansion and port multiplicity check."""
    from .dataflow_compile import expand_relational_inputs
    from .dataflow_policy import resolve_runtime_policy

    _, bounds = resolve_runtime_policy({}, {}, provider="local")
    bindings = expand_relational_inputs(source, path=(), bounds=bounds)
    if len(bindings) != expected_inputs:
        raise DataflowError("INVALID_CONTRACT", "relational input port multiplicity was not preserved")
