"""Physical runtime policy and native-option translation (lane P, C03).

Single versioned mapping from the legacy ``run.limits`` fields and the v3
runtime options onto one ``RuntimePolicy``/``Bounds`` pair, plus the only
translation of semantic native execution options onto the unified
profile/partition contract. Consumers must not inject their own defaults.
"""
from __future__ import annotations

import math
import re
from typing import Mapping

from demiflow.execution.contracts.dataflow import Bounds, RuntimePolicy
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.runtime_limits import execution_limits

RUNTIME_POLICY_VERSION = "demiflow.runtime_policy.v3"

_PROVIDERS = frozenset({"local", "ray_core"})
_DURABILITY = frozenset({"ephemeral", "durable"})
_RECOVERY = frozenset({"checkpoint", "final_commit", "delivery_only", "attempt_only"})

# Versioned ceilings. Resource budgets tighten against their default; the two
# scale knobs tighten against a hard ceiling so parallelism stays bounded.
_MAX_REPLICAS_CEILING = 1024
_POLICY_DEFAULTS = {
    "durability": "ephemeral",
    "default_partitions": 1,
    "max_replicas": 8,
    "fusion": True,
    "fairness_quantum_bytes": 1024 * 1024,
    "control_timeout_ms": 30_000,
    "store_id": "local",
    "required_recovery": "attempt_only",
}
_BOUNDS_DEFAULTS = {
    "max_nodes": 4096,
    "max_edges": 16384,
    "max_partitions": 4096,
    "max_depth": 64,
    "max_pending_events": 4096,
    "max_open_files": 256,
}
_INT_BUDGET_OPTIONS = frozenset(_BOUNDS_DEFAULTS) | {
    "fairness_quantum_bytes", "control_timeout_ms",
}
_SCALE_OPTIONS = frozenset({"default_partitions", "max_replicas"})
_STR_OPTIONS = {"durability": _DURABILITY, "required_recovery": _RECOVERY}
_BOOL_OPTIONS = frozenset({"fusion"})
_FREE_STR_OPTIONS = frozenset({"store_id"})
_OPTION_KEYS = (
    _INT_BUDGET_OPTIONS | _SCALE_OPTIONS | frozenset(_STR_OPTIONS)
    | _BOOL_OPTIONS | _FREE_STR_OPTIONS
)


def _reject(message: str, **context: object) -> DataflowError:
    return DataflowError("INVALID_CONTRACT", message, context=context)


def _positive_int(value: object, name: str) -> int:
    # bool is a subclass of int; a boolean budget is never a finite window.
    if type(value) is not int or value <= 0:
        raise _reject(f"{name} must be a positive int (0 never means unlimited)", field=name)
    return value


def _tighten(value: object, ceiling: int, name: str) -> int:
    return min(_positive_int(value, name), ceiling)


def resolve_runtime_policy(
    options: Mapping[str, object],
    limits: Mapping[str, object],
    *,
    provider: str,
) -> tuple[RuntimePolicy, Bounds]:
    if provider not in _PROVIDERS:
        raise _reject("provider must be local or ray_core", field="provider")
    if not isinstance(options, Mapping):
        raise _reject("options must be a mapping")
    if not isinstance(limits, Mapping):
        raise _reject("limits must be a mapping")
    unknown = set(options) - _OPTION_KEYS
    if unknown:
        raise _reject("unknown runtime option", field=sorted(unknown)[0])

    # The legacy limits authority fills versioned defaults and rejects unknown
    # keys, booleans, zero and negative budgets in one place.
    try:
        native = execution_limits(dict(limits))
    except Exception as exc:  # RecoveryContractError; preserve semantics only.
        raise _reject(f"invalid run limits: {exc}") from exc

    structural = {
        name: _tighten(options[name], default, name) if name in options else default
        for name, default in _BOUNDS_DEFAULTS.items()
    }
    bounds = Bounds(
        max_nodes=structural["max_nodes"],
        max_edges=structural["max_edges"],
        max_partitions=structural["max_partitions"],
        max_depth=structural["max_depth"],
        max_control_bytes=native["max_control_bytes"],
        max_row_bytes=native["max_row_bytes"],
        max_batch_bytes=native["max_batch_bytes"],
        max_resident_bytes=native["memory_bytes"],
        max_spill_bytes=native["max_scratch_bytes"],
        max_state_bytes=native["max_state_bytes"],
        max_pending_events=structural["max_pending_events"],
        max_open_files=structural["max_open_files"],
        max_output_rows=native["max_output_rows"],
        timeout_ms=native["timeout_s"] * 1000,
    )

    if "default_partitions" in options:
        default_partitions = _tighten(options["default_partitions"], bounds.max_partitions, "default_partitions")
    else:
        default_partitions = _POLICY_DEFAULTS["default_partitions"]
    if "max_replicas" in options:
        max_replicas = _tighten(options["max_replicas"], _MAX_REPLICAS_CEILING, "max_replicas")
    else:
        max_replicas = _POLICY_DEFAULTS["max_replicas"]

    durability = options.get("durability", _POLICY_DEFAULTS["durability"])
    if durability not in _DURABILITY:
        raise _reject("durability must be ephemeral or durable", field="durability")
    required_recovery = options.get("required_recovery", _POLICY_DEFAULTS["required_recovery"])
    if required_recovery not in _RECOVERY:
        raise _reject("unknown required_recovery", field="required_recovery")
    fusion = options.get("fusion", _POLICY_DEFAULTS["fusion"])
    if type(fusion) is not bool:
        raise _reject("fusion must be a bool", field="fusion")
    store_id = options.get("store_id", _POLICY_DEFAULTS["store_id"])
    if type(store_id) is not str or not store_id:
        raise _reject("store_id must be a non-empty string", field="store_id")
    fairness = (
        _tighten(options["fairness_quantum_bytes"], _POLICY_DEFAULTS["fairness_quantum_bytes"], "fairness_quantum_bytes")
        if "fairness_quantum_bytes" in options else _POLICY_DEFAULTS["fairness_quantum_bytes"]
    )
    control_timeout = (
        _tighten(options["control_timeout_ms"], _POLICY_DEFAULTS["control_timeout_ms"], "control_timeout_ms")
        if "control_timeout_ms" in options else _POLICY_DEFAULTS["control_timeout_ms"]
    )

    policy = RuntimePolicy(
        provider=provider,
        durability=durability,
        default_partitions=default_partitions,
        max_replicas=max_replicas,
        fusion=fusion,
        checkpoint_rows=native["checkpoint_rows"],
        checkpoint_interval_ms=native["checkpoint_interval_ms"],
        fairness_quantum_bytes=fairness,
        control_timeout_ms=control_timeout,
        store_id=store_id,
        required_recovery=required_recovery,
    )
    return policy, bounds


# Semantic native options that carry business meaning and map onto the unified
# profile/partition contract. Anything else is reported, never silently dropped.
_RAY_DATA_TUNING = frozenset({
    "ray_remote_args", "compute", "target_max_block_size", "target_min_block_size",
    "streaming_read_buffer_size", "override_num_blocks", "parallelism",
    "execution_options", "resource_limits", "exclude_resources",
})
_PARTITION_KEYS = frozenset({"partitions", "num_partitions", "partition"})
_REPLICA_KEYS = frozenset({"concurrency", "min_workers", "initial_workers", "max_workers"})
_RESOURCE_KEYS = frozenset({"resources", "custom_resources"})
_NATIVE_KEYS = (
    frozenset({"num_cpus", "num_gpus", "memory"}) | _PARTITION_KEYS
    | _REPLICA_KEYS | _RESOURCE_KEYS
)
_SAFE_RESOURCE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")


def _finite_number(value: object, name: str) -> float:
    if type(value) is bool or not isinstance(value, (int, float)):
        raise DataflowError("UNSUPPORTED_CAPABILITY", f"{name} must be a finite number", context={"field": name})
    number = float(value)
    if not math.isfinite(number):
        raise DataflowError("UNSUPPORTED_CAPABILITY", f"{name} must be finite", context={"field": name})
    return number


def normalize_native_options(
    options: Mapping[str, object] | None,
    *,
    operation_kind: str,
) -> Mapping[str, object]:
    if type(operation_kind) is not str or not operation_kind:
        raise _reject("operation_kind must be a non-empty string", field="operation_kind")
    if options is None:
        return {}
    if not isinstance(options, Mapping):
        raise _reject("native options must be a mapping")

    profile: dict[str, object] = {}
    partitions: int | None = None
    replicas: dict[str, int] = {}
    for key in sorted(options):
        value = options[key]
        if key in _RAY_DATA_TUNING:
            raise DataflowError(
                "UNSUPPORTED_CAPABILITY",
                f"Ray Data tuning option '{key}' has no unified equivalent; "
                "declare resources/partitions on the operation instead",
                context={"field": key, "operation_kind": operation_kind},
            )
        if key not in _NATIVE_KEYS:
            raise DataflowError(
                "UNSUPPORTED_CAPABILITY",
                f"unknown native option '{key}' is not silently dropped",
                context={"field": key, "operation_kind": operation_kind},
            )
        if key == "num_cpus":
            cpu = _finite_number(value, "num_cpus")
            if cpu <= 0:
                raise DataflowError("UNSUPPORTED_CAPABILITY", "num_cpus must be positive", context={"field": key})
            profile["cpu_millis"] = max(1, int(round(cpu * 1000)))
        elif key == "num_gpus":
            gpu = _finite_number(value, "num_gpus")
            if gpu < 0 or gpu != int(gpu):
                raise DataflowError(
                    "UNSUPPORTED_CAPABILITY",
                    "num_gpus must be a non-negative whole device count",
                    context={"field": key},
                )
            if gpu > 0:
                profile["accelerators"] = [{"type": "gpu", "count": int(gpu)}]
        elif key == "memory":
            memory = _positive_int(value, "memory")
            profile["memory_bytes"] = memory
        elif key in _PARTITION_KEYS:
            count = _positive_int(value, key)
            if partitions is not None and partitions != count:
                raise _reject("conflicting partition declarations", field=key)
            partitions = count
        elif key in _REPLICA_KEYS:
            count = _positive_int(value, key)
            slot = "target" if key in {"concurrency", "initial_workers"} else (
                "min" if key == "min_workers" else "max"
            )
            if slot in replicas and replicas[slot] != count:
                raise _reject("conflicting replica declarations", field=key)
            replicas[slot] = count
        elif key in _RESOURCE_KEYS:
            if not isinstance(value, Mapping):
                raise DataflowError("UNSUPPORTED_CAPABILITY", f"{key} must be a mapping", context={"field": key})
            custom: dict[str, object] = {}
            for name in sorted(value):
                amount = _finite_number(value[name], f"{key}.{name}")
                if type(name) is not str or not _SAFE_RESOURCE_NAME.fullmatch(name) or amount <= 0:
                    raise DataflowError("UNSUPPORTED_CAPABILITY", f"invalid custom resource '{name}'", context={"field": key})
                custom[name] = amount
            if custom:
                profile["custom_resources"] = custom

    if replicas:
        target = replicas.get("target", replicas.get("max", replicas.get("min", 1)))
        minimum = replicas.get("min", target)
        maximum = replicas.get("max", target)
        if not minimum <= target <= maximum:
            raise _reject("replica range must satisfy min <= target <= max")
        replicas = {"min": minimum, "target": target, "max": maximum}

    result: dict[str, object] = {}
    if profile:
        result["profile"] = profile
    if partitions is not None:
        result["partitions"] = partitions
    if replicas:
        result["replicas"] = replicas
    return result