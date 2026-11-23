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
from demiflow.resources.contracts import ResourceProfile

from demiflow.execution.contracts.dataflow import Bounds, RuntimePolicy
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow import BATCH_TARGET_BYTES, MAX_NATIVE_ROW_BYTES

def native_row_limit(pool, rss, ceiling=MAX_NATIVE_ROW_BYTES):
    """Reuse native Dataset's pool/16 and RSS/64 resident-copy allowance."""
    estimate = max(BATCH_TARGET_BYTES, min(MAX_NATIVE_ROW_BYTES, pool // 16, rss // 64))
    return max(1, min(ceiling, estimate, pool, max(1, rss // 2)))

def run_row_limit(limits):
    # The driver and one native worker share the run RSS allowance, as in Forge.
    native_rss = max(1, limits['max_rss_bytes'] // 2)
    pool = min(limits['memory_bytes'], max(1, native_rss // 2))
    return native_row_limit(pool, native_rss)

def resolve_payload_bounds(limits, overrides=None, *, inherited_explicit=(), ceiling=None, local_memory=False):
    """Default estimates never replace an explicitly declared finite ceiling."""
    explicit = set(overrides or ()) | set(inherited_explicit)
    derived = run_row_limit(limits)
    # A separately tightened local executor has historically shared this pool
    # between driver/worker payload and codec. Changing a worker-count ceiling
    # does not change which rows the action is allowed to accept.
    if local_memory and ceiling is not None and limits['memory_bytes'] < ceiling['memory_bytes']:
        derived = min(derived, max(1, limits['memory_bytes'] // 4))
    row = limits['max_row_bytes'] if 'max_row_bytes' in explicit else derived
    row = min(row, MAX_NATIVE_ROW_BYTES, limits['memory_bytes'], max(1, limits['max_rss_bytes'] // 4))
    batch = limits['max_batch_bytes'] if 'max_batch_bytes' in explicit else max(derived, row)
    batch = min(batch, limits['memory_bytes'])
    if ceiling is not None:
        row, batch = min(row, ceiling['max_row_bytes']), min(batch, ceiling['max_batch_bytes'])
    return dict(max_row_bytes=min(row, batch), max_batch_bytes=batch)

# Finite execution defaults; Forge may admit/tighten the same native budgets.

DEFAULT_LIMITS = dict(timeout_s=600, admission_timeout_s=30, cancel_grace_s=10,
    memory_bytes=256 * 1024**2, max_rss_bytes=4 * 1024**3,
    max_scratch_bytes=4 * 1024**3, max_output_bytes=2 * 1024**3, max_evidence_bytes=64 * 1024**2,
    max_input_rows=100000, max_output_rows=200000, batch_rows=256, log_bytes=16 * 1024**2)


DEFAULT_LIMITS.update(max_row_bytes=run_row_limit(DEFAULT_LIMITS),
                      max_batch_bytes=run_row_limit(DEFAULT_LIMITS))

RECOVERY_LIMITS = dict(max_state_bytes=16 * 1024**2, max_control_bytes=64 * 1024**2,
                       max_commits=16384, max_requests=100000, max_artifacts=65536,
                       checkpoint_rows=512, checkpoint_interval_ms=1000, checkpoint_timeout_s=60,
                       checkpoint_unconfirmed_rows=4096, checkpoint_unconfirmed_bytes=64 * 1024**2)


def declared_limit_keys(values, explicit):
    """A full admitted envelope may carry a smaller set of caller declarations."""
    from demiflow.execution.recovery_contract import RecoveryContractError
    if values is not None and type(values) is not dict:
        raise RecoveryContractError('Unknown execution budget')
    keys = frozenset(values or ())
    if explicit is None:
        return keys
    declared = frozenset(explicit)
    if not declared <= keys:
        raise RecoveryContractError("Explicit execution budgets must have admitted values")
    return declared


def execution_limits(values=None):
    from demiflow.execution.recovery_contract import RecoveryContractError
    result = {**DEFAULT_LIMITS, **RECOVERY_LIMITS}
    if values is not None:
        if type(values) is not dict or values.keys() - result.keys():
            raise RecoveryContractError("Unknown execution budget")
        result.update(values)
    if any(type(v) is not int or not 0 < v < 2**63 for v in result.values()):
        raise RecoveryContractError("Execution budgets must be finite positive integers")

    result.update(resolve_payload_bounds(result, values))
    if result["max_row_bytes"] > result["max_batch_bytes"]:
        raise RecoveryContractError("A row must fit the admitted batch budget")
    if result["max_commits"] > 16384 or result["max_artifacts"] > 65536:
        raise RecoveryContractError("Execution metadata cardinality exceeds protocol limits")
    return result


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
    "fairness_quantum_bytes", "control_timeout_ms", "memory_bytes",
}
_SCALE_OPTIONS = frozenset({"default_partitions", "max_replicas", "max_active_workers"})
_STR_OPTIONS = {"durability": _DURABILITY, "required_recovery": _RECOVERY,
                "worker_mode": frozenset({"process", "thread"})}
_BOOL_OPTIONS = frozenset({"fusion"})
_FREE_STR_OPTIONS = frozenset({"store_id"})
_OPTION_KEYS = (
    _INT_BUDGET_OPTIONS | _SCALE_OPTIONS | frozenset(_STR_OPTIONS)
    | _BOOL_OPTIONS | _FREE_STR_OPTIONS
)


def gpu_count(profile: ResourceProfile) -> int:
    total = 0
    for item in profile.requests.get("accelerators", []):
        if item.get("type") == "gpu":
            total += int(item.get("count", 0))
    return total


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
    explicit_limits: tuple[str, ...] = (),
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

    if "memory_bytes" in options:

        ceiling = dict(native)
        native["memory_bytes"] = _tighten(options["memory_bytes"], native["memory_bytes"], "memory_bytes")
        explicit = set(explicit_limits)
        if native['memory_bytes'] < ceiling['memory_bytes']:
            native.update(resolve_payload_bounds(native, {k: native[k] for k in explicit if k in native},
                                                 ceiling=ceiling, local_memory=True))

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

    mode = options.get("worker_mode", "process")
    if mode == "thread" and provider != "local":
        raise _reject("thread worker mode requires the local provider")
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
        worker_mode="thread" if mode == "thread" else None,
        max_active_workers=(_tighten(options["max_active_workers"], bounds.max_partitions, "max_active_workers")
                            if "max_active_workers" in options else None),
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


_NATIVE_RESOURCE_FIELDS = {'memory_bytes', 'max_rss_bytes', 'max_scratch_bytes', 'max_row_bytes', 'batch_rows',
           'threads', 'partitions', 'timeout_s', 'admission_timeout_s', 'cgroup_headroom_bytes'}

def parse_native_resources(value):
    """Snapshot a small closed budget; never expose engine configuration."""
    if value is None:
        return None
    if isinstance(value, tuple):
        # LocalKernelOptions may be copied by dataclasses.replace.
        if len(value) > len(_NATIVE_RESOURCE_FIELDS):
            raise ValueError('Too many native_resources fields')
        value = dict(value)
    if not isinstance(value, dict) or len(value) > len(_NATIVE_RESOURCE_FIELDS) or set(value) - _NATIVE_RESOURCE_FIELDS:
        raise ValueError('native_resources requires a dict of supported resource limits')
    for name, number in value.items():
        if name.endswith('_s'):
            valid = type(number) in (int, float) and math.isfinite(number) and number > 0
        else:
            valid = type(number) is int and number > 0
        if not valid:
            raise ValueError('native_resources ' + name + ' must be finite and positive')
    return tuple(sorted(value.items()))


PREFLIGHT_BOUNDS = Bounds(
    max_nodes=4096, max_edges=16384, max_partitions=4096, max_depth=64,
    max_control_bytes=65536, max_row_bytes=16 * 1024 * 1024,
    max_batch_bytes=64 * 1024 * 1024, max_resident_bytes=1024 * 1024 * 1024,
    max_spill_bytes=1024 * 1024 * 1024, max_state_bytes=256 * 1024 * 1024,
    max_pending_events=1024, max_open_files=256, max_output_rows=1000000,
    timeout_ms=60000)


def explain_policy(provider, options, *, action_kinds):
    """Validate explain declarations without source I/O or runtime admission."""
    if provider not in ("local", "ray_core"):
        raise DataflowError("INVALID_CONTRACT", "unknown provider: " + str(provider))
    metadata_fields = {'action_kind', 'terminal_category', 'terminal_native_options',
                       'source_parallelism_cap', 'terminal_parallelism_cap'}
    if not isinstance(options, Mapping):
        raise DataflowError('INVALID_CONTRACT', 'explain options must be a mapping')
    # Executor plan() carries declaration metadata alongside runtime policy.
    # Only these closed metadata fields are separated; unknown policy keys still
    # pass to the canonical validator and fail rather than being swallowed.
    runtime_options = {key: value for key, value in options.items() if key not in metadata_fields}
    policy, bounds = resolve_runtime_policy(runtime_options, {}, provider=provider)
    action_kind = options.get('action_kind', 'collect_summary')
    if type(action_kind) is not str or action_kind not in action_kinds:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'unknown explain action kind')
    category = options.get('terminal_category', 'action')
    if type(category) is not str or category not in ('action', 'sink'):
        raise DataflowError('INVALID_CONTRACT', 'unknown explain terminal category')
    for name in ('source_parallelism_cap', 'terminal_parallelism_cap'):
        value = options.get(name)
        if value is not None and (type(value) is not int or not 1 <= value <= bounds.max_partitions):
            raise DataflowError('INVALID_CONTRACT', name + ' must be within the partition budget')
    if options.get('terminal_native_options') is not None:
        normalize_native_options(options['terminal_native_options'], operation_kind=action_kind)
    return policy, bounds
