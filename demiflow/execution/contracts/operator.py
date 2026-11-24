"""K2 OperatorSpec: durable operator identity compiled from CallableSpec (§7).

The persistent protocol accepts artifact entries plus JSON argument digests
only; cloudpickle bytes never enter this document. Existing in-run pickle
paths and CallableSpec.create/instantiate semantics are untouched.
"""
import inspect
import re

from demiflow.execution.contracts.recovery_contract import document, identifier, identity, RecoveryContractError
from .errors import ProtocolError

SPEC_SCHEMA = "demiflow.operator_spec.v1"
_MODULE_KEY = re.compile(r"p_[0-9a-f]{24}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_TOP_KEYS = frozenset({
    "schema_version", "spec_id", "logical_node_id", "occurrence_id", "entry",
    "lifecycle", "io", "cardinality", "ordering", "state", "effect", "batching",
    "resources", "closure_check",
})
_CARDINALITY = frozenset({"zero", "one", "many", "zero_or_one"})
_ORDERING = frozenset({"unordered", "preserve_input", "requires_global_order"})
_OWNERSHIP = frozenset({"none", "partitionable", "shared_colocated"})
_EFFECTS = frozenset({"pure", "external_paid", "external_unknown"})


def _json_digest(value, label):
    try:
        document(value)
    except RecoveryContractError as error:
        raise ProtocolError("INVALID_SPEC", label + " must be strict JSON for the durable protocol") from error
    return identity(value)


def _callable_path(target):
    """Importable artifact entry path; lambdas/local scopes/__main__ reject."""
    if inspect.isclass(target):
        module, qualname = target.__module__, target.__qualname__
    elif inspect.ismethod(target):
        owner = target.__self__
        cls = owner if inspect.isclass(owner) else type(owner)
        module, qualname = cls.__module__, cls.__qualname__ + "." + target.__name__
    elif inspect.isfunction(target):
        module, qualname = target.__module__, target.__qualname__
    else:
        module, qualname = type(target).__module__, type(target).__qualname__
    if module == "__main__" or "<lambda>" in qualname or "<locals>" in qualname:
        raise ProtocolError("INVALID_SPEC",
                            "callable has no durable artifact entry: " + module + "." + qualname)
    return module + "." + qualname


def _lifecycle_facts(target):
    kind = "actor" if inspect.isclass(target) or not inspect.isfunction(target) else "fn"
    if inspect.ismethod(target):
        kind = "actor"
    owner = target if inspect.isclass(target) else (
        target.__self__ if inspect.ismethod(target) else (
            target if kind == "actor" else None))
    hooks = {}
    for name in ("astart", "astop", "aclose"):
        hooks[name] = owner is not None and callable(getattr(owner, name, None))
    return {"kind": kind, **hooks}


def _adapter_present(target):
    owner = target
    if inspect.isfunction(owner):
        return False
    # A class is a factory blueprint: adapter_present means the platform's
    # deferred adaptation is available, not that an instance was verified. The
    # real instance is the mandatory check after construction (never built here).
    if inspect.isclass(owner):
        return True
    if inspect.ismethod(owner):
        owner = owner.__self__
    return callable(getattr(owner, "snapshot_state", None)) and callable(getattr(owner, "restore_state", None))


def compile_operator_spec(callable_spec, *, logical_node_id, occurrence_id,
                          artifact_manifest_ref, module_key, traits=None,
                          bound_inputs=None, bound_outputs=None,
                          profile_ref=None, native_options_digest=None,
                          batching=None):
    """Compile existing fn/actor declarations without changing their semantics."""
    identifier(logical_node_id)
    identifier(occurrence_id)
    if type(artifact_manifest_ref) is not dict or set(artifact_manifest_ref) != {"manifest_id", "sha256"}:
        raise ProtocolError("INVALID_SPEC", "artifact_manifest_ref needs manifest_id/sha256")
    identifier(artifact_manifest_ref["manifest_id"])
    if _SHA256.fullmatch(artifact_manifest_ref["sha256"]) is None:
        raise ProtocolError("INVALID_SPEC", "artifact_manifest_ref.sha256 must be a digest")
    if type(module_key) is not str or _MODULE_KEY.fullmatch(module_key) is None:
        raise ProtocolError("INVALID_SPEC", "module_key must be a bundle-bound p_<24 hex> key")
    target = callable_spec.target
    entry_path = _callable_path(target)
    constructor = {"args": list(callable_spec.constructor_args),
                   "kwargs": dict(callable_spec.constructor_kwargs)}
    call = {"args": list(callable_spec.call_args), "kwargs": dict(callable_spec.call_kwargs)}
    entry = {
        "artifact_manifest_ref": dict(artifact_manifest_ref), "module_key": module_key,
        "callable_path": entry_path,
        "constructor_args_digest": _json_digest(constructor, "constructor arguments"),
        "call_args_digest": _json_digest(call, "call arguments"),
    }
    traits = traits or {}
    cardinality = traits.get("cardinality", "one")
    ordering = traits.get("ordering", "unordered")
    ownership = traits.get("state_ownership", "none")
    effect_kind = traits.get("effect_kind", "pure")
    if cardinality not in _CARDINALITY or ordering not in _ORDERING \
            or ownership not in _OWNERSHIP or effect_kind not in _EFFECTS:
        raise ProtocolError("INVALID_SPEC", "unsupported trait value")
    adapter = _adapter_present(target)
    if not adapter and not inspect.isfunction(target) and not inspect.isclass(target):
        # A hookless plain-data instance is platform-adaptable (auto_json) only
        # under shared_colocated; the same C04 policy gates precheck and compile.
        from demiflow.execution.compiler_support.operator_policy import classify_operator_state
        policy = classify_operator_state(target, phase="instance", ownership=ownership)
        if policy.mode == "auto_json" and ownership == "shared_colocated":
            adapter = True
    if ownership != "none" and not adapter:
        raise ProtocolError("STATE_ADAPTER_MISSING",
                            "stateful operator lacks snapshot_state/restore_state")
    if ownership == "none" and not inspect.isfunction(target) and not inspect.isclass(target):
        instance_state = getattr(target, "__dict__", {})
        if instance_state and not adapter:
            raise ProtocolError("STATE_ADAPTER_MISSING",
                                "callable instance carries state without a state adapter")
    state = {"ownership": ownership, "adapter_present": adapter}
    contract = callable_spec.runtime_contract or {}
    derivation = "declared" if contract.get("request_key_derivation") == "declared" else "occurrence_lineage"
    if batching is None:
        batching = {"unit": traits.get("batching", "record")}
    if type(batching) is not dict or batching.get("unit") not in ("record", "batch") \
            or set(batching) - {"unit", "max_batch", "flush_interval_ms"}:
        raise ProtocolError("INVALID_SPEC", "unsupported batching declaration")
    if native_options_digest is not None and _SHA256.fullmatch(native_options_digest) is None:
        raise ProtocolError("INVALID_SPEC", "native_options_digest must be a digest")
    resources = {"profile_ref": profile_ref}
    if native_options_digest is not None:
        resources["native_options_digest"] = native_options_digest
    value = {
        "schema_version": SPEC_SCHEMA,
        "logical_node_id": logical_node_id, "occurrence_id": occurrence_id,
        "entry": entry, "lifecycle": _lifecycle_facts(target),
        "io": {"input_schema_ref": None, "output_schema_ref": None,
               "bound_inputs": dict(bound_inputs) if bound_inputs is not None else None,
               "bound_outputs": dict(bound_outputs) if bound_outputs is not None else None},
        "cardinality": cardinality, "ordering": ordering, "state": state,
        "effect": {"kind": effect_kind, "request_key_derivation": derivation},
        "batching": batching, "resources": resources,
        "closure_check": {"packable": True, "method": "artifact_entry"},
    }
    value["spec_id"] = spec_id(value)
    return validate_operator_spec(value)


def spec_id(value):
    return identity({key: item for key, item in value.items() if key != "spec_id"})


def validate_operator_spec(value):
    if type(value) is not dict or set(value) != _TOP_KEYS:
        raise ProtocolError("INVALID_SPEC", "operator spec key set is unsupported")
    if value["schema_version"] != SPEC_SCHEMA:
        raise ProtocolError("PROTOCOL_VERSION_UNSUPPORTED",
                            "unsupported operator spec schema: " + str(value["schema_version"]))
    identifier(value["logical_node_id"])
    identifier(value["occurrence_id"])
    entry = value["entry"]
    if type(entry) is not dict or set(entry) != {
            "artifact_manifest_ref", "module_key", "callable_path",
            "constructor_args_digest", "call_args_digest"}:
        raise ProtocolError("INVALID_SPEC", "operator entry key set is unsupported")
    if type(entry["callable_path"]) is not str or not entry["callable_path"]:
        raise ProtocolError("INVALID_SPEC", "callable_path is required")
    for name in ("constructor_args_digest", "call_args_digest"):
        if _SHA256.fullmatch(entry[name]) is None:
            raise ProtocolError("INVALID_SPEC", name + " must be a digest")
    lifecycle = value["lifecycle"]
    if type(lifecycle) is not dict or set(lifecycle) != {"kind", "astart", "astop", "aclose"} \
            or lifecycle["kind"] not in ("fn", "actor") \
            or any(type(lifecycle[name]) is not bool for name in ("astart", "astop", "aclose")):
        raise ProtocolError("INVALID_SPEC", "lifecycle declaration is unsupported")
    if value["cardinality"] not in _CARDINALITY or value["ordering"] not in _ORDERING:
        raise ProtocolError("INVALID_SPEC", "unsupported trait value")
    state = value["state"]
    if type(state) is not dict or not {"ownership", "adapter_present"} <= set(state) \
            or set(state) - {"ownership", "adapter_present", "state_schema_version"} \
            or state["ownership"] not in _OWNERSHIP or type(state["adapter_present"]) is not bool:
        raise ProtocolError("INVALID_SPEC", "state declaration is unsupported")
    if state["ownership"] != "none" and not state["adapter_present"]:
        raise ProtocolError("STATE_ADAPTER_MISSING", "stateful operator lacks a state adapter")
    if "state_schema_version" in state and (
            type(state["state_schema_version"]) is not int or state["state_schema_version"] < 1):
        raise ProtocolError("INVALID_SPEC", "state_schema_version must be a positive int")
    effect = value["effect"]
    if type(effect) is not dict or set(effect) != {"kind", "request_key_derivation"} \
            or effect["kind"] not in _EFFECTS \
            or effect["request_key_derivation"] not in ("occurrence_lineage", "declared"):
        raise ProtocolError("INVALID_SPEC", "effect declaration is unsupported")
    closure = value["closure_check"]
    if type(closure) is not dict or set(closure) != {"packable", "method"} \
            or closure["method"] not in ("artifact_entry", "validated_pickle"):
        raise ProtocolError("INVALID_SPEC", "closure_check is unsupported")
    if closure["method"] == "artifact_entry" and closure["packable"] is not True:
        raise ProtocolError("INVALID_SPEC", "durable artifact entries must be packable")
    if spec_id(value) != value["spec_id"]:
        raise ProtocolError("ARTIFACT_MISMATCH", "spec_id does not bind the content")
    document(value)
    return value