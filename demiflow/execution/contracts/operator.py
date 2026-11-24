"""K2 OperatorSpec: passive durable operator values and validation (§7).

The persistent protocol accepts artifact entries plus JSON argument digests
only; cloudpickle bytes never enter this document. Existing in-run pickle
paths and CallableSpec.create/instantiate semantics are untouched.
"""
import re

from demiflow.execution.contracts.recovery_contract import document, identifier, identity, RecoveryContractError
from .errors import ProtocolError

SPEC_SCHEMA = "demiflow.operator_spec.v1"
SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_TOP_KEYS = frozenset({
    "schema_version", "spec_id", "logical_node_id", "occurrence_id", "entry",
    "lifecycle", "io", "cardinality", "ordering", "state", "effect", "batching",
    "resources", "closure_check",
})
CARDINALITY = frozenset({"zero", "one", "many", "zero_or_one"})
ORDERING = frozenset({"unordered", "preserve_input", "requires_global_order"})
OWNERSHIP = frozenset({"none", "partitionable", "shared_colocated"})
EFFECTS = frozenset({"pure", "external_paid", "external_unknown"})


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
        if SHA256.fullmatch(entry[name]) is None:
            raise ProtocolError("INVALID_SPEC", name + " must be a digest")
    lifecycle = value["lifecycle"]
    if type(lifecycle) is not dict or set(lifecycle) != {"kind", "astart", "astop", "aclose"} \
            or lifecycle["kind"] not in ("fn", "actor") \
            or any(type(lifecycle[name]) is not bool for name in ("astart", "astop", "aclose")):
        raise ProtocolError("INVALID_SPEC", "lifecycle declaration is unsupported")
    if value["cardinality"] not in CARDINALITY or value["ordering"] not in ORDERING:
        raise ProtocolError("INVALID_SPEC", "unsupported trait value")
    state = value["state"]
    if type(state) is not dict or not {"ownership", "adapter_present"} <= set(state) \
            or set(state) - {"ownership", "adapter_present", "state_schema_version"} \
            or state["ownership"] not in OWNERSHIP or type(state["adapter_present"]) is not bool:
        raise ProtocolError("INVALID_SPEC", "state declaration is unsupported")
    if state["ownership"] != "none" and not state["adapter_present"]:
        raise ProtocolError("STATE_ADAPTER_MISSING", "stateful operator lacks a state adapter")
    if "state_schema_version" in state and (
            type(state["state_schema_version"]) is not int or state["state_schema_version"] < 1):
        raise ProtocolError("INVALID_SPEC", "state_schema_version must be a positive int")
    effect = value["effect"]
    if type(effect) is not dict or set(effect) != {"kind", "request_key_derivation"} \
            or effect["kind"] not in EFFECTS \
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
