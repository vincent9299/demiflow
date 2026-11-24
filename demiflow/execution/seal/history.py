"""History implementation."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from demiflow.execution.contracts.dataflow import ArtifactRef, HistoricalBinding, SealedExecution
from demiflow.execution.contracts.dataflow_errors import DataflowError
_err = DataflowError
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, identifier
from demiflow.execution.contracts.dataflow_codec import history_carrier_reference as _carrier_ref, decode_history_reference as _ref_from_json, history_reference_list as _ref_list
from demiflow.execution.storage.reading import read_history_carrier as _read_carrier
from demiflow.execution.seal.plan import _validate_seal_marker, verify_fit_membership
SEALED_PLAN_FACTS_SCHEMA = "demiflow.sealed_plan_facts.v1"
_MAX_SEALED_PLAN_BYTES = 4 * 1024 ** 2
_SEALED_REF_KEYS = ("logical_ref", "physical_ref", "code_pin_ref",
                    "identity_bridge_ref", "constants_ref")
_CODE_PIN_MEDIA = "application/x-demiflow-code-pin"
RUN_STORE_ID = "run"


def offline_model_declaration(directory, action_id, node_id, *, route, store, bounds):
    """Verify the original sealed factory and return its offline input declaration."""
    from demiflow.execution.seal.plan import load_dataflow, load_sealed_dataflow, verify_dataflow_program
    from demiflow.execution.operator_bindings.dataflow_model_factory import PromptFactory, RoutedPromptFactory
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    graph, _, sealed = load_dataflow(directory, action_id,
                                    store=store, bounds=bounds)
    node = next((node for node in graph.nodes if node.occurrence_id == node_id), None)
    if node is None or node.kernel.kind not in {'prompt', 'agent'} or node.program_ref is None:
        raise DataflowError('INVALID_CONTRACT', 'Offline lookup has no sealed model occurrence')
    _, _, pin = load_sealed_dataflow(sealed, action_id=action_id, store=store, bounds=bounds)
    program = verify_dataflow_program(node.program_ref,
        expected_pin=pin['programs'][node_id], store=store, bounds=bounds)
    spec = program.value
    if route is not None:
        if (not isinstance(spec, CallableSpec) or spec.target is not RoutedPromptFactory
                or len(spec.constructor_args) != 5 or route not in spec.constructor_args[0]):
            raise DataflowError('INVALID_CONTRACT', 'Offline route is absent from the sealed prompt factory')
        spec = spec.constructor_args[0][route]
    if (not isinstance(spec, CallableSpec) or spec.target is not PromptFactory
            or len(spec.constructor_args) != 2):
        raise DataflowError('INVALID_CONTRACT', 'Offline lookup requires the original prompt factory')
    operation, _ = spec.constructor_args
    options = dict(operation.options or {})
    if set(options) not in ({'offline_dir'}, {'offline_store'}):
        raise DataflowError('INVALID_CONTRACT', 'Occurrence declares no offline input store')
    declaration = options, operation.config.prompt_definitions[operation.prompt_name].model
    maximum = sum(len(node.kernel.configuration.get('routes', {})) or 1 for node in graph.nodes)
    return declaration, maximum


def load_v3_binding(run_id, directory, action_id, marker):
    _validate_seal_marker(marker)
    schema = marker["schema"]
    if marker["run_id"] != run_id:
        raise _err("INVALID_CONTRACT", "v3 seal run_id differs from the journal recovery identity")
    if marker["action_id"] != action_id:
        raise _err("INVALID_CONTRACT", "v3 seal action_id differs from the requested action")
    for key in ("dag_id", "plan_id"):
        try:
            identifier(marker[key])
        except (RecoveryContractError, ValueError) as exc:
            raise _err("INVALID_CONTRACT", key + " invalid: " + str(exc))
    refs = {key: _ref_from_json(marker[key], key) for key in _SEALED_REF_KEYS}
    programs = tuple(_ref_from_json(item, "programs[]") for item in _ref_list(marker["programs"]))
    source_refs = tuple(_ref_from_json(item, "source_refs[]") for item in _ref_list(marker["source_refs"]))
    sealed = SealedExecution(logical_ref=refs["logical_ref"], physical_ref=refs["physical_ref"],
                             programs=programs, code_pin_ref=refs["code_pin_ref"],
                             identity_bridge_ref=refs["identity_bridge_ref"],
                             source_refs=source_refs, constants_ref=refs["constants_ref"])
    from demiflow.execution.seal.sealed_plans import sealed_read_bounds
    from demiflow.execution.storage.objects import readonly_run_store
    from demiflow.execution.seal.plan import load_sealed_dataflow
    bounds = sealed_read_bounds()
    store = readonly_run_store(directory, store_id=sealed.physical_ref.store_id, bounds=bounds)
    graph, physical, _ = load_sealed_dataflow(sealed, action_id=action_id, store=store, bounds=bounds)
    verify_fit_membership(marker, graph)
    if "observation_labels" in marker:
        labels = marker["observation_labels"]
        if (type(labels) is not dict
                or not set(labels) <= {node.occurrence_id for node in graph.nodes}
                or any(type(value) is not str or not value for value in labels.values())
                or len(set(labels.values())) != len(labels)):
            raise _err("INVALID_CONTRACT", "v3 observation labels must uniquely name original occurrences")
        from demiflow.execution.contracts.recovery_contract import document
        try:
            document(labels, bounds.max_control_bytes)
        except RecoveryContractError as exc:
            raise _err("INVALID_CONTRACT", "v3 observation labels exceed their original control budget") from exc
    if (graph.dag_id != marker["dag_id"] or physical.plan_id != marker["plan_id"]
            or graph.dag_id != marker["graph_digest"] or physical.plan_id != marker["physical_digest"]
            or physical.plan_revision != marker["plan_revision"]):
        raise _err("CORRUPT_ARTIFACT", "sealed facts differ from original graph/physical artifacts")
    return HistoricalBinding(schema_version=schema, original_plan_ref=sealed.physical_ref,
                             original_code_pin_ref=sealed.code_pin_ref, compatible=True,
                             reason=None, restore_image=None)

def load_native_binding(run_id, directory, action_id, facts, *, max_bytes):
    from demiflow.planning.dag import validate_logical_dag, validate_physical_execution_plan
    if type(facts) is not dict:
        raise _err("INCOMPATIBLE_HISTORY", "sealed plan facts are not a JSON object")
    schema = facts.get("schema")
    if schema != SEALED_PLAN_FACTS_SCHEMA:
        raise _err("INCOMPATIBLE_HISTORY",
                   "unsupported sealed plan facts schema: " + repr(schema))
    expected_keys = {"schema", "run_id", "action_id", "dag_id", "plan_id",
                     "artifact_manifest_ref", "logical_dag_ref", "physical_plan_ref", "sealed_at"}
    if set(facts) != expected_keys:
        raise _err("INVALID_CONTRACT", "sealed plan facts key set is unsupported")
    if facts["run_id"] != run_id:
        raise _err("INVALID_CONTRACT", "F1 run_id differs from the journal recovery identity")
    if facts["action_id"] != action_id:
        raise _err("INVALID_CONTRACT", "F1 action_id differs from the requested action")
    logical = _read_carrier(directory, facts["logical_dag_ref"], max_bytes=max_bytes)
    physical = _read_carrier(directory, facts["physical_plan_ref"], max_bytes=max_bytes)
    try:
        validate_logical_dag(logical)
        validate_physical_execution_plan(physical)
    except (ValueError, RecoveryContractError) as exc:
        raise _err("INVALID_CONTRACT", "sealed plan failed original validation: " + str(exc))
    if logical["dag_id"] != facts["dag_id"] or physical["plan_id"] != facts["plan_id"]:
        raise _err("INVALID_CONTRACT", "sealed dag_id/plan_id differs from the facts")
    if logical["action_id"] != action_id:
        raise _err("INVALID_CONTRACT", "sealed dag action_id differs from the facts")
    code_pin = _read_code_pin(directory, facts, max_bytes=max_bytes)
    return HistoricalBinding(schema_version=schema,
                             original_plan_ref=_carrier_ref(facts["physical_plan_ref"]),
                             original_code_pin_ref=code_pin, compatible=True,
                             reason=None, restore_image=None)

def _read_code_pin(directory, facts, *, max_bytes):
    """Extract the original code-pin identity from the sealed K1 manifest.

    The manifest is content-addressed at the run root by its sha256. The code-pin
    digest is the executed business code identity (``definition_ref``), never a
    recomputed hash of current source. A missing code source is reported later by
    ``validate_resume_compatibility``; here a missing manifest is incompatible.
    """
    manifest_ref = facts["artifact_manifest_ref"]
    if type(manifest_ref) is not dict or set(manifest_ref) != {"manifest_id", "sha256"}:
        raise _err("INVALID_CONTRACT", "manifest_ref needs manifest_id/sha256")
    digest = manifest_ref["sha256"]
    try:
        identifier(digest)
    except (RecoveryContractError, ValueError) as exc:
        raise _err("INVALID_CONTRACT", "manifest sha256 invalid: " + str(exc))
    path = directory / (digest + ".json")
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise _err("INVALID_CONTRACT", "manifest path contains a symlink")
    if not path.is_file():
        raise _err("INCOMPATIBLE_HISTORY", "sealed K1 manifest is missing; original code identity unknown")
    ceiling = min(max_bytes, _MAX_SEALED_PLAN_BYTES)
    if path.stat().st_size > ceiling:
        raise _err("BUDGET_EXCEEDED", "sealed manifest exceeds the read budget")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise _err("CORRUPT_ARTIFACT", "sealed manifest digest differs")
    try:
        manifest = json.loads(raw)
    except ValueError:
        raise _err("CORRUPT_ARTIFACT", "sealed manifest is not valid JSON")
    try:
        entrypoints = manifest["business"]["entrypoints"]
        definition = entrypoints[0]["definition_ref"]
        code_digest = definition["sha256"]
        location = definition["location"]
    except (KeyError, IndexError, TypeError):
        raise _err("INCOMPATIBLE_HISTORY", "sealed manifest has no code-pin definition_ref")
    try:
        identifier(code_digest)
    except (RecoveryContractError, ValueError) as exc:
        raise _err("INVALID_CONTRACT", "code-pin digest invalid: " + str(exc))
    if type(location) is not str or not location:
        raise _err("INVALID_CONTRACT", "code-pin location is not a bounded string")
    size = 0
    code_path = directory / location
    if not any(item.is_symlink() for item in (code_path, *code_path.parents)) and code_path.is_file():
        size = code_path.stat().st_size
    return ArtifactRef(digest=code_digest, size_bytes=size, store_id=RUN_STORE_ID,
                       key=location, media_type=_CODE_PIN_MEDIA)
