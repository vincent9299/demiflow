"""L001 unit: execution_bindings.v2 structural/relational validation.

Pure interpretation against the real frozen compilers and a fixed-schema v2
document. Positive: a driver-only native_driver_v2 binding validates. Negative:
unsupported slice, v2 worker placement/group, bad backend/recovery, keyset,
duplicate/coverage/ownership gaps and identity drift all fail closed on .reason.
The v1 path is never relaxed (a v1 document still validates unchanged).
"""
import copy

import pytest

from demiflow.execution.seal.plan_interpreter import EXECUTION_BINDINGS_SCHEMA_V2, PlanContractError, interpret_physical_plan
from demiflow.execution.recovery_contract import identity
from demiflow.planning.dag import compile_physical_execution_plan
from test_execution_layout import PROFILE_REF, RECOVERY, bundle_for, chain_fixture, stage_id
import _v2_native_fixtures as fx


def _interpret_doc(n, doc):
    return interpret_physical_plan(n.ld, n.pp, bindings=bundle_for(doc), manifest_ref=n.mref)


def _reason(exc):
    assert isinstance(exc.value, PlanContractError)
    return exc.value.reason


def test_v2_driver_binding_validates():
    n = fx.native_v2_doc()
    layout = _interpret_doc(n, n.doc)
    assert layout.supported_slice == "native_driver_v1"
    assert len(layout.executions) == 1
    assert layout.executions[0].placement == "driver"


def test_v1_document_still_validates_unchanged():
    v1 = chain_fixture()
    layout = interpret_physical_plan(v1.ld, v1.pp, bindings=v1.bundle, manifest_ref=v1.mref)
    assert layout.supported_slice == "chain_v1"
    assert layout.executions == ()


def test_unsupported_v2_slice_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["supported_slice"] = "chain_v1"
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "unsupported_layout"


def test_v2_worker_placement_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["executions"][0]["placement"] = "worker"
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "unsupported_layout"


def test_v2_worker_group_id_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["executions"][0]["group_id"] = "g0"
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "invalid_contract"


def test_unknown_backend_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["executions"][0]["backend"] = "ray"
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "invalid_contract"


def test_unknown_recovery_guarantee_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["executions"][0]["recovery"] = "bogus"
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "invalid_contract"


def test_missing_executions_key_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    del doc["executions"]
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "invalid_contract"


def test_duplicate_execution_stage_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["executions"][0]["stage_ids"] = n.op_stages * 2
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "invalid_contract"


def test_program_coverage_gap_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["executions"][0]["programs"] = []
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "invalid_contract"


def test_driver_missing_terminal_stage_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["driver_stage_ids"] = n.driver_stage_ids[:-1]
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "invalid_contract"


def test_dag_identity_drift_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["dag_id"] = identity(["wrong-dag"])
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "identity_mismatch"


def test_unknown_profile_ref_rejected():
    n = fx.native_v2_doc()
    doc = copy.deepcopy(n.doc)
    doc["executions"][0]["profile_ref"] = identity(["absent-profile"])
    with pytest.raises(PlanContractError) as exc:
        _interpret_doc(n, doc)
    assert _reason(exc) == "invalid_contract"


def test_nonempty_worker_groups_rejected():
    n = fx.native_v2_doc()
    op_nodes = n.ld["nodes"][1:-1]
    pp = compile_physical_execution_plan(
        logical_dag=n.ld, plan_revision=0, artifact_manifest_ref=n.mref,
        worker_groups=[{"group_id": "g0", "role": "cpu_prepare", "profile_ref": PROFILE_REF,
                        "stage_refs": [stage_id(n.ld, node) for node in op_nodes],
                        "min_workers": 1, "initial_workers": 1, "max_workers": 1}],
        recovery=dict(RECOVERY), feasibility_snapshot_ref=identity(["test-feasibility"]),
        read_domain="test-domain", durability_declared="durable",
        partition_count=1, scheme_kind="single")
    doc = copy.deepcopy(n.doc)
    doc["plan_id"] = pp["plan_id"]
    with pytest.raises(PlanContractError) as exc:
        interpret_physical_plan(n.ld, pp, bindings=bundle_for(doc), manifest_ref=n.mref)
    assert _reason(exc) == "unsupported_layout"


def test_v2_schema_constant_is_additive():
    assert EXECUTION_BINDINGS_SCHEMA_V2 == "demiflow.execution_bindings.v2"