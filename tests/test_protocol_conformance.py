"""U1 protocol conformance matrix (E2 design draft §14).

Pure protocol/compilation-level tests only: no execution chain, no transport,
no real channels. Node ids map one-to-one to the reviewed design matrix.
PRT-01/PRT-02 are covered by the separate baseline regression run and the
protection evidence, not by this file.
"""
import pytest

from demiflow.data import plan as ops
from demiflow.data.plan import CallableSpec, LogicalPlan
from demiflow.data.sources import ItemsSource
from demiflow.execution.contracts import (
    artifact, checkpoint, envelope, exchange, operator, worker,
)
from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.recovery_contract import identity
from demiflow.planning.dag import (
    compile_declared_dag, compile_logical_dag, compile_physical_execution_plan,
    select_exchange,
)
from demiflow.planning.model import OperatorTraits
from demiflow.planning.traits import operation_traits, unified_operation_traits


def _double(row):
    return row


class _Counter:
    def __init__(self, start=0):
        self.value = start

    def __call__(self, row):
        self.value += 1
        return row

    def snapshot_state(self):
        return {"value": self.value}

    def restore_state(self, state):
        self.value = state["value"]


class _StatefulNoAdapter:
    def __call__(self, row):
        return row


def _code(error):
    return error.value.code


def _manifest(**overrides):
    values = dict(
        run_id="run-1",
        platform={"source_digest": "a" * 64, "distribution": "demiurge",
                  "version": "1.0", "runtime_identity_ref": "runtime-ref"},
        business={"bundle_digest": "sha256:" + "b" * 64,
                  "entrypoints": [{"name": "main", "module_key": "p_" + "b" * 24,
                                   "definition_ref": {"sha256": "c" * 64}}]},
        config={"resolved_config_digest": "d" * 64, "model_config_refs": []},
        environment={"requirements_sha256": "e" * 64, "lock_digest": "f" * 64,
                     "resolution_digest": "0" * 64},
        protocol_versions={"worker": {"major": 1, "minor": 0},
                           "exchange": {"major": 1, "minor": 0},
                           "checkpoint_schema": 1, "state_schema": 1,
                           "resource_protocol": "1.0"},
        secret_refs=[],
    )
    values.update(overrides)
    return artifact.build_manifest(**values)


def _grant(**overrides):
    value = {"grant_id": "g1", "run_id": "run-1", "attempt": 1, "fence": "f1",
             "scope_id": "sc1", "allocation_id": "al1", "boot_id": "bo1",
             "controller_epoch": 3, "expires_at_ms": 10000}
    value.update(overrides)
    return value


def _message(operation_id, kind, payload, *, epoch=3, boot="bo1", allocation="al1"):
    value = {"protocol_version": {"major": 1, "minor": 0}, "kind": kind,
             "scope_id": "sc1", "run_id": "run-1", "operation_id": operation_id,
             "controller_epoch": epoch, "authority_ref": "auth-1",
             "boot_id": boot, "payload": payload}
    if allocation is not None:
        value["allocation_id"] = allocation
    return value


def _model(**kwargs):
    return worker.WorkerProtocolModel(
        scope_id="sc1", run_id="run-1", allocation_id="al1", boot_id="bo1",
        controller_epoch=3, grant=_grant(), capabilities=["gpu"], **kwargs)


def _register(model, operation_id="op-register"):
    return model.handle(_message(operation_id, "Register", {
        "boot_id": "bo1", "allocation_id": "al1", "scope_id": "sc1",
        "capabilities": ["gpu"], "artifact_digest_observed": "a" * 64}), now_ms=1)


def _assignment(**overrides):
    value = {"schema_version": "demiflow.worker_assignment.v1",
             "assignment_id": "as1", "operation_id": "op-assign",
             "scope_id": "sc1", "allocation_id": "al1", "boot_id": "bo1",
             "worker_id": "w1", "controller_epoch": 3, "grant_ref": "g1",
             "plan_revision": 2, "expected_plan_revision": 1,
             "artifact_manifest_ref": {"manifest_id": "m" * 64, "sha256": "a" * 64},
             "stages": ["st1"], "partitions": ["p0"],
             "channels": {"inputs": [{"channel_id": "c1"}], "outputs": ["c2"]},
             "restore_point": None, "route_epoch": 0, "topology_revision": 0,
             "ready_deadline_ms": 5000}
    value.update(overrides)
    return value


def _checkpoint_manifest(**overrides):
    value = {
        "schema_version": "demiflow.checkpoint_manifest.v1",
        "checkpoint_id": "ck1", "run_id": "run-1", "action_id": "action-1",
        "epoch": 1, "topology_revision": 0, "route_epoch": 0, "plan_revision": 0,
        "artifact_manifest_ref": {"manifest_id": "m" * 64, "sha256": "a" * 64},
        "participants": {
            "sources": [{"source_id": "src-1", "partition_id": "p0"}],
            "channels": ["ch1"],
            "operators": [{"logical_node_id": "n1", "occurrence_id": "o1"}],
            "sinks": ["sink1"],
        },
        "source_progress": [{"source_id": "src-1", "partition_id": "p0",
                             "position": {"next_ordinal": 7}}],
        "channel_cuts": [{"channel_id": "ch1", "producer_cut_sequence": 4,
                          "consumer_offsets": {"reader-1": 4}}],
        "operator_states": [{"logical_node_id": "n1", "occurrence_id": "o1",
                             "state_ref": {"state_id": "st1", "path": "state/st1",
                                           "bytes": 10, "sha256": "a" * 64}}],
        "inflight": [{"request_key": "req-1", "status": "known_completed",
                      "effect_ref": "eff-1"},
                     {"request_key": "req-2", "status": "unknown"}],
        "sink_receipts": [{"sink_id": "sink1", "commit_id": "cm1",
                           "receipt_ref": "rc1"}],
        "seal": {"sealed": False},
    }
    value.update(overrides)
    return value


def _barrier(channel_id, sequence, checkpoint_id="ck1", cut=None):
    return {"kind": "barrier", "channel_id": channel_id, "sequence": sequence,
            "checkpoint_id": checkpoint_id, "epoch": 1,
            "cut": {"producer_sequence_bound": sequence if cut is None else cut},
            "topology_revision": 0}


def _linear_dag(action_id="action-1"):
    source = ItemsSource(items=({"x": 1}, {"x": 2}))
    plan = LogicalPlan((ops.MapOp(callable=CallableSpec.create(_double)),
                        ops.FilterOp(callable=CallableSpec.create(_double))))
    return compile_logical_dag(action_id=action_id, source=source,
                               operations=plan.operations, terminal_kind="count")


def _physical(dag, **overrides):
    values = dict(
        logical_dag=dag, plan_revision=0,
        artifact_manifest_ref={"manifest_id": "m" * 64, "sha256": "a" * 64},
        worker_groups=[],
        recovery={"checkpoint_interval_ms": 1000, "unconfirmed_rows": 100,
                  "unconfirmed_bytes": 1024, "terminals": []},
        feasibility_snapshot_ref="snap-1", read_domain="local",
    )
    values.update(overrides)
    return compile_physical_execution_plan(**values)


# --- VER: version/capability negotiation -----------------------------------

def test_ver_01_major_mismatch_rejects():
    with pytest.raises(ProtocolError) as error:
        envelope.negotiate_version({"major": 1, "minor": 0}, {"major": 2, "minor": 0})
    assert _code(error) == "PROTOCOL_VERSION_UNSUPPORTED"
    with pytest.raises(ProtocolError) as error:
        artifact.validate_manifest(dict(_manifest(), schema_version="demiflow.run_artifact_manifest.v2"))
    assert _code(error) == "PROTOCOL_VERSION_UNSUPPORTED"


def test_ver_02_minor_extensions_negotiate_down():
    assert envelope.negotiate_version({"major": 1, "minor": 2}, {"major": 1, "minor": 0}) == {
        "major": 1, "minor": 0}
    assert envelope.negotiate_version({"major": 1, "minor": 0}, {"major": 1, "minor": 3}) == {
        "major": 1, "minor": 0}


def test_ver_03_required_capability_missing_rejects():
    with pytest.raises(ProtocolError) as error:
        envelope.require_capabilities(["gpu"], ["gpu", "tpu"])
    assert _code(error) == "CAPABILITY_REQUIRED_MISSING"
    model = _model()
    _register(model)
    with pytest.raises(ProtocolError) as error:
        model.handle(_message("op-verify", "VerifyArtifactAndCapabilities", {
            "artifact_manifest_ref": {"manifest_id": "m" * 64, "sha256": "a" * 64},
            "required_capabilities": ["tpu"]}), now_ms=1)
    assert _code(error) == "CAPABILITY_REQUIRED_MISSING"


# --- IDE: idempotency -------------------------------------------------------

def test_ide_01_same_operation_same_payload_returns_original_receipt():
    ledger = envelope.OperationReceipts()
    calls = []
    first = ledger.apply("op-1", {"a": 1}, lambda: calls.append(1) or {"ok": True})
    second = ledger.apply("op-1", {"a": 1}, lambda: calls.append(2) or {"ok": False})
    assert first == second == {"ok": True}
    assert calls == [1]


def test_ide_02_same_operation_different_payload_conflicts():
    ledger = envelope.OperationReceipts()
    ledger.apply("op-1", {"a": 1}, lambda: {"ok": True})
    with pytest.raises(ProtocolError) as error:
        ledger.apply("op-1", {"a": 2}, lambda: {"ok": True})
    assert _code(error) == "IDEMPOTENCY_CONFLICT"


def test_ide_03_repeated_register_and_assign_create_no_new_facts():
    model = _model(plan_revision=1)
    first = _register(model, "op-register")
    again = _register(model, "op-register")
    assert first == again and model.state == "REGISTERED"
    payload = {"assignment": _assignment()}
    assigned = model.handle(_message("op-assign", "Assign", payload), now_ms=1)
    repeated = model.handle(_message("op-assign", "Assign", payload), now_ms=1)
    assert assigned == repeated
    assert model.plan_revision == 2  # re-execution would hit the CAS and conflict


# --- EPO: epochs and fences -------------------------------------------------

def test_epo_01_stale_controller_epoch_fenced():
    model = _model()
    with pytest.raises(ProtocolError) as error:
        model.handle(_message("op-hb", "Heartbeat",
                              {"boot_id": "bo1", "observed_at_ms": 1}, epoch=2), now_ms=1)
    assert _code(error) == "FENCED"


def test_epo_02_stale_boot_and_grant_cannot_publish():
    model = _model()
    with pytest.raises(ProtocolError) as error:
        model.handle(_message("op-hb", "Heartbeat",
                              {"boot_id": "bo-old", "observed_at_ms": 1}), now_ms=1)
    assert _code(error) == "BOOT_FENCED"
    with pytest.raises(ProtocolError) as error:
        envelope.check_grant(_grant(controller_epoch=2), controller_epoch=3,
                             allocation_id="al1", boot_id="bo1", now_ms=1)
    assert _code(error) == "GRANT_REVOKED"
    with pytest.raises(ProtocolError) as error:
        envelope.check_grant(_grant(), controller_epoch=3,
                             allocation_id="al1", boot_id="bo1", now_ms=20000)
    assert _code(error) == "GRANT_REVOKED"


def test_epo_03_stale_plan_revision_assignment_conflicts():
    with pytest.raises(ProtocolError) as error:
        worker.apply_assignment(5, _assignment())
    assert _code(error) == "ASSIGNMENT_PLAN_CONFLICT"
    assert worker.apply_assignment(1, _assignment()) == 2


def test_epo_04_stale_route_epoch_data_rejected():
    with pytest.raises(ProtocolError) as error:
        exchange.check_route_epoch(3, 2)
    assert _code(error) == "FENCED"
    exchange.check_route_epoch(3, 3)


# --- LAT: late/duplicate events ---------------------------------------------

def _event(event_id, subject, revision, state):
    return {"event_id": event_id, "subject_id": subject,
            "observation_revision": revision, "state": state}


def test_lat_01_duplicate_events_dedup_by_id():
    ordering = envelope.EventOrdering()
    assert ordering.observe(_event("e1", "s1", 1, "allocating"))["status"] == "applied"
    assert ordering.observe(_event("e1", "s1", 1, "allocating"))["status"] == "duplicate"


def test_lat_02_stale_observation_never_regresses_terminal():
    ordering = envelope.EventOrdering()
    ordering.observe(_event("e1", "s1", 1, "active"))
    assert ordering.observe(_event("e2", "s1", 0, "allocating"))["status"] == "stale"
    ordering.observe(_event("e3", "s1", 2, "retired"))
    result = ordering.observe(_event("e4", "s1", 3, "ready"))
    assert result["status"] == "stale" and result["state"] == "retired"


def test_lat_03_cancel_tombstone_suppresses_late_allocation():
    ordering = envelope.EventOrdering()
    ordering.observe(_event("e1", "s1", 1, "cancelled"))
    result = ordering.observe(_event("e2", "s1", 2, "allocating"))
    assert result["status"] == "suppressed" and result["state"] == "cancelled"


def test_lat_04_heartbeat_loss_is_unknown_without_retry():
    ordering = envelope.EventOrdering()
    ordering.observe(_event("e1", "s1", 1, "active"))
    assert ordering.heartbeat_expired("s1") == {"state": "unknown", "retry": False}
    assert ordering.heartbeat_expired("never-seen") == {"state": "unknown", "retry": False}


# --- ART: immutable run artifacts --------------------------------------------

def test_art_01_sealed_manifest_is_immutable():
    sealed = artifact.seal_manifest(_manifest(), 123)
    with pytest.raises(ProtocolError) as error:
        artifact.seal_manifest(sealed, 456)
    assert _code(error) == "ARTIFACT_SEALED"
    with pytest.raises(ProtocolError) as error:
        artifact.amend_manifest(sealed, {"secret_refs": []})
    assert _code(error) == "ARTIFACT_SEALED"
    amended = artifact.amend_manifest(_manifest(), {"secret_refs": []})
    assert amended["sealed_at"] is None


def test_art_02_digest_mismatch_rejects():
    manifest = _manifest()
    ref = {"manifest_id": manifest["manifest_id"], "sha256": artifact.manifest_digest(manifest)}
    assert artifact.verify_manifest(manifest, ref)
    with pytest.raises(ProtocolError) as error:
        artifact.verify_manifest(manifest, dict(ref, sha256="9" * 64))
    assert _code(error) == "ARTIFACT_MISMATCH"
    tampered = dict(manifest, run_id="run-2")
    with pytest.raises(ProtocolError) as error:
        artifact.validate_manifest(tampered)
    assert _code(error) == "ARTIFACT_MISMATCH"


def test_art_03_entry_without_digest_binding_rejects():
    with pytest.raises(ProtocolError) as error:
        _manifest(business={"bundle_digest": "sha256:" + "b" * 64,
                            "entrypoints": [{"name": "main", "module_key": "p_" + "b" * 24,
                                             "definition_ref": {"location": "/mutable/repo"}}]})
    assert _code(error) == "ARTIFACT_MISMATCH"
    with pytest.raises(ProtocolError) as error:
        _manifest(business={"bundle_digest": "sha256:" + "b" * 64,
                            "entrypoints": [{"name": "main", "module_key": "p_" + "c" * 24,
                                             "definition_ref": {"sha256": "c" * 64}}]})
    assert _code(error) == "ARTIFACT_MISMATCH"


def test_art_04_secrets_are_references_only():
    manifest = _manifest(secret_refs=[{"name": "api-key", "external_ref": "vault://key"}])
    assert manifest["secret_refs"] == [{"name": "api-key", "external_ref": "vault://key"}]
    with pytest.raises(ProtocolError):
        _manifest(secret_refs=[{"name": "api-key", "external_ref": "vault://key",
                                "value": "inline-secret"}])


def test_art_manifest_persistence_roundtrip(tmp_path):
    manifest = artifact.seal_manifest(_manifest(), 123)
    ref = artifact.save_manifest(tmp_path, manifest)
    assert set(ref) == {"manifest_id", "path", "bytes", "sha256"}
    assert artifact.verify_manifest(manifest, ref)


# --- ID: stable logical identity ---------------------------------------------

def test_id_01_same_semantic_plan_compiles_to_same_dag_id():
    assert _linear_dag()["dag_id"] == _linear_dag()["dag_id"]
    different = _linear_dag(action_id="action-2")
    assert different["dag_id"] != _linear_dag()["dag_id"]


def test_id_02_worker_count_and_chunking_do_not_change_logical_identity():
    dag = _linear_dag()
    small = _physical(dag, partition_count=1)
    large = _physical(dag, partition_count=4, worker_groups=[
        {"group_id": "g1", "role": "cpu_prepare", "profile_ref": "prof",
         "stage_refs": [stage["stage_id"] for stage in small["stages"]],
         "min_workers": 1, "initial_workers": 4, "max_workers": 8}])
    assert small["plan_id"] != large["plan_id"]  # physical plans differ
    assert [stage["logical_node_id"] for stage in small["stages"]] == [
        stage["logical_node_id"] for stage in large["stages"]]
    assert [stage["occurrence_id"] for stage in small["stages"]] == [
        stage["occurrence_id"] for stage in large["stages"]]
    record = exchange.record_identity("src-1", 5)
    assert record == exchange.record_identity("src-1", 5)
    occurrence = dag["nodes"][1]["occurrence_id"]
    effect_key = identity([occurrence, record])
    assert effect_key == identity([occurrence, exchange.record_identity("src-1", 5)])
    spec_kwargs = dict(logical_node_id="n1", occurrence_id="o1",
                       artifact_manifest_ref={"manifest_id": "m" * 64, "sha256": "a" * 64},
                       module_key="p_" + "b" * 24)
    spec_a = operator.compile_operator_spec(CallableSpec.create(_double), **spec_kwargs)
    spec_b = operator.compile_operator_spec(CallableSpec.create(_double), **spec_kwargs)
    assert spec_a["spec_id"] == spec_b["spec_id"]


def test_id_03_expansion_and_filter_lineage():
    parent = exchange.record_identity("src-1", 5)
    child_a = exchange.record_identity("src-1", 5, exchange.expanded_lineage([], 0))
    child_b = exchange.record_identity("src-1", 5, exchange.expanded_lineage([], 1))
    assert len({parent, child_a, child_b}) == 3
    grandchild = exchange.record_identity("src-1", 5, exchange.expanded_lineage([0], 2))
    assert grandchild not in {parent, child_a, child_b}
    # filtering removes a record without altering any surviving identity
    assert exchange.record_identity("src-1", 6) != parent
    nested = exchange.record_identity("src-1", 5, [0, 2])
    assert nested == grandchild


def test_id_04_transient_locations_are_not_recoverable_identity():
    with pytest.raises(ProtocolError):
        exchange.build_data_ref(kind="ray_object", read_domain="ray-cluster-1",
                                location="ObjectRef(00ffffff)", payload_bytes=10,
                                sha256="a" * 64, durability="durable", scope_ref="run-1")
    ref = exchange.build_data_ref(kind="file", read_domain="host-a",
                                  location="/data/x.bin", payload_bytes=10,
                                  sha256="a" * 64, durability="durable", scope_ref="run-1")
    assert exchange.check_read_domain(ref, "host-a")
    with pytest.raises(ProtocolError) as error:
        exchange.check_read_domain(ref, "host-b")
    assert _code(error) == "REF_READ_DOMAIN_MISMATCH"
    same_path_other_host = exchange.build_data_ref(
        kind="file", read_domain="host-b", location="/data/x.bin", payload_bytes=10,
        sha256="a" * 64, durability="durable", scope_ref="run-1")
    assert same_path_other_host["ref_id"] != ref["ref_id"]


# --- ORD: channel sequence and barrier alignment ------------------------------

def test_ord_01_data_and_control_share_total_order():
    producer = exchange.ChannelSequence()
    cursor = exchange.ChannelCursor()
    data_seq = producer.issue()
    control_seq = producer.issue()
    assert control_seq == data_seq + 1
    cursor.accept(data_seq)
    cursor.accept(control_seq)
    with pytest.raises(ProtocolError) as error:
        cursor.accept(data_seq)
    assert _code(error) == "SEQUENCE_ORDER_VIOLATION"


def test_ord_02_barrier_never_overtakes_pre_barrier_data():
    gate = exchange.InputGate(["c1"])
    gate.push("c1", {"kind": "data", "sequence": 0})
    gate.push("c1", _barrier("c1", 1))
    gate.push("c1", {"kind": "data", "sequence": 2})  # post-barrier, held
    with pytest.raises(ProtocolError) as error:
        gate.barrier_effect("c1")
    assert _code(error) == "BARRIER_OVERTAKE"
    assert gate.consume("c1")["sequence"] == 0
    assert gate.barrier_effect("c1")["checkpoint_id"] == "ck1"
    assert gate.consume("c1") is None  # post-barrier data stays held
    assert gate.release("ck1") == 1
    assert gate.consume("c1")["sequence"] == 2


def test_ord_03_multi_input_alignment_holds_post_barrier_data():
    gate = exchange.InputGate(["c1", "c2"])
    gate.push("c1", {"kind": "data", "sequence": 0})
    gate.push("c1", _barrier("c1", 1))
    gate.push("c1", {"kind": "data", "sequence": 2})  # post-barrier, must be held
    gate.push("c2", {"kind": "data", "sequence": 0})  # other channel keeps progressing
    assert gate.consume("c2")["sequence"] == 0
    assert not gate.aligned("ck1")
    with pytest.raises(ProtocolError) as error:
        gate.release("ck1")
    assert _code(error) == "BARRIER_OVERTAKE"
    assert gate.consume("c1")["sequence"] == 0
    gate.push("c2", _barrier("c2", 1))
    assert gate.aligned("ck1")
    assert gate.release("ck1") == 1
    assert gate.consume("c1")["sequence"] == 2


def test_ord_04_empty_and_finished_channels_participate_in_cuts():
    gate = exchange.InputGate(["c1", "empty"])
    gate.push("empty", {"kind": "eoi", "channel_id": "empty", "sequence": 0,
                        "producer_partition_id": "p0", "final": True})
    gate.push("c1", _barrier("c1", 0))
    assert not gate.aligned("ck1")  # finished channel still owes its barrier
    gate.push("empty", _barrier("empty", 1, cut=0))
    assert gate.aligned("ck1")
    with pytest.raises(ProtocolError):
        gate.push("outsider", {"kind": "data", "sequence": 0})


def test_ord_05_failure_cancel_idle_are_not_seal_zero_rows_may_seal():
    assert exchange.seal_allowed(["success", "success"])
    assert exchange.seal_allowed([])  # zero producers/rows may seal
    for status in ("failed", "cancelled", "idle"):
        with pytest.raises(ProtocolError):
            exchange.seal_allowed(["success", status])
    seal = {"channel_id": "c1", "sequence": 3,
            "seal_scope": {"scope_id": "scope-1", "membership": ["p0"], "generation": 1},
            "manifest_ref": {"sha256": "a" * 64}, "status": "success"}
    assert exchange.validate_seal(seal)
    with pytest.raises(ProtocolError):
        exchange.validate_seal(dict(seal, status="failed"))


# --- CKP: checkpoint coverage and legacy compatibility ------------------------

def test_ckp_01_participant_coverage_is_complete():
    manifest = _checkpoint_manifest()
    assert checkpoint.validate_checkpoint_manifest(manifest)
    missing_source = dict(manifest, source_progress=[])
    with pytest.raises(ProtocolError) as error:
        checkpoint.validate_checkpoint_manifest(missing_source)
    assert _code(error) == "CHECKPOINT_PARTICIPANT_MISSING"
    missing_channel = dict(manifest, channel_cuts=[])
    with pytest.raises(ProtocolError) as error:
        checkpoint.validate_checkpoint_manifest(missing_channel)
    assert _code(error) == "CHECKPOINT_PARTICIPANT_MISSING"
    wrong_topology = {"sources": [{"source_id": "src-2", "partition_id": "p0"}],
                      "channels": ["ch1"]}
    with pytest.raises(ProtocolError) as error:
        checkpoint.validate_checkpoint_manifest(manifest, topology=wrong_topology)
    assert _code(error) == "CHECKPOINT_PARTICIPANT_MISSING"


def test_ckp_02_single_ordinal_cannot_cover_multi_source_dag():
    with pytest.raises(ProtocolError) as error:
        checkpoint.legacy_checkpoint_fragment(
            {"source_id": "src-1", "next_ordinal": 7},
            source_ids=["src-1", "src-2"])
    assert _code(error) == "CHECKPOINT_SCHEMA_INCOMPATIBLE"
    with pytest.raises(ProtocolError) as error:
        checkpoint.legacy_checkpoint_fragment(
            {"source_id": "src-1", "next_ordinal": 7},
            source_ids=["src-1"], channel_ids=["ch1"])
    assert _code(error) == "CHECKPOINT_SCHEMA_INCOMPATIBLE"
    with pytest.raises(ProtocolError) as error:
        checkpoint.legacy_checkpoint_fragment(
            {"source_id": "src-1", "next_ordinal": 7},
            source_ids=["src-1"], partition_count=2)
    assert _code(error) == "CHECKPOINT_SCHEMA_INCOMPATIBLE"


def test_ckp_03_legacy_single_source_reads_as_degenerate_vector():
    fragment = checkpoint.legacy_checkpoint_fragment(
        {"source_id": "src-1", "next_ordinal": 7}, source_ids=["src-1"])
    assert fragment["participants"]["sources"] == [{"source_id": "src-1", "partition_id": "p0"}]
    assert fragment["participants"]["channels"] == []
    assert fragment["source_progress"] == [
        {"source_id": "src-1", "partition_id": "p0", "position": {"next_ordinal": 7}}]
    assert fragment["channel_cuts"] == []


def test_ckp_04_unknown_schema_major_rejects_explicitly():
    with pytest.raises(ProtocolError) as error:
        checkpoint.check_schema_version("demiflow.checkpoint_manifest.v2")
    assert _code(error) == "CHECKPOINT_SCHEMA_INCOMPATIBLE"
    with pytest.raises(ProtocolError) as error:
        checkpoint.validate_checkpoint_manifest(
            _checkpoint_manifest(schema_version="demiflow.checkpoint_manifest.v9"))
    assert _code(error) == "CHECKPOINT_SCHEMA_INCOMPATIBLE"
    with pytest.raises(ProtocolError) as error:
        checkpoint.check_schema_version("some-other-format")
    assert _code(error) == "CHECKPOINT_SCHEMA_INCOMPATIBLE"


def test_ckp_05_historical_runs_stay_on_legacy_path():
    assert checkpoint.recovery_route({}) == "legacy"
    assert checkpoint.recovery_route({"artifact_manifest_ref": None}) == "legacy"
    assert checkpoint.recovery_route(
        {"artifact_manifest_ref": {"manifest_id": "m" * 64, "sha256": "a" * 64}}) == "protocol"


def test_ckp_06_unknown_paid_effects_are_never_auto_replayed():
    manifest = _checkpoint_manifest()
    assert checkpoint.replayable_effects(manifest) == ["eff-1"]
    unknown_only = dict(manifest, inflight=[{"request_key": "req-2", "status": "unknown"}])
    assert checkpoint.replayable_effects(unknown_only) == []


# --- OP: operator compilation preserves fn/actor ------------------------------

def test_op_01_fn_and_actor_compile_from_existing_callable_spec():
    ref = {"manifest_id": "m" * 64, "sha256": "a" * 64}
    kwargs = dict(artifact_manifest_ref=ref, module_key="p_" + "b" * 24)
    fn_spec = operator.compile_operator_spec(
        CallableSpec.create(_double), logical_node_id="n1", occurrence_id="o1", **kwargs)
    assert fn_spec["lifecycle"] == {"kind": "fn", "astart": False, "astop": False, "aclose": False}
    actor_spec = operator.compile_operator_spec(
        CallableSpec.create(_Counter, fn_constructor_kwargs={"start": 3}),
        logical_node_id="n2", occurrence_id="o2",
        traits={"state_ownership": "partitionable"}, **kwargs)
    assert actor_spec["lifecycle"]["kind"] == "actor"
    assert actor_spec["state"] == {"ownership": "partitionable", "adapter_present": True}
    assert actor_spec["effect"]["request_key_derivation"] == "occurrence_lineage"
    # existing CallableSpec semantics untouched
    instance = CallableSpec.create(_Counter, fn_constructor_kwargs={"start": 3}).instantiate()
    assert instance({"x": 1}) == {"x": 1} and instance.value == 4


def test_op_02_pickle_only_closures_do_not_enter_durable_protocol():
    ref = {"manifest_id": "m" * 64, "sha256": "a" * 64}
    kwargs = dict(logical_node_id="n1", occurrence_id="o1",
                  artifact_manifest_ref=ref, module_key="p_" + "b" * 24)
    closure_fn = lambda row: row  # noqa: E731 - intentionally unpicklable-by-path
    with pytest.raises(ProtocolError):
        operator.compile_operator_spec(CallableSpec.create(closure_fn), **kwargs)
    with pytest.raises(ProtocolError):
        operator.compile_operator_spec(
            CallableSpec.create(_double, fn_args=({"opaque object"},)), **kwargs)


def test_op_03_missing_state_adapter_fails_precheck():
    ref = {"manifest_id": "m" * 64, "sha256": "a" * 64}
    with pytest.raises(ProtocolError) as error:
        operator.compile_operator_spec(
            CallableSpec.create(_StatefulNoAdapter), logical_node_id="n1", occurrence_id="o1",
            artifact_manifest_ref=ref, module_key="p_" + "b" * 24,
            traits={"state_ownership": "shared_colocated"})
    assert _code(error) == "STATE_ADAPTER_MISSING"


def test_op_04_traits_defaults_keep_legacy_construction():
    legacy = OperatorTraits("transform", "ephemeral", "transient", "ephemeral_worker")
    assert legacy == operation_traits(ops.MapOp(callable=CallableSpec.create(_double)))
    assert legacy.input_mode == "incremental" and legacy.cardinality == "one"
    assert legacy.ordering == "unordered" and legacy.state_ownership == "none"
    assert legacy.effect_kind == "pure" and legacy.batching == "record"
    assert unified_operation_traits(ops.SortOp(keys=("x",))).input_mode == "finite_scope"
    assert unified_operation_traits(ops.FlatMapOp(callable=CallableSpec.create(_double))).cardinality == "many"
    assert select_exchange({"input_mode": "finite_scope"})[0] == "finite_sealed"
    assert select_exchange({"input_mode": "incremental"})[0] == "memory_incremental"
    assert select_exchange({"input_mode": "incremental"}, durability_declared="durable")[0] == "durable_incremental"


# --- DAG: declared-flow compatibility and shared ancestors --------------------

def test_dag_declared_flow_shared_ancestor_executes_once():
    nodes = [{"id": "src", "kind": "from_items", "role": "source"},
             {"id": "shared", "kind": "materialize", "role": "bridge"},
             {"id": "left", "kind": "map", "role": "transform"},
             {"id": "right", "kind": "map", "role": "transform"},
             {"id": "act", "kind": "count", "role": "action"}]
    edges = [{"from": "src", "to": "shared", "port": 0},
             {"from": "shared", "to": "left", "port": 0},
             {"from": "shared", "to": "right", "port": 0},
             {"from": "left", "to": "act", "port": 0},
             {"from": "right", "to": "act", "port": 1}]
    descriptors = {node["id"]: {"kind": node["kind"], "declaration": node["id"]}
                   for node in nodes}
    dag = compile_declared_dag(action_id="act", nodes=nodes, edges=edges,
                               descriptors=descriptors)
    assert dag["shared_ancestors"]
    shared_id = dag["shared_ancestors"][0]["logical_node_id"]
    executions = [node for node in dag["nodes"] if node["logical_node_id"] == shared_id]
    assert executions  # present once per occurrence; reuse never re-expands children
    again = compile_declared_dag(action_id="act", nodes=nodes, edges=edges,
                                 descriptors=descriptors)
    assert dag["dag_id"] == again["dag_id"]