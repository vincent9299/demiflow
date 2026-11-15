"""Lane V integration acceptance scenarios (V006-V035).

Each test drives a *real* public Dataset/Pipeline/workbench path through
RuntimeV3Fixture.run and asserts the unified-engine evidence: the real
``execute_action`` facade was invoked, ``ray.data`` was never imported, and
every loaded demiflow module comes from this worktree. This catches "fake
unification" (a green ``_new_graph`` unit test while the default Pipeline still
takes the old path).

Before the owning lanes deliver, ``run`` raises NotRunError (recorded not_run
with the missing environment/lane). That is an honest failing node classified to
the gate's not_run status — never a skip counted as pass. Once a module lands
the scenario really runs and really fails if the default path is not unified.
"""
from __future__ import annotations

import pytest

import runtime_v3_support as s

# Scenarios whose real work is not an in-process Dataset action (identity
# formula, repository collection, subprocess re-verification, process crash).
_NON_PIPELINE = {"history_identity", "all_regressions", "cache_materialize"}


def _run_unified(fx, scenario, *, fault=None):
    evidence = fx.run(scenario, fault=fault)
    assert evidence["ray_data_import_attempts"] == [], f"{scenario}: ray.data must never be imported"
    assert all(p.startswith(str(s.WORKTREE)) for p in evidence["module_paths"].values()), (
        f"{scenario}: a demiflow module loaded outside the worktree"
    )
    if scenario not in _NON_PIPELINE:
        assert evidence["execute_action_calls"], (
            f"{scenario}: default path did not invoke the unified execute_action facade (fake unification)"
        )
    return evidence


def test_no_ray_data(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "no_ray_data")


def test_api_matrix(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "api_matrix")
    assert evidence["api_count"] == 98
    assert len(evidence["retired_rejected"]) == 4


def test_shared_diamond(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "shared_diamond")
    assert evidence["shared_calls"] == 3 and evidence["independent_calls"] == 2


def test_incremental_first_row(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "incremental_first_row")
    assert evidence["first_row_before_eoi"] is True


def test_three_exchange_modes(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "three_exchange_modes")
    assert evidence["modes"] == {m: 6 for m in ("memory_incremental", "durable_incremental", "finite_sealed")}


def test_cpu_gpu_isolation(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "cpu_gpu_isolation")


def test_fairness_deadlock(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "fairness_deadlock")


def test_relational_semantics(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "relational_semantics")
    assert evidence["inner"] == 4 and evidence["semi"] == 2 and evidence["anti"] == 2 and evidence["left"] == 6


def test_sort_shuffle_aggregate(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "sort_shuffle_aggregate")
    assert evidence["shuffle_stable"] is True


def test_source_snapshots(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "source_snapshots")
    assert evidence["pinned_version"] == 1 and evidence["head"] == 2


def test_multiworker_barrier(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "multiworker_barrier")
    assert evidence["joined"] == 9


def test_route_recovery(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "route_recovery")


def test_commit_crash_matrix(runtime_v3_fixture):
    boundaries = (
        "before_intent", "after_intent", "after_prepare_before_record",
        "after_prepared_before_publish", "after_publish_before_receipt", "after_receipt_before_ack",
    )
    for boundary in boundaries:
        evidence = _run_unified(runtime_v3_fixture, "commit_crash_matrix", fault=boundary)
        assert evidence["crash"]["killed"] is True
        assert evidence["crash"]["returncode"] == -9


def test_model_effect_recovery(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "model_effect_recovery")
    assert evidence["transport"] == "local_fixture"


def test_actor_state_lifecycle(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "actor_state_lifecycle")
    assert evidence["final_total"] == sum(range(5))


def test_cache_materialize(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "cache_materialize")
    outcomes = evidence["historical_defects"]["outcomes"]
    assert len(outcomes) == 7
    assert all(status == "PASSED" for status in outcomes.values()), (
        "the 7 historical cache/async-materialize defects must be fixed to pass, not weakened"
    )


def test_queue_full_business(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "queue_full_business")


def test_memory_peak(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "memory_peak")
    assert evidence["rows"] == 16


def test_disk_gc(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "disk_gc")
    assert evidence["released"] is True


def test_remote_journal(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "remote_journal")
    assert evidence["worker_sqlite"] == "forbidden"


def test_two_hosts(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "two_hosts")
    assert len(evidence["hosts"]) >= 2 and len(evidence["nodes"]) >= 2


def test_node_loss(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "node_loss")


def test_driver_restart(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "driver_restart")


def test_history_identity(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "history_identity")
    assert len(evidence["enum_identities"]) == 3


def test_workbench_real_plan(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "workbench_real_plan")


def test_product_regression(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "product_regression")


def test_bounded_terminal(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "bounded_terminal")
    assert evidence["take0"] == 0 and evidence["take1"] == 1 and evidence["take_all"] == 10 and evidence["limit5"] == "overflow_rejected"


def test_artifact_security(runtime_v3_fixture):
    _run_unified(runtime_v3_fixture, "artifact_security")


def test_performance(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "performance")
    assert len(evidence["measurements"]) == 5 and evidence["warmup_separate"] is True


def test_all_regressions(runtime_v3_fixture):
    evidence = _run_unified(runtime_v3_fixture, "all_regressions")
    collection = evidence["collection"]
    assert collection["exit_code"] == 0
    assert collection["nodeid_count"] >= collection["baseline_file_count"]
    assert len(evidence["dispositions"]) == 44
