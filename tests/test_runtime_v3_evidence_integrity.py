"""Evidence rules, not claims that the corresponding business scenarios passed."""
from types import SimpleNamespace

import pytest
import runtime_v3_support as support


@pytest.mark.parametrize("scenario", [
    "api_matrix",
    "cpu_gpu_isolation", "fairness_deadlock", "multiworker_barrier", "route_recovery",
    "actor_state_lifecycle",
    "queue_full_business", "memory_peak", "disk_gc", "two_hosts",
    "node_loss", "driver_restart", "history_identity", "workbench_real_plan",
    "product_regression", "artifact_security", "performance", "all_regressions",
])
def test_placeholder_scenarios_cannot_produce_passing_evidence(scenario):
    recorded = []
    fixture = SimpleNamespace(_record_not_run=lambda name, detail: recorded.append((name, detail)))
    with pytest.raises(support.NotRunError):
        support._SCENARIO_FUNCS[scenario](fixture, None)
    assert recorded[0][0] == scenario
    assert recorded[0][1]["missing_semantic_harness"]
