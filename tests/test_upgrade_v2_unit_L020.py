"""Historical K3 assignments distinguish worker groups from driver-only placements."""
from dataclasses import replace

import pytest

from demiflow.execution.contracts.errors import PlanContractError
from demiflow.execution.seal.plan_interpreter import interpret_physical_plan, validate_worker_assignment
from demiflow.execution.seal.sealed_plans import load_execution_bindings, load_sealed_plan
from test_execution_layout import assignment_for, expected_channel_ids
from test_sealed_execution_bindings import sealed_chain, workdir
from test_runtime_v3_sealed_version_selection import dual_seal


def archived_layout(root, action_id):
    sealed = load_sealed_plan(root, action_id, version='v2')
    return interpret_physical_plan(sealed['logical_dag'], sealed['physical_plan'],
        bindings=load_execution_bindings(root, action_id, sealed_plan=sealed),
        manifest_ref=sealed['facts']['artifact_manifest_ref'])


def test_historical_worker_grant_matches_exact_group_and_stage_limits(sealed_chain):
    layout = archived_layout(sealed_chain.root, sealed_chain.action_id)
    worker = layout.workers[0]
    inputs, outputs = expected_channel_ids(layout, worker)
    assignment = assignment_for(layout, worker, sorted(inputs), sorted(outputs))
    actual = validate_worker_assignment(layout, assignment, group_id=worker.group_id)
    assert actual == worker
    assert layout.checkpoint.checkpoint_rows == sealed_chain.fx.doc['checkpoint_rows']
    budgets = {s['stage_id']: s['budgets'] for s in layout.stages}
    assert actual.operator_bindings
    assert all(budgets[b['stage_id']]['inflight_rows'] > 0 for b in actual.operator_bindings)
    with pytest.raises(PlanContractError, match='unknown worker group'):
        validate_worker_assignment(layout, assignment, group_id='missing')


def test_driver_cannot_impersonate_published_worker_even_on_matching_id(sealed_chain, dual_seal):
    workers = archived_layout(sealed_chain.root, sealed_chain.action_id)
    driver = archived_layout(*dual_seal).executions[0]
    worker = workers.workers[0]
    colliding = replace(workers, executions=(replace(driver, group_id=worker.group_id),))
    inputs, outputs = expected_channel_ids(workers, worker)
    assignment = assignment_for(workers, worker, sorted(inputs), sorted(outputs))
    with pytest.raises(PlanContractError, match='driver execution'):
        validate_worker_assignment(colliding, assignment, group_id=worker.group_id)
