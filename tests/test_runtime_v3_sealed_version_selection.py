"""Two actual immutable seal formats remain independently readable and checked."""
import sqlite3
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_api import make_action_request
from demiflow.execution.dataflow_services import make_driver_services
from demiflow.execution.native_plan import compile_native_topology
from demiflow.execution.plan_interpreter import PlanContractError
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.run_context import RunContext
from demiflow.execution.sealed_plans import load_sealed_plan
from demiflow.planning.dataflow_policy import resolve_runtime_policy


@pytest.fixture
def dual_seal(tmp_path):
    root = tmp_path / 'run'
    with RunContext.start(root) as run, data.local_execution():
        ds = data.from_items([{'id': 1}]).map(lambda row: row)
        with run.action(plan_identity('materialize', ds._source, ds._plan)) as action:
            compile_native_topology(action, ds._source, ds._plan,
                decision=SimpleNamespace(action_kind='materialize', backend='datafusion',
                    recovery='final_commit', route='native_relational'))
            request = make_action_request(ds._executor, ds._source, ds._plan,
                kind='materialize', arguments={})
            policy, bounds = resolve_runtime_policy({}, run.limits, provider='local')
            services = make_driver_services(action, policy=policy, bounds=bounds)
            execution = DataflowExecution(request, services=services)
            try:
                execution.prepare()
            finally:
                execution.close()
            # A preparation-only fixture; no worker or business result is claimed.
            action.complete({'kind': 'test-preparation-only'})
    return root, 'action-1'


def test_dual_seals_select_exact_original_bundle(dual_seal):
    root, action = dual_seal
    old = load_sealed_plan(root, action, version='v2')
    new = load_sealed_plan(root, action, version='v3')
    assert set(old) == {'facts', 'logical_dag', 'physical_plan'}
    assert new['version'] == 'v3'
    assert old['facts']['action_id'] == new['facts']['action_id'] == action
    assert load_sealed_plan(root, action) == new


@pytest.mark.parametrize('version', ['v2', 'v3'])
def test_corrupt_selected_carrier_cannot_fall_back(dual_seal, version):
    root, action = dual_seal
    chosen = load_sealed_plan(root, action, version=version)
    other = 'v2' if version == 'v3' else 'v3'
    expected_other = load_sealed_plan(root, action, version=other)
    ref = (chosen['facts']['logical_dag_ref'] if version == 'v2'
           else chosen['facts']['physical_ref'])
    path = (root / ref['path'] if version == 'v2' else
            root / '.dataflow-store' / ref['store_id'] / 'objects' / ref['digest'])
    original = path.read_bytes()
    path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
    try:
        with pytest.raises((PlanContractError, DataflowError)):
            load_sealed_plan(root, action, version=version)
        assert load_sealed_plan(root, action, version=other) == expected_other
    finally:
        path.write_bytes(original)


@pytest.mark.parametrize('version,key', [('v2', '-sealed-plan'), ('v3', '-dataflow-sealed')])
def test_explicit_missing_version_cannot_fall_back(dual_seal, version, key):
    root, action = dual_seal
    with sqlite3.connect(root / 'run.sqlite') as db:
        db.execute('DELETE FROM runtime_sources WHERE action_id=?', (action + key,))
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(root, action, version=version)
    assert exc.value.reason == 'missing_plan'


@pytest.mark.parametrize('version', [0, True, '', 'v1', 'latest', []])
def test_invalid_version_is_rejected_without_reading(tmp_path, version):
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(tmp_path / 'absent', 'action-1', version=version)
    assert exc.value.reason == 'invalid_contract'
