"""Published v2 and v3 writer artifacts stay independently readable and checked."""
import os
import sqlite3
import subprocess
import sys

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.plan_interpreter import PlanContractError
from demiflow.execution.sealed_plans import load_sealed_plan


# Final published main before legacy writer retirement. Both original bindings are
# written in the same run. The current package is imported only by the reader.
WRITE_DUAL_SEAL = """
import sys
from pathlib import Path
import demiflow
from types import SimpleNamespace

from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.dataflow_api import make_action_request
from demiflow.execution.dataflow_services import make_driver_services
from demiflow.execution.native_plan import compile_native_topology
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.run_context import RunContext
from demiflow.planning.dataflow_policy import resolve_runtime_policy

assert Path(demiflow.__file__).resolve().parent.parent == Path(sys.argv[1])
root = Path(sys.argv[2])
shape, kind, also_v3 = sys.argv[3:]
with RunContext.start(root) as run, data.local_execution():
    ds = data.range(3).map(lambda row: row)
    if shape == 'union':
        ds = ds.union(data.range(4).map(lambda row: row))
    if shape == 'join':
        ds = ds.join(data.range(4).map(lambda row: row), on='id')
    with run.action(plan_identity(kind, ds._source, ds._plan)) as action:
        compile_native_topology(action, ds._source, ds._plan,
            decision=SimpleNamespace(action_kind=kind, backend='datafusion',
                recovery='final_commit', route='native_relational'))
        if also_v3 == 'yes':
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
"""


def write_published_native_seal(checkout, root, *, shape='root', kind='materialize', also_v3=False):
    result = subprocess.run(
        [sys.executable, '-B', '-c', WRITE_DUAL_SEAL, str(checkout), str(root), shape, kind,
         'yes' if also_v3 else 'no'],
        cwd=root.parent, env=dict(os.environ, PYTHONPATH=str(checkout)),
        capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    return root, 'action-1'


@pytest.fixture
def dual_seal(tmp_path, legacy_seal_writer):
    return write_published_native_seal(legacy_seal_writer, tmp_path / 'run', also_v3=True)


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
