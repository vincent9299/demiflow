"""Actual Ray Core uses the same native adapter seal, cast, limits and ACKs."""
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest
import pyarrow as pa

from demiflow.data.api import DataAPI
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.executors.ray import RayDatasetExecutor
from demiflow.execution.run_context import RunContext
from test_runtime_v3_native_execution_binding import source, MiB


def test_actual_ray_public_arrow_delivery_preserves_budget_schema_and_completed_result(tmp_path, monkeypatch):
    ray = pytest.importorskip('ray')
    assert not ray.is_initialized()
    physicals, charges = [], []
    original = DataflowExecution.prepare
    def observe(execution):
        result = original(execution)
        physicals.append(execution._physical)
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', observe)
    from demiflow.execution.resource_state import RunResourceState
    charged = RunResourceState._charged
    def charge(state, db, scope):
        result = charged(state, db, scope)
        charges.append(dict(result))
        assert result['cpu_millis'] <= 2000
        assert result['memory_bytes'] <= 32*MiB
        return result
    monkeypatch.setattr(RunResourceState, '_charged', charge)
    class Interrupted(Exception):
        pass
    uri = source(tmp_path)
    schema = pa.schema([('id', pa.int64())])
    executor = RayDatasetExecutor(workers=2, native_resources={'batch_rows': 2})
    api = DataAPI(executor=executor)
    def consume():
        ds = api.read_lance(uri, version=1).select_columns(['id'])
        return list(ds.iter_batches(batch_size=2, batch_format='pyarrow'))
    try:
        ray.init(address='local', num_cpus=4, num_gpus=0, include_dashboard=False,
            object_store_memory=128*MiB,
            _temp_dir=tempfile.mkdtemp(prefix='demiflow-native-adapter-ray-', dir='/tmp'))
        with pytest.raises(Interrupted):
            with RunContext.start(tmp_path/'run', limits={'memory_bytes':32*MiB,
                    'max_row_bytes':8192, 'max_batch_bytes':16384, 'timeout_s':90}) as run:
                ref = run.ref
                expected = consume()
                commits = run.journal.execution_commits('action-1')
                assert commits and all(c['status'] == 'acked' for c in commits)
                assert run.position == 1 and run.journal.execution_action('action-2') is None
                raise Interrupted()
        launches = []
        monkeypatch.setattr(DataflowExecution, '_prepare_launch', lambda *args: launches.append(args))
        with RunContext.resume(ref) as run:
            actual = consume()
            assert run.journal.execution_commits('action-1') == commits
            assert run.position == 1
        assert not launches
        assert pa.concat_tables(actual).equals(pa.concat_tables(expected), check_metadata=True)
        assert sum(map(len, actual)) == 8
        assert all(isinstance(batch, pa.Table) and len(batch) <= 2
                   and batch.schema.equals(schema, check_metadata=True) for batch in actual)
        assert physicals and all(p.policy.provider == 'ray_core' for p in physicals)
        assert all(p.bounds.max_resident_bytes <= 32*MiB and p.policy.native_limits['batch_rows'] == 2 for p in physicals)
        assert len({p.plan_id for p in physicals}) == 1
        assert charges and any(c['cpu_millis'] for c in charges)
        with sqlite3.connect(str(Path(ref.directory)/'run.sqlite')) as db:
            allocations = db.execute('SELECT cleanup FROM resource_allocations').fetchall()
        assert allocations and all(status == 'CONFIRMED' for status, in allocations)
        assert not any(name == 'ray.data' or name.startswith('ray.data.') for name in sys.modules)
    finally:
        executor.close()
        ray.shutdown()
