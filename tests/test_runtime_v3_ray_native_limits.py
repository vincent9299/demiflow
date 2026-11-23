"""Public Ray executor limits feed the same sealed policy and native kernels."""
import tempfile

import pytest

from demiflow.data.api import DataAPI
from demiflow.execution.executors.ray import RayDatasetExecutor
from demiflow.execution.run_context import RunContext


@pytest.mark.parametrize('workers', [True, 0, -1, 1.5])
def test_ray_worker_limit_rejects_invalid_declaration(workers):
    with pytest.raises(ValueError, match='workers'):
        RayDatasetExecutor(workers=workers)


def test_ray_native_limit_declaration_is_closed_and_copied():
    with pytest.raises(ValueError, match='native_resources'):
        RayDatasetExecutor(native_resources={'unlimited': True})
    declared = {'threads': 2, 'partitions': 2}
    executor = RayDatasetExecutor(workers=2, native_resources=declared)
    declared['threads'] = 999
    assert dict(executor._native_resources) == {'threads': 2, 'partitions': 2}
    executor.close()


@pytest.mark.parametrize('partitions', [1, 2])
def test_real_ray_join_seals_public_native_limits(tmp_path, monkeypatch, partitions):
    ray = pytest.importorskip('ray')
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    assert not ray.is_initialized()
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path/'capacity'))
    native = dict(threads=2, partitions=partitions, memory_bytes=64*1024**2,
                  max_rss_bytes=2*1024**3, max_scratch_bytes=32*1024**2,
                  batch_rows=2, timeout_s=60)
    executor = RayDatasetExecutor(workers=2, native_resources=native)
    api = DataAPI(executor=executor)
    try:
        ray.init(address='local', num_cpus=4, num_gpus=0, include_dashboard=False,
                 object_store_memory=128*1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-native-limits-', dir='/tmp'))
        with RunContext.start(tmp_path/'run', limits={'memory_bytes':128*1024**2,
                'max_row_bytes':65536, 'max_batch_bytes':262144,
                'max_scratch_bytes':64*1024**2, 'timeout_s':90}) as run:
            left = api.from_items([{'k': i, 'a': str(i)} for i in range(8)])
            right = api.from_items([{'k': i, 'b': i*2} for i in range(8)])
            rows = left.join(right, on='k').take_all()
            assert sorted(rows, key=lambda r:r['k']) == [
                {'k':i, 'a':str(i), 'b':i*2} for i in range(8)]
            physical = load_sealed_plan(run.ref.directory, 'action-1')['physical']
            assert physical.policy.provider == 'ray_core'
            assert physical.policy.max_active_workers == 2
            assert dict(physical.policy.native_limits) == native
            assert physical.bounds.max_resident_bytes == native['memory_bytes']
            assert physical.bounds.max_spill_bytes == native['max_scratch_bytes']
            assert physical.bounds.timeout_ms <= 60000
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    finally:
        executor.close()
        ray.shutdown()
