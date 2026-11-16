"""The unchanged pack/legacy recovery assertions execute on real Ray Core."""
from contextlib import nullcontext
import tempfile

import pytest


@pytest.mark.parametrize('old_carrier', [False, True])
def test_real_ray_pack_and_old_image_recovery(tmp_path, monkeypatch, old_carrier):
    ray = pytest.importorskip('ray')
    from demiflow import data
    from demiflow.data.api import _use_executor
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from runtime_v3_support import _RayDataBlocker
    import test_runtime_v3_kernel_pack as proof

    assert not ray.is_initialized(), 'this test only owns its fresh Ray runtime'
    prepare, providers = DataflowExecution.prepare, []

    def prepared(execution):
        result = prepare(execution)
        providers.append(execution._physical.policy.provider)
        return result

    monkeypatch.setattr(DataflowExecution, 'prepare', prepared)
    # Reuse every original output, original-ACK-prefix, old-carrier and new-cut
    # assertion; only its executor context is supplied by this real provider.
    monkeypatch.setattr(data, 'local_execution', lambda **options: nullcontext())
    with tempfile.TemporaryDirectory(prefix='demiflow-pack-ray-', dir='/tmp') as directory:
        ray.init(address='local', num_cpus=4, num_gpus=0, include_dashboard=False,
                 object_store_memory=100 * 1024**2, _temp_dir=directory)
        executor = RayDatasetExecutor(workers=2)
        try:
            with _RayDataBlocker() as blocker, _use_executor(executor):
                proof.test_real_dataset_restores_committed_pack_or_old_images_then_continues_packing(
                    tmp_path, monkeypatch, old_carrier)
                assert len(providers) == 2 and set(providers) == {'ray_core'}
                assert not blocker.attempts
        finally:
            executor.close()
            ray.shutdown()
        assert not ray.is_initialized()
