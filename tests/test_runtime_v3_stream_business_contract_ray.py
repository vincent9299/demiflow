"""Original public stream assertions on actual Ray Core, with no Local fallback."""
import json
import os
from pathlib import Path
import tempfile

import pytest

import test_runtime_v3_stream_business_contract as original_cases


@pytest.fixture(scope='module')
def stream_owned_ray():
    import ray
    from demiflow.data.api import DataAPI
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from runtime_v3_regression_inventory import assert_private
    from runtime_v3_support import _RayDataBlocker

    assert_private()
    assert Path(ray.__file__).is_file() and not ray.is_initialized()
    assert Path(os.environ['DEMIFLOW_LOCATION_CACHE']).is_relative_to('/tmp')
    with _RayDataBlocker() as blocked:
        ray.init(address='local', num_cpus=2, num_gpus=0, include_dashboard=False,
                 object_store_memory=100*1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-stream-ray-', dir='/tmp'))
        executor = RayDatasetExecutor(workers=2)
        try:
            yield DataAPI(executor=executor)
        finally:
            try:
                executor.close()
            finally:
                ray.shutdown()
        assert not blocked.attempts and not ray.is_initialized()


@pytest.fixture
def actual_ray_binding(stream_owned_ray, monkeypatch, tmp_path):
    from demiflow.execution.dataflow import DataflowExecution

    activations = []
    original = DataflowExecution._prepare_launch

    def prepare(execution, lease, stage):
        assert execution._policy.provider == 'ray_core'
        assert execution._physical.policy.provider == 'ray_core'
        activations.append({'provider': execution._policy.provider,
                            'plan_id': execution._physical.plan_id,
                            'stage_id': stage.stage_id,
                            'allocation_id': lease.allocation_id})
        return original(execution, lease, stage)

    monkeypatch.setattr(DataflowExecution, '_prepare_launch', prepare)
    monkeypatch.setattr(original_cases, 'DataAPI', lambda: stream_owned_ray)
    yield
    assert activations, 'the public API never activated an actual Ray worker'
    (tmp_path/'real-ray-activations.json').write_text(json.dumps(activations, indent=2))


@pytest.mark.parametrize('batch', [False, True])
def test_local_exception_whitelist_preserves_qualified_identity_on_ray(actual_ray_binding, batch):
    original_cases.test_local_exception_whitelist_preserves_qualified_identity(batch)


def test_identically_named_exception_outside_whitelist_still_fails_on_ray(actual_ray_binding):
    original_cases.test_identically_named_exception_outside_whitelist_still_fails()


def test_stream_progress_comes_from_intermediate_business_frontiers_on_ray(actual_ray_binding):
    original_cases.test_stream_progress_comes_from_intermediate_business_frontiers()
