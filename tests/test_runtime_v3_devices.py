"""Managed process device grants without loading a CUDA runtime or real GPU."""
import json
import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext


def inventory(tmp_path, monkeypatch):
    root = tmp_path/'capacity'
    path = tmp_path/'inventory.json'
    path.write_text(json.dumps({'devices': [{'device_id': 'GPU-test-controlled',
        'type': 'gpu', 'memory_bytes': 4 * 1024**3}]}))
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(root))
    monkeypatch.setenv('DEMIFLOW_DEVICE_INVENTORY', str(path))
    return root


def visible(row):
    import os
    from demiflow.execution.hosts.devices import gpu_capacity
    if gpu_capacity('local') != 0:
        raise RuntimeError('Child opened a second GPU authority')
    return {'n': row['n'], 'devices': os.environ.get('CUDA_VISIBLE_DEVICES')}


def test_real_dataset_gpu_grant_reaches_child_and_releases_on_exit(tmp_path, monkeypatch):
    root = inventory(tmp_path, monkeypatch)
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        actual = data.from_items([{'n': 1}]).map(visible, backend_options={'ray': {'worker_resources': {'gpu': 1}}}).take_all()
        assert actual == [{'n': 1, 'devices': 'GPU-test-controlled'}]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    from demiflow.execution.hosts.devices import device_pool
    assert device_pool('local').snapshot()['charged'] == {}
    records = [json.loads(p.read_text()) for p in (root/'devices'/'allocations').glob('*.json')]
    assert records and all(r['state'] == 'RELEASED' for r in records)


def test_cpu_worker_does_not_inherit_ungranted_configured_gpu(tmp_path, monkeypatch):
    inventory(tmp_path, monkeypatch)
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', 'ungranted-device')
    with RunContext.start(tmp_path/'run'), data.local_execution():
        assert data.from_items([{'n': 1}]).map(visible).take_all() == [{'n': 1, 'devices': ''}]


def test_gpu_without_inventory_rejects_before_business_activation(tmp_path, monkeypatch):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    monkeypatch.delenv('DEMIFLOW_DEVICE_INVENTORY', raising=False)
    with pytest.raises(DataflowError, match='GPU capacity'):
        with RunContext.start(tmp_path/'run'), data.local_execution():
            data.from_items([{'n': 1}]).map(visible, backend_options={'ray': {'worker_resources': {'gpu': 1}}}).take_all()


def test_nested_run_cannot_create_second_device_authority(tmp_path, monkeypatch):
    root = inventory(tmp_path, monkeypatch)
    from demiflow.resources import capacity
    from demiflow.execution.hosts.devices import device_pool, gpu_capacity
    monkeypatch.setattr(capacity, 'inherited_envelope', lambda: {'allocation_id': 'outer'})
    assert device_pool('local') is None and gpu_capacity('local') == 0
    assert not (root/'devices').exists()


def test_ray_core_device_grant_uses_ray_assigned_visibility(tmp_path, monkeypatch):
    import tempfile
    ray = pytest.importorskip('ray')
    from demiflow.data.dataset import Dataset
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from demiflow.execution.hosts.devices import device_pool
    inventory(tmp_path, monkeypatch)
    try:
        # Controlled logical GPU scheduling only: no CUDA library is loaded.
        ray.init(address='local', num_cpus=4, num_gpus=1, include_dashboard=False,
                 object_store_memory=100*1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-device-'))
        with RunContext.start(tmp_path/'run') as run:
            ds = data.from_items([{'n': 1}]).map(visible,
                backend_options={'ray': {'worker_resources': {'gpu': 1}}})
            actual = Dataset(ds._source, ds._plan, RayDatasetExecutor()).take_all()
            assert actual == [{'n': 1, 'devices': '0'}]
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
        assert device_pool('ray_core').snapshot()['charged'] == {}
    finally:
        ray.shutdown()
