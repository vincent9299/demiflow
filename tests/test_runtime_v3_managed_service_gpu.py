"""One explicitly allowed real local GPU, native Dataset/cache/cancel paths."""
import json
import os
from pathlib import Path
import threading
import time

import psutil
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.dataflow_managed_devices import _physical_devices
from demiflow.execution.hosts.devices import device_pool
from runtime_v3_managed_http_scenarios import managed_fixture, events
from test_runtime_v3_managed_embedding_service import dataset


@pytest.fixture
def gpu_service(tmp_path, monkeypatch, request):
    if os.environ.get('DEMIFLOW_PHYSICAL_GPU') != '1':
        pytest.skip('explicit real-GPU execution opt-in required')
    physical = _physical_devices()
    expected = os.environ['DEMIFLOW_MANAGED_TEST_GPU_UUID']
    index = next(i for i, row in enumerate(physical) if row['uuid'] == expected)
    device = physical[index]
    inventory = tmp_path / 'inventory.json'
    inventory.write_text(json.dumps({'devices': [{'device_id': expected, 'type': 'gpu',
        'memory_bytes': device['memory_bytes'], 'max_residents': 1}]}))
    monkeypatch.setenv('DEMIFLOW_DEVICE_INVENTORY', str(inventory))
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path / 'capacity'))
    monkeypatch.setenv('CUDA_DEVICE_ORDER', 'PCI_BUS_ID')
    fixture = managed_fixture(tmp_path, monkeypatch, request=request,
        mode={'normal': 'cuda_wait', 'cancel': 'cuda_slow', 'overrss': 'cuda_overrss'}[request.param])
    fixture['service'].gpus = (index,)
    fixture['service'].startup_timeout_s = 30
    fixture['expected_uuid'] = expected
    return fixture


def _released(fixture):
    rows = events(fixture)
    for row in rows:
        if row['event'] == 'start':
            assert row['cuda'] == fixture['expected_uuid']
            assert not psutil.pid_exists(row['pid'])
    snapshot = device_pool('local').snapshot()
    assert not snapshot['active'] and not snapshot['charged']
    for p in Path(os.environ['DEMIFLOW_RESOURCE_ROOT']).glob('devices/allocations/*.json'):
        item = json.loads(p.read_text())
        assert item['state'] == 'RELEASED' and item['release_evidence']
    return snapshot


@pytest.mark.parametrize('gpu_service', ['normal', 'cancel'], indirect=True)
def test_real_managed_gpu_uses_original_uuid_grant_and_releases_tree(tmp_path, gpu_service):
    fixture = gpu_service
    mode = 'normal' if fixture['service'].command[-1] == 'cuda_wait' else 'cancel'
    observations, failures = [], []
    stopped = threading.Event()
    limits = {'memory_bytes': 4 * 1024**3, 'max_rss_bytes': 2 * 1024**3, 'timeout_s': 60}

    def observe(ref):
        try:
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline and not stopped.is_set():
                rows = events(fixture)
                if any(row['event'] == 'request' for row in rows):
                    cuda = next(row for row in rows if row['event'] == 'cuda')
                    assert cuda['devices'] == 1 and cuda['values'] == [3., 4., 0.] and cuda['allocated'] > 0
                    snapshot = device_pool('local').snapshot()
                    assert len(snapshot['active']) == 1
                    assert [d['device_id'] for d in snapshot['active'][0]['devices']] == [fixture['expected_uuid']]
                    allocation_id = snapshot['active'][0]['allocation_id']
                    cpu_record = json.loads((Path(os.environ['DEMIFLOW_RESOURCE_ROOT']) /
                        'capacity/allocations' / (allocation_id + '.json')).read_text())
                    assert cpu_record['effective']['memory_bytes'] == 2*1024**3
                    import subprocess
                    nvml = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,gpu_uuid,used_gpu_memory',
                        '--format=csv,noheader,nounits'], text=True)
                    assert any(line.split(',')[0].strip() == str(cuda['pid'])
                        and line.split(',')[1].strip() == fixture['expected_uuid'] for line in nvml.splitlines())
                    observations.append({'cuda': cuda, 'device_pool': snapshot, 'nvml': nvml,
                                         'original_cpu_memory_reservation': cpu_record})
                    if mode == 'cancel':
                        RunContext.cancel(ref)
                    fixture['events'].with_suffix('.release').touch()
                    return
                time.sleep(.01)
            if not stopped.is_set():
                raise AssertionError('Managed service did not reach its actual GPU request')
        except BaseException as exc:
            failures.append(repr(exc))
            fixture['events'].with_suffix('.release').touch()

    def execute():
        with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(
                workers=4, memory_bytes=4*1024**3, native_resources={'max_rss_bytes': 2*1024**3}):
            ds = dataset(fixture, tmp_path, cached=mode == 'normal')
            thread = threading.Thread(target=observe, args=(run.ref,)); thread.start()
            try:
                result = ds.take_all()
            finally:
                stopped.set()
                thread.join(42)
            assert not thread.is_alive()
            return result
    if mode == 'cancel':
        with pytest.raises(Exception, match='[Cc]ancel'):
            execute()
    else:
        result = execute()
        assert result[0]['embedding'] == pytest.approx([.6, .8, 0.])
    assert observations and not failures, failures
    released = _released(fixture)
    if mode == 'normal':
        fixture['script'].unlink()
        with RunContext.start(tmp_path / 'replay', limits=limits), data.local_execution(
                workers=4, memory_bytes=4*1024**3, native_resources={'max_rss_bytes': 2*1024**3}):
            assert dataset(fixture, tmp_path).take_all()[0]['call']['reused']
        assert sum(r['event'] == 'start' for r in events(fixture)) == 1
        _released(fixture)
    assert not list((tmp_path / 'port-locks').glob('gpu_*.lock'))
    assert not list((tmp_path / 'service-port-locks').rglob('gpu_*.lock'))
    (tmp_path / 'gpu-proof.json').write_text(json.dumps({'observations': observations,
        'released': released, 'events': events(fixture)}, indent=2))


def test_gpu_service_requires_explicit_model_rss_before_launch(tmp_path, monkeypatch, request):
    fixture = managed_fixture(tmp_path, monkeypatch, request=request)
    fixture['service'].gpus = (0,)
    with pytest.raises(Exception, match='explicit native_resources.max_rss_bytes'):
        dataset(fixture, tmp_path).take_all()
    assert events(fixture) == []


@pytest.mark.parametrize('missing', ['device_order', 'inventory'])
def test_gpu_service_rejects_ambiguous_or_undeployed_devices_before_launch(tmp_path, monkeypatch, request, missing):
    fixture = managed_fixture(tmp_path, monkeypatch, request=request)
    fixture['service'].gpus = (0,)
    monkeypatch.delenv('DEMIFLOW_DEVICE_INVENTORY', raising=False)
    monkeypatch.delenv('CUDA_DEVICE_ORDER', raising=False)
    if missing == 'inventory':
        monkeypatch.setenv('CUDA_DEVICE_ORDER', 'PCI_BUS_ID')
    with pytest.raises(Exception, match='PCI_BUS_ID' if missing == 'device_order' else 'explicit deployment inventory'):
        with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 4*1024**3,
                'max_rss_bytes': 2*1024**3}), data.local_execution(
                memory_bytes=4*1024**3, native_resources={'max_rss_bytes': 2*1024**3}):
            dataset(fixture, tmp_path).take_all()
    assert events(fixture) == []


def test_gpu_service_profile_does_not_expand_run_or_admit_ray_indices():
    from types import SimpleNamespace
    from demiflow.execution.dataflow_managed_devices import service_profile
    from demiflow.resources.contracts import ResourceProfile
    kernel = SimpleNamespace(configuration={'managed_http_devices': {'indices': [0]}},
        profile=ResourceProfile(requests={'cpu_millis': 100, 'memory_bytes': 32*1024**2,
                                           'accelerators': [{'type': 'gpu', 'count': 1}]}))
    bounds = SimpleNamespace(max_resident_bytes=256*1024**2)
    policy = SimpleNamespace(provider='local', worker_mode=None, native_limits={'max_rss_bytes': 512*1024**2})
    with pytest.raises(Exception, match='original run envelope'):
        service_profile(kernel, policy=policy, bounds=bounds)
    policy.provider = 'ray_core'
    with pytest.raises(Exception, match='Local process host'):
        service_profile(kernel, policy=policy, bounds=bounds)


@pytest.mark.parametrize('gpu_service', ['overrss'], indirect=True)
def test_real_gpu_service_tree_rss_exhaustion_stops_and_releases_original_grant(tmp_path, gpu_service):
    fixture = gpu_service
    with pytest.raises(Exception):
        with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 4*1024**3,
                'max_rss_bytes': 1024**3, 'timeout_s': 60}), data.local_execution(
                workers=4, memory_bytes=4*1024**3, native_resources={'max_rss_bytes': 1024**3}):
            dataset(fixture, tmp_path, cached=False).take_all()
    log = fixture['service'].log_path.read_text()
    assert 'worker/service tree exceeds its original max_rss_bytes grant' in log
    _released(fixture)
    records = [json.loads(p.read_text()) for p in Path(os.environ['DEMIFLOW_RESOURCE_ROOT']).glob('capacity/allocations/*.json')]
    assert any(r['effective']['memory_bytes'] == 1024**3 for r in records)
    assert all(r['state'] == 'RELEASED' for r in records)
    (tmp_path / 'rss-proof.json').write_text(json.dumps({'original_allocations': records,
        'events': events(fixture), 'guardian_log': log}, indent=2))
