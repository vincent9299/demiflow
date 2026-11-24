"""Four real devices simultaneously owned by one public Dataset graph.

This is single-host CPU/GPU separation, not a multi-host or throughput claim.
The gate is reached only after real CUDA computation on every original grant.
"""
import json
import os
from pathlib import Path
import threading
import time

import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from test_runtime_v3_physical_gpu import (
    _calls, _cpu_after, _cpu_before, _cuda_calculate, _limits, _smi,
)


@pytest.fixture
def four_gpus(tmp_path, monkeypatch):
    if os.environ.get('DEMIFLOW_PHYSICAL_GPU') != '1':
        pytest.skip('explicit real-GPU execution opt-in required')
    pytest.importorskip('torch')
    devices = []
    for line in _smi('index,uuid,name,memory.total,memory.used,utilization.gpu').splitlines():
        index, uuid, name, total, used, utilization = [part.strip() for part in line.split(',')]
        devices.append({'index': int(index), 'device_id': uuid, 'name': name,
                        'memory_bytes': int(total)*1024**2, 'used_mib': int(used),
                        'utilization': int(utilization)})
    assert len(devices) == 4 and {d['index'] for d in devices} == set(range(4))
    assert all(d['used_mib'] == 0 and d['utilization'] == 0 for d in devices), devices
    assert not _smi('pid,gpu_uuid,used_gpu_memory', 'compute-apps')
    inventory = tmp_path / 'actual-inventory.json'
    inventory.write_text(json.dumps({'devices': [
        {'device_id': d['device_id'], 'type': 'gpu', 'memory_bytes': d['memory_bytes'],
         'max_residents': 1} for d in devices]}))
    monkeypatch.setenv('DEMIFLOW_DEVICE_INVENTORY', str(inventory))
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path/'capacity'))
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', ','.join(d['device_id'] for d in devices))
    (tmp_path/'hardware-before.json').write_text(json.dumps(devices, indent=2))
    (tmp_path/'calls').mkdir(); (tmp_path/'ready').mkdir()
    yield devices
    from demiflow.execution.hosts.devices import device_pool
    snapshot = device_pool('local').snapshot()
    assert not snapshot['active'] and not snapshot['charged']
    allocations = [json.loads(p.read_text()) for p in
        (tmp_path/'capacity'/'devices'/'allocations').glob('*.json')]
    calls = _calls(tmp_path/'calls')
    assert not calls or len(allocations) >= 4
    assert all(a['state'] == 'RELEASED' and a['release_evidence'] for a in allocations)
    assert all(not Path('/proc/'+str(c['pid'])).exists() for c in calls)
    deadline = time.monotonic()+5
    while _smi('pid,gpu_uuid,used_gpu_memory', 'compute-apps'):
        assert time.monotonic() < deadline, 'an owned CUDA context remains after physical cleanup'
        time.sleep(.05)
    (tmp_path/'released.json').write_text(json.dumps({
        'snapshot': snapshot, 'allocations': allocations, 'calls': calls,
        'compute_apps_after': _smi('pid,gpu_uuid,used_gpu_memory', 'compute-apps')}, indent=2))


def _parallel_cuda(row, *, lane):
    import json
    import os
    from pathlib import Path
    import time
    visible = os.environ.get('CUDA_VISIBLE_DEVICES')
    if visible not in row['actual_uuids']:
        raise RuntimeError('GPU stage did not receive one of the physical device grants')
    result = _cuda_calculate({**row, 'device_uuid': visible})
    ready = Path(row['ready'])
    temporary = ready/(str(os.getpid())+'.tmp')
    temporary.write_text(json.dumps({**result['cuda'], 'lane': lane}))
    temporary.replace(ready/(str(lane)+'.json'))
    deadline = time.monotonic()+45
    while not Path(row['gate']).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError('four physical CUDA grants did not reach the same live window')
        time.sleep(.01)
    if row['mode'] == 'failure' and lane == 2:
        raise RuntimeError('owned four-GPU business failure after simultaneous CUDA')
    return {**result, 'lane': lane}


def _four_branch_pipeline(tmp_path, devices, mode):
    source = data.from_items([{
        'n': 0, 'evidence': str(tmp_path/'calls'), 'ready': str(tmp_path/'ready'),
        'gate': str(tmp_path/'release'), 'mode': mode,
        'actual_uuids': [d['device_id'] for d in devices],
    }]).map(_cpu_before)
    branches = [source.map(_parallel_cuda, fn_kwargs={'lane': lane},
        backend_options={'ray': {'worker_resources': {
            'gpu': 1, 'memory_bytes': 2*1024**3}}}) for lane in range(4)]
    return branches[0].union(*branches[1:]).map(_cpu_after)


@pytest.mark.parametrize('mode', ['normal', 'failure', 'cancel'])
def test_four_physical_gpu_workers_overlap_and_release(tmp_path, four_gpus, mode):
    from demiflow.execution.hosts.devices import device_pool
    devices = four_gpus
    failures = []
    observed = []
    completed = threading.Event()
    def observe_original_grants(ref):
        try:
            deadline = time.monotonic()+60
            while len(list((tmp_path/'ready').glob('*.json'))) < 4:
                if completed.wait(.02):
                    raise AssertionError('Dataset ended before all four real GPU callbacks ran')
                assert time.monotonic() < deadline, 'four real GPU workers never became ready'
            ready = [json.loads(p.read_text()) for p in (tmp_path/'ready').glob('*.json')]
            expected = {d['device_id'] for d in devices}
            assert {r['visible'] for r in ready} == expected
            assert len({r['pid'] for r in ready}) == 4 and {r['lane'] for r in ready} == set(range(4))
            snapshot = device_pool('local').snapshot()
            assert len(snapshot['active']) == 4
            allocations = {a['allocation_id']: a for a in snapshot['active']}
            for r in ready:
                assert r['allocation_id'] in allocations
                assert [d['device_id'] for d in allocations[r['allocation_id']]['devices']] == [r['visible']]
                assert r['device_count'] == 1 and 0 < r['allocated_peak'] <= 1024**2
            actual = _smi('pid,gpu_uuid,used_gpu_memory', 'compute-apps')
            physical = {(int(p.strip()), u.strip()): int(m.strip())
                        for p, u, m in (line.split(',') for line in actual.splitlines())}
            assert {(r['pid'], r['visible']) for r in ready} <= set(physical)
            assert all(0 < physical[(r['pid'], r['visible'])] < 2048 for r in ready)
            result = {'ready': ready, 'snapshot': snapshot, 'nvml': actual, 'mode': mode}
            observed.append(result)
            (tmp_path/'simultaneous.json').write_text(json.dumps(result, indent=2))
            if mode == 'cancel':
                RunContext.cancel(ref)
        except BaseException as exc:
            failures.append(exc)
        finally:
            (tmp_path/'release').write_text('original grants inspected')
    thread = None
    def execute():
        nonlocal thread
        with RunContext.start(tmp_path/'run', limits=_limits(memory_bytes=12*1024**3,
                timeout_s=120)) as run, data.local_execution(workers=12, worker_mode='process',
                    partitions=1, memory_bytes=12*1024**3):
            thread = threading.Thread(target=observe_original_grants, args=(run.ref,), daemon=True)
            thread.start()
            result = _four_branch_pipeline(tmp_path, devices, mode).take_all()
            assert mode == 'normal'
            assert {r['lane'] for r in result} == set(range(4)) and len(result) == 4
            assert len({r['cuda']['visible'] for r in result}) == 4
            assert len({r['cpu_before'] for r in result}) == 1, 'the shared CPU predecessor ran once'
            assert all(r['cpu_before'] != r['cuda']['pid'] != r['cpu_after'] for r in result)
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    try:
        if mode == 'normal':
            execute()
        else:
            with pytest.raises(Exception, match=('owned four-GPU business failure' if mode == 'failure'
                    else '(?i)cancel|stopping')):
                execute()
    finally:
        completed.set()
        if thread is not None:
            thread.join(5)
            assert not thread.is_alive()
    assert not failures, failures
    assert len(observed) == 1 and len(_calls(tmp_path/'calls')) == 4
