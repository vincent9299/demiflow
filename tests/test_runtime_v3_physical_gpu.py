"""Opt-in physical CUDA proofs, distinct from logical inventory unit tests.

Run only on explicitly authorised, idle hardware with
DEMIFLOW_PHYSICAL_GPU=1.  Inventory is derived from nvidia-smi, never invented.
The test reserves one whole real card; tensor peaks are measured, not presented
as CUDA-driver/cgroup hard isolation or as a fractional GPU reservation.
"""
import json
import os
from pathlib import Path
import subprocess
import time

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def _smi(fields, kind='gpu'):
    return subprocess.check_output(['nvidia-smi', '--query-' + kind + '=' + fields,
        '--format=csv,noheader,nounits'], text=True, timeout=15).strip()


@pytest.fixture
def physical_gpu(tmp_path, monkeypatch):
    if os.environ.get('DEMIFLOW_PHYSICAL_GPU') != '1':
        pytest.skip('explicit real-GPU execution opt-in required')
    pytest.importorskip('torch')
    inventory = []
    for line in _smi('index,uuid,name,memory.total,memory.used,utilization.gpu').splitlines():
        index, uuid, name, total, used, utilization = [part.strip() for part in line.split(',')]
        inventory.append(dict(index=int(index), uuid=uuid, name=name,
                              memory_mib=int(total), used_mib=int(used), utilization=int(utilization)))
    selected = next(gpu for gpu in inventory if gpu['index'] == 0)
    assert selected['used_mib'] == 0 and selected['utilization'] == 0, selected
    assert selected['uuid'] not in _smi('pid,gpu_uuid,used_gpu_memory', 'compute-apps')
    path = tmp_path / 'physical-inventory.json'
    path.write_text(json.dumps({'devices': [{'device_id': selected['uuid'], 'type': 'gpu',
        'memory_bytes': selected['memory_mib'] * 1024**2, 'max_residents': 1}]}))
    monkeypatch.setenv('DEMIFLOW_DEVICE_INVENTORY', str(path))
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path / 'capacity'))
    # CPU children must explicitly clear even a real valid inherited device.
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', selected['uuid'])
    (tmp_path / 'hardware-before.json').write_text(json.dumps(inventory, indent=2))
    evidence = tmp_path / 'cuda-calls'
    evidence.mkdir()
    yield selected, evidence
    _assert_released(tmp_path, selected, evidence)


def _cpu_before(row):
    import os
    if os.environ.get('CUDA_VISIBLE_DEVICES') != '':
        raise RuntimeError('CPU source stage inherited a CUDA device')
    return {**row, 'cpu_before': os.getpid(), 'cpu_before_visible': ''}


def _cuda_calculate(row):
    import json
    import os
    from pathlib import Path
    import time
    import torch
    from demiflow.execution.dataflow_devices import gpu_capacity
    from demiflow.resources.capacity import inherited_envelope
    if gpu_capacity('local') != 0:
        raise RuntimeError('worker opened a second GPU allocator')
    envelope = inherited_envelope()
    if not envelope or not envelope['allocation_id']:
        raise RuntimeError('missing original resource grant')
    if not torch.cuda.is_available() or torch.cuda.device_count() != 1:
        raise RuntimeError('real CUDA device was not bound exclusively')
    visible = os.environ['CUDA_VISIBLE_DEVICES']
    if visible != row['device_uuid']:
        raise RuntimeError('CUDA visibility differs from physical grant')
    torch.cuda.reset_peak_memory_stats()
    values = torch.arange(4096, dtype=torch.float32, device='cuda')
    answer = int((values * 2 + row['n']).sum().item())
    torch.cuda.synchronize()
    measured = {'n': row['n'], 'answer': answer, 'pid': os.getpid(), 'visible': visible,
        'device_count': torch.cuda.device_count(), 'device_name': torch.cuda.get_device_name(0),
        'torch': str(torch.__version__), 'cuda': torch.version.cuda,
        'allocated_peak': torch.cuda.max_memory_allocated(),
        'reserved_peak': torch.cuda.max_memory_reserved(),
        'allocation_id': envelope['allocation_id']}
    if not 0 < measured['allocated_peak'] <= 1024**2 or measured['reserved_peak'] > 8 * 1024**2:
        raise RuntimeError('test CUDA tensor memory exceeded its finite window')
    (Path(row['evidence']) / (str(os.getpid()) + '-' + str(row['n']) + '.json')).write_text(json.dumps(measured))
    if row.get('fail') and row['n'] == 1:
        raise RuntimeError('owned physical CUDA business failure')
    # Work remains after the first live result during the cancellation case.
    if row.get('pace'):
        time.sleep(0.03)
    return {**row, 'cuda': measured}


def _cpu_after(row):
    import os
    if os.environ.get('CUDA_VISIBLE_DEVICES') != '':
        raise RuntimeError('CPU consumer stage inherited a CUDA device')
    if row['cuda']['answer'] != 4096 * 4095 + row['n'] * 4096:
        raise RuntimeError('CUDA arithmetic result differs')
    return {**row, 'cpu_after': os.getpid(), 'cpu_after_visible': ''}


def _pipeline(selected, evidence, count=3, **flags):
    rows = [{'n': n, 'device_uuid': selected['uuid'], 'evidence': str(evidence), **flags}
            for n in range(count)]
    return data.from_items(rows).map(_cpu_before).map(_cuda_calculate,
        backend_options={'ray': {'worker_resources': {'gpu': 1, 'memory_bytes': 2*1024**3}}}
    ).map(_cpu_after)


def _local():
    return data.local_execution(workers=5, worker_mode='process', partitions=1,
                                memory_bytes=8*1024**3)


def _limits(**kwargs):
    return {'timeout_s': 90, 'checkpoint_interval_ms': 60000, 'memory_bytes': 8*1024**3,
            'max_row_bytes': 64*1024, 'max_batch_bytes': 256*1024, **kwargs}


def _calls(evidence):
    return [json.loads(path.read_text()) for path in sorted(evidence.glob('*.json'))]


def _assert_released(tmp_path, selected, evidence):
    from demiflow.execution.dataflow_devices import device_pool
    snapshot = device_pool('local').snapshot()
    assert snapshot['charged'] == {} and snapshot['active'] == []
    allocations = [json.loads(path.read_text()) for path in
        (tmp_path / 'capacity' / 'devices' / 'allocations').glob('*.json')]
    observed_pids = {call['pid'] for call in _calls(evidence)}
    assert allocations or not observed_pids
    assert all(entry['state'] == 'RELEASED' for entry in allocations)
    assert all(not Path('/proc/' + str(pid)).exists() for pid in observed_pids)
    # CUDA contexts can take a short time to disappear from NVML after pidfd exit.
    deadline = time.monotonic() + 5
    while selected['uuid'] in _smi('pid,gpu_uuid,used_gpu_memory', 'compute-apps'):
        assert time.monotonic() < deadline, 'owned GPU context was not released'
        time.sleep(0.05)
    (tmp_path / 'physical-release.json').write_text(json.dumps({
        'device_uuid': selected['uuid'], 'snapshot': snapshot, 'allocations': allocations,
        'observed_cuda_pids': sorted(observed_pids), 'compute_apps': _smi('pid,gpu_uuid,used_gpu_memory', 'compute-apps')}, indent=2))


def test_physical_cuda_cpu_gpu_cpu_final_ack_and_replay(tmp_path, physical_gpu, monkeypatch):
    selected, evidence = physical_gpu
    class InterruptedAfterAction(Exception):
        pass
    with pytest.raises(InterruptedAfterAction):
        with RunContext.start(tmp_path/'run', limits=_limits()) as run, _local():
            ref = run.ref
            result = _pipeline(selected, evidence).take_all()
            assert [row['n'] for row in result] == [0, 1, 2]
            for row in result:
                assert row['cpu_before'] != row['cuda']['pid'] != row['cpu_after']
            commits = run.journal.execution_commits('action-1')
            assert commits[-1]['status'] == 'acked'
            raise InterruptedAfterAction()
    _assert_released(tmp_path, selected, evidence)
    original_calls = _calls(evidence)
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    def no_launch(*args, **kwargs):
        raise AssertionError('final ACK must replay without new CPU/GPU work')
    monkeypatch.setattr(DataflowExecution, '_launch_phase', no_launch)
    with RunContext.resume(ref) as run, _local():
        assert _pipeline(selected, evidence).take_all() == result
        assert run.journal.execution_commits('action-1') == commits
    assert _calls(evidence) == original_calls and len(original_calls) == 3


def test_physical_cuda_failure_releases_original_grant(tmp_path, physical_gpu):
    selected, evidence = physical_gpu
    with pytest.raises(Exception, match='owned physical CUDA business failure'):
        with RunContext.start(tmp_path/'run', limits=_limits()), _local():
            _pipeline(selected, evidence, fail=True).take_all()
    assert {call['n'] for call in _calls(evidence)} >= {0, 1}


def test_physical_cuda_early_close_releases_original_grant(tmp_path, physical_gpu, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    selected, evidence = physical_gpu
    executions = []
    start = DataflowExecution.start
    def capture(execution):
        executions.append(execution)
        return start(execution)
    monkeypatch.setattr(DataflowExecution, 'start', capture)
    with RunContext.start(tmp_path/'run', limits=_limits()) as run, _local():
        # Exceed all bounded inter-stage credits so the source itself remains
        # incomplete when the first CUDA result reaches the user.
        rows = _pipeline(selected, evidence, count=4096, pace=True).iter_rows()
        first = next(rows)
        assert first['n'] == 0 and first['cuda']['answer'] == 4096 * 4095
        rows.close()
        execution = executions[0]
        assert execution._stopping and not execution._restore_image.cut.final
        assert run.journal.execution_commit(execution._restore_image.commit_id)['status'] == 'acked'
    assert 0 < len(_calls(evidence)) < 4096


def test_physical_cuda_nonfinal_ack_recovery_uses_original_cut(tmp_path, physical_gpu, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    selected, evidence = physical_gpu
    class InterruptedAfterAck(Exception):
        pass
    advance = DataflowExecution._advance_cut
    saved = []
    def stop_after_ack(execution):
        value = advance(execution)
        image = execution._restore_image
        if image is not None and not image.cut.final:
            rows = execution._publication(image)['manifest']['total_rows']
            if rows:
                assert execution._action.context.journal.execution_commit(image.commit_id)['status'] == 'acked'
                saved.append((image.commit_id, rows))
                raise InterruptedAfterAck()
        return value
    with pytest.raises(InterruptedAfterAck):
        with RunContext.start(tmp_path/'run', limits=_limits(checkpoint_rows=2)) as run, _local():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_advance_cut', stop_after_ack)
                _pipeline(selected, evidence, count=32, pace=True).take_all()
    assert saved and 0 < saved[0][1] < 32
    _assert_released(tmp_path, selected, evidence)
    with RunContext.resume(ref) as run, _local():
        result = _pipeline(selected, evidence, count=32, pace=True).take_all()
        assert [row['n'] for row in result] == list(range(32))
        commits = run.journal.execution_commits('action-1')
        assert any(entry['commit_id'] == saved[0][0] for entry in commits)
        assert commits[-1]['status'] == 'acked'
    assert len({call['pid'] for call in _calls(evidence)}) >= 2
