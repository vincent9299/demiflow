"""Real CPU preparation, resident CUDA model and slow IO on original grants.

The dedicated interpreter owns its private runtime. No fake CUDA inventory,
worker, allocator, journal or checkpoint is substituted. Only observations and
the business fixture's bounded synchronization files are test-owned.
"""
from pathlib import Path
import json
import os
import subprocess
import sys
import time


def _write(root, name, value):
    path = Path(root) / (name + '.json')
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value))
    tmp.replace(path)


def _read(path):
    return json.loads(Path(path).read_text())


async def _prepare(row):
    import asyncio
    assert os.environ.get('CUDA_VISIBLE_DEVICES') == ''
    if row['n'] == 1:
        # A co-located/blocking preparation stage cannot get past this point.
        # The GPU must consume row zero before preparation releases row one.
        deadline = time.monotonic() + 30
        while not (Path(row['events']) / 'gpu-0.json').exists():
            assert time.monotonic() < deadline, 'CPU preparation blocked the independent GPU stage'
            await asyncio.sleep(.01)
    _write(row['events'], 'cpu-' + str(row['n']), {'pid': os.getpid(), 'n': row['n']})
    return {**row, 'cpu_pid': os.getpid()}


class ResidentModel:
    def __init__(self, directory, provider):
        import torch
        from demiflow.execution.hosts.devices import gpu_capacity
        assert gpu_capacity(provider) == 0, 'worker must not create another device allocator'
        assert torch.cuda.is_available() and torch.cuda.device_count() == 1
        self.directory, self.provider, self.calls = directory, provider, 0
        self.model = torch.nn.Linear(16, 16, bias=False).to('cuda')
        with torch.no_grad():
            self.model.weight.fill_(1)
        self.identity = id(self.model)
        self.credits = {'samples': 0, 'peak_bytes': 0, 'peak_leases': 0}
        from demiflow.execution.exchange.dataflow_state import ChannelSetState
        self.original_reserve = ChannelSetState.reserve
        original, report = self.original_reserve, self.credits
        def reserve(state, *args, **kwargs):
            value = original(state, *args, **kwargs)
            with state._lock:
                size = state._resident_bytes + state._lease_bytes
                leases = len(state._leases)
                assert size <= state.bounds.max_resident_bytes
                assert leases <= state.bounds.max_pending_events
                for spec in state.specs:
                    for reader in spec.consumer_partitions:
                        credit = state.reader_credit(spec.channel_id, reader)
                        assert 0 <= credit['rows_left'] <= spec.credit_rows
                        assert 0 <= credit['bytes_left'] <= spec.credit_bytes
            report['samples'] += 1
            report['peak_bytes'] = max(report['peak_bytes'], size)
            report['peak_leases'] = max(report['peak_leases'], leases)
            return value
        ChannelSetState.reserve = reserve
        _write(directory, 'model-created', {'pid': os.getpid(), 'model_id': self.identity,
            'visible': os.environ['CUDA_VISIBLE_DEVICES']})

    def __call__(self, row):
        import torch
        self.calls += 1
        with torch.no_grad():
            result = self.model(torch.full((16,), float(row['n']), device='cuda'))
            answer = int(result.sum().item())
        torch.cuda.synchronize()
        assert answer == row['n'] * 256
        measured = {'pid': os.getpid(), 'model_id': self.identity, 'n': row['n'],
            'answer': answer, 'allocated': torch.cuda.max_memory_allocated(),
            'reserved': torch.cuda.max_memory_reserved(), 'cuda_window':64*1024**2}
        _write(self.directory, 'gpu-' + str(row['n']), measured)
        # Linear uses a real cuBLAS workspace (32 MiB in the pinned runtime),
        # unlike the earlier pointwise CUDA fixture. This separate finite
        # observation window is below the original 2 GiB worker reservation;
        # it is not a claim that the allocator enforces a hard VRAM ceiling.
        assert 0 < measured['allocated'] <= measured['reserved'] <= measured['cuda_window'], measured
        return {**row, 'gpu': measured}

    def snapshot_state(self):
        # Immutable model weights are rebuilt by the same code-pinned factory;
        # only the actual cross-row counter is checkpointed, never a CUDA handle.
        return {'calls': self.calls}

    def restore_state(self, state):
        self.calls = state['calls']

    def close(self):
        from demiflow.execution.exchange.dataflow_state import ChannelSetState
        ChannelSetState.reserve = self.original_reserve
        _write(self.directory, 'model-closed', {'pid': os.getpid(), 'calls': self.calls,
            'model_id': self.identity, 'credits': self.credits})
        self.model = None


async def _io(row):
    import asyncio
    assert os.environ.get('CUDA_VISIBLE_DEVICES') == ''
    _write(row['events'], 'io-' + str(row['n']), {'pid': os.getpid(), 'n': row['n']})
    if row['n'] == 0:
        deadline = time.monotonic() + 30
        while not (Path(row['events']) / 'release').exists():
            assert time.monotonic() < deadline, 'test-owned slow IO was not released'
            await asyncio.sleep(.01)
    return {'n': row['n'], 'answer': row['gpu']['answer'], 'cpu_pid': row['cpu_pid'],
            'gpu_pid': row['gpu']['pid'], 'io_pid': os.getpid(), 'model_id': row['gpu']['model_id']}


def _execute(root, provider):
    import threading
    from types import SimpleNamespace
    from unittest.mock import patch
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.hosts.devices import device_pool
    from runtime_v3_residency_scenarios import _provider
    from runtime_v3_performance_scenarios import _plain
    from test_runtime_v3_physical_gpu import _smi
    from runtime_v3_support import _ExecuteActionRecorder, _RayDataBlocker, assert_tree_binding
    root = Path(root)
    assert root.parent == Path('/tmp') and root.name.startswith('demiflow-')
    events = root / 'gpu-events'; events.mkdir()
    devices = []
    for line in _smi('index,uuid,memory.total,memory.used,utilization.gpu').splitlines():
        index, uuid, total, used, utilization = [p.strip() for p in line.split(',')]
        if index == '0':
            assert int(used) == int(utilization) == 0, 'the explicitly selected GPU is busy'
            devices.append({'device_id': uuid, 'type': 'gpu', 'memory_bytes': int(total)*1024**2,
                            'max_residents': 1})
    assert len(devices) == 1 and devices[0]['device_id'] not in _smi('pid,gpu_uuid,used_gpu_memory', 'compute-apps')
    inventory = root / 'gpu-inventory.json'; inventory.write_text(json.dumps({'devices': devices}))
    environment = {'DEMIFLOW_DEVICE_INVENTORY': str(inventory),
        'DEMIFLOW_RESOURCE_ROOT': str(root/'capacity'), 'CUDA_VISIBLE_DEVICES': devices[0]['device_id']}
    plans, observed, failures = [], [], []
    complete = threading.Event()
    prepare = DataflowExecution.prepare
    def prepared(execution):
        result = prepare(execution)
        plans.append({'graph': _plain(execution._graph), 'physical': _plain(execution._physical)})
        return result
    def watch():
        try:
            deadline = time.monotonic()+45
            while not (events/'io-0.json').exists():
                assert not complete.wait(.01), 'execution ended before real slow IO'
                assert time.monotonic()<deadline, 'independent CPU/GPU/IO never overlapped'
            time.sleep(.4)
            gpu_calls = sorted(events.glob('gpu-*.json'))
            cpu_calls = sorted(events.glob('cpu-*.json'))
            physical, graph = plans[0]['physical'], plans[0]['graph']
            gpu_stages = [s for s in physical['stages'] if s['profile']['requests'].get('accelerators')]
            assert len(gpu_stages) == 1
            gpu_stage = gpu_stages[0]
            # At most the outgoing channel credit, each adjacent operator's
            # active/queued row, and one pending handoff at either boundary.
            windows = [x['credit_rows'] for x in physical['exchanges']
                       if set(x['producer_partitions']) & set(gpu_stage['partitions'])]
            bound = sum(windows) + 2*(1+1) + 2
            assert windows and 1 <= len(gpu_calls) <= bound < 64, (len(gpu_calls), bound)
            assert 1 < len(cpu_calls) < 64, len(cpu_calls)
            snapshot = device_pool(provider).snapshot()
            assert len(snapshot['active']) == 1
            grant = snapshot['active'][0]
            assert [d['device_id'] for d in grant['devices']] == [devices[0]['device_id']]
            gpu_pid = _read(events/'model-created.json')['pid']
            nvml = _smi('pid,gpu_uuid,used_gpu_memory', 'compute-apps')
            assert any(int(p.strip()) == gpu_pid and u.strip() == devices[0]['device_id']
                       for p,u,_ in (line.split(',') for line in nvml.splitlines()))
            assert gpu_stage['profile']['requests']['memory_bytes'] == 2*1024**3
            observed.append({'gpu_calls_while_io_blocked': len(gpu_calls), 'cpu_prepared':len(cpu_calls),
                'prefetch_bound': bound, 'outgoing_credit_rows':windows, 'original_grant':grant, 'nvml':nvml})
        except BaseException as exc:
            failures.append(repr(exc))
        finally:
            (events/'release').touch()
    ray = None
    with patch.dict(os.environ, environment), patch.object(DataflowExecution, 'prepare', prepared):
        try:
            if provider == 'ray_core':
                import ray
                import tempfile
                assert not ray.is_initialized()
                ray.init(address='local',num_cpus=6,num_gpus=1,include_dashboard=False,
                    object_store_memory=128*1024**2,
                    _temp_dir=tempfile.mkdtemp(prefix='demiflow-gpu-',dir='/tmp'))
            with _ExecuteActionRecorder() as recorder, _RayDataBlocker() as blocked, \
                    _provider(SimpleNamespace(root=root,provider=provider),workers=5), \
                    RunContext.start(root/'gpu-run', limits={'timeout_s':90,'memory_bytes':8*1024**3,
                        'max_row_bytes':65536,'max_batch_bytes':262144,'max_state_bytes':2*1024**2,
                        'checkpoint_rows':128,'checkpoint_interval_ms':60000}) as run:
                thread = threading.Thread(target=watch); thread.start()
                try:
                    rows = [{'n':i,'events':str(events),'payload':'x'*4096} for i in range(64)]
                    output = data.from_items(rows).map_async(_prepare,concurrency=1,queue_depth=1).map(
                        ResidentModel,fn_constructor_args=[str(events),provider],execution='thread',
                        concurrency=1,queue_depth=1,callable_scope='stage',backend_options={'ray':{
                            'worker_resources':{'gpu':1,'memory_bytes':2*1024**3}}}).map_async(
                                _io,concurrency=1,queue_depth=1).take_all()
                    commits = run.journal.execution_commits('action-1')
                finally:
                    complete.set(); (events/'release').touch(); thread.join(47)
                    assert not thread.is_alive()
            assert not failures, failures
            assert len(observed) == 1 and [r['n'] for r in output] == list(range(64))
            assert all(r['answer'] == r['n']*256 for r in output)
            assert all(len({r['cpu_pid'],r['gpu_pid'],r['io_pid']}) == 3 for r in output)
            assert len({(r['gpu_pid'],r['model_id']) for r in output}) == 1
            assert commits and commits[-1]['status'] == 'acked'
            closed = _read(events/'model-closed.json')
            assert closed['calls'] == 64 and closed['credits']['samples'] > 0
            snapshot = device_pool(provider).snapshot()
            assert not snapshot['active'] and not snapshot['charged']
            allocations = [_read(p) for p in (root/'capacity/devices/allocations').glob('*.json')]
            assert len(allocations) == 1 and allocations[0]['state'] == 'RELEASED'
            assert not Path('/proc/'+str(closed['pid'])).exists()
            proof = {'provider':provider,'output':output,'plans':plans,'overlap':observed,
                'model':closed,'commits':commits,'device_allocations':allocations,
                'child_execute_action_calls':recorder.calls,'child_ray_data_import_attempts':blocked.attempts,
                'child_module_paths':assert_tree_binding()}
        finally:
            if ray is not None and ray.is_initialized():
                ray.shutdown()
    _write(root/'evidence', 'gpu-native', proof)
    return proof


def validate_evidence(proof):
    assert proof['output'] and len(proof['output']) == 64
    assert all(len({r['cpu_pid'],r['gpu_pid'],r['io_pid']}) == 3 for r in proof['output'])
    assert [r['answer'] for r in proof['output']] == [i*256 for i in range(64)]
    assert proof['model']['calls'] == 64 and proof['model']['credits']['samples'] > 0
    assert len({(r['gpu_pid'],r['model_id']) for r in proof['output']}) == 1
    assert len(proof['overlap']) == 1 and proof['overlap'][0]['gpu_calls_while_io_blocked'] <= proof['overlap'][0]['prefetch_bound'] < 64
    assert proof['commits'] and proof['commits'][-1]['status'] == 'acked'
    assert proof['device_allocations'] and all(a['state']=='RELEASED' for a in proof['device_allocations'])
    assert proof['child_execute_action_calls'] and not proof['child_ray_data_import_attempts']
    from runtime_v3_support import WORKTREE
    assert proof['child_module_paths'] and all(Path(p).is_relative_to(WORKTREE)
        for p in proof['child_module_paths'].values())


def cpu_gpu_isolation(fx, fault=None):
    from runtime_v3_support import NotRunError, WORKTREE
    if os.environ.get('DEMIFLOW_PHYSICAL_GPU') != '1':
        detail = {'missing_semantic_harness':'explicit authorized physical GPU invocation required'}
        fx._record_not_run('cpu_gpu_isolation',detail)
        raise NotRunError('cpu_gpu_isolation',detail)
    log = fx.evidence_dir/'gpu-child.log'
    with log.open('w') as stream:
        result = subprocess.run([sys.executable,'-P','-m','runtime_v3_gpu_scenarios',str(fx.root),fx.provider],
            cwd=WORKTREE,stdout=stream,stderr=subprocess.STDOUT,timeout=180)
    assert result.returncode == 0, log.read_text()[-12000:]
    proof = _read(fx.evidence_dir/'gpu-native.json')
    validate_evidence(proof)
    return proof


if __name__ == '__main__':
    _execute(sys.argv[1],sys.argv[2])
