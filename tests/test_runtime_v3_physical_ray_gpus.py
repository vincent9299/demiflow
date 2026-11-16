"""Explicit real-hardware gates; no logical GPU stand-ins or shared clusters."""
import json
import os
from pathlib import Path
import tempfile
import threading
import time

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def _before(row):
    import os
    import socket
    if os.environ.get('CUDA_VISIBLE_DEVICES') != '':
        raise RuntimeError('CPU predecessor inherited an ungranted GPU')
    return {**row,'cpu_before':os.getpid(),'cpu_before_host':socket.gethostname()}


def _after(row):
    import os
    import socket
    if os.environ.get('CUDA_VISIBLE_DEVICES') != '':
        raise RuntimeError('CPU successor inherited an ungranted GPU')
    return {**row,'cpu_after':os.getpid(),'cpu_after_host':socket.gethostname()}


def _cuda(row, *, lane=0):
    import json
    import os
    from pathlib import Path
    import socket
    import time
    import ray
    import torch
    from demiflow.execution.dataflow_ray_devices import worker_device_binding
    from demiflow.execution.dataflow_devices import gpu_capacity
    if gpu_capacity('ray_core') != 0 or torch.cuda.device_count()!=1:
        raise RuntimeError('GPU worker did not receive exactly its original grant')
    binding=worker_device_binding()
    torch.cuda.reset_peak_memory_stats()
    values=torch.arange(4096,dtype=torch.float32,device='cuda')
    answer=int((values*2+lane).sum().item());torch.cuda.synchronize()
    if answer!=4096*4095+lane*4096:
        raise RuntimeError('Actual CUDA arithmetic failed')
    proof={'lane':lane,'pid':os.getpid(),'hostname':socket.gethostname(),
           'actor_id':ray.get_runtime_context().get_actor_id(),
           'binding':binding,'answer':answer,'visible':os.environ['CUDA_VISIBLE_DEVICES'],
           'allocated_peak':torch.cuda.max_memory_allocated(),'reserved_peak':torch.cuda.max_memory_reserved()}
    if not 0<proof['allocated_peak']<=1024**2 or proof['reserved_peak']>8*1024**2:
        raise RuntimeError('Test CUDA tensor exceeded its finite window')
    if row.get('ready'):
        path=Path(row['ready'])/(str(lane)+'.json');tmp=path.with_suffix('.tmp')
        tmp.write_text(json.dumps(proof));tmp.replace(path)
        deadline=time.monotonic()+60
        while not Path(row['gate']).exists():
            if time.monotonic()>deadline:
                raise RuntimeError('Six actual GPU grants failed to overlap')
            time.sleep(.01)
    if row.get('mode')=='failure' and lane==2:
        raise RuntimeError('owned multi-host GPU business failure')
    return {'n':row['n'],'cpu_before':row['cpu_before'],'cpu_before_host':row['cpu_before_host'],'gpu':proof}


@pytest.mark.parametrize('mode',['normal','failure','cancel'])
def test_multi_host_six_real_gpu_grants_and_cleanup(tmp_path,monkeypatch,mode):
    if os.environ.get('DEMIFLOW_PHYSICAL_TWO_HOST_GPU')!='1':
        pytest.skip('requires explicitly owned two-physical-host six-GPU cluster')
    import ray
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from demiflow.resources.device_pool import DevicePool
    from runtime_v3_support import _RayDataBlocker
    assert ray.is_initialized() and ray.__file__
    nodes=[n for n in ray.nodes() if n['Alive']]
    assert len(nodes)==2 and len({n['NodeManagerHostname'] for n in nodes})==2
    assert sorted(n['Resources'].get('GPU',0) for n in nodes)==[2,4]
    shared=Path(os.environ['DEMIFLOW_PHYSICAL_GPU_ROOT']).resolve()
    owner=json.loads((shared/'owner.json').read_text())
    assert owner['root']==str(shared) and owner['purpose']=='demiflow-physical-host-gpu-validation'
    assert shared.parent==Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/physical-host-tests')
    root=shared/mode;root.mkdir();(root/'ready').mkdir()
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',str(tmp_path/'capacity'))
    inventory=json.loads(Path(os.environ['DEMIFLOW_DEVICE_INVENTORY']).read_text())
    pool=DevicePool(tmp_path/'capacity/devices',inventory={'devices':[d for h in inventory['hosts'] for d in h['devices']]})
    failures=[];observed=[];finished=threading.Event()
    def inspect_original(run):
        try:
            deadline=time.monotonic()+90
            while len(list((root/'ready').glob('*.json')))<6:
                if finished.wait(.02):
                    raise AssertionError('Pipeline ended before six actual GPU callbacks')
                assert time.monotonic()<deadline
            rows=[json.loads(p.read_text()) for p in (root/'ready').glob('*.json')]
            assert len({r['hostname'] for r in rows})==2
            assert len({r['binding']['devices'][0]['uuid'] for r in rows})==6
            snapshot=pool.snapshot();assert len(snapshot['active'])==6
            active={a['allocation_id']:a for a in snapshot['active']}
            with run.journal._connection() as db:
                allocations={r[0]:json.loads(r[1]) for r in db.execute(
                    "SELECT allocation_id,document FROM resource_allocations WHERE cleanup!='CONFIRMED'")}
            by_actor={a['placement']['actor_id']:(ident,a) for ident,a in allocations.items() if a.get('placement')}
            for r in rows:
                ident,allocation=by_actor[r['actor_id']]
                assert active[ident]['devices'][0]['device_id']==r['binding']['devices'][0]['uuid']
                assert allocation['placement']['device_ids']==[r['binding']['devices'][0]['uuid']]
                assert allocation['cleanup']=='NONE'
            observed.append({'rows':rows,'pool':snapshot,'allocations':allocations})
            if mode=='cancel':RunContext.cancel(run.ref)
        except BaseException as exc:
            failures.append(exc)
        finally:(root/'gate').write_text('original grants verified')
    thread=None
    def execute():
        nonlocal thread
        with RunContext.start(root/'run',limits={'timeout_s':150,'memory_bytes':16*1024**3,
                'max_row_bytes':64*1024,'max_batch_bytes':256*1024,'checkpoint_interval_ms':60000}) as run:
            thread=threading.Thread(target=inspect_original,args=(run,),daemon=True);thread.start()
            source=data.from_items([{'n':0,'mode':mode,'ready':str(root/'ready'),'gate':str(root/'gate')}]).map(_before)
            branches=[source.map(_cuda,fn_kwargs={'lane':lane},backend_options={'ray':{'worker_resources':{
                'gpu':1,'memory_bytes':2*1024**3}}}) for lane in range(6)]
            result=branches[0].union(*branches[1:]).map(_after).take_all()
            assert mode=='normal' and len(result)==6 and {r['gpu']['lane'] for r in result}==set(range(6))
            assert len({r['cpu_before'] for r in result})==1
            assert all((r['cpu_before_host'],r['cpu_before'])!=(r['gpu']['hostname'],r['gpu']['pid'])
                       !=(r['cpu_after_host'],r['cpu_after']) for r in result)
            assert run.journal.execution_commits('action-1')[-1]['status']=='acked'
    executor=RayDatasetExecutor()
    try:
        with _RayDataBlocker() as blocker,_use_executor(executor):
            if mode=='normal':execute()
            else:
                with pytest.raises(Exception,match=('owned multi-host GPU business failure' if mode=='failure' else '(?i)cancel|stopping')):
                    execute()
            assert not blocker.attempts
    finally:
        finished.set()
        if thread is not None:thread.join(5);assert not thread.is_alive()
        executor.close()
    assert not failures,failures
    assert len(observed)==1 and not pool.snapshot()['active']
    records=[json.loads(p.read_text()) for p in (tmp_path/'capacity/devices/allocations').glob('*.json')]
    assert len(records)==6 and all(r['state']=='RELEASED' and r['release_evidence'] for r in records)
    evidence=Path(os.environ['DEMIFLOW_PHYSICAL_HOSTS_EVIDENCE'])
    (evidence/('gpu-'+mode+'.json')).write_text(json.dumps({'simultaneous':observed,'released':records},indent=2))


def test_legacy_numeric_inventory_uses_default_ray_visibility(tmp_path,monkeypatch):
    if os.environ.get('DEMIFLOW_PHYSICAL_GPU')!='1':
        pytest.skip('requires explicit real-GPU execution')
    import ray
    import subprocess
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from demiflow.resources.device_pool import DevicePool
    assert not ray.is_initialized()
    monkeypatch.delenv('CUDA_DEVICE_ORDER',raising=False);monkeypatch.delenv('CUDA_VISIBLE_DEVICES',raising=False)
    hardware=subprocess.check_output(['nvidia-smi','--query-gpu=uuid,memory.total','--format=csv,noheader,nounits'],text=True,timeout=5)
    devices=[{'device_id':str(i),'type':'gpu','memory_bytes':int(line.split(',')[1])*1024**2}
             for i,line in enumerate(hardware.splitlines())]
    inventory=tmp_path/'legacy.json';inventory.write_text(json.dumps({'devices':devices}))
    monkeypatch.setenv('DEMIFLOW_DEVICE_INVENTORY',str(inventory));monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',str(tmp_path/'capacity'))
    try:
        ray.init(address='local',num_cpus=6,num_gpus=len(devices),include_dashboard=False,
                 object_store_memory=128*1024**2,_temp_dir=tempfile.mkdtemp(prefix='demiflow-numeric-gpu-'))
        with RunContext.start(tmp_path/'run',limits={'timeout_s':90,'memory_bytes':4*1024**3}),_use_executor(RayDatasetExecutor()):
            rows=data.from_items([{'n':0}]).map(_before).map(_cuda,backend_options={'ray':{'worker_resources':{
                'gpu':1,'memory_bytes':2*1024**3}}}).map(_after).take_all()
        assert len(rows)==1 and rows[0]['gpu']['visible'].isdecimal()
        assert rows[0]['gpu']['binding']['devices'][0]['uuid'] in hardware
        pool=DevicePool(tmp_path/'capacity/devices',inventory={'devices':devices});assert not pool.snapshot()['active']
        records=[json.loads(p.read_text()) for p in (tmp_path/'capacity/devices/allocations').glob('*.json')]
        assert len(records)==1 and records[0]['devices'][0]['device_id']==rows[0]['gpu']['visible']
    finally:ray.shutdown()
