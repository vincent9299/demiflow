"""Managed resource contracts and opt-in real CUDA, original authority only."""
import copy
import json
import os
from pathlib import Path

import pytest

from demiflow import data
from demiflow.data.native_options import parse_native_options, resource_only_options
from demiflow.planning.traits import check_call
from demiflow.execution.seal.plan_facts import operation_facts
from demiflow.forge.schema import ForgeError
from demiflow.workbench.runs import manager as runs, native_resources
from test_forge_runs import base, root, custom, execute
from test_forge_markdown_reading import browser


def identity(row):
    return row


@pytest.mark.parametrize('raw', [
    {'compute': {'kind': 'task_pool', 'size': 1}}, {'tasks': {'min': 1}}, {'writers': {'min': 1}},
    {'surprise': 1}, {'worker_resources': {'gpu': .5}}, {'worker_resources': {'cpu': 0}},
    {'worker_resources': {'memory_bytes': 0}}, {'worker_resources': {'gpu': True}},
])
def test_resource_only_contract_rejects_legacy_or_invalid_controls(raw):
    with pytest.raises((ValueError, TypeError)):
        resource_only_options({'ray': raw})


def test_resource_declaration_binds_actual_options_without_changing_old_facts():
    old = operation_facts(data.from_items([1]).map(identity)._plan.operations[0])
    assert old == {'kind':'map','driver':'pull','native_options':False,'stream_options':False,
                   'bound':False,'arguments':{'inputs':None,'output':None,'outputs':None}}
    options = {'ray': {'worker_resources': {'gpu':1,'cpu':.5,'memory_bytes':1024**3}}}
    op = data.from_items([1]).map(identity, backend_options=options)._plan.operations[0]
    actual = operation_facts(op)
    assert actual['native_options'] is True
    assert actual['resource_declaration'] == resource_only_options(options) == resource_only_options(parse_native_options(options,family='row_transform'))
    assert check_call(actual)['supported']
    for bad in (None, {'gpu':.5}, {'unregistered':1}):
        assert not check_call({**actual,'resource_declaration':bad})['supported']
    from demiflow.execution.pipeline.checked import CheckedExecution
    from demiflow.data.runtime_contract import RuntimeContractError
    context=CheckedExecution.__new__(CheckedExecution)
    node={'id':'changed-resource','capability_fact':{**actual,'resource_declaration':{'gpu':2}}}
    with pytest.raises(RuntimeContractError, match='resource_declaration'):
        context._operation(op,node)
    legacy=data.from_items([1]).map(identity,backend_options={'ray':{'compute':{'kind':'task_pool','size':1}}})._plan.operations[0]
    with pytest.raises(RuntimeContractError,match='Backend option facts require a supported native contract'):
        operation_facts(legacy)


def deployment(tmp_path):
    return dict(format=native_resources.FORMAT, worker_mode='process', resource_root=str(tmp_path/'shared'),
        device_inventory={'devices':[{'device_id':'gpu-test','type':'gpu','memory_bytes':8*1024**3,'max_residents':1}]},
        virtual_address_bytes=64*1024**3)


def test_sealed_inventory_shared_domain_and_cpu_environment(tmp_path, monkeypatch):
    value=deployment(tmp_path)
    targets=[tmp_path/'run-a',tmp_path/'run-b']
    for target in targets:
        target.mkdir();native_resources.seal(target,value)
        assert native_resources.verify(target,value)==value
        env=runs.worker_environment(target,native_execution=value)
        assert env['DEMIFLOW_RESOURCE_ROOT']==value['resource_root']
        assert env['CUDA_VISIBLE_DEVICES']==''
    monkeypatch.setenv('DEMIFLOW_DEVICE_INVENTORY','/untrusted/should-not-leak')
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT','/untrusted/should-not-leak')
    assert 'DEMIFLOW_DEVICE_INVENTORY' not in runs.worker_environment(targets[0])
    assert runs.worker_environment(targets[0])['CUDA_VISIBLE_DEVICES']==''
    path=targets[0]/native_resources.INVENTORY
    path.chmod(0o600); path.write_text('{}')
    with pytest.raises(ForgeError,match='inventory differs'):
        runs.worker_environment(targets[0],native_execution=value)
    from demiflow.workbench.runs.operations import OPERATIONS, worker_arguments
    args={'entry':'flows/a.py:run','params':'flows/a.json','native_deployment':value}
    with pytest.raises(ForgeError,match='unknown'):
        OPERATIONS['prepare'].validate(args)  # HTTP/public cannot choose a pool.
    assert worker_arguments('run_prepare',args)['native_deployment']==value
    invalid={**value,'virtual_address_bytes':True}
    with pytest.raises(ForgeError): native_resources.validate(invalid)


FLOW = '''from operators.custom import cpu_before, gpu, cpu_after
def run(*, source, target):
    d = data.read_lance(source['uri'], version=source['version'])
    d = d.map(cpu_before, inputs={'value':'case_id'}, output='before', backend_options={'ray':{'worker_resources':{'memory_bytes':1073741824}}})
    d = d.map(gpu, inputs={'value':'case_id'}, output='cuda', backend_options={'ray':{'worker_resources':{'gpu':1, 'memory_bytes':2147483648}}})
    d = d.map(cpu_after, inputs={'value':'cuda'}, output='after', backend_options={'ray':{'worker_resources':{'memory_bytes':1073741824}}})
    return d.write_lance(target['uri'], mode='create', return_receipt=True)
'''
OPERATORS = '''import torch

def cpu_before(value: str) -> int:
    return torch.cuda.device_count()

def gpu(value: str) -> list[int]:
    tensor = torch.arange(4096, dtype=torch.float32, device='cuda')
    return [torch.cuda.device_count(), int((tensor * 2).sum().item())]

def cpu_after(value: list[int]) -> int:
    return torch.cuda.device_count()
'''


def test_gpu_prepare_refuses_absent_deployment(root, monkeypatch):
    monkeypatch.delenv('DEMIFLOW_DEVICE_INVENTORY',raising=False)
    with pytest.raises(ForgeError,match='deployment inventory'):
        custom(root,FLOW,operators='\n'.join('def '+name+'(value: str) -> str:\n    return value\n'
                                           for name in ('cpu_before','gpu','cpu_after')))


def test_torch_prepare_requires_original_bound_extra(root, tmp_path, monkeypatch):
    value=deployment(tmp_path)
    inventory=tmp_path/'inventory.json';inventory.write_text(json.dumps(value['device_inventory']))
    monkeypatch.setenv('DEMIFLOW_DEVICE_INVENTORY',str(inventory))
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',value['resource_root'])
    monkeypatch.setenv('DEMIFLOW_WORKBENCH_GPU_VIRTUAL_BYTES',str(value['virtual_address_bytes']))
    with pytest.raises(ForgeError,match='outside the platform snapshot.*torch'):
        custom(root,FLOW,operators=OPERATORS)


def test_real_managed_cuda_plan_bridge_and_browser(root, tmp_path, monkeypatch, browser, record_property):
    if os.environ.get('DEMIFLOW_PHYSICAL_GPU')!='1':
        pytest.skip('explicit authorised physical GPU test required')
    import subprocess
    from demiflow.forge.platform import content_digest
    from demiflow.workbench.projects import workspace as ws
    from demiflow.workbench.graph import model
    from demiflow.execution.hosts.devices import device_pool
    from runtime_v3_product_scenarios import read_native_execution
    from test_forge_public_api import service
    from test_forge_workbench_browser import page_for
    from playwright.sync_api import expect
    query=['nvidia-smi','--query-gpu=index,uuid,memory.total,memory.used,memory.free,memory.reserved,utilization.gpu','--format=csv,noheader,nounits']
    raw=subprocess.check_output(query,text=True)
    index,uuid,memory,used,free,reserved,util=[v.strip() for v in raw.splitlines()[0].split(',')]
    assert index=='0' and int(util)==0 and int(free)>2048
    assert uuid not in subprocess.check_output(['nvidia-smi','--query-compute-apps=pid,gpu_uuid,used_gpu_memory','--format=csv,noheader,nounits'],text=True)
    inventory=tmp_path/'inventory.json'
    inventory.write_text(json.dumps({'devices':[{'device_id':uuid,'type':'gpu','memory_bytes':int(free)*1024**2,'max_residents':1}]}))
    monkeypatch.setenv('DEMIFLOW_DEVICE_INVENTORY',str(inventory))
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT',str(tmp_path/'capacity'))
    monkeypatch.setenv('DEMIFLOW_WORKBENCH_GPU_VIRTUAL_BYTES',str(64*1024**3))
    assert not device_pool('local').snapshot()['active']
    # This owned test deployment reserves both run RSS and the original
    # supervisor/preparation overhead. It never changes the shared policy.
    domain=Path(os.environ['DEMIFLOW_FORGE_DOMAIN']);domain.mkdir(exist_ok=True)
    (domain/'policy.json').write_text(json.dumps({'max_rss_bytes':20*1024**3}))
    control=ws.read_control(root/'workspace.json')
    ws.bind_platform(root,expected=control['platform']['digest'],extras=['gpu'])
    control=ws.read_control(root/'workspace.json')
    ws.set_budgets(root,{'memory_bytes':6*1024**3,'max_rss_bytes':16*1024**3,
        'max_row_bytes':65536,'max_batch_bytes':262144},expected=content_digest(control))
    value=custom(root,FLOW,operators=OPERATORS)
    manifest=runs.journal(root,value['run_id']).execution_view()['manifest']
    assert manifest['native_execution']['resource_root']==str(tmp_path/'capacity')
    assert manifest['native_execution']['worker_mode']=='process'
    assert 'gpu' in manifest['platform']['environment']['extras']
    assert any(d['name']=='torch' and d['status']=='installed'
               for d in manifest['platform']['environment']['dependencies'])
    result=execute(root,value)
    assert result['status']=='succeeded',result
    owner=next(p for p in root.parents if p.parent==Path('/tmp') and p.name.startswith('demiflow-'))
    proof=read_native_execution(root,result['run_id'],attempt=result['attempt'],owned_root=owner)
    rows=proof['outputs'][0]['rows'];assert len(rows)==3
    calls=[row['cuda'] for row in rows]
    assert all(row['before']==row['after']==0 and row['cuda']==[1,16773120] for row in rows)
    assert manifest['native_execution']['virtual_address_bytes']==64*1024**3
    snapshot=device_pool('local').snapshot();assert not snapshot['active'] and not snapshot['charged']
    allocations=[json.loads(p.read_text()) for p in (tmp_path/'capacity/devices/allocations').glob('*.json')]
    assert allocations and all(a['state']=='RELEASED' for a in allocations)
    wrong_environment=copy.deepcopy(manifest)
    next(d for d in wrong_environment['platform']['environment']['dependencies'] if d['name']=='torch')['version']='0.0'
    with pytest.raises(ForgeError,match='dependency metadata changed'):
        runs.verify_seal(runs.directory(root,result['run_id']),wrong_environment)

    # A second real run reaches the same original device ledger. Cancel while
    # its new device grant is charged, then require confirmed subtree exit and
    # released capacity. The first run above separately proves actual CUDA.
    import time
    from test_forge_runs import wait
    second=runs.prepare(root,'flows/custom.py:run',params='flows/custom.json')
    charged=None
    try:
        runs.start(root,second['run_id'],expected=second['digest'])
        deadline=time.monotonic()+45
        while time.monotonic()<deadline:
            charged=device_pool('local').snapshot()
            if charged['active']:
                break
            assert runs.show(root,second['run_id'])['status'] in runs.ACTIVE
            time.sleep(.05)
        assert charged and charged['active'],'actual second device grant was never observed'
        runs.cancel(root,second['run_id'])
        cancelled=wait(root,second['run_id'],timeout=30)
        assert cancelled['status']=='cancelled' and cancelled['state']['execution_stopped']
        assert cancelled['state']['cleanup']=='complete'
        assert not device_pool('local').snapshot()['active'] and not device_pool('local').snapshot()['charged']
        assert not any(o['result'] for o in cancelled['outputs'])
    finally:
        if runs.show(root,second['run_id'])['status'] in runs.ACTIVE:
            runs.cancel(root,second['run_id']);wait(root,second['run_id'],timeout=30)
    graph=model.get(root,basis_kind='run',run_id=result['run_id'],attempt=result['attempt'])
    entry=next(e for e in graph['logical']['actions'] if e.get('binding_level')=='sealed')
    report_id=next(n['id'] for n in graph['nodes'] if (n.get('operator') or {}).get('name')=='gpu')
    occurrence=next(e['occurrence_id'] for e in entry['identity_bridge']['bridge']['entries'] if e['report_node_id']==report_id)
    stage=entry['physical']['stages'][occurrence]
    assert stage['profile']['requests']['accelerators']==[{'type':'gpu','count':1}]
    with service(root,enable_runs=True) as app,page_for(browser,app) as (page,errors):
        # The original UI opens this exact historical native run, not a fixture graph.
        page.evaluate("view=>{location.hash=ForgeView.encodeView(view);}",
            {'v':2,'workspace':next(iter(app.single['projects'])),'kind':'run',
             'run_id':result['run_id'],'attempt':result['attempt']})
        page.reload()  # Ordinary share links are consumed on startup, not hashchange.
        expect(page.locator('.wb-basis')).to_contain_text('封存运行图')
        page.get_by_role('button',name='运行封存计划（E2）',exact=True).click()
        page.locator('.wb-node').filter(has_text='gpu').click()
        expect(page.locator('.wb-inspector')).to_contain_text('accelerators=')
        expect(page.locator('.wb-inspector')).to_contain_text('gpu')
        assert not errors,errors
        page.screenshot(path=str(tmp_path/'managed-gpu-display.png'),full_page=True)
    record_property('original_native_execution',proof)
    after_hardware=subprocess.check_output(query,text=True)
    assert uuid not in subprocess.check_output(['nvidia-smi','--query-compute-apps=pid,gpu_uuid,used_gpu_memory','--format=csv,noheader,nounits'],text=True)
    observed={'calls':calls,'profile':stage,'device_pool_after':snapshot,'allocations':allocations,
              'cancelled_run':cancelled,'charged_before_cancel':charged,
              'hardware_before':raw,'hardware_after':after_hardware}
    record_property('physical_cuda',observed)
    (tmp_path/'managed-gpu-evidence.json').write_text(json.dumps({'native':proof,'physical':observed},indent=2))
