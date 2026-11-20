"""Opt-in two-physical-host proof on an explicitly owned private Ray cluster."""
import json
import os
from pathlib import Path
import socket

import pytest

from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext


def _tag_host(row):
    import ray
    import socket
    return {**row, 'worker_host': socket.gethostname(),
            'worker_node': ray.get_runtime_context().get_node_id()}


def _left(row):
    return {**row, 'branch':'left'}


def _right(row):
    return {**row, 'branch':'right'}


@pytest.fixture(scope='module')
def physical_hosts():
    root = os.environ.get('DEMIFLOW_PHYSICAL_HOSTS_ROOT')
    if root is None:
        pytest.skip('requires an explicitly owned two-physical-host Ray test cluster and shared run directory')
    import ray
    from runtime_v3_support import _RayDataBlocker
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    assert ray.is_initialized()
    nodes = [node for node in ray.nodes() if node['Alive']]
    assert len(nodes) == 2
    assert len({node['NodeManagerHostname'] for node in nodes}) == 2
    assert all(node['Resources'].get('GPU',0) == 0 for node in nodes)
    root = Path(root).resolve()
    owner = json.loads((root/'owner.json').read_text())
    assert owner['root'] == str(root) and owner['purpose'] == 'demiflow-physical-host-cpu-validation'
    assert root.parent == Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/physical-host-tests')
    executor = RayDatasetExecutor()
    try:
        with _RayDataBlocker() as blocker, _use_executor(executor):
            yield root, {node['NodeID']:node for node in nodes}
            assert not blocker.attempts
    finally:
        executor.close()


@pytest.fixture
def observed(monkeypatch, physical_hosts, request):
    import ray
    root,nodes = physical_hosts
    facts = {'name':request.node.name,'phases':[],'cuts':[]}
    launch = DataflowExecution._launch_phase
    advance = DataflowExecution._advance_cut
    def on_launch(execution):
        result = launch(execution)
        assert execution._policy.provider == 'ray_core'
        placements = []
        for allocation,endpoint in execution._endpoints.items():
            lease = execution._scheduler.leases[allocation]
            actor = endpoint._connector._actor(allocation)
            info = ray._private.state.actors(actor._actor_id.hex())
            actual = info['Address']['NodeID']
            assert actual == lease.host_id.removeprefix('ray-')
            assert actual in nodes
            placements.append({'allocation_id':allocation,'boot_id':lease.boot_id,
                'stage_id':lease.stage_id,'node_id':actual,'hostname':nodes[actual]['NodeManagerHostname'],
                'actor_id':actor._actor_id.hex(),'actor_state':info['State'],'pid':info.get('Pid')})
        facts['phases'].append({'attempt':execution._attempt(),'plan_id':execution._physical.plan_id,
                               'placements':placements})
        return result
    def on_advance(execution):
        result = advance(execution)
        image = execution._restore_image
        if image is not None and all(c['commit_id'] != image.commit_id for c in facts['cuts']):
            fact = execution._action.context.journal.execution_commit(image.commit_id)
            assert fact['status'] == 'acked'
            facts['cuts'].append({'commit_id':image.commit_id,'cut_id':image.cut.cut_id,
                'final':image.cut.final,'attempt':execution._attempt(),'status':fact['status'],
                'rows':execution._publication(image)['manifest']['total_rows']})
        return result
    monkeypatch.setattr(DataflowExecution,'_launch_phase',on_launch)
    monkeypatch.setattr(DataflowExecution,'_advance_cut',on_advance)
    yield facts
    evidence=Path(os.environ['DEMIFLOW_PHYSICAL_HOSTS_EVIDENCE']);evidence.mkdir(exist_ok=True)
    (evidence/(request.node.name+'.json')).write_text(json.dumps(facts,indent=2))


def test_unshared_tmp_domain_is_rejected_before_execution(tmp_path, physical_hosts, observed):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError,match='cannot prove access'):
        with RunContext.start(tmp_path/'run',limits={'timeout_s':30}):
            data.range(1).take_all()
    assert not observed['phases'] and not observed['cuts']


def test_dataset_cross_host_source_transform_sink_ack(physical_hosts, observed):
    import lance
    root,_=physical_hosts
    target=root/'normal-output.lance'
    with RunContext.start(root/'normal-run',limits={'timeout_s':60}) as run:
        receipt=data.range(12).map(_tag_host).write_lance(str(target),mode='create',return_receipt=True)
        commits=run.journal.execution_commits('action-1')
        assert commits and commits[-1]['status']=='acked'
        observed['receipt']=receipt.to_dict()
    rows=lance.dataset(str(target)).to_table().to_pylist()
    assert sorted(row['id'] for row in rows)==list(range(12))
    actual={p['node_id'] for phase in observed['phases'] for p in phase['placements']}
    assert len(actual)==2
    assert all(row['worker_node'] in actual for row in rows)
    observed['rows']=rows
    assert observed['cuts'][-1]['final']


def test_owned_remote_actor_loss_resumes_original_cut(physical_hosts, observed, monkeypatch):
    import lance
    import ray
    root,nodes=physical_hosts
    target=root/'recovered-output.lance'
    original=DataflowExecution._advance_cut
    killed=[]
    class InjectedActorLoss(Exception):
        pass
    def fail_after_ack(execution):
        result=original(execution)
        image=execution._restore_image
        if image is not None and not image.cut.final and not killed:
            fact=execution._action.context.journal.execution_commit(image.commit_id)
            assert fact['status']=='acked'
            remote=[(allocation,endpoint) for allocation,endpoint in execution._endpoints.items()
                    if nodes[execution._scheduler.leases[allocation].host_id.removeprefix('ray-')]['NodeManagerHostname'] != socket.gethostname()]
            assert remote
            allocation,endpoint=remote[0]
            actor=endpoint._connector._actor(allocation)
            killed.append({'actor_id':actor._actor_id.hex(),'allocation_id':allocation,
                           'commit_id':image.commit_id,'plan_id':execution._physical.plan_id})
            ray.kill(actor,no_restart=True)
            raise InjectedActorLoss('only the test-owned remote actor was killed after a real ACK')
        return result
    with pytest.raises(InjectedActorLoss):
        with RunContext.start(root/'recovery-run',limits={'timeout_s':90,
                'checkpoint_rows':4,'checkpoint_interval_ms':60000}) as run:
            ref=run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution,'_advance_cut',fail_after_ack)
                data.range(12).map(_tag_host).write_lance(str(target),mode='create',return_receipt=True)
    assert len(killed)==1
    with RunContext.resume(ref) as run:
        before=run.journal.execution_commits('action-1')
        assert before and before[-1]['status']=='acked'
        receipt=data.range(12).map(_tag_host).write_lance(str(target),mode='create',return_receipt=True)
        after=run.journal.execution_commits('action-1')
        assert after[:len(before)]==before
        assert after[-1]['status']=='acked'
    rows=lance.dataset(str(target)).to_table().to_pylist()
    assert sorted(row['id'] for row in rows)==list(range(12))
    assert len({phase['plan_id'] for phase in observed['phases']})==1
    assert len({phase['attempt'] for phase in observed['phases']})==2
    assert observed['cuts'][-1]['final']
    observed.update(killed=killed,rows=rows,receipt=receipt.to_dict())


def test_shared_source_union_routes_preserve_multiplicity(physical_hosts, observed):
    from collections import Counter
    root,_=physical_hosts
    with RunContext.start(root/'union-run',limits={'timeout_s':90}) as run:
        source=data.range(8)
        rows=source.map(_left).union(source.map(_right)).repartition(3).take_all()
        assert Counter((r['id'],r['branch']) for r in rows)==Counter((i,b) for i in range(8) for b in ('left','right'))
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'
    assert len({p['node_id'] for phase in observed['phases'] for p in phase['placements']})==2
    observed['rows']=rows
