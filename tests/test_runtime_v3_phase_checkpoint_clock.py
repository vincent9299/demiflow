"""Idle host preparation must not consume a fresh durable business interval."""
from types import SimpleNamespace as NS

import pytest

from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.contracts.dataflow_errors import DataflowError
from test_runtime_v3_checkpoint_progress import execution


def driver(monkeypatch,*,damage=None):
    clock=[100.0]
    monkeypatch.setattr('demiflow.execution.dataflow.time.monotonic',lambda:clock[0])
    monkeypatch.setattr('demiflow.execution.dataflow.time.sleep',lambda _:None)
    ex=execution({('source','p0'):23},source_rows=23)
    ex._checkpoint_at=100.0
    ex._policy=NS(checkpoint_rows=64,checkpoint_interval_ms=1000,provider='thread',
        native_limits=None,control_timeout_ms=30000)
    image=NS(cut=NS(final=False))
    ex._restore_image=image;ex._checkpoint=NS(_last_image=image)
    ex._active_worker_allocations=set();ex._by_partition={};ex._lease_by_operation={}
    ex._endpoints={};ex._durable_residency=NS(launched=lambda:None)
    ex._phases=[('stage',)];ex._phase_index=0
    stage=NS(stage_id='stage',partitions=('p0',))
    ex._stage_order=lambda:[stage]
    ex._initial_work=lambda stage,partition:NS(operation_id='operation')
    ex._place_hosts=lambda pending:{'operation':'host'}
    ex._physical=NS(exchanges=());ex._finite_seals={}
    ex._prepare_launch=lambda *args:None
    ex.activations=[]
    endpoint=NS(poll=lambda **kwargs:[],send_business=ex.sent.append,
        _assignment=NS(source_splits=('p0',)))
    def place(work,*,prepare,host_id):
        clock[0]+=2.4 # real red host preparation is longer than the 1s interval
        if damage=='admission_failure':raise DataflowError('RESOURCE_INFEASIBLE','owned failure')
        ex._endpoints['new']=endpoint
        if damage=='dirty_during_prepare':ex._node_metrics[('source','p0')]['rows_output']+=1
        if damage=='activated_during_prepare':ex._active_worker_allocations.add('unexpected')
        return NS(allocation_id='new')
    ex._scheduler=NS(place=place)
    def activate(lease,stage):
        ex.activations.append((clock[0],ex._checkpoint_at))
        ex._active_worker_allocations.add(lease.allocation_id)
        ex._by_partition['p0']=endpoint
    ex._activate=activate
    if damage=='dirty':ex._node_metrics[('source','p0')]['rows_output']+=1
    if damage=='old_endpoint':ex._endpoints['old']=endpoint
    if damage=='old_active':ex._active_worker_allocations.add('old')
    if damage=='old_partition':ex._by_partition['old']=endpoint
    if damage=='old_lease':ex._lease_by_operation['old']=object()
    if damage=='unacknowledged':ex._checkpoint._last_image=None
    if damage=='initial_start':ex._started=False
    return ex,clock


def test_acked_retired_handoff_starts_original_interval_before_first_activation(monkeypatch):
    ex,clock=driver(monkeypatch)
    ex._launch_phase()
    assert ex.activations==[(102.4,102.4)]
    assert ex._last_checkpoint_rows==23 and ex._checkpoint_deliveries==0
    ex._node_metrics[('source','p0')]['rows_output']=24
    clock[0]=102.5;ex.pump(deadline_ms=1)
    assert not ex._checkpoint_requested
    clock[0]=103.401;ex.pump(deadline_ms=1)
    assert ex._checkpoint_requested and ex.sent==[{'kind':'pause'}]
    assert ex._policy.checkpoint_interval_ms==1000
    assert ex._last_checkpoint_rows==23 # time never acknowledges a row


@pytest.mark.parametrize('damage',['dirty','old_endpoint','old_active','old_partition',
    'old_lease','unacknowledged','initial_start','dirty_during_prepare','activated_during_prepare'])
def test_uncommitted_or_mixed_ownership_never_resets_checkpoint_clock(monkeypatch,damage):
    ex,clock=driver(monkeypatch,damage=damage)
    ex._launch_phase()
    assert ex._checkpoint_at==100.0
    assert ex.activations==[(102.4,100.0)]


def test_failed_admission_cannot_extend_checkpoint_or_global_deadline(monkeypatch):
    ex,clock=driver(monkeypatch,damage='admission_failure')
    with pytest.raises(DataflowError,match='owned failure'):ex._launch_phase()
    assert ex._checkpoint_at==100.0 and not ex.activations


def test_actor_activation_time_is_never_excluded_by_later_ready_workers(monkeypatch):
    ex,clock=driver(monkeypatch)
    stage=ex._stage_order()[0];stage.partitions=('p0','p1')
    ex._initial_work=lambda stage,partition:NS(operation_id=partition)
    ex._place_hosts=lambda pending:{work.operation_id:'host' for _,work in pending}
    place=ex._scheduler.place
    def preparing(work,**kwargs):
        place(work,**kwargs)
        ex._endpoints[work.operation_id]=ex._endpoints.pop('new')
        return NS(allocation_id=work.operation_id)
    ex._scheduler.place=preparing
    def activate(lease,stage):
        ex.activations.append((clock[0],ex._checkpoint_at))
        ex._active_worker_allocations.add(lease.allocation_id)
        clock[0]+=.6
    ex._activate=activate
    ex._launch_phase()
    assert ex.activations[0][0]==pytest.approx(104.8)
    assert ex.activations[1][0]==pytest.approx(105.4)
    assert all(epoch==pytest.approx(104.8) for _,epoch in ex.activations)
