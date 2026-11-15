"""Checked values are validated at actual calls, retaining original owner cleanup."""
from dataclasses import replace
from pathlib import Path
import pytest
from demiflow import data
from demiflow.data.plan import LogicalPlan
from demiflow.data.flow_contract import scalar,field
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.run_context import RunContext


def checked(ds,*,batch=False):
    row={**scalar('record'),'fields':{'x':field(scalar('int'))}}
    value={**scalar('list'),'item':row} if batch else row
    contract={'label':'real-row-boundary','parameters':[{'name':'rows' if batch else 'row','type':value}],
        'returns':value,'max_row_bytes':1024*1024,'max_input_bytes':1024*1024,'max_return_bytes':1024*1024}
    op=ds._plan.operations[-1]
    op=replace(op,callable=replace(op.callable,runtime_contract=contract))
    return data.Dataset(ds._source,LogicalPlan((*ds._plan.operations[:-1],op)),ds._executor)


@pytest.mark.parametrize('kind',['sync','await','batch'])
@pytest.mark.parametrize('invalid',['input','output'])
def test_real_call_rejects_wrong_value_before_sink_commit(tmp_path,kind,invalid):
    calls=tmp_path/'calls'
    def result(row):
        with calls.open('a') as f:f.write('called\n')
        return {'x':'wrong'} if invalid=='output' else row
    async def awaiting(row):return result(row)
    def batch(rows):return [result(row) for row in rows]
    rows=data.from_items([{'x':'wrong' if invalid=='input' else 1}])
    ds=rows.map(result) if kind=='sync' else rows.map_async(awaiting,concurrency=1) if kind=='await' else rows.batch_map(batch,max_batch=2)
    ds=checked(ds,batch=kind=='batch')
    target=tmp_path/'out.lance'
    with pytest.raises(RuntimeContractError,match='expected int'):
        with RunContext.start(tmp_path/'run'),data.local_execution(workers=4):
            ds.write_lance(str(target),mode='create')
    assert not target.exists()
    assert calls.exists()==(invalid=='output')


def test_original_actor_state_owner_is_closed_after_output_rejection(tmp_path):
    marker=str(tmp_path/'lifecycle')
    class Actor:
        def __init__(self):self.marker=marker
        async def astart(self):
            with open(self.marker,'a') as f:f.write('start\n')
        async def __call__(self,row):return {'x':'wrong'}
        async def aclose(self):
            with open(self.marker,'a') as f:f.write('close\n')
    ds=checked(data.from_items([{'x':1}]).map_async(Actor,concurrency=1))
    with pytest.raises(RuntimeContractError,match='expected int'):
        with RunContext.start(tmp_path/'run'),data.local_execution(workers=4):ds.take_all()
    assert Path(marker).read_text().splitlines()==['start','close']


def test_checked_bound_call_arguments_and_unchecked_compatibility(tmp_path):
    def adjusted(row,amount=1):return {'x':row['x']+amount}
    ds=checked(data.from_items([{'x':1}]).map(adjusted,fn_kwargs={'amount':2}))
    with RunContext.start(tmp_path/'run'),data.local_execution(workers=4):
        assert ds.take_all()==[{'x':3}]
        assert data.from_items([{'x':1}]).map(lambda row:{'x':'allowed-without-a-contract'}).take_all()==[{'x':'allowed-without-a-contract'}]
