"""Recovery rows are original actor inputs, with bounded recoverable cursors."""
import json
from pathlib import Path
import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext


class RecoveryActor:
    def __init__(self, log, rows=2):
        self.log, self.pending, self.calls = str(log), list(range(101, 101+rows)), 0

    def event(self, name, **fields):
        with open(self.log, 'a') as stream:
            stream.write(json.dumps({'event':name, **fields})+'\n')

    def snapshot_state(self):
        return {'pending':self.pending, 'calls':self.calls}

    def restore_state(self, value):
        self.pending, self.calls = value['pending'], value['calls']
        self.event('restore', calls=self.calls)

    def recovery_rows(self):
        self.event('recover')
        pending, self.pending = self.pending, []
        return ({'id':value, 'visits':0} for value in pending)

    async def __call__(self, row):
        self.calls += 1
        self.event('call', id=row['id'])
        return {**row, 'visits':row.get('visits',0)+1}


@pytest.mark.parametrize('shared', [False, True])
def test_recovery_rows_are_processed_by_original_actor(tmp_path, shared):
    actor = RecoveryActor(tmp_path/'events')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':1,
            'max_row_bytes':1024**2, 'max_batch_bytes':1024**2}):
        ds = data.from_items([]).map_async(actor)
        if shared:
            ds = ds.map_async(actor)
        result = ds.materialize().take_all()
    assert result == [{'id':101,'visits':2 if shared else 1}, {'id':102,'visits':2 if shared else 1}]
    events=[json.loads(v) for v in (tmp_path/'events').read_text().splitlines()]
    assert sum(v['event']=='recover' for v in events)==1
    assert sum(v['event']=='call' for v in events)==(4 if shared else 2)


class ExpandingRecoveryActor(RecoveryActor):
    async def __call__(self, row):
        import asyncio
        await asyncio.sleep(.02)
        await super().__call__(row)
        return [{**row, 'part':i} for i in range(3)]


def test_recovery_output_prefix_resumes_without_reinvoking_actor(tmp_path, monkeypatch):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    original = DataflowCheckpointCoordinator.commit
    stopped=[]
    confirmed=[]
    def crash(coordinator, *args, **kwargs):
        image=original(coordinator, *args, **kwargs)
        fact=coordinator._action.context.journal.execution_commit(image.commit_id)
        receipt=fact['receipts'].get('materialize')
        rows=receipt['data']['rows'] if receipt else 0
        if not stopped and 0 < rows < 30 and not image.cut.final:
            from demiflow.execution.kernels import state_codec
            for contribution in image.contributions:
                for ref in contribution.operator_states.values():
                    if ref.media_type != 'application/x-demiflow-kernel-image-v3':
                        continue
                    raw=b''.join(coordinator._store.read(ref,max_bytes=coordinator._physical.bounds.max_state_bytes))
                    value=state_codec.loads(raw,max_bytes=coordinator._physical.bounds.max_state_bytes)
                    recovery=value.get('recovery_rows')
                    if recovery and 0 < recovery['output_cursor'] < recovery['outputs']['count']:
                        assert value['state']['value']['calls']==recovery['input_cursor']
                        confirmed.append(recovery['input_cursor'])
            if confirmed:
                stopped.append(image.commit_id)
                raise RuntimeError('recover-output-prefix-crash')
        return image
    monkeypatch.setattr(DataflowCheckpointCoordinator, 'commit', crash)
    limits={'checkpoint_interval_ms':1, 'checkpoint_rows':1,
            'max_row_bytes':1024**2, 'max_batch_bytes':1024**2}
    with pytest.raises(RuntimeError, match='recover-output-prefix-crash'):
        with RunContext.start(tmp_path/'run', limits=limits) as run:
            data.from_items([]).map_async(ExpandingRecoveryActor(tmp_path/'events',rows=10)).materialize()
    assert stopped
    with RunContext.resume(run.ref):
        rows=data.from_items([]).map_async(ExpandingRecoveryActor(tmp_path/'events',rows=10)).materialize().take_all()
    assert rows==[{'id':i,'visits':0,'part':part} for i in range(101,111) for part in range(3)]
    events=[json.loads(v) for v in (tmp_path/'events').read_text().splitlines()]
    assert sum(v['event']=='recover' for v in events)==1
    assert len(confirmed)==1 and 0 < confirmed[0] < 10
    boundary=101+confirmed[0]
    # Publication overlaps later pure work. Only this immutable ACK prefix
    # (including the fully staged callback whose output is partially emitted)
    # forbids reinvocation. Unconfirmed suffix work may replay on process loss.
    assert sorted(v['id'] for v in events if v['event']=='call' and v['id']<boundary)==list(range(101,boundary))
    restored=[i for i,v in enumerate(events) if v['event']=='restore']
    assert len(restored)==1 and events[restored[0]]['calls']==confirmed[0]
    assert sorted(v['id'] for v in events[restored[0]+1:] if v['event']=='call')==list(range(boundary,111))


class TimedSharedRecovery(RecoveryActor):
    async def __call__(self, row):
        import asyncio
        await asyncio.sleep(.005)
        return await super().__call__(row)


async def batch_rows(rows):
    return rows


def test_shared_recovery_waits_live_drain_before_timed_batch_tail(tmp_path):
    actor=TimedSharedRecovery(tmp_path/'events')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':2,
            'max_row_bytes':1024**2,'max_batch_bytes':1024**2}):
        rows=(data.from_items([{'id':i,'visits':0} for i in range(4)])
            .map_async(actor,concurrency=2).map_async(actor,concurrency=2)
            .batch_map(batch_rows,max_batch=3,concurrency=1,flush_interval=.01)
            .materialize().take_all())
    assert sorted(rows,key=lambda r:r['id'])==[
        {'id':i,'visits':2} for i in [0,1,2,3,101,102]]
    events=[json.loads(v) for v in (tmp_path/'events').read_text().splitlines()]
    recovery=next(i for i,v in enumerate(events) if v['event']=='recover')
    assert sorted(v['id'] for v in events[:recovery] if v['event']=='call')==[0,0,1,1,2,2,3,3]


class OversizedRecovery(RecoveryActor):
    def recovery_rows(self):
        return [{'id':1,'payload':'x'*4096}]


class InfiniteRecovery(RecoveryActor):
    def recovery_rows(self):
        index=0
        while True:
            yield {'id':index}
            index+=1


@pytest.mark.parametrize('case', ['row', 'count'])
def test_recovery_rows_reject_original_budget_before_actor_call(tmp_path, case):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    actor=(OversizedRecovery if case=='row' else InfiniteRecovery)(tmp_path/'events')
    limits={'max_row_bytes':1024,'max_batch_bytes':4096,'max_output_rows':3}
    with pytest.raises(DataflowError) as error:
        with RunContext.start(tmp_path/'run',limits=limits):
            data.from_items([]).map_async(actor).materialize()
    assert error.value.code=='BUDGET_EXCEEDED'
    events=[json.loads(v) for v in (tmp_path/'events').read_text().splitlines()] if (tmp_path/'events').exists() else []
    assert not [v for v in events if v['event']=='call']


def test_shared_recovery_across_finite_stage_rejects_before_business_start(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    actor=RecoveryActor(tmp_path/'events')
    with pytest.raises(DataflowError,match='Shared recovery_rows cannot cross a finite-input stage') as error:
        with RunContext.start(tmp_path/'run'):
            data.from_items([]).map_async(actor).sort('id').map_async(actor).materialize()
    assert error.value.code=='UNSUPPORTED_CAPABILITY'
    assert not (tmp_path/'events').exists()


def test_shared_recovery_branch_does_not_wait_for_unrelated_batch_tail(tmp_path):
    actor=TimedSharedRecovery(tmp_path/'events')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':2,
            'max_row_bytes':1024**2,'max_batch_bytes':1024**2}), \
            data.local_execution(workers=6,partitions=1):
        base=data.from_items([{'id':i,'visits':0} for i in range(2)]).map_async(actor,concurrency=2)
        left=base.map_async(actor,concurrency=2)
        right=base.batch_map(batch_rows,max_batch=3,concurrency=1,flush_interval=.01)
        rows=left.union(right).materialize().take_all()
    assert sorted((r['id'],r['visits']) for r in rows)==[(i,v) for i in [0,1,101,102] for v in [1,2]]
    events=[json.loads(v) for v in (tmp_path/'events').read_text().splitlines()]
    recovery=next(i for i,v in enumerate(events) if v['event']=='recover')
    assert sorted(v['id'] for v in events[:recovery] if v['event']=='call')==[0,0,1,1]


from test_dataflow_shared_actor import owned_ray


def test_real_ray_shared_recovery_rows(tmp_path, owned_ray):
    actor=RecoveryActor(tmp_path/'events')
    with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':1,
            'max_row_bytes':1024**2,'max_batch_bytes':1024**2}):
        rows=owned_ray.from_items([]).map_async(actor).map_async(actor).materialize().take_all()
    assert rows==[{'id':101,'visits':2},{'id':102,'visits':2}]
    events=[json.loads(v) for v in (tmp_path/'events').read_text().splitlines()]
    assert sum(v['event']=='recover' for v in events)==1
    assert sum(v['event']=='call' for v in events)==4
