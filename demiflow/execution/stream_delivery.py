"""Bounded native stream-to-iterator delivery, using the existing stream driver.

The terminal determines delivery-only semantics: objects and the small handoff
batch commit before yield, but user receipt is not replayed across attempts.
There is no selectable recovery mode or model-specific executor here.
"""
import asyncio
import contextvars
import queue
import threading

from ..data.runtime_contract import check_size
from .commit import CommitCoordinator
from .object_sink import ObjectRowsSink,image_object_specs
from .recovery_contract import RecoveryContractError,SourcePosition
from .recovery_identity import plan_identity
from .run_context import execution_scope
from .stream_materialize import stream_view


class _DeliveryStopped(Exception):
    pass


def iter_stream_rows(executor,source,plan,*,limit=None,action=None,execution=None):
    if limit is not None and (type(limit) is not int or limit<0):
        raise ValueError('Native delivery limit must be a nonnegative integer')
    from .queue_producer import producer_actor
    if producer_actor(plan) is not None:
        raise RecoveryContractError('Incremental enqueue requires run_stream; delivery terminals cannot publish tasks')
    if limit==0:return
    from contextlib import nullcontext
    from .payload_limits import payload_scope
    from .run_context import admit_stream,current_action
    from .recovery_identity import source_identity
    borrowed=action if action is not None else current_action()
    with execution_scope() as run, payload_scope(run,plan,arguments={'_retained_rows':limit or 0}):
        if borrowed is not None:
            if borrowed.context is not run:
                raise RecoveryContractError('Delivery action belongs to a different run context')
            declaration=borrowed.declaration
            if source_identity(source)!=declaration.get('source'):
                raise RecoveryContractError('Delivery action source differs from the terminal source')
            if len(plan.operations)!=len(declaration.get('operations',())):
                raise RecoveryContractError('Delivery action plan differs from the terminal plan')
            action_cm=nullcontext(borrowed);owns_complete=False
        else:
            action_cm=run.action(plan_identity('delivery',source,plan,arguments={'limit':limit}))
            owns_complete=True
        residency=admit_stream(plan)
        retained_bytes=0
        delivered_rows=0
        result_budget=run.limits['memory_bytes']-residency['reserved_bytes']-run.limits['max_row_bytes']
        executor._run_ref=run.ref
        with action_cm as action:
            sink=ObjectRowsSink(action,object_specs=image_object_specs(plan))
            action.object_sinks=sink.object_sinks
            coordinator=CommitCoordinator(action)
            handoff=queue.Queue(maxsize=1)
            stopped=threading.Event();done=threading.Event()
            state={'error':None,'rows':0,'handoff_rows':0,'sequence':0}
            transfer_context=contextvars.copy_context()
            batch=[];batch_bytes=0

            def send(value):
                while not stopped.is_set():
                    run.check_stop()
                    try:handoff.put(value,timeout=0.05);return
                    except queue.Full:continue
                raise _DeliveryStopped()

            def flush():
                nonlocal batch,batch_bytes
                if not batch:return
                state['sequence']+=1
                fact=coordinator.commit(state['sequence'],
                    outputs={'output':(sink,iter(batch))},
                    position=SourcePosition(action.semantic_id,state['sequence']))
                # All referenced bytes have native receipts before the caller
                # can see any row. Only this snapshot is removed; late bounded
                # in-flight results remain registered for the next unit/cleanup.
                sink.reset(fact['prepared']['sinks']['output'])
                value=batch;batch=[];batch_bytes=0
                send(value)
                state['handoff_rows']+=len(value)

            def execute():
                guard=None
                async def consume(row):
                    nonlocal batch_bytes,guard
                    if guard is None:guard=asyncio.Lock()
                    async with guard:
                        if stopped.is_set():raise _DeliveryStopped()
                        if state['rows']>=run.limits['max_output_rows']:
                            raise RecoveryContractError('Delivery row budget exceeded')
                        size=check_size(row,run.limits['max_row_bytes'])
                        if batch and batch_bytes+size>run.limits['max_batch_bytes']:
                            await asyncio.to_thread(flush)
                        batch.append(row);batch_bytes+=size;state['rows']+=1
                        if len(batch)>=16 or (limit is not None and state['rows']>=limit):
                            await asyncio.to_thread(flush)
                        if limit is not None and state['rows']>=limit:raise _DeliveryStopped()
                # Dataset._run_stream is synchronous and owns the sole event
                # loop; the callback belongs to that loop, not this caller.
                return consume

            def produce():
                try:
                    consume=execute()
                    try:
                        from .native_plan import root_stage_limits
                        stream_view(executor,source,plan)._run_stream(consume=consume,
                            stage_limits=root_stage_limits(execution) if execution is not None else None)
                    except _DeliveryStopped:pass
                    if not stopped.is_set():flush()
                    if owns_complete:
                        action.complete({'kind':'delivery','rows':state['handoff_rows'],'commits':state['sequence']})
                except BaseException as exc:
                    state['error']=exc
                finally:done.set()

            worker=threading.Thread(target=lambda:transfer_context.run(produce),name='demiflow-native-delivery',daemon=False)
            worker.start()
            try:
                while True:
                    try:value=handoff.get(timeout=0.05)
                    except queue.Empty:
                        if done.is_set():
                            if state['error'] is not None:raise state['error']
                            break
                        run.check_stop();continue
                    for row in value:
                        if limit is not None:
                            retained_bytes+=check_size(row,run.limits['max_row_bytes'])
                            if retained_bytes>result_budget:
                                raise RecoveryContractError('Bounded take result exceeds its driver byte budget')
                        yield row
                        delivered_rows += 1
                        if limit is not None and delivered_rows >= limit:
                            return
            finally:
                stopped.set()
                worker.join(timeout=run.limits['cancel_grace_s']+2)
                if worker.is_alive():
                    run.quarantine_worker(worker)
                    raise RecoveryContractError('Native delivery exceeded cancel/drain budget; owned writer isolated')
