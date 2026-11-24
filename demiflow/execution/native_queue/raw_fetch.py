"""Native async-map actor for one raw acquisition and its durable receipt."""
import asyncio
from datetime import datetime, timezone
import os
import shutil
import time

from demiflow.objects import LocalObjectStore
from demiflow.collect.raw_contract import fetch_policy, integer, request as validate_request
from demiflow.services.blocking_io import BlockingIOPool
from demiflow.execution.checkpoint.commit import CommitCoordinator
from demiflow.execution.native_queue.raw_budget import RawBudget
from demiflow.execution.native_queue.raw_objects import RawNamespace, RawObjectSink, _directory
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, SourcePosition, document, identifier, identity
from demiflow.execution.worker.row_scope import current_row_scope
from demiflow.execution.pipeline.run_context import RunAction, current_action


class RawFetch:
    def __init__(self, *, request='request', output='result', store, policy=None,
                 concurrency=4, queue_depth=None, label='fetch'):
        for value in (request,output,label):
            identifier(value)
        if '_queue_delivery' in {request,output}:
            raise ValueError('Fetch fields overlap native queue delivery metadata')
        if request == output:
            raise ValueError('Fetch output must differ from request')
        if type(store) is not LocalObjectStore:
            raise TypeError('fetch requires LocalObjectStore')
        integer(concurrency,'concurrency',1,64)
        queue_depth = concurrency if queue_depth is None else queue_depth
        integer(queue_depth,'queue_depth',1,256)
        self.request, self.output, self.store = request, output, store
        self.policy = fetch_policy(policy)
        self.concurrency, self.queue_depth, self.label = concurrency,queue_depth,label
        self.queue_source = None
        self.run = self.node_id = self.io = self.transfer = self.namespace = None

    def recovery_identity(self):
        return dict(contract='demiflow.collect.fetch.v1',request=self.request,output=self.output,
                    store=self.store.recovery_identity(),policy=self.policy.declaration())

    def bind_execution(self, run, node_id):
        if self.io is not None and (self.run is not run or self.node_id not in (None,node_id)):
            raise RecoveryContractError('Raw actor binding changed while active')
        self.run, self.node_id = run, node_id

    def snapshot_state(self):
        return {}

    def restore_state(self,state):
        if state != {}:
            raise RecoveryContractError('Raw acquisition progress belongs to native commits')

    async def astart(self):
        self.parent = current_action()
        if self.parent is None:
            raise RecoveryContractError('Raw fetch has no native action')
        if self.run is None:
            self.run = self.parent.context
        self.io = BlockingIOPool(self.concurrency,name='demiflow-raw-io')
        if self.queue_source is None:
            self.namespace = RawNamespace(self.store,self.run.ref.run_id)
            await self.io.run(self.namespace.open)
        else:
            self.namespace = self.queue_source.namespace
        from demiflow.collect.raw_transport import Transfer
        self.transfer = Transfer(self.policy,self.concurrency,self.io)
        self.transfer.open()

    async def astop(self):
        await self.aclose()

    async def aclose(self):
        try:
            if self.transfer is not None:
                await self.transfer.aclose()
        finally:
            if self.io is not None:
                await self.io.aclose()
            self.transfer = self.io = None

    def _scope(self, occurrence, declaration, delivery=None):
        if self.queue_source is not None:
            from demiflow.execution.native_queue.queue_commit import task_action
            source=self.queue_source
            if delivery is None or source.active.get(delivery.task_id) is not delivery:
                raise RecoveryContractError('Raw task lost its source-owned delivery')
            return task_action(self.parent,source.channel,source.pool,delivery,'acquisition',source.raw_node_id,source.contract,stop_check=source._check)
        scope_id = identity([self.parent.action_id,self.node_id,occurrence,'acquisition'])
        self.run.journal.bind_commit_scope(self.run.fence,scope_id,self.parent.action_id,declaration)
        return RunAction(self.run,scope_id,declaration)

    async def __call__(self,row):
        from demiflow.collect.raw_transport import Outcome
        if type(row) is not dict or self.output in row:
            raise ValueError('Fetch requires a record with an unused output field')
        req = validate_request(row[self.request])
        document(req,self.policy.max_request_bytes)
        scope = current_row_scope()
        if scope is None:
            raise RecoveryContractError('Fetch lost native source occurrence')
        self.run.check_stop()
        action = await self.io.run(self._scope,scope.occurrence,
            dict(kind='raw_acquisition',node=self.node_id,occurrence=scope.occurrence,
                 request=req,contract=self.recovery_identity()),scope.delivery)
        sink = RawObjectSink(self.namespace,self.policy.max_object_bytes)
        coordinator = CommitCoordinator(action)
        facts = await self.io.run(action.context.journal.execution_commits,action.action_id)
        budget = RawBudget(action.context,action.action_id,self.policy)
        if facts and facts[0]['status'] != 'pending':
            await self.io.run(coordinator.recover,{'raw':sink})
            fact = await self.io.run(action.context.journal.execution_commit,facts[0]['commit_id'])
            prepared = fact['prepared']['sinks']['raw']
            await self.io.run(self._settle,budget,prepared)
            self._link(fact['commit_id'])
            return {**row,self.output:prepared['result']}
        commit_id = identity([action.context.ref.run_id,action.action_id,1])
        bound = min(req.get('expected_size',self.policy.max_object_bytes),self.policy.max_object_bytes)
        admission_deadline=time.monotonic()+self.run.limits['admission_timeout_s']
        for admission_try in range(32):
            admitted=await self.io.run(budget.open,dict(request=req,policy=self.policy.declaration()),bound,self.namespace.domain)
            if admitted:break
            remaining=admission_deadline-time.monotonic()
            if remaining<=0:
                raise RecoveryContractError('Raw storage admission wait expired')
            await asyncio.sleep(min(remaining, min(1.,.05*2**min(admission_try,5))))
        else:
            raise RecoveryContractError('Raw storage admission control budget exhausted')
        free = await self.io.run(shutil.disk_usage,self.namespace.root)
        if free.free < bound + 65536:
            raise RecoveryContractError('Raw target has insufficient free space for admission')
        result, writer, fact_object = None,None,None
        error = Outcome('attempts_exhausted',retryable=True)
        refreshed = 0
        current_attempt = None
        try:
            if req.get('expected_size',0) > self.policy.max_object_bytes:
                error = Outcome('object_too_large')
            else:
                while True:
                    self.run.check_stop()
                    next_attempt = await self.io.run(budget.start_attempt)
                    if next_attempt is None:
                        break
                    number,remaining = next_attempt
                    current_attempt = number
                    writer = await self.io.run(self.namespace.stage,commit_id,bound)
                    deadline = asyncio.timeout(min(remaining,self.policy.attempt_timeout_s))
                    try:
                        async with deadline:
                            source = await self.transfer.attempt(req,writer,budget,number)
                        fact_object = await self.io.run(writer.finish)
                        source['_sha256'] = fact_object['sha256']
                        result = self._result(req,None,source,writer.size,commit_id,action.action_id)
                        break
                    except Outcome as exc:
                        error = exc
                    except TimeoutError:
                        if not deadline.expired():
                            raise
                        error = Outcome('attempt_deadline',retryable=True,partial=writer.size)
                    await self.io.run(writer.close)
                    await self.io.run(budget.close_attempt_disk,number)
                    writer = None
                    if error.reason == 'ephemeral_access_denied':
                        error = Outcome('access_denied')
                        if refreshed < self.policy.ephemeral_redirect_refreshes and number < self.policy.max_attempts:
                            refreshed += 1
                            continue
                    if not error.retryable or number == self.policy.max_attempts:
                        break
                    delay = min(self.policy.backoff_cap_s,self.policy.backoff_initial_s * 2**(number-1))
                    delay = max(delay,error.retry_after)
                    if delay >= remaining:
                        break
                    await asyncio.sleep(delay)
            if result is None:
                result = self._result(req,error,None,error.partial,commit_id,action.action_id)
            result.update(await self.io.run(budget.totals))
            document(result,self.policy.max_receipt_bytes)
            prepared = dict(format=1,object=fact_object,result=result,
                            domain=self.namespace.domain if fact_object is not None else None)
            # Once preparation could be durable, unknown outcomes retain bytes.
            if writer is not None:
                writer.retained = True
            fact = await self.io.run(coordinator.commit,1,outputs={'raw':(sink,prepared)},
                    position=SourcePosition(identity([action.semantic_id,scope.occurrence]),1))
            await self.io.run(self._settle,budget,prepared)
            if writer is not None:
                # The shared CAS object is immutable; only this phase's staging
                # link is disposable after its native publication is confirmed.
                writer.retained = False
                await self.io.run(writer.close)
                writer = None
            self._link(fact['commit_id'])
            return {**row,self.output:result}
        finally:
            if writer is not None:
                await self.io.run(writer.close)
                if not writer.retained and current_attempt is not None:
                    await self.io.run(budget.close_attempt_disk,current_attempt)

    def _settle(self,budget,prepared):
        fact=prepared['object']
        budget.settle_disk(fact['bytes'] if fact else 0,
                           successful_attempt=prepared['result']['attempts'] if fact else None)
        if fact:
            # Publication and its native receipt are confirmed before removing
            # only this phase's staging link. The CAS content is never deleted.
            parent=_directory(self.namespace.root/'.pending')
            try:
                try:os.unlink(fact['staging'],dir_fd=parent)
                except FileNotFoundError:pass
                os.fsync(parent)
            finally:os.close(parent)

    def _link(self, commit_id):
        if self.queue_source is not None:return
        pending = getattr(self.parent,'raw_acquisitions',None)
        if pending is None:
            pending = self.parent.raw_acquisitions = set()
        if len(pending) >= min(8192,self.run.limits['max_commits']):
            raise RecoveryContractError('Unconfirmed raw acquisition reference count exceeded')
        pending.add(commit_id)

    def _result(self,request,error,source,size,commit_id,scope_id):
        ok = error is None
        return dict(status='ok' if ok else 'retryable' if error.retryable else 'failed',
            terminal=ok or not error.retryable,raw_ref=dict(uri=self.namespace.target(source_hash).as_uri(),sha256=source_hash)
                if ok and (source_hash := source.pop('_sha256',None)) else None,
            size_bytes=size if ok else None,partial_bytes=0 if ok else size,
            reason=None if ok else error.reason,detail=None,
            source=source or dict(uri=request['uri'],final_uri=None,final_origin=None,locator_redacted=False,
                                 redirect_hops=0,source_version=request.get('source_version'),pinned=False),
            checks=dict(expected_size=request.get('expected_size'),expected_sha256=request.get('expected_sha256'),
                        size_verified=ok and 'expected_size' in request,sha256_verified=ok and 'expected_sha256' in request),
            attempts=0,http_requests=0,retrieved_at=datetime.now(timezone.utc).isoformat(),
            acquisition_ref=dict(commit_id=commit_id,scope_id=scope_id),
            storage_domain=self.namespace.domain if ok else None)


def raw_fetch_facts(actor):
    if type(actor) is not RawFetch:
        raise TypeError('Unknown raw acquisition actor')
    return dict(native=actor.recovery_identity(),effects=['network','objects'],
                resources=[dict(kind='objects',root=str(actor.store.directory),access='write'),
                    dict(kind='network',source='bounded_row_uri',policy=actor.policy.declaration())],reserved_bytes=actor.concurrency*(8*65536+2*actor.policy.chunk_bytes+65536))
