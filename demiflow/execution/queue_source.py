"""Explicit native queue datasource; leases travel in stream sidecars only."""
import asyncio
import threading
import time

from ..data.datasource import Datasource
from demiflow.services.blocking_io import BlockingIOPool
from .queue import QueueChannel
from .queue_contract import queue_config, QueueIdleTimeout, QueueIncomplete
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, identity, identifier
from demiflow.execution.worker.row_scope import RowScope, ScopedRow


class QueueDatasource(Datasource):
    def __init__(self,config,*,pool='default',settlement='sink',max_inflight=8,
                 idle_timeout_s=300,lease_s=60,max_tasks=10000,stop=None):
        from ..collect.raw_contract import integer
        import math
        self.config=queue_config(config)
        self.pool=identifier(pool)
        if settlement not in {'sink','explicit'}:raise ValueError('Unknown queue settlement mode')
        self.settlement=settlement
        self.max_inflight=integer(max_inflight,'max_inflight',1,16)
        self.max_tasks=integer(max_tasks,'max_tasks',1,100000)
        for name,value,lo,hi in [('idle_timeout_s',idle_timeout_s,.01,86400),('lease_s',lease_s,30,3600)]:
            if type(value) not in (int,float) or not math.isfinite(value) or not lo<=value<=hi:
                raise ValueError('Invalid queue time budget: '+name)
        if stop is not None and (type(stop) is not str or not stop.startswith('/') or len(stop)>4096):
            raise ValueError('Queue stop requires an explicit absolute file path')
        self.idle_timeout_s,self.lease_s,self.stop=idle_timeout_s,lease_s,stop
        self.channel=self.parent=self.fetch_actor=self.sink=self.contract=self.worker_id=None
        self.active={};self.lock=threading.Lock();self.closed=threading.Event();self.stopping=threading.Event()
        self.io=self.lease_io=self.heartbeat=None;self.failure=None
        self.counts=dict(claimed=0,settled=0,recovered=0,exhausted=0)
        self.stop_reason=None
        self.blocked_artifact_domain=0

    def declaration(self):
        return dict(contract='demiflow.queue.source.v2',config=self.config.declaration(),pool=self.pool,
            settlement=self.settlement,max_inflight=self.max_inflight,idle_timeout_s=self.idle_timeout_s,
            lease_s=self.lease_s,max_tasks=self.max_tasks,stop=self.stop)

    def snapshot_identity(self):
        return self.declaration()

    def recovery_identity(self):
        return self.declaration()

    def get_read_tasks(self,parallelism,per_task_row_limit=None,data_context=None):
        raise RecoveryContractError('Queue sources require the native task terminal; open queues cannot be materialized or delivered without settlement')

    def configure(self,parent,fetch_actor,sink,contract,node_id):
        if self.worker_id is not None:
            raise RecoveryContractError('Queue source instance has already been executed')
        self.parent,self.fetch_actor,self.sink,self.contract=parent,fetch_actor,sink,contract
        self.raw_node_id=node_id
        fetch_actor.queue_source=self

    def bind_execution(self,run,node_id):
        if self.parent is None or run is not self.parent.context:
            raise RecoveryContractError('Queue source has no validated native task action')

    async def astart(self):
        if self.parent is None:raise RecoveryContractError('Queue source must pass task topology validation')
        self.io=BlockingIOPool(1,name='demiflow-queue-control')
        self.lease_io=BlockingIOPool(1,name='demiflow-queue-lease')
        self.channel=QueueChannel(self.config)
        await self.io.run(self.channel._attach)
        snapshot=await self.io.run(self.channel.snapshot,self.pool,kind='tasks')
        self.generation=snapshot.generation
        from .queue_commit import bind_queue_budget
        self.available_commits=await self.io.run(bind_queue_budget,self.parent,self.channel)
        from .raw_objects import RawNamespace
        self.namespace=RawNamespace(self.fetch_actor.store,identity([snapshot.queue_uuid,snapshot.generation]))
        domain=await self.io.run(self.namespace.open)
        run=self.parent.context
        binding=dict(queue_uuid=snapshot.queue_uuid,generation=self.generation,consumer_contract=identity(self.contract))
        await self.io.run(run.journal.bind_source,run.fence,self.parent.action_id+'-queue',binding)
        await self.io.run(self._open_window,domain)
        await self.io.run(self._reconcile_references)
        self.heartbeat=asyncio.create_task(self._heartbeat(),name='demiflow-queue-lease')

    def _open_window(self,domain):
        run=self.parent.context
        self.reference_key=self.parent.action_id+'-queue-window'
        binding=run.journal.source_binding(self.reference_key)
        if binding is None:
            available=self.available_commits
            self.window_limit=min(self.max_tasks,available//2)
            if self.window_limit<1:raise RecoveryContractError('No native commit capacity remains before task claim')
            limits={**run.limits,'run_budget_owner':run.ref.run_id,'queue_max_tasks':self.window_limit,'queue_max_inflight':self.max_inflight}
            self.worker_id=self.channel.register_worker(self.pool,domain,self.contract,limits)
            binding=dict(worker_id=self.worker_id,queue_uuid=self.channel.database.queue_uuid,generation=self.generation)
            run.journal.bind_source(run.fence,self.reference_key,binding)
        else:self.worker_id=binding['worker_id']
        if binding['queue_uuid']!=self.channel.database.queue_uuid or binding['generation']!=self.generation:
            raise RecoveryContractError('Queue worker window belongs to another generation')
        with self.channel.database.connection() as db:
            worker=db.execute('SELECT domain,limits,claimed,settled,exhausted FROM queue_workers WHERE worker_id=?',(self.worker_id,)).fetchone()
        import json
        if worker is None or json.loads(worker[0])!=domain:
            from .queue_contract import ArtifactDomainUnavailable
            raise ArtifactDomainUnavailable('Native worker window artifact domain is unavailable')
        self.window_limit=json.loads(worker[1])['queue_max_tasks']
        self.counts.update(claimed=worker[2],settled=worker[3],exhausted=worker[4])
        with run.journal.execution_guard(run.fence) as db:
            db.execute('CREATE TABLE IF NOT EXISTS runtime_queue_windows (source_id TEXT PRIMARY KEY,confirmed_elsewhere INTEGER NOT NULL DEFAULT 0)')
            db.execute('CREATE TABLE IF NOT EXISTS runtime_queue_refs (source_id TEXT NOT NULL,task_id TEXT NOT NULL,fingerprint TEXT NOT NULL,PRIMARY KEY(source_id,task_id))')
            db.execute('INSERT OR IGNORE INTO runtime_queue_windows VALUES (?,0)',(self.reference_key,))
            self.counts['settled']+=db.execute('SELECT confirmed_elsewhere FROM runtime_queue_windows WHERE source_id=?',(self.reference_key,)).fetchone()[0]
            self.unconfirmed={r[0] for r in db.execute('SELECT task_id FROM runtime_queue_refs WHERE source_id=?',(self.reference_key,)).fetchall()}
        # A process can die after the backend grants a lease but before this
        # run receives it. Recover only this window's bounded unfinished claims.
        with self.channel.database.connection() as db:
            unfinished=db.execute("SELECT task_id,fingerprint FROM queue_tasks WHERE worker_id=? AND status='leased' LIMIT ?",(self.worker_id,self.max_inflight+1)).fetchall()
        if len(unfinished)>self.max_inflight:raise RecoveryContractError('Worker unfinished claim bound exceeded')
        from types import SimpleNamespace
        for task_id,fingerprint in unfinished:
            self._record_reference(SimpleNamespace(task_id=task_id,fingerprint=fingerprint))
        self.reconcile_hash_bytes=0

    def _record_reference(self,delivery):
        run=self.parent.context
        with run.journal.execution_guard(run.fence) as db:
            old=db.execute('SELECT fingerprint FROM runtime_queue_refs WHERE source_id=? AND task_id=?',(self.reference_key,delivery.task_id)).fetchone()
            if old is not None:
                if old[0]!=delivery.fingerprint:raise RecoveryContractError('Unconfirmed task fingerprint changed')
            else:
                if db.execute('SELECT count(*) FROM runtime_queue_refs WHERE source_id=?',(self.reference_key,)).fetchone()[0]>=self.max_inflight:
                    raise RecoveryContractError('Unconfirmed completion reference bound exceeded')
                from .raw_budget import budget_delta
                budget_delta(db,'control_bytes',256,run.limits['max_control_bytes'])
                db.execute('INSERT INTO runtime_queue_refs VALUES (?,?,?)',(self.reference_key,delivery.task_id,delivery.fingerprint))
        self.unconfirmed.add(delivery.task_id)

    def _drop_reference(self,task_id,*,elsewhere=False):
        run=self.parent.context
        with run.journal.execution_guard(run.fence) as db:
            removed=db.execute('DELETE FROM runtime_queue_refs WHERE source_id=? AND task_id=?',(self.reference_key,task_id)).rowcount
            if removed and elsewhere:
                db.execute('UPDATE runtime_queue_windows SET confirmed_elsewhere=confirmed_elsewhere+1 WHERE source_id=?',(self.reference_key,))
        self.unconfirmed.discard(task_id)

    def _reconcile_references(self):
        # At most max_inflight exact lookups from this run. A fresh worker has
        # no references and performs zero completed-record scans or hashes.
        from demiflow.execution.sources.queue_history import inspect_result
        from .queue_commit import QueueCommitStore
        run=self.parent.context
        with run.journal._connection() as db:
            refs=db.execute('SELECT task_id,fingerprint FROM runtime_queue_refs WHERE source_id=?',(self.reference_key,)).fetchall()
        if len(refs)>self.max_inflight:raise RecoveryContractError('Native unconfirmed reference bound exceeded')
        for task_id,fingerprint in refs:
            proof=self.channel.completion(self.generation,task_id)
            if proof is None:continue
            if proof['fingerprint']!=fingerprint or proof['consumer_contract']!=identity(self.contract):
                raise RecoveryContractError('Unconfirmed completion differs from its original source binding')
            with self.channel.database.connection() as db:
                fact=QueueCommitStore._fact(db,proof['acquisition_commit'])
                worker=db.execute('SELECT worker_id FROM queue_tasks WHERE generation=? AND task_id=?',(self.generation,task_id)).fetchone()[0]
            obj=fact['prepared']['sinks']['raw']['object']
            amount=0 if obj is None else obj['bytes']
            if self.reconcile_hash_bytes+amount>run.limits['max_output_bytes']:
                raise RecoveryContractError('Unconfirmed completion artifact verification budget exhausted')
            snapshot=self.channel.snapshot(self.pool,kind='results')
            inspect_result(self.channel,snapshot,task_id,check_stop=run.check_stop)
            self.reconcile_hash_bytes+=amount
            self._drop_reference(task_id,elsewhere=worker!=self.worker_id)
        with self.channel.database.connection() as db:
            counts=db.execute('SELECT claimed,settled,exhausted FROM queue_workers WHERE worker_id=?',(self.worker_id,)).fetchone()
        with run.journal._connection() as db:
            elsewhere=db.execute('SELECT confirmed_elsewhere FROM runtime_queue_windows WHERE source_id=?',(self.reference_key,)).fetchone()[0]
        self.counts.update(claimed=counts[0],settled=counts[1]+elsewhere,exhausted=counts[2])

    async def _heartbeat(self):
        try:
            while not self.closed.is_set():
                await asyncio.sleep(min(15,self.lease_s/4))
                with self.lock:values=list(self.active.values())
                for delivery in values:
                    from .queue_contract import StaleQueueOwner
                    try:await self.lease_io.run(self.channel.renew,delivery,lease_s=self.lease_s)
                    except StaleQueueOwner:
                        # Confirmation may have won after this heartbeat took
                        # its snapshot. Only this exact immutable completion
                        # permits a read-only observation of that race.
                        proof=await self.lease_io.run(self.channel.completion,delivery.generation,delivery.task_id)
                        if proof is None or proof['fingerprint']!=delivery.fingerprint:raise
                        with self.lock:still_active=self.active.get(delivery.task_id) is delivery
                        if still_active:raise

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failure=exc
            self.closed.set()

    def _check(self):
        self.parent.context.check_stop()
        if self.failure is not None:raise self.failure
        if self.stopping.is_set():raise QueueIncomplete('Queue source execution was interrupted')
        if self.stop is not None:
            from pathlib import Path
            if Path(self.stop).exists():
                raise QueueIncomplete('Queue worker stop requested')

    def rows(self):
        idle_since=time.monotonic()
        while not self.closed.is_set():
            self._check()
            with self.lock:active=len(self.active)
            if self.counts['claimed']>=self.window_limit and (active or not self.unconfirmed):
                self.stop_reason='window_complete'
                return
            if active>=self.max_inflight:
                self.closed.wait(.05)
                continue
            delivery=self.channel.claim(self.worker_id,lease_s=self.lease_s)
            if delivery is None:
                if not active and self.unconfirmed:self._reconcile_references()
                state=self.channel._state(self.pool,domain_id=self.namespace.domain['domain_id'])
                self.blocked_artifact_domain=state['blocked_artifact_domain']
                if state['queue_drained']:
                    self.stop_reason='queue_drained'
                    return
                if state['counts'].get('exhausted'):
                    raise QueueIncomplete('Queue contains exhausted retryable observations')
                if active:
                    idle_since=time.monotonic()
                elif time.monotonic()-idle_since>=self.idle_timeout_s:
                    if self.blocked_artifact_domain:
                        from .queue_contract import ArtifactDomainUnavailable
                        raise ArtifactDomainUnavailable('Unfinished artifacts are outside this worker reading domain')
                    raise QueueIdleTimeout('Queue is not drained before its idle deadline')
                self.closed.wait(.05)
                continue
            idle_since=time.monotonic()
            self._record_reference(delivery)
            with self.lock:self.active[delivery.task_id]=delivery
            with self.channel.database.connection() as db:
                self.counts['claimed']=db.execute('SELECT claimed FROM queue_workers WHERE worker_id=?',(self.worker_id,)).fetchone()[0]
            if self._recover_final(delivery):
                continue
            display=dict(task_id=delivery.task_id,fingerprint=delivery.fingerprint,attempt=delivery.epoch)
            if '_queue_delivery' in delivery.payload:raise RecoveryContractError('Queue payload overlaps its reserved delivery display')
            row={**delivery.payload,'_queue_delivery':display}
            yield ScopedRow(row,RowScope(identity([delivery.generation,delivery.task_id,delivery.fingerprint]),delivery))
        self._check()
        raise QueueIncomplete('Queue worker stopped with unconfirmed tasks')

    def _recover_final(self,delivery):
        with self.channel.database.connection() as db:
            row=db.execute('SELECT final_scope FROM queue_tasks WHERE generation=? AND task_id=?',(delivery.generation,delivery.task_id)).fetchone()
            fact=None if not row or not row[0] else db.execute('SELECT prepared FROM runtime_commits WHERE action_id=?',(row[0],)).fetchone()
        if fact is None or fact[0] is None:return False
        from .queue_execution import finalize_task
        finalize_task(self,delivery,None)
        self.counts['recovered']+=1
        return True

    def confirmed(self,delivery,*,exhausted=False):
        with self.lock:
            if self.active.get(delivery.task_id) is not delivery:
                raise RecoveryContractError('Task sidecar is not owned by this source')
            del self.active[delivery.task_id]
        self._drop_reference(delivery.task_id)
        self.counts['exhausted' if exhausted else 'settled']+=1

    def request_stop(self):
        self.stopping.set()
        if self.stop_reason is None:self.stop_reason='interrupted'

    def acknowledge(self,position,context):
        raise RecoveryContractError('Queue ACK requires its atomic native final task commit')

    def assert_settled(self):
        self._check()
        with self.lock:
            active=len(self.active)
        if active or self.unconfirmed or self.counts['claimed']!=self.counts['settled']+self.counts['exhausted']:
            raise QueueIncomplete('Queue terminal has unconfirmed claimed tasks')
        if self.counts['exhausted'] or self.stop_reason not in {'window_complete','queue_drained'}:
            raise QueueIncomplete('Queue terminal has no complete worker-window outcome')

    async def aclose(self):
        self.closed.set()
        if self.heartbeat is not None:
            self.heartbeat.cancel()
            await asyncio.gather(self.heartbeat,return_exceptions=True)
        if self.lease_io is not None:await self.lease_io.aclose()
        if self.io is not None:
            await self.io.aclose()
        # Uncertain effects keep their lease/domain facts until expiry. Closing
        # an actor must never manufacture ACK or discard prepared artifacts.

    def snapshot_metrics(self):
        return {**self.counts,'stop_reason':self.stop_reason,'active':len(self.active),
                'blocked_artifact_domain':self.blocked_artifact_domain}
