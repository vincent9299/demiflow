"""Queue database implementation of the native commit-store interface.

The coordinator remains in commit.py. This module owns fenced SQL transitions,
including the single transaction closing final checkpoint and source completion.
"""
from contextlib import contextmanager
import json
from types import SimpleNamespace

from .queue import QueueChannel
from .queue_contract import StaleQueueOwner
from .recovery_contract import COMMIT_STATES, RecoveryContractError, document, identity
from demiflow.execution.pipeline.run_context import RunAction


class QueueBudgetTransaction:
    """Pool and native-run quotas change together in this transaction."""
    def __init__(self,db,store):
        self.db,self.store=db,store

    def execute(self,*args,**kwargs):return self.db.execute(*args,**kwargs)
    def lock(self,*args,**kwargs):return self.db.lock(*args,**kwargs)
    def now(self):return self.db.now()

    def _names(self,name,owner=None):
        local=getattr(self.store,'local_run',None)
        if owner is None or self.store.delivery is not None and owner==self.store.delivery.worker_id:
            window_owner=local.ref.run_id if local is not None else self.store.delivery.worker_id
        else:
            worker=self.db.execute('SELECT limits FROM queue_workers WHERE worker_id=?',(owner,)).fetchone()
            window_owner=json.loads(worker[0]).get('run_budget_owner',owner) if worker else owner
        return 'pool-'+self.store.generation+'-'+name,'run-'+window_owner+'-'+name

    def _pool_limit(self,name,worker_ceiling):
        limits=self.store.channel.config.limits
        return {'output_admission_bytes':limits.max_output_bytes,'output_bytes':limits.max_output_bytes,
                'object_bytes':limits.max_output_bytes,'row_bytes':limits.max_output_bytes,
                'result_row_bytes':limits.max_result_total_bytes,
                'control_bytes':limits.max_control_bytes,'raw_disk_bytes':limits.max_scratch_bytes,
                'raw_wire_bytes':limits.max_wire_bytes,'requests':limits.max_requests,
                'raw_control_grants':limits.max_control_grants,'queue_commit_admission':limits.max_commits,
                'native_scope_records':limits.max_commits,'native_commit_records':limits.max_commits,
                'producer_scope_records':limits.max_producer_commits,'reservation_records':limits.max_reservations,
                'raw_resource_records':64}.get(name,worker_ceiling)

    def native_budget_value(self,name):
        _,window=self._names(name)
        return self.db.execute('SELECT consumed,ceiling FROM runtime_budgets WHERE name=?',(window,)).fetchone()

    def native_budget_delta(self,name,delta,ceiling,*,owner=None,pool_delta=None):
        from .raw_budget import budget_delta
        pool,window=self._names(name,owner)
        if name=='control_bytes':
            budget_delta(self.db,'database-control-bytes',delta,self.store.channel.config.limits.max_control_bytes)
        # Pool row is the common lock across all workers. Insert-on-conflict
        # and UPDATE remain in the task-locked outer transaction.
        pool_limit=self._pool_limit(name,ceiling)
        self.db.execute('INSERT INTO runtime_budgets VALUES (?,0,?) ON CONFLICT(name) DO NOTHING',(pool,pool_limit))
        self.db.execute(self.db.lock('SELECT consumed FROM runtime_budgets WHERE name=?'),(pool,)).fetchone()
        budget_delta(self.db,pool,delta if pool_delta is None else pool_delta,pool_limit)
        # On recovery the refund follows the original reservation window; a new
        # owner's untouched allowance must never be credited for old I/O.
        old=self.db.execute('SELECT ceiling FROM runtime_budgets WHERE name=?',(window,)).fetchone()
        if old is not None and owner is not None:ceiling=old[0]
        return budget_delta(self.db,window,delta,ceiling)


def bind_queue_budget(parent,channel):
    """One native run delegates task budgets to exactly one queue database."""
    run=parent.context
    key='queue-budget-authority'
    old=run.journal.source_binding(key)
    declaration=dict(protocol='demiflow.queue.budget.v2',queue_uuid=channel.database.queue_uuid)
    if old is None:
        with run.journal._connection() as db:
            local=db.execute('SELECT count(*) FROM runtime_commits').fetchone()[0]
        if local:
            raise RecoveryContractError('Queue task budgets require a separate run from local durable effects')
        run.journal.bind_source(run.fence,key,declaration)
    elif old!=declaration:
        raise RecoveryContractError('A native run cannot mix independent queue budget authorities')
    with channel.database.connection() as db:
        row=db.execute('SELECT consumed FROM runtime_budgets WHERE name=?',('run-'+run.ref.run_id+'-queue_commit_admission',)).fetchone()
    return run.limits['max_commits']-(row[0] if row else 0)


class QueueCommitStore:
    def __init__(self, channel, *, pool, local_run, scope_id, declaration, phase,
                 delivery=None, producer=None, stop_check=None):
        if phase not in {'acquisition','final','producer','source'} or (delivery is None)==(producer is None):
            raise RecoveryContractError('Invalid native queue commit scope')
        self.channel,self.pool,self.local_run=channel,pool,local_run
        self.scope_id,self.phase,self.delivery,self.producer=scope_id,phase,delivery,producer
        channel._attach()
        self.generation=delivery.generation if delivery else producer.generation
        self.run_id=identity([channel.database.queue_uuid,self.generation])
        self.fence=delivery.token if delivery else producer.token
        self.declaration=declaration
        self.stop_check=stop_check or local_run.check_stop
        self.bind_commit_scope(self.fence,scope_id,
            'task-'+identity([self.generation,delivery.task_id]) if delivery else 'producer-'+self.generation,declaration)

    def _control(self,db,amount):
        from .raw_budget import budget_delta
        budget_delta(db,'control_bytes',amount,self.local_run.limits['max_control_bytes'])

    def _artifact_delta(self,db,domain,delta):
        if domain is None:return
        row=db.execute('SELECT amount FROM queue_domain_counts WHERE generation=? AND domain_id=?',(self.generation,domain)).fetchone()
        if row is None:
            if delta!=1 or db.execute('SELECT count(*) FROM queue_domain_counts WHERE generation=?',(self.generation,)).fetchone()[0]>=64:
                raise RecoveryContractError('Queue artifact domain count budget exhausted or inconsistent')
            self._control(db,256)
            db.execute('INSERT INTO queue_domain_counts VALUES (?,?,1)',(self.generation,domain))
        else:
            if row[0]+delta<0:raise RecoveryContractError('Queue artifact domain count is inconsistent')
            db.execute('UPDATE queue_domain_counts SET amount=amount+? WHERE generation=? AND domain_id=?',(delta,self.generation,domain))

    def database_time(self,db):
        return float(db.now())

    @contextmanager
    def execution_guard(self,fence):
        self.local_run.check_stop()
        if fence!=self.fence:
            raise StaleQueueOwner('Queue scope fence differs')
        with self.channel.database.connection(write=True) as db:
            if self.delivery is not None:
                QueueChannel.lock_delivery(db,self.delivery)
            else:
                row=QueueChannel._producer(db,self.producer)
                if row[4]:
                    raise StaleQueueOwner('Sealed producer cannot mutate native progress')
            self.channel.database.lock_budgets(db)
            # Waiting for the shared quota row must not extend an owner lease
            # or the generation deadline checked before acquiring that row.
            self.local_run.check_stop()
            if self.delivery is not None:QueueChannel.lock_delivery(db,self.delivery)
            else:QueueChannel._producer(db,self.producer)
            yield QueueBudgetTransaction(db,self)

    @contextmanager
    def _connection(self):
        with self.channel.database.connection() as db:
            yield db

    def bind_commit_scope(self,fence,scope_id,parent_action_id,declaration):
        if scope_id!=self.scope_id:
            raise RecoveryContractError('Scope binding escaped queue owner')
        raw=document(declaration,65536).decode()
        with self.execution_guard(fence) as db:
            old=db.execute('SELECT parent_action_id,declaration FROM runtime_commit_scopes WHERE scope_id=?',(scope_id,)).fetchone()
            if old:
                if old!=(parent_action_id,raw):
                    raise RecoveryContractError('Queue native scope declaration changed')
                return
            db.native_budget_delta('native_scope_records',1,self.local_run.limits['max_commits'])
            if self.phase in {'producer','source'}:
                db.native_budget_delta('producer_scope_records',1,self.local_run.limits['max_commits'])
            self._control(db,len(raw)+256)
            db.execute('INSERT INTO runtime_commit_scopes VALUES (?,?,?)',(scope_id,parent_action_id,raw))
            if self.delivery is not None:
                column='acquisition_scope' if self.phase=='acquisition' else 'final_scope'
                old=db.execute('SELECT '+column+' FROM queue_tasks WHERE generation=? AND task_id=?',
                               (self.generation,self.delivery.task_id)).fetchone()[0]
                if old is not None and old!=scope_id:
                    raise RecoveryContractError('Task phase scope identity changed')
                db.execute('UPDATE queue_tasks SET '+column+'=? WHERE generation=? AND task_id=?',
                           (scope_id,self.generation,self.delivery.task_id))

    def begin_commit(self,fence,action_id,sequence,intent):
        if action_id!=self.scope_id or sequence!=1:
            raise RecoveryContractError('Queue phase has exactly one native commit sequence')
        raw=document(intent).decode()
        commit_id=identity([self.run_id,action_id,sequence])
        with self.execution_guard(fence) as db:
            old=db.execute('SELECT intent FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
            if old:
                if old[0]!=raw:
                    raise RecoveryContractError('Queue commit intent changed')
            else:
                db.native_budget_delta('native_commit_records',1,self.local_run.limits['max_commits'])
                if self.phase in {'producer','source'}:
                    db.native_budget_delta('queue_commit_admission',1,self.local_run.limits['max_commits'])
                self._control(db,len(raw)+256)
                db.execute('INSERT INTO runtime_commits VALUES (?,?,?,?,?,NULL)',(commit_id,action_id,sequence,'pending',raw))
        return self.execution_commit(commit_id)

    @staticmethod
    def _fact(db,commit_id):
        row=db.execute('SELECT action_id,sequence,status,intent,prepared FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
        if row is None:return None
        receipts={key:json.loads(raw) for key,raw in db.execute('SELECT sink_id,document FROM runtime_receipts WHERE commit_id=?',(commit_id,)).fetchall()}
        return dict(commit_id=commit_id,action_id=row[0],sequence=row[1],status=row[2],intent=json.loads(row[3]),
                    prepared=json.loads(row[4]) if row[4] else None,receipts=receipts)

    def execution_commit(self,commit_id):
        with self._connection() as db:return self._fact(db,commit_id)

    def execution_commits(self,action_id):
        with self._connection() as db:
            rows=db.execute('SELECT commit_id FROM runtime_commits WHERE action_id=? ORDER BY sequence LIMIT 2',(action_id,)).fetchall()
            if len(rows)>1:raise RecoveryContractError('Task phase has multiple native commit sequences')
            return [self._fact(db,r[0]) for r in rows]

    def prepare_commit(self,fence,commit_id,prepared):
        raw=document(prepared).decode()
        if prepared['state'] or prepared.get('raw_acquisitions'):
            raise RecoveryContractError('Queue phase state must use its native source/acquisition facts')
        expected={'acquisition':{'raw'},'final':{'result'},'producer':{'publish'},'source':{'search'}}[self.phase]
        if set(prepared['sinks'])!=expected:
            raise RecoveryContractError('Queue phase sink capability differs')
        with self.execution_guard(fence) as db:
            row=db.execute('SELECT status,prepared FROM runtime_commits WHERE commit_id=? AND action_id=?',(commit_id,self.scope_id)).fetchone()
            if row is None:raise RecoveryContractError('Unknown queue scope commit')
            if row[0]!='pending':
                if row[1]==raw:return
                raise RecoveryContractError('Prepared queue effect changed')
            if self.phase=='acquisition':
                raw_part=prepared['sinks']['raw']
                if raw_part['object'] is not None:
                    domain=raw_part['domain']['domain_id']
                    if domain!=self.delivery.domain['domain_id']:
                        raise RecoveryContractError('Prepared raw artifact differs from dispatch domain')
                    task=QueueChannel.lock_delivery(db,self.delivery)
                    if task[7] is not None and task[7]!=domain:
                        raise RecoveryContractError('Prepared task artifact domain changed')
                    if task[7] is None:self._artifact_delta(db,domain,1)
                    db.execute('UPDATE queue_tasks SET artifact_domain=? WHERE generation=? AND task_id=?',(domain,self.generation,self.delivery.task_id))
            self._control(db,len(raw))
            db.execute("UPDATE runtime_commits SET status='prepared',prepared=? WHERE commit_id=?",(raw,commit_id))

    def publish_receipt(self,db,commit_id,sink_id,receipt):
        raw=document(receipt).decode()
        old=db.execute('SELECT document FROM runtime_receipts WHERE commit_id=? AND sink_id=?',(commit_id,sink_id)).fetchone()
        if old is not None and old[0]!=raw:
            raise RecoveryContractError('Native queue receipt changed')
        if old is None:self._control(db,len(raw)+128)
        db.execute('INSERT INTO runtime_receipts VALUES (?,?,?) ON CONFLICT(commit_id,sink_id) DO NOTHING',(commit_id,sink_id,raw))

    def confirm_commit(self,fence,commit_id,*,expected,status):
        if expected not in COMMIT_STATES or status not in COMMIT_STATES or COMMIT_STATES.index(status)!=COMMIT_STATES.index(expected)+1:
            raise RecoveryContractError('Invalid native queue transition')
        if status=='acked' and self.phase=='final':
            raise RecoveryContractError('Final source completion requires atomic finalize_source_commit')
        with self.execution_guard(fence) as db:
            fact=self._fact(db,commit_id)
            if fact is None or fact['action_id']!=self.scope_id:raise RecoveryContractError('Unknown queue scope commit')
            if COMMIT_STATES.index(fact['status'])>=COMMIT_STATES.index(status):return
            if fact['status']!=expected:raise RecoveryContractError('Native queue state changed')
            if status=='data_committed' and set(fact['receipts'])!=set(fact['intent']['sinks']):
                raise RecoveryContractError('Queue data lacks native receipts')
            db.execute('UPDATE runtime_commits SET status=? WHERE commit_id=?',(status,commit_id))

    def finalize_source_commit(self,fence,commit_id):
        if self.phase!='final' or self.delivery is None:
            raise RecoveryContractError('Only native final task scopes may settle a source')
        d=self.delivery
        with self.execution_guard(fence) as db:
            task=QueueChannel.lock_delivery(db,d)
            fact=self._fact(db,commit_id)
            if fact is None or fact['status']!='checkpoint_committed' or fact['action_id']!=self.scope_id or set(fact['receipts'])!={'result'}:
                raise RecoveryContractError('Final task commit is not checkpoint committed')
            prepared=fact['prepared']['sinks']['result']
            result_row=db.execute('SELECT row_document FROM queue_results WHERE generation=? AND task_id=? AND commit_id=?',
                                  (d.generation,d.task_id,commit_id)).fetchone()
            if result_row is None or identity(json.loads(result_row[0]))!=prepared['row_sha256']:
                raise RecoveryContractError('Final result is missing or changed')
            row=json.loads(result_row[0]); result=row[prepared['result_field']]
            acq_id=result['acquisition_ref']['commit_id']
            acq=self._fact(db,acq_id)
            if (acq is None or acq['status']!='acked' or acq['action_id']!=task[8] or
                    acq['prepared']['sinks']['raw']['result']!=result or set(acq['receipts'])!={'raw'}):
                raise RecoveryContractError('Final task receipt lacks exact acquired raw evidence')
            terminal=result['status'] in {'ok','failed'} and result['terminal'] is True
            if not terminal and not (result['status']=='retryable' and result['terminal'] is False):
                raise RecoveryContractError('Task result has no valid settlement outcome')
            kind='results' if terminal else 'observations'
            ordinal=self.channel.database.publication(db,d.generation,kind)
            # The publication lock may have waited. Lease time is checked again
            # with the backend clock immediately before source-state mutation.
            QueueChannel.lock_delivery(db,d)
            self.stop_check()
            consumer=db.execute('SELECT consumer_contract FROM queue_pools WHERE generation=?',(d.generation,)).fetchone()[0]
            proof=dict(queue_uuid=self.channel.database.queue_uuid,generation=d.generation,task_id=d.task_id,
                fingerprint=d.fingerprint,consumer_contract=consumer,acquisition_commit=acq_id,final_commit=commit_id,
                receipt_sha256=identity(fact['receipts']['result']),artifact_domain=task[7],ordinal=ordinal,
                result_ref=dict(commit_id=commit_id,sha256=prepared['row_sha256']),settled=terminal,
                status=result['status'])
            table='queue_completions' if terminal else 'queue_observations'
            proof_raw=document(proof,16384)
            self._control(db,len(proof_raw)+128)
            db.execute('INSERT INTO '+table+' VALUES (?,?,?,?)',(d.generation,d.task_id,ordinal,proof_raw.decode()))
            status=('done_ok' if result['status']=='ok' else 'done_failed') if terminal else 'exhausted'
            self._artifact_delta(db,task[7],-1)
            QueueChannel.transition(db,d.generation,'leased',status)
            counter='settled' if terminal else 'exhausted'
            db.execute('UPDATE queue_workers SET '+counter+'='+counter+'+1 WHERE worker_id=?',(d.worker_id,))
            db.execute('UPDATE queue_tasks SET status=?,token=NULL,lease_until=NULL WHERE generation=? AND task_id=?',(status,d.generation,d.task_id))
            if terminal:
                db.execute("UPDATE runtime_commits SET status='acked' WHERE commit_id=?",(commit_id,))
        return proof


def task_action(parent,channel,pool,delivery,phase,node_id,contract,*,stop_check=None):
    declaration=dict(protocol='demiflow.queue.task.v2',queue_uuid=channel.database.queue_uuid,
        generation=delivery.generation,task_id=delivery.task_id,fingerprint=delivery.fingerprint,
        phase=phase,node=node_id,consumer=contract)
    scope_id=identity(declaration)
    store=QueueCommitStore(channel,pool=pool,local_run=parent.context,scope_id=scope_id,
                           declaration=declaration,phase=phase,delivery=delivery,stop_check=stop_check)
    context=SimpleNamespace(journal=store,ref=SimpleNamespace(run_id=store.run_id),fence=delivery.token,
        attempt=delivery.epoch,directory=parent.context.directory,limits=parent.context.limits,
        check_stop=stop_check or parent.context.check_stop,reservation_owner=delivery.worker_id)
    return RunAction(context,scope_id,declaration)


def producer_action(parent,channel,producer,occurrence,contract,*,phase='producer'):
    declaration=dict(protocol='demiflow.queue.producer.v2',queue_uuid=producer.queue_uuid,
        generation=producer.generation,source=producer.source.source_id,occurrence=occurrence,transform=contract,phase=phase)
    scope_id=identity(declaration)
    store=QueueCommitStore(channel,pool=producer.pool,local_run=parent.context,scope_id=scope_id,
        declaration=declaration,phase=phase,producer=producer)
    context=SimpleNamespace(journal=store,ref=SimpleNamespace(run_id=store.run_id),fence=producer.token,
        attempt=producer.epoch,directory=parent.context.directory,limits=parent.context.limits,
        check_stop=parent.context.check_stop)
    return RunAction(context,scope_id,declaration)
