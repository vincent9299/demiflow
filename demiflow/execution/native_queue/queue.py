"""Public native task channel: declarations and bounded administrative operations.

Dataset execution owns producer/consumer progress. Administrative methods never
execute callbacks, fetch bytes, invent EOS, or grant arbitrary sink ACK authority.
"""
from dataclasses import asdict, dataclass
import json
import math
import uuid

from demiflow.execution.contracts.queue_contract import QueueConfig, QueueLimits, ProducerSourceSpec, ProducerRef, QueueSnapshot, HistoryReadLimits, QueueIncomplete, QueueIdleTimeout, StaleQueueOwner, ArtifactDomainUnavailable, HistoryReadLimit, queue_config, task_message
from demiflow.execution.native_queue.queue_sql import QueueDatabase
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, document, identifier, identity


@dataclass(frozen=True)
class TaskDelivery:
    generation: str
    task_id: str
    fingerprint: str
    payload: dict
    epoch: int
    token: str
    worker_id: str
    lease_until: float
    domain: dict


class QueueChannel:
    def __init__(self, config):
        self.config = queue_config(config)
        self.database = QueueDatabase(self.config)

    def _attach(self):
        if self.database.queue_uuid is None:
            self.database.attach()

    def _control(self, db, generation, amount):
        from demiflow.execution.native_queue.raw_budget import budget_delta
        self.database.lock_budgets(db)
        ceiling = self.config.limits.max_control_bytes
        budget_delta(db, 'database-control-bytes', amount, ceiling)
        budget_delta(db, 'pool-'+generation+'-control_bytes', amount, ceiling)

    def open(self, pool, *, producer_id, source, expected_tasks=None):
        identifier(pool);identifier(producer_id)
        if type(source) is not ProducerSourceSpec:
            raise TypeError('open requires a fixed ProducerSourceSpec')
        if expected_tasks is not None and (type(expected_tasks) is not int or not 0<=expected_tasks<=self.config.limits.max_tasks):
            raise ValueError('expected_tasks exceeds queue budget')
        self.database.initialize()
        raw = document(asdict(source),16384).decode()
        token = uuid.uuid4().hex
        with self.database.connection(write=True) as db:
            # A per-queue row serializes bounded generation creation only.
            db.execute(db.lock('SELECT queue_uuid FROM queue_identity WHERE id=1')).fetchone()
            row = db.execute(db.lock('SELECT generation,producer_id,source,expected_tasks,epoch,sealed,deadline FROM queue_pools WHERE pool=?'),(pool,)).fetchone()
            if row:
                if row[1:4] != (producer_id,raw,expected_tasks):
                    raise RecoveryContractError('Producer source or expected task count changed')
                if db.now()>=row[6]:raise QueueIncomplete('Queue generation elapsed budget exhausted')
                if row[5]:
                    raise RecoveryContractError('Sealed pool cannot reopen; declare a new generation')
                generation,epoch=row[0],row[4]+1
                db.execute('UPDATE queue_pools SET epoch=?,token=? WHERE pool=?',(epoch,token,pool))
            else:
                if db.execute('SELECT count(*) FROM queue_pools').fetchone()[0]>=self.config.limits.max_pools:
                    raise RecoveryContractError('Queue pool budget exceeded')
                generation,epoch=uuid.uuid4().hex,1
                self._control(db,generation,len(raw.encode('utf8'))+2048)
                db.execute('INSERT INTO queue_pools (pool,generation,producer_id,source,expected_tasks,epoch,token,digest,deadline) VALUES (?,?,?,?,?,?,?,?,?)',
                           (pool,generation,producer_id,raw,expected_tasks,epoch,token,identity(['producer',raw]),float(db.now())+self.config.limits.max_elapsed_s))
                for status in ('pending','leased','done_ok','done_failed','exhausted'):
                    db.execute('INSERT INTO queue_counts VALUES (?,?,0)',(generation,status))
                for kind in ('tasks','results','observations'):
                    db.execute('INSERT INTO queue_publications VALUES (?,?,0)',(generation,kind))
        return ProducerRef(self.database.queue_uuid,pool,generation,producer_id,source,epoch,token)

    @staticmethod
    def _producer(db, producer):
        row=db.execute(db.lock('SELECT generation,producer_id,epoch,token,sealed,eos,tasks,expected_tasks,digest,deadline FROM queue_pools WHERE pool=?'),(producer.pool,)).fetchone()
        if row is None or row[:4] != (producer.generation,producer.producer_id,producer.epoch,producer.token):
            raise StaleQueueOwner('Producer epoch no longer owns publication')
        if db.now()>=row[9]:raise QueueIncomplete('Queue generation elapsed budget exhausted')
        return row

    def seal(self, producer, *, expected_tasks=None):
        self._attach()
        if type(producer) is not ProducerRef or producer.queue_uuid != self.database.queue_uuid:
            raise RecoveryContractError('Producer belongs to another queue')
        with self.database.connection(write=True) as db:
            row=self._producer(db,producer)
            if not row[5]:
                raise RecoveryContractError('Cannot seal without native source EOS evidence')
            pending=db.execute("SELECT 1 FROM runtime_commits c JOIN runtime_commit_scopes s ON c.action_id=s.scope_id WHERE s.parent_action_id=? AND c.status!='acked' LIMIT 1",
                               ('producer-'+producer.generation,)).fetchone()
            if pending:
                raise RecoveryContractError('Producer still has unconfirmed native publications')
            expected=row[7] if expected_tasks is None else expected_tasks
            if expected is not None and (type(expected) is not int or expected != row[6]):
                raise RecoveryContractError('Sealed task count differs from declared expectation')
            db.execute('UPDATE queue_pools SET sealed=1 WHERE pool=?',(producer.pool,))
            cursor=db.execute("SELECT ordinal FROM queue_publications WHERE generation=? AND kind='tasks'",(producer.generation,)).fetchone()[0]
            return dict(queue_uuid=producer.queue_uuid,pool=producer.pool,generation=producer.generation,
                        actual_tasks=row[6],cursor=cursor,digest=row[8],sealed=True)

    def snapshot(self, pool, *, kind='results'):
        self._attach();identifier(pool)
        if kind not in {'tasks','results','observations'}:
            raise ValueError('Unknown queue history kind')
        with self.database.connection() as db:
            row=db.execute('SELECT p.generation,c.ordinal FROM queue_pools p JOIN queue_publications c ON c.generation=p.generation WHERE p.pool=? AND c.kind=?',(pool,kind)).fetchone()
        if row is None:
            raise RecoveryContractError('Unknown queue pool')
        return QueueSnapshot(self.database.queue_uuid,pool,row[0],kind,row[1])

    def state(self,pool):
        return self._state(pool)

    def _state(self,pool,*,domain_id=None):
        self._attach();identifier(pool)
        if domain_id is not None:identifier(domain_id)
        with self.database.connection() as db:
            row=db.execute('SELECT generation,sealed,eos,tasks FROM queue_pools WHERE pool=?',(pool,)).fetchone()
            if row is None:
                raise RecoveryContractError('Unknown queue pool')
            counts={status:amount for status,amount in db.execute('SELECT status,amount FROM queue_counts WHERE generation=?',(row[0],)).fetchall() if amount}
            domains=db.execute('SELECT domain_id,amount FROM queue_domain_counts WHERE generation=? LIMIT 65',(row[0],)).fetchall()
            if len(domains)>64 or any(amount<0 for _,amount in domains):
                raise RecoveryContractError('Queue artifact domain counts exceed their native bound')
            domains={key:amount for key,amount in domains if amount}
        settled=counts.get('done_ok',0)+counts.get('done_failed',0)
        return dict(pool=pool,generation=row[0],sealed=bool(row[1]),eos=bool(row[2]),tasks=row[3],counts=counts,
                    artifact_domains=domains,artifact_bound_tasks=sum(domains.values()),
                    blocked_artifact_domain=None if domain_id is None else sum(amount for key,amount in domains.items() if key!=domain_id),
                    artifact_availability='not_probed' if domain_id is None else 'current_worker_domain',
                    queue_drained=bool(row[1]) and settled==row[3])

    def register_worker(self, pool, domain, consumer_contract, limits):
        self._attach();identifier(pool)
        document(domain,8192);document(consumer_contract,65536);document(limits,16384)
        contract=identity(consumer_contract)
        worker=uuid.uuid4().hex
        with self.database.connection(write=True) as db:
            row=db.execute(db.lock('SELECT generation,consumer_contract FROM queue_pools WHERE pool=?'),(pool,)).fetchone()
            if row is None:
                raise RecoveryContractError('Unknown queue pool')
            if row[1] is not None and row[1] != contract:
                raise RecoveryContractError('Queue consumer contract differs')
            if db.execute('SELECT count(*) FROM queue_workers WHERE generation=?',(row[0],)).fetchone()[0]>=self.config.limits.max_workers:
                raise RecoveryContractError('Queue worker registration budget exceeded')
            self._control(db,row[0],len(document(domain,8192))+len(document(limits,16384))+512)
            db.execute('UPDATE queue_pools SET consumer_contract=? WHERE pool=?',(contract,pool))
            db.execute('INSERT INTO queue_workers (worker_id,generation,domain,consumer_contract,limits,started) VALUES (?,?,?,?,?,?)',
                       (worker,row[0],document(domain,8192).decode(),contract,document(limits,16384).decode(),db.now()))
        return worker

    def claim(self, worker_id, *, lease_s=60, task_id=None):
        self._attach();identifier(worker_id)
        if task_id is not None:identifier(task_id)
        if type(lease_s) not in (int,float) or not 30<=lease_s<=3600:
            raise ValueError('Lease must be 30..3600 seconds')
        with self.database.connection(write=True) as db:
            worker=db.execute('SELECT generation,domain,limits,claimed FROM queue_workers WHERE worker_id=?',(worker_id,)).fetchone()
            if worker is None:
                raise RecoveryContractError('Unknown registered worker')
            domain=json.loads(worker[1]); now=db.now()
            deadline=db.execute('SELECT deadline FROM queue_pools WHERE generation=?',(worker[0],)).fetchone()[0]
            if now>=deadline:raise QueueIncomplete('Queue generation elapsed budget exhausted')
            active=db.execute("SELECT amount FROM queue_counts WHERE generation=? AND status='leased'",(worker[0],)).fetchone()[0]
            allow_pending=active<self.config.limits.max_active_tasks
            allow_fresh=worker[3]<json.loads(worker[2]).get('queue_max_tasks',10000)
            row=db.execute(db.lock("SELECT task_id,fingerprint,payload,epoch,status,lease_until,worker_id FROM queue_tasks WHERE generation=? AND (status='pending' OR status='leased' AND lease_until<=?) AND (artifact_domain IS NULL OR artifact_domain=?) AND (worker_id=? OR ?=1) AND (status='leased' OR ?=1)" + (" AND task_id=?" if task_id is not None else "") + " ORDER BY ordinal LIMIT 1",skip=True),
                           (worker[0],now,domain['domain_id'],worker_id,int(allow_fresh),int(allow_pending)) + ((task_id,) if task_id is not None else ())).fetchone()
            if row is None:
                return None
            # PostgreSQL clock_timestamp() is evaluated after the task lock.
            now=db.now()
            if row[4]=='leased' and row[5]>now:
                return None
            self.database.lock_budgets(db)
            now=db.now()
            if now>=deadline:raise QueueIncomplete('Queue generation elapsed budget exhausted')
            if row[3]>=self.config.limits.max_claims_per_task:
                raise QueueIncomplete('Task claim budget exhausted')
            if row[4]=='pending':
                active=db.execute(db.lock("SELECT amount FROM queue_counts WHERE generation=? AND status='leased'"),(worker[0],)).fetchone()[0]
                if active>=self.config.limits.max_active_tasks:return None
            window=db.execute(db.lock('SELECT claimed,limits FROM queue_workers WHERE worker_id=?'),(worker_id,)).fetchone()
            limits=json.loads(window[1])
            fresh=row[6]!=worker_id
            if fresh and window[0]>=limits.get('queue_max_tasks',10000):return None
            inflight=db.execute("SELECT count(*) FROM queue_tasks WHERE worker_id=? AND status='leased'",(worker_id,)).fetchone()[0]
            if not fresh:inflight-=1
            if inflight>=limits.get('queue_max_inflight',8):return None
            from types import SimpleNamespace
            from demiflow.execution.native_queue.queue_commit import QueueBudgetTransaction
            reservation=identity([worker[0],row[0],'commit-capacity'])
            previous=db.execute("SELECT amount FROM runtime_reservations WHERE name='queue_commit_capacity' AND reservation_id=?",(reservation,)).fetchone()
            budget=QueueBudgetTransaction(db,SimpleNamespace(channel=self,generation=worker[0],delivery=SimpleNamespace(worker_id=worker_id),
                local_run=SimpleNamespace(ref=SimpleNamespace(run_id=limits.get('run_budget_owner',worker_id)))))
            if fresh:budget.native_budget_delta('queue_commit_admission',2,limits.get('max_commits',16384),pool_delta=0 if previous else 2)
            if previous is None:
                budget.native_budget_delta('reservation_records',1,self.config.limits.max_reservations)
                budget.native_budget_delta('control_bytes',256,limits.get('max_control_bytes',self.config.limits.max_control_bytes))
                db.execute("INSERT INTO runtime_reservations VALUES ('queue_commit_capacity',?,2)",(reservation,))
            if fresh:db.execute('UPDATE queue_workers SET claimed=claimed+1 WHERE worker_id=?',(worker_id,))
            self.transition(db,worker[0],row[4],'leased')
            token=uuid.uuid4().hex;epoch=row[3]+1;until=now+lease_s
            db.execute("UPDATE queue_tasks SET status='leased',epoch=?,token=?,lease_until=?,worker_id=?,dispatch_domain=? WHERE generation=? AND task_id=?",
                       (epoch,token,until,worker_id,domain['domain_id'],worker[0],row[0]))
            return TaskDelivery(worker[0],row[0],row[1],json.loads(row[2]),epoch,token,worker_id,until,domain)

    def restore_delivery(self, worker_id, *, generation, task_id, fingerprint, epoch, lease_s=60):
        """Resume one authenticated original window entry, using native fencing.

        The caller must own its RunJournal window. Tokens never leave this
        driver authority. Expired leases go through the same claim admission;
        an active lease owned by another window is never adopted.
        """
        self._attach()
        for value in (worker_id,generation,task_id,fingerprint):identifier(value)
        if type(epoch) is not int or epoch < 1:
            raise RecoveryContractError('Invalid original queue delivery epoch')
        if type(lease_s) not in (int,float) or not math.isfinite(lease_s) or not 30<=lease_s<=3600:
            raise ValueError('Lease must be 30..3600 seconds')
        with self.database.connection(write=True) as db:
            worker=db.execute('SELECT generation,domain FROM queue_workers WHERE worker_id=?',(worker_id,)).fetchone()
            row=db.execute(db.lock('SELECT fingerprint,payload,status,epoch,token,worker_id,lease_until FROM queue_tasks WHERE generation=? AND task_id=?'),(generation,task_id)).fetchone()
            if (worker is None or worker[0]!=generation or row is None
                    or row[0]!=fingerprint or row[2]!='leased'
                    or row[3]<epoch or row[5]!=worker_id):
                raise StaleQueueOwner('Original queue delivery no longer belongs to this worker window')
            if row[6]>db.now():
                delivery=TaskDelivery(generation,task_id,fingerprint,json.loads(row[1]),row[3],row[4],worker_id,row[6],json.loads(worker[1]))
                self.lock_delivery(db,delivery)
                db.execute('UPDATE queue_tasks SET lease_until=? WHERE generation=? AND task_id=?',(db.now()+lease_s,generation,task_id))
                return delivery
        delivery=self.claim(worker_id,lease_s=lease_s,task_id=task_id)
        if delivery is None or delivery.generation!=generation or delivery.fingerprint!=fingerprint:
            raise StaleQueueOwner('Original queue delivery cannot be reclaimed')
        return delivery

    @staticmethod
    def transition(db,generation,old,new):
        if old==new:return
        for status in sorted((old,new)):
            row=db.execute(db.lock('SELECT amount FROM queue_counts WHERE generation=? AND status=?'),(generation,status)).fetchone()
            if row is None or row[0]+(-1 if status==old else 1)<0:
                raise RecoveryContractError('Queue state count differs from its native transition')
        db.execute('UPDATE queue_counts SET amount=amount-1 WHERE generation=? AND status=?',(generation,old))
        db.execute('UPDATE queue_counts SET amount=amount+1 WHERE generation=? AND status=?',(generation,new))

    @staticmethod
    def lock_delivery(db, delivery):
        row=db.execute(db.lock('SELECT fingerprint,status,epoch,token,lease_until,worker_id,dispatch_domain,artifact_domain,acquisition_scope,final_scope FROM queue_tasks WHERE generation=? AND task_id=?'),
                       (delivery.generation,delivery.task_id)).fetchone()
        if (row is None or row[0]!=delivery.fingerprint or row[1]!='leased' or
                row[2:4]!=(delivery.epoch,delivery.token) or row[4]<=db.now() or
                row[5]!=delivery.worker_id or row[6]!=delivery.domain['domain_id']):
            raise StaleQueueOwner('Task lease or owner epoch is no longer current')
        deadline=db.execute('SELECT deadline FROM queue_pools WHERE generation=?',(delivery.generation,)).fetchone()[0]
        if db.now()>=deadline:raise QueueIncomplete('Queue generation elapsed budget exhausted')
        return row

    def renew(self, delivery, *, lease_s=60):
        if type(lease_s) not in (int,float) or not math.isfinite(lease_s) or not 30<=lease_s<=3600:
            raise ValueError('Lease must be 30..3600 seconds')
        self._attach()
        with self.database.connection(write=True) as db:
            self.lock_delivery(db,delivery)
            until=db.now()+lease_s
            db.execute('UPDATE queue_tasks SET lease_until=? WHERE generation=? AND task_id=?',(until,delivery.generation,delivery.task_id))
            return until

    def release(self, delivery):
        self._attach()
        with self.database.connection(write=True) as db:
            self.lock_delivery(db,delivery)
            self.transition(db,delivery.generation,'leased','pending')
            db.execute("UPDATE queue_tasks SET status='pending',token=NULL,lease_until=NULL WHERE generation=? AND task_id=?",(delivery.generation,delivery.task_id))

    def completion(self,generation,task_id):
        """Exact immutable lookup, no lease, mutation, scan, repair or reclaim."""
        self._attach();identifier(generation);identifier(task_id)
        with self.database.connection() as db:
            row=db.execute('SELECT proof FROM queue_completions WHERE generation=? AND task_id=?',(generation,task_id)).fetchone()
        return json.loads(row[0]) if row else None


def __getattr__(name):
    if name=='QueueResultSink':
        from demiflow.execution.native_queue.queue_sinks import QueueResultSink
        return QueueResultSink
    raise AttributeError(name)
