"""Cumulative, fenced raw-transfer reservations in the native control store.

One bounded row per scope/attempt, never one reservation per network chunk.
Unknown grants survive crashes; only a live, drained attempt can return slack.
"""
import time
import json
from .recovery_contract import RecoveryContractError, document, identity


def schema(db):
    db.execute('CREATE TABLE IF NOT EXISTS runtime_raw_scopes (scope_id TEXT PRIMARY KEY, declaration TEXT NOT NULL, deadline DOUBLE PRECISION NOT NULL, disk BIGINT NOT NULL, settled INTEGER NOT NULL DEFAULT 0, owner TEXT NOT NULL, retained BIGINT)')
    db.execute('CREATE TABLE IF NOT EXISTS runtime_raw_attempts (scope_id TEXT NOT NULL, attempt INTEGER NOT NULL, grant_no INTEGER NOT NULL, granted BIGINT NOT NULL, used BIGINT NOT NULL, requests INTEGER NOT NULL, finalized INTEGER NOT NULL, disk_bytes BIGINT NOT NULL, disk_closed INTEGER NOT NULL, owner TEXT NOT NULL, PRIMARY KEY(scope_id,attempt))')
    db.execute('CREATE TABLE IF NOT EXISTS runtime_raw_resources (resource_id TEXT PRIMARY KEY, document TEXT NOT NULL)')


def budget_delta(db, name, delta, ceiling, *, owner=None):
    native=getattr(db,'native_budget_delta',None)
    if native is not None:
        return native(name,delta,ceiling,owner=owner)
    old = db.execute('SELECT consumed,ceiling FROM runtime_budgets WHERE name=?',(name,)).fetchone()
    if old is not None and old[1] != ceiling:
        raise RecoveryContractError('Native budget ceiling changed: ' + name)
    value = (old[0] if old else 0) + delta
    if not 0 <= value <= ceiling:
        raise RecoveryContractError('Native budget exhausted: ' + name)
    if old is None:
        db.execute('INSERT INTO runtime_budgets VALUES (?,?,?)',(name,value,ceiling))
    else:
        db.execute('UPDATE runtime_budgets SET consumed=? WHERE name=?',(value,name))
    return value


def budget_value(db,name):
    native=getattr(db,'native_budget_value',None)
    if native is not None:return native(name)
    return db.execute('SELECT consumed,ceiling FROM runtime_budgets WHERE name=?',(name,)).fetchone()


def record_admission(db, ceiling):
    # The same durable counter covers native reservation/scope/attempt records.
    # Never scan an ever-growing pool on each chunk or task admission.
    budget_delta(db,'reservation_records',1,ceiling)


class RawBudget:
    def __init__(self, run, scope_id, policy):
        self.run, self.store, self.scope_id, self.policy = run, run.journal, scope_id, policy
        # Representation body accounting, distinct from transport/IP bytes.
        self.owner = getattr(run,'reservation_owner',run.fence)
        channel=getattr(self.store,'channel',None)
        self.record_limit=min(100000,channel.config.limits.max_reservations) if channel is not None else 100000
        self.wire_limit = run.limits['max_output_bytes'] * 8 + run.limits['max_requests'] * 4096

    def open(self, declaration, bound, domain):
        raw = document(declaration,65536).decode()
        with self.store.execution_guard(self.run.fence) as db:
            self.run.check_stop()
            if not hasattr(self.store,'channel'):schema(db)
            resource = document(domain,8192).decode()
            key = identity([domain['store_id'],domain['domain_id'],domain['physical_root']])
            old = db.execute('SELECT document FROM runtime_raw_resources WHERE resource_id=?',(key,)).fetchone()
            if old is not None and old[0] != resource:
                raise RecoveryContractError('Raw resource binding changed')
            if old is None:
                budget_delta(db,'raw_resource_records',1,64)
                budget_delta(db,'control_bytes',len(resource.encode('utf8'))+128,self.run.limits['max_control_bytes'])
                db.execute('INSERT INTO runtime_raw_resources VALUES (?,?)',(key,resource))
            row = db.execute('SELECT declaration,deadline,disk FROM runtime_raw_scopes WHERE scope_id=?',(self.scope_id,)).fetchone()
            if row:
                if row[0] != raw:
                    raise RecoveryContractError('Raw scope declaration changed')
                self.deadline = row[1]
                return True
            budget_delta(db,'raw_control_grants',1,262144)
            for name,ceiling in [('output_admission_bytes',self.run.limits['max_output_bytes']),
                                 ('raw_disk_bytes',self.run.limits['max_scratch_bytes'])]:
                current=budget_value(db,name)
                consumed=current[0] if current else 0
                if current and current[1]!=ceiling:
                    raise RecoveryContractError('Native budget ceiling changed')
                if consumed+bound>ceiling:
                    active=db.execute('SELECT COALESCE(SUM(disk),0) FROM runtime_raw_scopes WHERE settled=0 AND owner=?',(self.owner,)).fetchone()[0]
                    if bound>ceiling or consumed-active+bound>ceiling or not active:
                        raise RecoveryContractError('Native raw retained capacity exhausted: '+name)
                    return False
            record_admission(db,self.record_limit)
            self.deadline = self.now(db) + self.policy.total_timeout_s
            budget_delta(db,'output_admission_bytes',bound,self.run.limits['max_output_bytes'])
            budget_delta(db,'raw_disk_bytes',bound,self.run.limits['max_scratch_bytes'])
            budget_delta(db,'control_bytes',len(raw.encode('utf8'))+256,self.run.limits['max_control_bytes'])
            db.execute('INSERT INTO runtime_raw_scopes VALUES (?,?,?,?,0,?,NULL)',(self.scope_id,raw,self.deadline,bound,self.owner))
            return True

    def now(self, db):
        now = getattr(self.store,'database_time',None)
        return now(db) if now else time.time()

    def start_attempt(self):
        with self.store.execution_guard(self.run.fence) as db:
            self.run.check_stop()
            row = db.execute('SELECT deadline FROM runtime_raw_scopes WHERE scope_id=?',(self.scope_id,)).fetchone()
            if row is None:
                raise RecoveryContractError('Raw scope is not registered')
            n = db.execute('SELECT count(*) FROM runtime_raw_attempts WHERE scope_id=?',(self.scope_id,)).fetchone()[0] + 1
            if n > self.policy.max_attempts or row[0] <= self.now(db):
                return None
            record_admission(db,self.record_limit)
            declaration=json.loads(db.execute('SELECT declaration FROM runtime_raw_scopes WHERE scope_id=?',(self.scope_id,)).fetchone()[0])
            bound=min(declaration['request'].get('expected_size',self.policy.max_object_bytes),self.policy.max_object_bytes)
            previous=db.execute('SELECT disk_closed FROM runtime_raw_attempts WHERE scope_id=? AND attempt=?',(self.scope_id,n-1)).fetchone()
            current_owner=db.execute('SELECT owner FROM runtime_raw_scopes WHERE scope_id=?',(self.scope_id,)).fetchone()[0]
            if previous is not None and not previous[0]:
                # An interrupted writer may still occupy its complete grant.
                # A replacement attempt never spends that grant a second time.
                budget_delta(db,'output_admission_bytes',bound,self.run.limits['max_output_bytes'])
                budget_delta(db,'raw_disk_bytes',bound,self.run.limits['max_scratch_bytes'])
                db.execute('UPDATE runtime_raw_scopes SET disk=disk+? WHERE scope_id=?',(bound,self.scope_id))
            elif current_owner!=self.owner and hasattr(db,'native_budget_delta'):
                # No live writer exists for this reusable grant. Transfer its
                # window ownership atomically, preserving the pool total.
                for name,ceiling in [('output_admission_bytes',self.run.limits['max_output_bytes']),
                                     ('raw_disk_bytes',self.run.limits['max_scratch_bytes'])]:
                    db.native_budget_delta(name,-bound,ceiling,owner=current_owner,pool_delta=0)
                    db.native_budget_delta(name,bound,ceiling,pool_delta=0)
            db.execute('UPDATE runtime_raw_scopes SET owner=? WHERE scope_id=?',(self.owner,self.scope_id))
            budget_delta(db,'control_bytes',256,self.run.limits['max_control_bytes'])
            db.execute('INSERT INTO runtime_raw_attempts VALUES (?,?,0,0,0,0,0,?,0,?)',(self.scope_id,n,bound,self.owner))
            return n, row[0] - self.now(db)

    def extend(self, attempt, expected_grant, granted, requests):
        with self.store.execution_guard(self.run.fence) as db:
            self.run.check_stop()
            row = db.execute('SELECT grant_no,granted,used,requests,finalized FROM runtime_raw_attempts WHERE scope_id=? AND attempt=?',(self.scope_id,attempt)).fetchone()
            if row is None or row[4]:
                raise RecoveryContractError('Raw attempt is not live')
            if row[0] == expected_grant+1 and row[1] == granted and row[3] == requests:
                return row[0]
            if row[0] != expected_grant or granted < row[1] or requests < row[3] or requests > self.policy.redirects+1:
                raise RecoveryContractError('Raw cumulative grant conflicts')
            if expected_grant >= 8192:
                raise RecoveryContractError('Raw attempt control transaction budget exceeded')
            budget_delta(db,'raw_control_grants',1,262144)
            budget_delta(db,'raw_wire_bytes',granted-row[1],self.wire_limit)
            budget_delta(db,'requests',requests-row[3],self.run.limits['max_requests'])
            db.execute('UPDATE runtime_raw_attempts SET grant_no=?,granted=?,requests=? WHERE scope_id=? AND attempt=?',
                       (expected_grant+1,granted,requests,self.scope_id,attempt))
            return expected_grant+1

    def finish_attempt(self, attempt, used):
        with self.store.execution_guard(self.run.fence) as db:
            row = db.execute('SELECT granted,used,finalized FROM runtime_raw_attempts WHERE scope_id=? AND attempt=?',(self.scope_id,attempt)).fetchone()
            if row is None or not 0 <= used <= row[0]:
                raise RecoveryContractError('Invalid raw attempt accounting')
            if row[2]:
                if row[1] != used:
                    raise RecoveryContractError('Finalized attempt changed')
                return
            budget_delta(db,'raw_wire_bytes',used-row[0],self.wire_limit)
            db.execute('UPDATE runtime_raw_attempts SET used=?,finalized=1 WHERE scope_id=? AND attempt=?',(used,self.scope_id,attempt))

    def close_attempt_disk(self,attempt):
        """Called only after this live writer has drained and unlinked staging."""
        with self.store.execution_guard(self.run.fence) as db:
            settled=db.execute('SELECT settled FROM runtime_raw_scopes WHERE scope_id=?',(self.scope_id,)).fetchone()
            if settled is not None and settled[0]:return
            db.execute('UPDATE runtime_raw_attempts SET disk_bytes=0,disk_closed=1 WHERE scope_id=? AND attempt=?',
                       (self.scope_id,attempt))

    def settle_disk(self, retained, *, successful_attempt=None):
        with self.store.execution_guard(self.run.fence) as db:
            row = db.execute('SELECT disk,settled,retained FROM runtime_raw_scopes WHERE scope_id=?',(self.scope_id,)).fetchone()
            if row is None or not 0 <= retained <= row[0]:
                raise RecoveryContractError('Invalid retained raw bytes')
            if row[1]:
                if row[2] != retained:
                    raise RecoveryContractError('Raw disk settlement changed')
                return
            if successful_attempt is not None:
                attempt=db.execute('SELECT finalized FROM runtime_raw_attempts WHERE scope_id=? AND attempt=?',
                                   (self.scope_id,successful_attempt)).fetchone()
                if attempt is None or not attempt[0]:
                    raise RecoveryContractError('Published raw object has no drained transfer fact')
                db.execute('UPDATE runtime_raw_attempts SET disk_bytes=?,disk_closed=1 WHERE scope_id=? AND attempt=?',
                           (retained,self.scope_id,successful_attempt))
            unknown=db.execute('SELECT COALESCE(SUM(disk_bytes),0) FROM runtime_raw_attempts WHERE scope_id=? AND disk_closed=0',
                               (self.scope_id,)).fetchone()[0]
            charged=retained+unknown
            if charged>row[0]:
                raise RecoveryContractError('Raw disk facts exceed reserved capacity')
            owner=db.execute('SELECT owner FROM runtime_raw_attempts WHERE scope_id=? ORDER BY attempt DESC LIMIT 1',(self.scope_id,)).fetchone()
            if owner is None:
                owner=db.execute('SELECT owner FROM runtime_raw_scopes WHERE scope_id=?',(self.scope_id,)).fetchone()
            budget_delta(db,'output_admission_bytes',charged-row[0],self.run.limits['max_output_bytes'],owner=owner[0])
            budget_delta(db,'raw_disk_bytes',charged-row[0],self.run.limits['max_scratch_bytes'],owner=owner[0])
            budget_delta(db,'output_bytes',retained,self.run.limits['max_output_bytes'],owner=owner[0])
            budget_delta(db,'object_bytes',retained,self.run.limits['max_output_bytes'],owner=owner[0])
            db.execute('UPDATE runtime_raw_scopes SET disk=?,settled=1,retained=? WHERE scope_id=?',(charged,retained,self.scope_id))

    def totals(self):
        with self.store.execution_guard(self.run.fence) as db:
            rows = db.execute('SELECT attempt,requests,used,finalized FROM runtime_raw_attempts WHERE scope_id=? ORDER BY attempt LIMIT 9',(self.scope_id,)).fetchall()
            if len(rows) > 8:
                raise RecoveryContractError('Raw attempt history exceeds bound')
            return dict(attempts=len(rows),http_requests=sum(r[1] for r in rows))
