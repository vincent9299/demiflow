"""RunResourceState: resource intent CAS, dedup, native fence, pending external
operations, observations/events and close tombstones inside the existing
RunJournal storage and transactions.

This is the only implementation of the resource namespace schema. Tables are
created lazily (the runtime_commit_scopes precedent); existing run_journal
tables and their cumulative budget semantics are never altered. Resource rows
are control facts, not business effects, result commits or source ACKs.
"""
from contextlib import contextmanager
import json
import time

from demiflow.errors import ExecutionConflict
from demiflow.resources.contracts import ACCELERATOR_BUDGET_PREFIX, MAX_DOCUMENT_BYTES, ResourceError, ResourceScope, TERMINAL_HOST_PHASES, accelerator_budget_key, document, identifier, spec_hash, validate_observation
from demiflow.execution.contracts.recovery_contract import RecoveryContractError

_CLEANUP_RANK = {"NONE": 0, "REQUESTED": 1, "UNCONFIRMED": 2, "CONFIRMED": 3}
_OBSERVATION_KEYS = (
    "allocation_id", "provider_uid", "boot_id", "phase", "observation",
    "observed_at_ms", "cleanup", "effective_resources", "conditions",
    "evidence_ref", "placement", "ownership", "observation_revision",
)
_CLEANUP_ENTRY_KEYS = frozenset({
    "allocation_id", "cleanup", "observed_at_ms", "observation_revision",
    "evidence_ref", "conditions",
})
_STALE_ENTRY_KEYS = frozenset({
    "allocation_id", "observation", "observed_at_ms", "observation_revision",
    "evidence_ref", "conditions",
})
_SCHEMA = """
CREATE TABLE IF NOT EXISTS resource_scopes (
  scope_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, document TEXT NOT NULL,
  state TEXT NOT NULL, desired_revision INTEGER NOT NULL,
  observed_revision INTEGER NOT NULL, close_document TEXT
);
CREATE TABLE IF NOT EXISTS resource_allocations (
  allocation_id TEXT PRIMARY KEY, scope_id TEXT NOT NULL, spec_hash TEXT NOT NULL,
  spec TEXT NOT NULL, effective TEXT NOT NULL, deadline_ms INTEGER NOT NULL,
  phase TEXT NOT NULL, cleanup TEXT NOT NULL, terminal INTEGER NOT NULL,
  desired INTEGER NOT NULL, observation_revision INTEGER NOT NULL,
  observed_at_ms INTEGER NOT NULL, document TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS resource_operations (
  scope_id TEXT NOT NULL, operation_id TEXT NOT NULL, kind TEXT NOT NULL,
  payload_hash TEXT NOT NULL, receipt TEXT NOT NULL, created_ms INTEGER NOT NULL,
  expires_ms INTEGER NOT NULL, PRIMARY KEY(scope_id, operation_id)
);
CREATE TABLE IF NOT EXISTS resource_intents (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT, scope_id TEXT NOT NULL,
  allocation_id TEXT NOT NULL, kind TEXT NOT NULL, document TEXT NOT NULL,
  status TEXT NOT NULL, desired_revision INTEGER NOT NULL, created_ms INTEGER NOT NULL,
  claimed_epoch INTEGER, claimed_until_ms INTEGER
);
CREATE TABLE IF NOT EXISTS resource_events (
  cursor INTEGER PRIMARY KEY AUTOINCREMENT, scope_id TEXT NOT NULL,
  event_id TEXT NOT NULL UNIQUE, document TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS resource_allocations_scope ON resource_allocations(scope_id);
CREATE INDEX IF NOT EXISTS resource_intents_claim ON resource_intents(status, sequence);
CREATE INDEX IF NOT EXISTS resource_events_scope ON resource_events(scope_id, cursor);
"""


def _now_ms():
    return int(time.time() * 1000)


def _merge_cleanup(old, new):
    if new is None or new == "NONE" or old == "CONFIRMED":
        return old
    return new if _CLEANUP_RANK[new] >= _CLEANUP_RANK[old] else old


class RunResourceState:
    def __init__(self, journal, *, event_window=1024, operation_retention_ms=3600000,
                 claim_lease_ms=30000, page_size=64, max_allocations=4096,
                 max_operations=4096, cleanup_scope=None, operation_scope=None):
        self.journal = journal
        self.event_window = event_window
        self.operation_retention_ms = operation_retention_ms
        self.claim_lease_ms = claim_lease_ms
        self.page_size = page_size
        self.max_allocations = max_allocations
        self.max_operations = max_operations
        self.operation_scope = identifier(operation_scope) if operation_scope else None
        self.cleanup_scope = None if cleanup_scope is None else identifier(cleanup_scope)

    @property
    def cleanup_only(self):
        return self.cleanup_scope is not None

    def _check_cleanup_scope(self, scope_id):
        if self.cleanup_only and scope_id != self.cleanup_scope:
            raise ResourceError('FENCED', 'cleanup is restricted to its original resource scope')

    @contextmanager
    def _guard(self, fence, scope_id=None):
        try:
            if scope_id is not None:
                self._check_cleanup_scope(scope_id)
            guard = (self.journal._resource_cleanup_guard if self.cleanup_only
                     else self.journal.execution_guard)
            with guard(fence) as db:
                yield db
        except ExecutionConflict as exc:
            raise ResourceError("FENCED", str(exc), scope_id=scope_id) from exc
        except RecoveryContractError as exc:
            raise ResourceError("QUOTA_EXCEEDED", str(exc), scope_id=scope_id, retriable=True) from exc

    @staticmethod
    def _ensure_tables(db):
        db.executescript(_SCHEMA)

    def _existing_cleanup_tables(self, db):
        if self.cleanup_only:
            # executescript would commit the already-fenced transaction.
            # Recovery never creates a resource namespace or runs DDL.
            if not self._tables_exist(db):
                raise ResourceError('INVALID_SPEC', 'original resource namespace is unavailable')
        else:
            self._ensure_tables(db)

    def _document_column(self, name, *, bounded=None):
        bounded = self.cleanup_only if bounded is None else bounded
        return f'substr(CAST({name} AS BLOB),1,{MAX_DOCUMENT_BYTES + 1})' if bounded else name

    def _read_document(self, raw, *, bounded=None):
        bounded = self.cleanup_only if bounded is None else bounded
        if bounded and len(raw) > MAX_DOCUMENT_BYTES:
            raise ResourceError('QUOTA_EXCEEDED', 'original resource control document exceeds byte budget')
        return json.loads(raw)

    @staticmethod
    def _tables_exist(db):
        return db.execute("SELECT 1 FROM sqlite_master WHERE name='resource_scopes'").fetchone() is not None

    def _epoch(self, db):
        column = self._document_column('document')
        return self._read_document(db.execute(f"SELECT {column} FROM execution_state").fetchone()[0])["attempt"]

    def _check_epoch(self, db, context):
        if context.controller_epoch != self._epoch(db):
            raise ResourceError("FENCED", "controller_epoch is not the current native attempt",
                                scope_id=context.scope_id)

    def _scope_row(self, db, scope_id, *, bounded=None):
        columns = ','.join(self._document_column(name, bounded=bounded) for name in ('document', 'close_document'))
        row = db.execute("SELECT scope_id,run_id,state,desired_revision,observed_revision," + columns + " "
                         "FROM resource_scopes WHERE scope_id=?", (scope_id,)).fetchone()
        if row is None:
            raise ResourceError("INVALID_SPEC", "unknown resource scope", scope_id=scope_id)
        return {"scope_id": row[0], "run_id": row[1], "state": row[2],
                "desired_revision": row[3], "observed_revision": row[4],
                "document": self._read_document(row[5], bounded=bounded),
                "close_document": self._read_document(row[6], bounded=bounded) if row[6] else None}

    def scope(self, scope_id):
        identifier(scope_id)
        with self.journal._connection() as db:
            if not self._tables_exist(db) or db.execute(
                    'SELECT 1 FROM resource_scopes WHERE scope_id=?', (scope_id,)).fetchone() is None:
                return None
            return self._scope_row(db, scope_id, bounded=True)

    def unfinished_scopes(self, run_id, *, after=''):
        """One bounded original-Journal page; never scan deployment-wide pools."""
        identifier(run_id)
        with self.journal._connection() as db:
            if not self._tables_exist(db):
                return []
            rows = db.execute("SELECT scope_id FROM resource_scopes WHERE run_id=? AND state!='CLOSED' "
                              "AND scope_id>? ORDER BY scope_id LIMIT 64", (run_id, after)).fetchall()
            return [self._scope_row(db, row[0], bounded=True) for row in rows]

    def cleanup_complete(self, scope_id):
        self._check_cleanup_scope(scope_id)
        with self.journal._connection() as db:
            row = db.execute('SELECT state FROM resource_scopes WHERE scope_id=?', (scope_id,)).fetchone()
        if row is None:
            raise ResourceError('INVALID_SPEC', 'unknown resource scope', scope_id=scope_id)
        return row[0] == 'CLOSED'

    def _allocation_row(self, db, allocation_id):
        spec, effective, observation = (self._document_column(name) for name in ('spec', 'effective', 'document'))
        row = db.execute(f"SELECT allocation_id,scope_id,spec_hash,{spec},{effective},deadline_ms,phase,cleanup,"
                         f"terminal,desired,observation_revision,observed_at_ms,{observation} "
                         "FROM resource_allocations WHERE allocation_id=?", (allocation_id,)).fetchone()
        if row is None:
            raise ResourceError("INVALID_SPEC", "unknown allocation", allocation_id=allocation_id)
        return {"allocation_id": row[0], "scope_id": row[1], "spec_hash": row[2],
                "spec": self._read_document(row[3]), "effective": self._read_document(row[4]),
                "deadline_ms": row[5], "phase": row[6], "cleanup": row[7],
                "terminal": bool(row[8]), "desired": bool(row[9]),
                "observation_revision": row[10], "observed_at_ms": row[11],
                "document": self._read_document(row[12])}

    def _emit_event(self, db, scope_id, kind, *, allocation_id=None, desired_revision=0,
                    observation_revision=0, reason="", evidence_ref=None):
        observed_at = _now_ms()
        event_id = spec_hash([scope_id, kind, allocation_id, desired_revision,
                              observation_revision, reason])[:32]
        envelope = {"event_id": event_id, "scope_id": scope_id, "cursor": None,
                    "allocation_id_if_any": allocation_id, "desired_revision": desired_revision,
                    "observation_revision": observation_revision, "kind": kind,
                    "observed_at": observed_at, "reason": reason[:256],
                    "evidence_ref": evidence_ref}
        raw = document(envelope).decode()
        db.execute("INSERT OR IGNORE INTO resource_events(scope_id,event_id,document) VALUES (?,?,?)",
                   (scope_id, event_id, raw))
        column = self._document_column('document')
        row = db.execute(f"SELECT cursor,{column} FROM resource_events WHERE event_id=?", (event_id,)).fetchone()
        envelope = self._read_document(row[1])
        envelope["cursor"] = row[0]
        db.execute("UPDATE resource_events SET document=? WHERE event_id=?", (document(envelope).decode(), event_id))
        maximum = db.execute("SELECT MAX(cursor) FROM resource_events").fetchone()[0] or 0
        db.execute("DELETE FROM resource_events WHERE cursor<=?", (maximum - self.event_window,))
        return row[0]

    def _dedup(self, db, context, kind, payload_hash, now_ms):
        """Return a stored receipt for an identical retry, or None. Conflicts raise."""
        column = self._document_column('receipt')
        row = db.execute(f"SELECT kind,payload_hash,{column},expires_ms FROM resource_operations "
                         "WHERE scope_id=? AND operation_id=?",
                         (context.scope_id, context.operation_id)).fetchone()
        if row is None:
            return None
        if row[0] != kind or row[1] != payload_hash:
            raise ResourceError("IDEMPOTENCY_CONFLICT", "operation_id reused with a different payload",
                                scope_id=context.scope_id, operation_id=context.operation_id)
        if row[3] < now_ms:
            raise ResourceError("EXPIRED_OPERATION", "operation receipt retention expired; query native scope authority",
                                scope_id=context.scope_id, operation_id=context.operation_id)
        return self._read_document(row[2])

    def _store_receipt(self, db, context, kind, payload_hash, receipt, now_ms):
        db.execute("DELETE FROM resource_operations WHERE expires_ms<?", (now_ms,))
        count = db.execute("SELECT count(*) FROM resource_operations").fetchone()[0]
        if count >= self.max_operations:
            raise ResourceError("QUOTA_EXCEEDED", "operation dedup table exceeds budget",
                                scope_id=context.scope_id, retriable=True)
        db.execute("INSERT INTO resource_operations VALUES (?,?,?,?,?,?,?)",
                   (context.scope_id, context.operation_id, kind, payload_hash,
                    document(receipt).decode(), now_ms, now_ms + self.operation_retention_ms))

    def _new_intent(self, db, scope_id, allocation_id, kind, spec, revision, now_ms):
        db.execute("INSERT INTO resource_intents(scope_id,allocation_id,kind,document,status,"
                   "desired_revision,created_ms,claimed_epoch,claimed_until_ms) "
                   "VALUES (?,?,?,?,'pending',?,?,NULL,NULL)",
                   (scope_id, allocation_id, kind, document(spec).decode(), revision, now_ms))

    def _pending_absent(self, db, scope_id, allocation_id):
        return db.execute("SELECT 1 FROM resource_intents WHERE scope_id=? AND allocation_id=? "
                          "AND kind='ensure_absent' AND status IN ('pending','claimed') LIMIT 1",
                          (scope_id, allocation_id)).fetchone() is not None

    def _finish_intents(self, db, scope_id, allocation_id, kind):
        db.execute("UPDATE resource_intents SET status='done' WHERE scope_id=? AND allocation_id=? "
                   "AND kind=? AND status IN ('pending','claimed')", (scope_id, allocation_id, kind))

    def _advance_observed(self, db, scope_id):
        scope = self._scope_row(db, scope_id)
        outstanding = db.execute("SELECT MIN(desired_revision) FROM resource_intents "
                                 "WHERE scope_id=? AND status IN ('pending','claimed')", (scope_id,)).fetchone()[0]
        target = scope["desired_revision"] if outstanding is None else outstanding - 1
        if target > scope["observed_revision"]:
            db.execute("UPDATE resource_scopes SET observed_revision=? WHERE scope_id=?", (target, scope_id))

    def _check_closed(self, db, scope_id):
        scope = self._scope_row(db, scope_id)
        if scope["state"] != "CLOSING":
            return
        unconfirmed = db.execute("SELECT 1 FROM resource_allocations WHERE scope_id=? AND cleanup!='CONFIRMED' LIMIT 1",
                                 (scope_id,)).fetchone()
        outstanding = db.execute("SELECT 1 FROM resource_intents WHERE scope_id=? AND status IN ('pending','claimed') LIMIT 1",
                                 (scope_id,)).fetchone()
        if unconfirmed is None and outstanding is None:
            db.execute("UPDATE resource_scopes SET state='CLOSED' WHERE scope_id=?", (scope_id,))
            self._emit_event(db, scope_id, "scope_closed", desired_revision=scope["desired_revision"],
                             reason="all cleanup obligations confirmed")

    def open_scope(self, fence, scope):
        if self.cleanup_only:
            raise ResourceError('PERMISSION_DENIED', 'cleanup cannot open a resource scope')
        if type(scope) is not ResourceScope:
            raise ResourceError("INVALID_SPEC", "open_scope needs a ResourceScope")
        raw = document(scope.to_dict()).decode()
        with self._guard(fence, scope.scope_id) as db:
            self._ensure_tables(db)
            row = db.execute("SELECT document,state,desired_revision FROM resource_scopes WHERE scope_id=?",
                             (scope.scope_id,)).fetchone()
            if row is not None:
                if row[0] != raw:
                    raise ResourceError("INVALID_SPEC", "resource scope is immutable", scope_id=scope.scope_id)
                return {"scope_id": scope.scope_id, "state": row[1], "desired_revision": row[2]}
            db.execute("INSERT INTO resource_scopes VALUES (?,?,?,'OPEN',0,0,NULL)",
                       (scope.scope_id, scope.run_id, raw))
            self._emit_event(db, scope.scope_id, "scope_opened", reason="scope created")
        return {"scope_id": scope.scope_id, "state": "OPEN", "desired_revision": 0}

    def accept_plan(self, context, validated):
        if self.cleanup_only:
            raise ResourceError('PERMISSION_DENIED', 'cleanup cannot admit a resource plan')
        scope_id = context.scope_id
        now_ms = _now_ms()
        payload_hash = validated["payload_hash"]
        with self._guard(context.authority_token, scope_id) as db:
            self._ensure_tables(db)
            self._check_epoch(db, context)
            receipt = self._dedup(db, context, "apply", payload_hash, now_ms)
            if receipt is not None:
                return receipt
            scope = self._scope_row(db, scope_id)
            if scope["state"] != "OPEN":
                raise ResourceError("SCOPE_CLOSED", "scope no longer accepts plans", scope_id=scope_id)
            if context.expected_revision != scope["desired_revision"]:
                raise ResourceError("REVISION_CONFLICT", "expected_revision is stale", scope_id=scope_id,
                                    observed_revision=scope["desired_revision"])
            plan = validated["plan"]
            workers = validated["workers"]
            if plan["scope_id"] != scope_id:
                raise ResourceError("INVALID_SPEC", "plan scope differs", scope_id=scope_id)
            existing = {r["allocation_id"]: r for r in self._scope_allocations(db, scope_id,
                include_ids=tuple(workers) + tuple(r["allocation_id"] for r in plan["retirements"]), active_only=True)}
            budget = scope["document"]["budget"]
            ceiling = plan.get("budgets") or budget
            if plan.get("budgets"):
                for key in ("cpu_millis", "memory_bytes", "max_workers", "max_creates_per_minute"):
                    if key in ceiling and key in budget and ceiling[key] > budget[key]:
                        raise ResourceError("INVALID_SPEC", "plan budget exceeds authorized scope ceiling", scope_id=scope_id)
                for key in ceiling:
                    if (key == "scratch_bytes" or key.startswith(ACCELERATOR_BUDGET_PREFIX)) \
                            and ceiling[key] > budget.get(key, 0):
                        raise ResourceError("INVALID_SPEC", "plan budget exceeds authorized scope ceiling", scope_id=scope_id)
            new_ids = [a for a in workers if a not in existing]
            if sum(r["cleanup"] != "CONFIRMED" for r in existing.values()) + len(new_ids) > self.max_allocations:
                raise ResourceError("QUOTA_EXCEEDED", "active scope allocation count exceeds budget", scope_id=scope_id)
            rate = ceiling.get("max_creates_per_minute")
            if rate is not None and new_ids:
                recent = db.execute("SELECT count(*) FROM resource_intents WHERE scope_id=? AND kind='ensure_present' "
                                    "AND created_ms>?", (scope_id, now_ms - 60000)).fetchone()[0]
                if recent + len(new_ids) > rate:
                    raise ResourceError("QUOTA_EXCEEDED", "create rate exceeds budget", scope_id=scope_id,
                                        retriable=True, retry_after_ms=60000)
            revision = scope["desired_revision"] + 1
            retirement_ids = {r["allocation_id"] for r in plan["retirements"]}
            for allocation_id, worker in workers.items():
                identifier(allocation_id)
                if allocation_id in existing:
                    row = existing[allocation_id]
                    if row["spec_hash"] != worker["spec_hash"]:
                        raise ResourceError("INVALID_SPEC", "allocation spec is immutable",
                                            scope_id=scope_id, allocation_id=allocation_id)
                    if row["terminal"] or row["cleanup"] == "CONFIRMED":
                        raise ResourceError("INVALID_SPEC", "terminal/released allocation cannot be desired again; use a new id",
                                            scope_id=scope_id, allocation_id=allocation_id)
                    db.execute("UPDATE resource_allocations SET desired=1 WHERE allocation_id=?", (allocation_id,))
                    continue
                immutable = {"scope_id": scope_id, "allocation_id": allocation_id,
                             "request": worker["request"], "spec_hash": worker["spec_hash"],
                             "effective": worker["effective"], "requirements": worker.get("requirements"),
                             "binding": scope["document"]["provider_binding"]}
                deadline_ms = now_ms + worker["request"]["provision_deadline_ms"]
                observation = {"allocation_id": allocation_id, "provider_uid": None, "boot_id": None,
                               "phase": "REQUESTED", "observation": "FRESH", "observed_at_ms": now_ms,
                               "observation_revision": 0, "cleanup": "NONE",
                               "effective_resources": worker["effective"], "conditions": [],
                               "evidence_ref": None, "placement": None,
                               "ownership": {"scope_owned": [allocation_id], "shared": []}}
                validate_observation(observation)
                db.execute("INSERT INTO resource_allocations VALUES (?,?,?,?,?,?,?,?,?,1,0,?,?)",
                           (allocation_id, scope_id, worker["spec_hash"], document(immutable).decode(),
                            document(worker["effective"]).decode(), deadline_ms, "REQUESTED", "NONE", 0,
                            now_ms, document(observation).decode()))
                self._new_intent(db, scope_id, allocation_id, "ensure_present", immutable, revision, now_ms)
            for allocation_id, row in existing.items():
                if allocation_id in workers or allocation_id in retirement_ids:
                    continue
                db.execute("UPDATE resource_allocations SET desired=0 WHERE allocation_id=?", (allocation_id,))
                if not row["terminal"]:
                    raise ResourceError("INVALID_SPEC", "removing a live worker requires a ReleaseSpec",
                                        scope_id=scope_id, allocation_id=allocation_id)
                if row["cleanup"] != "CONFIRMED" and not self._pending_absent(db, scope_id, allocation_id):
                    release = {"allocation_id": allocation_id, "mode": "ABANDON",
                               "engine_receipt_ref": ("terminal-drop-" + str(revision))[:128],
                               "terminate_deadline_ms": 30000, "reason": "terminal_dropped_from_desired"}
                    self._new_intent(db, scope_id, allocation_id, "ensure_absent", release, revision, now_ms)
            for release in plan["retirements"]:
                allocation_id = release["allocation_id"]
                if allocation_id not in existing:
                    raise ResourceError("INVALID_SPEC", "retirement targets an unknown allocation",
                                        scope_id=scope_id, allocation_id=allocation_id)
                row = existing[allocation_id]
                db.execute("UPDATE resource_allocations SET desired=0,cleanup=? WHERE allocation_id=?",
                           (_merge_cleanup(row["cleanup"], "REQUESTED"), allocation_id))
                if row["cleanup"] != "CONFIRMED" and not self._pending_absent(db, scope_id, allocation_id):
                    self._new_intent(db, scope_id, allocation_id, "ensure_absent", release, revision, now_ms)
            charged = self._charged(db, scope_id)
            for key in ("cpu_millis", "memory_bytes"):
                if charged[key] > ceiling.get(key, budget[key]):
                    raise ResourceError("QUOTA_EXCEEDED", "charged resources exceed scope budget: " + key,
                                        scope_id=scope_id, observed_revision=scope["desired_revision"])
            # Additive dimensions are fail-closed: a scope that never authorized
            # scratch/devices has a zero ceiling for them.
            for key in charged:
                if (key == "scratch_bytes" or key.startswith(ACCELERATOR_BUDGET_PREFIX)) \
                        and charged[key] > ceiling.get(key, budget.get(key, 0)):
                    raise ResourceError("QUOTA_EXCEEDED", "charged resources exceed scope budget: " + key,
                                        scope_id=scope_id, observed_revision=scope["desired_revision"])
            if "max_workers" in ceiling and charged["workers"] > ceiling["max_workers"]:
                raise ResourceError("QUOTA_EXCEEDED", "charged worker count exceeds scope budget", scope_id=scope_id)
            db.execute("UPDATE resource_scopes SET desired_revision=? WHERE scope_id=?", (revision, scope_id))
            db.execute("DELETE FROM resource_intents WHERE scope_id=? AND status IN ('done','superseded') "
                       "AND desired_revision<?", (scope_id, revision))
            self._emit_event(db, scope_id, "plan_accepted", desired_revision=revision,
                             reason=str(len(workers)) + " desired workers")
            receipt = {"operation_id": context.operation_id, "scope_id": scope_id,
                       "desired_revision": revision, "accepted_spec_hash": payload_hash,
                       "status": "ACCEPTED",
                       "warnings_with_effective_mapping": {a: w["effective"] for a, w in workers.items()}}
            placement_warnings = {a: w["warnings"] for a, w in workers.items() if w.get("warnings")}
            if placement_warnings:
                receipt["placement_warnings"] = placement_warnings
            self._store_receipt(db, context, "apply", payload_hash, receipt, now_ms)
        return receipt

    def _scope_allocations(self, db, scope_id, *, include_ids=(), active_only=False):
        predicate = "scope_id=?"
        params = [scope_id]
        if active_only:
            predicate += " AND (cleanup!='CONFIRMED'"
            if include_ids:
                predicate += " OR allocation_id IN (" + ",".join("?" for _ in include_ids) + ")"
                params.extend(include_ids)
            predicate += ")"
        rows = db.execute("SELECT allocation_id FROM resource_allocations WHERE " + predicate +
                          " ORDER BY allocation_id", params).fetchall()
        return [self._allocation_row(db, r[0]) for r in rows]

    def read_allocation(self, scope_id, allocation_id):
        """Exact original-journal observation, independent of history pagination."""
        identifier(scope_id)
        identifier(allocation_id)
        with self.journal._connection() as db:
            row = db.execute("SELECT document FROM resource_allocations WHERE scope_id=? AND allocation_id=?",
                             (scope_id, allocation_id)).fetchone()
        return None if row is None else json.loads(row[0])

    def read_allocation_identity(self, scope_id, allocation_id):
        """Read original immutable ownership and its latest bounded observation."""
        identifier(scope_id)
        identifier(allocation_id)
        with self.journal._connection() as db:
            row = db.execute('SELECT spec_hash,document FROM resource_allocations '
                             'WHERE scope_id=? AND allocation_id=?',
                             (scope_id, allocation_id)).fetchone()
        return None if row is None else {'spec_hash': row[0], 'observation': json.loads(row[1])}

    def read_cleanup_allocation(self, scope_id, allocation_id):
        """Original immutable request and charge for scoped exit verification."""
        identifier(scope_id); identifier(allocation_id)
        self._check_cleanup_scope(scope_id)
        with self.journal._connection() as db:
            row = self._allocation_row(db, allocation_id)
        if row['scope_id'] != scope_id:
            raise ResourceError('FENCED', 'allocation belongs to another resource scope')
        document(row['spec']); document(row['effective'])
        return row

    def _charged(self, db, scope_id):
        rows = db.execute("SELECT effective FROM resource_allocations WHERE scope_id=? AND cleanup!='CONFIRMED'",
                          (scope_id,)).fetchall()
        total = {"cpu_millis": 0, "memory_bytes": 0, "workers": len(rows)}
        scratch = 0
        has_scratch = False
        for row in rows:
            effective = json.loads(row[0])
            total["cpu_millis"] += effective["cpu_millis"]
            total["memory_bytes"] += effective["memory_bytes"]
            if "scratch_bytes" in effective:
                has_scratch = True
                scratch += effective["scratch_bytes"]
            for item in effective.get("accelerators", []):
                key = accelerator_budget_key(item["type"])
                total[key] = total.get(key, 0) + item["count"]
        if has_scratch:
            total["scratch_bytes"] = scratch
        return total

    def begin_close(self, context, close_spec):
        scope_id = context.scope_id
        if self.cleanup_only and close_spec['mode'] != 'ABANDON':
            raise ResourceError('PERMISSION_DENIED', 'recovery cleanup requires ABANDON')
        now_ms = _now_ms()
        payload_hash = spec_hash(close_spec)
        with self._guard(context.authority_token, scope_id) as db:
            self._existing_cleanup_tables(db)
            self._check_epoch(db, context)
            receipt = self._dedup(db, context, "close", payload_hash, now_ms)
            if receipt is not None:
                return receipt
            scope = self._scope_row(db, scope_id)
            if scope["state"] != "OPEN":
                raise ResourceError("SCOPE_CLOSED", "scope is already closing or closed", scope_id=scope_id)
            if context.expected_revision != scope["desired_revision"]:
                raise ResourceError("REVISION_CONFLICT", "expected_revision is stale", scope_id=scope_id,
                                    observed_revision=scope["desired_revision"])
            revision = scope["desired_revision"] + 1
            db.execute("UPDATE resource_scopes SET state='CLOSING',desired_revision=?,close_document=? WHERE scope_id=?",
                       (revision, document(close_spec).decode(), scope_id))
            db.execute("UPDATE resource_intents SET status='superseded' WHERE scope_id=? AND kind='ensure_present' "
                       "AND status IN ('pending','claimed')", (scope_id,))
            for row in self._scope_allocations(db, scope_id, active_only=True):
                allocation_id = row["allocation_id"]
                if row["cleanup"] == "CONFIRMED":
                    continue
                db.execute("UPDATE resource_allocations SET cleanup=? WHERE allocation_id=?",
                           (_merge_cleanup(row["cleanup"], "REQUESTED"), allocation_id))
                if not self._pending_absent(db, scope_id, allocation_id):
                    release = {"allocation_id": allocation_id, "mode": close_spec["mode"],
                               "engine_receipt_ref": close_spec["engine_receipt_ref"],
                               "terminate_deadline_ms": close_spec["terminate_deadline_ms"],
                               "reason": close_spec["reason"]}
                    self._new_intent(db, scope_id, allocation_id, "ensure_absent", release, revision, now_ms)
            self._emit_event(db, scope_id, "scope_closing", desired_revision=revision, reason=close_spec["reason"])
            receipt = {"operation_id": context.operation_id, "scope_id": scope_id,
                       "desired_revision": revision, "accepted_spec_hash": payload_hash,
                       "status": "CLOSING", "warnings_with_effective_mapping": {}}
            self._store_receipt(db, context, "close", payload_hash, receipt, now_ms)
            self._check_closed(db, scope_id)
        return receipt

    def claim_operations(self, authority, now_ms, limit):
        if type(limit) is not int or limit <= 0:
            raise ResourceError("INVALID_SPEC", "claim limit must be positive")
        operations = []
        touched = set()
        with self._guard(authority) as db:
            self._existing_cleanup_tables(db)
            epoch = self._epoch(db)
            if self.cleanup_only:
                rows = db.execute("SELECT sequence,scope_id,allocation_id,kind,desired_revision FROM resource_intents "
                    "WHERE scope_id=? AND kind='ensure_absent' AND (status='pending' OR "
                    "(status='claimed' AND (claimed_until_ms<? OR claimed_epoch<?))) "
                    "ORDER BY sequence LIMIT ?", (self.cleanup_scope, now_ms, epoch, min(limit, 64))).fetchall()
            elif self.operation_scope is not None:
                rows = db.execute("SELECT sequence,scope_id,allocation_id,kind,desired_revision FROM resource_intents "
                    "WHERE scope_id=? AND (status='pending' OR (status='claimed' AND "
                    "(claimed_until_ms<? OR claimed_epoch<?))) ORDER BY sequence LIMIT ?",
                    (self.operation_scope, now_ms, epoch, limit)).fetchall()
            else:
                rows = db.execute("SELECT sequence,scope_id,allocation_id,kind,desired_revision FROM resource_intents "
                                  "WHERE status='pending' OR (status='claimed' AND claimed_until_ms<?) "
                                  "ORDER BY sequence LIMIT ?", (now_ms, limit)).fetchall()
            for sequence, scope_id, allocation_id, kind, revision in rows:
                touched.add(scope_id)
                scope = self._scope_row(db, scope_id)
                if self.cleanup_only and scope['state'] != 'CLOSING':
                    raise ResourceError('INVALID_SPEC', 'cleanup requires an original closing scope')
                alloc = self._allocation_row(db, allocation_id)
                if kind == "ensure_present":
                    if scope["state"] != "OPEN" or alloc["cleanup"] in ("REQUESTED", "CONFIRMED"):
                        db.execute("UPDATE resource_intents SET status='superseded' WHERE sequence=?", (sequence,))
                        continue
                    if alloc["phase"] in ("RUNNING",) or alloc["terminal"]:
                        db.execute("UPDATE resource_intents SET status='done' WHERE sequence=?", (sequence,))
                        continue
                    if alloc["phase"] in ("REQUESTED", "PENDING", "RESERVED", "STARTING") and now_ms > alloc["deadline_ms"]:
                        db.execute("UPDATE resource_intents SET status='expired' WHERE sequence=?", (sequence,))
                        release = {"allocation_id": allocation_id, "mode": "ABANDON",
                                   "engine_receipt_ref": ("provision-deadline-" + str(sequence))[:128],
                                   "terminate_deadline_ms": 30000, "reason": "PROVISION_DEADLINE_EXCEEDED"}
                        self._new_intent(db, scope_id, allocation_id, "ensure_absent", release, revision, now_ms)
                        new_sequence = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                        db.execute("UPDATE resource_intents SET status='claimed',claimed_epoch=?,claimed_until_ms=? WHERE sequence=?",
                                   (epoch, now_ms + self.claim_lease_ms, new_sequence))
                        self._emit_event(db, scope_id, "provision_deadline_exceeded", allocation_id=allocation_id,
                                         desired_revision=revision, reason="unfinished request past provision deadline")
                        operations.append(self._operation(db, new_sequence, scope_id, allocation_id, "ensure_absent", epoch))
                        continue
                    spec = alloc["spec"]
                else:
                    if alloc["cleanup"] == "CONFIRMED":
                        db.execute("UPDATE resource_intents SET status='done' WHERE sequence=?", (sequence,))
                        continue
                    column = self._document_column('document')
                    intent = db.execute(f"SELECT {column} FROM resource_intents WHERE sequence=?", (sequence,)).fetchone()
                    release = self._read_document(intent[0])
                    spec = {"terminate_deadline_ms": release["terminate_deadline_ms"],
                            "provider_uid": alloc["document"].get("provider_uid"),
                            "boot_id": alloc["document"].get("boot_id"), "release": release}
                db.execute("UPDATE resource_intents SET status='claimed',claimed_epoch=?,claimed_until_ms=? WHERE sequence=?",
                           (epoch, now_ms + self.claim_lease_ms, sequence))
                operations.append({"sequence": sequence, "scope_id": scope_id, "allocation_id": allocation_id,
                                   "kind": kind, "document": spec, "claimed_epoch": epoch})
            for scope_id in touched:
                self._advance_observed(db, scope_id)
        return operations

    def _operation(self, db, sequence, scope_id, allocation_id, kind, epoch):
        alloc = self._allocation_row(db, allocation_id)
        column = self._document_column('document')
        intent = db.execute(f"SELECT {column} FROM resource_intents WHERE sequence=?", (sequence,)).fetchone()
        release = self._read_document(intent[0])
        spec = {"terminate_deadline_ms": release["terminate_deadline_ms"],
                "provider_uid": alloc["document"].get("provider_uid"),
                "boot_id": alloc["document"].get("boot_id"), "release": release}
        return {"sequence": sequence, "scope_id": scope_id, "allocation_id": allocation_id,
                "kind": kind, "document": spec, "claimed_epoch": epoch}

    def record_observations(self, authority, observations):
        if type(observations) is not list or len(observations) > 256:
            raise ResourceError("INVALID_SPEC", "observation batch exceeds budget")
        if self.cleanup_only and any(type(entry) is not dict or 'cleanup' not in entry
                                    or set(entry) - _CLEANUP_ENTRY_KEYS for entry in observations):
            raise ResourceError('PERMISSION_DENIED', 'cleanup cannot record a worker admission observation')
        touched = set()
        with self._guard(authority) as db:
            self._existing_cleanup_tables(db)
            for entry in observations:
                if type(entry) is not dict or "allocation_id" not in entry:
                    raise ResourceError("INVALID_SPEC", "observation needs allocation_id")
                alloc = self._allocation_row(db, entry["allocation_id"])
                scope_id = alloc["scope_id"]
                self._check_cleanup_scope(scope_id)
                touched.add(scope_id)
                revision = entry.get("observation_revision")
                if type(revision) is not int or revision <= alloc["observation_revision"]:
                    continue
                merged = dict(alloc["document"])
                if "phase" in entry:
                    validate_observation(entry)
                    if alloc["terminal"] and entry["phase"] != alloc["phase"]:
                        continue
                    for key in _OBSERVATION_KEYS:
                        if key in entry and key != "cleanup":
                            merged[key] = entry[key]
                    merged["cleanup"] = _merge_cleanup(alloc["cleanup"], entry.get("cleanup"))
                    kind = "observation"
                elif "cleanup" in entry:
                    if set(entry) - _CLEANUP_ENTRY_KEYS or "observed_at_ms" not in entry:
                        raise ResourceError("INVALID_SPEC", "cleanup entry key set is unsupported")
                    if entry["cleanup"] not in ("REQUESTED", "CONFIRMED", "UNCONFIRMED"):
                        raise ResourceError("INVALID_SPEC", "invalid cleanup state")
                    if entry["cleanup"] == "CONFIRMED" and not entry.get("evidence_ref"):
                        raise ResourceError("INVALID_SPEC", "confirmed cleanup requires exit evidence")
                    merged["cleanup"] = _merge_cleanup(alloc["cleanup"], entry["cleanup"])
                    if entry.get("evidence_ref") is not None:
                        merged["evidence_ref"] = entry["evidence_ref"]
                    if entry.get("conditions") is not None:
                        merged["conditions"] = entry["conditions"]
                    merged["observed_at_ms"] = entry["observed_at_ms"]
                    kind = "cleanup"
                elif entry.get("observation") == "STALE_UNKNOWN":
                    if set(entry) - _STALE_ENTRY_KEYS or "observed_at_ms" not in entry:
                        raise ResourceError("INVALID_SPEC", "stale entry key set is unsupported")
                    merged["observation"] = "STALE_UNKNOWN"
                    if entry.get("conditions") is not None:
                        merged["conditions"] = entry["conditions"]
                    if entry.get("evidence_ref") is not None:
                        merged["evidence_ref"] = entry["evidence_ref"]
                    merged["observed_at_ms"] = entry["observed_at_ms"]
                    kind = "stale"
                else:
                    raise ResourceError("INVALID_SPEC", "observation entry shape is unsupported")
                merged["observation_revision"] = revision
                validate_observation(merged)
                terminal = merged["phase"] in TERMINAL_HOST_PHASES or merged["cleanup"] == "CONFIRMED"
                db.execute("UPDATE resource_allocations SET phase=?,cleanup=?,terminal=?,observation_revision=?,"
                           "observed_at_ms=?,document=? WHERE allocation_id=?",
                           (merged["phase"], merged["cleanup"], int(terminal), revision,
                            merged["observed_at_ms"], document(merged).decode(), entry["allocation_id"]))
                if merged["phase"] == "RUNNING" or terminal:
                    self._finish_intents(db, scope_id, entry["allocation_id"], "ensure_present")
                elif kind == 'observation' and merged['observation'] == 'FRESH' and merged['phase'] == 'PENDING':
                    # The provider returned: its I/O claim no longer needs the
                    # crash lease. Retry capacity with bounded backoff before
                    # the (often equal-length) provision deadline expires.
                    db.execute("UPDATE resource_intents SET claimed_until_ms=? "
                               "WHERE allocation_id=? AND kind='ensure_present' AND status='claimed'",
                               (int(time.time() * 1000) + min(100, self.claim_lease_ms), entry['allocation_id']))
                if merged["cleanup"] == "CONFIRMED":
                    self._finish_intents(db, scope_id, entry["allocation_id"], "ensure_absent")
                elif kind == 'cleanup' and entry['cleanup'] == 'UNCONFIRMED':
                    # Provider I/O has returned; retain the cleanup obligation
                    # and all charges, but do not hold its crash-recovery lease
                    # until the execution's equal-length close deadline. This
                    # is a retry of the original exit proof, never a refund.
                    db.execute("UPDATE resource_intents SET claimed_until_ms=? "
                               "WHERE allocation_id=? AND kind='ensure_absent' AND status='claimed'",
                               (int(time.time() * 1000) + min(100, self.claim_lease_ms), entry['allocation_id']))
                self._emit_event(db, scope_id, kind, allocation_id=entry["allocation_id"],
                                 observation_revision=revision,
                                 reason=";".join(merged.get("conditions") or [])[:256],
                                 evidence_ref=merged.get("evidence_ref"))
            for scope_id in touched:
                self._advance_observed(db, scope_id)
                self._check_closed(db, scope_id)
            cursor = db.execute("SELECT MAX(cursor) FROM resource_events").fetchone()[0] or 0
        return cursor

    def read_snapshot(self, scope_id, page_token):
        identifier(scope_id)
        # Composed reader over the journal's own read-only connection; BEGIN gives
        # one coherent view of intent rows and recorded observations per page.
        with self.journal._connection() as db:
            db.execute("BEGIN")
            if not self._tables_exist(db):
                raise ResourceError("INVALID_SPEC", "unknown resource scope", scope_id=scope_id)
            scope = self._scope_row(db, scope_id)
            maximum = db.execute("SELECT MAX(cursor) FROM resource_events").fetchone()[0] or 0
            count = db.execute("SELECT count(*) FROM resource_allocations WHERE scope_id=?", (scope_id,)).fetchone()[0]
            snapshot_id = spec_hash([scope["desired_revision"], scope["observed_revision"], maximum, count])[:32]
            after = None
            if page_token is not None:
                if type(page_token) is not str or ":" not in page_token:
                    raise ResourceError("RESYNC_REQUIRED", "invalid snapshot page token", scope_id=scope_id)
                token_snapshot, _, token_after = page_token.partition(":")
                if token_snapshot != snapshot_id:
                    raise ResourceError("RESYNC_REQUIRED", "snapshot changed; rebuild from the first page", scope_id=scope_id)
                after = token_after or None
            rows = db.execute("SELECT allocation_id,document FROM resource_allocations WHERE scope_id=? "
                              "AND (? IS NULL OR allocation_id>?) ORDER BY allocation_id LIMIT ?",
                              (scope_id, after, after, self.page_size + 1)).fetchall()
            page = rows[:self.page_size]
            has_more = len(rows) > self.page_size
            next_token = snapshot_id + ":" + page[-1][0] if has_more else None
            allocations = [json.loads(r[1]) for r in page]
            pending = [json.loads(r[0]) for r in db.execute(
                "SELECT document FROM resource_allocations WHERE scope_id=? AND desired=1 "
                "AND phase NOT IN ('RUNNING','TERMINATED','FAILED','LOST') ORDER BY allocation_id", (scope_id,))]
            obligations = [json.loads(r[0]) for r in db.execute(
                "SELECT document FROM resource_allocations WHERE scope_id=? AND cleanup!='CONFIRMED' "
                "AND (cleanup!='NONE' OR terminal=1) ORDER BY allocation_id", (scope_id,))]
            stale = [json.loads(r[0])["allocation_id"] for r in db.execute(
                "SELECT document FROM resource_allocations WHERE scope_id=? AND cleanup!='CONFIRMED'", (scope_id,))
                if json.loads(r[0]).get("observation") == "STALE_UNKNOWN"]
        return {"desired_revision": scope["desired_revision"], "observed_revision": scope["observed_revision"],
                "snapshot_id": snapshot_id, "cursor": maximum, "allocations": allocations,
                "pending_and_shortfall": [{"allocation_id": a["allocation_id"], "phase": a["phase"],
                                           "conditions": a.get("conditions", [])} for a in pending],
                "cleanup_obligations": [{"allocation_id": a["allocation_id"], "cleanup": a["cleanup"],
                                         "evidence_ref": a.get("evidence_ref")} for a in obligations],
                "freshness": {"sampled_at_ms": _now_ms(), "stale_unknown": stale},
                "next_page_token": next_token}

    def read_events(self, scope_id, cursor, limit):
        identifier(scope_id)
        if type(cursor) is not int or cursor < 0:
            raise ResourceError("INVALID_SPEC", "event cursor must be nonnegative")
        if type(limit) is not int or not 1 <= limit <= self.event_window:
            raise ResourceError("INVALID_SPEC", "event limit exceeds the window")
        with self.journal._connection() as db:
            db.execute("BEGIN")
            if not self._tables_exist(db):
                raise ResourceError("INVALID_SPEC", "unknown resource scope", scope_id=scope_id)
            self._scope_row(db, scope_id)
            minimum = db.execute("SELECT MIN(cursor) FROM resource_events").fetchone()[0]
            if minimum is not None and cursor < minimum - 1:
                raise ResourceError("RESYNC_REQUIRED", "event cursor fell out of the retained window; rebuild with get",
                                    scope_id=scope_id)
            rows = db.execute("SELECT document FROM resource_events WHERE scope_id=? AND cursor>? "
                              "ORDER BY cursor LIMIT ?", (scope_id, cursor, limit)).fetchall()
            events = [json.loads(r[0]) for r in rows]
        return {"events": events, "next_cursor": events[-1]["cursor"] if events else cursor, "resync": False}
