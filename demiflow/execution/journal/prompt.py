"""Run-owned model request ledger, reservations and protected result references."""
from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.parse import unquote, urlsplit
from demiflow.operator_llm.sqlite_journal import _SQLiteJournalShard
from demiflow.operator_llm.stream_recovery import CALL_RESERVATION
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.contracts.errors import _CapacityPending


def _confirmed_unsent(error):
    call = (json.loads(error).get('call') or {}) if error else {}
    return call.get('request_sent') is False and call.get('batch_cancelled_before_dispatch') is True

class _ProtectedPayload(tuple):
    def __new__(cls, payload, references):
        value = super().__new__(cls, payload)
        value.references = references
        return value

class RunPromptJournal(_SQLiteJournalShard):
    def __init__(self, run):
        super().__init__(run.journal.path, max_requests=run.limits['max_requests'],
            throughput={'batch_size': 64, 'flush_interval_s': .005,
                        'max_record_bytes': min(16 * 1024**2, run.limits['max_control_bytes']),
                        'max_queue_bytes': 32 * 1024**2})
        self.run = run
        self.stores = {}

    def check_recovery(self):
        # One indexed startup check, before any new model dispatch. Complete
        # historical responses are not scanned or decoded on this path.
        from demiflow.operator_llm.journal import UncertainPromptCall
        with self._lock:
            db = self._connect()
            db.execute('CREATE INDEX IF NOT EXISTS native_unresolved_calls ON calls(request_key) WHERE response_json IS NULL')
            row = db.execute("""SELECT request_key FROM calls WHERE response_json IS NULL
                AND NOT (COALESCE(json_extract(error_json,'$.call.request_sent'),1)=0
                         AND COALESCE(json_extract(error_json,'$.call.batch_cancelled_before_dispatch'),0)=1)
                LIMIT 1""").fetchone()
        if row is not None:
            raise UncertainPromptCall('Request reserved without complete response: ' + row[0] +
                                      '; automatic regeneration is forbidden')

    def media_references(self, record, *, register=False):
        # A model response has a single generated image and optional input image.
        body = record.get('body')
        refs = [record.get('image'), body.get('image') if type(body) is dict else None, record.get('input_ref')]
        names = []
        for ref in refs:
            if type(ref) is not dict or set(ref) != {'uri', 'sha256'}:
                continue
            uri = urlsplit(ref['uri'])
            if uri.scheme != 'file':
                continue
            path, sha = Path(unquote(uri.path)), ref['sha256']
            for store in tuple(self.stores.values()):
                name = store.reference_name(path, sha, register=register)
                if name is None:
                    continue
                names.append(name)
                break
        return tuple(names)

    def _lookup(self, *args, **kwargs):
        from demiflow.operator_llm.journal import UncertainPromptCall
        try:
            return super()._lookup(*args, **kwargs)
        except UncertainPromptCall as error:
            call = getattr(error, 'call', {})
            if call.get('request_sent') is False and call.get('batch_cancelled_before_dispatch') is True:
                return None, None
            raise UncertainPromptCall(str(error) + '; automatic regeneration is forbidden') from error

    def _enqueue(self, operation, payload, size):
        if size > self.max_record_bytes:
            raise RecoveryContractError('Native request/result exceeds record byte budget')
        if operation == 'reserve':
            payload = _ProtectedPayload(payload, ())
            payload.budget = CALL_RESERVATION.get()
            payload.response_bound = json.loads(payload[1]).get('native_response_bytes', self.max_record_bytes)
        if operation == 'save' and payload[1] == 'response_json':
            payload = _ProtectedPayload(payload, self.media_references(json.loads(payload[2])))
        return super()._enqueue(operation, payload, size)

    def _apply_operation(self, db, item, effects):
        from demiflow.execution.journal.run_journal import ExecutionConflict
        owner = db.execute('SELECT attempt,fence FROM runtime_owner').fetchone()
        if owner != (self.run.attempt, self.run.fence):
            raise ExecutionConflict('Stale execution owner cannot persist a model result')
        if item.operation == 'reserve':
            key = item.payload[0]
            prior = db.execute('SELECT request_json,response_json,error_json FROM calls WHERE request_key=?', (key,)).fetchone()
            if prior is not None:
                if prior[1] is not None or not _confirmed_unsent(prior[2]):
                    return super()._apply_operation(db, item, effects)
                if db.execute('SELECT 1 FROM writer_leases WHERE request_key=?', (key,)).fetchone():
                    raise RecoveryContractError('Confirmed-unsent request still has a live writer')
                attempt = db.execute('SELECT COALESCE(MAX(attempt),0)+1 FROM recovery_events WHERE request_key=?', (key,)).fetchone()[0]
                db.execute('INSERT INTO recovery_events(operation_id,request_key,actor,reason,recovered_at,request_json,error_json,attempt) VALUES (?,?,?,?,?,?,?,?)',
                    ('native-unsent:' + key + ':' + str(attempt), key, 'native-stream-recovery',
                     'Transport durably confirmed cancellation before dispatch', time.time(), prior[0], prior[2], attempt))
                db.execute('DELETE FROM calls WHERE request_key=?', (key,))
                old_bound = json.loads(prior[0])['native_response_bytes']
                db.execute("UPDATE journal_state SET value=CAST(value AS INTEGER)-? WHERE name='native_response_reserved'", (old_bound,))
            bound = item.payload.response_bound
            if type(bound) is not int or not 1 <= bound <= self.max_record_bytes:
                raise RecoveryContractError('Native response admission exceeds record budget')
            db.execute("INSERT OR IGNORE INTO journal_state VALUES ('native_response_reserved','0')")
            reserved = int(db.execute("SELECT value FROM journal_state WHERE name='native_response_reserved'").fetchone()[0])
            page_size = db.execute('PRAGMA page_size').fetchone()[0]
            pages = db.execute('PRAGMA page_count').fetchone()[0]
            if pages * page_size + reserved + item.size * 2 + bound > self.run.limits['max_control_bytes']:
                if reserved and (self._leases or any(effects.values())):
                    raise _CapacityPending()
                raise RecoveryContractError('Native request/result control budget exhausted before dispatch')
            if item.payload.budget is None:
                raise RecoveryContractError('Native request has no execution budget reservation')
            reservation, charges = item.payload.budget
            from demiflow.execution.contracts.errors import ExecutionBudgetExceeded
            try:
                self.run.journal.reserve_budgets_in_transaction(db, reservation, charges)
            except ExecutionBudgetExceeded as error:
                if error.budget != 'requests' and not error.budget.startswith('requests-'):
                    raise
                # The same finite model quota applies to first execution and
                # replay. Other contract/fence/control-storage failures remain
                # fatal, and the enclosing savepoint rolls back this intent.
                from demiflow.operator_llm.errors import PromptBudgetExceededError
                raise PromptBudgetExceededError('Operator LLM request budget exhausted') from error
            db.execute("UPDATE journal_state SET value=? WHERE name='native_response_reserved'", (str(reserved + bound),))
        else:
            key, column, _ = item.payload
            prior = db.execute('SELECT response_json IS NULL,request_json FROM calls WHERE request_key=?', (key,)).fetchone()
            if column == 'response_json' and prior is not None and prior[0]:
                bound = json.loads(prior[1]).get('native_response_bytes', self.max_record_bytes)
                if item.size > bound:
                    raise RecoveryContractError('Native response exceeded its admitted bound')
                db.execute("UPDATE journal_state SET value=CAST(value AS INTEGER)-? WHERE name='native_response_reserved'", (bound,))
        for name in getattr(item.payload, 'references', ()):
            if db.execute('SELECT path FROM runtime_artifacts WHERE path=?', (name,)).fetchone() is None:
                raise RecoveryContractError('Native result lost its owned object registration')
            db.execute('UPDATE runtime_artifacts SET protected=1 WHERE path=?', (name,))
        # The parent publishes in-memory leases only at the end. No fallible
        # extension work may follow it: savepoint rollback must leave them intact.
        return super()._apply_operation(db, item, effects)
