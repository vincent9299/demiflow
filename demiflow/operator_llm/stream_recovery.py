"""Native row-local replay using the existing SQLite prompt journal.

The run owns one bounded group writer in its existing database. HTTP clients
retain their request/response contract; no per-image commit scopes or files are
created. Only model-specific code interprets saved media references.
"""
import json
import asyncio
import contextlib
import inspect
import time
import uuid
from contextvars import ContextVar
from pathlib import Path
from urllib.parse import unquote, urlsplit

from .sqlite_journal import _SQLiteJournalShard
from .journal import RequestRecord
from ..execution.recovery_contract import RecoveryContractError, identity


CALL_RESERVATION = ContextVar('native_prompt_budget_reservation', default=None)


@contextlib.contextmanager
def native_request_budget(run, *, node_id, max_requests):
    # The reservation is only staged here; RunPromptJournal applies it in the
    # same savepoint as the request intent. None inherits the run ceiling while
    # an explicit 0 forbids fresh dispatch, so the two must not collapse.
    if max_requests is not None and (type(max_requests) is not int or max_requests < 0):
        raise RecoveryContractError('Native request budget ceiling must be a non-negative integer or None')
    run_ceiling = run.limits['max_requests']
    node_ceiling = run_ceiling if max_requests is None else max_requests
    reservation = 'operator-llm-' + uuid.uuid4().hex
    charges = {'requests': (1, run_ceiling),
               'requests-' + identity(node_id): (1, node_ceiling)}
    run.check_stop()
    token = CALL_RESERVATION.set((reservation, charges))
    try:
        yield reservation
    finally:
        CALL_RESERVATION.reset(token)


class _CapacityPending(Exception):
    """No intent was written: a live response can release admission capacity."""


def _confirmed_unsent(error):
    call = (json.loads(error).get('call') or {}) if error else {}
    return call.get('request_sent') is False and call.get('batch_cancelled_before_dispatch') is True


def _stable_metadata(value):
    """JSON journal key ordering must not change an inferred Arrow struct."""
    if type(value) is dict:
        return {key: _stable_metadata(value[key]) for key in sorted(value)}
    if type(value) is list:
        return [_stable_metadata(item) for item in value]
    return value


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
        from .journal import UncertainPromptCall
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
        from .journal import UncertainPromptCall
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
        from ..execution.run_journal import ExecutionConflict
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
            from ..execution.run_journal import ExecutionBudgetExceeded
            try:
                self.run.journal.reserve_budgets_in_transaction(db, reservation, charges)
            except ExecutionBudgetExceeded as error:
                if error.budget != 'requests' and not error.budget.startswith('requests-'):
                    raise
                # The same finite model quota applies to first execution and
                # replay. Other contract/fence/control-storage failures remain
                # fatal, and the enclosing savepoint rolls back this intent.
                from .errors import PromptBudgetExceededError
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


class _BorrowedJournal:
    """Clients drain their I/O pools; only the run closes the shared writer."""
    def __init__(self, journal):
        self.journal = journal

    def __getattr__(self, name):
        return getattr(self.journal, name)

    def close(self):
        pass


class NativePromptRecovery:
    def __init__(self, actor, action, node_id):
        self.actor, self.action, self.node_id = actor, action, node_id
        run = action.context
        journal = getattr(run, '_native_prompt_journal', None)
        if journal is None:
            journal = run._native_prompt_journal = RunPromptJournal(run)
            journal.check_recovery()
        journal.stores.update(getattr(action, 'object_sinks', {}))
        self.journal = journal
        action.native_image_effects = True
        predicate = getattr(actor, 'when', None)
        self.semantic_owners = (() if predicate is None or inspect.isfunction(predicate) or inspect.isbuiltin(predicate)
                                else (predicate,))
        self.row_local = not self.semantic_owners
        self.copy_inputs = predicate is not None

    def delivery_trace(self, trace):
        """Keep native call_output bound to its original provider execution.

        The journal key includes this action/node/row occurrence. A lookup is
        replay of that original call, not a new business result. Attempt-local
        cache hits are observed BEFORE this projection by PromptActor; they
        must not change a downstream row or its native commit digest.

        Private journals and offline inputs never install this binding, and
        retain their existing delivery semantics. Only trace-owned flags are
        projected; response bodies and arbitrary user metadata are untouched.
        """
        if not getattr(self, '_native_delivery_bound', False):
            return trace
        def original(call):
            return {**call, 'reused': False} if 'reused' in call else dict(call)
        result = original(trace)
        if 'attempts' in trace:
            result['attempts'] = [original(call) for call in trace['attempts']]
        return result

    def bind_client(self, client):
        # Explicit request journals remain the authoritative caller-selected
        # cache. They already persist model responses before returning them.
        if getattr(client, 'journal', None) is not None:
            return
        if not hasattr(client, 'record'):
            raise RecoveryContractError('Native asynchronous recovery needs a journal-capable transport')
        client.journal = _BorrowedJournal(self.journal)
        self.actor._runtime.coordinator._native_group_journal = True
        bound = (128 * 1024 if client.model.provider else
                 client.options.get('max_response_bytes', 8 * 1024**2) + 64 * 1024)
        if bound > self.journal.max_record_bytes:
            raise RecoveryContractError('Native response ceiling exceeds the admitted journal record budget')
        record = client.record
        from .client import _PreparedCall
        def scoped(request):
            if isinstance(request, _PreparedCall):
                return request.source
            from demiflow.execution.row_scope import current_row_scope
            scope = current_row_scope()
            if scope is None:
                raise RecoveryContractError('Native model request lost its source occurrence')
            return RequestRecord({**record(request), 'native_response_bytes': bound, 'native_occurrence': identity([
                self.action.action_id, self.node_id, scope.occurrence])})
        client.record = scoped
        execute = client.execute
        async def admitted(request):
            while True:
                self.action.context.check_stop()
                try:
                    return await execute(request)
                except _CapacityPending:
                    # The failed admission wrote no intent and made no HTTP
                    # call. Release the journal pool slot so responses can save.
                    await asyncio.sleep(.01)
        client.execute = admitted
        decode = client.decode
        def saved(request, value, reused=False, refs=None):
            if reused:
                self.journal.media_references(value, register=True)
            response = decode(request, value, reused, refs)
            from dataclasses import replace
            content = response.content
            if client.model.provider and type(content) is dict and content.get('image') is not None:
                from ..objects import ObjectRef
                content = {**content, 'image': ObjectRef(**content['image']).to_dict()}
            return replace(response, content=content, metadata=_stable_metadata(response.metadata))
        client.decode = saved
        self._native_delivery_bound = True
