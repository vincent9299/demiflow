"""Native row-local embedding replay using the run-owned prompt journal.

Embedding HTTP requests are sent by EmbeddingActor._exchange/_fresh_exchange;
there is no PromptClient.record/execute/decode, so this adapter binds the actor
directly rather than copying NativePromptRecovery.bind_client. The run owns the
single bounded writer; the actor only borrows it and never closes it.
"""
import asyncio
import math

from ..operator_llm.journal import RequestRecord
from ..operator_llm.stream_recovery import RunPromptJournal, _BorrowedJournal
from ..execution.recovery_contract import RecoveryContractError, identity
from ..execution.row_scope import current_row_scope


class NativeEmbeddingRecovery:
    def __init__(self, actor, action, node_id):
        self.actor, self.action, self.node_id = actor, action, node_id
        run = action.context
        if actor.options.get('sqlite_journal') is not None:
            raise RecoveryContractError('Native embedding recovery cannot coexist with a private SQLite journal')
        if actor.options['max_response_bytes'] + 64 * 1024 > min(16 * 1024**2, run.limits['max_control_bytes']):
            raise RecoveryContractError('Native response ceiling exceeds the admitted journal record budget')
        journal = getattr(run, '_native_prompt_journal', None)
        if journal is None:
            journal = run._native_prompt_journal = RunPromptJournal(run)
            journal.check_recovery()
        journal.stores.update(getattr(action, 'object_sinks', {}))
        self.journal = journal
        self.semantic_owners = ()
        self.row_local = True
        self.copy_inputs = False
        self.inflight = {}
        self._response_bound = None

    def bind_actor(self):
        # A private SQLite journal is a second budget authority; refuse it
        # explicitly instead of silently dual-writing. The run owns the writer.
        if self.actor._journal is not None:
            raise RecoveryContractError('Native embedding recovery cannot coexist with a private SQLite journal')
        bound = self.actor.options['max_response_bytes'] + 64 * 1024
        if bound > self.journal.max_record_bytes:
            raise RecoveryContractError('Native response ceiling exceeds the admitted journal record budget')
        self._response_bound = bound
        self.actor._journal = _BorrowedJournal(self.journal)

    def scope_request(self, request, *, part=0):
        scope = current_row_scope()
        if scope is None:
            raise RecoveryContractError('Native embedding request lost its source occurrence')
        occurrence = scope.occurrence if part == 0 else identity([scope.occurrence, 'part', part])
        return RequestRecord({**request, 'native_response_bytes': self._response_bound,
            'native_occurrence': identity([self.action.action_id, self.node_id, occurrence])})

    def verify_request_membership(self, request):
        # A known batch occurrence can never silently acquire different input
        # membership (e.g. a non-deterministic upstream or timed boundary).
        # Bind through the existing run journal before lookup/send, not another
        # effect ledger. Unknown history is never interpreted as a fresh request.
        key = identity(['embedding-request-membership', request['native_occurrence']])
        run = self.action.context
        run.journal.bind_source(run.fence, key, {'request_key': request.request_key})

    async def drain(self, *, timeout_s):
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError('Native embedding drain timeout must be positive and finite')
        current = asyncio.current_task()
        tasks = [task for task in self.inflight.values() if task is not current]
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=timeout_s)
        if pending:
            # Leave still-running tasks, the client and the journal untouched;
            # the original failure/isolation path owns the hard deadline.
            raise TimeoutError('Native embedding drain timed out with in-flight requests')
        await asyncio.gather(*tasks, return_exceptions=True)
        for key, task in list(self.inflight.items()):
            if task.done():
                self.inflight.pop(key, None)
