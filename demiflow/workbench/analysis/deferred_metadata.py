"""Suspend remote checks without holding a metadata SQL lock/transaction.

Only the closed, process-free Metadata.route is evaluated here. On a missing
authority response, its local SQL evaluation is rolled back before returning
to the selector. The next turn evaluates the same request/CAS again with the
exact earlier response transcript; the next distinct check makes a fresh RPC.
This never resends an authority RPC and never dispatches a cell/native job.
The existing precommit checks, request receipts and SQL atomicity are retained.
External IO and runtime operations must not be added to Metadata.route.
"""
from __future__ import annotations

import time

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.authority import Authority
from demiflow.workbench.analysis.authority_rpc import Exchange


class Pending(BaseException):
    """Internal suspension which must unwind/rollback SQL context managers."""


class DeferredAuthority(Authority):
    def __init__(self, task):
        self.task = task
        super().__init__(task.deployment, task.peer, task.project)

    def call(self, operation, payload):
        return self.task.call(self, operation, payload)

    def detached(self):
        # The authenticated principal remains server-owned. Later lifecycle
        # checks are new exchanges, not this metadata request's transcript.
        result = Authority.__new__(Authority)
        result.deployment, result.peer, result.project, result.principal = (
            self.deployment, self.peer, self.project, self.principal)
        if getattr(self,'reader_environment',None) is not None:
            result.reader_environment=self.reader_environment
        return result

    def case_review_watermark(self, binding, read):
        # A metadata route is reevaluated after each suspended fresh check.
        # Freeze this request's cut before its first native/source RPC; later
        # saves must not silently change that RPC's payload on reevaluation.
        c.sha(binding,'Case review query binding')
        if self.task.review_cut is None:
            watermark=read();c.integer(watermark,'Case review cut',2**63-1)
            self.task.review_cut=(binding,watermark)
        if self.task.review_cut[0]!=binding:
            c.reject('Case review query changed during pending authority',code='conflict')
        return self.task.review_cut[1]


class Task:
    MAX_CHECKS = 32
    MAX_RESPONSE_BYTES = 512*1024

    def __init__(self, deployment, peer, project, metadata, operation, payload, *, expected_principal=None):
        self.deployment, self.peer, self.project = deployment, peer, project
        self.metadata, self.operation, self.payload = metadata, operation, payload
        c.encoded(payload)
        self.database = metadata.records.database
        self.responses = []
        self.pending = None
        self.cursor = self.response_bytes = 0
        self.deadline = time.monotonic() + 60
        self.done, self.closed = False, False
        self.value = self.authority = None
        self.expected_principal = expected_principal
        self.review_cut = None

    def call(self, authority, operation, payload):
        key = c.digest(dict(operation=operation, payload=payload, principal=authority.principal))
        index = self.cursor
        self.cursor += 1
        if index < len(self.responses):
            previous, value = self.responses[index]
            if previous != key:
                c.reject("Metadata changed while its checks were pending; preserve the draft and refresh", code="conflict")
            return value
        if index != len(self.responses) or self.pending is not None:
            c.reject("Metadata authority transcript is inconsistent", code="conflict")
        if index >= self.MAX_CHECKS:
            c.reject("Metadata authority check limit exceeded", code="resource_limit")
        self.pending = (key, Exchange(authority, operation, payload))
        raise Pending()

    def step(self):
        if self.done:
            return True
        if self.closed:
            c.reject("Original metadata request is closed", code="operation_rejected")
        try:
            if time.monotonic() >= self.deadline or not self.peer.live():
                c.reject("Metadata authority deadline or original Web parent ended", code="operation_rejected")
            if self.pending is not None:
                key, exchange = self.pending
                if not exchange.advance():
                    return False
                charge = c.encoded_size(exchange.value)
                if self.response_bytes + charge > self.MAX_RESPONSE_BYTES:
                    c.reject("Metadata authority response budget exceeded", code="resource_limit")
                self.responses.append((key, exchange.value))
                self.response_bytes += charge
                self.pending = None
            self.cursor = 0
            authority = DeferredAuthority(self)
            if self.expected_principal is not None and authority.principal != self.expected_principal:
                c.reject("Calibration requires its explicitly owned fixture principal", code="operation_rejected")
            try:
                result = self.metadata.route(authority, self.operation, self.payload)
            except Pending:
                # Database.transaction catches BaseException and rolls back;
                # no lock or incomplete transaction may cross selector turns.
                self._unlocked()
                return False
            if self.cursor != len(self.responses):
                c.reject("Metadata changed during pending authorization; refresh its receipt/version", code="conflict")
            self._unlocked()
            self.value, self.authority, self.done = result, authority.detached(), True
            return True
        except Pending:
            # Includes the initial whoami check in DeferredAuthority.__init__.
            self._unlocked()
            return False
        except BaseException:
            self.close()
            raise

    def _unlocked(self):
        self.database.guard.check()
        if self.database.connection.in_transaction or self.database._mutex.locked():
            c.reject("Metadata suspension retained an unfinished SQL transaction/lock", code="conflict")

    def close(self):
        self.closed = True
        self.review_cut = None
        if self.pending is not None:
            self.pending[1].close()
            self.pending = None
        self.responses.clear()
