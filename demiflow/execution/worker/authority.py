"""Worker-side restricted authority: a granted-fence proxy over RunJournal.

The worker never claims, transitions, finishes, or binds actions. After fork it
opens its own RunJournal(attempt=granted), verifies runtime_owner==(attempt,
granted fence) on attach, and relies on execution_guard for every fenced write.
``current_run()``/``current_action()`` return these proxies so the existing
stream runtime (stream.py / StreamMapRuntime / OperatorStates /
NativePromptRecovery / admit_stream) runs unchanged inside the worker host.

A driver restart replaces the fence; the worker's stale granted fence then fails
execution_guard on any write and check_stop on any poll, so it can never publish
into the new attempt (design §5.1/§5.2 journal backstop).
"""
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from ...errors import ExecutionConflict
from ..contracts.envelope import validate_grant
from ..run_context import _CURRENT, _ACTION, _PERSISTING_EFFECT
from ..run_journal import RunJournal
from ..recovery_contract import RunRef, RecoveryContractError, identity
from demiflow.execution.run_context import active_limits


class WorkerAuthority:
    """The fenced subset of RunContext a worker may use; lifecycle stays driver-only."""

    def __init__(self, directory, grant, *, check_stop=None):
        validate_grant(grant)
        self._grant = dict(grant)
        raw = Path(directory).absolute()
        if any(p.is_symlink() for p in (raw, *raw.parents)):
            raise RecoveryContractError("Run directory contains a symlink")
        self.directory = raw
        self.fence = grant["fence"]
        self.attempt = grant["attempt"]
        self.scope_id = grant["scope_id"]
        self.allocation_id = grant["allocation_id"]
        self.boot_id = grant["boot_id"]
        self.controller_epoch = grant["controller_epoch"]
        self.check_stop_callback = check_stop
        self.journal = RunJournal(self.directory, attempt=self.attempt)
        self._object_resource_lock = threading.RLock()
        self._quarantined_workers = []
        self._local_cancel = False
        self._draining = False
        self.active = False
        self.position = 0
        self._connections = None
        self._token = None

    def _verify_owner(self):
        with self.journal._connection() as db:
            row = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        if row is None or tuple(row) != (self.attempt, self.fence):
            raise ExecutionConflict("Worker grant is not the current execution owner")

    def __enter__(self):
        if _CURRENT.get() is not None:
            raise RecoveryContractError("A worker cannot nest an independent run context")
        configuration = self.journal.recovery_identity()
        if configuration is None:
            raise RecoveryContractError("Worker run has no unified recovery contract")
        if configuration["run_id"] != self._grant["run_id"]:
            raise RecoveryContractError("Worker grant run identity differs")
        self._verify_owner()
        self.ref = RunRef(str(self.directory), configuration["run_id"])
        self._limits = configuration["limits"]
        self.explicit_limits = frozenset(configuration.get("limits_explicit", ()))
        self.deadline = configuration["started_at"] + self._limits["timeout_s"]
        self.started = time.monotonic()
        self._connections = self.journal.connection_scope()
        self._connections.__enter__()
        self.active = True
        self._token = _CURRENT.set(self)
        return self

    def __exit__(self, exc_type, exc, tb):
        self.active = False
        try:
            native_journal = getattr(self, "_native_prompt_journal", None)
            if native_journal is not None:
                native_journal.close()
        finally:
            try:
                if self._connections is not None:
                    self._connections.__exit__(exc_type, exc, tb)
            finally:
                if self._token is not None:
                    _CURRENT.reset(self._token)
        return False

    @property
    def limits(self):
        return active_limits(self, self._limits)

    @limits.setter
    def limits(self, value):
        self._limits = value

    def cancel_local(self):
        self._local_cancel = True

    def check_stop(self):
        if not self.active:
            raise RecoveryContractError("Execution context is closed")
        if self._local_cancel:
            raise RecoveryContractError("Worker cancellation requested")
        if int(time.time() * 1000) >= self._grant["expires_at_ms"]:
            raise RecoveryContractError("Worker grant expired")
        if time.time() > self.deadline or time.monotonic() - self.started > self._limits["timeout_s"]:
            raise RecoveryContractError("Execution time budget exhausted")
        if _PERSISTING_EFFECT.get() is self:
            return
        if self._draining:
            raise RecoveryContractError("Execution is draining an isolated native writer")
        if self.check_stop_callback is not None:
            self.check_stop_callback()
        self._verify_owner()
        state = self.journal.execution_status()
        if state["status"] in {"stopping", "cancelled"}:
            raise RecoveryContractError("Execution cancellation requested")

    @contextmanager
    def persist_admitted_effect(self):
        token = _PERSISTING_EFFECT.set(self)
        try:
            yield
        finally:
            _PERSISTING_EFFECT.reset(token)

    @contextmanager
    def action_scope(self, action_id):
        """Bind the driver's pre-bound child action read-only; never bind a new one."""
        record = self.journal.execution_action(action_id)
        if record is None:
            raise RecoveryContractError("Worker action was not bound by the driver")
        action = WorkerAction(self, action_id, record["declaration"])
        action.replaying = record["result"] is not None
        token = _ACTION.set(action)
        try:
            yield action
        finally:
            _ACTION.reset(token)

    def quarantine_worker(self, worker):
        if worker not in self._quarantined_workers:
            self._quarantined_workers.append(worker)

    def allocate_artifact(self, path, *, commit_id=None, size=0, atomic=True):
        self.journal.allocate_artifact(self.fence,
            self.journal.artifact_name(path), commit_id=commit_id, size=size, atomic=atomic)

    def reserve_artifact_bytes(self, path, size, *, atomic=False):
        self.journal.resize_artifact(self.fence, self.journal.artifact_name(path), size, atomic=atomic)

    def release_temporaries(self, paths):
        names = []
        for path in paths:
            name = self.journal.artifact_name(path)
            names.extend((name, name + ".staging"))
        self.journal.release_temporaries(self.fence, names)

    @contextmanager
    def temporary_artifacts(self):
        paths = []
        def allocate(path):
            if len(paths) >= 64:
                raise RecoveryContractError("Temporary producer allocation count exceeds budget")
            self.allocate_artifact(path, atomic=False)
            paths.append(path)
        try:
            yield allocate
        finally:
            self.release_temporaries(paths)

    def object_namespace(self, logical_root):
        from demiflow.execution.storage.object_resources import bind_namespace
        with self._object_resource_lock:
            return bind_namespace(self, logical_root)

    def publication_guard(self):
        return self.journal.publication_guard(self.fence)

    def reserve(self, name, amount=1, *, reservation_id=None, ceiling=None):
        import uuid
        self.check_stop()
        ceiling = self.limits["max_requests"] if ceiling is None else ceiling
        return self.journal.reserve_budget(self.fence, name,
            reservation_id or uuid.uuid4().hex, amount, ceiling)

    def reserve_output(self, component, amount, *, reservation_id=None, admitted=False):
        import uuid
        self.check_stop()
        if component not in {"row_bytes", "object_bytes"}:
            raise RecoveryContractError("Unknown native output component")
        limit = self.limits["max_output_bytes"]
        charges = {component: (amount, limit), "output_bytes": (amount, limit)}
        if not admitted:
            charges["output_admission_bytes"] = (amount, limit)
        return self.journal.reserve_budgets(self.fence, reservation_id or uuid.uuid4().hex, charges)


class WorkerAction:
    """Read-only proxy of the driver-bound child action; completion stays driver-only."""

    def __init__(self, authority, action_id, declaration):
        self.context = authority
        self.action_id = action_id
        self.declaration = declaration
        self.semantic_id = identity(declaration)
        self.replaying = False
        self.object_sinks = {}
        self.native_image_effects = False
        self._image_replay_checked = False

    @property
    def record(self):
        return self.context.journal.execution_action(self.action_id)