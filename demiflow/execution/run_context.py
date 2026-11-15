"""Execution-owned context for the existing Dataset drivers.

No scheduler lives here. Drivers bind one context, lower their ordinary plans,
and use RunJournal for every durable execution decision.
"""
from contextlib import contextmanager, ExitStack
from contextvars import ContextVar, copy_context
from pathlib import Path
import fcntl
import os
import tempfile
import threading
import time
import uuid

from .recovery_contract import RunRef, RecoveryContractError, identity
from .runtime_limits import execution_limits
from .run_journal import RunJournal

_CURRENT = ContextVar("demiflow_run_context", default=None)
_ACTION = ContextVar("demiflow_run_action", default=None)
_PERSISTING_EFFECT = ContextVar("demiflow_persisting_admitted_effect", default=None)


def current_run():
    return _CURRENT.get()


def current_action():
    return _ACTION.get()


def create_target_absent_binding(run_id, action_id, declaration, intent):
    """Derive a create admission identity from native facts, without storage IO."""
    from ..errors import DemiflowError
    from .lance_sink import spec_from_identity
    if (type(declaration) is not dict or declaration.get('kind') != 'write_table'
            or type(intent) is not dict or intent.get('kind') != 'write_table'):
        return None
    try:
        spec = spec_from_identity(declaration['sink'])
        if (spec.mode != 'create' or spec.expected_version is not None
                or intent.get('spec') != spec.to_dict()
                or intent.get('request_hash') != spec.content_hash
                or intent.get('operation_id') != identity([run_id, action_id, 1])):
            return None
    except (KeyError, TypeError, ValueError, DemiflowError, RecoveryContractError):
        return None
    return dict(kind='create-target-absent', action_id=action_id,
                operation_id=intent['operation_id'], request_hash=spec.content_hash,
                uri=spec.uri, mode='create')


class RunContext:
    """Platform control scope. Pipeline/operator arguments never carry this."""

    def __init__(self, directory, *, entry_identity=None, limits=None, resume=False,
                 journal=None, check_stop=None):
        raw = Path(directory).absolute()
        if any(p.is_symlink() for p in (raw, *raw.parents)):
            raise RecoveryContractError("Run directory contains a symlink")
        self.directory = raw
        self.entry_identity = entry_identity
        self.requested_limits = limits
        self.resuming = resume
        self.external_control = journal is not None
        self.journal = journal or RunJournal(raw)
        self.check_stop_callback = check_stop
        self.fence = uuid.uuid4().hex
        self.position = 0
        self.active = False
        self._lease = None
        self._journal_connections = ExitStack()
        self._object_resource_lock = threading.RLock()
        self.started = None
        self._draining = False
        self._quarantined_workers = []

    @property
    def limits(self):
        from .payload_limits import active_limits
        return active_limits(self, self._limits)

    @limits.setter
    def limits(self, value):
        self._limits = value

    @classmethod
    def start(cls, directory=None, *, entry_identity=None, limits=None):
        directory = directory or Path(tempfile.gettempdir()) / "demiflow-runs" / uuid.uuid4().hex
        return cls(directory, entry_identity=entry_identity, limits=limits)

    @classmethod
    def resume(cls, reference, *, entry_identity=None, limits=None):
        if not isinstance(reference, RunRef):
            raise TypeError("Resume requires the original platform RunRef")
        # Unified history admission before any business start: the run must carry a
        # unified recovery contract produced by this same engine revision. The
        # original RunRef is preserved; per-action code/source/state/sink/cursor
        # compatibility is enforced by the recovery path (load_execution_version /
        # validate_resume_compatibility), never by rewriting source or fabricating a
        # replacement successful run.
        from .recovery_identity import platform_identity
        journal = RunJournal(reference.directory)
        binding = journal.recovery_identity()
        if binding is None:
            raise RecoveryContractError("Run has no unified recovery contract; cannot resume history")
        if binding.get("platform_revision") != platform_identity():
            raise RecoveryContractError(
                "Platform implementation changed; resume requires the original engine revision")
        if binding.get("run_id") != reference.run_id:
            raise RecoveryContractError("RunRef identity does not match its directory")
        context = cls(reference.directory, entry_identity=entry_identity, limits=limits, resume=True)
        context.expected_run_id = reference.run_id
        return context

    @staticmethod
    def cancel(reference):
        """Request cancellation through the existing lifecycle control record."""
        journal = RunJournal(reference.directory)
        binding = journal.recovery_identity()
        if binding is None or binding['run_id'] != reference.run_id:
            raise RecoveryContractError('Cancellation RunRef identity differs')
        return journal.transition({'running', 'starting'}, 'stopping')

    @classmethod
    def attach(cls, journal, *, entry_identity=None, limits=None, check_stop=None):
        """Adopt a supervisor's existing run/attempt; do not invent another run."""
        return cls(journal.directory, journal=journal, entry_identity=entry_identity,
                   limits=limits, check_stop=check_stop)

    def __enter__(self):
        if current_run() is not None:
            raise RecoveryContractError("A driver cannot nest independent run contexts")
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lease = os.open(self.directory / ".runtime.lock",
                              os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self._lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            from .recovery_identity import platform_identity
            revision = platform_identity()
            old = self.journal.recovery_identity()
            if self.resuming and old is None:
                raise RecoveryContractError("Run has no unified recovery contract")
            if not self.resuming and not self.external_control and self.journal.path.exists():
                raise RecoveryContractError("Run already exists; resume its RunRef explicitly")
            if old is not None:
                if old["platform_revision"] != revision:
                    raise RecoveryContractError("Platform implementation changed; resume requires its fixed revision")
                if self.entry_identity is not None and old["entry_identity"] != self.entry_identity:
                    raise RecoveryContractError("Run entry identity changed")
                if self.requested_limits is not None and execution_limits(self.requested_limits) != old["limits"]:
                    raise RecoveryContractError("Run limits changed on resume")
                if self.requested_limits is not None and sorted(self.requested_limits) != old.get('limits_explicit', []):
                    raise RecoveryContractError('Run limits semantics changed on resume')
                configuration = old
            else:
                configuration = dict(format=1, stream_protocol="aligned-barrier-v2", run_id=uuid.uuid4().hex,
                    entry_identity=self.entry_identity, limits=execution_limits(self.requested_limits),
                    platform_revision=revision, started_at=time.time(),
                    limits_explicit=sorted((self.requested_limits or {}).keys()))
            self.ref = RunRef(str(self.directory), configuration["run_id"])
            if self.resuming and self.ref.run_id != self.expected_run_id:
                raise RecoveryContractError("RunRef identity does not match its directory")
            self.limits = configuration["limits"]
            self.explicit_limits = frozenset(configuration.get("limits_explicit", ()))
            self.deadline = configuration["started_at"] + self.limits["timeout_s"]
            if not self.external_control:
                if not self.resuming:
                    self.journal.initialize(configuration, attempts=True)
                    self.journal.transition({None}, "prepared")
                else:
                    state = self.journal.execution_status()
                    if state["status"] in {"running", "starting", "stopping", "prepared"}:
                        self.journal.transition({state["status"]}, "interrupted")
                        state = self.journal.execution_status()
                    if state["status"] == "succeeded":
                        raise RecoveryContractError("A completed run cannot create another attempt")
                    self.journal.begin_attempt(expected_attempt=state["attempt"],
                                               expected_revision=state["revision"])
                self.journal.transition({"prepared"}, "running")
            self.attempt = self.journal.claim_execution(configuration, self.fence)
            self.journal = RunJournal(self.directory, attempt=self.attempt)
            self._journal_connections.enter_context(self.journal.connection_scope())
            self.journal.clean_orphans(self.fence)
            self.active = True
            self.started = time.monotonic()
            self._token = _CURRENT.set(self)
            return self
        except BaseException:
            try:
                self._journal_connections.close()
            finally:
                os.close(self._lease)
                self._lease = None
            raise

    def __exit__(self, exc_type, exc, tb):
        failure = exc
        workers = [worker for worker in self._quarantined_workers if worker.is_alive()]
        if workers:
            # A blocked native writer cannot be killed safely. Keep its handles
            # and exclusive run lease isolated until its producer exits. This
            # finalizer schedules no graph work and permits no replacement owner.
            self._draining = True
            _CURRENT.reset(self._token)
            def finish_owned_resources():
                try:
                    for worker in workers:
                        worker.join()
                    native_journal = getattr(self, '_native_prompt_journal', None)
                    if native_journal is not None:
                        native_journal.close()
                    self.journal.clean_orphans(self.fence)
                    if not self.external_control:
                        self.journal.finish_execution(self.fence, 'failed')
                    else:
                        self.journal.release_execution(self.fence)
                finally:
                    self.active = False
                    try:
                        self._journal_connections.close()
                    finally:
                        os.close(self._lease)
                        self._lease = None
            self._drain_finalizer = threading.Thread(target=finish_owned_resources,
                name='demiflow-owned-writer-drain', daemon=True)
            self._drain_finalizer.start()
            if exc is None:
                raise RecoveryContractError('Native writer is isolated while draining; run lease is retained')
            return False
        try:
            if self.active:
                native_journal = getattr(self, '_native_prompt_journal', None)
                if native_journal is not None:
                    native_journal.close()
                self.journal.clean_orphans(self.fence)
                if failure is None:
                    try:
                        self.check_stop()
                    except BaseException as stop_error:
                        failure = stop_error
                if failure is None and self.journal.incomplete_execution():
                    failure = RecoveryContractError("Run has unconfirmed actions, outputs, state or source ACKs")
                if not self.external_control:
                    self.journal.finish_execution(self.fence, 'succeeded' if failure is None else 'failed')
                else:
                    self.journal.release_execution(self.fence)
        finally:
            self.active = False
            try:
                self._journal_connections.close()
            finally:
                _CURRENT.reset(self._token)
                if self._lease is not None:
                    os.close(self._lease)
                    self._lease = None
        if exc is None and failure is not None:
            raise failure
        return False

    def quarantine_worker(self, worker):
        if worker not in self._quarantined_workers:
            self._quarantined_workers.append(worker)

    def check_stop(self):
        if not self.active:
            raise RecoveryContractError("Execution context is closed")
        if time.time() > self.deadline or time.monotonic() - self.started > self.limits["timeout_s"]:
            raise RecoveryContractError("Execution time budget exhausted")
        if _PERSISTING_EFFECT.get() is self:
            return  # Only local durability of an already received native effect.
        if self._draining:
            raise RecoveryContractError('Execution is draining an isolated native writer')
        if self.check_stop_callback is not None:
            self.check_stop_callback()
        state = self.journal.execution_status()
        if state["status"] in {"stopping", "cancelled"}:
            raise RecoveryContractError("Execution cancellation requested")

    @contextmanager
    def persist_admitted_effect(self):
        """Finish received bytes during cooperative stop, under the same fence.

        The native model adapter alone uses this around local result storage.
        It cannot dispatch a request or publish a row. Resource/time admission,
        journal fencing and the enclosing driver's drain deadline still apply.
        """
        token = _PERSISTING_EFFECT.set(self)
        try:
            yield
        finally:
            _PERSISTING_EFFECT.reset(token)

    @contextmanager
    def action(self, declaration, *, action_id=None):
        self.check_stop()
        if current_action() is not None:
            raise RecoveryContractError("Nested Dataset actions need an explicit completed data boundary")
        self.position += 1
        action_id = action_id or "action-" + str(self.position)
        old = self.journal.execution_action(action_id)
        restriction = self.journal.source_binding(action_id + '-state-recovery-restriction')
        if old is not None and old.get('result') is None and restriction is not None:
            raise RecoveryContractError('Cannot resume attempt-only actor state; '
                                        'snapshot_state/restore_state is required')
        if old is not None and declaration.get('kind')=='delivery':
            raise RecoveryContractError('Delivery has no replay guarantee; committed image artifacts remain readable')
        source=declaration.get('source')
        source_contract=source.get('contract') if type(source) is dict else None
        native_stream=(source_contract=='demiflow.queue.source.v2' or any(
            type(part) is dict and type(part.get('native')) is dict and
            part['native'].get('contract')=='demiflow.queue.enqueue.v2' for part in declaration.get('operations',[])))
        authority=self.journal.source_binding('queue-budget-authority')
        if authority is not None and not native_stream:
            raise RecoveryContractError('Queue task runs cannot also authorize local durable actions; use a separate bounded run')
        if old is not None and (declaration.get('kind') in {'write_file', 'delivery'} or
                declaration.get('kind')=='run_stream' and not native_stream or
                declaration.get('kind') == 'write_table' and not self.journal.execution_commits(action_id)
                and not self._create_target_was_absent(action_id, declaration)):
            raise RecoveryContractError('This delivery-only entry needs its P3 native sink adapter before replay')
        self.journal.bind_execution_action(self.fence, action_id, declaration)
        action = RunAction(self, action_id, declaration)
        action.replaying = old is not None
        token = _ACTION.set(action)
        try:
            yield action
        finally:
            _ACTION.reset(token)

    def _create_target_was_absent(self, action_id, declaration):
        binding = self.journal.source_binding(action_id + '-create-target-absent')
        if binding is None:
            return False
        view = self.journal.execution_view(detail='summary', action_ids=[action_id])
        intent = next((event['value'] for event in view['events']
                       if event['kind'] == 'intent'), None)
        expected = create_target_absent_binding(self.ref.run_id, action_id, declaration, intent)
        # This cross-attempt fact grants replay admission, not permission to
        # overwrite. LanceSink.prepare must recheck absence at the actual write.
        return expected is not None and binding == expected

    def allocate_artifact(self, path, *, commit_id=None, size=0, atomic=True):
        self.journal.allocate_artifact(self.fence,
            self.journal.artifact_name(path), commit_id=commit_id, size=size, atomic=atomic)

    def reserve_artifact_bytes(self, path, size, *, atomic=False):
        self.journal.resize_artifact(self.fence, self.journal.artifact_name(path), size, atomic=atomic)

    def release_temporaries(self, paths):
        names = []
        for path in paths:
            name = self.journal.artifact_name(path)
            names.extend((name, name + '.staging'))
        self.journal.release_temporaries(self.fence, names)

    @contextmanager
    def temporary_artifacts(self):
        """A producer-owned bounded allocation group; remove files before exit."""
        paths = []
        def allocate(path):
            if len(paths) >= 64:
                raise RecoveryContractError('Temporary producer allocation count exceeds budget')
            self.allocate_artifact(path, atomic=False)
            paths.append(path)
        try:
            yield allocate
        finally:
            self.release_temporaries(paths)

    def object_namespace(self, logical_root):
        """Platform-owned physical namespace under a stable logical destination."""
        from .object_resources import bind_namespace
        with self._object_resource_lock:
            return bind_namespace(self, logical_root)

    def reserve(self, name, amount=1, *, reservation_id=None, ceiling=None):
        self.check_stop()
        ceiling = self.limits["max_requests"] if ceiling is None else ceiling
        return self.journal.reserve_budget(self.fence, name,
            reservation_id or uuid.uuid4().hex, amount, ceiling)

    def reserve_output(self, component, amount, *, reservation_id=None, admitted=False):
        """Record real encoded object bytes separately from row bytes.

        Both consume the same output_bytes ceiling; retries reuse only the
        identical artifact reservation, never a saved model request/result.
        """
        from .recovery_contract import RecoveryContractError
        self.check_stop()
        if component not in {'row_bytes', 'object_bytes'}:
            raise RecoveryContractError('Unknown native output component')
        limit = self.limits['max_output_bytes']
        charges={component:(amount,limit),'output_bytes':(amount,limit)}
        if not admitted:
            charges['output_admission_bytes']=(amount,limit)
        return self.journal.reserve_budgets(self.fence,reservation_id or uuid.uuid4().hex,charges)


class RunAction:
    def __init__(self, context, action_id, declaration):
        self.context, self.action_id, self.declaration = context, action_id, declaration
        self.semantic_id = identity(declaration)

    @property
    def record(self):
        return self.context.journal.execution_action(self.action_id)

    def complete(self, result):
        self.context.journal.complete_execution_action(self.context.fence, self.action_id, result)


@contextmanager
def execution_scope():
    """All ordinary actions acquire the same context; a launcher may bind it."""
    context = current_run()
    if context is not None:
        context.check_stop()
        yield context
    else:
        with RunContext.start() as context:
            yield context


class _ContextIterator:
    """Keep one pull iterator's context across native callback threads."""

    def __init__(self, rows):
        self._context = copy_context()
        self._lock = threading.RLock()
        self._closed = False
        self._rows = None
        self._rows = iter(rows)

    def __iter__(self):
        return self

    def __next__(self):
        with self._lock:
            if self._closed:
                raise StopIteration
            try:
                return self._context.run(next, self._rows)
            except BaseException:
                self.close()
                raise

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                close = getattr(self._rows, 'close', None)
                if close is not None:
                    self._context.run(close)
            finally:
                self._rows = self._context = None

    def __del__(self):
        self.close()


def bind_iteration(rows):
    """Bind existing execution/observation context; creates no run or scheduler."""
    return _ContextIterator(rows)


def native_rows(method):
    from functools import wraps
    @wraps(method)
    def consume(executor, *args, **kwargs):
        def rows():
            with execution_scope() as context:
                executor._run_ref = context.ref
                iterator = iter(method(executor, *args, **kwargs))
                try:
                    for row in iterator:
                        context.check_stop()
                        yield row
                finally:
                    close = getattr(iterator, 'close', None)
                    if close is not None:
                        close()
        return bind_iteration(rows())
    return consume


def native_file_action(method):
    from functools import wraps
    @wraps(method)
    def execute(executor, source, plan, format_name, path, options):
        from .recovery_identity import plan_identity
        with execution_scope() as context:
            executor._run_ref = context.ref
            declaration = plan_identity('write_file', source, plan,
                arguments={'format': format_name, 'path': path, 'options': options})
            with context.action(declaration) as action:
                result = method(executor, source, plan, format_name, path, options)
                action.complete({'kind': 'write_file', 'delivery': 'completed'})
                return result
    return execute


def admit_stream(plan, *, source_batch_size=1):
    """Native resource admission applies equally with or without a checker."""
    from .plan_facts import operation_facts
    from .capabilities import stream_residency
    context = current_run()
    if context is None:
        raise RecoveryContractError('A stream requires its execution context')
    if len(plan.operations) > 1024:
        raise RecoveryContractError('Native operation count exceeds budget')
    try:
        facts = [operation_facts(operation) for operation in plan.operations]
        reservation = stream_residency(facts, context.limits)
    except ValueError as exc:
        raise RecoveryContractError(str(exc)) from exc
    if (type(source_batch_size) is not int or not 1 <= source_batch_size <= 256 or
            reservation['reserved_bytes'] + source_batch_size * context.limits['max_row_bytes'] > context.limits['memory_bytes']):
        raise RecoveryContractError('Stream source prefetch exceeds its run memory budget')
    return reservation
