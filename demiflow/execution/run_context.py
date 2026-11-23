"""Execution-owned context for the existing Dataset drivers.

No scheduler lives here. Drivers bind one context, lower their ordinary plans,
and use RunJournal for every durable execution decision.
"""

from demiflow.execution.contracts.dataflow import PAYLOAD_FIELDS
from demiflow.execution.compiler_support.payload_limits import stream_limits
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
from demiflow.planning.dataflow_policy import execution_limits, declared_limit_keys
from .run_journal import RunJournal

_CURRENT = ContextVar("demiflow_run_context", default=None)
_ACTION = ContextVar("demiflow_run_action", default=None)
_PERSISTING_EFFECT = ContextVar("demiflow_persisting_admitted_effect", default=None)


def current_run():
    return _CURRENT.get()


def current_action():
    return _ACTION.get()


def _open_runtime_lease(directory):
    descriptor = os.open(directory / '.runtime.lock',
                         os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _maintenance_snapshot(journal, expected_attempt, expected_revision, confirm_stopped,
                          *, terminal=False, action_id=None):
    """Check the caller's stop proof against the one original read snapshot."""
    from demiflow.errors import ExecutionConflict
    from demiflow.execution.journal.run_reader import read_maintenance_snapshot
    journal._check_maintenance_deadline()
    state, configuration, binding, declaration = read_maintenance_snapshot(journal,
        expected_attempt, expected_revision, terminal=terminal, action_id=action_id)
    snapshot = {name: state.get(name) for name in (
        'attempt', 'revision', 'status', 'execution_stopped', 'reason')}
    if (type(snapshot['status']) is not str
            or snapshot['execution_stopped'] is not None and type(snapshot['execution_stopped']) is not bool
            or snapshot['reason'] is not None and type(snapshot['reason']) is not str):
        raise ExecutionConflict('Invalid stopped execution proof fields')
    for name in ('unit_identity', 'supervisor', 'worker'):
        value = state.get(name)
        if value is None:
            snapshot[name] = None
            continue
        fields = {'device': int, 'inode': int} if name == 'unit_identity' else {'pid': int, 'start': str, 'boot': str}
        if type(value) is not dict or any(type(value.get(key)) is not kind for key, kind in fields.items()):
            raise ExecutionConflict('Invalid stopped execution process identity')
        snapshot[name] = {key: value[key] for key in fields}
    if confirm_stopped(snapshot) is not True:
        raise ExecutionConflict('Execution stop is not proven')
    journal._check_maintenance_deadline()
    return state, configuration, binding, declaration


@contextmanager
def _stopped_maintenance(journal, expected_attempt, expected_revision, deadline):
    import math
    if type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= 0:
        raise ValueError('Invalid maintenance deadline')
    if expected_attempt is not None and (type(expected_attempt) is not int or not 1 <= expected_attempt <= 32):
        raise ValueError('Invalid maintenance attempt')
    if expected_revision is not None and (type(expected_revision) is not int or expected_revision < 0):
        raise ValueError('Invalid maintenance revision')
    # Validate the original read policy before even creating a lock file.
    from demiflow.execution.journal.run_reader import read_run
    maintenance = RunJournal(journal.directory, attempt=journal.attempt, _maintenance_deadline=deadline)
    maintenance._check_maintenance_deadline()
    with read_run(journal.directory, deadline=deadline):
        pass
    try:
        descriptor = _open_runtime_lease(maintenance.directory)
    except BlockingIOError as exc:
        from ..errors import ExecutionConflict
        raise ExecutionConflict('Stopped execution runtime lease is busy') from exc
    try:
        with maintenance.connection_scope():
            yield maintenance
    finally:
        os.close(descriptor)


@contextmanager
def _maintenance_owner(journal, configuration, *, recover_resources=False, preserve_jobs=False):
    """Claim can fail after committing its owner: protect the claim itself."""
    fence = uuid.uuid4().hex
    claimed = False
    failure = None
    try:
        if recover_resources:
            journal.claim_execution(configuration, fence, recover_resources=True, preserve_jobs=preserve_jobs)
        else:
            journal.claim_execution(configuration, fence)
        claimed = True
        yield fence
    except BaseException as exc:
        failure = exc
        raise
    finally:
        release = True
        if recover_resources and not claimed:
            try:
                from demiflow.execution.hosts.run_state import RunResourceState
                release = not RunResourceState(journal).unfinished_scopes(configuration['run_id'])
            except BaseException:
                release = False  # Unknown resource cleanup retains this owner.
        if release:
            try:
                journal._check_maintenance_deadline()
                # Never release another owner's fence, including a partial claim.
                from demiflow.execution.journal.run_reader import read_maintenance_owner
                own = read_maintenance_owner(journal, fence)
                if own:
                    journal.release_execution(fence)
            except BaseException as cleanup:
                if failure is None:
                    raise
                failure.add_note('Maintenance owner release also failed: ' + str(cleanup))


@contextmanager
def stopped_publication_context(journal, *, expected_attempt, expected_revision, deadline, confirm_stopped):
    """Bind native commit operations to the original stopped owner's lease and fence.

    This context can settle admitted effects. It does not enter the active
    Dataset context, start an attempt or grant action/worker admission.
    """
    with _stopped_maintenance(journal, expected_attempt, expected_revision, deadline) as native:
        state, config, _, _ = _maintenance_snapshot(native, expected_attempt, expected_revision,
                                                    confirm_stopped, terminal=True)
        if config is None:
            raise RecoveryContractError("Stopped publication requires an original run identity")
        with _maintenance_owner(native, config) as fence:
            run = RunContext(native.directory, journal=native)
            run.ref = RunRef(str(native.directory), config['run_id'])
            run.fence, run.attempt, run.limits = fence, state['attempt'], config['limits']
            run.started = time.monotonic()
            run.deadline = time.time() + max(0, deadline-run.started)
            run.active = True
            run.stopped_publication = True
            try:
                with run.persist_admitted_effect():
                    yield run
            finally:
                run.active = False


def clean_stopped_execution(journal, *, expected_attempt, confirm_stopped, deadline,
                            expected_revision=None, preserve_jobs=False):
    """Clean a proven stopped native execution without changing host lifecycle."""
    with _stopped_maintenance(journal, expected_attempt, expected_revision, deadline) as maintenance:
        state, configuration, _, _ = _maintenance_snapshot(maintenance, expected_attempt,
            expected_revision, confirm_stopped)
        if configuration is not None:
            with _maintenance_owner(maintenance, configuration, recover_resources=True, preserve_jobs=preserve_jobs) as fence:
                maintenance.clean_orphans(fence)
        return dict(attempt=expected_attempt, revision=state['revision'], native_work=configuration is not None)


def admit_absent_create(journal, *, action_id, expected_attempt, expected_revision,
                        expected_uri, confirm_stopped, deadline):
    """Record current absence against the selected attempt's original write identity."""
    from .recovery_contract import identifier
    from ..errors import LanceResourceNotFound, ExecutionConflict
    from demiflow.execution.outputs.lance_sink import spec_from_identity
    from ..lance.reconcile import reconcile_lance_create
    from ..lance.storage import inspect_lance
    identifier(action_id)
    if type(expected_uri) is not str or not expected_uri or len(expected_uri.encode()) > 4096:
        raise ValueError('Invalid expected create URI')
    if expected_revision is None:
        raise ValueError('Create admission requires a revision')
    with _stopped_maintenance(journal, expected_attempt, expected_revision, deadline) as maintenance:
        state, configuration, binding, declaration = _maintenance_snapshot(maintenance, expected_attempt,
            expected_revision, confirm_stopped, terminal=True, action_id=action_id)
        result = dict(attempt=expected_attempt, revision=state['revision'], admitted=False)
        if binding is None or binding['uri'] != expected_uri:
            return result
        spec = spec_from_identity(declaration['sink'])
        maintenance._check_maintenance_deadline()
        try:
            receipt = reconcile_lance_create(spec.uri, request_hash=binding['request_hash'],
                                             operation_id=binding['operation_id'])
        except LanceResourceNotFound:
            receipt = None
        maintenance._check_maintenance_deadline()
        if receipt is not None:
            return result
        try:
            inspect_lance(spec.uri, None, spec.storage_options)
        except LanceResourceNotFound:
            pass
        else:
            maintenance._check_maintenance_deadline()
            return result
        maintenance._check_maintenance_deadline()
        _, current_configuration, current_binding, _ = _maintenance_snapshot(maintenance, expected_attempt,
            expected_revision, confirm_stopped, terminal=True, action_id=action_id)
        if current_configuration != configuration or current_binding != binding:
            raise ExecutionConflict('Create admission identity changed during storage verification')
        with _maintenance_owner(maintenance, configuration) as fence:
            maintenance.bind_source(fence, action_id + '-create-target-absent', binding)
        result['admitted'] = True
        return result


def _validate_runtime_protocol(configuration):
    """Only the persisted state protocol, never its producer's source version."""
    if (type(configuration) is not dict or type(configuration.get('format')) is not int
            or configuration['format'] != 1):
        raise RecoveryContractError('Unsupported or missing runtime identity format')
    if configuration.get('stream_protocol') != 'aligned-barrier-v2':
        raise RecoveryContractError('Unsupported or missing runtime stream protocol')


class RunContext:
    """Platform control scope. Pipeline/operator arguments never carry this."""

    def __init__(self, directory, *, entry_identity=None, limits=None, resume=False,
                 journal=None, check_stop=None, training=None, explicit_limits=None):
        from .plan_capture import current_capture
        if current_capture() is not None:
            from .contracts.dataflow_errors import DataflowError
            raise DataflowError('UNSUPPORTED_CAPABILITY',
                               'plan capture cannot start or resume an explicit run; use the construction entry')
        raw = Path(directory).absolute()
        if any(p.is_symlink() for p in (raw, *raw.parents)):
            raise RecoveryContractError("Run directory contains a symlink")
        self.directory = raw
        self.entry_identity = entry_identity
        self.requested_limits = limits
        self.explicit_limits = declared_limit_keys(limits, explicit_limits)
        self.resuming = resume
        self.training_project_root = None
        self.training = training
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
        from demiflow.execution.run_context import active_limits
        return active_limits(self, self._limits)

    @limits.setter
    def limits(self, value):
        self._limits = value

    @staticmethod
    def validate_recovery_identity(configuration):
        """Validate native durable protocol before external lifecycle changes."""
        _validate_runtime_protocol(configuration)

    @classmethod
    def start(cls, directory=None, *, entry_identity=None, limits=None, training=None):
        directory = directory or Path(tempfile.gettempdir()) / "demiflow-runs" / uuid.uuid4().hex
        return cls(directory, entry_identity=entry_identity, limits=limits, training=training)

    @classmethod
    def resume(cls, reference, *, entry_identity=None, limits=None, training=None):
        if not isinstance(reference, RunRef):
            raise TypeError("Resume requires the original platform RunRef")
        # Unified history admission before any business start: the run must carry a
        # unified recovery contract with an understood durable protocol. The
        # original RunRef is preserved; per-action code/source/state/sink/cursor
        # compatibility is enforced by the recovery path (load_execution_version /
        # validate_resume_compatibility), never by rewriting source or fabricating a
        # replacement successful run.
        journal = RunJournal(reference.directory)
        binding = journal.recovery_identity()
        if binding is None:
            raise RecoveryContractError("Run has no unified recovery contract; cannot resume history")
        _validate_runtime_protocol(binding)
        if binding.get("run_id") != reference.run_id:
            raise RecoveryContractError("RunRef identity does not match its directory")
        context = cls(reference.directory, entry_identity=entry_identity, limits=limits, resume=True, training=training)
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
    def attach(cls, journal, *, entry_identity=None, limits=None, explicit_limits=None, check_stop=None, training_project_root=None):
        """Adopt a supervisor's existing run/attempt; do not invent another run."""
        state = journal.execution_status()
        resuming = state is not None and state['attempt'] > 1
        binding = journal.recovery_identity() if resuming else None
        if binding is not None and limits is not None and explicit_limits is None:
            # Older manifests recorded provenance only in the original run identity.
            explicit_limits = binding.get('limits_explicit', ())
        context = cls(journal.directory, journal=journal, entry_identity=entry_identity,
                      limits=limits, explicit_limits=explicit_limits, check_stop=check_stop, resume=resuming)
        context.training_project_root = training_project_root
        if resuming:
            context.expected_run_id = binding['run_id'] if binding is not None else None
        return context

    def __enter__(self):
        if current_run() is not None:
            raise RecoveryContractError("A driver cannot nest independent run contexts")
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lease = _open_runtime_lease(self.directory)
        try:
            old = self.journal.recovery_identity()
            if self.resuming and old is None:
                raise RecoveryContractError("Run has no unified recovery contract")
            if not self.resuming and not self.external_control and self.journal.path.exists():
                raise RecoveryContractError("Run already exists; resume its RunRef explicitly")
            if old is not None:
                _validate_runtime_protocol(old)
                if self.entry_identity is not None and old["entry_identity"] != self.entry_identity:
                    raise RecoveryContractError("Run entry identity changed")
                if self.requested_limits is not None and execution_limits(self.requested_limits) != old["limits"]:
                    raise RecoveryContractError("Run limits changed on resume")
                if self.requested_limits is not None and sorted(self.explicit_limits) != old.get('limits_explicit', []):
                    raise RecoveryContractError('Run limits semantics changed on resume')
                configuration = old
            else:
                from .recovery_identity import platform_identity
                revision = platform_identity()  # Birth provenance only; never a resume gate.
                configuration = dict(format=1, stream_protocol="aligned-barrier-v2", run_id=uuid.uuid4().hex,
                    entry_identity=self.entry_identity, limits=execution_limits(self.requested_limits),
                    platform_revision=revision, started_at=time.time(),
                    limits_explicit=sorted(self.explicit_limits))
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
            self.attempt = self.journal.claim_execution(configuration, self.fence, recover_resources=True,
                                                        preserve_jobs=True)
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
                    from .storage.gc import finish_run_gc
                    finish_run_gc(self)
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
                from .storage.gc import finish_run_gc
                finish_run_gc(self)
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

        Used for local result storage and reconciling admitted model jobs.
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
        if getattr(self, 'stopped_publication', False):
            raise RecoveryContractError("Stopped publication cannot start a Dataset action")
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
        native_writer = (self.journal.source_binding(action_id + '-stream-lance') is not None
            and self.journal.source_binding(action_id + '-dataflow-sealed') is not None)
        native_stream = native_stream or native_writer
        authority=self.journal.source_binding('queue-budget-authority')
        if authority is not None and not native_stream:
            raise RecoveryContractError('Queue task runs cannot also authorize local durable actions; use a separate bounded run')
        native_file = (declaration.get('kind') == 'write_file'
            and self.journal.source_binding(action_id + '-file-target') is not None
            and self.journal.source_binding(action_id + '-dataflow-sealed') is not None)
        sealed_stream = self.journal.source_binding(action_id + '-dataflow-sealed')
        checkpointed_stream = (declaration.get('kind') == 'run_stream'
            and type(sealed_stream) is dict and sealed_stream.get('schema') in {'demiflow.dataflow_seal.v3', 'demiflow.dataflow_seal.v4'}
            and bool(self.journal.execution_commits(action_id)))
        # This admits the action to the original-contract recovery verifier. It is
        # not proof of success/recoverability: code, sources, actor state, effects
        # and the original journal cut must still pass before a worker starts.
        # An old delivery-only action or a directory containing no cut gains no
        # recovery authority, and queue authorization above is unchanged.
        if old is not None and (declaration.get('kind') == 'delivery' or
                declaration.get('kind') == 'write_file' and not native_file or
                declaration.get('kind')=='run_stream' and not native_stream and not checkpointed_stream or
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
        if intent is None and self.attempt > 1:
            history = self.journal.action_history([action_id], before_attempt=self.attempt, kinds=['intent'])
            intent = history[-1]['value'] if history else None
        from demiflow.execution.outputs.lance_sink import create_target_absent_binding
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
        from demiflow.execution.storage.object_resources import bind_namespace
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
        self.replaying = False

    @property
    def record(self):
        return self.context.journal.execution_action(self.action_id)

    def complete(self, result):
        self.context.journal.complete_execution_action(self.context.fence, self.action_id, result)


@contextmanager
def execution_scope():
    """All ordinary actions acquire the same context; a launcher may bind it."""
    from .plan_capture import current_capture
    capture = current_capture()
    if capture is not None:
        yield capture.context
        return
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


_ACTIVE = ContextVar('demiflow_payload_limits', default=None)

def active_limits(run, base):
    active = _ACTIVE.get()
    return active[1] if active is not None and active[0] is run else base

@contextmanager
def payload_scope(run, plan, *, owner_limits=None, arguments=None, source=None, physical_dataflow=False):
    limits = dict(run.limits)
    if owner_limits is not None:
        for name in PAYLOAD_FIELDS:
            if name in owner_limits:
                limits[name] = min(limits[name], owner_limits[name])
    if source is not None:
        from demiflow.execution.queue_execution import queue_source
        queued=queue_source(source)
        if queued is not None:
            ceiling=min(queued.config.limits.max_payload_bytes,queued.config.limits.max_result_bytes)
            limits['max_row_bytes']=min(limits['max_row_bytes'],ceiling)
            limits['max_batch_bytes']=min(limits['max_batch_bytes'],ceiling)
    # The unified physical plan admits each worker/edge profile. Applying the
    # former folded single-stream residency model here both invents a second
    # topology and rejects valid combinations (e.g. async model -> sort).
    if plan is not None and not physical_dataflow:
        from demiflow.data.plan import is_stream_operation
        if any(is_stream_operation(op) for op in plan.operations):
            from demiflow.execution.plan_facts import operation_facts
            from demiflow.execution.recovery_contract import RecoveryContractError
            try:
                limits = stream_limits([operation_facts(op) for op in plan.operations], limits,
                                       (arguments or {}).get('source_batch_size') or 1,
                                       retained_rows=(arguments or {}).get('_retained_rows',0))
            except ValueError as exc:
                raise RecoveryContractError(str(exc)) from exc
    token = _ACTIVE.set((run, limits))
    try:
        yield limits
    finally:
        _ACTIVE.reset(token)
