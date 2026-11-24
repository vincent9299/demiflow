"""Same-process host for the original worker shell, with real exit evidence.

Only the host transport differs from Local process/Ray. Business execution,
resource grants, control protocol, journal RPC and snapshots remain unchanged.
"""
from concurrent.futures import Future, TimeoutError
from contextvars import Context, copy_context
from queue import Queue, Full
import sys
import threading
import time

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.hosts.endpoints import _EndpointBase
from demiflow.execution.worker.runtime import validate_worker_launch


class _ThreadUnit:
    def __init__(self, launch, *, spec_hash, envelope, context=None):
        self.launch = launch
        self.spec_hash = spec_hash
        self.envelope = envelope
        self.host = None
        self.ready = Future()
        self.commands = Queue(maxsize=launch['dataflow_v3']['bounds']['max_pending_events'])
        self.owned = set()
        self.owned_lock = threading.Lock()
        self.cleanup_error = None
        self._close_future = None
        self._stop_sent = False
        self.thread = threading.Thread(target=(context or Context()).run, args=(self._run,),
                                       name='demiflow-thread-host', daemon=True)
        self.thread.start()

    def _track(self, thread):
        with self.owned_lock:
            # Dead threads have positive exit evidence and need not accumulate
            # across task/actor rotations inside one long-lived allocation.
            self.owned = {t for t in self.owned if t.is_alive()}
            self.owned.add(thread)

    def _run(self):
        from demiflow.resources.capacity import allocation_envelope
        from demiflow.execution.hosts.dataflow_worker_host import HostedDataflowWorker
        try:
            with allocation_envelope(self.envelope, track_thread=self._track):
                self.host = HostedDataflowWorker(self.launch, self.spec_hash, self.launch['boot_id'])
                self.ready.set_result(None)
                while True:
                    item = self.commands.get()
                    if item is None:
                        return
                    future, name, args = item
                    if not future.set_running_or_notify_cancel():
                        continue
                    try:
                        result = getattr(self.host, name)(*args)
                    except BaseException as exc:
                        if name == 'close':
                            self.cleanup_error = exc
                        future.set_exception(exc)
                    else:
                        if (name == "handle" and args[0].get("kind") in {"Drain", "Quiesce", "Cancel"}
                                and "error" in result):
                            self.cleanup_error = RuntimeError(str(result["error"]))
                        future.set_result(result)
        except BaseException as exc:
            self.cleanup_error = exc
            if not self.ready.done():
                self.ready.set_exception(exc)

    def submit(self, name, *args):
        if not self.thread.is_alive() or self._stop_sent:
            raise DataflowError('UNKNOWN_EFFECT', 'thread worker control loop is not live')
        future = Future()
        try:
            self.commands.put_nowait((future, name, args))
        except Full as exc:
            raise DataflowError('BUDGET_EXCEEDED', 'thread control queue is full') from exc
        return future

    def shutdown(self, *, timeout_ms):
        deadline = time.monotonic() + timeout_ms / 1000
        if not self.ready.done():
            try:
                self.ready.result(timeout=max(0, deadline-time.monotonic()))
            except TimeoutError:
                return False
            except BaseException as exc:
                self.cleanup_error = exc
        if self._close_future is None and self.host is not None and self.thread.is_alive():
            self._close_future = self.submit('close')
        if self._close_future is not None:
            try:
                self._close_future.result(timeout=max(0, deadline-time.monotonic()))
            except TimeoutError:
                if not self._close_future.done():
                    return False
            except BaseException as exc:
                self.cleanup_error = exc
        if not self._stop_sent and self.thread.is_alive():
            try:
                self.commands.put_nowait(None)
            except Full:
                return False
            self._stop_sent = True
        self.thread.join(timeout=max(0, deadline-time.monotonic()))
        with self.owned_lock:
            owned = tuple(self.owned)
        for thread in owned:
            if thread is not threading.current_thread():
                thread.join(timeout=max(0, deadline-time.monotonic()))
        return not self.thread.is_alive() and all(not t.is_alive() for t in owned)


class DataflowThreadConnector:
    def __init__(self):
        self._staged = {}
        self._contexts = {}
        self.units = {}

    def stage(self, allocation_id, launch_spec):
        validate_worker_launch(launch_spec)
        if allocation_id in self._staged and self._staged[allocation_id] != launch_spec:
            raise DataflowError('FENCED', 'thread allocation launch changed')
        self._staged[allocation_id] = launch_spec
        context = Context()
        # Preserve business ContextVars without transporting driver authorities.
        for variable, value in copy_context().items():
            if not variable.name.startswith(("demiflow_", "dataflow_", "native_prompt_", "stream_first_")):
                context.run(variable.set, value)
        self._contexts[allocation_id] = context

    def launch_spec(self, allocation_id):
        return self._staged[allocation_id]

    def start(self, allocation_id, *, spec_hash, envelope):
        if allocation_id in self.units:
            return
        self.units[allocation_id] = _ThreadUnit(self._staged[allocation_id],
                                              spec_hash=spec_hash, envelope=envelope, context=self._contexts[allocation_id])

    def _host(self, allocation_id):
        unit = self.units[allocation_id]
        if not unit.ready.done():
            return None
        unit.ready.result()
        return unit.host

    def pull(self, allocation_id, *, limit):
        host = self._host(allocation_id)
        return () if host is None else host.pull_events(limit)

    def deliver_control_reply(self, allocation_id, reply):
        self._host(allocation_id).deliver_control_reply(reply)

    def request_cut(self, allocation_id, request):
        self._host(allocation_id).request_cut(request)

    def abort(self, allocation_id):
        self.retire(allocation_id)

    def retire(self, allocation_id):
        unit = self.units.get(allocation_id)
        if unit is not None and unit.thread.is_alive():
            raise DataflowError('FENCED', 'cannot retire a live thread host')
        self.units.pop(allocation_id, None)
        self._staged.pop(allocation_id, None)
        self._contexts.pop(allocation_id, None)


class ThreadHostEndpoint(_EndpointBase):
    _entrypoint = 'demiflow-worker'

    def __init__(self, lease, *, services):
        super().__init__(lease, services=services)
        self._argv = (sys.executable, '-B', '-m', 'demiflow.execution.hosts.dataflow_worker_host')

    def activate(self, assignment):
        from demiflow.execution.contracts.worker import make_worker_control_assignment
        if self._assignment != assignment:
            raise DataflowError('FENCED', 'thread worker assignment was not staged')
        _, grant, manifest_ref = self._build_launch(assignment)
        control = make_worker_control_assignment(assignment, grant=grant, manifest_ref=manifest_ref,
                                                 ready_deadline_ms=60000)
        self._send_k3('VerifyArtifactAndCapabilities',
                      {'artifact_manifest_ref': manifest_ref, 'required_capabilities': ['cpu-map', 'stream']})
        self._send_k3('Restore', {'assignment_ref': assignment.action_id,
                                 'restore_point': control['restore_point'], 'state_refs': []})
        self._send_k3('Activate', {'grant_ref': grant['grant_id'],
                                  'controller_epoch': assignment.attempt, 'operation_id': control['operation_id']})
        self._send_k3('Assign', {'assignment': control})
        self._activated = True

    def _send_k3(self, kind, payload, *, timeout_ms=None):
        cleanup = kind in ('Drain', 'Quiesce', 'Cancel')
        future = getattr(self, '_cleanup_future', None) if cleanup else None
        if future is None:
            future = self._connector.units[self._allocation_id].submit('handle', self._k3_envelope(kind, payload))
            if cleanup:
                self._cleanup_future = future
        timeout_ms = min(self._services.policy.control_timeout_ms,
            timeout_ms if timeout_ms is not None else payload.get('deadline_ms', self._services.policy.control_timeout_ms))
        deadline = time.monotonic() + timeout_ms / 1000
        while not future.done():
            self._deliver_pending_journal_replies()
            for event in self._connector.pull(self._allocation_id, limit=self._services.bounds.max_pending_events):
                self._early_event(event)
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise DataflowError('DEADLINE_EXCEEDED', 'thread worker control command timed out')
            try:
                future.result(timeout=min(.01, remaining))
            except TimeoutError:
                pass
        result = future.result()
        self._accept_final_observations(result)
        if 'error' in result:
            from demiflow.execution.contracts.dataflow_failures import restore_failure
            raise restore_failure(result['error'])
        if kind in ('Drain', 'Quiesce', 'Cancel'):
            self._cleanup_completed = True
        return result.get('receipt')

    def cancel(self, reason, *, deadline_ms):
        import uuid
        try:
            self._send_k3('Cancel', {'operation_id': uuid.uuid4().hex, 'reason': reason},
                          timeout_ms=deadline_ms)
        except DataflowError:
            pass  # Original provider retains every unconfirmed physical owner.

    def send_business(self, command):
        host = self._connector._host(self._allocation_id)
        if host is None or not host.assigned() or not self._activated:
            raise DataflowError('FENCED', 'business command before thread activation')
        # receive enqueues the command; it never invokes user code here.
        command = self._activity_command(command)
        host.receive(command)
        self._activity_generation = command.get('activity_generation', getattr(self, '_activity_generation', 0))

    def poll(self, *, max_events, deadline_ms):
        events = []
        while self._events and len(events) < max_events:
            events.append(self._events.popleft())
        if len(events) < max_events:
            events.extend(self._connector.pull(self._allocation_id, limit=max_events-len(events)))
        return tuple(events)

    def deliver_reply(self, reply):
        self._connector.deliver_control_reply(self._allocation_id, reply)
