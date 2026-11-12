"""Worker-side pure-transform carrier for unified queue settlement (MVP-2 A1).

The driver owns the queue lifecycle, the single RawFetch acquisition and
finalize_task; this executor only applies the validated pure stateless maps
suffix that follows RawFetch. It never constructs a CommitCoordinator or owns
any commit/recovery authority (guardrail 2, absolute). Acquired rows arrive
incrementally as K4 DataRefs over the control channel and are read from the
shared pickle-4 segment data plane (type-preserving, bounded, one row in
flight); each transformed row keeps its native ``_queue_delivery`` display so
the driver can correlate it back to the source-owned delivery and settle the
task under its own journal. A business failure is returned as an explicit
request_id-correlated response carrying the original exception, never left to
surface as a driver control timeout.
"""
import hashlib
import queue
import threading
import traceback

from ..chain import WORKER_FAILURE_SCHEMA
from ..exchange.carriage_local import LocalSegmentStore
from ..recovery_contract import RecoveryContractError
from ..stream import run_stream

TRANSFORM_REQUEST_SCHEMA = "demiflow.queue_transform_request.v1"
TRANSFORM_RESPONSE_SCHEMA = "demiflow.queue_transform_response.v1"

_EOS = object()


class QueueTransformExecutor:
    """The ``executor`` seam injected into WorkerRuntime for the queue carrier.

    On Assign it drives ``run_stream`` over the pure-maps suffix in a dedicated
    thread under the granted-fence authority. Rows are fed one at a time by the
    control thread (``feed``) and each result is emitted back as a
    TransformResponse; there is no source capture, barrier or commit cut.
    """

    def __init__(self, launch_spec, *, send):
        execution = launch_spec["queue_transform"]
        self._send = send
        self.plan_ref = execution["plan_ref"]
        self.sealed_operators = execution.get("sealed_operators")
        self.authority = None
        self._store = None
        self._thread = None
        self._inbox = queue.Queue()
        self._error = None
        self._current_request_id = None
        self._done = threading.Event()
        self.rows = 0

    def _load_plan(self):
        import cloudpickle
        from ..chain import verify_loaded_plan
        path = self.authority.directory / self.plan_ref["path"]
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != self.plan_ref["sha256"]:
            raise RecoveryContractError("Worker transform plan artifact digest differs")
        plan = cloudpickle.loads(raw)
        verify_loaded_plan(plan, self.plan_ref, self.sealed_operators)
        return plan

    def start(self, authority, assignment):
        if self._thread is not None:
            return
        self.authority = authority
        self._store = LocalSegmentStore(authority.directory / "queue-transform-segments", host_id="local")
        self._thread = threading.Thread(target=self._run, name="demiflow-queue-transform", daemon=True)
        self._thread.start()

    def _run(self):
        from ..run_context import _CURRENT
        token = _CURRENT.set(self.authority)
        try:
            plan = self._load_plan()
            run_stream(self._source_rows(), self._stream_plan(plan), consume=self._emit,
                       source_batch_size=1)
        except BaseException as exc:
            self._error = exc
            self._send_failure(exc)
        finally:
            _CURRENT.reset(token)
            self._done.set()

    def _send_failure(self, exc):
        # One row is in flight (driver concurrency=1), so the current request_id
        # unambiguously correlates the failure. The original exception rides the
        # pickle data plane to preserve its type/args; a bounded traceback string
        # rides the control frame for diagnostics.
        request_id = self._current_request_id
        detail = traceback.format_exc()[-8192:]
        if request_id is None:
            # Load/init failure before the first feed: no request_id to correlate,
            # so send a worker-level failure frame the driver raises immediately
            # (F8) instead of leaving it to block on the transform response.
            self._send({"schema_version": WORKER_FAILURE_SCHEMA,
                        "error": {"type": type(exc).__name__, "message": str(exc)},
                        "traceback": detail})
            return
        frame = {"schema_version": TRANSFORM_RESPONSE_SCHEMA, "request_id": request_id,
                 "traceback": detail}
        try:
            _, refs = self._store.write([exc], scope_ref=self.authority.allocation_id,
                                        durability="ephemeral")
            frame["error_ref"] = refs[0]
        except Exception:
            frame["error"] = {"type": type(exc).__name__, "message": str(exc)}
        self._send(frame)

    @staticmethod
    def _stream_plan(suffix):
        # run_stream requires at least one streaming operation; the validated
        # suffix is pure pull-model maps (RawFetch stays driver-side), so a
        # pass-through entry drives the stream without changing any row.
        from ...data.plan import AsyncMapOp, CallableSpec, LogicalPlan
        entry = AsyncMapOp(CallableSpec.create(lambda row: row), 1, 1, (),
                           'queue_transform_entry', None, 'inline')
        return LogicalPlan((entry,) + suffix.operations)

    def _source_rows(self):
        while True:
            ref = self._inbox.get()
            if ref is _EOS:
                return
            row = self._store.read([ref])[0]
            self._store.discard([ref])
            yield row

    def _emit(self, row):
        if type(row) is not dict or "_queue_delivery" not in row:
            raise RecoveryContractError("Queue transform lost its native delivery display")
        self.rows += 1
        _, refs = self._store.write([row], scope_ref=self.authority.allocation_id,
                                    durability="ephemeral")
        self._send({"schema_version": TRANSFORM_RESPONSE_SCHEMA,
                    "request_id": row["_queue_delivery"]["task_id"], "row_ref": refs[0]})

    def feed(self, request_id, row_ref):
        self._current_request_id = request_id
        self._inbox.put(row_ref)

    def checkpoint(self, cut):
        # Pure maps hold no durable state; the driver's per-task finalize_task is
        # the only commit authority, so a checkpoint cut is a no-op here.
        pass

    def drain(self, payload):
        deadline_s = max(0, payload.get("deadline_ms", 0)) / 1000.0
        self._inbox.put(_EOS)
        self._join(deadline_s)
        if self._error is not None:
            raise self._error
        return {"release_hint": "DRAINED", "rows": self.rows}

    def cancel(self, payload):
        if self.authority is not None:
            self.authority.cancel_local()
        grace_s = self.authority.limits["cancel_grace_s"] if self.authority is not None else 10
        self._inbox.put(_EOS)
        self._join(grace_s)
        if self._thread is not None and self._thread.is_alive():
            if self.authority is not None:
                self.authority.quarantine_worker(self._thread)
            raise RecoveryContractError("Queue transform exceeded cancel grace; owned writer isolated")

    def _join(self, timeout_s):
        if self._thread is None:
            return
        self._thread.join(timeout=max(0.0, timeout_s))

    def join(self, timeout_s=None):
        self._join(timeout_s if timeout_s is not None else 30)

    @property
    def done(self):
        return self._done.is_set()

    @property
    def error(self):
        return self._error