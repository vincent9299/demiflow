"""Worker-side pure-transform carrier for unified queue settlement (MVP-2 A1).

The driver owns the queue lifecycle, the single RawFetch acquisition and
finalize_task; this executor only applies the validated pure stateless maps
suffix that follows RawFetch. It never constructs a CommitCoordinator or owns
any commit/recovery authority (guardrail 2, absolute). Acquired rows arrive
incrementally over the control channel; each transformed row keeps its native
``_queue_delivery`` display so the driver can correlate it back to the
source-owned delivery and settle the task under its own journal.
"""
import hashlib
import queue
import threading

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
        self.authority = None
        self._thread = None
        self._inbox = queue.Queue()
        self._error = None
        self._done = threading.Event()
        self.rows = 0

    def _load_plan(self):
        import cloudpickle
        path = self.authority.directory / self.plan_ref["path"]
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != self.plan_ref["sha256"]:
            raise RecoveryContractError("Worker transform plan artifact digest differs")
        return cloudpickle.loads(raw)

    def start(self, authority, assignment):
        if self._thread is not None:
            return
        self.authority = authority
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
        finally:
            _CURRENT.reset(token)
            self._done.set()

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
            item = self._inbox.get()
            if item is _EOS:
                return
            yield item

    def _emit(self, row):
        if type(row) is not dict or "_queue_delivery" not in row:
            raise RecoveryContractError("Queue transform lost its native delivery display")
        self.rows += 1
        self._send({"schema_version": TRANSFORM_RESPONSE_SCHEMA,
                    "request_id": row["_queue_delivery"]["task_id"], "row": row})

    def feed(self, request_id, row):
        self._inbox.put(row)

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