"""Worker-side data chain execution (U2/S4, D3 single worker host).

This is the ``executor`` seam injected into ``WorkerRuntime``. On Assign it drives
the existing ``run_stream`` in a dedicated thread under the granted-fence
authority: it reads the driver-captured source blocks directly (source-in) and
stages result blocks (result-out) without ever building a commit authority. Each
E1 barrier seal requests a durable commit from the driver over the control channel
(``CommitCut`` -> ``CommitReceipt``); the worker advances its confirmed ledger only
on the receipt. The main control thread delivers receipts and converges
CheckpointRequest/Drain/Cancel onto the same barrier, so a cut can never overtake
its pre-cut output descriptor.
"""
import asyncio
import hashlib
import threading
import time
import traceback
import uuid
from concurrent.futures import Future
from pathlib import Path

from ..block_sink import BlockSink
from ..chain import WORKER_FAILURE_SCHEMA, build_commit_cut, validate_commit_receipt
from ..recovery_contract import RecoveryContractError, identity
from ..stream import run_stream
from .progress import WorkerStreamProgress


class WorkerChainExecutor:
    def __init__(self, launch_spec, *, send):
        execution = launch_spec["execution"]
        self._send = send
        self.source_child_id = execution["source_child_id"]
        self.output_child_id = execution["output_child_id"]
        self.plan_ref = execution["plan_ref"]
        self.source_reference = execution["source_reference"]
        self.channel_id = execution["channel_id"]
        self.object_specs = execution.get("object_specs")
        self.restore = execution.get("restore")
        self.sealed_operators = execution.get("sealed_operators")
        self._execution_plan = launch_spec.get("execution_plan")
        self.authority = None
        self.progress = None
        self.stats = None
        self._thread = None
        self._pending = None
        self._lock = threading.Lock()
        self._error = None
        self._done = threading.Event()

    def _load_plan(self):
        import cloudpickle
        from ..chain import verify_loaded_plan
        path = self.authority.directory / self.plan_ref["path"]
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != self.plan_ref["sha256"]:
            raise RecoveryContractError("Worker plan artifact digest differs")
        plan = cloudpickle.loads(raw)
        verify_loaded_plan(plan, self.plan_ref, self.sealed_operators)
        return plan

    def start(self, authority, assignment):
        if self._thread is not None:
            return
        self.authority = authority
        self._assignment = assignment
        self._thread = threading.Thread(target=self._run, name="demiflow-worker-chain", daemon=True)
        self._thread.start()

    def _run(self):
        from ..run_context import _CURRENT
        token = _CURRENT.set(self.authority)
        try:
            plan = self._load_plan()
            checkpoint_policy = None
            stage_limits = None
            if self._execution_plan is not None:
                from .layout import load_worker_layout
                _, checkpoint_policy, stage_limits = load_worker_layout(
                    {"execution_plan": self._execution_plan},
                    self.authority, self._assignment)
            with self.authority.action_scope(self.source_child_id) as source_action:
                source_sink = BlockSink(source_action, "source", input_capture=True)
            with self.authority.action_scope(self.output_child_id) as output_action:
                self.progress = WorkerStreamProgress(
                    output_action, self.source_reference, source_sink=source_sink,
                    object_specs=self.object_specs, restore=self.restore,
                    request_commit=self._request_commit,
                    stop_requested=self._stop_requested,
                    checkpoint_policy=checkpoint_policy)
                self.stats = run_stream(self.progress.source_rows(), plan,
                                        consume=self.progress.append, progress=self.progress,
                                        stage_limits=stage_limits)
        except BaseException as exc:
            self._error = exc
            with self._lock:
                if self._pending is not None and not self._pending.done():
                    self._pending.set_exception(exc)
                    carried = True
                else:
                    carried = False
            if not carried:
                # Load/init failure or a row error outside an outstanding cut: no
                # CommitCut response can carry it, so send a worker-level failure
                # frame the driver raises immediately (F8) instead of timing out.
                self._send({"schema_version": WORKER_FAILURE_SCHEMA,
                            "error": {"type": type(exc).__name__, "message": str(exc)},
                            "traceback": traceback.format_exc()[-8192:]})
        finally:
            _CURRENT.reset(token)
            self._done.set()

    def _stop_requested(self):
        return not self.authority.active or self.authority._local_cancel

    async def _request_commit(self, next_ordinal, sealed, states, final):
        """Lock-step commit request from the barrier publish, awaited on the loop."""
        descriptor = sealed.descriptor()
        operation_id = uuid.uuid4().hex
        checkpoint_id = identity([self.output_child_id, "ck", next_ordinal, operation_id])
        sequence = self.progress.sequence + 1
        cut = build_commit_cut(
            operation_id, checkpoint_id, source_id=self.source_reference["source_id"],
            next_ordinal=next_ordinal, sequence=sequence, channel_id=self.channel_id,
            channel_cut_sequence=sequence - 1, output_descriptor=descriptor,
            states=states, inflight=(), final=final)
        future = Future()
        with self._lock:
            self._pending = future
        self._send(cut)
        authority = self.authority
        now = time.time()
        bound = min(authority.limits["checkpoint_timeout_s"],
                    authority._grant["expires_at_ms"] / 1000.0 - now,
                    authority.deadline - now,
                    authority._limits["timeout_s"] - (time.monotonic() - authority.started))
        try:
            receipt = await asyncio.wait_for(asyncio.wrap_future(future), timeout=bound)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            # Invalidate the pending cut so a late ACK cannot advance a later cut;
            # the commit stays unknown: no resend, no early ownership release.
            with self._lock:
                if self._pending is future:
                    self._pending = None
            raise
        validate_commit_receipt(receipt)
        if receipt["operation_id"] != operation_id or receipt["sequence"] != sequence:
            raise RecoveryContractError("CommitReceipt does not match the outstanding cut")
        if receipt["status"] != "acked":
            raise RecoveryContractError("Worker cut was not durably acknowledged")
        self.progress.inputs = next_ordinal
        self.progress.sequence = receipt["sequence"]
        self.progress.rows += descriptor["rows"]
        with self._lock:
            self._pending = None
        return receipt

    def deliver_receipt(self, receipt):
        with self._lock:
            future = self._pending
        if future is not None and not future.done():
            future.set_result(receipt)

    def checkpoint(self, cut):
        # U2 single-source degenerate chain: the driver-initiated producer bound
        # is the source ordinal at which the same E1 barrier converges.
        if self.progress is not None:
            self.progress.pending_cut = cut["producer_sequence_bound"]

    def drain(self, payload):
        deadline_s = max(0, payload.get("deadline_ms", 0)) / 1000.0
        self._join(deadline_s)
        if self._error is not None:
            raise self._error
        return {"release_hint": "DRAINED", "rows": self.progress.rows if self.progress else 0,
                "sequence": self.progress.sequence if self.progress else 0}

    def cancel(self, payload):
        if self.authority is not None:
            self.authority.cancel_local()
        grace_s = self.authority.limits["cancel_grace_s"] if self.authority is not None else 10
        self._join(grace_s)
        if self._thread is not None and self._thread.is_alive():
            if self.authority is not None:
                self.authority.quarantine_worker(self._thread)
            raise RecoveryContractError("Worker chain exceeded cancel grace; owned writer isolated")

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