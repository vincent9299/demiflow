"""DataflowScheduler: bounded ready queue, DRR fairness, one ResourceManager.

Lane E (E009-E014). The scheduler consumes the sealed ``PhysicalDataflow``
layout (residency/scopes) and never invents a second physical ledger: every
placement, retirement and exit proof flows through the single ResourceManager
instance the driver hands in. Ready work, leases and resident reservations are
all bounded; there is no unbounded queue. Resident occupancy is returned only after confirmed physical exit; cumulative
cost is never refunded. A completion receipt alone cannot free a worker slot.
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
import uuid
from collections import deque

from .contracts.dataflow import (
    ArtifactRef, PhysicalDataflow, ReadyWork, ScheduleChoice, WorkerLease,
)
from .contracts.dataflow_errors import DataflowError
from ..resources.contracts import (
    RESOURCE_PROTOCOL_VERSION, MutationContext, ReleaseSpec, ResourcePlan,
    WorkerLaunchSpec, WorkerRequest, identifier,
)
from ..resources.manager import ResourceManager


def _now_ms() -> int:
    return int(time.time() * 1000)


def lease_ttl_ms(policy):
    return max(120000, policy.control_timeout_ms * 3)


def _grant_bytes(document_value: dict) -> bytes:
    return json.dumps(document_value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


class DataflowScheduler:
    def __init__(self, physical: PhysicalDataflow, *, resources: ResourceManager, sealed=None, placements=None) -> None:
        if not isinstance(physical, PhysicalDataflow):
            raise DataflowError("INVALID_CONTRACT", "scheduler requires a sealed PhysicalDataflow")
        if resources is None:
            raise DataflowError("INVALID_CONTRACT", "scheduler requires the driver ResourceManager")
        if sealed is not None:
            from .contracts.dataflow import SealedExecution
            if not isinstance(sealed, SealedExecution):
                raise DataflowError('INVALID_CONTRACT', 'scheduler requires sealed launch artifacts')
        self._sealed = sealed
        self._physical = physical
        self._resources = resources
        self._policy = physical.policy
        self._bounds = physical.bounds
        self.scope_id = "df-" + physical.plan_id
        identifier(self.scope_id)
        self._stages = {stage.stage_id: stage for stage in physical.stages}
        self._placements = dict(placements or {})
        if set(self._placements) - self._stages.keys():
            raise DataflowError('INVALID_CONTRACT', 'Worker placement names an unsealed stage')
        if len(self._stages) != len(physical.stages):
            raise DataflowError("INVALID_CONTRACT", "duplicate stage_id in physical plan")
        self._residency = physical.residency
        self._quantum = self._policy.fairness_quantum_bytes
        if type(self._quantum) is not int or self._quantum <= 0:
            raise DataflowError("INVALID_CONTRACT", "fairness_quantum_bytes must be a positive int")
        self._resident_budget = self._bounds.max_resident_bytes
        self._control_reserved = self._residency.control_reserved_bytes
        self._consumer_slots = self._residency.consumer_reserved_slots
        self._ready_limit = self._bounds.max_pending_events
        self._lease_limit = self._bounds.max_partitions
        self._ready: deque[ReadyWork] = deque()
        self._ready_index: dict[str, ReadyWork] = {}
        self._deficit: dict[str, int] = {stage_id: 0 for stage_id in self._stages}
        self._rotation: list[str] = list(self._stages)
        self._cursor = 0
        # lease表 (E009 state): the driver reads it to open the scope and to
        # recover physical resources on close; only confirmed exit frees a slot.
        self.leases: dict[str, WorkerLease] = {}
        self._lease_stage: dict[str, str] = {}
        self._inflight: dict[str, ReadyWork] = {}
        self._resident_used = 0
        self._resident_by_allocation = {}
        self._retirements = {}
        self._cumulative_cost = 0
        self._completed: set[str] = set()
        self._receipts: dict[str, ArtifactRef] = {}
        self._desired_revision = 0
        self._closed = False
        self._physical_exit_confirmed = False
        self._requests = {}
        self._allocation_by_operation = {}
        # Grant documents staged by the driver before activate; the lease carries
        # the content identity, the driver reconciles it with the stored ref.
        self.grants: dict[str, bytes] = {}
        self.grant_refs: dict[str, ArtifactRef] = {}

    # -- resource binding -------------------------------------------------
    def _owner_binding(self) -> tuple[str, int, str]:
        journal = self._resources.state.journal
        identity = journal.recovery_identity()
        if not identity or "run_id" not in identity:
            raise DataflowError("FENCED", "resource scope has no run identity")
        with journal._connection() as db:
            row = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        if row is None:
            raise DataflowError("FENCED", "no current resource owner")
        return identity["run_id"], int(row[0]), row[1]

    def _context(self, operation_id: str) -> MutationContext:
        _, attempt, fence = self._owner_binding()
        return MutationContext(RESOURCE_PROTOCOL_VERSION, self.scope_id, attempt, fence,
                               operation_id, self._desired_revision)

    # -- E010 enqueue -----------------------------------------------------
    def enqueue(self, work: ReadyWork) -> None:
        if self._closed:
            raise DataflowError("CANCELLED", "scheduler is closed")
        if not isinstance(work, ReadyWork):
            raise DataflowError("INVALID_CONTRACT", "enqueue requires ReadyWork")
        stage = self._stages.get(work.stage_id)
        if stage is None:
            raise DataflowError("INVALID_CONTRACT", "unknown stage: " + work.stage_id)
        if work.partition_id not in stage.partitions:
            raise DataflowError("INVALID_CONTRACT", "partition not in stage: " + work.partition_id)
        if type(work.resident_bytes) is not int or work.resident_bytes <= 0:
            raise DataflowError("INVALID_CONTRACT", "resident_bytes must be a positive int")
        if work.resident_bytes > self._bounds.max_resident_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "work exceeds resident budget")
        payload_bytes = sum(ref.size_bytes for ref in work.input_refs)
        if payload_bytes > self._bounds.max_batch_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "work payload exceeds batch budget")
        existing = self._ready_index.get(work.operation_id)
        if existing is not None:
            if existing != work:
                raise DataflowError("INVALID_CONTRACT", "operation_id reused with different content")
            return
        if work.operation_id in self._inflight or work.operation_id in self._completed:
            raise DataflowError("INVALID_CONTRACT", "operation_id already scheduled")
        if len(self._ready) >= self._ready_limit:
            raise DataflowError("CAPACITY_PENDING", "ready queue is full", retryable=True)
        self._ready.append(work)
        self._ready_index[work.operation_id] = work

    # -- E011 choose ------------------------------------------------------
    def _feasible(self, work: ReadyWork) -> bool:
        if work.operation_id in self._allocation_by_operation:
            return True  # This pending allocation already holds its reservation.
        available = self._resident_budget - self._control_reserved - self._resident_used
        return work.resident_bytes <= available

    def choose(self, *, now_ms: int) -> ScheduleChoice:
        if self._closed:
            raise DataflowError("CANCELLED", "scheduler is closed")
        if not self._ready:
            return ScheduleChoice(None, self._policy.control_timeout_ms, "idle")
        groups = [stage_id for stage_id in self._rotation
                  if any(w.stage_id == stage_id for w in self._ready)]
        if not groups:
            return ScheduleChoice(None, self._policy.control_timeout_ms, "idle")
        for _ in range(len(groups)):
            stage_id = groups[self._cursor % len(groups)]
            self._cursor += 1
            self._deficit[stage_id] += self._quantum
            candidates = [w for w in self._ready if w.stage_id == stage_id]
            for work in candidates:
                if work.resident_bytes > self._deficit[stage_id]:
                    continue
                if not self._feasible(work):
                    return ScheduleChoice(None, self._policy.control_timeout_ms, "resident_backpressure")
                self._ready.remove(work)
                del self._ready_index[work.operation_id]
                self._deficit[stage_id] -= work.resident_bytes
                return ScheduleChoice(work, 0, None)
        return ScheduleChoice(None, self._policy.control_timeout_ms, "deficit_wait")

    # -- E012 place -------------------------------------------------------
    def _launch_spec(self, stage) -> WorkerLaunchSpec:
        artifact_refs = {}
        # The sealed physical artifact already names every stage/kernel ref.
        # One authenticated root keeps WorkerLaunchSpec's original eight-ref
        # bound even for a large fused stage. Workers still load and verify all
        # assigned programs through load_sealed_dataflow before activation.
        roots = ((('physical', self._sealed.physical_ref),) if self._sealed is not None
                 else tuple(('kernel-' + str(i), ref) for i, ref in enumerate(stage.kernel_refs)))
        for name, ref in roots:
            artifact_refs[name] = {
                "digest": ref.digest, "location": "store://" + ref.store_id + "/" + ref.key}
        artifact_refs["constants"] = {
            "digest": self._physical.constants_ref.digest,
            "location": "store://" + self._physical.constants_ref.store_id + "/" + self._physical.constants_ref.key}
        return WorkerLaunchSpec(
            artifact_refs=artifact_refs, entrypoint="demiflow-worker",
            argv=(sys.executable, "-B", "-m", "demiflow.execution.worker.bootstrap"),
            bootstrap_ref="bootstrap-ref", access_refs={},
            runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)

    def _grant(self, allocation_id: str, boot_id: str, work: ReadyWork,
               run_id: str, attempt: int, fence: str) -> ArtifactRef:
        expires_at_ms = _now_ms() + lease_ttl_ms(self._policy)
        document_value = {
            "grant_id": uuid.uuid4().hex, "run_id": run_id, "attempt": attempt,
            "fence": fence, "scope_id": self.scope_id, "allocation_id": allocation_id,
            "boot_id": boot_id, "controller_epoch": attempt, "stage_id": work.stage_id,
            "partition_id": work.partition_id, "operation_id": work.operation_id,
            "plan_revision": self._physical.plan_revision,
            "expires_at_ms": expires_at_ms,
        }
        raw = _grant_bytes(document_value)
        digest = hashlib.sha256(raw).hexdigest()
        ref = ArtifactRef(digest=digest, size_bytes=len(raw), store_id=self._policy.store_id,
                          key="grants/" + allocation_id + "/" + digest, media_type="application/json")
        self.grants[allocation_id] = raw
        self.grant_refs[allocation_id] = ref
        return ref

    def _observation(self, allocation_id):
        reader = getattr(self._resources.state, 'read_allocation', None)
        if reader is not None:
            return reader(self.scope_id, allocation_id)
        return next((a for a in self._resources.get(self.scope_id)['allocations']
                     if a['allocation_id'] == allocation_id), None)

    def place(self, work: ReadyWork, *, prepare=None, host_id: str | None = None) -> WorkerLease:
        """Stage the immutable launch before asking the sole allocator to spawn.

        A pending allocation retains its identity and request across retries.
        The desired plan includes existing live workers. Failed/pending physical
        allocations remain visible to close(), even if activation never ran.
        """
        if self._closed:
            raise DataflowError("CANCELLED", "scheduler is closed")
        stage = self._stages.get(work.stage_id)
        if stage is None or work.partition_id not in stage.partitions:
            raise DataflowError("INVALID_CONTRACT", "unknown stage/partition")
        if work.operation_id in self._inflight:
            raise DataflowError("INVALID_CONTRACT", "operation already placed")
        run_id, attempt, fence = self._owner_binding()
        allocation_id = self._allocation_by_operation.get(work.operation_id)
        if allocation_id is None:
            if len(self.leases) >= self._lease_limit or not self._feasible(work):
                raise DataflowError("CAPACITY_PENDING", "worker/resident admission pending", retryable=True)
            allocation_id, boot_id = uuid.uuid4().hex, uuid.uuid4().hex
            grant_ref = self._grant(allocation_id, boot_id, work, run_id, attempt, fence)
            lease = WorkerLease(allocation_id, boot_id, work.stage_id, (work.partition_id,),
                                host_id or self._policy.provider, allocation_id, grant_ref,
                                json.loads(self.grants[allocation_id])['expires_at_ms'])
            if prepare is None:
                raise DataflowError("INVALID_CONTRACT", "placement requires a staged worker launch")
            lease = prepare(lease, stage)
            if not isinstance(lease, WorkerLease) or lease.allocation_id != allocation_id:
                raise DataflowError("INVALID_CONTRACT", "launch preparation changed allocation identity")
            from .dataflow_native_limits import admission_timeout_ms
            request = WorkerRequest(uuid.uuid4().hex, allocation_id, stage.profile,
                self._launch_spec(stage), provision_deadline_ms=admission_timeout_ms(self._policy),
                placement=self._placements.get(work.stage_id), reservation_group=work.stage_id)
            desired = (*self._requests.values(), request)
            accepted = self._resources.apply(self._context(uuid.uuid4().hex),
                                             ResourcePlan(self.scope_id, desired, tuple(self._retirements.values())))
            self._desired_revision = accepted["desired_revision"]
            self._requests[allocation_id] = request
            self._allocation_by_operation[work.operation_id] = allocation_id
            self.leases[allocation_id] = lease
            self._lease_stage[allocation_id] = work.stage_id
            self._resident_used += work.resident_bytes
            self._resident_by_allocation[allocation_id] = work.resident_bytes
            self._cumulative_cost += work.resident_bytes
        lease = self.leases[allocation_id]
        from .dataflow_native_limits import admission_timeout_ms
        self._resources.reconcile_once(fence,
            time.monotonic() + admission_timeout_ms(self._policy) / 1000.0, 16)
        observed = self._observation(allocation_id)
        phase = observed["phase"] if observed else "absent"
        if phase != "RUNNING":
            pending = phase in ("REQUESTED", "PENDING", "STARTING", "absent")
            raise DataflowError("CAPACITY_PENDING" if pending else "RESOURCE_INFEASIBLE",
                                "worker did not reach RUNNING: " + phase,
                                retryable=pending, context={"allocation_id": allocation_id,
                                "conditions": list((observed or {}).get("conditions") or ())})
        if observed.get("boot_id") not in (None, lease.boot_id):
            raise DataflowError("FENCED", "provider reported a different worker boot")
        self._inflight[work.operation_id] = work
        return lease

    def close(self, *, deadline_ms: int) -> None:
        if self._closed:
            return
        _, _, fence = self._owner_binding()
        try:
            accepted = self._resources.close(self._context("close-" + uuid.uuid4().hex), {
                "mode": "ABANDON", "engine_receipt_ref": "execution-close",
                "terminate_deadline_ms": deadline_ms, "reason": "execution_close"})
        except Exception as failure:
            # A failed scope terminal must not strand its already-owned workers.
            # Retire only the original outstanding requests through the SAME
            # ResourceManager. This does not invent an exit proof or claim that
            # the scope closed; the original failure remains observable.
            try:
                self._retire_after_close_failure(deadline_ms=deadline_ms)
                self._physical_exit_confirmed = True
            except Exception as cleanup:
                failure.add_note("Owned worker retirement also failed: " + str(cleanup))
            raise
        self._desired_revision = accepted["desired_revision"]
        deadline = time.monotonic() + deadline_ms / 1000.0
        while True:
            self._resources.reconcile_once(fence, deadline, 16)
            snapshot = self._resources.get(self.scope_id)
            if not snapshot.get("cleanup_obligations") and all(a.get("cleanup") == "CONFIRMED" for a in snapshot["allocations"]):
                self._closed = True
                self._physical_exit_confirmed = True
                return
            if time.monotonic() >= deadline:
                raise DataflowError("DEADLINE_EXCEEDED", "worker exit remains unconfirmed")
            time.sleep(min(0.01, max(0, deadline - time.monotonic())))

    def _retire_after_close_failure(self, *, deadline_ms: int) -> None:
        allocations = tuple(self._requests)
        if not allocations:
            return
        deadline = time.monotonic() + deadline_ms / 1000.0
        snapshot = self._resources.get(self.scope_id)
        self._desired_revision = snapshot["desired_revision"]
        retirements = tuple(ReleaseSpec(allocation_id, "ABANDON",
            "close-failed-" + allocation_id, max(1, deadline_ms), "scope_close_failed")
            for allocation_id in allocations)
        try:
            accepted = self._resources.apply(self._context(uuid.uuid4().hex),
                ResourcePlan(self.scope_id, (), retirements))
            self._desired_revision = accepted["desired_revision"]
        except Exception as failure:
            # close() may have committed and then lost its reply. In that case
            # its original terminal intents already own the cleanup work.
            if getattr(failure, "code", None) != "SCOPE_CLOSED":
                raise
        _, _, fence = self._owner_binding()
        while True:
            self._resources.reconcile_once(fence, deadline, 16)
            reader = getattr(self._resources.state, "read_allocation", None)
            if reader is not None:
                observations = [reader(self.scope_id, allocation_id) for allocation_id in allocations]
            else:
                rows = {row["allocation_id"]: row for row in self._resources.get(self.scope_id)["allocations"]}
                observations = [rows.get(allocation_id) for allocation_id in allocations]
            if all(row is not None and row.get("cleanup") == "CONFIRMED" for row in observations):
                return
            if time.monotonic() >= deadline:
                raise DataflowError("DEADLINE_EXCEEDED", "owned worker exit remains unconfirmed after scope close failed")
            time.sleep(min(.01, max(0, deadline - time.monotonic())))

    # -- E013 complete ----------------------------------------------------
    def complete(self, operation_id: str, *, receipt_ref: ArtifactRef) -> None:
        if operation_id in self._completed:
            return
        work = self._inflight.get(operation_id)
        if work is None:
            raise DataflowError("INVALID_CONTRACT", "complete for unknown operation: " + operation_id)
        if not isinstance(receipt_ref, ArtifactRef):
            raise DataflowError("INVALID_CONTRACT", "complete requires an ArtifactRef receipt")
        self._completed.add(operation_id)
        self._receipts[operation_id] = receipt_ref
        del self._inflight[operation_id]
        # Completion is not process exit. Keep its resident reservation until
        # ResourceManager confirms physical cleanup during drain.

    # -- E014 drain -------------------------------------------------------
    def _release_lease(self, allocation_id: str, *, deadline_ms: int, reason: str) -> bool:
        _, attempt, fence = self._owner_binding()
        context = MutationContext(RESOURCE_PROTOCOL_VERSION, self.scope_id, attempt, fence,
                                  uuid.uuid4().hex, self._desired_revision)
        release = ReleaseSpec(allocation_id, "DRAINED", "drain-" + allocation_id,
                              max(1, deadline_ms), reason)
        desired = tuple(request for key, request in self._requests.items() if key != allocation_id)
        self._retirements[allocation_id] = release
        accepted = self._resources.apply(context, ResourcePlan(self.scope_id, desired, tuple(self._retirements.values())))
        self._requests.pop(allocation_id, None)
        self._desired_revision = accepted["desired_revision"]
        deadline = time.monotonic() + deadline_ms / 1000.0
        while True:
            self._resources.reconcile_once(fence, deadline, 16)
            observation = self._observation(allocation_id)
            if observation is not None and observation.get("cleanup") == "CONFIRMED":
                self._resident_used -= self._resident_by_allocation.pop(allocation_id, 0)
                self._retirements.pop(allocation_id, None)
                for operation, allocation in tuple(self._allocation_by_operation.items()):
                    if allocation == allocation_id:
                        del self._allocation_by_operation[operation]
                        self._completed.discard(operation)
                        self._receipts.pop(operation, None)
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(min(.01, max(0, deadline - time.monotonic())))

    def drain(self, stage_id: str, *, deadline_ms: int) -> None:
        if stage_id not in self._stages:
            raise DataflowError("INVALID_CONTRACT", "unknown stage: " + stage_id)
        if type(deadline_ms) is not int or deadline_ms <= 0:
            raise DataflowError("INVALID_CONTRACT", "drain deadline_ms must be a positive int")
        deadline = time.monotonic() + deadline_ms / 1000.0
        pending = [op for op, work in self._inflight.items() if work.stage_id == stage_id]
        if pending:
            # Non-migratable state: never preempt a model whose work is unfinished.
            raise DataflowError("DEADLINE_EXCEEDED", "stage has unfinished work: " + stage_id,
                                context={"stage_id": stage_id, "pending": len(pending)})
        leases = [alloc for alloc, sid in self._lease_stage.items() if sid == stage_id]
        for allocation_id in leases:
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
            if not self._release_lease(allocation_id, deadline_ms=remaining_ms, reason="stage_drain"):
                raise DataflowError("DEADLINE_EXCEEDED", "exit not confirmed: " + allocation_id,
                                    context={"allocation_id": allocation_id})
            del self.leases[allocation_id]
            del self._lease_stage[allocation_id]
            self.grants.pop(allocation_id, None)
            self.grant_refs.pop(allocation_id, None)
