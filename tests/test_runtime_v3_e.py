"""Lane E gate G-E: test_e001..test_e019 against the real E symbols.

Upstream lanes (C DTOs, G/P/S/D/H/Q/T/X/M) are not yet implemented; this test
injects strict spec-bound fakes into sys.modules for those modules only. The
three E modules under test are imported REAL from this worktree. The frozen
resource contracts/manager and run_context/recovery_identity stay real.
"""
from __future__ import annotations

import sys
import types
import uuid
from dataclasses import field, make_dataclass
from pathlib import Path

import pytest

WORKTREE = Path(__file__).resolve().parents[1]

# -- in-process tree binding (IL012) --------------------------------------
import demiflow  # noqa: E402

assert str(Path(demiflow.__file__).resolve()).startswith(str(WORKTREE)), demiflow.__file__
import demiflow.execution  # noqa: E402
import demiflow.execution.contracts  # noqa: E402
import demiflow.execution.exchange  # noqa: E402
import demiflow.planning  # noqa: E402
from demiflow.data.plan import CallableSpec, LogicalPlan  # noqa: E402
from demiflow.data.sources import ItemsSource  # noqa: E402

_ND = object()


def _dto(name, fields):
    spec = [(n, object) if d is _ND else (n, object, field(default=d)) for n, d in fields]
    return make_dataclass(name, spec, frozen=True)


# -- fake C lane DTO module -----------------------------------------------
dto = types.ModuleType("demiflow.execution.contracts.dataflow")
dto.ArtifactRef = _dto("ArtifactRef", [("digest", _ND), ("size_bytes", _ND), ("store_id", _ND), ("key", _ND), ("media_type", _ND)])
dto.Bounds = _dto("Bounds", [(n, _ND) for n in (
    "max_nodes", "max_edges", "max_partitions", "max_depth", "max_control_bytes",
    "max_row_bytes", "max_batch_bytes", "max_resident_bytes", "max_spill_bytes",
    "max_state_bytes", "max_pending_events", "max_open_files", "max_output_rows", "timeout_ms")])
dto.RuntimePolicy = _dto("RuntimePolicy", [(n, _ND) for n in (
    "provider", "durability", "default_partitions", "max_replicas", "fusion",
    "checkpoint_rows", "checkpoint_interval_ms", "fairness_quantum_bytes",
    "control_timeout_ms", "store_id", "required_recovery")])
dto.ReadyWork = _dto("ReadyWork", [("stage_id", _ND), ("partition_id", _ND), ("input_refs", _ND), ("resident_bytes", _ND), ("operation_id", _ND)])
dto.ScheduleChoice = _dto("ScheduleChoice", [("work", _ND), ("retry_after_ms", _ND), ("reason", _ND)])
dto.WorkerLease = _dto("WorkerLease", [(n, _ND) for n in (
    "allocation_id", "boot_id", "stage_id", "partition_ids", "host_id", "endpoint_ref", "grant_ref", "expires_at_ms")])
dto.ResourceProfile = _dto("ResourceProfile", [("requests", _ND), ("limits", None), ("requirements", None)])
dto.StageSpec = _dto("StageSpec", [(n, _ND) for n in (
    "stage_id", "occurrences", "partitions", "profile", "replica_min", "replica_target",
    "replica_max", "resident_bytes", "output_bytes", "state_bytes", "kernel_refs")])
dto.ResidencyPlan = _dto("ResidencyPlan", [("simultaneous_groups", _ND), ("control_reserved_bytes", _ND), ("consumer_reserved_slots", _ND), ("required_spill_bytes", _ND)])
dto.ExchangeSpec = _dto("ExchangeSpec", [(n, _ND) for n in (
    "channel_id", "edge_id", "producer_partitions", "consumer_partitions", "mode",
    "routing", "keys", "scope_id", "route_epoch", "credit_rows", "credit_bytes",
    "storage_bytes", "read_domain", "reason")])
dto.SourceSplit = _dto("SourceSplit", [("split_id", _ND), ("source_id", _ND), ("locator", _ND), ("start_cursor", _ND), ("estimated_bytes", _ND), ("required_domain", _ND)])
dto.SourceSnapshot = _dto("SourceSnapshot", [("source_id", _ND), ("kind", _ND), ("immutable_identity", _ND), ("finite", _ND), ("replay", _ND), ("manifest_ref", _ND)])
dto.HostDescriptor = _dto("HostDescriptor", [("host_id", _ND), ("provider", _ND), ("runtime_revision", _ND), ("store_domains", _ND), ("logical_resources", _ND), ("isolation_capabilities", _ND)])
dto.PhysicalDataflow = _dto("PhysicalDataflow", [(n, _ND) for n in (
    "schema", "dag_id", "plan_id", "plan_revision", "stages", "exchanges", "residency",
    "scopes", "sources", "splits", "policy", "bounds", "recovery", "limitations", "constants_ref")])
dto.SealedExecution = _dto("SealedExecution", [(n, _ND) for n in (
    "logical_ref", "physical_ref", "programs", "code_pin_ref", "identity_bridge_ref", "source_refs", "constants_ref")])
dto.DataflowGraph = _dto("DataflowGraph", [("schema", _ND), ("action_id", _ND), ("dag_id", _ND), ("nodes", _ND), ("edges", _ND), ("terminals", _ND), ("sources", _ND)])
dto.CompiledDataflow = _dto("CompiledDataflow", [("graph", _ND), ("declarations", _ND)])
dto.ActionRequest = _dto("ActionRequest", [(n, _ND) for n in (
    "kind", "source", "plan", "arguments", "sink", "programs", "provider", "action")])
dto.ActionProgram = _dto("ActionProgram", [("name", _ND), ("kind", _ND), ("value", _ND)])
dto.NativeOperation = _dto("NativeOperation", [("kind", _ND), ("configuration", _ND), ("bindings", _ND), ("factory", _ND)])
dto.DriverServices = _dto("DriverServices", [(n, _ND) for n in (
    "action", "journal_service", "store", "resources", "policy", "bounds", "hosts", "clock_ms")])
dto.ExecutionStatus = _dto("ExecutionStatus", [("state", _ND), ("pending_events", _ND), ("confirmed_rows", _ND)])
dto.TerminalResult = _dto("TerminalResult", [(n, _ND) for n in (
    "kind", "rows", "scalar", "result_ref", "write_result", "complete", "completion")])
dto.RecoveryImage = _dto("RecoveryImage", [("commit_id", _ND), ("cut", _ND), ("contributions", _ND), ("confirmed_sinks", _ND), ("source_vector", _ND)])
dto.CutRequest = _dto("CutRequest", [("cut_id", _ND), ("sequence", _ND), ("plan_revision", _ND), ("route_epoch", _ND), ("participants", _ND), ("final", _ND)])
dto.CutContribution = _dto("CutContribution", [(n, _ND) for n in (
    "cut_id", "participant_id", "boot_id", "sources", "operator_states", "channels", "buffers", "effects", "sinks", "final")])
dto.WorkerAssignment = _dto("WorkerAssignment", [(n, _ND) for n in (
    "schema", "run_id", "action_id", "attempt", "fence", "plan_id", "plan_revision",
    "lease", "sealed", "stages", "inputs", "outputs", "source_splits", "restore_refs", "bounds")])
dto.WorkerEvent = _dto("WorkerEvent", [(n, _ND) for n in (
    "schema", "kind", "operation_id", "run_id", "attempt", "fence", "allocation_id",
    "boot_id", "plan_revision", "scope_id", "controller_epoch", "payload_ref", "payload")])
dto.ControlReply = _dto("ControlReply", [("operation_id", _ND), ("status", _ND), ("payload", _ND), ("payload_ref", _ND), ("error_code", _ND)])
dto.CompletedControl = _dto("CompletedControl", [("allocation_id", _ND), ("boot_id", _ND), ("reply", _ND)])
dto.CapabilityReport = _dto("CapabilityReport", [("supported", _ND), ("reasons", _ND), ("recovery", _ND), ("required_domains", _ND), ("required_profiles", _ND)])
dto.KernelSpec = _dto("KernelSpec", [("kind", _ND), ("configuration", _ND)])


class _ActionHandle:
    def __init__(self, execution=None):
        self._execution = execution

    def rows(self):
        return iter(())

    def result(self):
        return dto.TerminalResult(kind="take", rows=0, scalar=None, result_ref=None,
                                  write_result=None, complete=True, completion="terminal_satisfied")

    def close(self):
        if self._execution is not None:
            self._execution.close()


dto.ActionHandle = _ActionHandle

# -- fake C lane errors ----------------------------------------------------
errors = types.ModuleType("demiflow.execution.contracts.dataflow_errors")


class DataflowError(Exception):
    def __init__(self, code, message, *, retryable=False, context=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.context = dict(context or {})


errors.DataflowError = DataflowError

# -- fake resource manager / journal / store -------------------------------
class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql):
        return self

    def fetchone(self):
        return (1, "fence-1")


class FakeJournal:
    def recovery_identity(self):
        return {"run_id": "run-1", "platform_revision": "r1", "limits": {}}

    def execution_status(self):
        return {"attempt": 1, "status": "running", "revision": 1}

    def _connection(self):
        return _FakeConn()


class FakeState:
    def __init__(self):
        self.journal = FakeJournal()
        self.scopes = []

    def open_scope(self, fence, scope):
        self.scopes.append((fence, scope))


class FakeResourceManager:
    def __init__(self, phase="RUNNING", cleanup="CONFIRMED"):
        self.state = FakeState()
        self._allocs = {}
        self._phase = phase
        self._cleanup = cleanup
        self.closed = []
        self.applied = []
        self.described = []

    def describe(self, binding, requirements):
        self.described.append((binding, requirements))
        return {}

    def apply(self, context, plan):
        for w in plan.desired_workers:
            self._allocs[w.allocation_id] = {
                "allocation_id": w.allocation_id, "phase": self._phase,
                "placement": {"host_id": "host-1"}, "boot_id": w.allocation_id,
                "cleanup": self._cleanup}
        self.applied.append(plan)
        return {"desired_revision": len(self.applied)}

    def reconcile_once(self, authority, deadline, limit):
        return None

    def get(self, scope_id, page_token=None):
        return {"allocations": list(self._allocs.values())}

    def close(self, context, spec):
        self.closed.append(spec.allocation_id)
        if spec.allocation_id in self._allocs:
            self._allocs[spec.allocation_id]["cleanup"] = self._cleanup
        return {"desired_revision": 99}


class FakeStore:
    def __init__(self):
        self.puts = []

    def put(self, chunks, *, max_bytes, media_type):
        raw = b"".join(chunks)
        self.puts.append(raw)
        import hashlib
        return dto.ArtifactRef(digest=hashlib.sha256(raw).hexdigest(), size_bytes=len(raw),
                               store_id="store-1", key="staged/" + uuid.uuid4().hex,
                               media_type=media_type)


class FakeJournalService:
    def __init__(self):
        self.submitted = []
        self.bound = []
        self.replies = ()
        self.closed = False

    def bind_queue(self, service):
        self.bound.append(service)

    def submit(self, event):
        self.submitted.append(event)
        return True

    def poll_replies(self, *, max_replies):
        return self.replies

    def close(self, *, deadline_ms):
        self.closed = True


class FakeEndpoint:
    def __init__(self, lease):
        self.lease = lease
        self.activated = []
        self.events = []
        self.delivered = []
        self.cancelled = []

    def activate(self, assignment):
        self.activated.append(assignment)

    def poll(self, *, max_events, deadline_ms):
        out = self.events[:max_events]
        self.events = self.events[max_events:]
        return out

    def deliver_reply(self, reply):
        self.delivered.append(reply)

    def cancel(self, reason, *, deadline_ms):
        self.cancelled.append(reason)


class FakeCheckpoint:
    def __init__(self, action, physical, *, store):
        self.action = action
        self.physical = physical
        self.store = store
        self.begun = []
        self.committed = []
        self.contributions = []
        self.aborted = []
        self.recovered = []

    def recover(self, *, sinks):
        self.recovered.append(sinks)
        return dto.RecoveryImage(commit_id="commit-1", cut=None, contributions=(),
                                 confirmed_sinks={}, source_vector={})

    def begin(self, *, final):
        self.begun.append(final)
        return dto.CutRequest(cut_id="cut-1", sequence=1, plan_revision=1, route_epoch=0,
                              participants=(), final=final)

    def commit(self, cut_id, *, sinks):
        self.committed.append((cut_id, sinks))
        return dto.RecoveryImage(commit_id="commit-2", cut=None, contributions=(),
                                 confirmed_sinks={}, source_vector={})

    def contribute(self, contribution):
        self.contributions.append(contribution)

    def abort(self, cut_id, *, reason):
        self.aborted.append((cut_id, reason))


class FakeAcquisition:
    def __init__(self, action, *, services):
        self.action = action
        self.services = services
        self.configured = []
        self.started = False
        self.finished = False
        self.closed = False

    def configure(self, source, plan, sink):
        self.configured.append((source, plan, sink))

    def start(self):
        self.started = True

    def finish(self):
        self.finished = True
        return {}

    def close(self):
        self.closed = True


# -- fake upstream lane modules -------------------------------------------
def _real_module_available(name):
    import importlib
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def _mod(name):
    m = types.ModuleType(name)
    if not _real_module_available(name):
        sys.modules[name] = m
    return m


def _ref(key="k"):
    return dto.ArtifactRef(digest="0" * 64, size_bytes=1, store_id="store-1", key=key,
                           media_type="application/json")


def make_policy(provider="local"):
    return dto.RuntimePolicy(provider=provider, durability="final_commit", default_partitions=1,
                             max_replicas=1, fusion=False, checkpoint_rows=1000,
                             checkpoint_interval_ms=1000, fairness_quantum_bytes=64,
                             control_timeout_ms=5000, store_id="store-1", required_recovery="final_commit")


def make_bounds():
    return dto.Bounds(max_nodes=4096, max_edges=16384, max_partitions=4, max_depth=64,
                      max_control_bytes=65536, max_row_bytes=1024, max_batch_bytes=4096,
                      max_resident_bytes=1000, max_spill_bytes=1000, max_state_bytes=1000,
                      max_pending_events=10, max_open_files=16, max_output_rows=1000, timeout_ms=5000)


def make_stage(stage_id="stage-0", partitions=("p0",), resident=1, terminal=False):
    return dto.StageSpec(stage_id=stage_id, occurrences=(stage_id,), partitions=partitions,
                         profile=dto.ResourceProfile(requests={"cpu_millis": 1, "memory_bytes": 1}, requirements={}),
                         replica_min=1, replica_target=1, replica_max=1, resident_bytes=resident,
                         output_bytes=1, state_bytes=1, kernel_refs=(_ref("kernel"),))


def make_physical(stages=None, policy=None, bounds=None):
    stages = tuple(stages or (make_stage(),))
    return dto.PhysicalDataflow(
        schema="s", dag_id="dag-1", plan_id="a" * 64, plan_revision=1, stages=stages,
        exchanges=(), residency=dto.ResidencyPlan(simultaneous_groups=1, control_reserved_bytes=0,
                                                  consumer_reserved_slots=1, required_spill_bytes=0),
        scopes=(), sources=(dto.SourceSnapshot(source_id="src-1", kind="items", immutable_identity="i",
                                               finite=True, replay="attempt_only", manifest_ref=_ref()),),
        splits=(dto.SourceSplit(split_id="split-1", source_id="src-1", locator="l", start_cursor={},
                                estimated_bytes=1, required_domain="d"),),
        policy=policy or make_policy(), bounds=bounds or make_bounds(), recovery="final_commit",
        limitations=(), constants_ref=_ref("constants"))


class FakeRef:
    run_id = "run-1"


class FakeContext:
    fence = "fence-1"
    attempt = 1
    ref = FakeRef()

    def check_stop(self):
        return None


class FakeAction:
    def __init__(self, action_id="action-1"):
        self.context = FakeContext()
        self.action_id = action_id
        self.declaration = {}


def make_request(kind="take", provider="local", sink=None):
    return dto.ActionRequest(kind=kind, source=ItemsSource(items=()), plan=LogicalPlan(),
                             arguments={}, sink=sink, programs=(), provider=provider, action=FakeAction())


def build_services(action=None, policy=None, bounds=None, resources=None):
    return dto.DriverServices(action=action or FakeAction(), journal_service=FakeJournalService(),
                              store=FakeStore(), resources=resources or FakeResourceManager(),
                              policy=policy or make_policy(), bounds=bounds or make_bounds(),
                              hosts=(dto.HostDescriptor(host_id="host-1", provider="local", runtime_revision="r1",
                                                        store_domains=("d",), logical_resources={"cpu_millis": 1.0},
                                                        isolation_capabilities=()),),
                              clock_ms=lambda: 0)


M_services = _mod("demiflow.execution.dataflow_services")
M_services.make_driver_services = lambda action, *, policy, bounds: build_services(action, policy, bounds)

M_terminals = _mod("demiflow.execution.dataflow_terminals")
M_terminals.open_terminal = lambda request, *, execution: _ActionHandle(execution)

M_checkpoint = _mod("demiflow.execution.dataflow_checkpoint")
M_checkpoint.DataflowCheckpointCoordinator = FakeCheckpoint

M_seal = _mod("demiflow.execution.dataflow_seal")
M_seal.seal_dataflow = lambda action, graph, physical, *, store: dto.SealedExecution(
    logical_ref=_ref(), physical_ref=_ref(), programs=(), code_pin_ref=_ref(),
    identity_bridge_ref=_ref(), source_refs=(), constants_ref=physical.constants_ref)
M_seal.stage_dataflow_programs = lambda compiled, *, action, store, bounds: compiled.graph
M_seal.resolve_execution_constants = lambda graph, *, action, store: _ref("constants")

M_sources = _mod("demiflow.execution.dataflow_sources")
M_sources.snapshot_source = lambda source, *, action, store, bounds: dto.SourceSnapshot(
    source_id="src-1", kind="items", immutable_identity="i", finite=True, replay="attempt_only", manifest_ref=_ref())
M_sources.plan_source_splits = lambda snapshot, source, *, target_partitions, bounds: (
    dto.SourceSplit(split_id="split-1", source_id="src-1", locator="l", start_cursor={}, estimated_bytes=1, required_domain="d"),)

M_queue = _mod("demiflow.execution.dataflow_queue")
M_queue.NativeAcquisitionService = FakeAcquisition

M_artifacts = _mod("demiflow.execution.dataflow_artifacts")
M_artifacts.stage_runtime_bundle = lambda sealed, *, store, bounds: _ref("bundle")

M_hosts = _mod("demiflow.execution.dataflow_hosts")
M_hosts.make_host_endpoint = lambda lease, *, services: FakeEndpoint(lease)

M_channels = _mod("demiflow.execution.exchange.dataflow_channels")
M_channels.close_channels = lambda channels, *, reason, deadline_ms: None

M_compile = _mod("demiflow.planning.dataflow_compile")
M_compile.lower_action = lambda request, *, bounds: dto.CompiledDataflow(
    graph=dto.DataflowGraph(schema="s", action_id="action-1", dag_id="dag-1", nodes=(), edges=(),
                            terminals=("stage-0",), sources=()), declarations=())

M_place = _mod("demiflow.planning.dataflow_place")
M_place.compile_physical_dataflow = lambda graph, snapshots, splits, *, policy, hosts, bounds, constants_ref: make_physical(policy=policy, bounds=bounds)
M_place.infer_recovery = lambda graph, snapshots, *, policy: dto.CapabilityReport(
    supported=True, reasons=(), recovery="final_commit", required_domains=(), required_profiles=())

M_policy = _mod("demiflow.planning.dataflow_policy")
M_policy.normalize_native_options = lambda options, *, operation_kind: {}
M_policy.resolve_runtime_policy = lambda options, limits, *, provider: (make_policy(provider), make_bounds())

M_models = _mod("demiflow.execution.dataflow_models")


def _build_model_factory(operation, *, program_ref, bounds):
    if operation.configuration.get("fail") is True:
        raise DataflowError("INVALID_CONTRACT", "native configuration rejected")
    return dto.KernelSpec(kind=operation.kind, configuration=dict(operation.configuration))


M_models.build_model_factory = _build_model_factory

# -- register fakes and import the REAL E modules --------------------------
for _name, _m in {
    "demiflow.execution.contracts.dataflow": dto,
    "demiflow.execution.contracts.dataflow_errors": errors,
}.items():
    if not _real_module_available(_name):
        sys.modules[_name] = _m
        setattr(demiflow.execution.contracts, _name.rsplit(".", 1)[1], _m)

if _real_module_available("demiflow.execution.contracts.dataflow"):
    import importlib
    dto = importlib.import_module("demiflow.execution.contracts.dataflow")
    errors = importlib.import_module("demiflow.execution.contracts.dataflow_errors")

from demiflow.execution import dataflow as E  # noqa: E402
from demiflow.execution import dataflow_scheduler as S  # noqa: E402
from demiflow.execution import dataflow_api as API  # noqa: E402

for _m in (E, S, API):
    assert str(Path(sys.modules[_m.__name__].__file__).resolve()).startswith(str(WORKTREE))

ArtifactRef = dto.ArtifactRef
ReadyWork = dto.ReadyWork


def make_work(stage_id="stage-0", partition_id="p0", resident=1, op=None):
    return ReadyWork(stage_id=stage_id, partition_id=partition_id, input_refs=(),
                     resident_bytes=resident, operation_id=op or uuid.uuid4().hex)


def new_scheduler(physical=None, resources=None):
    return S.DataflowScheduler(physical or make_physical(), resources=resources or FakeResourceManager())


# -- E001 execute_action ---------------------------------------------------
def test_e001():
    request = make_request()
    handle = E.execute_action(request, policy=make_policy(), bounds=make_bounds())
    assert isinstance(handle, _ActionHandle)
    # failure path closes the execution and re-raises
    original = E.open_terminal

    def boom(request, *, execution):
        raise DataflowError("INTERNAL_PROTOCOL", "terminal failed")

    E.open_terminal = boom
    try:
        with pytest.raises(DataflowError):
            E.execute_action(make_request(), policy=make_policy(), bounds=make_bounds())
    finally:
        E.open_terminal = original


# -- E002 DataflowExecution.__init__ ---------------------------------------
def test_e002():
    services = build_services()
    execution = E.DataflowExecution(make_request(), services=services)
    assert execution._state == "preparing"
    assert execution._started is False and execution._prepared is False
    with pytest.raises(DataflowError):
        E.DataflowExecution(object(), services=services)
    with pytest.raises(DataflowError):
        E.DataflowExecution(make_request(), services=None)


# -- E003 prepare ----------------------------------------------------------
def test_e003():
    services = build_services()
    execution = E.DataflowExecution(make_request(), services=services)
    sealed = execution.prepare()
    assert isinstance(sealed, dto.SealedExecution)
    assert execution._prepared is True
    assert execution._scheduler is not None and execution._checkpoint is not None
    assert services.journal_service.bound  # bind_queue called
    assert execution.prepare() is sealed  # idempotent


# -- E004 start ------------------------------------------------------------
def test_e004():
    services = build_services()
    execution = E.DataflowExecution(make_request(), services=services)
    with pytest.raises(DataflowError):
        execution.start()  # requires prepare
    execution.prepare()
    execution.start()
    assert execution._started is True and execution._state == "running"
    assert services.resources.state.scopes  # scope opened
    assert execution._acquisition.started is True
    assert execution._endpoints  # activated at least one lease
    execution.start()  # idempotent


# -- E005 pump -------------------------------------------------------------
def test_e005():
    services = build_services()
    execution = E.DataflowExecution(make_request(), services=services)
    execution.prepare()
    execution.start()
    status = execution.pump(deadline_ms=100)
    assert isinstance(status, dto.ExecutionStatus)
    assert status.state == "running"
    with pytest.raises(DataflowError):
        execution.pump(deadline_ms=0)
    # fenced event rejected
    alloc = next(iter(execution._endpoints))
    lease = execution._scheduler.leases[alloc]
    bad = dto.WorkerEvent(schema="v3", kind="progress", operation_id="op", run_id="run-1",
                          attempt=99, fence="fence-1", allocation_id=alloc, boot_id=lease.boot_id,
                          plan_revision=1, scope_id="df-" + "a" * 64, controller_epoch=1,
                          payload_ref=None, payload={"rows": 1})
    execution._endpoints[alloc].events.append(bad)
    with pytest.raises(DataflowError) as exc:
        execution.pump(deadline_ms=100)
    assert exc.value.code == "FENCED"


# -- E006 finish -----------------------------------------------------------
def test_e006():
    services = build_services()
    execution = E.DataflowExecution(make_request(), services=services)
    execution.prepare()
    execution.start()
    for op in list(execution._scheduler._inflight):
        execution._scheduler.complete(op, receipt_ref=_ref("receipt"))
    result = execution.finish()
    assert isinstance(result, dto.TerminalResult)
    assert result.complete is True
    assert execution._checkpoint.begun == [True]
    assert execution._checkpoint.committed
    assert execution._acquisition.finished is True
    assert execution.finish() is result  # idempotent


# -- E007 cancel -----------------------------------------------------------
def test_e007():
    services = build_services()
    execution = E.DataflowExecution(make_request(), services=services)
    execution.prepare()
    execution.start()
    execution.cancel("user-stop")
    assert execution._cancelled is True and execution._state == "cancelled"
    assert all(ep.cancelled for ep in execution._endpoints.values())
    execution.cancel("again")  # idempotent
    with pytest.raises(DataflowError):
        execution.finish()


# -- E008 close ------------------------------------------------------------
def test_e008():
    services = build_services()
    execution = E.DataflowExecution(make_request(), services=services)
    execution.prepare()
    execution.start()
    execution.close()
    assert execution._closed is True
    assert services.resources.closed  # leases released
    assert execution._acquisition.closed is True
    assert services.journal_service.closed is True
    execution.close()  # idempotent


# -- E009 scheduler init ---------------------------------------------------
def test_e009():
    scheduler = new_scheduler()
    assert scheduler.scope_id == "df-" + "a" * 64
    assert scheduler.leases == {}
    with pytest.raises(DataflowError):
        S.DataflowScheduler(object(), resources=FakeResourceManager())
    with pytest.raises(DataflowError):
        S.DataflowScheduler(make_physical(), resources=None)
    dup = make_physical(stages=(make_stage("stage-0"), make_stage("stage-0")))
    with pytest.raises(DataflowError):
        S.DataflowScheduler(dup, resources=FakeResourceManager())


# -- E010 enqueue ----------------------------------------------------------
def test_e010():
    scheduler = new_scheduler()
    work = make_work()
    scheduler.enqueue(work)
    assert len(scheduler._ready) == 1
    scheduler.enqueue(work)  # dedup same content
    assert len(scheduler._ready) == 1
    with pytest.raises(DataflowError):
        scheduler.enqueue(make_work(stage_id="unknown"))
    with pytest.raises(DataflowError):
        scheduler.enqueue(make_work(partition_id="nope"))
    with pytest.raises(DataflowError):
        scheduler.enqueue(make_work(resident=0))
    with pytest.raises(DataflowError):
        scheduler.enqueue(make_work(resident=100000))
    conflicting = ReadyWork(stage_id="stage-0", partition_id="p0", input_refs=(),
                            resident_bytes=2, operation_id=work.operation_id)
    with pytest.raises(DataflowError):
        scheduler.enqueue(conflicting)


# -- E011 choose -----------------------------------------------------------
def test_e011():
    scheduler = new_scheduler()
    idle = scheduler.choose(now_ms=0)
    assert idle.work is None and idle.reason == "idle"
    scheduler.enqueue(make_work(resident=8))
    choice = scheduler.choose(now_ms=0)
    assert choice.work is not None and choice.retry_after_ms == 0
    assert len(scheduler._ready) == 0
    # resident backpressure when budget exhausted
    sched2 = new_scheduler(resources=FakeResourceManager())
    sched2._resident_used = sched2._resident_budget
    sched2.enqueue(make_work(resident=1))
    bp = sched2.choose(now_ms=0)
    assert bp.work is None and bp.reason == "resident_backpressure"


# -- E012 place ------------------------------------------------------------
def test_e012():
    resources = FakeResourceManager()
    scheduler = new_scheduler(resources=resources)
    work = make_work(resident=4)
    lease = scheduler.place(work)
    assert isinstance(lease, dto.WorkerLease)
    assert lease.allocation_id in scheduler.leases
    assert scheduler._resident_used == 4 and scheduler._cumulative_cost == 4
    assert resources.described  # provider preflight
    with pytest.raises(DataflowError):
        scheduler.place(work)  # duplicate inflight
    # not RUNNING -> RESOURCE_INFEASIBLE
    bad = new_scheduler(resources=FakeResourceManager(phase="PENDING"))
    with pytest.raises(DataflowError) as exc:
        bad.place(make_work())
    assert exc.value.code == "RESOURCE_INFEASIBLE"


# -- E013 complete ---------------------------------------------------------
def test_e013():
    scheduler = new_scheduler()
    work = make_work(resident=4)
    scheduler.place(work)
    assert scheduler._resident_used == 4
    scheduler.complete(work.operation_id, receipt_ref=_ref("receipt"))
    assert scheduler._resident_used == 0  # transient returned
    assert scheduler._cumulative_cost == 4  # cumulative not refunded
    assert work.operation_id in scheduler._completed
    scheduler.complete(work.operation_id, receipt_ref=_ref("receipt"))  # idempotent
    with pytest.raises(DataflowError):
        scheduler.complete("unknown-op", receipt_ref=_ref())
    with pytest.raises(DataflowError):
        scheduler.complete(make_work().operation_id, receipt_ref="not-a-ref")


# -- E014 drain ------------------------------------------------------------
def test_e014():
    resources = FakeResourceManager()
    scheduler = new_scheduler(resources=resources)
    work = make_work()
    scheduler.place(work)
    scheduler.complete(work.operation_id, receipt_ref=_ref())
    scheduler.drain("stage-0", deadline_ms=100)
    assert scheduler.leases == {}
    assert resources.closed
    with pytest.raises(DataflowError):
        scheduler.drain("unknown", deadline_ms=100)
    # unfinished work cannot be preempted
    sched2 = new_scheduler()
    sched2.place(make_work())
    with pytest.raises(DataflowError) as exc:
        sched2.drain("stage-0", deadline_ms=100)
    assert exc.value.code == "DEADLINE_EXCEEDED"
    # unconfirmed exit -> DEADLINE_EXCEEDED
    sched3 = new_scheduler(resources=FakeResourceManager(cleanup="PENDING"))
    w3 = make_work()
    sched3.place(w3)
    sched3.complete(w3.operation_id, receipt_ref=_ref())
    with pytest.raises(DataflowError) as exc3:
        sched3.drain("stage-0", deadline_ms=100)
    assert exc3.value.code == "DEADLINE_EXCEEDED"


# -- E015 make_action_request ----------------------------------------------
def test_e015(monkeypatch):
    class Executor:
        NAME = "local"

    bound = []

    class FakeRun:
        def action(self, declaration):
            bound.append(declaration)
            from contextlib import contextmanager

            @contextmanager
            def cm():
                yield FakeAction()
            return cm()

    monkeypatch.setattr(API, "current_action", lambda: None)
    monkeypatch.setattr(API, "current_run", lambda: FakeRun())
    request = API.make_action_request(Executor(), ItemsSource(items=()), LogicalPlan(),
                                      kind="take", arguments={})
    assert request.provider == "local" and request.kind == "take"
    assert len(bound) == 1  # admission exactly once
    # reuse current action, no new binding
    monkeypatch.setattr(API, "current_action", lambda: FakeAction("existing"))
    request2 = API.make_action_request(Executor(), ItemsSource(items=()), LogicalPlan(),
                                       kind="count", arguments={})
    assert request2.action.action_id == "existing"
    assert len(bound) == 1
    # ray provider mapping
    class RayExecutor:
        NAME = "ray"

    monkeypatch.setattr(API, "current_action", lambda: FakeAction())
    request3 = API.make_action_request(RayExecutor(), ItemsSource(items=()), LogicalPlan(),
                                       kind="take", arguments={})
    assert request3.provider == "ray_core"
    with pytest.raises(DataflowError):
        API.make_action_request(Executor(), ItemsSource(items=()), LogicalPlan(),
                                kind="not-a-kind", arguments={})


# -- E016 run_dataset_action -----------------------------------------------
def test_e016(monkeypatch):
    seen = {}

    def fake_resolve(options, limits, *, provider):
        seen["provider"] = provider
        return make_policy(provider), make_bounds()

    sentinel = object()

    class Handle:
        def result(self):
            return sentinel

    def fake_execute(request, *, policy, bounds):
        seen["executed"] = request.provider
        return Handle()

    monkeypatch.setattr(API, "resolve_runtime_policy", fake_resolve)
    monkeypatch.setattr(API, "execute_action", fake_execute)
    out = API.run_dataset_action(make_request(provider="ray_core"))
    assert out is sentinel
    assert seen["provider"] == "ray_core" and seen["executed"] == "ray_core"


# -- E017 make_native_operation --------------------------------------------
def test_e017():
    op = API.make_native_operation("embed", {"model": "m"}, bindings={"port": "p"})
    assert isinstance(op, dto.NativeOperation)
    assert op.kind == "embed" and op.configuration == {"model": "m"}
    assert callable(op.factory.target)
    with pytest.raises(DataflowError):
        API.make_native_operation("embed", {"fail": True}, bindings={})
    with pytest.raises(DataflowError):
        API.make_native_operation("", {}, bindings={})
    with pytest.raises(DataflowError):
        API.make_native_operation("embed", {"x": float("nan")}, bindings={})


# -- E018 explain_dataset --------------------------------------------------
def test_e018(monkeypatch):
    seen = {}

    def fake_lower(request, *, bounds):
        seen["lower"] = request.kind
        return dto.CompiledDataflow(graph=dto.DataflowGraph(schema="s", action_id="a", dag_id="d",
                                                            nodes=(), edges=(), terminals=(), sources=()),
                                    declarations=())

    def fake_infer(graph, snapshots, *, policy):
        seen["snapshots"] = snapshots
        return dto.CapabilityReport(supported=True, reasons=(), recovery="attempt_only",
                                    required_domains=(), required_profiles=())

    monkeypatch.setattr(API, "lower_action", fake_lower)
    monkeypatch.setattr(API, "infer_recovery", fake_infer)
    report = API.explain_dataset(ItemsSource(items=()), LogicalPlan(), provider="local", options={})
    assert isinstance(report, dto.CapabilityReport)
    assert seen["snapshots"] == ()  # no snapshots claimed verified
    with pytest.raises(DataflowError):
        API.explain_dataset(ItemsSource(items=()), LogicalPlan(), provider="bad", options={})


# -- E019 declare_action_v3 ------------------------------------------------
def test_e019():
    historical = API.declare_action_v3(None, LogicalPlan(), kind="take", arguments={"n": 1},
                                       sink_declaration=None)
    assert historical["kind"] == "take" and historical["operations"] == []
    assert historical["arguments"] == {"n": 1}

    def factory():
        return None

    native = dto.NativeOperation(kind="embed", configuration={"m": 1}, bindings={},
                                 factory=CallableSpec.create(target=factory))
    declared = API.declare_action_v3(None, LogicalPlan(operations=(native,)), kind="take",
                                     arguments={}, sink_declaration=None)
    identity = declared["operations"][0]
    assert identity["native"]["contract"] == "demiflow.native.operation.v3"
    assert identity["native"]["kind"] == "embed"
    assert "code_identity" in identity["native"]
    with pytest.raises(DataflowError):
        API.declare_action_v3(None, LogicalPlan(), kind="bogus", arguments={}, sink_declaration=None)