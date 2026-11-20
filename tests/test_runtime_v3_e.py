"""E001–E019 against integrated contracts, real driver and native journal.

Only the scheduler's resource-provider observation boundary is simulated. The
execution cases launch real managed workers; no replacement DTO/module graph.
"""
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace as NS
import time
import uuid

import pytest
import demiflow
from demiflow import data
from demiflow.data.plan import CallableSpec, LogicalPlan
from demiflow.data.sources import ItemsSource
from demiflow.execution import dataflow as E, dataflow_api as API, dataflow_scheduler as S
from demiflow.execution.contracts import dataflow as dto
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_services import make_driver_services
from demiflow.execution.recovery_contract import identity
from demiflow.execution.run_context import RunContext
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from demiflow.resources.contracts import ResourceProfile

WORKTREE = Path(__file__).resolve().parents[1]
assert Path(demiflow.__file__).resolve().is_relative_to(WORKTREE)


def _ref(key='fixture'):
    return dto.ArtifactRef(identity(key), 1, 'store-1', identity(key), 'application/json')


@contextmanager
def inspection_run(directory):
    class InspectionEnded(Exception):
        pass
    try:
        with RunContext.start(directory) as run:
            yield run
            # Lifecycle inspection deliberately ends before a business cut.
            # Mark it interrupted instead of falsely finishing the run green.
            if run.journal.incomplete_execution():
                raise InspectionEnded()
    except InspectionEnded:
        assert run.journal.execution_status()['status'] == 'failed'


@contextmanager
def runtime(tmp_path, *, kind='take_all'):
    with inspection_run(tmp_path/'run') as run:
        source, plan = ItemsSource(items=({'x': 1}, {'x': 2})), LogicalPlan()
        declaration = API.declare_action_v3(source, plan, kind=kind, arguments={}, sink_declaration=None)
        with run.action(declaration) as action:
            request = dto.ActionRequest(kind, source, plan, {}, None, (), 'local', action)
            policy, bounds = resolve_runtime_policy({}, run.limits, provider='local')
            services = make_driver_services(action, policy=policy, bounds=bounds)
            execution = E.DataflowExecution(request, services=services)
            try:
                yield execution
            finally:
                execution.close()


class ResourceObservations:
    """Provider observations only; production scheduler constructs all grants."""
    def __init__(self, *, phase='RUNNING', cleanup='CONFIRMED'):
        class Conn:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql): return self
            def fetchone(self): return (1, 'fence-1')
        self.state = NS(journal=NS(recovery_identity=lambda: {'run_id': 'run-1'}, _connection=Conn))
        self.phase, self.cleanup = phase, cleanup
        self.allocations, self.applied = {}, []
        self.boots = {}

    def prepare(self, lease, stage):
        self.boots[lease.allocation_id] = lease.boot_id
        return lease

    def apply(self, context, plan):
        self.applied.append(plan)
        for worker in plan.desired_workers:
            self.allocations[worker.allocation_id] = dict(allocation_id=worker.allocation_id,
                phase=self.phase, boot_id=self.boots[worker.allocation_id], cleanup='PENDING')
        for release in plan.retirements:
            self.allocations[release.allocation_id]['cleanup'] = self.cleanup
        return {'desired_revision': len(self.applied)}

    def reconcile_once(self, *args): pass
    def get(self, *args, **kwargs): return {'allocations': list(self.allocations.values())}


def make_physical(*, duplicate=False):
    policy, bounds = resolve_runtime_policy({}, {}, provider='local')
    bounds = replace(bounds, max_resident_bytes=1024, max_pending_events=10)
    stage = dto.StageSpec('stage-0', ('occurrence-0',), ('p0',),
        ResourceProfile(requests={'cpu_millis': 100, 'memory_bytes': 16}),
        1, 1, 1, 16, 1, 1, (_ref('kernel'),))
    return dto.PhysicalDataflow('demiflow.physical_dataflow.v3', identity('dag'), identity('plan'),
        0, (stage, stage) if duplicate else (stage,), (), dto.ResidencyPlan((('stage-0',),), 0, 1, 0),
        (), (), (), policy, bounds, 'checkpoint', (), _ref('constants'))


def new_scheduler(**kwargs):
    return S.DataflowScheduler(make_physical(), resources=ResourceObservations(**kwargs))


def make_work(*, stage_id='stage-0', partition_id='p0', resident=4, op=None):
    return dto.ReadyWork(stage_id, partition_id, (), resident, op or identity(uuid.uuid4().hex))


def place(scheduler, work):
    return scheduler.place(work, prepare=scheduler._resources.prepare)


def test_e001(tmp_path, monkeypatch):
    with runtime(tmp_path) as execution:
        handle = E.execute_action(execution._request, policy=execution._policy, bounds=execution._bounds)
        try:
            assert [dict(record.inline) for record in handle.rows()] == [{'x': 1}, {'x': 2}]
            assert handle.result().complete
        finally:
            handle.close()
    closed = []
    original = E.DataflowExecution.close
    def close(self):
        try: return original(self)
        finally: closed.append(self._closed)
    def fail(request, *, execution):
        raise DataflowError('INTERNAL_PROTOCOL', 'terminal failed')
    monkeypatch.setattr(E.DataflowExecution, 'close', close)
    monkeypatch.setattr(E, 'open_terminal', fail)
    with runtime(tmp_path/'failure') as execution:
        with pytest.raises(DataflowError, match='terminal failed'):
            E.execute_action(execution._request, policy=execution._policy, bounds=execution._bounds)
        assert closed and all(closed)


def test_e002(tmp_path):
    with runtime(tmp_path) as execution:
        assert execution._state == 'preparing' and not execution._started and not execution._prepared
        with pytest.raises(DataflowError): E.DataflowExecution(object(), services=execution._services)
        with pytest.raises(DataflowError): E.DataflowExecution(execution._request, services=None)


def test_e003(tmp_path):
    with runtime(tmp_path) as execution:
        sealed = execution.prepare()
        assert isinstance(sealed, dto.SealedExecution) and execution._prepared
        assert execution._scheduler is not None and execution._checkpoint is not None
        assert execution.prepare() is sealed
        binding = execution._action.context.journal.source_binding(execution._action.action_id+'-dataflow-sealed')
        assert binding and execution._acquisition is None  # pure Dataset opens no queue owner


def test_e004(tmp_path):
    with runtime(tmp_path) as execution:
        with pytest.raises(DataflowError): execution.start()
        execution.prepare(); execution.start()
        assert execution._started and execution._state == 'running'
        assert execution._scope_open and execution._endpoints
        allocations = execution._services.resources.get(execution._scheduler.scope_id)['allocations']
        assert allocations and all(a['phase'] == 'RUNNING' for a in allocations)
        old = dict(execution._endpoints)
        execution.start()
        assert execution._endpoints == old


def test_e005(tmp_path, monkeypatch):
    with runtime(tmp_path) as execution:
        execution.prepare(); execution.start()
        status = execution.pump(deadline_ms=100)
        assert isinstance(status, dto.ExecutionStatus)
        with pytest.raises(DataflowError): execution.pump(deadline_ms=0)
        allocation, endpoint = next(iter(execution._endpoints.items()))
        lease = execution._scheduler.leases[allocation]
        bad = dto.WorkerEvent('demiflow.dataflow_event.v3', 'progress', identity('bad-event'),
            execution._run_id(), execution._attempt()+1, execution._fence(), allocation, lease.boot_id,
            execution._physical.plan_revision, execution._scheduler.scope_id, execution._attempt(), None, {'rows': 1})
        monkeypatch.setattr(endpoint, 'poll', lambda **kwargs: (bad,))
        with pytest.raises(DataflowError) as exc: execution.pump(deadline_ms=100)
        assert exc.value.code == 'FENCED'


def test_e006(tmp_path):
    with runtime(tmp_path, kind='count') as execution:
        execution.prepare(); execution.start()
        with pytest.raises(DataflowError): execution.finish()  # no fabricated success before cut
        deadline = time.monotonic()+30
        while execution._terminal_result is None:
            assert time.monotonic() < deadline
            execution.pump(deadline_ms=50)
        result = execution.finish()
        assert result.complete and result.scalar == 2 and execution.finish() is result
        facts = execution._action.context.journal.execution_commits(execution._action.action_id)
        assert facts and facts[-1]['status'] == 'acked'
        carrier = execution._checkpoint._artifacts.read(facts[-1]['prepared']['state']['dataflow-cut-v3'])
        assert carrier['cut']['final']


def test_e007(tmp_path, monkeypatch):
    with runtime(tmp_path) as execution:
        execution.prepare(); execution.start()
        calls = []
        for endpoint in execution._endpoints.values():
            original = endpoint.cancel
            def cancel(reason, *, deadline_ms, native=original):
                calls.append(reason); return native(reason, deadline_ms=deadline_ms)
            monkeypatch.setattr(endpoint, 'cancel', cancel)
        execution.cancel('user-stop'); execution.cancel('again')
        assert execution._cancelled and execution._state == 'cancelled'
        assert calls and set(calls) == {'user-stop'}
        with pytest.raises(DataflowError): execution.finish()
        assert not execution._action.context.journal.execution_commits(execution._action.action_id)


def test_e008(tmp_path):
    with runtime(tmp_path) as execution:
        execution.prepare(); execution.start()
        scope = execution._scheduler.scope_id
        execution.close(); execution.close()
        assert execution._closed and execution._services.journal_service._closed
        allocations = execution._services.resources.get(scope)['allocations']
        assert allocations and all(a['cleanup'] == 'CONFIRMED' for a in allocations)


def test_e009():
    scheduler = new_scheduler()
    assert scheduler.scope_id == 'df-'+identity('plan') and not scheduler.leases
    with pytest.raises(DataflowError): S.DataflowScheduler(object(), resources=ResourceObservations())
    with pytest.raises(DataflowError): S.DataflowScheduler(make_physical(), resources=None)
    with pytest.raises(DataflowError): S.DataflowScheduler(make_physical(duplicate=True), resources=ResourceObservations())


def test_e010():
    scheduler, work = new_scheduler(), make_work()
    scheduler.enqueue(work); scheduler.enqueue(work)
    assert len(scheduler._ready) == 1
    for fields in ({'stage_id':'unknown'}, {'partition_id':'nope'}, {'resident':0}, {'resident':100000}):
        with pytest.raises(DataflowError): scheduler.enqueue(make_work(**fields))
    with pytest.raises(DataflowError): scheduler.enqueue(replace(work, resident_bytes=2))


def test_e011():
    scheduler = new_scheduler()
    assert scheduler.choose(now_ms=0).reason == 'idle'
    scheduler.enqueue(make_work(resident=8))
    choice = scheduler.choose(now_ms=0)
    assert choice.work is not None and choice.retry_after_ms == 0 and not scheduler._ready
    scheduler._resident_used = scheduler._resident_budget
    scheduler.enqueue(make_work(resident=1))
    assert scheduler.choose(now_ms=0).reason == 'resident_backpressure'


def test_e012():
    scheduler, work = new_scheduler(), make_work()
    lease = place(scheduler, work)
    assert isinstance(lease, dto.WorkerLease) and lease.allocation_id in scheduler.leases
    assert scheduler._resident_used == scheduler._cumulative_cost == 4
    assert scheduler._resources.applied and scheduler.grants[lease.allocation_id]
    with pytest.raises(DataflowError): place(scheduler, work)
    pending, item = new_scheduler(phase='PENDING'), make_work()
    for _ in range(2):
        with pytest.raises(DataflowError) as exc: place(pending, item)
        assert exc.value.code == 'CAPACITY_PENDING' and exc.value.retryable
    assert len(pending.leases) == len(pending._resources.applied) == 1
    assert pending._resident_used == 4  # pending retains its one grant
    with pytest.raises(DataflowError) as exc: place(new_scheduler(phase='FAILED'), make_work())
    assert exc.value.code == 'RESOURCE_INFEASIBLE'


def test_e013():
    scheduler, work = new_scheduler(), make_work()
    place(scheduler, work)
    scheduler.complete(work.operation_id, receipt_ref=_ref('receipt'))
    scheduler.complete(work.operation_id, receipt_ref=_ref('receipt'))
    assert scheduler._resident_used == 4  # actual process still owns its memory
    assert scheduler._cumulative_cost == 4 and work.operation_id in scheduler._completed
    scheduler.drain('stage-0', deadline_ms=100)
    assert scheduler._resident_used == 0 and scheduler._cumulative_cost == 4
    with pytest.raises(DataflowError): scheduler.complete(identity('unknown'), receipt_ref=_ref())
    other = make_work()
    place(scheduler, other)
    with pytest.raises(DataflowError): scheduler.complete(other.operation_id, receipt_ref='not-a-ref')


def test_e014():
    scheduler, work = new_scheduler(), make_work()
    place(scheduler, work); scheduler.complete(work.operation_id, receipt_ref=_ref())
    scheduler.drain('stage-0', deadline_ms=100)
    assert not scheduler.leases
    with pytest.raises(DataflowError): scheduler.drain('unknown', deadline_ms=100)
    running = new_scheduler(); place(running, make_work())
    with pytest.raises(DataflowError) as exc: running.drain('stage-0', deadline_ms=100)
    assert exc.value.code == 'DEADLINE_EXCEEDED'
    pending, work = new_scheduler(cleanup='PENDING'), make_work()
    place(pending, work); pending.complete(work.operation_id, receipt_ref=_ref())
    with pytest.raises(DataflowError) as exc: pending.drain('stage-0', deadline_ms=100)
    assert exc.value.code == 'DEADLINE_EXCEEDED' and pending._resident_used == 4


def test_e015(tmp_path, monkeypatch):
    with inspection_run(tmp_path/'run') as run:
        calls, native = [], run.action
        def action(declaration): calls.append(declaration); return native(declaration)
        monkeypatch.setattr(run, 'action', action)
        source, plan = ItemsSource(items=()), LogicalPlan()
        request = API.make_action_request(NS(NAME='local'), source, plan, kind='take', arguments={})
        assert request.provider == 'local' and len(calls) == 1
        monkeypatch.setattr(API, 'current_action', lambda: request.action)
        reused = API.make_action_request(NS(NAME='local'), source, plan, kind='count', arguments={})
        assert reused.action is request.action and len(calls) == 1
        ray = API.make_action_request(NS(NAME='ray'), source, plan, kind='take', arguments={})
        assert ray.provider == 'ray_core'
        with pytest.raises(DataflowError): API.make_action_request(NS(NAME='local'), source, plan, kind='bad', arguments={})


def test_e016(tmp_path, monkeypatch):
    calls, native = [], API.execute_action
    def execute(request, **kwargs): calls.append(request.provider); return native(request, **kwargs)
    monkeypatch.setattr(API, 'execute_action', execute)
    with RunContext.start(tmp_path/'run'):
        assert data.from_items([{'x':1}, {'x':2}]).count() == 2
    assert calls == ['local']


def test_e017():
    op = API.make_native_operation('embedding', {'model':'m'}, bindings={'port':'p'})
    assert isinstance(op, dto.NativeOperation) and op.configuration == {'model':'m'}
    assert callable(op.factory.target)
    with pytest.raises(DataflowError): op.factory.target()  # never construct runtime on driver
    for kind, config in (('not-a-native-op', {}), ('', {}), ('embedding', {'x':float('nan')}),
                         ('embedding', {'client':object()})):
        with pytest.raises(DataflowError): API.make_native_operation(kind, config, bindings={})


def test_e018(monkeypatch):
    seen, original = [], API.infer_recovery
    def infer(graph, snapshots, *, policy):
        seen.append(snapshots); return original(graph, snapshots, policy=policy)
    monkeypatch.setattr(API, 'infer_recovery', infer)
    report = API.explain_dataset(ItemsSource(items=()), LogicalPlan(), provider='local', options={})
    assert isinstance(report, dto.CapabilityReport) and seen == [()]
    with pytest.raises(DataflowError): API.explain_dataset(ItemsSource(items=()), LogicalPlan(), provider='bad', options={})


def test_e019():
    historical = API.declare_action_v3(None, LogicalPlan(), kind='take', arguments={'n':1}, sink_declaration=None)
    assert historical['kind'] == 'take' and historical['operations'] == []
    assert historical['arguments'] == {'n':1}
    op = API.make_native_operation('embedding', {'model':'m'}, bindings={})
    declared = API.declare_action_v3(None, LogicalPlan(operations=(op,)), kind='take', arguments={}, sink_declaration=None)
    native = declared['operations'][0]['native']
    assert native['contract'] == 'demiflow.native.operation.v3' and native['kind'] == 'embedding'
    assert 'code_identity' in native
    with pytest.raises(DataflowError): API.declare_action_v3(None, LogicalPlan(), kind='bogus', arguments={}, sink_declaration=None)


@pytest.mark.parametrize('committed', [False, True])
def test_uncertain_worker_acceptance_preserves_original_mutation_and_reservation(committed):
    scheduler = new_scheduler()
    resources = scheduler._resources
    submit = resources.apply
    snapshots = resources.get
    calls = []

    def lose_first_receipt(context, plan):
        calls.append((context, plan))
        if len(calls) == 1:
            if committed:
                submit(context, plan)
            raise ConnectionError('receipt unavailable')
        return submit(context, plan)

    resources.apply = lose_first_receipt
    resources.get = lambda scope: dict(snapshots(scope), desired_revision=len(resources.applied))
    work = make_work()
    if not committed:
        with pytest.raises(DataflowError, match='acceptance remains unconfirmed'):
            place(scheduler, work)
        assert not scheduler.leases and scheduler._resident_used == 0
        with pytest.raises(DataflowError, match='another worker acceptance'):
            place(scheduler, make_work())
        assert len(calls) == len(resources.boots) == 1
    lease = place(scheduler, work)
    if not committed:
        assert len(calls) == 2 and calls[0] == calls[1]
    assert len(resources.applied) == len(resources.boots) == len(scheduler.leases) == 1
    assert resources.boots[lease.allocation_id] == lease.boot_id
    assert scheduler._resident_used == scheduler._cumulative_cost == work.resident_bytes


def test_group_placement_reserves_publication_and_checks_each_host():
    physical = make_physical()
    stage = physical.stages[0]
    hosts = tuple(NS(host_id=name, logical_resources={'cpu_millis': 2000, 'memory_bytes': 512})
                  for name in ('one', 'two'))
    scheduler = S.DataflowScheduler(physical, resources=ResourceObservations(), hosts=hosts,
                                    publication_reservation=NS(reserved=lambda stages: 128))
    work = make_work(resident=400)
    too_big = replace(stage, profile=ResourceProfile(requests={'cpu_millis': 3000, 'memory_bytes': 16}))
    with pytest.raises(DataflowError, match='No verified host'):
        scheduler.place_hosts([(too_big, work)])
    with pytest.raises(DataflowError, match='memory budget'):
        scheduler.place_hosts([(stage, work), (stage, make_work(resident=500))])
    assert scheduler.place_hosts([(stage, work)]) == {work.operation_id: 'one'}
    assert not scheduler._resources.applied and not scheduler.leases


@pytest.mark.parametrize('failure', ['before_accept', 'after_accept', 'second_activate'])
def test_partial_group_launch_retires_original_workers(tmp_path, monkeypatch, failure):
    from demiflow import data
    from demiflow.resources.manager import ResourceManager
    resolve = API.resolve_runtime_policy

    def policy(*args, **kwargs):
        policy, bounds = resolve(*args, **kwargs)
        return replace(policy, default_partitions=8, max_replicas=8), bounds

    monkeypatch.setattr(API, 'resolve_runtime_policy', policy)

    starts, activated = [], []
    original_apply = ResourceManager.apply
    original_prepare = E.DataflowExecution._prepare_launch
    original_activate = E.DataflowExecution._activate

    def prepare(execution, lease, stage):
        staged = original_prepare(execution, lease, stage)
        starts.append((execution, staged))
        return staged

    def apply(resources, context, plan):
        if len(plan.desired_workers) == 2 and failure != 'second_activate':
            if failure == 'after_accept':
                original_apply(resources, context, plan)
            # Interrupt the phase without offering driver-level retries.
            raise KeyboardInterrupt('second worker interrupted')
        return original_apply(resources, context, plan)

    def activate(execution, lease, stage):
        if failure == 'second_activate' and activated:
            raise KeyboardInterrupt('second worker interrupted')
        activated.append(lease.allocation_id)
        return original_activate(execution, lease, stage)

    monkeypatch.setattr(E.DataflowExecution, '_prepare_launch', prepare)
    monkeypatch.setattr(E.DataflowExecution, '_activate', activate)
    monkeypatch.setattr(ResourceManager, 'apply', apply)
    with pytest.raises(KeyboardInterrupt, match='second worker'):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=2):
            data.from_items([{'n': n} for n in range(8)]).map(lambda row: {'n': row['n'] + 1}).take_all()
    assert len(starts) == 2
    assert activated == ([starts[0][1].allocation_id] if failure == 'second_activate' else [])
    execution = starts[0][0]
    assert execution._scheduler.physical_exit_confirmed
    connector = execution._services.resources.provider._control
    assert not connector._staged
    # Inspect the actual journal after the owning context closes.
    import sqlite3
    with sqlite3.connect(str(run.journal.path)) as db:
        rows = dict(db.execute("SELECT allocation_id,cleanup FROM resource_allocations"))
    assert rows == {lease.allocation_id: 'CONFIRMED' for _, lease in
                    (starts[:1] if failure == 'before_accept' else starts)}
