"""Assemble one action's driver services on its existing journal and store."""
from __future__ import annotations

import time
from demiflow.execution.contracts.dataflow import Bounds, DriverServices, RuntimePolicy
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_authority import DriverArtifactPort, DriverJournalService
from demiflow.execution.storage.objects import RunArtifactStore
from demiflow.execution.hosts.configuration import driver_resources, driver_hosts


def make_driver_services(action: "RunAction", *, policy: RuntimePolicy, bounds: Bounds) -> DriverServices:
    if not isinstance(policy, RuntimePolicy):
        raise DataflowError("INVALID_CONTRACT", "policy must be a RuntimePolicy")
    if not isinstance(bounds, Bounds):
        raise DataflowError("INVALID_CONTRACT", "bounds must be a Bounds")
    run = action.context
    port = DriverArtifactPort(action, bounds=bounds)
    store = RunArtifactStore(root=run.directory / ".dataflow-store", store_id=policy.store_id,
                             journal=port, bounds=bounds)
    journal_service = None
    try:
        if action.replaying and run.journal.source_binding(action.action_id + "-dataflow-sealed") is not None:
            from dataclasses import replace
            from demiflow.execution.seal.plan import load_dataflow
            _, physical, _ = load_dataflow(run.directory, action.action_id, store=store, bounds=bounds)
            policy = replace(policy, worker_mode=physical.policy.worker_mode)
        resources, job_resources, job_gpu = driver_resources(action, policy)
        hosts = driver_hosts(action, policy, store, bounds, provider=resources.provider)
        journal_service = DriverJournalService(action, store=store, bounds=bounds)
        return DriverServices(action=action, journal_service=journal_service, store=store,
                              resources=resources, policy=policy, bounds=bounds, hosts=hosts,
                              clock_ms=lambda: int(time.time() * 1000), job_resources=job_resources, job_gpu=job_gpu)
    except BaseException as failure:
        try:
            if journal_service is not None:
                journal_service.close(deadline_ms=bounds.timeout_ms)
            store.close()
        except BaseException as cleanup:
            failure.add_note('Driver store cleanup failed: ' + str(cleanup))
        raise


def resolve_action_runtime(request):
    """Bind the current public executor and run envelope to one action policy."""
    executor = getattr(request.action, '_dataflow_executor', None)
    local = getattr(executor, '_local_kernel', None)
    options = {}
    workers = getattr(executor, '_workers', None)
    if workers is not None:
        options['max_active_workers'] = workers
    if local is not None:
        options['memory_bytes'] = local.memory_bytes
    mode = select_worker_mode(request, local)
    if mode is not None:
        options['worker_mode'] = mode
    run = request.action.context
    limits = dict(run.limits)
    explicit = set(run.explicit_limits)
    from demiflow.execution.contracts.dataflow import EXECUTION_LIMITS, execution_ceilings
    ceilings = execution_ceilings(request.arguments.get(EXECUTION_LIMITS))
    declared = getattr(executor, '_native_resources', None)
    native_inherited = declared is None and getattr(local, 'native_resources_inherited', False)
    if declared is None:
        declared = getattr(local, 'native_resources', None)
    aggregate_sizes = None
    if request.kind == 'aggregate' and not request.action.replaying:
        aggregate_sizes = (getattr(executor, '_block_size', limits['batch_rows']),
                           limits['batch_rows'], getattr(local, 'batch_rows', limits['batch_rows']))
    from demiflow.execution.scheduler.native_limits import resolve_action_limits
    return resolve_action_limits(options=options, limits=limits, explicit=explicit, ceilings=ceilings,
        declared=declared, native_inherited=native_inherited, aggregate_sizes=aggregate_sizes,
        provider=request.provider)


def select_worker_mode(request, local):
    """Explicit streaming thread scopes select a local host unless overridden."""
    if request.provider != 'local':
        return None
    if local is not None and getattr(local, 'worker_mode_explicit', True):
        return local.worker_mode
    from demiflow.planning.dataflow_compile import expand_relational_inputs, expand_operation_inputs
    from demiflow.data.plan import AsyncMapOp, MapOp, BoundMapOp
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    _, bounds = resolve_runtime_policy({}, request.action.context.limits, provider='local')
    pending = [(request.source, request.plan, ())]
    count = 0
    while pending:
        source, plan, path = pending.pop()
        count += 1 + len(plan.operations)
        if count > bounds.max_nodes:
            raise DataflowError('BUDGET_EXCEEDED', 'thread mode selection exceeds graph budget')
        for operation in plan.operations:
            stream = operation.stream_options if isinstance(operation, (MapOp, BoundMapOp)) else None
            if (stream is not None and stream.execution == 'thread'
                    or isinstance(operation, AsyncMapOp) and operation.execution == 'thread'):
                return 'thread'
            pending.extend((b.source, b.plan, b.path) for b in
                           expand_operation_inputs(operation, path=path, bounds=bounds))
        pending.extend((b.source, b.plan, b.path) for b in expand_relational_inputs(source, path=path, bounds=bounds))
    return getattr(local, 'worker_mode', 'process')
