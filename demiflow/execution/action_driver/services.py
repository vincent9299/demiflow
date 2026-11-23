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
