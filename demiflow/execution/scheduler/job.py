"""One sealed job occupies one whole resource group until the provider proves exit."""
import time

from ...resources.contracts import (RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan,
                                   ResourceScope, WorkerRequest)
from ..contracts.dataflow_errors import DataflowError


class JobAllocation:
    def __init__(self, run, key, manager):
        self.run = run
        self.key = key
        self.scope_id = 'fit-' + key
        self.manager = manager

    def _context(self, operation, revision):
        return MutationContext(RESOURCE_PROTOCOL_VERSION, self.scope_id, self.run.attempt,
                               self.run.fence, operation + '-' + self.key, revision)

    def admit(self, scheduler, occurrence, profile, launch, *, authority, timeout_ms):
        host, budget = scheduler.place_job(occurrence, profile, self.key)
        if host != 'local':
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'The managed process carrier needs one local resource host')
        binding = dict(provider='local', worker_mode='process', capacity_root=str(self.manager.provider.pool.root),
                       job_authority=authority)
        self.manager.state.open_scope(self.run.fence, ResourceScope(self.scope_id, self.run.ref.run_id,
                                      binding, self.scope_id, budget))
        request = WorkerRequest(self.key, self.key, profile, launch, provision_deadline_ms=timeout_ms)
        self.manager.apply(self._context('launch', 0), ResourcePlan(self.scope_id, (request,), ()))

    def accepted(self):
        if self.manager.state.scope(self.scope_id) is None:
            return False
        return self.manager.state.read_allocation(self.scope_id, self.key) is not None

    def allocated_gpu_seconds(self):
        if not self.manager.state.cleanup_complete(self.scope_id):
            return None
        row = self.manager.state.read_cleanup_allocation(self.scope_id, self.key)
        if not row['effective'].get('accelerators'):
            return 0.0
        return self.manager.provider.released_device_seconds(self.key, row['spec_hash'], 'gpu')

    def refresh(self, *, deadline, observe_only=False):
        if not observe_only:
            self.manager.reconcile_once(self.run.fence, deadline, 1)
        status = self.manager.state.read_allocation(self.scope_id, self.key)
        observations = self.manager.provider.inspect(self.scope_id, None)['allocations']
        if observations:
            item, = observations
            if item['allocation_id'] != self.key or item['observation'] != 'FRESH':
                return 'unknown'
            return item['phase'].lower()
        return status['phase'].lower() if status else 'unknown'

    def retire(self, *, cancelled, timeout_ms):
        scope = self.manager.state.scope(self.scope_id)
        if scope['state'] == 'OPEN':
            self.manager.close(self._context('release', scope['desired_revision']),
                dict(mode='ABANDON' if cancelled else 'DRAINED', engine_receipt_ref=self.key,
                     terminate_deadline_ms=timeout_ms, reason='job_cancelled' if cancelled else 'job_ended'))
        self.manager.reconcile_once(self.run.fence, time.monotonic() + timeout_ms / 1000, 1)
        return self.manager.state.cleanup_complete(self.scope_id)
