"""One sealed job occupies one whole resource group until the provider proves exit."""
import time

from ...resources.contracts import (RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan,
                                   ResourceScope, WorkerRequest)
from ..contracts.dataflow_errors import DataflowError
from ..recovery_contract import identity


class JobAllocation:
    def __init__(self, run, key, manager):
        self.run = run
        self.key = key
        self.scope_id = 'fit-' + key
        self.manager = manager
        scope = manager.state.scope(self.scope_id)
        if scope is not None:
            self.verify_authority(manager, scope, run, key)

    @staticmethod
    def verify_authority(manager, scope, run, key):
        binding = scope['document']['provider_binding']
        if (scope['run_id'] != run.ref.run_id or binding.get('provider') != 'local'
                or binding.get('worker_mode') != 'process'
                or binding.get('capacity_root') != str(manager.provider.pool.root)
                or binding.get('job_authority') != run.journal.source_binding('fit-resource-' + key)):
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Original training resource domain changed')
        if 'device_authority' in binding:
            if binding['device_authority'] != identity(manager.provider.device_authority()):
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Original training device authority changed')
        elif manager.state.read_allocation(scope['scope_id'], key):
            record = manager.state.read_cleanup_allocation(scope['scope_id'], key)
            if record['effective'].get('accelerators'):
                raise DataflowError('UNKNOWN_EFFECT', 'Original training device authority has no receipt')

    def _context(self, operation, revision):
        return MutationContext(RESOURCE_PROTOCOL_VERSION, self.scope_id, self.run.attempt,
                               self.run.fence, operation + '-' + self.key, revision)

    def admit(self, scheduler, occurrence, profile, launch, *, authority, timeout_ms):
        host, budget = scheduler.place_job(occurrence, profile, self.key)
        if host != 'local':
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'The managed process carrier needs one local resource host')
        binding = dict(provider='local', worker_mode='process', capacity_root=str(self.manager.provider.pool.root),
                       job_authority=authority)
        if profile.requests.get('accelerators'):
            grant = self.manager.provider.device_authority()
            if grant is None:
                raise DataflowError('RESOURCE_INFEASIBLE', 'Managed job requires a device authority')
            binding['device_authority'] = identity(grant)
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
        seconds = self.manager.provider.released_device_seconds(self.key, row['spec_hash'], 'gpu')
        if seconds is None:
            raise DataflowError('UNKNOWN_EFFECT', 'Original GPU reservation lacks a confirmed release receipt')
        return seconds

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
