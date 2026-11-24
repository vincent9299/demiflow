"""Actual model service binding inside an admitted worker."""

from demiflow.execution.contracts.dataflow_kinds import MANAGED_HTTP_DEVICES_FIELD

import os
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.services.http import restore_managed_http

def restore_worker_managed_http(value):
    """Only admitted workers instantiate the owner with original unit custody."""
    from demiflow.services.http import ManagedHTTPService, _HTTPOwner
    restore_managed_http(value)  # Revalidate the original canonical declaration.

    class WorkerOwner(_HTTPOwner):
        def _spawn_process(self, env):
            from demiflow.execution.hosts.managed_http_child import spawn_owned_service
            return spawn_owned_service(self.declaration, env=env, resource_fds=self._resource_fds)

        async def _release(self):
            from demiflow.execution.hosts.managed_http_child import close_owned_service
            await close_owned_service(self)

        async def aclose(self):
            if self._closed and self._process is not None:
                await self._release()  # A prior unknown close can be retried.
            else:
                await super().aclose()

    class WorkerService(ManagedHTTPService):
        def bind(self, model=None):
            super().bind(model)  # Original endpoint/model validation, no spawn.
            if self.gpus:
                # Original DevicePool is the only GPU allocator. This owner
                # keeps only port locks; the child receives its verified grant.
                cpu = ManagedHTTPService(**dict(value['parameters'], gpus=[]))
                cpu._worker_gpu_requested = True
                cpu._worker_device_binding = getattr(self, '_worker_device_binding', None)
                cpu._worker_device_check = getattr(self, '_worker_device_check', None)
                return WorkerOwner(cpu)
            return WorkerOwner(self)

    return WorkerService(**value['parameters'])


def bind_worker_service(actor, spec, *, services, node_id, service=None):
    from demiflow.execution.hosts.managed_devices import device_binding
    if not spec.configuration.get(MANAGED_HTTP_DEVICES_FIELD):
        return
    binding = device_binding(services.constants, node_id)
    from demiflow.resources.capacity import inherited_envelope

    def check():
        services.check_stop()
        services.journal.check_grant()
        envelope = inherited_envelope()
        if envelope is None or envelope['memory_bytes'] < binding['max_rss_bytes']:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU service has no original worker memory grant')
        visible = os.environ.get('CUDA_VISIBLE_DEVICES', '').split(',')
        if set(visible) != set(binding['device_ids']) or len(visible) != len(binding['device_ids']):
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU service UUIDs differ from the actual worker grant')
        return binding

    check()
    service = actor.service if service is None else service
    if tuple(service.gpus) != tuple(binding['indices']):
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed service GPU declaration differs from its original device binding')
    service._worker_device_binding = binding
    service._worker_device_check = check
