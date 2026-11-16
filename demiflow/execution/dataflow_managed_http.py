"""Bounded passive CPU service declarations for native embedding workers.

The original service owner keeps lazy startup, port exclusion and process-group
cleanup. No owner, client, lock or independent device allocation crosses the wire.
"""
import inspect
import json
from pathlib import Path

from .contracts.dataflow_errors import DataflowError
from .recovery_contract import document

_SCHEMA = 'demiflow.managed_http_declaration.v1'
_MAX_BYTES = 65536
_PATHS = frozenset({'root', 'log_path', 'resource_directory'})
_FIELDS = frozenset({'command', 'base_url', 'root', 'gpus', 'env', 'ready_path',
    'expected_model', 'log_path', 'startup_timeout_s', 'shutdown_timeout_s',
    'readiness_timeout_s', 'poll_interval_s', 'resource_wait_timeout_s',
    'startup_log_interval_s', 'model_match', 'required_paths', 'log',
    'resource_directory', 'request_concurrency', 'log_max_bytes'})


def _invalid(reason):
    return DataflowError('INVALID_CONTRACT', reason)


def freeze_managed_http(service):
    from ..services.http import ManagedHTTPService, _NATIVE_MANAGED_HTTP_METHODS
    if type(service) is not ManagedHTTPService:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Embedding service requires a passive native ManagedHTTPService')
    if (inspect.getattr_static(ManagedHTTPService, '__init__'),
            inspect.getattr_static(ManagedHTTPService, 'bind')) != _NATIVE_MANAGED_HTTP_METHODS:
        raise _invalid('Managed HTTP service methods differ from the native declaration')
    values = vars(service)
    if set(values) != _FIELDS:
        raise _invalid('Managed HTTP service contains modified methods or unknown live state')
    if values['log'] is not None:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed embedding service log callbacks require a code-bound declaration')
    parameters = {}
    for name, value in values.items():
        if name in _PATHS:
            if type(value) is not type(Path()):
                raise _invalid('Managed HTTP paths must be passive native paths')
            value = str(value)
        elif name == 'required_paths':
            if type(value) is not tuple or len(value) > 256 or any(type(p) is not type(Path()) for p in value):
                raise _invalid('Managed HTTP required paths must be bounded native paths')
            value = [str(p) for p in value]
        parameters[name] = value
    # Validate structural/byte limits before allocating the normalized copy.
    encoded = document({'schema': _SCHEMA, 'parameters': parameters}, _MAX_BYTES)
    return json.loads(encoded)


def restore_managed_http(value):
    from ..services.http import ManagedHTTPService
    if (type(value) is not dict or set(value) != {'schema', 'parameters'}
            or value['schema'] != _SCHEMA or type(value['parameters']) is not dict
            or set(value['parameters']) != _FIELDS):
        raise _invalid('Invalid sealed managed HTTP declaration')
    document(value, _MAX_BYTES)
    service = ManagedHTTPService(**value['parameters'])
    if freeze_managed_http(service) != value:
        raise _invalid('Managed HTTP declaration was not canonically sealed')
    return service


def managed_http_digest(service):
    import hashlib
    return hashlib.sha256(document(freeze_managed_http(service), _MAX_BYTES)).hexdigest()


def restore_worker_managed_http(value):
    """Only admitted workers instantiate the owner with original unit custody."""
    from ..services.http import ManagedHTTPService, _HTTPOwner
    restore_managed_http(value)  # Revalidate the original canonical declaration.

    class WorkerOwner(_HTTPOwner):
        def _spawn_process(self, env):
            from .dataflow_managed_http_child import spawn_owned_service
            return spawn_owned_service(self.declaration, env=env, resource_fds=self._resource_fds)

        async def _release(self):
            from .dataflow_managed_http_child import close_owned_service
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
