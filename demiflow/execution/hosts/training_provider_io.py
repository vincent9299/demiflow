"""Deadline-aware transport calls; only the driver may mutate durable fit state."""

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import identity

from importlib import import_module
from concurrent.futures import Future
import inspect
import threading
import time


# No task queue. An uncooperative vendor transport retains a slot until actual
# exit. It can neither strand interpreter shutdown nor create unbounded retries.
_TRANSPORT_LIMIT = threading.BoundedSemaphore(16)


def check_deadlines(provider, methods):
    for name in methods:
        method = getattr(provider, name, None)
        if not callable(method):
            raise ValueError("Missing provider operation " + name)
        try:
            inspect.signature(method).bind_partial(deadline=0.0)
        except (ValueError, TypeError) as exc:
            raise ValueError("Provider operation must accept keyword-only deadline: " + name) from exc


def _invoke(completion, method, args, deadline):
    try:
        if time.monotonic() >= deadline:
            raise TimeoutError("Provider operation expired before transport")
        completion.set_result(method(*args, deadline=deadline))
    except BaseException as exc:
        completion.set_exception(exc)
    finally:
        _TRANSPORT_LIMIT.release()


class ProviderCalls:
    def __init__(self, provider, timeout_s):
        self.provider = provider
        self.timeout_s = timeout_s
        self.outstanding = None

    def call(self, method, args, run_deadline, *, before_start=None):
        deadline = min(time.monotonic() + self.timeout_s, run_deadline)
        while self.outstanding is not None and not self.outstanding.done():
            if time.monotonic() >= deadline:
                raise TimeoutError("Original provider call has not returned")
            yield
        while not _TRANSPORT_LIMIT.acquire(blocking=False):
            if time.monotonic() >= deadline:
                raise TimeoutError("Training provider transport capacity exhausted")
            yield
        completion = Future()
        self.outstanding = completion
        try:
            if before_start is not None:
                before_start()  # Driver thread, after capacity and immediately before transport.
            threading.Thread(name='demiflow-fit-provider', daemon=True, target=_invoke,
                             args=(completion, getattr(self.provider,method), args, deadline)).start()
        except BaseException:
            _TRANSPORT_LIMIT.release()
            self.outstanding = None
            raise
        while not completion.done():
            if time.monotonic() >= deadline:
                raise TimeoutError("Training provider operation exceeded its deadline")
            yield
        return completion.result()


def instantiate_provider(factory, binding):
    module, separator, name = factory.partition(':')
    if not separator or not all(part.isidentifier() for part in module.split('.')) or not name.isidentifier():
        raise ValueError("Invalid deployment provider factory")
    return getattr(import_module(module), name)(binding)


def verify_job_authority(manager, scope, run, key):
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


def job_provider_binding(manager, profile, authority):
    binding = dict(provider='local', worker_mode='process', capacity_root=str(manager.provider.pool.root),
                   job_authority=authority)
    if profile.requests.get('accelerators'):
        grant = manager.provider.device_authority()
        if grant is None:
            raise DataflowError('RESOURCE_INFEASIBLE', 'Managed job requires a device authority')
        binding['device_authority'] = identity(grant)
    return binding


def released_gpu_seconds(provider, key, spec_hash):
    seconds = provider.released_device_seconds(key, spec_hash, 'gpu')
    if seconds is None:
        raise DataflowError('UNKNOWN_EFFECT', 'Original GPU reservation lacks a confirmed release receipt')
    return seconds


def observe_job_phase(provider, scope_id, key, status):
    observations = provider.inspect(scope_id, None)['allocations']
    if observations:
        item, = observations
        if item['allocation_id'] != key or item['observation'] != 'FRESH':
            return 'unknown'
        return item['phase'].lower()
    return status['phase'].lower() if status else 'unknown'
