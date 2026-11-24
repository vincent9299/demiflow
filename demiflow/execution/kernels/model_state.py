"""Model state implementation."""
from __future__ import annotations

import inspect
import json
from demiflow.execution.contracts.dataflow import ArtifactRef, CallableInstance, KernelSpec, Kernel, LoadedProgram, Record, RuntimeServices
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_codec import _json_config
from demiflow.data.result_cache import CachedMap, CachedSyncMap
from demiflow.operator_llm.journal import canonical


def _state_hooks(instance):
    snapshot = getattr(instance, 'snapshot_state', None)
    restore = getattr(instance, 'restore_state', None)
    if (snapshot is None) != (restore is None):
        raise DataflowError('INVALID_CONTRACT', 'Actor state hooks must be paired (snapshot_state/restore_state)')
    return snapshot, restore

def _business_state(instance):
    # Public business attributes only; private runtime resources (locks/pools/
    # clients/astart Tasks) are rebuilt locally and never sealed or erased.
    state = {}
    for name, value in vars(instance).items():
        if name.startswith('_') or callable(value):
            continue
        state[name] = _json_config(value)
    return state

def _read_state(ref: ArtifactRef, services: RuntimeServices):
    data = b''.join(services.store.read(ref, max_bytes=services.bounds.max_state_bytes))
    if len(data) > services.bounds.max_state_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'Actor state exceeds the state byte budget')
    return json.loads(data)

def restore_actor_state(instance: CallableInstance, *, ref: ArtifactRef | None, services: RuntimeServices) -> None:
    _snapshot, restore = _state_hooks(instance)
    if ref is None:
        return
    state = _read_state(ref, services)
    if restore is not None:
        restore(state)
        return
    for name, value in state.items():
        setattr(instance, name, value)

def snapshot_actor_state(instance: CallableInstance, *, services: RuntimeServices) -> ArtifactRef:
    snapshot, _restore = _state_hooks(instance)
    # Post-construction check of the real instance, not a class exemption.
    state = snapshot() if snapshot is not None else _business_state(instance)
    encoded = canonical(state).encode('utf-8')
    if len(encoded) > services.bounds.max_state_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'Actor state exceeds the state byte budget')
    return services.store.put(iter([encoded]), max_bytes=services.bounds.max_state_bytes,
                              media_type='application/json')

class _CachedMapKernel:
    """Kernel reusing the original cache namespace/result sharing.

    The cache directory/lock are coordination resources, not business state, so
    snapshot/restore carry nothing; the wrapped instance lifecycle is independent.
    """

    def __init__(self, spec: KernelSpec, services: RuntimeServices) -> None:
        self._spec = spec
        self._services = services
        self._instance = None
        self._cached = None

    def start(self) -> None:
        config = self._spec.configuration
        program: LoadedProgram = self._services.programs[config['node_id']]
        fn = program.value
        self._instance = fn
        cache = config['cache']
        kwargs = {'inputs': cache.get('inputs'), 'output': cache.get('output'),
                  'outputs': cache.get('outputs'), 'namespace': cache.get('namespace', 'default')}
        if inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(getattr(fn, '__call__', None)):
            self._cached = CachedMap(fn, cache['cache_dir'], cache['version'], **kwargs)
            self._async = True
        else:
            self._cached = CachedSyncMap(fn, cache['cache_dir'], cache['version'], **kwargs)
            self._async = False
        hook = getattr(fn, 'astart', None)
        if hook is not None:
            self._run_hook(hook)

    def _run_hook(self, hook):
        value = hook()
        if inspect.isawaitable(value):
            import asyncio
            asyncio.run(value)

    def push(self, port: str, record: Record):
        row = record.inline if record.inline is not None else json.loads(
            b''.join(self._services.store.read(record.payload_ref, max_bytes=self._spec.output_bound_bytes)))
        if self._async:
            import asyncio
            result = asyncio.run(self._cached(row))
        else:
            result = self._cached(row)
        # inline carries the exact result object; the size hint must not force a
        # JSON round-trip that would corrupt a bytes payload.
        size = len(result) if isinstance(result, bytes) else len(canonical(result).encode('utf-8'))
        from demiflow.execution.contracts.dataflow import clone_record
        yield clone_record(record, payload_ref=None, inline=result, encoded_bytes=size, schema_ref=None)

    def finish(self, port: str):
        return iter(())

    def snapshot(self):
        return None  # cache Path/lock are not business state

    def restore(self, ref) -> None:
        return None

    def close(self) -> None:
        hook = getattr(self._instance, 'aclose', None)
        if hook is not None:
            self._run_hook(hook)

def bind_cached_map(spec: KernelSpec, *, services: RuntimeServices) -> Kernel:
    if 'cache' not in spec.configuration:
        raise DataflowError('INVALID_CONTRACT', 'Cached map kernel requires a cache declaration')
    return _CachedMapKernel(spec, services)
