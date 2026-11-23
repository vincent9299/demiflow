"""Worker-side model factory, state and result-cache adaptation (lane M).

build_model_factory freezes only declarative code/config (never live locks or
clients); bind_model_services wires the native effect/journal budget and lifecycle
after the worker constructs the instance. State uses the original adapter/auto_json
hooks; the result cache reuses the original namespace and sharing substrate. Model
result-cache identity is kept separate from the native single-request effect key.
"""
from __future__ import annotations

import inspect
import json
import math
from dataclasses import fields, is_dataclass
from typing import Mapping

from demiflow.execution.contracts.dataflow import (
    ArtifactRef,
    Bounds,
    CallableInstance,
    EffectCommand,
    EffectReply,
    InputPort,
    KernelSpec,
    Kernel,
    LoadedProgram,
    NativeEffectTransport,
    Record,
    RuntimeServices,
    WorkerJournalPort,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import identity
from demiflow.operator_llm.journal import canonical
from demiflow.resources.contracts import ResourceProfile
from demiflow.data.plan import (
    AgentMapOp,
    EmbeddingMapOp,
    LogicalOp,
    OperatorLLMMapOp,
    ReadDocumentsOp,
    RegisterDocumentsBatchOp,
    RegisterDocumentsOp,
    SearchWebOp,
    VectorSearchOp,
)
from demiflow.data.result_cache import CachedMap, CachedSyncMap

# Pure batch packing/concurrency knobs never change a per-record model result
# cache identity; model/normalization/task semantics always do.
_PACKING_KEYS = frozenset({'batch_size', 'max_batch', 'concurrency', 'queue_depth',
                           'flush_interval', 'packing'})


def _json_config(value):
    """Project a declaration to strict JSON, rejecting live code/locks/clients."""
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise DataflowError('INVALID_CONTRACT', 'Model configuration has a nonfinite number')
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_config(item) for key, item in value.items()}
    if type(value) in (list, tuple):
        return [_json_config(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _json_config(getattr(value, field.name)) for field in fields(value)}
    raise DataflowError('INVALID_CONTRACT', 'Model configuration must be declarative code/config, not a live object')


def _prompt_model_json(model):
    return {'name': model.name, 'transport': model.transport, 'base_url_env': model.base_url_env,
            'api_key_env': model.api_key_env, 'base_url': model.base_url, 'api_version': model.api_version,
            'provider': _json_config(model.provider),
            'deployment': _json_config(model.deployment) if model.deployment is not None else None}


def _input_ports(names):
    return tuple(InputPort(name=name, requirement='incremental', ordering='none', schema_ref=None)
                 for name in names)


def _model_profile(resource_role, bounds, concurrency, state, *, operation=None,
                   codec_workspace=True):
    # Native actors carry the same typed records as ordinary kernels. Their
    # codec copies must fit the original stage grant too; input+output alone
    # is not that peak. Keep one planner and every explicit worker constraint.
    from ..planning.dataflow_budget import default_profile
    profile = default_profile(operation, bounds=bounds, state=state,
        codec_workspace=codec_workspace, concurrency=concurrency)
    requests = dict(profile.requests)
    resources = getattr(getattr(operation, 'native_options', None), 'worker_resources', None) or {}
    if 'cpu' not in resources:
        requests['cpu_millis'] = 100
    if resource_role == 'gpu' and 'accelerators' not in requests:
        requests['accelerators'] = [{'type': 'gpu', 'count': 1}]
    return ResourceProfile(requests=requests, limits=profile.limits, requirements=profile.requirements)


def build_model_factory(operation: LogicalOp, *, program_ref: ArtifactRef | None, bounds: Bounds) -> KernelSpec:
    """Freeze the full declarative config/construction declaration of a model node.

    No model, client, lock or pool is constructed here (driver construction count
    is zero); the program artifact carries code, the configuration carries only
    strict-JSON declaration.
    """
    if isinstance(operation, OperatorLLMMapOp):
        prompt = operation.config.prompt_definitions[operation.prompt_name]
        gpu = prompt.model.transport == 'diffusers'
        role = 'gpu' if gpu else 'remote_api'
        config = {'prompt_name': operation.prompt_name, 'inputs': _json_config(operation.inputs),
                  'output': operation.output, 'outputs': _json_config(operation.outputs),
                  'options': _json_config(operation.options or {}), 'max_requests': operation.max_requests,
                  'response_format': prompt.response_format, 'model': _prompt_model_json(prompt.model),
                  'content_hash': operation.config.content_hash}
        kind, state, effect = 'prompt', 'none', 'journaled'
        from .dataflow_prompt_journal import private_prompt_declaration
        private = private_prompt_declaration(operation.options)
        if private is not None:
            if prompt.response_format == 'image' or prompt.model.deployment is not None:
                raise DataflowError('UNSUPPORTED_CAPABILITY', 'Private Prompt cache requires external text HTTP delivery')
            config['private_journal'] = private
            config['collect_journal_totals'] = (operation.options or {}).get('collect_journal_totals', True)
            if type(config['collect_journal_totals']) is not bool:
                raise DataflowError('INVALID_CONTRACT', 'collect_journal_totals must be boolean')
            state = 'nonrecoverable'
        ports = _input_ports(('main',))
    elif isinstance(operation, EmbeddingMapOp):
        config = {'inputs': _json_config(operation.inputs), 'output': operation.output,
                  'max_batch': operation.max_batch, 'flush_interval': operation.flush_interval,
                  'concurrency': operation.concurrency, 'model_contract': _json_config(operation.model_contract)}
        kind, state, effect, role = 'embedding', 'none', 'journaled', 'remote_api'
        actor = getattr(operation.callable.target, '__self__', None)
        service = getattr(actor, 'service', None)
        if service is not None and service.gpus:
            config['managed_http_devices'] = {'indices': list(service.gpus),
                'device_order': service.env.get('CUDA_DEVICE_ORDER')}
            role = 'gpu'
        private = getattr(actor, 'options', {}).get('sqlite_journal')
        if private is not None:
            from ..embeddings.flow_contract import embedding_facts
            declared = embedding_facts(operation)['arguments']
            # Only explicit legacy cache declarations add fields. Existing
            # native configuration/identity bytes remain unchanged.
            config['private_journal'] = _json_config(declared['options']['sqlite_journal'])
            config['max_requests'] = declared['max_requests']
            config['collect_journal_totals'] = declared['options'].get('collect_journal_totals', True)
            state = 'nonrecoverable'
        ports = _input_ports(('main',))
    elif isinstance(operation, AgentMapOp):
        environment = operation.environment
        runtime_kind = getattr(environment, 'runtime', 'demiflow') if environment is not None else 'demiflow'
        config = {'concurrency': operation.concurrency, 'queue_depth': operation.queue_depth,
                  'label': operation.label, 'runtime': runtime_kind,
                  'resources': getattr(environment, 'resources', None)}
        kind, state = 'agent', 'nonrecoverable' if runtime_kind == 'codex' else 'none'
        effect = 'unknown_replay_forbidden' if runtime_kind == 'codex' else 'journaled'
        role = 'remote_api'
        ports = _input_ports(('main',))
    elif isinstance(operation, (SearchWebOp, VectorSearchOp)):
        config = {'output': operation.output, 'concurrency': operation.concurrency}
        if isinstance(operation, SearchWebOp):
            config.update({'requests': operation.requests, 'checkpoint': operation.checkpoint,
                           'checkpoint_operator': operation.checkpoint_operator})
        else:
            config.update({'query': operation.query, 'search_spec': _json_config(operation.search_spec)})
        kind, state, effect, role = 'fetch', 'none', 'journaled', 'remote_api'
        ports = _input_ports(('main',))
    elif isinstance(operation, (ReadDocumentsOp, RegisterDocumentsOp, RegisterDocumentsBatchOp)):
        config = {'request': operation.request, 'output': operation.output}
        kind, state, effect, role = 'document', 'none', 'journaled', 'io'
        ports = _input_ports(('main',))
    else:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Not a native model operation: ' + type(operation).__name__)

    node_id = getattr(operation, 'node_id', None) or identity(['model-node', kind, config])
    config['node_id'] = node_id
    profile = _model_profile(role, bounds, config.get('concurrency', 1), state, operation=operation)
    if kind == 'embedding' and config.get('managed_http_devices'):
        from dataclasses import replace
        profile = replace(profile, requests={**profile.requests, 'accelerators': [
            {'type': 'gpu', 'count': len(config['managed_http_devices']['indices'])}]})
    return KernelSpec(kind=kind, implementation_ref=program_ref, configuration=config, inputs=ports,
                      state=state, effect=effect, resource_role=role, profile=profile,
                      output_bound_bytes=bounds.max_row_bytes, mergeable=False)


def lower_prompt_declaration(operation: LogicalOp, declaration, *, program_ref: ArtifactRef | None,
                             bounds: Bounds) -> KernelSpec:
    """Lower a validated prompt-factory declaration (map_prompt_async, agentmap_async, typed prompt).

    The declaration carries the native operation and the actor settings. The model
    kernel is the one build_model_factory freezes; concurrency, the caught
    exception types and an owned service are settings of the declaration.
    Registered in execution.dataflow_native.DECLARATION_LOWERINGS; the plan
    compiler resolves it by symbol and never names this module.
    """
    from dataclasses import replace
    native_op, settings = declaration.constructor_args
    model = build_model_factory(operation if isinstance(operation, AgentMapOp) else native_op,
                                program_ref=program_ref, bounds=bounds)
    # A KernelSpec configuration is frozen strict Json, for which _json_config is the plain copy.
    configuration = _json_config(model.configuration)
    configuration.update(concurrency=settings.get('concurrency', 1),
                         catch=[c.__module__ + '.' + c.__qualname__ for c in getattr(operation, 'catch', ())])
    model = replace(model, configuration=configuration,
        profile=_model_profile(model.resource_role, bounds, configuration["concurrency"],
                               model.state, operation=operation))
    from .dataflow_prompt_service import service_kernel
    return service_kernel(model, settings)


class _EffectTransport:
    """Typed driver RPC adapter over the native effect ledger (no second ledger)."""

    def __init__(self, port: WorkerJournalPort, actor_id: str, bounds: Bounds) -> None:
        self._port = port
        self._actor_id = actor_id
        self._bounds = bounds

    def _check(self, command: EffectCommand):
        if command.actor_id != self._actor_id:
            raise DataflowError('INVALID_CONTRACT', 'Effect command actor mismatch')
        if not command.request_digest:
            raise DataflowError('INVALID_CONTRACT', 'Effect command requires the original request digest')

    def _send(self, command: EffectCommand) -> EffectReply:
        self._check(command)
        reply = self._port.effect(command)
        if reply.operation_id != command.operation_id:
            raise DataflowError('INTERNAL_PROTOCOL', 'Effect reply operation_id mismatch')
        return reply

    def lookup(self, command: EffectCommand) -> EffectReply:
        # A confirmed-but-lost response is reused by effect_id; never re-sent.
        return self._send(command)

    def submit(self, command: EffectCommand) -> EffectReply:
        # The driver enforces the state machine; unknown effects refuse replay.
        return self._send(command)


def make_effect_transport(port: WorkerJournalPort, *, actor_id: str, bounds: Bounds) -> NativeEffectTransport:
    return _EffectTransport(port, actor_id, bounds)


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


def embedding_cache_key(record_id: str, *, model_digest: str, payload_digest: str,
                        batch_semantics: Mapping[str, object]) -> str:
    """Per-record model RESULT cache identity, distinct from single-request idempotency.

    Pure batch packing/concurrency changes keep the key; model/normalization/task
    semantics changes never reuse a result.
    """
    semantic = {key: value for key, value in batch_semantics.items() if key not in _PACKING_KEYS}
    return identity(['embedding-result-cache', record_id, model_digest, payload_digest, semantic])


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


def bind_cached_model_services(fn, *, services: RuntimeServices, node_id: str) -> None:
    """Bind the cached callable to the effect journal for this worker occurrence."""
    from ..operator_llm.native_actor import is_native_prompt_actor
    if is_native_prompt_actor(fn):
        from demiflow.execution.operator_bindings.dataflow_model_journal import bind_agent_adapter, bind_prompt_adapter
        if fn.environment is None:
            bind_prompt_adapter(fn, services=services, node_id=node_id)
        else:
            bind_agent_adapter(fn, services=services, node_id=node_id)


def bind_model_services(instance: CallableInstance, spec: KernelSpec, *, services: RuntimeServices) -> CallableInstance:
    from demiflow.execution.operator_bindings import dataflow_model_journal as journal
    node_id = spec.configuration.get('occurrence_id', spec.configuration.get('node_id'))
    kind = spec.kind
    if kind == 'prompt':
        if 'routes' not in spec.configuration:
            from .dataflow_prompt_service import bind_prompt_service
            bind_prompt_service(instance, spec, services=services, node_id=node_id)
        if 'routes' in spec.configuration:
            from .dataflow_routed_prompt import bind_routed_prompt
            bind_routed_prompt(instance, services=services, node_id=node_id, configuration=spec.configuration)
        elif spec.configuration.get('private_journal') is not None:
            from .dataflow_prompt_journal import bind_private_prompt_adapter
            bind_private_prompt_adapter(instance, services=services, node_id=node_id, configuration=spec.configuration)
        elif spec.configuration.get('response_format') == 'image':
            journal.bind_media_adapter(instance, services=services, node_id=node_id)
        else:
            journal.bind_prompt_adapter(instance, services=services, node_id=node_id)
    elif kind == 'embedding':
        from .dataflow_managed_devices import bind_worker_service
        bind_worker_service(instance, spec, services=services, node_id=node_id)
        # Embedding owns its NativeEmbeddingRecovery and atomic budget; it has no
        # Prompt coordinator and never borrows bind_client.
        if spec.configuration.get('private_journal') is not None:
            from .dataflow_embedding_journal import bind_private_embedding_adapter
            bind_private_embedding_adapter(instance, services=services, node_id=node_id,
                                            configuration=spec.configuration)
        else:
            journal.bind_embedding_adapter(instance, services=services, node_id=node_id)
    elif kind == 'agent':
        journal.bind_agent_adapter(instance, services=services, node_id=node_id)
    elif kind in ('fetch', 'document'):
        instance._effect_transport = make_effect_transport(services.journal, actor_id=node_id, bounds=services.bounds)
    else:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Unknown model kernel kind: ' + str(kind))
    return instance
