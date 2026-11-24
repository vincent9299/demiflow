"""Bind worker model instances to their authorized effect and result-cache services."""
from __future__ import annotations

from demiflow.execution.journal.dataflow_model_observations import validate_summaries, validate_routed_model_summaries

from demiflow.execution.journal.dataflow_usage import empty_usage


from typing import Mapping

from demiflow.execution.contracts.dataflow import Bounds, CallableInstance, EffectCommand, EffectReply, KernelSpec, NativeEffectTransport, RuntimeServices, WorkerJournalPort
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import identity


# Pure batch packing/concurrency knobs never change a per-record model result
# cache identity; model/normalization/task semantics always do.
_PACKING_KEYS = frozenset({'batch_size', 'max_batch', 'concurrency', 'queue_depth',
                           'flush_interval', 'packing'})


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


def embedding_cache_key(record_id: str, *, model_digest: str, payload_digest: str,
                        batch_semantics: Mapping[str, object]) -> str:
    """Per-record model RESULT cache identity, distinct from single-request idempotency.

    Pure batch packing/concurrency changes keep the key; model/normalization/task
    semantics changes never reuse a result.
    """
    semantic = {key: value for key, value in batch_semantics.items() if key not in _PACKING_KEYS}
    return identity(['embedding-result-cache', record_id, model_digest, payload_digest, semantic])


def bind_cached_model_services(fn, *, services: RuntimeServices, node_id: str) -> None:
    """Bind the cached callable to the effect journal for this worker occurrence."""
    from demiflow.operator_llm.native_actor import is_native_prompt_actor
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
            from demiflow.execution.operator_bindings.dataflow_prompt_service import bind_prompt_service
            bind_prompt_service(instance, spec, services=services, node_id=node_id)
        if 'routes' in spec.configuration:
            from demiflow.execution.operator_bindings.dataflow_routed_prompt import bind_routed_prompt
            bind_routed_prompt(instance, services=services, node_id=node_id, configuration=spec.configuration)
        elif spec.configuration.get('private_journal') is not None:
            from demiflow.execution.operator_bindings.dataflow_prompt_journal import bind_private_prompt_adapter
            bind_private_prompt_adapter(instance, services=services, node_id=node_id, configuration=spec.configuration)
        elif spec.configuration.get('response_format') == 'image':
            journal.bind_media_adapter(instance, services=services, node_id=node_id)
        else:
            journal.bind_prompt_adapter(instance, services=services, node_id=node_id)
    elif kind == 'embedding':
        from demiflow.execution.operator_bindings.dataflow_managed_http import bind_worker_service
        bind_worker_service(instance, spec, services=services, node_id=node_id)
        # Embedding owns its NativeEmbeddingRecovery and atomic budget; it has no
        # Prompt coordinator and never borrows bind_client.
        if spec.configuration.get('private_journal') is not None:
            from demiflow.execution.operator_bindings.dataflow_embedding_journal import bind_private_embedding_adapter
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


def worker_prompt_usage(kernel, *, final=False):
    from demiflow.operator_llm.runtime import PromptActor
    from demiflow.operator_llm.routed import RoutedPromptActor
    from demiflow.data.result_cache import CacheCall
    if getattr(kernel, '_closed', False) and not final:
        return getattr(kernel, '_prompt_usage', None)
    totals, observed = empty_usage(), False
    for instance in getattr(kernel, '_instances', ()):
        owner = kernel._state_owner(instance)
        if isinstance(owner, CacheCall):
            owner = owner.fn
        owners = owner.actors.values() if isinstance(owner, RoutedPromptActor) else (owner,)
        for child in owners:
            if not isinstance(child, PromptActor):
                continue
            observed = True
            for key, value in child._runtime.coordinator.usage().to_dict().items():
                totals[key] += value
    if observed:
        kernel._prompt_usage = totals
        return totals
    return None


def _snapshot_actor(actor):
    gate = actor._runtime.request_gate
    summary = actor.call_metrics.summary()
    if gate is not None:
        summary = {**summary, 'shared_admission': {
            'requests': gate.admitted, 'peak': gate.peak, 'wait_s': gate.wait_s,
            **({'adaptive': gate.snapshot()} if hasattr(gate, 'snapshot') else {})}}
    return summary

def worker_model_observations(kernel, *, final=False):
    kind = getattr(getattr(kernel, '_spec', None), 'kind', None)
    if kind not in {'embedding', 'prompt', 'agent'}:
        return None
    if getattr(kernel, '_closed', False) and not final:
        return getattr(kernel, '_model_observations', None)
    if kernel.pending_calls and not final:
        return getattr(kernel, '_model_observations', None)
    from demiflow.embeddings.runtime import EmbeddingActor
    from demiflow.operator_llm.runtime import PromptActor
    configuration = kernel._spec.configuration
    routed = kind == 'prompt' and 'routes' in configuration
    summaries = []
    for index, instance in enumerate(kernel._instances):
        if instance is None:
            continue
        actor = kernel._state_owner(instance)
        if routed:
            from demiflow.operator_llm.routed import RoutedPromptActor
            if not isinstance(actor, RoutedPromptActor) or set(actor.actors) != set(configuration['routes']):
                raise DataflowError('INVALID_CONTRACT', 'routed actor differs from sealed model declaration')
            for route, child in sorted(actor.actors.items()):
                if not isinstance(child, PromptActor):
                    raise DataflowError('INVALID_CONTRACT', 'route does not own a Prompt actor')
                summaries.append({'instance': index, 'route': route, 'summary': _snapshot_actor(child)})
        else:
            expected = EmbeddingActor if kind == 'embedding' else PromptActor
            if not isinstance(actor, expected):
                continue
            summaries.append(_snapshot_actor(actor))
    if not summaries:
        return None
    if routed:
        result = validate_routed_model_summaries(summaries, configuration=configuration,
                                  max_bytes=kernel._services.bounds.max_control_bytes)
    else:
        result = validate_summaries(summaries, maximum=configuration.get('concurrency', 1),
                                   max_bytes=kernel._services.bounds.max_control_bytes, kind=kind)
    kernel._model_observations = result
    return result
