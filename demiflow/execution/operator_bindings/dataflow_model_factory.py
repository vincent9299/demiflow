"""Passive native declarations; live clients/coordinators are worker-owned."""

import re
from demiflow.execution.contracts.dataflow_errors import DataflowError
from dataclasses import replace

from demiflow.data.plan import CallableSpec, OperatorLLMMapOp, EmbeddingMapOp


class PromptFactory:
    def __new__(cls, operation, settings):
        from demiflow.operator_llm.runtime import PromptActor, InProcessOperatorLLMCoordinator
        service = None
        if settings.get('service') is not None:
            from demiflow.execution.operator_bindings.dataflow_managed_http import restore_worker_managed_http
            service = restore_worker_managed_http(settings['service'])
        actor = PromptActor(operation, operation.config,
                            InProcessOperatorLLMCoordinator(operation.max_requests), operation.options, service=service)
        if service is not None:
            import hashlib
            from demiflow.execution.recovery_contract import document
            actor._sealed_service_declaration = hashlib.sha256(document(settings['service'], 65536)).hexdigest()
        for name in ('when', 'call_output', 'error_output', 'environment'):
            setattr(actor, name, settings.get(name))
        actor._runtime.concurrency = settings.get('concurrency', 1)
        actor._runtime.token_budget = settings.get('token_budget')
        policy = settings.get('request_policy')
        if policy is not None:
            actor._request_policy = tuple(sorted(policy.items()))
        return actor


class EmbeddingFactory:
    def __new__(cls, declaration, policy):
        from demiflow.embeddings.runtime import EmbeddingActor
        from demiflow.inference import resolve_request_gate as _resolve_request_gate
        if declaration.get('service') is not None:
            from demiflow.execution.operator_bindings.dataflow_managed_http import restore_worker_managed_http
            declaration = dict(declaration, service=restore_worker_managed_http(declaration['service']))
        actor = EmbeddingActor(**declaration, request_gate=_resolve_request_gate(
            policy=policy, gate=None, concurrency=declaration['concurrency']))
        actor._request_policy = policy
        return actor


class RoutedPromptFactory:
    def __new__(cls, declarations, limits, route, isolate_failures, max_requests):
        from demiflow.operator_llm.routed import RoutedPromptActor
        return RoutedPromptActor({name: value.instantiate() for name, value in declarations.items()},
            limits, route, isolate_failures=isolate_failures, max_requests=max_requests)


class CacheFactory:
    def __new__(cls, declaration, directory, version, options, synchronous):
        from demiflow.data.result_cache import CachedMap, CachedSyncMap
        from demiflow.execution.compiler_support.operator_policy import function_residency_kind
        from demiflow.execution.kernels.task_scope import clone_resident_function
        fn = declaration.instantiate()
        actor = (CachedSyncMap if synchronous else CachedMap)(fn, directory, version, **options)
        if actor.validate_runtime_state() is not None:
            return actor  # Retain borrowed/aliased identity during this attempt.
        # A loaded program is a sealed declaration, never the mutable owner of
        # admitted closure/default/partial slots for a running cache instance.
        cloned = {}
        for name in ('fn', 'when', 'cache_when'):
            value = getattr(actor, name)
            if value is not None and function_residency_kind(value) == 'adapter':
                if id(value) not in cloned:
                    cloned[id(value)] = clone_resident_function(value, max_bytes=options['max_input_bytes'])
                setattr(actor, name, cloned[id(value)])
        return actor


def native_model_declaration(operation):
    """Return a factory only after validating the original native declaration.

    No started actor, lock, client or mutable admission gate crosses the wire.
    Native fact guards remain the authority for rejecting modified/live actors.
    """
    if isinstance(operation, OperatorLLMMapOp):
        from demiflow.operator_llm.runtime import validate_prompt_binding
        validate_prompt_binding(operation, operation.config)
        # The execution occurrence supplies the per-node request namespace.
        return CallableSpec.create(PromptFactory, fn_constructor_args=(replace(operation, node_id=None), {}))
    target = getattr(getattr(operation, 'callable', None), 'target', None)
    actor = getattr(target, '__self__', target)
    from demiflow.data.plan import SaveLanceOp
    if type(operation) is SaveLanceOp:
        from demiflow.execution.dataflow_stream_lance import StreamLanceFactory, declaration
        return CallableSpec.create(StreamLanceFactory, fn_constructor_args=(declaration(operation, actor), actor.when))
    # None and ordinary functions cannot be one of the exact native actor
    # classes below. Typed model operations retain their original fact guards,
    # including rejection of a forged function in place of their native actor.
    import types
    if not isinstance(operation, EmbeddingMapOp) and (
            actor is None or type(actor) in (types.FunctionType, types.BuiltinFunctionType)):
        return None
    from demiflow.operators.lookup import LanceLookup
    from demiflow.lance.search import VectorSearch
    if type(actor) is LanceLookup:
        from demiflow.lance.flow_contract import lookup_declaration
        return CallableSpec.create(LanceLookup, fn_constructor_kwargs=lookup_declaration(actor, target))
    if type(actor) is VectorSearch:
        from demiflow.data.plan import VectorSearchOp
        from demiflow.execution.plan_facts import operation_facts
        from demiflow.execution.contracts.dataflow_errors import DataflowError
        if type(operation) is not VectorSearchOp:
            raise DataflowError('INVALID_CONTRACT', 'Vector search requires its typed declaration')
        operation_facts(operation)  # Exact native actor, bound method and spec agree.
        from demiflow.lance.flow_contract import vector_search_declaration
        return CallableSpec.create(VectorSearch, fn_constructor_kwargs=vector_search_declaration(actor))
    from demiflow.collect.search_contract import is_native_search_actor, search_facts
    if is_native_search_actor(actor):
        from demiflow.execution.dataflow_search import NativeSearchCall
        from demiflow.execution.recovery_contract import identity
        search_facts(actor)
        if actor.started:
            raise ValueError('Native search requires an unstarted actor declaration')
        return CallableSpec.create(NativeSearchCall, fn_constructor_args=(identity(actor.recovery_identity()),))
    from demiflow.execution.native_queue.queue_producer import QueueProducer, producer_facts
    if type(actor) is QueueProducer:
        from demiflow.execution.dataflow_producer import ProducerCall
        producer_facts(actor)
        return CallableSpec.create(ProducerCall)
    from demiflow.execution.raw_fetch import RawFetch, raw_fetch_facts
    from demiflow.execution.queue_ack import QueueAcknowledge, ack_facts
    if type(actor) in (RawFetch, QueueAcknowledge):
        from demiflow.execution.native_queue.dataflow_acquisition_call import AcquisitionCall
        (raw_fetch_facts if type(actor) is RawFetch else ack_facts)(actor)
        return CallableSpec.create(AcquisitionCall, fn_constructor_args=(
            'acquire' if type(actor) is RawFetch else 'finalize', getattr(actor, 'keep_row', True)))
    from demiflow.data.result_cache import CachedMap, CachedSyncMap
    if type(actor) in (CachedMap, CachedSyncMap):
        from types import SimpleNamespace
        actor.validate_runtime_state()
        wrapped = native_model_declaration(SimpleNamespace(callable=CallableSpec.create(actor.fn)))
        wrapped = wrapped if wrapped is not None else CallableSpec.create(actor.fn)
        from demiflow.data.result_cache import cache_constructor_options
        options = cache_constructor_options(actor)
        return CallableSpec.create(CacheFactory, fn_constructor_args=(wrapped, str(actor.directory),
            actor.version, options, type(actor) is CachedSyncMap))
    from demiflow.operator_llm.routed import RoutedPromptActor
    if type(actor) is RoutedPromptActor:
        from demiflow.execution.operator_bindings.dataflow_routed_prompt import routed_declaration
        return routed_declaration(actor, target)
    from demiflow.operator_llm.native_actor import is_native_prompt_actor
    if is_native_prompt_actor(actor):
        from demiflow.operator_llm.flow_contract import prompt_constructor_settings
        settings = prompt_constructor_settings(actor, operation)
        return CallableSpec.create(PromptFactory, fn_constructor_args=(replace(actor._operation, node_id=None), settings))
    if isinstance(operation, EmbeddingMapOp):
        from demiflow.embeddings.flow_contract import embedding_constructor_parameters
        declaration, policy = embedding_constructor_parameters(operation, actor)
        return CallableSpec.create(EmbeddingFactory, fn_constructor_args=(declaration, policy))
    return None


def native_environment_names(spec, *, depth=0):
    from demiflow.data.plan import CallableSpec, OperatorLLMMapOp
    from demiflow.operator_llm.model import PromptPack, PromptDefinition, PromptModel
    from demiflow.embeddings.model import EmbeddingModel

    if type(spec) is not CallableSpec:
        return ()
    if depth > 4:
        raise DataflowError('BUDGET_EXCEEDED', 'Native environment declaration exceeds depth bound')
    args = spec.constructor_args
    if spec.target is CacheFactory:
        return native_environment_names(args[0], depth=depth + 1)
    if spec.target is RoutedPromptFactory:
        # Read only the exact passive declarations authenticated by the native
        # program pin. Route actors/clients are never instantiated here, and
        # values remain ephemeral allocation data rather than sealed identity.
        children = args[0] if len(args) == 5 else None
        if (type(children) is not dict or not 1 <= len(children) <= 32
                or any(type(name) is not str or not name
                       or type(child) is not CallableSpec or child.target is not PromptFactory
                       for name, child in children.items())):
            raise DataflowError('INVALID_CONTRACT', 'Native routed prompt environment requires original passive child declarations')
        names = tuple(sorted({name for child in children.values()
            for name in native_environment_names(child, depth=depth + 1)}))
    elif spec.target is PromptFactory:
        operation = args[0]
        if type(operation) is not OperatorLLMMapOp or type(operation.config) is not PromptPack:
            raise DataflowError('INVALID_CONTRACT', 'Native prompt environment requires the original passive declaration')
        pack = operation.config
        if (any(type(prompt) is not PromptDefinition or type(prompt.model) is not PromptModel
                for prompt in pack.prompts)):
            raise DataflowError('INVALID_CONTRACT', 'Native prompt environment requires platform model declarations')
        # Offline inputs use the driver's sealed store and never construct an
        # HTTP provider. Match transport selection rather than treating a zero
        # request budget (which can also describe online replay) as offline.
        options = operation.options or {}
        names = () if any(key in options for key in ('offline_dir', 'offline_store')) else pack.required_environment_names
    elif spec.target is EmbeddingFactory:
        declaration = args[0]
        model = declaration.get('model') if type(declaration) is dict else None
        if type(model) is not EmbeddingModel:
            raise DataflowError('INVALID_CONTRACT', 'Native embedding environment requires the original model declaration')
        names = (model.api_key_env,) if model.api_key_env else ()
    else:
        return ()
    if len(names) > 128 or any(type(name) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,255}', name) for name in names):
        raise DataflowError('INVALID_CONTRACT', 'Native environment names must be bounded identifiers')
    return tuple(sorted(set(names)))
