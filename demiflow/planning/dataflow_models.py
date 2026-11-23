"""Lower passive model declarations to resource-bounded kernel specifications."""
from __future__ import annotations


from dataclasses import replace

from demiflow.execution.contracts.dataflow import ArtifactRef, Bounds, InputPort, KernelSpec
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_codec import _json_config
from demiflow.execution.recovery_contract import identity
from demiflow.data.plan import AgentMapOp, EmbeddingMapOp, LogicalOp, OperatorLLMMapOp, ReadDocumentsOp, RegisterDocumentsBatchOp, RegisterDocumentsOp, SearchWebOp, VectorSearchOp
from demiflow.planning.dataflow_budget import _model_profile


def _prompt_model_json(model):
    return {'name': model.name, 'transport': model.transport, 'base_url_env': model.base_url_env,
            'api_key_env': model.api_key_env, 'base_url': model.base_url, 'api_version': model.api_version,
            'provider': _json_config(model.provider),
            'deployment': _json_config(model.deployment) if model.deployment is not None else None}

def _input_ports(names):
    return tuple(InputPort(name=name, requirement='incremental', ordering='none', schema_ref=None)
                 for name in names)

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
        from demiflow.operator_llm.flow_contract import private_prompt_declaration
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
            from demiflow.embeddings.flow_contract import embedding_facts
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

    return service_kernel(model, settings)


def service_kernel(model, settings):
    declaration = settings.get('service')
    if declaration is None:
        return model
    if model.kind != 'prompt':
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed HTTP service requires a native Prompt node')
    from demiflow.execution.contracts.dataflow_codec import _json_config
    import hashlib
    from demiflow.execution.recovery_contract import document
    # Execution programs retain the passive declaration. Public plan facts do
    # not duplicate service environment values or arbitrary command arguments.
    digest = hashlib.sha256(document(declaration, 65536)).hexdigest()
    configuration = dict(_json_config(model.configuration), managed_http_service_digest=digest)
    parameters = declaration['parameters']
    requests = dict(model.profile.requests)
    role = model.resource_role
    if parameters['gpus']:
        configuration['managed_http_devices'] = {
            'indices': parameters['gpus'], 'device_order': parameters['env'].get('CUDA_DEVICE_ORDER')}
        requests['accelerators'] = [{'type': 'gpu', 'count': len(parameters['gpus'])}]
        role = 'gpu'
    return replace(model, configuration=configuration, resource_role=role,
                   profile=replace(model.profile, requests=requests))


def compile_routed_prompt(operation, declaration, *, program_ref, bounds):

    from demiflow.planning.dataflow_budget import _model_profile
    from demiflow.execution.contracts.dataflow_codec import _json_config
    children, limits, route, isolate, max_requests = declaration.constructor_args
    if any(child.constructor_args[1].get('service') is not None for child in children.values()):
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Owned Prompt route services require independent service placement')
    models = {name: build_model_factory(child.constructor_args[0], program_ref=None, bounds=bounds)
              for name, child in children.items()}
    first = next(iter(models.values()))
    configuration = {'route': route, 'routes': {name: _json_config(model.configuration)
        for name, model in models.items()}, 'route_limits': limits,
        'isolate_route_failures': isolate, 'concurrency': operation.concurrency,
        'max_requests': max_requests,
        'catch': [c.__module__ + '.' + c.__qualname__ for c in operation.catch]}
    configuration['node_id'] = identity(['routed-prompt', configuration])
    from demiflow.execution.compiler_support.operator_policy import function_residency_kind
    state = 'nonrecoverable' if any('private_journal' in model.configuration for model in models.values()) else 'none'
    for child in children.values():
        predicate = child.constructor_args[1].get('when')
        if predicate is not None and function_residency_kind(predicate) != 'none':
            # The native journal recovers HTTP effects, not arbitrary predicate
            # state. Retain the runtime's existing attempt-only restriction.
            state = 'nonrecoverable'
    return replace(first, implementation_ref=program_ref, configuration=configuration, state=state,
        profile=_model_profile(first.resource_role, bounds, operation.concurrency, state,
                               operation=operation))
