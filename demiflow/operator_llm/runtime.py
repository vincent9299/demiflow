"""Operator LLM call coordination and backend-neutral row lowering."""
from __future__ import annotations

import json
import logging
import threading
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any, Mapping

from demiflow._compat.observability import log_event

from .client import create_operator_llm_client, create_async_operator_llm_client, _journal_io
from .errors import PromptBudgetExceededError, PromptReplayMissError, PromptResponseContractError, PromptResponseParseError
from .model import OperatorLLMRequest, OperatorLLMResponse, OperatorLLMUsage, PromptPack
from .parser import resolve_prompt
from .template import render_template
from .http_retry import HTTPRetryCall, retry_call
from demiflow.schema import SchemaValidationError, validate_instance

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Lookup:
    request: OperatorLLMRequest


def _prepare_lookup(client, request):
    """Blocking cache I/O and exact-payload hashing stay outside the event loop."""
    if hasattr(client, 'prepare'):
        request = client.prepare(request)
    return request, client.lookup(request)


class InProcessOperatorLLMCoordinator:
    @property
    def max_requests(self):
        return self._max_requests

    def __init__(self, max_requests: int | None = None, on_change=None) -> None:
        self._max_requests = max_requests
        self._on_change = on_change
        self._lock = threading.Lock()
        self._usage = OperatorLLMUsage()
        self._reservations: set[str] = set()
        self._execution_binding = None

    def bind_execution(self, context, node_id):
        from demiflow.execution.recovery_contract import identity
        binding = (context, 'requests-' + identity(node_id))
        if self._execution_binding is not None and self._execution_binding != binding:
            raise RuntimeError('A request coordinator cannot be shared across execution nodes')
        self._execution_binding = binding

    def attempted(self) -> None:
        with self._lock:
            self._usage = replace(self._usage, calls_attempted=self._usage.calls_attempted + 1)
        self._publish()

    def release_execution(self):
        with self._lock:
            if self._reservations:
                raise RuntimeError('Cannot release an execution with in-flight model work')
            self._execution_binding=None
            self._artifact_request_budget=None
            self._native_group_journal=False

    def reserve(self) -> str:
        with self._lock:
            if self._max_requests is not None and self._usage.requests_reserved >= self._max_requests:
                raise PromptBudgetExceededError("Operator LLM request budget exhausted")
            value = "operator-llm-" + uuid.uuid4().hex
            if self._execution_binding is not None:
                context, name = self._execution_binding
                artifact_budget=getattr(self,'_artifact_request_budget',None)
                charges = {'requests': (1, context.limits['max_requests']),
                           name: (1, self._max_requests or context.limits['max_requests'])}
                if artifact_budget is not None:
                    charges[artifact_budget] = (1, context.limits['max_requests'])
                context.check_stop()
                if getattr(self, '_native_group_journal', False):
                    from .stream_recovery import CALL_RESERVATION
                    CALL_RESERVATION.set((value, charges))
                else:
                    context.journal.reserve_budgets(context.fence, value, charges)
            self._reservations.add(value)
            self._usage = replace(self._usage, requests_reserved=self._usage.requests_reserved + 1)
        self._publish()
        return value

    def started(self, value: str) -> None:
        self._update(value, "requests_started")

    def completed(self, value: str, response: OperatorLLMResponse) -> None:
        self._finish(value, "requests_completed", response)

    def failed(self, value: str, response: OperatorLLMResponse | None = None) -> None:
        self._finish(value, "requests_failed", response)

    def usage(self) -> OperatorLLMUsage:
        with self._lock:
            return self._usage

    def _update(self, value: str, field: str) -> None:
        with self._lock:
            self._require(value)
            self._usage = replace(self._usage, **{field: getattr(self._usage, field) + 1})
        self._publish()

    def _finish(self, value: str, field: str, response: OperatorLLMResponse | None) -> None:
        with self._lock:
            self._require(value)
            self._reservations.remove(value)
            usage = response.usage if response else None
            self._usage = replace(
                self._usage,
                **{
                    field: getattr(self._usage, field) + 1,
                    "input_tokens": self._usage.input_tokens + (usage.input_tokens if usage else 0),
                    "output_tokens": self._usage.output_tokens + (usage.output_tokens if usage else 0),
                },
            )
        self._publish()

    def _require(self, value: str) -> None:
        if value not in self._reservations:
            raise RuntimeError("unknown Operator LLM reservation")

    def _publish(self) -> None:
        if self._on_change:
            self._on_change(self._usage)


class OperatorLLMRuntime:
    def __init__(self, config: PromptPack, coordinator, options=None) -> None:
        self.config = config
        self.coordinator = coordinator
        self.options = options
        self._local = threading.local()

    def call(self, prompt_name: str, values: Mapping[str, Any]) -> dict[str, Any]:
        if resolve_prompt(self.config, prompt_name).response_format == 'image':
            raise ValueError('Image output requires Dataset.map_prompt_async')
        if resolve_prompt(self.config, prompt_name).model.deployment is not None:
            raise ValueError('Managed model deployment requires map_prompt_async or the async agent runtime')
        if self.options is not None:
            # The synchronous worker uses the same option-aware transport,
            # including offline responses and journals, and closes it per call.
            import asyncio
            async def execute():
                runtime = AsyncOperatorLLMRuntime(self.config, self.coordinator, self.options)
                try:
                    return await runtime.call(prompt_name, values)
                finally:
                    await runtime.aclose()
            return asyncio.run(execute())
        prompt = resolve_prompt(self.config, prompt_name)
        from .messages import render_input
        parts, messages = render_input(prompt, values)
        clients = getattr(self._local, "clients", None)
        if clients is None:
            clients = {}
            self._local.clients = clients
        client = clients.get(prompt.model)
        if client is None:
            client = create_operator_llm_client(prompt.model)
            clients[prompt.model] = client
        from .systemone import render_questions
        exchange = self._exchange(prompt, parts, messages=messages,
                                  systemone=render_questions(prompt.systemone, values))
        request = next(exchange)
        while True:
            try:
                response = client.execute(request)
            except BaseException as exc:
                exchange.throw(exc)
                raise
            try:
                request = exchange.send(response)
            except StopIteration as done:
                return done.value

    def _exchange(self, prompt, parts, client=None, traces=None, response_parser=None, messages=None, systemone=None):
        """Shared schema retries and accounting for both transport forms."""
        contract_errors: list[Exception] = []
        validation_feedback = ""
        for attempt in range(prompt.schema_retries + 1):
            request = OperatorLLMRequest(
                prompt.name,
                prompt.version,
                prompt.model.name,
                parts,
                response_schema=prompt.response_schema,
                response_format=prompt.response_format,
                schema_attempt=attempt + 1,
                validation_feedback=validation_feedback,
                messages=messages,
                template=prompt.template.source,
                systemone=prompt.systemone if systemone is None else systemone,
            )
            self.coordinator.attempted()
            cached = None
            if client is not None and hasattr(client, 'lookup'):
                request, cached = yield _Lookup(request)
            if cached is None and getattr(getattr(client, 'journal', None), 'read_only', False):
                raise PromptReplayMissError('Read-only replay has no saved response for this request')
            original_request = request
            policy = (getattr(client, 'options', None) or {}).get('http_error_retry')
            while True:
                reservation = self.coordinator.reserve() if cached is None else None
                log_event(logger, "demiflow.operator_llm.request_started",
                    prompt=prompt.name, model=prompt.model.name, schema_attempt=attempt + 1)
                try:
                    if cached is None:
                        self.coordinator.started(reservation)
                        response = yield request
                    else:
                        response = cached
                    if traces is not None:traces.append(dict(response.metadata))
                    break
                except BaseException as exc:
                    if reservation is not None:self.coordinator.failed(reservation)
                    retry = retry_call(original_request, exc, policy) if cached is None else None
                    if retry is None:
                        if traces:
                            exc.call = {**(getattr(exc, 'call', None) or {}), 'attempts': [*traces, dict(getattr(exc, 'call', None) or {})]}
                        raise
                    if traces is not None:
                        traces.append({**getattr(exc, 'call', {}), 'http_retry_scheduled': True,
                                       'retry_delay_s': retry.delay_s})
                    request = retry
            try:
                if response_parser is not None:
                    # Agent envelopes are parsed here; operator arguments are
                    # validated by the row loop so errors can be observations.
                    result = response_parser(response.content)
                elif prompt.response_format == "text":
                    if not isinstance(response.content, str):
                        raise PromptResponseParseError("text response must be a string")
                    result = {"result": response.content}
                else:
                    result = _strict_object(response.content, prompt.name)
                if response_parser is None:
                    try:
                        validate_instance(
                            result, prompt.response_schema,
                            label=f"prompt {prompt.name!r} response",
                        )
                    except SchemaValidationError as exc:
                        raise PromptResponseContractError(str(exc)) from exc
            except (PromptResponseContractError, PromptResponseParseError) as exc:
                if reservation is not None:self.coordinator.failed(reservation, response)
                exc.call = dict(response.metadata)
                if response_parser is not None:
                    exc.response_content = response.content
                contract_errors.append(exc)
                if attempt < prompt.schema_retries:
                    validation_feedback = str(exc)
                    continue
                if len(contract_errors) > 1:
                    details = "; ".join(
                        f"attempt {index}: {error}"
                        for index, error in enumerate(contract_errors, start=1)
                    )
                    raise type(exc)(
                        f"Operator LLM structured response failed after "
                        f"{len(contract_errors)} attempts: {details}"
                    ) from exc
                raise
            except Exception:
                if reservation is not None:self.coordinator.failed(reservation, response)
                raise
            if reservation is not None:self.coordinator.completed(reservation, response)
            return result
        raise RuntimeError("unreachable Operator LLM schema retry state")



def validate_prompt_binding(operation, config):
    prompt = resolve_prompt(config, operation.prompt_name)
    required_inputs = set(prompt.arguments)
    if prompt.input_mode == 'messages' and any(k in (operation.options or {}) for k in ('codex_exec', 'codex_agent')):
        raise PromptResponseContractError('messages mode requires HTTP or offline transport; Codex does not preserve chat roles')
    if set(operation.inputs) != required_inputs:
        raise PromptResponseContractError('inputs must exactly cover prompt placeholders')
    if operation.output is not None:
        if len(prompt.response_keys) != 1:
            raise PromptResponseContractError('map_prompt output requires exactly one required response key')
    elif set(operation.outputs or {}) != set(prompt.response_keys):
        raise PromptResponseContractError('outputs must map every required response property')
    return prompt


class BoundOperatorLLMMap:
    def __init__(self, operation, runtime: OperatorLLMRuntime) -> None:
        self._operation = operation
        self._runtime = runtime
        self._prompt = validate_prompt_binding(operation, runtime.config)

    def values(self, row):
        if not isinstance(row, Mapping):
            raise TypeError('OperatorLLMMapOp expects a mapping row')
        values = {}
        for argument, field in self._operation.inputs.items():
            if field not in row:
                raise KeyError(f'Operator LLM prompt missing row field {field!r}')
            values[argument] = row[field]
        return values

    def merge(self, row, result):
        if self._operation.output is not None:
            return {**row, self._operation.output: result[self._prompt.response_keys[0]]}
        return {**row, **{field: result[key] for key, field in self._operation.outputs.items()}}

    def __call__(self, row: Mapping[str, Any]) -> dict[str, Any]:
        result = self._runtime.call(self._operation.prompt_name, self.values(row))
        return self.merge(row, result)


class AsyncOperatorLLMRuntime(OperatorLLMRuntime):
    def __init__(self, config, coordinator, options=None, *, service=None):
        super().__init__(config, coordinator)
        self._clients = {}
        self.options=options
        self._service = service
        self._service_owner = None
        self._service_owners = {}
        self.request_gate = None
        self.token_budget = None
        self.concurrency = 1
        self.artifact_store=None

    @asynccontextmanager
    async def transport_context(self, model):
        service = self._service
        if model.deployment is not None and service is not None:
            raise ValueError('model.deployment cannot be combined with service=')
        owner = self._service_owners.get(model)
        if owner is None:
            if model.deployment is not None:
                from demiflow.services.deployment import DeploymentService
                service = DeploymentService(model.name, model.deployment)
            if service is not None:
                owner = service.bind(model)
                self._service_owners[model] = owner
                self._service_owner = owner
        if owner is not None:
            await owner.ensure_ready()
        from demiflow.services.contracts import service_request_slot
        base_url = getattr(owner, 'base_url', None)
        async with service_request_slot(owner) as endpoint:
            yield endpoint or base_url

    async def call(self, prompt_name, values):
        result, _ = await self.call_with_trace(prompt_name,values)
        return result

    async def call_with_trace(self, prompt_name, values, *, prompt_override=None, input_budget=None,
                              response_parser=None, client_override=None):
        prompt = prompt_override or resolve_prompt(self.config, prompt_name)
        from .messages import render_input, admit_template
        if prompt.response_format != 'image':
            admit_template(prompt, values, self.options or {})
        if prompt.response_format == 'image':
            from .media import bounded_values
            values = bounded_values(prompt, values, self.options)
        parts, messages = render_input(prompt, values)
        client = client_override if client_override is not None else self._clients.get(prompt.model)
        if client is None:
            try:
                recovery = getattr(self, 'native_recovery', None)
                native_journal = None
                if recovery is not None:
                    # Take the recovery journal first and pass it to the factory
                    # so the constructor's http_error_retry guard sees a verified
                    # writable journal; bind_client then installs the scopes.
                    from demiflow.execution.operator_bindings.dataflow_model_journal import RemotePromptJournal
                    journal = getattr(recovery, 'journal', None)
                    if isinstance(journal, RemotePromptJournal):
                        native_journal = journal
                # Standalone transports retain their original factory call
                # shape; only an actual native journal requires this binding.
                native_options = {} if native_journal is None else {'native_journal': native_journal}
                client = (create_async_operator_llm_client(prompt.model,self.options,**native_options)
                          if self.options else create_async_operator_llm_client(prompt.model,**native_options))
                if recovery is not None:
                    recovery.bind_client(client)
                configure = getattr(client, 'configure_capacity', None)
                if self.artifact_store is not None:client.artifact_store=self.artifact_store
                if configure is not None:
                    configure(self.concurrency)
                if hasattr(client, 'transport_context'):
                    from functools import partial
                    client.transport_context = partial(self.transport_context, prompt.model)
            except Exception as exc:
                if self.request_gate is not None:
                    self.request_gate.result(fatal=f'model client configuration failed: {type(exc).__name__}: {exc}')
                raise
            self._clients[prompt.model] = client
        traces=[]
        from .systemone import render_questions
        exchange = self._exchange(prompt, parts, client, traces, response_parser=response_parser, messages=messages,
                                  systemone=render_questions(prompt.systemone, values))
        try:request = next(exchange)
        except StopIteration as done:return done.value, {**(traces[-1] if traces else {}),"attempts":traces}
        while True:
            try:
                if isinstance(request, _Lookup):
                    validate_input = getattr(client, 'validate_input', None)
                    if validate_input is not None:
                        validate_input(request.request)
                    if input_budget is not None:
                        input_budget.validate(request.request)
                    if self.token_budget is not None:
                        self.token_budget.validate(request.request)
                    prepare_lookup = getattr(client, 'prepare_lookup', None)
                    response = (await prepare_lookup(request.request) if prepare_lookup is not None
                                else await _journal_io(_prepare_lookup, client, request.request))
                else:
                    # Cache hits never load a model or consume a new-call budget.
                    if self._service is not None and not hasattr(client, 'transport_context'):
                        if self._service_owner is None:
                            self._service_owner = self._service.bind(prompt.model)
                        await self._service_owner.ensure_ready()
                    if isinstance(request, HTTPRetryCall):
                        import asyncio
                        await asyncio.sleep(request.delay_s)
                    execute = client.execute_retry if isinstance(request, HTTPRetryCall) else client.execute
                    if self.request_gate is None:
                        response = await execute(request)
                    else:
                        import httpx
                        async with self.request_gate.enter():
                            import time
                            request_started=time.monotonic()
                            try:
                                response = await execute(request)
                            except (httpx.TransportError, TimeoutError):
                                self.request_gate.result(transient=True,elapsed_s=time.monotonic()-request_started)
                                raise
                            except Exception as exc:
                                from .errors import PromptStreamError
                                code = getattr(exc, 'http_status', None)
                                self.request_gate.result(transient=isinstance(exc, PromptStreamError) or code in {408,429} or (code is not None and code >= 500),
                                    elapsed_s=time.monotonic()-request_started,
                                    backpressure=code == 429 or (code is not None and code >= 500))
                                raise
                            else:
                                self.request_gate.result(success=True,elapsed_s=time.monotonic()-request_started)
            except BaseException as exc:
                try:
                    request = exchange.throw(exc)  # a declared HTTP retry can continue this exchange
                except StopIteration as done:
                    return done.value, {**(traces[-1] if traces else {}), 'attempts': traces}
                except BaseException as terminal:
                    if traces and not getattr(terminal, 'call', None):
                        terminal.call = {'attempts': list(traces)}
                    code = getattr(terminal, 'http_status', None)
                    # A saved error is a row outcome, not a fresh observation
                    # of this invocation's credentials/service health. In
                    # particular, cancellation during retry backoff leaves a
                    # complete 401 for replay; it must not stop unrelated rows.
                    reused = (getattr(terminal, 'call', None) or {}).get('reused') is True
                    provider_code = (getattr(terminal, 'call', None) or {}).get('provider_error_code')
                    row_error = any(rule['status'] == code and rule['code'] == provider_code
                                    for rule in (self.options or {}).get('nonfatal_http_errors', []))
                    if self.request_gate is not None and code in {400,401,403,404} and not reused and not row_error:
                        self.request_gate.result(fatal=f'model authentication/configuration HTTP {code}')
                    raise
                continue
            try:
                request = exchange.send(response)
            except StopIteration as done:
                return done.value, {**(traces[-1] if traces else {}),"attempts":traces}
            except Exception as exc:
                if traces:
                    exc.call = {**traces[-1], **(getattr(exc, 'call', None) or {}), 'attempts': traces}
                raise

    async def aclose(self):
        clients, self._clients = self._clients, {}
        owner, self._service_owner = self._service_owner, None
        owners, self._service_owners = self._service_owners, {}
        errors = []
        for client in clients.values():
            try:
                await client.aclose()
            except BaseException as exc:
                errors.append(exc)
        # Structural leases need not be hashable.
        seen=set()
        for resource in [*owners.values(), *([owner] if owner is not None else [])]:
            if id(resource) in seen: continue
            seen.add(id(resource))
            try: await resource.aclose()
            except BaseException as exc: errors.append(exc)
        if errors: raise errors[0]


class PromptActor(BoundOperatorLLMMap):
    """Native map_async actor; uses exactly the map_prompt row contract."""

    def native_stream_recovery(self, action, node_id):
        options = self._runtime.options or {}
        if self.environment is not None or any(k in options for k in ('offline_dir', 'offline_store', 'codex_exec', 'codex_agent')):
            return None
        from .stream_recovery import NativePromptRecovery
        self._runtime.native_recovery = NativePromptRecovery(self, action, node_id)
        return self._runtime.native_recovery

    def bind_execution(self, context, node_id):
        self._runtime.coordinator.bind_execution(context, node_id)
        if self._prompt.response_format=='image':
            from ..execution.run_context import current_action
            from .media import media_limits
            action=current_action()
            stores=getattr(action,'object_sinks',{}) if action is not None else {}
            store=stores.get(self._runtime.options['object_store'])
            if store is None:
                raise ValueError('Image output has no native artifact participant')
            from ..execution.object_sink import check_image_replay,image_request_budget
            check_image_replay(action)
            self._runtime.coordinator._artifact_request_budget=image_request_budget(action)
            store.reserve_capacity(self._operation.max_requests,
                media_limits(self._runtime.options)['max_image_bytes'],node_id)
            self._runtime.artifact_store=store

    def snapshot_state(self):
        return {}  # clients/leases/metrics are resources, not semantic row state

    def restore_state(self, state):
        if state != {}:
            raise ValueError('Prompt actor has no mutable semantic state')
    concurrency = 1
    queue_depth = None
    catch = ()

    @property
    def cancel_drain_timeout_s(self):
        options = self._runtime.options or {}
        return options.get('timeout_s',120)+1

    def __init__(self, operation, config, coordinator, options=None, *, service=None):
        model = validate_prompt_binding(operation, config).model
        if model.deployment is not None:
            if service is not None:
                raise ValueError('model.deployment cannot be combined with service=')
            if any(key in (options or {}) for key in ('offline_dir','offline_store','codex_exec','codex_agent')):
                raise ValueError('Managed deployment requires online HTTP prompt execution')
        if service is not None:
            # Validate endpoint/transport when declaring, without starting it.
            service.bind(validate_prompt_binding(operation, config).model)
        super().__init__(operation, AsyncOperatorLLMRuntime(config, coordinator, options, service=service))
        self.when=None;self.call_output=None;self.error_output=None
        self._call_validations = {}
        self.environment = None
        self.label = operation.prompt_name
        from demiflow.execution.journal.run_telemetry import CallMetrics
        self.call_metrics = CallMetrics()

    async def __call__(self, row):
        try:
            if self.when is not None:
                validation = self._call_validations.get('when')
                if validation is not None:
                    validation.inputs((row,), {})
                selected = self.when(row)
                if validation is not None:
                    validation.output(selected)
                if not selected:
                    return row
            if self.environment is None:
                result, trace = await self._runtime.call_with_trace(self._operation.prompt_name, self.values(row))
            else:
                from demiflow.environment import call_in_environment
                result, trace = await call_in_environment(self._runtime, self._operation.prompt_name,
                    self.values(row), row[self.environment.resources] if self.environment.resources else {},
                    self.environment)
            self._observe_call(trace)
            out=self.merge(row,result)
            if self.call_output:out[self.call_output]=self._delivery_trace(trace)
            if self._prompt.response_format == 'image' and self.error_output:
                out[self.error_output] = None
            return out
        except Exception as exc:
            self._observe_call(getattr(exc,'call',{}), error=True)
            from ..services import ModelServiceError
            from demiflow.services.request_limits import ServiceStopped
            import sqlite3
            from ..data.runtime_contract import RuntimeContractError
            if isinstance(exc, RuntimeContractError) or isinstance(exc, (ModelServiceError, ServiceStopped, OSError, sqlite3.Error)) and not isinstance(exc, TimeoutError):
                raise
            from .journal import UncertainPromptCall
            if isinstance(exc, UncertainPromptCall):
                raise
            if not self.error_output:raise
            if self._prompt.response_format == 'image':
                out = self.merge(row, {'image': None})
                return {**out, self.error_output: f'{type(exc).__name__}: {exc}',
                        self.call_output: self._delivery_trace(getattr(exc, 'call', None))}
            from .errors import error_category
            return {**row,self.error_output:{'category':error_category(exc),'type':type(exc).__name__,'detail':str(exc),'call':self._delivery_trace(getattr(exc,'call',{}))}}

    def _delivery_trace(self, trace):
        # Single-completion native Prompt rows carry original call facts;
        # monitoring above retains this attempt's true reuse/token counts.
        # Agent interaction traces and external-cache contracts are unchanged.
        recovery = getattr(self._runtime, 'native_recovery', None)
        project = getattr(recovery, 'delivery_trace', None)
        return project(trace) if trace is not None and self.environment is None and callable(project) else trace

    def _observe_call(self, trace, *, error=False):
        if 'environment' not in trace:
            self.call_metrics.observe(trace, error=error)
            return
        # Account each real model exchange; a row may mix reused/new rounds.
        self.call_metrics.observe({}, error=error)
        for turn in trace['environment']['turns']:
            for attempt in turn.get('attempts') or [turn]:
                self.call_metrics.observe(attempt)

    async def astart(self):
        self.call_metrics.reset()

    async def aclose(self):
        await self._runtime.aclose()
        self._runtime.coordinator.release_execution()
        self._runtime.artifact_store=None


def _strict_object(value: Any, prompt_name: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or "").strip())
    except Exception as exc:
        raise PromptResponseParseError(
            f"prompt {prompt_name!r} response is not strict JSON"
        ) from exc
    if not isinstance(parsed, dict):
        raise PromptResponseParseError(
            f"prompt {prompt_name!r} response must be a JSON object"
        )
    return parsed
