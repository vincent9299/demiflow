"""Operator LLM call coordination and backend-neutral row lowering."""
from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import replace
from typing import Any, Mapping

from demiflow._compat.observability import log_event

from .client import create_operator_llm_client, create_async_operator_llm_client
from .errors import PromptBudgetExceededError, PromptResponseContractError, PromptResponseParseError
from .model import OperatorLLMRequest, OperatorLLMResponse, OperatorLLMUsage, PromptPack
from .parser import resolve_prompt
from .template import render_template
from demiflow.schema import SchemaValidationError, validate_instance

logger = logging.getLogger(__name__)


class InProcessOperatorLLMCoordinator:
    def __init__(self, max_requests: int | None = None, on_change=None) -> None:
        self._max_requests = max_requests
        self._on_change = on_change
        self._lock = threading.Lock()
        self._usage = OperatorLLMUsage()
        self._reservations: set[str] = set()

    def attempted(self) -> None:
        with self._lock:
            self._usage = replace(self._usage, calls_attempted=self._usage.calls_attempted + 1)
        self._publish()

    def reserve(self) -> str:
        with self._lock:
            if self._max_requests is not None and self._usage.requests_reserved >= self._max_requests:
                raise PromptBudgetExceededError("Operator LLM request budget exhausted")
            value = "operator-llm-" + uuid.uuid4().hex
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
        parts = render_template(prompt.template, values)
        clients = getattr(self._local, "clients", None)
        if clients is None:
            clients = {}
            self._local.clients = clients
        client = clients.get(prompt.model)
        if client is None:
            client = create_operator_llm_client(prompt.model)
            clients[prompt.model] = client
        exchange = self._exchange(prompt, parts)
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

    def _exchange(self, prompt, parts, client=None, traces=None):
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
            )
            self.coordinator.attempted()
            cached = client.lookup(request) if client is not None and hasattr(client, "lookup") else None
            reservation = self.coordinator.reserve() if cached is None else None
            log_event(
                logger, "demiflow.operator_llm.request_started",
                prompt=prompt.name, model=prompt.model.name,
                schema_attempt=attempt + 1,
            )
            try:
                if cached is None:
                    self.coordinator.started(reservation)
                    response = yield request
                else:
                    response = cached
                if traces is not None:traces.append(dict(response.metadata))
            except BaseException:
                if reservation is not None:self.coordinator.failed(reservation)
                raise
            try:
                if prompt.response_format == "text":
                    if not isinstance(response.content, str):
                        raise PromptResponseParseError("text response must be a string")
                    result = {"result": response.content}
                else:
                    result = _strict_object(response.content, prompt.name)
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
    required_inputs = {p.name for p in prompt.template.placeholders}
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
    def __init__(self, config, coordinator, options=None):
        super().__init__(config, coordinator)
        self._clients = {}
        self.options=options

    async def call(self, prompt_name, values):
        result, _ = await self.call_with_trace(prompt_name,values)
        return result

    async def call_with_trace(self, prompt_name, values):
        prompt = resolve_prompt(self.config, prompt_name)
        parts = render_template(prompt.template, values)
        client = self._clients.get(prompt.model)
        if client is None:
            client = (create_async_operator_llm_client(prompt.model,self.options)
                      if self.options else create_async_operator_llm_client(prompt.model))
            self._clients[prompt.model] = client
        traces=[]
        exchange = self._exchange(prompt, parts, client, traces)
        try:request = next(exchange)
        except StopIteration as done:return done.value, {**(traces[-1] if traces else {}),"attempts":traces}
        while True:
            try:
                response = await client.execute(request)
            except BaseException as exc:
                exchange.throw(exc)  # cancellation also releases the reservation
                raise
            try:
                request = exchange.send(response)
            except StopIteration as done:
                return done.value, {**(traces[-1] if traces else {}),"attempts":traces}

    async def aclose(self):
        clients, self._clients = self._clients, {}
        try:
            for client in clients.values():
                await client.aclose()
        finally:
            self._clients.clear()


class PromptActor(BoundOperatorLLMMap):
    """Native map_async actor; uses exactly the map_prompt row contract."""
    concurrency = 1
    queue_depth = None
    catch = ()

    def __init__(self, operation, config, coordinator, options=None):
        super().__init__(operation, AsyncOperatorLLMRuntime(config, coordinator, options))
        self.when=None;self.call_output=None;self.error_output=None
        self.label = operation.prompt_name

    async def __call__(self, row):
        try:
            if self.when is not None and not self.when(row):return row
            result, trace = await self._runtime.call_with_trace(self._operation.prompt_name, self.values(row))
            out=self.merge(row,result)
            if self.call_output:out[self.call_output]=trace
            return out
        except Exception as exc:
            if not self.error_output:raise
            return {**row,self.error_output:{'type':type(exc).__name__,'detail':str(exc),'call':getattr(exc,'call',{})}}

    async def aclose(self):
        await self._runtime.aclose()


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
