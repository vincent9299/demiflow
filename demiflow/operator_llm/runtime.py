"""Operator LLM call coordination and backend-neutral row lowering."""
from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import replace
from typing import Any, Mapping

from demiflow._compat.observability import log_event

from .client import create_operator_llm_client
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
    def __init__(self, config: PromptPack, coordinator) -> None:
        self.config = config
        self.coordinator = coordinator
        self._local = threading.local()

    def call(self, prompt_name: str, values: Mapping[str, Any]) -> dict[str, Any]:
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
        contract_errors: list[Exception] = []
        validation_feedback = ""
        for attempt in range(prompt.schema_retries + 1):
            request = OperatorLLMRequest(
                prompt.name,
                prompt.version,
                prompt.model.name,
                parts,
                response_schema=prompt.response_schema,
                schema_attempt=attempt + 1,
                validation_feedback=validation_feedback,
            )
            self.coordinator.attempted()
            reservation = self.coordinator.reserve()
            log_event(
                logger, "demiflow.operator_llm.request_started",
                prompt=prompt.name, model=prompt.model.name,
                schema_attempt=attempt + 1,
            )
            try:
                self.coordinator.started(reservation)
                response = client.execute(request)
            except Exception:
                self.coordinator.failed(reservation)
                raise
            try:
                result = _strict_object(response.content, prompt.name)
                try:
                    validate_instance(
                        result, prompt.response_schema,
                        label=f"prompt {prompt.name!r} response",
                    )
                except SchemaValidationError as exc:
                    raise PromptResponseContractError(str(exc)) from exc
            except (PromptResponseContractError, PromptResponseParseError) as exc:
                self.coordinator.failed(reservation, response)
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
                self.coordinator.failed(reservation, response)
                raise
            self.coordinator.completed(reservation, response)
            return result
        raise RuntimeError("unreachable Operator LLM schema retry state")



class BoundOperatorLLMMap:
    def __init__(self, operation, runtime: OperatorLLMRuntime) -> None:
        self._operation = operation
        self._runtime = runtime

    def __call__(self, row: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(row, Mapping):
            raise TypeError("OperatorLLMMapOp expects a mapping row")
        values = {}
        for argument, field in self._operation.inputs.items():
            if field not in row:
                raise KeyError(f"Operator LLM prompt missing row field {field!r}")
            values[argument] = row[field]
        result = self._runtime.call(
            self._operation.prompt_name, values,
        )
        if self._operation.output is not None:
            prompt = resolve_prompt(self._runtime.config, self._operation.prompt_name)
            if len(prompt.response_keys) != 1:
                raise PromptResponseContractError(
                    "map_prompt output requires exactly one required response key"
                )
            return {**row, self._operation.output: result[prompt.response_keys[0]]}
        updates = {
            row_field: result[result_key]
            for result_key, row_field in (self._operation.outputs or {}).items()
        }
        return {**row, **updates}


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
