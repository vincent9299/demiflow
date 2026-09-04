"""Worker-local Operator LLM clients owned by Demiflow."""
from __future__ import annotations

import base64
import json
import os
import time
from typing import Any, Mapping

from .errors import PromptProviderUnavailableError
from .model import (
    ImagePart, OperatorLLMRequest, OperatorLLMRequestUsage,
    OperatorLLMResponse, PromptModel, TextPart,
)


def required_environment(
    names: tuple[str, ...], environment: Mapping[str, str] | None = None,
) -> dict[str, str]:
    source = os.environ if environment is None else environment
    missing = [name for name in names if not str(source.get(name) or "").strip()]
    if missing:
        raise PromptProviderUnavailableError(
            "Operator LLM requires environment variables: " + ", ".join(missing)
        )
    return {name: str(source[name]) for name in names}


class OpenAICompatibleOperatorLLMClient:
    def __init__(self, model: PromptModel) -> None:
        if model.transport != "openai_compatible":
            raise PromptProviderUnavailableError(
                f"unsupported Operator LLM transport: {model.transport}"
            )
        names = tuple(name for name in (model.base_url_env, model.api_key_env) if name)
        values = required_environment(names)
        self.base_url = (model.base_url or values[model.base_url_env]).rstrip("/")
        self.api_key = values[model.api_key_env]
        self.timeout_seconds = 120
        self.max_retries = 3

    def execute(self, request: OperatorLLMRequest) -> OperatorLLMResponse:
        user_content = _request_content(request)
        import requests
        response = None
        try:
            for attempt in range(self.max_retries + 1):
                try:
                    response = requests.post(
                        f"{self.base_url}/chat/completions",
                        headers={"Authorization": f"Bearer {self.api_key}"},
                        json={
                            "model": request.model,
                            "messages": [
                                {"role": "system", "content": _response_contract_instruction(request)},
                                {"role": "user", "content": user_content},
                            ],
                            "temperature": 0,
                        },
                        timeout=self.timeout_seconds,
                    )
                    if response.status_code not in {429, 500, 502, 503, 504}:
                        response.raise_for_status()
                        break
                    response.raise_for_status()
                except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
                    retryable = response is None or response.status_code in {
                        429, 500, 502, 503, 504,
                    }
                    if not retryable or attempt >= self.max_retries:
                        raise
                    time.sleep(min(0.1 * (2 ** attempt), 1.0))
            assert response is not None
            value = response.json()
            usage = OperatorLLMRequestUsage.from_value(value.get("usage"))
            return OperatorLLMResponse(
                value["choices"][0]["message"]["content"], usage,
                endpoint=str(response.url),
            )
        except Exception:
            raise


class AzureOpenAIOperatorLLMClient:
    def __init__(self, model: PromptModel) -> None:
        if model.transport != "azure_openai":
            raise PromptProviderUnavailableError(
                f"unsupported Operator LLM transport: {model.transport}"
            )
        names = tuple(name for name in (model.base_url_env, model.api_key_env) if name)
        values = required_environment(names)
        self.model = model
        self.base_url = (model.base_url or values[model.base_url_env]).rstrip("/")
        self.api_key = values[model.api_key_env]
        self.timeout_seconds = 120
        self.max_retries = 3

    def execute(self, request: OperatorLLMRequest) -> OperatorLLMResponse:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=self.api_key,
            api_version=self.model.api_version,
            azure_endpoint=self.base_url,
            timeout=self.timeout_seconds,
        )
        content = _request_content(request)
        try:
            response = None
            for attempt in range(self.max_retries + 1):
                try:
                    response = client.chat.completions.create(
                        model=request.model,
                        messages=[
                            {"role": "system", "content": _response_contract_instruction(request)},
                            {"role": "user", "content": content},
                        ],
                        temperature=1.0,
                    )
                    break
                except Exception:
                    if attempt >= self.max_retries:
                        raise
                    time.sleep(min(0.1 * (2 ** attempt), 1.0))
            assert response is not None
            choices = getattr(response, "choices", ()) or ()
            if not choices or getattr(choices[0], "message", None) is None:
                raise ValueError("Azure OpenAI response has no message")
            usage = OperatorLLMRequestUsage.from_value(getattr(response, "usage", None))
            return OperatorLLMResponse(
                getattr(choices[0].message, "content", "") or "", usage,
                endpoint=self.base_url,
            )
        except Exception:
            raise


def _request_content(request: OperatorLLMRequest) -> Any:
    content: list[dict[str, Any]] = []
    for part in request.parts:
        if isinstance(part, TextPart):
            content.append({"type": "text", "text": part.text})
        elif isinstance(part, ImagePart):
            image = part.image
            uri = image.uri or (
                f"data:{image.media_type};base64,"
                f"{base64.b64encode(image.data or b'').decode('ascii')}"
            )
            content.append({"type": "image_url", "image_url": {"url": uri}})
    return (
        content[0]["text"]
        if len(content) == 1 and content[0]["type"] == "text"
        else content
    )


def _response_contract_instruction(request: OperatorLLMRequest) -> str:
    """Render the frozen response contract into a model-visible instruction."""
    if not request.response_schema:
        return "Return one strict JSON object."
    schema = json.dumps(
        request.response_schema,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    lines = [
        "Return exactly one JSON object that satisfies the following JSON Schema.",
        "Output JSON only. Do not use Markdown fences or explanatory text.",
        "Include every required property at every nesting level and do not add forbidden properties.",
        f"JSON Schema: {schema}",
    ]
    if request.validation_feedback:
        lines.extend([
            "The previous response failed schema validation.",
            f"Validation error: {request.validation_feedback}",
            "Regenerate the entire JSON object. Do not return a patch or an explanation.",
        ])
    return "\n".join(lines)


def create_operator_llm_client(model: PromptModel):
    if model.transport == "azure_openai":
        return AzureOpenAIOperatorLLMClient(model)
    return OpenAICompatibleOperatorLLMClient(model)
