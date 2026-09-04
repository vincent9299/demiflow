"""Immutable Operator LLM prompt-pack and execution values."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping
import re



PROMPT_MODEL_TRANSPORTS = frozenset({"azure_openai", "openai_compatible"})
PROMPT_MODEL_FIELDS = frozenset({
    "name", "transport", "base_url", "base_url_env", "api_key_env", "api_version",
})
PROMPT_ENV_NAME_PATTERN = r"[A-Z_][A-Z0-9_]*"
_PROMPT_ENV_NAME = re.compile(PROMPT_ENV_NAME_PATTERN)


def parse_prompt_model(value: Any, *, label: str) -> "PromptModel":
    model, diagnostics = inspect_prompt_model(value, label=label)
    if diagnostics or model is None:
        raise ValueError("; ".join(item.message for item in diagnostics))
    return model

def inspect_prompt_model(value: Any, *, label: str, prompt: str = "") -> tuple["PromptModel | None", tuple[Any, ...]]:
    """Collect independent model configuration errors."""
    from .errors import PromptContractDiagnostic
    issues = []
    def issue(code, message, field=""):
        issues.append(PromptContractDiagnostic(code, message, field, prompt))
    if not isinstance(value, Mapping):
        issue("prompt_model_not_mapping", f"{label} must be a mapping", "model")
        return None, tuple(issues)
    extra = sorted(set(value) - PROMPT_MODEL_FIELDS)
    for name in extra:
        issue("prompt_model_field_unsupported", f"{label} contains unsupported field: {name}", f"model.{name}")
    normalized = {name: str(value.get(name) or "").strip() for name in PROMPT_MODEL_FIELDS}
    for name in ("name", "transport", "api_key_env"):
        if not normalized[name]:
            issue("prompt_model_field_required", f"{label} requires non-empty {name}", f"model.{name}")
    transport = normalized["transport"]
    if transport and transport not in PROMPT_MODEL_TRANSPORTS:
        issue("prompt_model_transport_invalid", f"{label} has unsupported transport: {transport}", "model.transport")
    base_url, base_url_env = normalized["base_url"], normalized["base_url_env"]
    if bool(base_url) == bool(base_url_env):
        issue("prompt_model_base_url_invalid", f"{label} requires exactly one of base_url or base_url_env", "model")
    for name in ("base_url_env", "api_key_env"):
        value_text = normalized[name]
        if value_text and not _PROMPT_ENV_NAME.fullmatch(value_text):
            issue("prompt_model_env_invalid", f"{label} {name} must be an uppercase variable name", f"model.{name}")
    api_version = normalized["api_version"]
    if transport == "azure_openai" and not api_version:
        issue("prompt_model_api_version_required", f"{label} azure_openai requires api_version", "model.api_version")
    if transport == "openai_compatible" and api_version:
        issue("prompt_model_api_version_forbidden", f"{label} openai_compatible does not accept api_version", "model.api_version")
    if issues:
        return None, tuple(issues)
    return PromptModel(normalized["name"], transport, base_url_env, normalized["api_key_env"], base_url, api_version), ()

class PromptPackVersion(str, Enum):
    V2 = "demiflow_prompt_pack_v2"


class PlaceholderKind(str, Enum):
    TEXT = "text"
    JSON = "json"
    IMAGE = "image"


@dataclass(frozen=True)
class PromptPlaceholder:
    name: str
    kind: PlaceholderKind
    start: int
    end: int


@dataclass(frozen=True)
class CompiledTemplate:
    source: str
    placeholders: tuple[PromptPlaceholder, ...]

    @property
    def arguments(self) -> Mapping[str, PlaceholderKind]:
        return MappingProxyType({item.name: item.kind for item in self.placeholders})


@dataclass(frozen=True)
class PromptModel:
    name: str
    transport: str
    base_url_env: str = ""
    api_key_env: str = ""
    base_url: str = ""
    api_version: str = ""


@dataclass(frozen=True)
class PromptDefinition:
    name: str
    version: str
    model: PromptModel
    template: CompiledTemplate
    response_schema: Mapping[str, Any]
    schema_retries: int = 0

    @property
    def response_keys(self) -> tuple[str, ...]:
        from demiflow.schema import required_properties
        return required_properties(self.response_schema)

    @property
    def input_modalities(self) -> tuple[str, ...]:
        values = ["text"]
        if any(item.kind is PlaceholderKind.IMAGE for item in self.template.placeholders):
            values.append("image")
        return tuple(values)


@dataclass(frozen=True)
class PromptPack:
    schema_version: PromptPackVersion
    prompts: tuple[PromptDefinition, ...]
    content_hash: str

    @property
    def prompt_definitions(self) -> Mapping[str, PromptDefinition]:
        return MappingProxyType({item.name: item for item in self.prompts})

    @property
    def required_environment_names(self) -> tuple[str, ...]:
        return tuple(sorted({
            name
            for prompt in self.prompts
            for name in (prompt.model.base_url_env, prompt.model.api_key_env)
            if name
        }))


@dataclass(frozen=True)
class TextPart:
    text: str


@dataclass(frozen=True)
class ImageValue:
    data: bytes | None = None
    uri: str = ""
    media_type: str = ""

    def __post_init__(self) -> None:
        if (self.data is None) == (not self.uri):
            raise ValueError("ImageValue requires exactly one of data or uri")
        if self.data is not None and not self.media_type:
            raise ValueError("binary ImageValue requires media_type")


@dataclass(frozen=True)
class ImagePart:
    image: ImageValue


OperatorLLMPart = TextPart | ImagePart


@dataclass(frozen=True)
class OperatorLLMRequest:
    prompt_name: str
    prompt_version: str
    model: str
    parts: tuple[OperatorLLMPart, ...]
    response_schema: Mapping[str, Any] = field(default_factory=dict)
    schema_attempt: int = 1
    validation_feedback: str = ""

    def __post_init__(self) -> None:
        if self.schema_attempt < 1:
            raise ValueError("Operator LLM schema_attempt must be positive")


@dataclass(frozen=True)
class OperatorLLMRequestUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0

    def __post_init__(self) -> None:
        if any(value < 0 for value in self.to_dict().values()):
            raise ValueError("Operator LLM request usage cannot be negative")

    def to_dict(self) -> dict[str, int]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cached_input_tokens": self.cached_input_tokens,
            "reasoning_tokens": self.reasoning_tokens,
        }

    @classmethod
    def from_value(cls, value: Any) -> "OperatorLLMRequestUsage | None":
        if value is None:
            return None
        if isinstance(value, cls):
            return value
        source: Mapping[str, Any] | Any = value

        def field(*names: str) -> int:
            for name in names:
                candidate = (
                    source.get(name)
                    if isinstance(source, Mapping)
                    else getattr(source, name, None)
                )
                if candidate is not None:
                    return max(0, int(candidate))
            return 0

        cached = field("cached_input_tokens", "cached_tokens")
        reasoning = field("reasoning_tokens")
        prompt_details = (
            source.get("prompt_tokens_details")
            if isinstance(source, Mapping)
            else getattr(source, "prompt_tokens_details", None)
        )
        completion_details = (
            source.get("completion_tokens_details")
            if isinstance(source, Mapping)
            else getattr(source, "completion_tokens_details", None)
        )
        if prompt_details is not None:
            cached = _usage_detail(prompt_details, "cached_tokens", cached)
        if completion_details is not None:
            reasoning = _usage_detail(completion_details, "reasoning_tokens", reasoning)
        return cls(
            field("input_tokens", "prompt_tokens"),
            field("output_tokens", "completion_tokens"),
            cached,
            reasoning,
        )


def _usage_detail(value: Any, name: str, default: int) -> int:
    candidate = value.get(name) if isinstance(value, Mapping) else getattr(value, name, None)
    return default if candidate is None else max(0, int(candidate))


@dataclass(frozen=True)
class OperatorLLMResponse:
    content: Any
    usage: OperatorLLMRequestUsage | None = None
    endpoint: str = ""


@dataclass(frozen=True)
class OperatorLLMUsage:
    calls_attempted: int = 0
    requests_reserved: int = 0
    requests_started: int = 0
    requests_completed: int = 0
    requests_failed: int = 0
    input_tokens: int = 0
    output_tokens: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "prompt_calls_attempted": self.calls_attempted,
            "provider_requests_reserved": self.requests_reserved,
            "provider_requests_started": self.requests_started,
            "provider_requests_completed": self.requests_completed,
            "provider_requests_failed": self.requests_failed,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
        }
