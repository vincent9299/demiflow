"""Compile minimal prompt templates and render ordered multimodal parts."""
from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from .errors import (
    PromptArgumentMissingError, PromptArgumentTypeError,
    PromptArgumentUnexpectedError, PromptTemplateSyntaxError,
)
from .model import (
    CompiledTemplate, ImagePart, ImageValue, PlaceholderKind,
    OperatorLLMPart, PromptPlaceholder, TextPart,
)

_PLACEHOLDER = re.compile(
    r"{{\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\|\s*([A-Za-z_][A-Za-z0-9_]*))?\s*}}"
)
_ANY_DELIMITER = re.compile(r"{{|}}")
_DATA_URL = re.compile(r"^data:(image/[A-Za-z0-9.+-]+);base64,", re.IGNORECASE)


def _compile_template_strict(source: str) -> CompiledTemplate:
    if not isinstance(source, str) or not source.strip():
        raise PromptTemplateSyntaxError("prompt template must be a non-empty string")
    placeholders: list[PromptPlaceholder] = []
    kinds: dict[str, PlaceholderKind] = {}
    covered: list[tuple[int, int]] = []
    for match in _PLACEHOLDER.finditer(source):
        renderer = (match.group(2) or PlaceholderKind.TEXT.value).lower()
        try:
            kind = PlaceholderKind(renderer)
        except ValueError as exc:
            allowed = [item.value for item in PlaceholderKind]
            raise PromptTemplateSyntaxError(
                f"unknown placeholder renderer {renderer!r}; allowed values: {allowed}"
            ) from exc
        name = match.group(1)
        previous = kinds.get(name)
        if previous is not None and previous is not kind:
            raise PromptTemplateSyntaxError(
                f"placeholder {name!r} uses conflicting renderers: "
                f"{previous.value!r} and {kind.value!r}"
            )
        kinds[name] = kind
        placeholders.append(PromptPlaceholder(name, kind, match.start(), match.end()))
        covered.append((match.start(), match.end()))
    for token in _ANY_DELIMITER.finditer(source):
        if not any(start <= token.start() < end for start, end in covered):
            raise PromptTemplateSyntaxError(
                f"invalid placeholder syntax at character {token.start()}"
            )
    return CompiledTemplate(source, tuple(placeholders))



def inspect_template(source: Any, *, prompt: str = "") -> tuple[CompiledTemplate | None, tuple[Any, ...]]:
    """Collect independent template delimiter and renderer errors."""
    from .errors import PromptContractDiagnostic
    if not isinstance(source, str):
        return None, (PromptContractDiagnostic(
            "prompt_template_not_string", f"prompt template must be a string, got {type(source).__name__}",
            "template", prompt,
        ),)
    if not source.strip():
        return None, (PromptContractDiagnostic(
            "prompt_template_empty", "prompt template must be a non-empty string",
            "template", prompt,
        ),)
    issues = []
    kinds: dict[str, PlaceholderKind] = {}
    covered = []
    for match in _PLACEHOLDER.finditer(source):
        renderer = (match.group(2) or PlaceholderKind.TEXT.value).lower()
        try:
            kind = PlaceholderKind(renderer)
        except ValueError:
            issues.append(PromptContractDiagnostic(
                "prompt_template_renderer_invalid",
                f"unknown placeholder renderer {renderer!r}; allowed values: {[item.value for item in PlaceholderKind]}",
                f"template@{match.start()}", prompt,
            ))
            covered.append((match.start(), match.end()))
            continue
        name = match.group(1)
        previous = kinds.get(name)
        if previous is not None and previous is not kind:
            issues.append(PromptContractDiagnostic(
                "prompt_template_renderer_conflict",
                f"placeholder {name!r} uses conflicting renderers: {previous.value!r} and {kind.value!r}",
                f"template@{match.start()}", prompt,
            ))
        else:
            kinds[name] = kind
        covered.append((match.start(), match.end()))
    for token in _ANY_DELIMITER.finditer(source):
        if not any(start <= token.start() < end for start, end in covered):
            issues.append(PromptContractDiagnostic(
                "prompt_template_syntax_invalid",
                f"invalid placeholder syntax at character {token.start()}",
                f"template@{token.start()}", prompt,
            ))
    if issues:
        return None, tuple(issues)
    return _compile_template_strict(source), ()


def compile_template(source: str) -> CompiledTemplate:
    template, diagnostics = inspect_template(source)
    if diagnostics or template is None:
        raise PromptTemplateSyntaxError("; ".join(item.message for item in diagnostics))
    return template

def render_template(
    template: CompiledTemplate,
    values: Mapping[str, Any],
) -> tuple[OperatorLLMPart, ...]:
    expected = set(template.arguments)
    supplied = {str(key) for key in values}
    missing = sorted(expected - supplied)
    extra = sorted(supplied - expected)
    if missing:
        raise PromptArgumentMissingError(f"missing prompt arguments: {missing}")
    if extra:
        raise PromptArgumentUnexpectedError(f"unexpected prompt arguments: {extra}")
    parts: list[OperatorLLMPart] = []
    cursor = 0
    for item in template.placeholders:
        _append_text(parts, template.source[cursor:item.start])
        value = values[item.name]
        if item.kind is PlaceholderKind.TEXT:
            _append_text(parts, _render_text(item.name, value))
        elif item.kind is PlaceholderKind.JSON:
            try:
                rendered = json.dumps(
                    value, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"),
                )
            except (TypeError, ValueError) as exc:
                raise PromptArgumentTypeError(
                    f"prompt argument {item.name!r} is not JSON serializable"
                ) from exc
            _append_text(parts, rendered)
        else:
            parts.extend(_render_images(item.name, value))
        cursor = item.end
    _append_text(parts, template.source[cursor:])
    return tuple(parts)


def _render_text(name: str, value: Any) -> str:
    if isinstance(value, str):
        return value
    if value is None or isinstance(value, (Mapping, list, tuple, set, bytes, bytearray)):
        raise PromptArgumentTypeError(
            f"prompt text argument {name!r} must be a string or scalar; use '| json' for structured values"
        )
    if isinstance(value, (bool, int, float)):
        return str(value)
    raise PromptArgumentTypeError(f"unsupported prompt text argument {name!r}: {type(value).__name__}")


def _render_images(name: str, value: Any) -> list[ImagePart]:
    values = (
        list(value)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, ImageValue))
        else [value]
    )
    out: list[ImagePart] = []
    for item in values:
        try:
            out.append(ImagePart(_image_value(item)))
        except (TypeError, ValueError) as exc:
            raise PromptArgumentTypeError(
                f"invalid image value for prompt argument {name!r}: {exc}"
            ) from exc
    return out


def _image_value(value: Any) -> ImageValue:
    if isinstance(value, ImageValue):
        return value
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        media_type = _detect_image_media_type(value)
        if not media_type:
            raise ValueError("binary image media type is unknown; pass ImageValue with media_type")
        return ImageValue(data=value, media_type=media_type)
    if isinstance(value, str):
        text = value.strip()
        if _DATA_URL.match(text) or text.startswith("https://") or text.startswith("http://"):
            return ImageValue(uri=text)
        raise ValueError("image string must be a data URL or HTTP(S) URL")
    raise TypeError(f"expected image bytes, URI, ImageValue, or sequence; got {type(value).__name__}")


def _detect_image_media_type(value: bytes) -> str:
    if value.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if value.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if value.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if value.startswith(b"RIFF") and value[8:12] == b"WEBP":
        return "image/webp"
    return ""


def _append_text(parts: list[OperatorLLMPart], text: str) -> None:
    if not text:
        return
    if parts and isinstance(parts[-1], TextPart):
        parts[-1] = TextPart(parts[-1].text + text)
    else:
        parts.append(TextPart(text))
