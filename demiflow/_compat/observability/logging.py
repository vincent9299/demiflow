"""Small structured logging helpers shared across demiurge subprojects.

Modules should still create normal stdlib loggers with
``logging.getLogger(__name__)``. This module only centralizes formatting and a
tiny event/span convention so diagnostic logs stay consistent.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
import time
from typing import Any, Dict, Iterator, Optional, TextIO

_FIELDS_ATTR = "_demiurge_fields"
_DEFAULT_MAX_FIELD_CHARS = 1000


class TextLogFormatter(logging.Formatter):
    """Human-readable formatter that appends structured fields."""

    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        fields = getattr(record, _FIELDS_ATTR, None)
        if not fields:
            return line
        return f"{line} {_format_text_fields(fields)}"


class JsonLogFormatter(logging.Formatter):
    """JSONL formatter for machine-readable logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        fields = getattr(record, _FIELDS_ATTR, None)
        if isinstance(fields, dict):
            payload.update(fields)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(
    *,
    level: Optional[str] = None,
    json_format: Optional[bool] = None,
    stream: Optional[TextIO] = None,
    force: bool = False,
) -> None:
    """Configure root logging for CLI/scripts.

    Environment variables:
    - ``DEMIURGE_LOG_LEVEL``: DEBUG / INFO / WARNING / ERROR (default WARNING).
    - ``DEMIURGE_LOG_FORMAT``: ``json`` for JSONL, otherwise text.
    - ``DEMIURGE_LOG_JSON``: truthy value also enables JSONL.
    """

    resolved_level = _resolve_level(level or os.getenv("DEMIURGE_LOG_LEVEL") or "WARNING")
    if json_format is None:
        json_format = _env_bool("DEMIURGE_LOG_JSON") or (
            os.getenv("DEMIURGE_LOG_FORMAT", "").strip().lower() == "json"
        )

    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonLogFormatter() if json_format else TextLogFormatter())
    logging.basicConfig(level=resolved_level, handlers=[handler], force=force)


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """Emit a structured event through a normal stdlib logger."""

    if not _can_emit(logger, level):
        return
    payload = _sanitize_fields({"event": event, **fields})
    logger.log(level, event, extra={_FIELDS_ATTR: payload})


@contextlib.contextmanager
def log_span(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> Iterator[None]:
    """Log ``event.start`` then ``event.ok`` / ``event.error`` with duration."""

    should_emit = _can_emit(logger, level)
    should_emit_error = _can_emit(logger, logging.ERROR)
    if not should_emit and not should_emit_error:
        yield
        return

    started = time.monotonic()
    if should_emit:
        log_event(logger, f"{event}.start", level=level, **fields)
    try:
        yield
    except Exception as exc:
        duration_ms = round((time.monotonic() - started) * 1000, 2)
        if should_emit_error:
            log_event(
                logger,
                f"{event}.error",
                level=logging.ERROR,
                **fields,
                duration_ms=duration_ms,
                error_type=exc.__class__.__name__,
                error=str(exc),
            )
        raise
    else:
        if should_emit:
            duration_ms = round((time.monotonic() - started) * 1000, 2)
            log_event(logger, f"{event}.ok", level=level, **fields, duration_ms=duration_ms)


def _can_emit(logger: logging.Logger, level: int) -> bool:
    # Avoid stdlib's lastResort handler surprising normal runs before a CLI/script
    # has intentionally configured logging.
    if not logger.isEnabledFor(level):
        return False
    return logger.hasHandlers() or logging.getLogger().hasHandlers()


def _resolve_level(value: str) -> int:
    name = str(value or "").strip().upper()
    if not name:
        return logging.WARNING
    if name.isdigit():
        return int(name)
    return int(getattr(logging, name, logging.WARNING))


def _env_bool(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _sanitize_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    return {str(key): _sanitize_value(value) for key, value in fields.items()}


def _sanitize_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate(value)
    if isinstance(value, (list, tuple)):
        return [_sanitize_value(v) for v in value[:20]]
    if isinstance(value, dict):
        return {str(k): _sanitize_value(v) for k, v in list(value.items())[:40]}
    return _truncate(str(value))


def _truncate(value: str) -> str:
    if len(value) <= _DEFAULT_MAX_FIELD_CHARS:
        return value
    return value[:_DEFAULT_MAX_FIELD_CHARS] + "...<truncated>"


def _format_text_fields(fields: Dict[str, Any]) -> str:
    parts = []
    for key in sorted(fields):
        value = fields[key]
        if isinstance(value, str):
            text = json.dumps(value, ensure_ascii=False)
        else:
            text = json.dumps(value, ensure_ascii=False, default=str)
        parts.append(f"{key}={text}")
    return " ".join(parts)
