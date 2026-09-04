"""Best-effort, backend-neutral Pipeline execution observations."""
from __future__ import annotations

import json
import math
from typing import Any

from .contracts import PipelineRunObservation, PipelineRunObserver

_MAX_LINE_BYTES = 64 * 1024
_ALLOWED_EVENTS = {
    "demiflow.pipeline.started", "demiflow.pipeline.completed", "demiflow.pipeline.failed",
    "demiflow.dataset.action_started", "demiflow.dataset.action_progress",
    "demiflow.dataset.action_completed", "demiflow.dataset.action_failed",
    "demiflow.datasource.tasks_planning_started",
    "demiflow.datasource.tasks_planning_completed", "demiflow.datasource.first_block",
    "demiflow.datasource.task_started", "demiflow.datasource.task_completed",
    "demiflow.datasource.task_failed", "demiflow.datasource.read_failed",
}

def emit_observation(observer: PipelineRunObserver | None, value: PipelineRunObservation) -> None:
    """Deliver diagnostics without allowing a consumer to affect execution."""
    if observer is None:
        return
    try:
        observer(value)
    except Exception:
        return


def parse_pipeline_log_observation(line: bytes | str, *, elapsed_ms: int) -> PipelineRunObservation | None:
    raw = line if isinstance(line, bytes) else line.encode("utf-8", errors="replace")
    if len(raw) > _MAX_LINE_BYTES:
        return None
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    event = value.get("event")
    logger = value.get("logger")
    if event not in _ALLOWED_EVENTS or not isinstance(logger, str) or not logger.startswith("demiflow"):
        return None
    kind = "heartbeat" if event.endswith("action_progress") else "event"
    phase = "running_driver"
    rows = _number(value.get("rows"), integer=True)
    batches = _number(value.get("batches"), integer=True)
    duration = _number(value.get("duration_ms"), integer=True)
    return PipelineRunObservation(
        kind=kind, phase=phase, elapsed_ms=max(0, int(elapsed_ms)), event=event,
        action=_safe_text(value.get("action")), action_phase=_safe_text(value.get("phase")),
        rows=rows, batches=batches, duration_ms=duration,
    )


def _number(value: Any, *, integer: bool) -> int | None:
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(float(value)) or value < 0:
        return None
    return int(value) if integer else value


def _safe_text(value: Any) -> str:
    return str(value)[:128] if isinstance(value, str) else ""

__all__ = ["emit_observation", "parse_pipeline_log_observation"]
