"""Host-owned hard-timeout policy for isolated generated-code operations."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class TimeoutResolution:
    requested_seconds: int | None
    effective_seconds: int
    host_limit_seconds: int
    deadline_limited: bool


@dataclass(frozen=True)
class ExecutionPolicy:
    pipeline_timeout_seconds: int = 7200
    max_pipeline_timeout_seconds: int = 7200
    termination_grace_seconds: int = 2

    def resolve(
        self, params: Mapping[str, Any], *, deadline_at: float | None = None,
    ) -> TimeoutResolution:
        default = self.pipeline_timeout_seconds
        host_limit = self.max_pipeline_timeout_seconds
        raw_requested = params.get("timeout_seconds")
        requested = None if raw_requested is None else _positive_int(raw_requested)
        effective = min(requested or default, host_limit)
        deadline_limited = False
        if deadline_at is not None:
            remaining = max(1, int(float(deadline_at) - time.time()))
            deadline_limited = remaining < effective
            effective = min(effective, remaining)
        return TimeoutResolution(
            requested, max(1, effective), host_limit, deadline_limited,
        )

    def timeout(self, params: Mapping[str, Any]) -> int:
        return self.resolve(params).effective_seconds


def _positive_int(value: Any) -> int:
    normalized = int(value)
    if normalized <= 0:
        raise ValueError("timeout_seconds must be positive")
    return normalized


DEFAULT_EXECUTION_POLICY = ExecutionPolicy()

__all__ = [
    "DEFAULT_EXECUTION_POLICY", "ExecutionPolicy", "TimeoutResolution",
]
