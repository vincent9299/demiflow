"""Backend-neutral execution metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Tuple


@dataclass(frozen=True)
class StageStats:
    name: str
    rows_input: Optional[int] = None
    rows_output: Optional[int] = None
    elapsed_seconds: Optional[float] = None


@dataclass(frozen=True)
class ExecutionMetadata:
    source_name: str
    stages: Tuple[StageStats, ...] = ()
    rows_read: Optional[int] = None
    rows_output: Optional[int] = None
    bytes_read: Optional[int] = None
    blocks_output: Optional[int] = None
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
