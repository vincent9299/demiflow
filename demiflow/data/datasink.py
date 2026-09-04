"""Backend-neutral datasink contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional
import json


@dataclass(frozen=True)
class WriteContext:
    task_index: int = 0


@dataclass(frozen=True)
class WriteResult:
    written_rows: Optional[int]
    failed_rows: Optional[int] = 0
    blocks_written: Optional[int] = 0
    target: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


def validate_write_result(result: WriteResult) -> WriteResult:
    counts = (result.written_rows, result.failed_rows, result.blocks_written)
    if any(
        value is not None
        and (isinstance(value, bool) or not isinstance(value, int) or value < 0)
        for value in counts
    ):
        raise ValueError("WriteResult counts must be non-negative integers or None")
    if not isinstance(result.target, str):
        raise TypeError("WriteResult target must be a string")
    if not isinstance(result.metadata, Mapping):
        raise TypeError("WriteResult metadata must be a mapping")
    try:
        json.dumps(dict(result.metadata), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise TypeError("WriteResult metadata must contain strict JSON values") from exc
    return result


class Datasink(ABC):
    def on_write_start(self, schema: Any = None) -> None:
        """Receive the input schema once before worker write calls begin."""
        pass

    @abstractmethod
    def write(self, blocks: Iterable[Any], context: WriteContext) -> Any:
        """Write one iterable of backend-native blocks and return a backend-supported task result."""
        raise NotImplementedError

    def on_write_complete(self, result: WriteResult) -> None:
        """Receive the validated aggregate WriteResult after all write tasks commit."""
        pass

    def on_write_failed(self, error: Exception) -> None:
        """Receive the original write exception when the managed write fails."""
        pass
