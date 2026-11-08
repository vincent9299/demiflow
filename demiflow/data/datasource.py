"""Backend-neutral datasource contracts aligned with Ray Data concepts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Tuple


@dataclass(frozen=True)
class BlockMetadata:
    num_rows: Optional[int] = None
    size_bytes: Optional[int] = None
    input_files: Optional[Tuple[str, ...]] = None


@dataclass(frozen=True)
class ReadTask:
    read_fn: Callable[[], Iterable[Any]]
    metadata: BlockMetadata = BlockMetadata()
    schema: Any = None
    per_task_row_limit: Optional[int] = None
    source_id: str | None = None
    partition_id: str | None = None
    start_position: int = 0


class Datasource(ABC):
    def snapshot_identity(self) -> Any:
        """Return immutable JSON source facts, or None for bounded capture.

        This declaration performs no acquisition or reads. Mutable/one-shot
        sources are captured by the existing executor before downstream effects.
        """
        return None

    def acknowledge(self, position: Any, context: Any) -> None:
        """Idempotent source ACK, called only after the runtime confirms progress.

        Ordinary replayable sources have no delivery obligation. Queue adapters
        validate their delivery token/epoch atomically when overriding this.
        """
        return None

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Optionally estimate total in-memory source bytes without reading data rows."""
        return None

    @abstractmethod
    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Any = None,
    ) -> list[ReadTask]:
        """Plan backend-neutral ReadTask values without reading data rows; each read function streams blocks when executed."""
        raise NotImplementedError
