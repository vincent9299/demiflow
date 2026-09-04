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


class Datasource(ABC):
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
