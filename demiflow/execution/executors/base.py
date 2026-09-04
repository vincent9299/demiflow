"""Physical Dataset execution protocol."""
from __future__ import annotations

from typing import Any, Iterable

from ...data.datasink import Datasink, WriteResult
from ...data.plan import LogicalPlan
from ...data.sources import SourcePlan
from ...data.stats import ExecutionMetadata
from ...lance.model import LanceWriteReceipt, LanceWriteSpec


class DatasetExecutor:
    NAME = ""

    def plan(
        self, source: SourcePlan, plan: LogicalPlan, action_kind: str, *,
        terminal_category: str = "action", terminal_native_options=None,
        source_parallelism_cap: int | None = None,
        terminal_parallelism_cap: int | None = None,
    ):
        raise NotImplementedError

    def operator_llm_usage(self) -> dict[str, int]:
        return {}

    def from_items(
        self, items: list[Any], *, override_num_blocks: int | None = None,
    ) -> Any:
        raise NotImplementedError

    def from_arrow(self, tables: Any) -> Any:
        raise NotImplementedError

    def from_numpy(self, arrays: Any) -> Any:
        raise NotImplementedError

    def from_pandas(
        self, frames: Any, *, override_num_blocks: int | None = None,
    ) -> Any:
        raise NotImplementedError

    def iter_rows(
        self, source: SourcePlan, plan: LogicalPlan,
    ) -> Iterable[dict[str, Any]]:
        raise NotImplementedError

    def iter_batches(
        self, source: SourcePlan, plan: LogicalPlan, *,
        prefetch_batches: int = 1, batch_size: int | None = 256,
        batch_format: str | None = "default", drop_last: bool = False,
    ) -> Iterable[Any]:
        raise NotImplementedError

    def materialize(self, source: SourcePlan, plan: LogicalPlan) -> Any:
        raise NotImplementedError

    def take(
        self, source: SourcePlan, plan: LogicalPlan, limit: int,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def take_batch(
        self, source: SourcePlan, plan: LogicalPlan, batch_size: int,
        *, batch_format: str,
    ) -> Any:
        raise NotImplementedError

    def count(self, source: SourcePlan, plan: LogicalPlan) -> int:
        return sum(1 for _ in self.iter_rows(source, plan))

    def aggregate(
        self, source: SourcePlan, plan: LogicalPlan,
        aggregates: tuple[Any, ...],
    ) -> Any:
        raise NotImplementedError

    def schema(
        self, source: SourcePlan, plan: LogicalPlan, *,
        fetch_if_missing: bool = True,
    ) -> Any:
        raise NotImplementedError

    def columns(
        self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool,
    ) -> list[str] | None:
        raise NotImplementedError

    def size_bytes(self, source: SourcePlan, plan: LogicalPlan) -> int:
        raise NotImplementedError

    def num_blocks(self, source: SourcePlan, plan: LogicalPlan) -> int:
        raise NotImplementedError

    def stats(self, source: SourcePlan, plan: LogicalPlan) -> str:
        raise NotImplementedError

    def execution_metadata(
        self, source: SourcePlan, plan: LogicalPlan,
    ) -> ExecutionMetadata | None:
        return None

    def write_datasink(
        self, source: SourcePlan, plan: LogicalPlan, datasink: Datasink,
        *, native_options=None,
    ) -> WriteResult:
        raise NotImplementedError

    def write_lance(
        self, source: SourcePlan, plan: LogicalPlan, spec: LanceWriteSpec,
        *, native_options=None,
    ) -> LanceWriteReceipt:
        raise NotImplementedError

    def write_file(
        self, source: SourcePlan, plan: LogicalPlan, format_name: str,
        path: str, options: dict[str, Any],
    ) -> WriteResult:
        raise NotImplementedError

    def close(self) -> None:
        pass
