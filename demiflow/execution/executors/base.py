"""Physical Dataset execution protocol."""
from __future__ import annotations

from typing import Any, TYPE_CHECKING

from ...data.plan import LogicalPlan
from ...data.sources import SourcePlan
from ...data.stats import ExecutionMetadata

if TYPE_CHECKING:
    from ...data.execution_contract import ActionSpec, InputSpec


class DatasetExecutor:
    NAME = ""

    def execute(self, request: ActionSpec):
        """Run one SDK action through the execution-owned lifecycle."""
        from ..dataset_runtime import execute_action
        return execute_action(self, request)

    def prepare_input(self, request: InputSpec):
        """Construct an SDK input without opening an action scope."""
        from ..input_runtime import prepare_input
        return prepare_input(self, request)

    def plan(
        self, source: SourcePlan, plan: LogicalPlan, action_kind: str, *,
        terminal_category: str = "action", terminal_native_options=None,
        source_parallelism_cap: int | None = None,
        terminal_parallelism_cap: int | None = None,
    ):
        raise NotImplementedError

    def operator_llm_usage(self) -> dict[str, int]:
        return {}

    def schema(
        self, source: SourcePlan, plan: LogicalPlan, *,
        fetch_if_missing: bool = True,
    ) -> Any:
        raise NotImplementedError

    def columns(
        self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool,
    ) -> list[str] | None:
        raise NotImplementedError

    def num_blocks(self, source: SourcePlan, plan: LogicalPlan) -> int:
        raise NotImplementedError

    def stats(self, source: SourcePlan, plan: LogicalPlan) -> str:
        raise NotImplementedError

    def execution_metadata(
        self, source: SourcePlan, plan: LogicalPlan,
    ) -> ExecutionMetadata | None:
        return None

    def close(self) -> None:
        pass
