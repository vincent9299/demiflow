"""Physical Dataset execution protocol."""
from __future__ import annotations

from collections.abc import Iterator
from demiflow.data.execution_contract import InputSpec
from demiflow.execution.sources.materialized_input import build_input_handle

from typing import Any, TYPE_CHECKING

from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import SourcePlan
from demiflow.data.stats import ExecutionMetadata

if TYPE_CHECKING:
    from demiflow.data.execution_contract import ActionSpec, InputSpec


class DatasetExecutor:
    NAME = ""

    @property
    def declaration_scope(self):
        """Immutable composition identity shared by independently created public readers."""
        return (self.NAME, self.resource_root)

    def execute(self, request: ActionSpec):
        """Run one SDK action through the execution-owned lifecycle."""
        from demiflow.execution.executors.dataset_runtime import execute_action
        return execute_action(self, request)

    def prepare_input(self, request: InputSpec):
        if request.kind == 'public_items':
            handle, _ = prepare_public_items(request, select_executor=lambda: self)
            return handle
        if request.kind == 'items':
            if not isinstance(request.value, list) or isinstance(request.value, Iterator):
                raise TypeError('from_items requires a materialized list')
            value = list(request.value)
        else:
            value = request.value
        return build_input_handle(value, format=request.kind,
                                  override_num_blocks=request.override_num_blocks)

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


def prepare_public_items(request: InputSpec, *, select_executor):
    """Keep validation/capture before default-host assembly, as in the public reader.

    The selector is the reader's private assembly callback, never an ActionSpec
    field or a caller-controlled validation policy. It runs only after checked
    admission, preserving the original validation and capture ordering.
    """
    from demiflow.execution.checked import action, current
    from demiflow.data.runtime_contract import check_size

    if request.kind != 'public_items':
        raise ValueError('prepare_public_items requires the public items request')
    items = request.value
    if current() is not None:
        if type(items) is not list or not 1 <= len(items) <= 128:
            raise ValueError('Managed from_items requires 1..128 explicit summary rows')
        check_size(items, 256 * 1024)
    with action('from_items', None, None, arguments={'items': items}) as checked:
        executor = select_executor()
        handle = executor.prepare_input(InputSpec('items', items, request.override_num_blocks))
        checked.materialized(handle)
        return handle, executor
