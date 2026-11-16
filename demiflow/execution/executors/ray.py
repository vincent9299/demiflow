"""Ray Core provider adapter for the unified v3 dataflow engine.

Ray hosts resources and workers only (I02, YARN-like). Every public terminal
forwards one declaration through make_action_request/run_dataset_action into the
single DataflowExecution engine shared with Local; dataflow semantics, scheduling,
handoff, commit and recovery all belong to that engine, never to Ray Data. No
Ray Data dataset is created or imported here. This mirrors LocalDatasetExecutor's
provider/return-value adapter form; the only intended difference is provider
'ray_core' and acceptance of Ray native options.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ...data.datasink import Datasink, WriteResult as DemiflowWriteResult
from ...data.plan import LogicalPlan, reject_streaming_map_options
from ...data.sources import MaterializedSource, SourcePlan
from ...data.stats import ExecutionMetadata
from ...errors import UnsupportedSourceError
from ...planning.dataflow_policy import resolve_runtime_policy
from ...planning.policy import parse_platform_planning_policy
from .base import DatasetExecutor


class RayDatasetExecutor(DatasetExecutor):
    NAME = "ray"
    PROVIDER = "ray_core"

    def __init__(
        self, *, aggregate_state_max_bytes: int = 8 * 1024 * 1024,
        resource_root=None, planning_policy=None, candidate_execution=None,
        unified_chain: bool = False, unified_graph: bool = False,
        workers: int | None = None, native_resources=None,
    ) -> None:
        if workers is not None and (type(workers) is not int or workers < 1):
            raise ValueError('workers must be a positive integer or None')
        from ..native_resources import parse_native_resources
        self._workers = workers
        self._native_resources = parse_native_resources(native_resources)
        self._aggregate_state_max_bytes = max(1, int(aggregate_state_max_bytes))
        self.resource_root = Path(resource_root or ".").resolve()
        self._planning_policy = planning_policy or parse_platform_planning_policy(None)
        self._candidate_execution = candidate_execution
        # Legacy unified switches are compatibility validation only; they never
        # select a separate execution engine (I01/I02).
        self._unified_chain = bool(unified_chain)
        self._unified_graph = bool(unified_graph)
        self._policy, self._bounds = resolve_runtime_policy(
            {}, {}, provider=self.PROVIDER,
        )
        self._prompt_coordinators: dict[str, Any] = {}
        self._hosted_scopes: list[Any] = []
        self._last_metadata: ExecutionMetadata | None = None
        self._run_ref = None
        self._closed = False

    def operator_llm_usage(self) -> dict[str, int]:
        from ..dataflow_usage import executor_prompt_usage
        return executor_prompt_usage(self)

    def from_items(self, items: list[Any], *, override_num_blocks: int | None = None) -> Any:
        if not isinstance(items, list):
            raise TypeError("from_items requires a materialized list")
        from ..dataflow_inputs import build_input_handle
        return build_input_handle(items, format="items", override_num_blocks=override_num_blocks)

    def from_arrow(self, tables: Any) -> Any:
        from ..dataflow_inputs import build_input_handle
        return build_input_handle(tables, format="arrow")

    def from_numpy(self, arrays: Any) -> Any:
        from ..dataflow_inputs import build_input_handle
        return build_input_handle(arrays, format="numpy")

    def from_pandas(
        self, frames: Any, *, override_num_blocks: int | None = None,
    ) -> Any:
        from ..dataflow_inputs import build_input_handle
        return build_input_handle(frames, format="pandas", override_num_blocks=override_num_blocks)

    def plan(
        self, source, plan, action_kind, *, terminal_category="action",
        terminal_native_options=None, source_parallelism_cap=None,
        terminal_parallelism_cap=None,
    ):
        reject_streaming_map_options(plan, 'ray')
        from ..dataflow_api import explain_dataset
        options: dict[str, Any] = {
            'action_kind': action_kind,
            'terminal_category': terminal_category,
        }
        if terminal_native_options is not None:
            options['terminal_native_options'] = terminal_native_options
        if source_parallelism_cap is not None:
            options['source_parallelism_cap'] = source_parallelism_cap
        if terminal_parallelism_cap is not None:
            options['terminal_parallelism_cap'] = terminal_parallelism_cap
        return explain_dataset(source, plan, provider=self.PROVIDER, options=options)

    def _run(
        self, source: SourcePlan, plan: LogicalPlan, kind: str,
        arguments: dict[str, Any], *, sink: Datasink | None = None,
        programs: tuple = (),
    ) -> Any:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(
            self, source, plan, kind=kind, arguments=arguments,
            sink=sink, programs=programs,
        )
        result = run_dataset_action(request)
        self._last_metadata = ExecutionMetadata(source_name=type(source).__name__)
        return result

    def iter_rows(self, source: SourcePlan, plan: LogicalPlan) -> Iterable[dict[str, Any]]:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind="iter_rows", arguments={})
        return run_dataset_action(request)

    def iter_batches(
        self, source: SourcePlan, plan: LogicalPlan, *, prefetch_batches: int = 1,
        batch_size: int | None = 256, batch_format: str | None = "default", drop_last: bool = False,
    ) -> Iterable[Any]:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind="iter_batches", arguments={
            "prefetch_batches": prefetch_batches, "batch_size": batch_size,
            "batch_format": batch_format, "drop_last": drop_last,
        })
        return run_dataset_action(request)

    def materialize(self, source: SourcePlan, plan: LogicalPlan) -> Any:
        return self._run(source, plan, "materialize", {})

    def materialize_stream(self, dataset, *, source=None, plan=None):
        selected_source = dataset._source if source is None else source
        selected_plan = dataset._plan if plan is None else plan
        return self._run(selected_source, selected_plan, "materialize", {})

    def materialize_graph(self, graph_spec):
        # The explicit fan-out graph_spec is translated to a standard v3 request
        # and forwarded to the same execute_action entry (F lane seam, parallel to
        # Local run_local_graph); there is no independent graph driver here.
        source = graph_spec["source"]
        plan = graph_spec["ancestor"]["plan"]
        arguments: dict[str, Any] = {
            "routing": graph_spec.get("routing", "broadcast"),
            "exchange_mode": graph_spec.get("exchange_mode", "memory_incremental"),
        }
        for key in ("key_fields", "target_field"):
            if graph_spec.get(key) is not None:
                arguments[key] = graph_spec[key]
        return self._run(source, plan, "materialize", arguments)

    def take(
        self, source: SourcePlan, plan: LogicalPlan, limit: int,
    ) -> list[dict[str, Any]]:
        return self._run(source, plan, "take", {"limit": limit})

    def take_batch(
        self, source: SourcePlan, plan: LogicalPlan, batch_size: int,
        *, batch_format: str,
    ) -> Any:
        return self._run(source, plan, "take_batch", {
            "batch_size": batch_size, "batch_format": batch_format,
        })

    def count(self, source: SourcePlan, plan: LogicalPlan) -> int:
        return self._run(source, plan, "count", {})

    def aggregate(
        self, source: SourcePlan, plan: LogicalPlan,
        aggregates: tuple[Any, ...],
    ) -> Any:
        from ..contracts.dataflow import ActionProgram
        programs = tuple(
            ActionProgram(name=aggregate.name, kind="aggregate", value=aggregate)
            for aggregate in aggregates
        )
        return self._run(
            source, plan, "aggregate",
            {"names": [aggregate.name for aggregate in aggregates]},
            programs=programs,
        )

    def schema(self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool = True) -> Any:
        if not fetch_if_missing:
            return None
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind="schema",
                                      arguments={"fetch_if_missing": fetch_if_missing})
        return run_dataset_action(request)

    def columns(
        self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool,
    ) -> list[str] | None:
        if not fetch_if_missing:
            return None
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind="columns",
                                      arguments={"fetch_if_missing": fetch_if_missing})
        return run_dataset_action(request)

    def size_bytes(self, source: SourcePlan, plan: LogicalPlan) -> int:
        return int(self._run(source, plan, "size_bytes", {}))

    def num_blocks(self, source: SourcePlan, plan: LogicalPlan) -> int:
        if not isinstance(source, MaterializedSource) or not plan.is_empty:
            raise TypeError("num_blocks is only available on MaterializedDataset")
        handle = source.handle
        num_blocks = getattr(handle, "num_blocks", None)
        if num_blocks is None:
            raise UnsupportedSourceError("materialized source belongs to another backend")
        return int(num_blocks())

    def stats(self, source: SourcePlan, plan: LogicalPlan) -> str:
        if self._last_metadata is None:
            return f"Dataset({type(source).__name__}, operations={len(plan.operations)}, not executed)"
        return str(self._last_metadata)

    def execution_metadata(self, source: SourcePlan, plan: LogicalPlan) -> ExecutionMetadata | None:
        from dataclasses import replace
        if self._last_metadata is not None:
            return replace(self._last_metadata, run_ref=self._run_ref)
        return None

    def write_datasink(
        self, source: SourcePlan, plan: LogicalPlan, datasink: Datasink,
        *, native_options=None,
    ) -> DemiflowWriteResult:
        arguments: dict[str, Any] = {}
        if native_options is not None:
            arguments["native_options"] = native_options
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind="write_datasink",
                                      arguments=arguments, sink=datasink)
        return run_dataset_action(request)

    def write_lance(self, source, plan, spec, *, native_options=None):
        from ..contracts.dataflow import ActionProgram
        arguments: dict[str, Any] = {}
        if native_options is not None:
            arguments["native_options"] = native_options
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(
            self, source, plan, kind="write_lance", arguments=arguments,
            programs=(ActionProgram(name="write_lance", kind="sink", value=spec),),
        )
        return run_dataset_action(request)

    def write_file(
        self, source: SourcePlan, plan: LogicalPlan, format_name: str,
        path: str, options: dict[str, Any],
    ) -> DemiflowWriteResult:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind="write_file", arguments={
            "format": format_name, "path": path, "options": dict(options),
        })
        return run_dataset_action(request)

    def close(self) -> None:
        # Release only this executor's hosted resource scope; unified ActionHandles
        # are owned by their callers and other runs' pins are never touched here.
        errors: list[BaseException] = []
        for scope in self._hosted_scopes:
            release = getattr(scope, "release", None)
            close = getattr(scope, "close", None)
            try:
                if callable(release):
                    release()
                elif callable(close):
                    close()
            except BaseException as exc:  # noqa: BLE001 - aggregate, never swallow
                errors.append(exc)
        self._hosted_scopes.clear()
        self._closed = True
        if errors:
            raise errors[0]
