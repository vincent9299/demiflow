"""Ray Core provider adapter for the unified v3 dataflow engine.

Ray hosts resources and workers only (I02, YARN-like). Every public terminal
forwards one declaration through make_action_request/run_dataset_action into the
single DataflowExecution engine shared with Local; dataflow semantics, scheduling,
handoff, commit and recovery all belong to that engine, never to Ray Data. No
Ray Data dataset is created or imported here, and no live Ray cluster is probed at
plan time: resource admission comes from the unified runtime policy.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

from ...data.constraints import (
    analyze_source_constraints, analyze_stage_work_units, stage_parallelism_caps,
)
from ...data.datasink import Datasink, WriteResult as DemiflowWriteResult
from ...data.plan import LogicalPlan, reject_streaming_map_options
from ...data.sources import LanceSource, MaterializedSource, SourcePlan
from ...data.stats import ExecutionMetadata
from ...errors import PhysicalPlanningError, UnsupportedSourceError
from ...planning import BackendResourceSnapshot, ResourceBundle, plan_action
from ...planning.dataflow_policy import resolve_runtime_policy
from ...planning.policy import parse_platform_planning_policy
from ...planning.traits import requires_native_ray, terminal_traits
from ..contracts.dataflow import ActionProgram
from ..dataflow_api import make_action_request, run_dataset_action
from ..dataflow_inputs import build_input_handle
from .base import DatasetExecutor


class RayDatasetExecutor(DatasetExecutor):
    NAME = "ray"
    PROVIDER = "ray_core"

    def __init__(
        self, *, aggregate_state_max_bytes: int = 8 * 1024 * 1024,
        resource_root=None, planning_policy=None, candidate_execution=None,
        unified_chain: bool = False, unified_graph: bool = False,
    ) -> None:
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
        self._handles: list[Any] = []
        self._results: list[Any] = []
        self._closed = False

    def _execute(
        self, source: SourcePlan, plan: LogicalPlan, kind: str,
        arguments: dict[str, Any], *, sink: Datasink | None = None,
        programs: tuple[ActionProgram, ...] = (),
    ) -> Any:
        # Single wiring seam for the modify cards: one declaration into the same
        # DataflowExecution engine Local uses, never a Ray Data dataset.
        request = make_action_request(
            self, source, plan, kind=kind, arguments=arguments,
            sink=sink, programs=programs,
        )
        result = run_dataset_action(request)
        self._results.append(result)
        if callable(getattr(result, "release", None)) or callable(getattr(result, "close", None)):
            self._handles.append(result)
        return result

    def operator_llm_usage(self) -> dict[str, int]:
        totals: dict[str, int] = {}
        for result in self._results:
            usage = getattr(result, "operator_llm_usage", None)
            facts = usage() if callable(usage) else getattr(result, "llm_usage", None)
            if isinstance(facts, dict):
                for key, value in facts.items():
                    totals[key] = totals.get(key, 0) + int(value)
        return totals

    def from_items(self, items: list[Any], *, override_num_blocks: int | None = None) -> Any:
        if not isinstance(items, list):
            raise TypeError("from_items requires a list")
        _validate_block_hint(override_num_blocks)
        return build_input_handle(list(items), format="items", bounds=self._bounds)

    def from_arrow(self, tables: Any) -> Any:
        return build_input_handle(tables, format="arrow", bounds=self._bounds)

    def from_numpy(self, arrays: Any) -> Any:
        return build_input_handle(arrays, format="numpy", bounds=self._bounds)

    def from_pandas(
        self, frames: Any, *, override_num_blocks: int | None = None,
    ) -> Any:
        _validate_block_hint(override_num_blocks)
        return build_input_handle(frames, format="pandas", bounds=self._bounds)

    def plan(
        self, source, plan, action_kind, *, terminal_category="action",
        terminal_native_options=None, source_parallelism_cap=None,
        terminal_parallelism_cap=None,
    ):
        reject_streaming_map_options(plan, 'ray')
        native = (
            getattr(source, "native_options", None) is not None
            or any(getattr(operation, "native_options", None) is not None for operation in plan.operations)
            or terminal_native_options is not None
        )
        if native and (self._candidate_execution is None or self._candidate_execution.mode != "native"):
            raise ValueError("Native options require Pipeline execution mode native")
        if self._candidate_execution is not None and self._candidate_execution.mode == "portable" and any(requires_native_ray(operation) for operation in plan.operations):
            raise PhysicalPlanningError("portable_operation_requires_ray_affinity", responsibility="candidate")
        # Unified v3 admission (same engine as Local); no Ray Data lowering.
        make_action_request(
            self, source, plan, kind=action_kind,
            arguments={"terminal_category": terminal_category},
        )
        # Resource snapshot describes the managed Ray Core pool from the unified
        # policy, not a live ray.nodes() probe; liveness is the ResourceManager's.
        pool = self._planning_policy.reusable_worker.scale(max(1, self._policy.max_replicas))
        snapshot = BackendResourceSnapshot(
            "ray", (pool,),
            hashlib.sha256(
                f"{self._policy.provider}:{self._policy.store_id}:{self._planning_policy.digest}".encode()
            ).hexdigest(),
            analysis_complete=True,
        )
        marker = type("Terminal", (), {})()
        bounds = (*analyze_stage_work_units(source, plan), None)
        if isinstance(source, LanceSource):
            from ...lance.model import LanceScanSpec
            leading_limit = analyze_source_constraints(plan).row_limit
            if not isinstance(source.query, LanceScanSpec) or source.query.limit is not None or leading_limit is not None:
                source_parallelism_cap = 1
        caps = stage_parallelism_caps(plan, source_cap=source_parallelism_cap, terminal_cap=terminal_parallelism_cap)
        return plan_action(
            backend="ray", action_kind=action_kind, source=source,
            operations=plan.operations, terminal_node=marker,
            terminal_traits=terminal_traits(terminal_category),
            policy=self._planning_policy, snapshot=snapshot,
            work_units_by_stage=bounds, terminal_native_options=terminal_native_options,
            parallelism_caps_by_stage=caps,
        )

    def iter_rows(self, source: SourcePlan, plan: LogicalPlan) -> Iterable[dict[str, Any]]:
        return self._execute(source, plan, "iter_rows", {})

    def iter_batches(
        self, source: SourcePlan, plan: LogicalPlan, *, prefetch_batches: int = 1,
        batch_size: int | None = 256, batch_format: str | None = "default", drop_last: bool = False,
    ) -> Iterable[Any]:
        return self._execute(source, plan, "iter_batches", {
            "prefetch_batches": prefetch_batches, "batch_size": batch_size,
            "batch_format": batch_format, "drop_last": drop_last,
        })

    def materialize(self, source: SourcePlan, plan: LogicalPlan) -> Any:
        return self._execute(source, plan, "materialize", {})

    def materialize_stream(self, dataset, *, source=None, plan=None):
        selected_source = source if source is not None else dataset._source
        selected_plan = plan if plan is not None else dataset._plan
        return self._execute(selected_source, selected_plan, "materialize", {})

    def materialize_graph(self, graph_spec):
        # Translate the explicit fan-out declaration to the standard v3 entry; the
        # full graph_spec->DataflowGraph lowering is the F lane seam (F012/F018).
        source = graph_spec["source"]
        plan = graph_spec["ancestor"]["plan"]
        arguments: dict[str, Any] = {
            "routing": graph_spec.get("routing", "broadcast"),
            "exchange_mode": graph_spec.get("exchange_mode", "memory_incremental"),
        }
        for key in ("key_fields", "target_field"):
            if graph_spec.get(key) is not None:
                arguments[key] = graph_spec[key]
        return self._execute(source, plan, "materialize", arguments)

    def take(
        self, source: SourcePlan, plan: LogicalPlan, limit: int,
    ) -> list[dict[str, Any]]:
        return self._execute(source, plan, "take", {"limit": limit})

    def take_batch(
        self, source: SourcePlan, plan: LogicalPlan, batch_size: int,
        *, batch_format: str,
    ) -> Any:
        return self._execute(source, plan, "take_batch", {
            "batch_size": batch_size, "batch_format": batch_format,
        })

    def count(self, source: SourcePlan, plan: LogicalPlan) -> int:
        return self._execute(source, plan, "count", {})

    def aggregate(
        self, source: SourcePlan, plan: LogicalPlan,
        aggregates: tuple[Any, ...],
    ) -> Any:
        # Aggregate callables travel as process-in program declarations (C16), not
        # serialized arguments; the engine enforces the state budget via bounds.
        programs = tuple(
            ActionProgram(name=aggregate.name, kind="aggregate", value=aggregate)
            for aggregate in aggregates
        )
        return self._execute(
            source, plan, "aggregate",
            {"names": [aggregate.name for aggregate in aggregates]},
            programs=programs,
        )

    def schema(self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool = True) -> Any:
        return self._execute(source, plan, "schema", {"fetch_if_missing": fetch_if_missing})

    def columns(
        self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool,
    ) -> list[str] | None:
        return self._execute(source, plan, "columns", {"fetch_if_missing": fetch_if_missing})

    def size_bytes(self, source: SourcePlan, plan: LogicalPlan) -> int:
        return int(self._execute(source, plan, "size_bytes", {}))

    def num_blocks(self, source: SourcePlan, plan: LogicalPlan) -> int:
        if not isinstance(source, MaterializedSource) or not plan.is_empty:
            raise TypeError("num_blocks is only available on MaterializedDataset")
        return int(source.handle.num_blocks())

    def stats(self, source: SourcePlan, plan: LogicalPlan) -> str:
        for result in reversed(self._results):
            stats = getattr(result, "stats", None)
            if callable(stats):
                return str(stats())
        return f"Dataset({type(source).__name__}, operations={len(plan.operations)}, not executed)"

    def execution_metadata(self, source: SourcePlan, plan: LogicalPlan) -> ExecutionMetadata | None:
        for result in reversed(self._results):
            metadata = getattr(result, "execution_metadata", None)
            if callable(metadata):
                return metadata()
            if isinstance(metadata, ExecutionMetadata):
                return metadata
        return None

    def write_datasink(
        self, source: SourcePlan, plan: LogicalPlan, datasink: Datasink,
        *, native_options=None,
    ) -> DemiflowWriteResult:
        arguments: dict[str, Any] = {}
        if native_options is not None:
            arguments["native_options"] = native_options
        return self._execute(source, plan, "write_datasink", arguments, sink=datasink)

    def write_lance(self, source, plan, spec, *, native_options=None):
        arguments: dict[str, Any] = {}
        if native_options is not None:
            arguments["native_options"] = native_options
        programs = (ActionProgram(name="write_lance", kind="sink", value=spec),)
        return self._execute(source, plan, "write_lance", arguments, programs=programs)

    def write_file(
        self, source: SourcePlan, plan: LogicalPlan, format_name: str,
        path: str, options: dict[str, Any],
    ) -> DemiflowWriteResult:
        options = dict(options)
        native_options = options.pop("_demiflow_native_options", None)
        arguments: dict[str, Any] = {
            "format": format_name, "path": path, "options": options,
        }
        if native_options is not None:
            arguments["native_options"] = native_options
        return self._execute(source, plan, "write_file", arguments)

    def close(self) -> None:
        # Release only the handles this executor owns; other runs keep their pins.
        errors: list[BaseException] = []
        for handle in self._handles:
            release = getattr(handle, "release", None)
            close = getattr(handle, "close", None)
            try:
                if callable(release):
                    release()
                elif callable(close):
                    close()
            except BaseException as exc:  # noqa: BLE001 - aggregate, never swallow
                errors.append(exc)
        self._handles.clear()
        self._results.clear()
        self._closed = True
        if errors:
            raise errors[0]


def _validate_block_hint(override_num_blocks: int | None) -> None:
    if override_num_blocks is None:
        return
    if not isinstance(override_num_blocks, int) or isinstance(override_num_blocks, bool) or override_num_blocks < 1:
        raise ValueError("override_num_blocks must be a positive integer or None")