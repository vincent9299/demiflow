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
from typing import Any

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

    def materialize_graph(self, graph_spec, *, host_id='local', resource_root=None, control_timeout_s=120):
        from .graph import materialize_graph
        return materialize_graph(self, graph_spec, host_id=host_id, resource_root=resource_root,
                                 control_timeout_s=control_timeout_s)

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
