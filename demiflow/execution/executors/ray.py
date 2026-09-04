"""Ray Data compiler/executor for demiflow lazy plans."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

import ray

from ...data.datasink import Datasink, WriteResult as DemiflowWriteResult
from ...errors import (
    AggregateSerializationError, AggregateStateLimitExceeded,
    UnsupportedSourceError,
)
from ...data.plan import (
    AddColumnOp, BoundCallable, BoundMapOp, DropColumnsOp, FilterOp, FlatMapOp, LimitOp,
    LogicalPlan, MapBatchesOp, MapOp, OperatorLLMMapOp, RandomSampleOp,
    RandomShuffleOp, RandomizeBlockOrderOp, RenameColumnsOp, RepartitionOp,
    SelectColumnsOp, SortOp, StandardCallable,
)
from ...operator_llm.model import OperatorLLMUsage
from ...operator_llm.runtime import BoundOperatorLLMMap, OperatorLLMRuntime
from ...data.sources import DatasourceSource, LanceSource, FileSource, ItemsSource, MaterializedSource, RangeSource, SourcePlan, SqlSource
from ...data.stats import ExecutionMetadata
from .base import DatasetExecutor
from ...planning import BackendResourceSnapshot, ResourceBundle, plan_action
from ...planning.policy import parse_platform_planning_policy
from ...planning.traits import requires_native_ray, terminal_traits
from ...data.constraints import (
    analyze_source_constraints, analyze_stage_work_units,
    stage_parallelism_caps,
)
import hashlib


class _RayDatasourceAdapter:
    """Created lazily after Ray import; adapts demiflow ReadTasks to Ray."""

    def __new__(cls, datasource):
        from ray.data.datasource import Datasource

        class Adapter(Datasource):
            def estimate_inmemory_data_size(self):
                return datasource.estimate_inmemory_data_size()

            def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
                from ray.data.block import BlockMetadata
                from ray.data.datasource import ReadTask

                tasks = datasource.get_read_tasks(
                    parallelism,
                    per_task_row_limit=per_task_row_limit,
                    data_context=data_context,
                )
                return [
                    ReadTask(
                        _RayBlockReadFn(task.read_fn),
                        BlockMetadata(
                            num_rows=task.metadata.num_rows,
                            size_bytes=task.metadata.size_bytes,
                            exec_stats=None,
                            input_files=task.metadata.input_files,
                        ),
                    )
                    for task in tasks
                ]

        return Adapter()


class _RayBlockReadFn:
    """Normalize backend-neutral blocks to Ray-supported Arrow/Pandas blocks."""

    def __init__(self, read_fn):
        self._read_fn = read_fn

    def __call__(self):
        import pyarrow as pa

        for block in self._read_fn():
            if isinstance(block, pa.RecordBatch):
                yield pa.Table.from_batches([block])
                continue
            if isinstance(block, list):
                if not block:
                    continue
                if isinstance(block[0], Mapping):
                    yield pa.Table.from_pylist(block)
                    continue
            if isinstance(block, Mapping):
                yield pa.Table.from_pylist([dict(block)])
                continue
            yield block


class _RayLanceDatasource:
    def __new__(cls, query):
        from ray.data.datasource import Datasource
        class Adapter(Datasource):
            def estimate_inmemory_data_size(self): return None
            def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
                del data_context
                from ray.data.block import BlockMetadata
                from ray.data.datasource import ReadTask
                from ...lance.model import LanceScanSpec
                from ...lance.read import (
                    constrain_lance_query, iter_lance_batches,
                    iter_lance_partition_batches, plan_lance_scan_partitions,
                )
                if (
                    isinstance(query, LanceScanSpec) and query.limit is None
                    and per_task_row_limit is None
                ):
                    partitions = plan_lance_scan_partitions(
                        query, target_partitions=parallelism,
                    )
                    return [
                        ReadTask(
                            _RayBlockReadFn(
                                lambda partition=partition: iter_lance_partition_batches(
                                    partition, batch_size=256,
                                )
                            ),
                            BlockMetadata(
                                num_rows=None, size_bytes=None, exec_stats=None,
                                input_files=[],
                            ),
                        )
                        for partition in partitions
                    ]
                constrained=constrain_lance_query(query,per_task_row_limit)
                def read(): yield from iter_lance_batches(constrained,batch_size=256)
                return [ReadTask(_RayBlockReadFn(read),BlockMetadata(
                    num_rows=None,size_bytes=None,exec_stats=None,
                    input_files=[],
                ))]
        return Adapter()


class _RayDatasinkAdapter:
    def __new__(cls, datasink: Datasink):
        from ray.data.datasource import Datasink as RayDatasink

        class Adapter(RayDatasink):
            def on_write_start(self, schema=None):
                datasink.on_write_start(schema)

            def write(self, blocks, ctx):
                from ...data.datasink import WriteContext
                return datasink.write(blocks, WriteContext(task_index=ctx.task_idx))

            def on_write_complete(self, _write_result):
                # The demiflow result is finalized by RayDatasetExecutor after Ray combines tasks.
                return None

            def on_write_failed(self, error):
                # RayDatasetExecutor owns the public lifecycle and reports failure once.
                return None

        return Adapter()


class _RayLanceDirectWriteSink:
    """Execute one backend-planned direct write in one Ray task."""

    def __init__(self, spec) -> None:
        self.spec = spec
        self.receipt = None

    def ray_datasink(self):
        from ray.data.datasource import Datasink as RayDatasink
        spec, owner = self.spec, self

        class Adapter(RayDatasink):
            @property
            def supports_distributed_writes(self):
                return False

            def write(self, blocks, ctx):
                del ctx
                from ...lance.write import append_lance
                return append_lance(spec, blocks).to_dict()

            def on_write_complete(self, write_results):
                values = getattr(write_results, "write_returns", write_results)
                if len(values) != 1:
                    raise RuntimeError("Lance direct write requires one result")
                from ...lance.model import LanceWriteReceipt
                owner.receipt = LanceWriteReceipt.from_dict(dict(values[0]))

        return Adapter()


class _RayLanceFragmentWriteSink:
    """Write uncommitted fragments in Ray tasks and commit once on the driver."""

    def __init__(self, prepared) -> None:
        self.prepared = prepared
        self.receipt = None

    def ray_datasink(self):
        from ray.data.datasource import Datasink as RayDatasink
        prepared, owner = self.prepared, self

        class Adapter(RayDatasink):
            def write(self, blocks, ctx):
                from ...lance.write import write_lance_fragment
                return write_lance_fragment(
                    prepared, task_index=ctx.task_idx, batches=blocks,
                ).to_dict()

            def on_write_complete(self, write_result):
                from ...lance.model import _LanceFragmentReceipt
                from ...lance.write import commit_lance_append
                fragments = tuple(
                    _LanceFragmentReceipt.from_dict(dict(value))
                    for value in write_result.write_returns
                )
                owner.receipt = commit_lance_append(prepared, fragments)

        return Adapter()


class RayDatasetExecutor(DatasetExecutor):
    NAME = "ray"

    def __init__(
        self, *, aggregate_state_max_bytes: int = 8 * 1024 * 1024,
        prompt_packs=None, planning_policy=None, candidate_execution=None,
    ) -> None:
        self._aggregate_state_max_bytes = max(1, int(aggregate_state_max_bytes))
        self._last_native = None
        self._prompt_packs = dict(prompt_packs or {})
        self._planning_policy = planning_policy or parse_platform_planning_policy(None)
        self._candidate_execution = candidate_execution
        self._active_physical_plan = None
        self._operator_llm_coordinator = None
        if self._prompt_packs:
            self._ensure_ray()
            coordinator = _RayOperatorLLMCoordinatorProxy(
                _ray_operator_llm_coordinator_class().remote()
            )
            self._operator_llm_coordinator = coordinator

    def _ensure_ray(self) -> None:
        if not ray.is_initialized():
            raise RuntimeError(
                "RayDatasetExecutor requires an initialized Ray driver"
            )

    def operator_llm_usage(self) -> dict[str, int]:
        if self._operator_llm_coordinator is None:
            return {}
        return self._operator_llm_coordinator.usage().to_dict()

    def from_items(self, items: list[Any], *, override_num_blocks: int | None = None) -> Any:
        self._ensure_ray()
        return ray.data.from_items(items, override_num_blocks=override_num_blocks)

    def from_arrow(self, tables: Any) -> Any:
        self._ensure_ray()
        return ray.data.from_arrow(tables)

    def from_numpy(self, arrays: Any) -> Any:
        self._ensure_ray()
        return ray.data.from_numpy(arrays)

    def from_pandas(
        self, frames: Any, *, override_num_blocks: int | None = None,
    ) -> Any:
        self._ensure_ray()
        return ray.data.from_pandas(
            frames, override_num_blocks=override_num_blocks,
        )

    def plan(
        self, source, plan, action_kind, *, terminal_category="action",
        terminal_native_options=None, source_parallelism_cap=None,
        terminal_parallelism_cap=None,
    ):
        self._ensure_ray()
        native=(getattr(source,"native_options",None) is not None or any(getattr(operation,"native_options",None) is not None for operation in plan.operations) or terminal_native_options is not None)
        if native and (self._candidate_execution is None or self._candidate_execution.mode != "native"):
            raise ValueError("Native options require Pipeline execution mode native")
        if self._candidate_execution is not None and self._candidate_execution.mode == "portable" and any(requires_native_ray(operation) for operation in plan.operations):
            from ...errors import PhysicalPlanningError
            raise PhysicalPlanningError("portable_operation_requires_ray_affinity",responsibility="candidate")
        raw_nodes=[item.get("Resources",{}) for item in ray.nodes() if item.get("Alive")]
        nodes=tuple(ResourceBundle(cpu=float(item.get("CPU",0.0)),gpu=float(item.get("GPU",0.0)),memory_bytes=int(item.get("memory",0.0)),custom_resources={key:float(value) for key,value in item.items() if key not in {"CPU","GPU","memory","object_store_memory"} and not key.startswith("node:")}) for item in raw_nodes)
        snapshot=BackendResourceSnapshot("ray",nodes,hashlib.sha256(repr([sorted(item.items()) for item in raw_nodes]).encode()).hexdigest(),bool(nodes))
        marker=type("Terminal",(),{})()
        bounds=(*analyze_stage_work_units(source,plan),None)
        if isinstance(source,LanceSource):
            from ...lance.model import LanceScanSpec
            leading_limit=analyze_source_constraints(plan).row_limit
            if not isinstance(source.query,LanceScanSpec) or source.query.limit is not None or leading_limit is not None:
                source_parallelism_cap=1
        caps=stage_parallelism_caps(plan,source_cap=source_parallelism_cap,terminal_cap=terminal_parallelism_cap)
        return plan_action(backend="ray",action_kind=action_kind,source=source,operations=plan.operations,terminal_node=marker,terminal_traits=terminal_traits(terminal_category),policy=self._planning_policy,snapshot=snapshot,work_units_by_stage=bounds,terminal_native_options=terminal_native_options,parallelism_caps_by_stage=caps)

    def _build_source(self, source: SourcePlan):
        self._ensure_ray()
        if isinstance(source, MaterializedSource):
            return source.handle
        if isinstance(source, ItemsSource):
            return ray.data.from_items(list(source.items))
        if isinstance(source, RangeSource):
            return ray.data.range(source.count, override_num_blocks=self._active_physical_plan.source.max_workers)
        if isinstance(source, FileSource):
            options = dict(source.options)
            options["override_num_blocks"]=self._active_physical_plan.source.max_workers
            options.update(_ray_resource_options(self._active_physical_plan.source.worker_resources))
            reader_names = {
                "parquet": "read_parquet", "json": "read_json",
                "csv": "read_csv", "text": "read_text",
                "binary_files": "read_binary_files", "images": "read_images",
            }
            reader_name = reader_names.get(source.format)
            function = getattr(ray.data, reader_name, None) if reader_name else None
            if function is None:
                raise UnsupportedSourceError(f"RayDatasetExecutor does not support read_{source.format}")
            paths: Any = source.paths[0] if len(source.paths) == 1 else list(source.paths)
            return function(paths, **options)
        if isinstance(source, SqlSource):
            options = dict(source.options)
            options["override_num_blocks"]=self._active_physical_plan.source.max_workers
            options.update(_ray_resource_options(self._active_physical_plan.source.worker_resources))
            return ray.data.read_sql(source.sql, source.connection_factory, **options)
        if isinstance(source, DatasourceSource):
            kwargs = _ray_resource_options(self._active_physical_plan.source.worker_resources)
            kwargs["override_num_blocks"]=self._active_physical_plan.source.max_workers
            return ray.data.read_datasource(
                _RayDatasourceAdapter(source.datasource),
                parallelism=self._active_physical_plan.source.initial_workers,
                **kwargs,
            )
        if isinstance(source, LanceSource):
            options = _ray_resource_options(self._active_physical_plan.source.worker_resources)
            options["override_num_blocks"]=self._active_physical_plan.source.max_workers
            return ray.data.read_datasource(
                _RayLanceDatasource(source.query),
                parallelism=self._active_physical_plan.source.initial_workers, **options,
            )
        raise UnsupportedSourceError(f"RayDatasetExecutor does not support {type(source).__name__}")

    @staticmethod
    def _lower_stage_options(stage, operation):
        resources=stage.worker_resources
        options={}
        if resources.cpu: options["num_cpus"]=resources.cpu
        if resources.gpu: options["num_gpus"]=resources.gpu
        if resources.memory_bytes: options["memory"]=resources.memory_bytes
        if resources.custom_resources: options["resources"]=dict(resources.custom_resources)
        if stage.traits.worker_model == "reusable":
            from ray.data import ActorPoolStrategy
            options["compute"]=ActorPoolStrategy(
                min_size=stage.min_workers, initial_size=stage.initial_workers,
                max_size=stage.max_workers,
            )
        else:
            options["concurrency"]=stage.max_workers
        return options

    def _compile(self, source: SourcePlan, plan: LogicalPlan, *, action_kind="execute", terminal_category="action", terminal_native_options=None, physical_plan=None):
        physical=physical_plan or self.plan(source,plan,action_kind,terminal_category=terminal_category,terminal_native_options=terminal_native_options)
        self._active_physical_plan=physical
        stages={stage.ordinal:stage for stage in physical.transforms}
        ds = self._build_source(source)
        for operation_index, operation in enumerate(plan.operations, start=1):
            stage=stages[operation_index]
            if stage.logical_node is not operation:
                raise RuntimeError("PhysicalPlan stage does not match LogicalPlan operation")
            options=self._lower_stage_options(stage,operation)
            if isinstance(operation, BoundMapOp):
                target = (
                    _make_ray_bound_callable_class(operation)
                    if stage.traits.worker_model == "reusable"
                    else BoundCallable(operation)
                )
                ds = ds.map(target, **options)
            elif isinstance(operation, OperatorLLMMapOp):
                if not self._prompt_packs or self._operator_llm_coordinator is None:
                    raise RuntimeError("OperatorLLMMapOp requires Operator LLM configuration")
                ds = ds.map(
                    make_ray_operator_llm_callable_class(
                        operation, self._prompt_packs[operation.config_path], self._operator_llm_coordinator,
                    ),
                    **options,
                )
            elif isinstance(operation, MapOp):
                callable_target = operation.callable.target
                if stage.traits.worker_model == "reusable":
                    if not operation.callable.is_class:
                        callable_target = _make_ray_callable_class(operation.callable)
                    # Preserve Ray actor semantics for callable classes where no
                    # field-binding wrapper is required.
                    ds = ds.map(
                        callable_target,
                        fn_constructor_args=operation.callable.constructor_args,
                        fn_constructor_kwargs=dict(operation.callable.constructor_kwargs),
                        fn_args=operation.callable.call_args,
                        fn_kwargs=dict(operation.callable.call_kwargs),
                        **options,
                    )
                else:
                    ds = ds.map(StandardCallable(operation.callable), **options)
            elif isinstance(operation, FlatMapOp):
                options = dict(options)
                callable_target = operation.callable.target
                if stage.traits.worker_model == "reusable":
                    if not operation.callable.is_class:
                        callable_target = _make_ray_callable_class(operation.callable)
                    options["fn_constructor_args"] = operation.callable.constructor_args
                    options["fn_constructor_kwargs"] = dict(
                        operation.callable.constructor_kwargs
                    )
                ds = ds.flat_map(
                    callable_target,
                    fn_args=operation.callable.call_args,
                    fn_kwargs=dict(operation.callable.call_kwargs),
                    **options,
                )
            elif isinstance(operation, MapBatchesOp):
                options = dict(options)
                callable_target = operation.callable.target
                if stage.traits.worker_model == "reusable":
                    if not operation.callable.is_class:
                        callable_target = _make_ray_callable_class(operation.callable)
                    options["fn_constructor_args"] = operation.callable.constructor_args
                    options["fn_constructor_kwargs"] = dict(
                        operation.callable.constructor_kwargs
                    )
                ds = ds.map_batches(
                    callable_target,
                    batch_size=operation.batch_size,
                    batch_format=operation.batch_format,
                    zero_copy_batch=operation.zero_copy_batch,
                    fn_args=operation.callable.call_args,
                    fn_kwargs=dict(operation.callable.call_kwargs),
                    **options,
                )
            elif isinstance(operation, FilterOp):
                if stage.traits.worker_model == "reusable":
                    callable_target = operation.callable.target
                    if not operation.callable.is_class:
                        callable_target = _make_ray_callable_class(operation.callable)
                    ds = ds.filter(
                        callable_target,
                        fn_constructor_args=operation.callable.constructor_args,
                        fn_constructor_kwargs=dict(
                            operation.callable.constructor_kwargs
                        ),
                        fn_args=operation.callable.call_args,
                        fn_kwargs=dict(operation.callable.call_kwargs),
                        **options,
                    )
                else:
                    ds = ds.filter(
                        StandardCallable(operation.callable),
                        **options,
                    )
            elif isinstance(operation, LimitOp):
                ds = ds.limit(operation.limit)
            elif isinstance(operation, SelectColumnsOp):
                ds = ds.select_columns(list(operation.columns))
            elif isinstance(operation, DropColumnsOp):
                ds = ds.drop_columns(list(operation.columns))
            elif isinstance(operation, RenameColumnsOp):
                ds = ds.rename_columns(
                    list(operation.names)
                    if isinstance(operation.names, tuple)
                    else dict(operation.names),
                )
            elif isinstance(operation, AddColumnOp):
                callable_target = operation.callable.target
                if stage.traits.worker_model == "reusable" and not operation.callable.is_class:
                    callable_target = _make_ray_callable_class(operation.callable)
                ds = ds.add_column(
                    operation.column, callable_target,
                    batch_format=operation.batch_format,
                    **options,
                )
            elif isinstance(operation, RandomSampleOp):
                ds = ds.random_sample(
                    operation.fraction, seed=operation.seed,
                )
            elif isinstance(operation, SortOp):
                ds = ds.sort(
                    operation.keys[0]
                    if len(operation.keys) == 1 else list(operation.keys),
                    descending=(
                        list(operation.descending)
                        if isinstance(operation.descending, tuple)
                        else operation.descending
                    ),
                    boundaries=(
                        None if operation.boundaries is None
                        else list(operation.boundaries)
                    ),
                )
            elif isinstance(operation, RepartitionOp):
                ds = ds.repartition(
                    operation.num_blocks,
                    operation.target_num_rows_per_block,
                    shuffle=operation.shuffle,
                    keys=(
                        None if operation.keys is None
                        else list(operation.keys)
                    ),
                    sort=operation.sort,
                )
            elif isinstance(operation, RandomShuffleOp):
                ds = ds.random_shuffle(
                    seed=operation.seed,
                    num_blocks=operation.num_blocks,
                )
            elif isinstance(operation, RandomizeBlockOrderOp):
                ds = ds.randomize_block_order(seed=operation.seed)
            else:
                raise TypeError(f"unknown logical operation: {type(operation).__name__}")
        self._last_native = ds
        return ds

    def iter_rows(self, source: SourcePlan, plan: LogicalPlan) -> Iterable[dict[str, Any]]:
        return self._compile(source, plan, action_kind="iter_rows").iter_rows()

    def iter_batches(
        self, source: SourcePlan, plan: LogicalPlan, *, prefetch_batches: int = 1,
        batch_size: int | None = 256, batch_format: str | None = "default", drop_last: bool = False,
    ) -> Iterable[Any]:
        return self._compile(source, plan, action_kind="iter_batches").iter_batches(
            prefetch_batches=prefetch_batches,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
        )

    def materialize(self, source: SourcePlan, plan: LogicalPlan) -> Any:
        native = self._compile(source, plan, action_kind="materialize").materialize()
        self._last_native = native
        return native

    def take(
        self, source: SourcePlan, plan: LogicalPlan, limit: int,
    ) -> list[dict[str, Any]]:
        # Bounded collection is a native Ray Data action. Keep iterator
        # lifecycle and early-stop cleanup inside the Ray backend.
        return self._compile(source, plan, action_kind="take").take(limit)

    def take_batch(
        self, source: SourcePlan, plan: LogicalPlan, batch_size: int,
        *, batch_format: str,
    ) -> Any:
        return self._compile(source, plan, action_kind="take_batch").take_batch(
            batch_size, batch_format=batch_format,
        )

    def count(self, source: SourcePlan, plan: LogicalPlan) -> int:
        return self._compile(source, plan, action_kind="count").count()

    def aggregate(
        self, source: SourcePlan, plan: LogicalPlan,
        aggregates: tuple[Any, ...],
    ) -> Any:
        native = self._compile(source, plan, action_kind="aggregate")
        for aggregate in aggregates:
            try:
                ray.cloudpickle.dumps(aggregate)
            except Exception as exc:
                raise AggregateSerializationError(
                    f"aggregation {aggregate.name!r} is not serializable: {exc}"
                ) from exc
            _validate_ray_aggregate_value(
                aggregate.initial_state(), aggregate.name,
                self._aggregate_state_max_bytes, "state",
            )
        return native.aggregate(*(
            _ray_aggregate(aggregate, self._aggregate_state_max_bytes)
            for aggregate in aggregates
        ))

    def schema(self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool = True) -> Any:
        return self._compile(source, plan, action_kind="schema").schema(fetch_if_missing=fetch_if_missing)

    def columns(
        self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool,
    ) -> list[str] | None:
        return self._compile(source, plan, action_kind="columns").columns(
            fetch_if_missing=fetch_if_missing,
        )

    def size_bytes(self, source: SourcePlan, plan: LogicalPlan) -> int:
        return int(self._compile(source, plan, action_kind="size_bytes").size_bytes())

    def num_blocks(self, source: SourcePlan, plan: LogicalPlan) -> int:
        if not isinstance(source, MaterializedSource) or not plan.is_empty:
            raise TypeError("num_blocks is only available on MaterializedDataset")
        return int(source.handle.num_blocks())

    def stats(self, source: SourcePlan, plan: LogicalPlan) -> str:
        native = self._last_native
        if native is None:
            return f"Dataset({type(source).__name__}, operations={len(plan.operations)}, not executed)"
        return native.stats()

    def execution_metadata(self, source: SourcePlan, plan: LogicalPlan) -> ExecutionMetadata | None:
        return None

    def write_datasink(
        self, source: SourcePlan, plan: LogicalPlan, datasink: Datasink,
        *, native_options=None,
    ) -> DemiflowWriteResult:
        try:
            physical=self.plan(source,plan,"write_datasink",terminal_category="sink",terminal_native_options=native_options)
            terminal=physical.terminal
            options=_stage_sink_options(terminal)
            self._active_physical_plan=physical
            self._compile(source, plan, action_kind="write_datasink", terminal_category="sink",terminal_native_options=native_options,physical_plan=physical).write_datasink(
                _RayDatasinkAdapter(datasink),
                **options,
            )
            result = DemiflowWriteResult(
                written_rows=None, failed_rows=None, blocks_written=None,
                target="", metadata={"backend": self.NAME},
            )
            datasink.on_write_complete(result)
            return result
        except Exception as exc:
            try:
                datasink.on_write_failed(exc)
            except Exception:
                pass
            raise

    def write_lance(self, source, plan, spec, *, native_options=None):
        if spec.expected_version is None:
            return self._write_lance_direct(
                source, plan, spec, native_options=native_options,
            )
        physical=self.plan(
            source,plan,"write_lance",terminal_category="sink",
            terminal_native_options=native_options,
        )
        from ...lance.write import prepare_lance_append
        prepared = prepare_lance_append(spec)
        if prepared is None:
            raise AssertionError("expected-version Lance append was not prepared")
        adapter = _RayLanceFragmentWriteSink(prepared)
        options=_stage_sink_options(physical.terminal)
        self._compile(source, plan, action_kind="write_lance", terminal_category="sink",terminal_native_options=native_options,physical_plan=physical).write_datasink(adapter.ray_datasink(), **options)
        if adapter.receipt is None:
            raise RuntimeError("Ray Lance distributed write returned no receipt")
        return adapter.receipt

    def _write_lance_direct(
        self, source, plan, spec, *, native_options=None,
    ):
        adapter = _RayLanceDirectWriteSink(spec)
        physical=self.plan(
            source,plan,"write_lance",terminal_category="sink",
            terminal_native_options=native_options,terminal_parallelism_cap=1,
        )
        options=_stage_sink_options(physical.terminal)
        self._compile(
            source, plan, action_kind="write_lance", terminal_category="sink",
            terminal_native_options=native_options, physical_plan=physical,
        ).repartition(1).write_datasink(
            adapter.ray_datasink(), **options,
        )
        if adapter.receipt is None:
            raise RuntimeError("Ray Lance direct write returned no receipt")
        return adapter.receipt

    def write_file(
        self, source: SourcePlan, plan: LogicalPlan, format_name: str,
        path: str, options: dict[str, Any],
    ) -> DemiflowWriteResult:
        native_options=options.pop("_demiflow_native_options",None)
        physical=self.plan(source,plan,f"write_{format_name}",terminal_category="sink",terminal_native_options=native_options)
        native = self._compile(source, plan, action_kind=f"write_{format_name}", terminal_category="sink",terminal_native_options=native_options,physical_plan=physical)
        method = getattr(native, f"write_{format_name}", None)
        if method is None:
            raise UnsupportedSourceError(f"RayDatasetExecutor does not support write_{format_name}")
        method(path, **options, **_stage_sink_options(physical.terminal))
        return DemiflowWriteResult(
            written_rows=None, failed_rows=None, blocks_written=None,
            target="", metadata={"format": format_name},
        )

    def close(self) -> None:
        pass


def make_ray_operator_llm_callable_class(operation, config, coordinator):
    class RayOperatorLLMCallable:
        def __init__(self):
            self._bound = BoundOperatorLLMMap(
                operation, OperatorLLMRuntime(config, coordinator),
            )

        def __call__(self, row):
            return self._bound(row)

    RayOperatorLLMCallable.__name__ = f"OperatorLLM{operation.prompt_name}"
    RayOperatorLLMCallable.__qualname__ = RayOperatorLLMCallable.__name__
    return RayOperatorLLMCallable


def _make_ray_bound_callable_class(operation):
    """Create the class-shaped UDF required by a validated reusable stage."""
    class RayFieldBoundCallable:
        def __init__(self):
            self._runtime = BoundCallable(operation)

        def __call__(self, row):
            return self._runtime(row)

    RayFieldBoundCallable.__name__ = f"Bound{operation.callable.name}"
    RayFieldBoundCallable.__qualname__ = RayFieldBoundCallable.__name__
    return RayFieldBoundCallable


def _make_ray_callable_class(spec):
    """Class-shape a function only for an explicit reusable PhysicalStage."""
    class RayReusableCallable:
        def __init__(self):
            self._target = spec.target

        def __call__(self, value, *args, **kwargs):
            return self._target(value, *args, **kwargs)

    RayReusableCallable.__name__ = f"Reusable{spec.name}"
    RayReusableCallable.__qualname__ = RayReusableCallable.__name__
    return RayReusableCallable


class _RayOperatorLLMCoordinatorProxy:
    def __init__(self, actor) -> None:
        self._actor = actor

    def attempted(self):
        return ray.get(self._actor.attempted.remote())

    def reserve(self):
        return ray.get(self._actor.reserve.remote())

    def started(self, value):
        return ray.get(self._actor.started.remote(value))

    def completed(self, value, response):
        return ray.get(self._actor.completed.remote(value, response))

    def failed(self, value, response=None):
        return ray.get(self._actor.failed.remote(value, response))

    def usage(self):
        return OperatorLLMUsage(**ray.get(self._actor.usage.remote()))


def _ray_operator_llm_coordinator_class():
    import uuid

    @ray.remote(num_cpus=0)
    class RayOperatorLLMCoordinator:
        def __init__(self):
            self.values = {
                "calls_attempted": 0,
                "requests_reserved": 0,
                "requests_started": 0,
                "requests_completed": 0,
                "requests_failed": 0,
                "input_tokens": 0,
                "output_tokens": 0,
            }
            self.reservations = set()

        def attempted(self):
            self.values["calls_attempted"] += 1

        def reserve(self):
            value = "operator-llm-" + uuid.uuid4().hex
            self.reservations.add(value)
            self.values["requests_reserved"] += 1
            return value

        def started(self, value):
            self._require(value)
            self.values["requests_started"] += 1

        def completed(self, value, response):
            self._finish(value, "requests_completed", response)

        def failed(self, value, response=None):
            self._finish(value, "requests_failed", response)

        def _finish(self, value, field, response):
            self._require(value)
            self.reservations.remove(value)
            self.values[field] += 1
            if response and response.usage:
                self.values["input_tokens"] += response.usage.input_tokens
                self.values["output_tokens"] += response.usage.output_tokens

        def usage(self):
            return dict(self.values)

        def _require(self, value):
            if value not in self.reservations:
                raise RuntimeError("unknown Operator LLM reservation")

    return RayOperatorLLMCoordinator


def _ray_aggregate(aggregate, state_max_bytes):
    """Lower one backend-neutral aggregate through Ray's public V2 contract."""
    from ray.data.aggregate import AggregateFnV2 as RayAggregateFnV2

    class RayAggregateAdapter(RayAggregateFnV2):
        def __init__(self):
            self._demiflow = aggregate
            super().__init__(
                aggregate.name,
                aggregate.create_zero,
                on=aggregate.get_target_column(),
                ignore_nulls=aggregate.ignore_nulls,
            )

        def aggregate_block(self, block):
            from ...data.aggregate import normalize_block
            value = self._demiflow.aggregate_block(normalize_block(block))
            _validate_ray_aggregate_value(
                value, self._demiflow.name, state_max_bytes, "state",
            )
            return value

        def combine(self, current_accumulator, new):
            value = self._demiflow.combine(current_accumulator, new)
            _validate_ray_aggregate_value(
                value, self._demiflow.name, state_max_bytes, "state",
            )
            return value

        def finalize(self, accumulator):
            value = self._demiflow.finalize(accumulator)
            _validate_ray_aggregate_value(
                value, self._demiflow.name, state_max_bytes, "result",
            )
            return value

    RayAggregateAdapter.__name__ = f"Ray{type(aggregate).__name__}"
    RayAggregateAdapter.__qualname__ = RayAggregateAdapter.__name__
    return RayAggregateAdapter()


def _validate_ray_aggregate_value(value, name, limit, kind):
    try:
        raw = ray.cloudpickle.dumps(value)
    except Exception as exc:
        raise AggregateSerializationError(
            f"{kind} for aggregation {name!r} is not serializable: {exc}"
        ) from exc
    if len(raw) > limit:
        raise AggregateStateLimitExceeded(
            f"{kind} for aggregation {name!r} exceeds {limit} bytes"
        )

def _stage_sink_options(stage):
    resources=stage.worker_resources
    remote={}
    if resources.cpu: remote["num_cpus"]=resources.cpu
    if resources.gpu: remote["num_gpus"]=resources.gpu
    if resources.memory_bytes: remote["memory"]=resources.memory_bytes
    if resources.custom_resources: remote["resources"]=dict(resources.custom_resources)
    return {"concurrency":stage.max_workers,"ray_remote_args":remote}

def _ray_resource_options(resources):
    options={}
    if resources.cpu: options["num_cpus"]=resources.cpu
    if resources.gpu: options["num_gpus"]=resources.gpu
    if resources.memory_bytes: options["memory"]=resources.memory_bytes
    if resources.custom_resources: options["ray_remote_args"]={"resources":dict(resources.custom_resources)}
    return options
