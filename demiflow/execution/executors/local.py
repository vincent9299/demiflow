"""Local compiler/executor for lazy demiflow source and logical plans."""

from __future__ import annotations

import csv
import itertools
import json
import os
import pickle
import random
import tempfile
import threading
import time
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional

from ...data.datasink import Datasink, WriteContext, WriteResult
from ...data.observability import DatasourceReadObserver, block_row_count
from ...errors import (
    AggregateSerializationError, AggregateStateLimitExceeded,
    UnsupportedExecutionOptionError,
    UnsupportedSourceError,
)
from ...data.plan import (
    AddColumnOp, BoundCallable, BoundMapOp, DropColumnsOp, FilterOp,
    FlatMapOp, LimitOp, LogicalPlan, MapBatchesOp, MapOp,
    OperatorLLMMapOp, RandomSampleOp, RenameColumnsOp, SelectColumnsOp,
    RandomShuffleOp, RandomizeBlockOrderOp, RepartitionOp, SortOp,
    StandardCallable,
)
from ...operator_llm.runtime import BoundOperatorLLMMap, InProcessOperatorLLMCoordinator, OperatorLLMRuntime
from ...data.sources import DatasourceSource, FileSource, ItemsSource, LanceSource, MaterializedSource, RangeSource, SourcePlan, SqlSource
from ...data.constraints import SourceReadConstraints, analyze_source_constraints, analyze_stage_work_units, stage_parallelism_caps
from ...observability import current_action_observer
from ...data.stats import ExecutionMetadata, StageStats
from .base import DatasetExecutor
from ...planning import BackendResourceSnapshot, ResourceBundle, plan_action
from ...planning.traits import terminal_traits


@dataclass(frozen=True)
class _LocalMaterializedHandle:
    blocks: tuple[Any, ...]
    row_count: int


@dataclass(frozen=True)
class _SpilledBlock:
    path: str


class _ThreadLocalRuntime:
    """Give each local worker its own stateful callable/client instance."""

    def __init__(self, factory) -> None:
        self._factory = factory
        self._local = threading.local()

    def __call__(self, row):
        runtime = getattr(self._local, "runtime", None)
        if runtime is None:
            runtime = self._factory()
            self._local.runtime = runtime
        return runtime(row)


class LocalDatasetExecutor(DatasetExecutor):
    NAME = "local"

    def __init__(
        self,
        workers: int = 4,
        *,
        block_size: int = 256,
        materialize_memory_limit: int = 64 * 1024 * 1024,
        aggregate_state_max_bytes: int = 8 * 1024 * 1024,
        prompt_packs=None,
        usage_callback=None,
        planning_policy=None,
        candidate_execution=None,
    ) -> None:
        self._workers = max(1, int(workers))
        from ...planning.policy import parse_platform_planning_policy
        self._planning_policy = planning_policy or parse_platform_planning_policy(None)
        self._candidate_execution = candidate_execution
        self._active_physical_plan = None
        self._block_size = max(1, int(block_size))
        self._materialize_memory_limit = max(1, int(materialize_memory_limit))
        self._aggregate_state_max_bytes = max(1, int(aggregate_state_max_bytes))
        self._pool: Optional[ThreadPoolExecutor] = None
        self._last_metadata: Optional[ExecutionMetadata] = None
        self._spill_paths: set[str] = set()
        self._prompt_packs = dict(prompt_packs or {})
        self._operator_llm_coordinator = (
            InProcessOperatorLLMCoordinator(on_change=usage_callback)
            if self._prompt_packs else None
        )

    def operator_llm_usage(self) -> dict[str, int]:
        if self._operator_llm_coordinator is None:
            return {}
        return self._operator_llm_coordinator.usage().to_dict()

    def plan(
        self, source, plan, action_kind, *, terminal_category="action",
        terminal_native_options=None, source_parallelism_cap=None,
        terminal_parallelism_cap=None,
    ):
        native=(getattr(source,"native_options",None) is not None or any(getattr(operation,"native_options",None) is not None for operation in plan.operations) or terminal_native_options is not None)
        if native and (self._candidate_execution is None or self._candidate_execution.mode != "native"):
            raise ValueError("Native options require Pipeline execution mode native")
        capacity=ResourceBundle(cpu=float(self._workers),memory_bytes=_host_memory_bytes())
        snapshot=BackendResourceSnapshot("local",(capacity,),hashlib.sha256(repr(capacity).encode()).hexdigest())
        marker=type("Terminal",(),{})()
        bounds=(*analyze_stage_work_units(source,plan),None)
        if isinstance(source,LanceSource): source_parallelism_cap=1
        caps=stage_parallelism_caps(plan,source_cap=source_parallelism_cap,terminal_cap=terminal_parallelism_cap)
        return plan_action(backend="local",action_kind=action_kind,source=source,operations=plan.operations,terminal_node=marker,terminal_traits=terminal_traits(terminal_category),policy=self._planning_policy,snapshot=snapshot,work_units_by_stage=bounds,terminal_native_options=terminal_native_options,parallelism_caps_by_stage=caps)

    @property
    def executor(self) -> ThreadPoolExecutor:
        if self._pool is None:
            self._pool = ThreadPoolExecutor(max_workers=self._workers)
        return self._pool

    def from_items(self, items: list[Any], *, override_num_blocks: int | None = None) -> _LocalMaterializedHandle:
        count = max(1, int(override_num_blocks or 1))
        size = max(1, (len(items) + count - 1) // count)
        blocks = tuple(tuple(items[i:i + size]) for i in range(0, len(items), size))
        return _LocalMaterializedHandle(blocks=blocks, row_count=len(items))

    def from_arrow(self, tables: Any) -> _LocalMaterializedHandle:
        values = tables if isinstance(tables, list) else [tables]
        rows = []
        for table in values:
            if isinstance(table, bytes):
                import pyarrow as pa
                table = pa.ipc.open_stream(pa.BufferReader(table)).read_all()
            rows.extend(table.to_pylist())
        return self.from_items(rows)

    def from_numpy(self, arrays: Any) -> _LocalMaterializedHandle:
        values = arrays if isinstance(arrays, list) else [arrays]
        rows = [
            {"data": item}
            for array in values
            for item in array
        ]
        return self.from_items(rows)

    def from_pandas(
        self, frames: Any, *, override_num_blocks: int | None = None,
    ) -> _LocalMaterializedHandle:
        values = frames if isinstance(frames, list) else [frames]
        rows = [row for frame in values for row in frame.to_dict(orient="records")]
        return self.from_items(rows, override_num_blocks=override_num_blocks)

    def _iter_block_rows(self, block: Any) -> Iterator[Any]:
        if block is None:
            return
        if isinstance(block, Mapping):
            yield dict(block)
            return
        if isinstance(block, _SpilledBlock):
            with open(block.path, "rb") as fh:
                yield from pickle.load(fh)
            return
        if isinstance(block, (list, tuple)):
            yield from block
            return
        if hasattr(block, "to_pylist"):
            yield from block.to_pylist()
            return
        if hasattr(block, "to_dict"):
            records = block.to_dict(orient="records")
            yield from records
            return
        yield from block

    def _iter_source(
        self, source: SourcePlan, *, constraints: SourceReadConstraints,
    ) -> Iterator[Any]:
        try:
            if isinstance(source, MaterializedSource):
                handle = source.handle
                if not isinstance(handle, _LocalMaterializedHandle):
                    raise UnsupportedSourceError("materialized source belongs to another backend")
                for block in handle.blocks:
                    yield from self._iter_block_rows(block)
                return
            if isinstance(source, ItemsSource):
                yield from source.items
                return
            if isinstance(source, RangeSource):
                for value in range(source.count):
                    yield {"id": value}
                return
            if isinstance(source, DatasourceSource):
                physical=self._active_physical_plan
                parallelism=(physical.source.initial_workers if physical is not None else 1)
                observation = DatasourceReadObserver(
                    source.datasource, constraints, current_action_observer(),
                )
                observation.planning_started(parallelism=parallelism)
                try:
                    tasks = source.datasource.get_read_tasks(
                        parallelism, per_task_row_limit=constraints.row_limit,
                    )
                    observation.planning_completed(
                        task_count=len(tasks), parallelism=parallelism,
                    )
                    global_remaining = constraints.row_limit
                    for task_index, task in enumerate(tasks):
                        if global_remaining is not None and global_remaining <= 0:
                            break
                        observation.task_started(task_index)
                        task_remaining = task.per_task_row_limit
                        if global_remaining is not None:
                            task_remaining = (
                                global_remaining
                                if task_remaining is None
                                else min(task_remaining, global_remaining)
                            )
                        early_stopped = False
                        try:
                            for block in task.read_fn():
                                observation.block_received(
                                    task_index, block_row_count(block),
                                )
                                for row in self._iter_block_rows(block):
                                    if task_remaining is not None and task_remaining <= 0:
                                        early_stopped = True
                                        break
                                    observation.row_yielded()
                                    if task_remaining is not None:
                                        task_remaining -= 1
                                    if global_remaining is not None:
                                        global_remaining -= 1
                                    yield row
                                if task_remaining is not None and task_remaining <= 0:
                                    early_stopped = True
                                    break
                        except GeneratorExit:
                            early_stopped = True
                            observation.task_completed(
                                task_index, early_stopped=early_stopped,
                            )
                            raise
                        except Exception as exc:
                            observation.task_failed(task_index, exc)
                            raise
                        else:
                            observation.task_completed(
                                task_index, early_stopped=early_stopped,
                            )
                        if global_remaining is not None and global_remaining <= 0:
                            break
                except Exception as exc:
                    observation.failed(exc)
                    raise
                return
            if isinstance(source, LanceSource):
                from ...lance.read import constrain_lance_query, iter_lance_batches
                query=constrain_lance_query(source.query,constraints.row_limit)
                for batch in iter_lance_batches(query,batch_size=self._block_size):
                    yield from batch.to_pylist()
                return
            if isinstance(source, FileSource):
                yield from self._iter_file_source(source)
                return
            if isinstance(source, SqlSource):
                yield from self._iter_sql(source)
                return
        except (UnsupportedExecutionOptionError, UnsupportedSourceError):
            raise
        raise UnsupportedSourceError(f"LocalDatasetExecutor does not support {type(source).__name__}")

    def _iter_file_source(self, source: FileSource) -> Iterator[Any]:
        options = dict(source.options)
        if source.format == "json":
            lines = bool(options.pop("lines", True))
            if options:
                raise TypeError(f"unsupported local read_json options: {sorted(options)}")
            for raw_path in source.paths:
                with Path(raw_path).open(encoding="utf-8") as fh:
                    if lines:
                        for line in fh:
                            if line.strip():
                                yield json.loads(line)
                    else:
                        value = json.load(fh)
                        yield from value if isinstance(value, list) else [value]
            return
        if source.format == "text":
            encoding = str(options.pop("encoding", "utf-8"))
            drop_empty = bool(options.pop("drop_empty_lines", True))
            if options:
                raise TypeError(f"unsupported local read_text options: {sorted(options)}")
            for raw_path in source.paths:
                with Path(raw_path).open(encoding=encoding) as fh:
                    for line in fh:
                        text = line.rstrip("\n")
                        if text or not drop_empty:
                            yield {"text": text}
            return
        if source.format == "binary_files":
            include_paths = bool(options.pop("include_paths", False))
            if options:
                raise TypeError(f"unsupported local read_binary_files options: {sorted(options)}")
            for raw_path in source.paths:
                data = Path(raw_path).read_bytes()
                yield (raw_path, data) if include_paths else {"bytes": data}
            return
        if source.format == "csv":
            if options:
                raise TypeError(f"unsupported local read_csv options: {sorted(options)}")
            for raw_path in source.paths:
                with Path(raw_path).open(newline="", encoding="utf-8") as fh:
                    yield from csv.DictReader(fh)
            return
        if source.format == "parquet":
            import pyarrow.dataset as pads
            columns = options.pop("columns", None)
            table = pads.dataset(list(source.paths), format="parquet").to_table(columns=columns, **options)
            yield from table.to_pylist()
            return
        raise UnsupportedSourceError(f"LocalDatasetExecutor does not support read_{source.format}")

    def _iter_sql(self, source: SqlSource) -> Iterator[Any]:
        options = dict(source.options)
        params = options.pop("sql_params", None)
        if options:
            raise TypeError(f"unsupported local read_sql options: {sorted(options)}")
        connection = source.connection_factory()
        try:
            cursor = connection.cursor()
            cursor.execute(source.sql, params or ())
            names = [item[0] for item in cursor.description]
            for values in cursor:
                yield dict(zip(names, values))
        finally:
            connection.close()

    @staticmethod
    def _normalize_concurrency(value: Any) -> int:
        if value is None:
            return 1
        if isinstance(value, tuple):
            return max(1, int(value[-1]))
        return max(1, int(value))

    def _map_stage(self, rows: Iterable[Any], fn: Any, concurrency: Any) -> Iterator[Any]:
        width = self._normalize_concurrency(concurrency)
        if width == 1:
            yield from map(fn, rows)
            return
        iterator = iter(rows)
        pending: dict[int, Any] = {}
        submitted = 0
        emitted = 0

        def submit_one() -> bool:
            nonlocal submitted
            try:
                row = next(iterator)
            except StopIteration:
                return False
            pending[submitted] = self.executor.submit(fn, row)
            submitted += 1
            return True

        for _ in range(width):
            if not submit_one():
                break
        while pending:
            future = pending.pop(emitted)
            yield future.result()
            emitted += 1
            submit_one()

    @staticmethod
    def _require_ray_backend(operation: Any) -> None:
        method = {
            SortOp: "sort",
            RepartitionOp: "repartition",
            RandomShuffleOp: "random_shuffle",
            RandomizeBlockOrderOp: "randomize_block_order",
        }[type(operation)]
        raise UnsupportedExecutionOptionError(
            f"Dataset.{method} requires a Ray PipelineExecutionTarget. "
            "Demiflow does not switch targets automatically."
        )

    @staticmethod
    def _rows_to_batch(rows: list[dict[str, Any]], batch_format: str) -> Any:
        if batch_format == "pyarrow":
            import pyarrow as pa
            return pa.Table.from_pylist(rows)
        if batch_format == "pandas":
            import pandas as pd
            return pd.DataFrame(rows)
        if batch_format in {"default", "numpy"}:
            import numpy as np
            columns = rows[0].keys() if rows else ()
            return {name: np.asarray([row[name] for row in rows]) for name in columns}
        raise ValueError(f"unsupported batch format: {batch_format}")

    def _batch_to_rows(self, batch: Any) -> Iterator[dict[str, Any]]:
        if isinstance(batch, Mapping):
            if not batch:
                return
            names = tuple(batch)
            columns = [batch[name] for name in names]
            lengths = {len(column) for column in columns}
            if len(lengths) != 1:
                raise ValueError("batch columns must have equal lengths")
            for index in range(lengths.pop()):
                yield {name: column[index] for name, column in zip(names, columns)}
            return
        yield from self._iter_block_rows(batch)

    def _map_batches_stage(
        self, rows: Iterable[Any], operation: MapBatchesOp, width: int,
    ) -> Iterator[Any]:
        if operation.zero_copy_batch:
            raise UnsupportedExecutionOptionError(
                "LocalDatasetExecutor does not support zero_copy_batch=True"
            )
        size = operation.batch_size or self._block_size
        iterator = iter(rows)
        runtime = _ThreadLocalRuntime(
            lambda spec=operation.callable: StandardCallable(spec)
        )

        def apply(batch_rows):
            batch = self._rows_to_batch(list(batch_rows), operation.batch_format)
            result = runtime(batch)
            if isinstance(result, Iterator):
                output = []
                for item in result:
                    output.extend(self._batch_to_rows(item))
                return output
            return list(self._batch_to_rows(result))

        batches = iter(lambda: tuple(itertools.islice(iterator, size)), ())
        for output_rows in self._map_stage(batches, apply, width):
            yield from output_rows

    def _filter_stage(self, rows: Iterable[Any], predicate: Any, concurrency: Any) -> Iterator[Any]:
        width = self._normalize_concurrency(concurrency)
        if width == 1:
            yield from filter(predicate, rows)
            return

        def evaluate(row):
            return row, bool(predicate(row))

        for row, keep in self._map_stage(rows, evaluate, width):
            if keep:
                yield row

    def _apply_plan(self, source: SourcePlan, plan: LogicalPlan, *, action_kind="iter_rows", terminal_category="action", terminal_native_options=None, physical_plan=None) -> Iterator[Any]:
        physical=physical_plan or self.plan(source,plan,action_kind,terminal_category=terminal_category,terminal_native_options=terminal_native_options)
        self._active_physical_plan=physical
        widths={stage.ordinal:stage.initial_workers for stage in physical.transforms}
        rows: Iterable[Any] = self._iter_source(
            source, constraints=analyze_source_constraints(plan),
        )
        stage_stats = []
        for operation_index, operation in enumerate(plan.operations, start=1):
            width=widths[operation_index]
            started = time.monotonic()
            if isinstance(operation, BoundMapOp):
                rows = self._map_stage(
                    rows,
                    _ThreadLocalRuntime(lambda op=operation: BoundCallable(op)),
                    width,
                )
            elif isinstance(operation, OperatorLLMMapOp):
                if self._operator_llm_coordinator is None:
                    raise RuntimeError("OperatorLLMMapOp requires Operator LLM configuration")
                rows = self._map_stage(
                    rows,
                    _ThreadLocalRuntime(
                        lambda op=operation: BoundOperatorLLMMap(
                            op, OperatorLLMRuntime(
                                self._prompt_packs[op.config_path],
                                self._operator_llm_coordinator,
                            ),
                        )
                    ),
                    width,
                )
            elif isinstance(operation, MapOp):
                rows = self._map_stage(
                    rows,
                    _ThreadLocalRuntime(lambda spec=operation.callable: StandardCallable(spec)),
                    width,
                )
            elif isinstance(operation, FilterOp):
                predicate = _ThreadLocalRuntime(
                    lambda spec=operation.callable: StandardCallable(spec)
                )
                rows = self._filter_stage(rows, predicate, width)
            elif isinstance(operation, FlatMapOp):
                mapped = self._map_stage(
                    rows,
                    _ThreadLocalRuntime(
                        lambda spec=operation.callable: StandardCallable(spec)
                    ),
                    width,
                )
                def flatten():
                    for values in mapped:
                        if isinstance(values, Mapping):
                            raise TypeError(
                                "flat_map callable must return an iterable of rows"
                            )
                        for item in values:
                            if not isinstance(item, Mapping):
                                raise TypeError(
                                    "flat_map output rows must be mappings"
                                )
                            yield dict(item)
                rows = flatten()
            elif isinstance(operation, MapBatchesOp):
                rows = self._map_batches_stage(rows, operation, width)
            elif isinstance(operation, LimitOp):
                rows = itertools.islice(rows, operation.limit)
            elif isinstance(operation, SelectColumnsOp):
                rows = self._map_stage(
                    rows,
                    lambda row, columns=operation.columns: {
                        column: row[column] for column in columns
                    },
                    width,
                )
            elif isinstance(operation, DropColumnsOp):
                def drop(row, columns=operation.columns):
                    missing = [column for column in columns if column not in row]
                    if missing:
                        raise KeyError(f"columns not found: {missing}")
                    return {
                        key: value for key, value in row.items()
                        if key not in columns
                    }
                rows = self._map_stage(
                    rows, drop, width,
                )
            elif isinstance(operation, RenameColumnsOp):
                names = operation.names
                if isinstance(names, Mapping):
                    def rename_mapping(row, mapping=names):
                        targets = [mapping.get(key, key) for key in row]
                        if len(targets) != len(set(targets)):
                            raise ValueError(
                                "rename_columns produces duplicate column names"
                            )
                        return {
                            mapping.get(key, key): value
                            for key, value in row.items()
                        }
                    rows = self._map_stage(
                        rows, rename_mapping, width,
                    )
                else:
                    def rename(row, target=names):
                        if len(row) != len(target):
                            raise ValueError("rename_columns list length must match schema")
                        return dict(zip(target, row.values()))
                    rows = self._map_stage(rows, rename, width)
            elif isinstance(operation, AddColumnOp):
                iterator = iter(rows)
                batches = iter(
                    lambda: tuple(itertools.islice(iterator, self._block_size)),
                    (),
                )
                runtime = _ThreadLocalRuntime(
                    lambda spec=operation.callable: StandardCallable(spec)
                )

                def add(batch_rows):
                    batch = self._rows_to_batch(
                        list(batch_rows), operation.batch_format,
                    )
                    column = runtime(batch)
                    values = (
                        column.to_pylist()
                        if hasattr(column, "to_pylist")
                        else list(column)
                    )
                    if len(values) != len(batch_rows):
                        raise ValueError(
                            "add_column result length must match batch"
                        )
                    return [
                        {**row, operation.column: value}
                        for row, value in zip(batch_rows, values)
                    ]

                mapped = self._map_stage(
                    batches, add, width,
                )
                rows = (row for batch_rows in mapped for row in batch_rows)
            elif isinstance(operation, RandomSampleOp):
                rng = random.Random(operation.seed)
                rows = (row for row in rows if rng.random() < operation.fraction)
            elif isinstance(operation, (
                SortOp, RepartitionOp, RandomShuffleOp,
                RandomizeBlockOrderOp,
            )):
                self._require_ray_backend(operation)
            else:
                raise TypeError(f"unknown logical operation: {type(operation).__name__}")
            stage_stats.append(StageStats(type(operation).__name__, elapsed_seconds=time.monotonic() - started))
        # Metadata is completed by actions that consume rows.
        self._last_metadata = ExecutionMetadata(
            source_name=type(source).__name__, stages=tuple(stage_stats)
        )
        yield from rows

    def iter_rows(self, source: SourcePlan, plan: LogicalPlan) -> Iterable[dict[str, Any]]:
        return self._apply_plan(source, plan, action_kind="iter_rows")

    def iter_batches(
        self, source: SourcePlan, plan: LogicalPlan, *, prefetch_batches: int = 1,
        batch_size: int | None = 256, batch_format: str | None = "default", drop_last: bool = False,
    ) -> Iterable[Any]:
        del prefetch_batches
        return self._rows_as_batches(
            self._apply_plan(source, plan, action_kind="iter_batches"),
            batch_size=batch_size, batch_format=batch_format,
            drop_last=drop_last,
        )

    def _rows_as_batches(
        self, rows, *, batch_size: int | None = 256,
        batch_format: str | None = "default", drop_last: bool = False,
    ):
        size = max(1, int(batch_size or self._block_size))
        iterator = iter(rows)
        while True:
            rows = list(itertools.islice(iterator, size))
            if not rows or (drop_last and len(rows) < size):
                return
            if batch_format in ("pyarrow", "default"):
                try:
                    import pyarrow as pa
                    yield pa.Table.from_pylist(rows)
                    continue
                except ImportError:
                    if batch_format == "pyarrow":
                        raise
            yield rows

    def materialize(self, source: SourcePlan, plan: LogicalPlan) -> _LocalMaterializedHandle:
        blocks = []
        rows_count = 0
        current = []
        memory_bytes = 0

        def finish_block(rows):
            nonlocal memory_bytes
            block = tuple(rows)
            size = len(pickle.dumps(block, protocol=pickle.HIGHEST_PROTOCOL))
            if memory_bytes + size <= self._materialize_memory_limit:
                memory_bytes += size
                return block
            fd, path = tempfile.mkstemp(prefix="demiflow-block-", suffix=".pkl")
            os.close(fd)
            with open(path, "wb") as fh:
                pickle.dump(block, fh, protocol=pickle.HIGHEST_PROTOCOL)
            self._spill_paths.add(path)
            return _SpilledBlock(path)

        for row in self._apply_plan(source, plan, action_kind="materialize"):
            current.append(row)
            rows_count += 1
            if len(current) >= self._block_size:
                blocks.append(finish_block(current))
                current = []
        if current:
            blocks.append(finish_block(current))
        self._last_metadata = ExecutionMetadata(
            source_name=type(source).__name__, rows_output=rows_count, blocks_output=len(blocks)
        )
        return _LocalMaterializedHandle(tuple(blocks), rows_count)

    def count(self, source: SourcePlan, plan: LogicalPlan) -> int:
        if isinstance(source, MaterializedSource) and plan.is_empty:
            handle = source.handle
            if isinstance(handle, _LocalMaterializedHandle):
                return handle.row_count
        return super().count(source, plan)

    def take(
        self, source: SourcePlan, plan: LogicalPlan, limit: int,
    ) -> list[dict[str, Any]]:
        return list(itertools.islice(self.iter_rows(source, plan), limit))

    def take_batch(
        self, source: SourcePlan, plan: LogicalPlan, batch_size: int,
        *, batch_format: str,
    ) -> Any:
        rows = self.take(source, plan, batch_size)
        return self._rows_to_batch(rows, batch_format)

    def aggregate(
        self, source: SourcePlan, plan: LogicalPlan,
        aggregates: tuple[Any, ...],
    ) -> Any:
        for aggregate in aggregates:
            self._validate_serialized(aggregate, f"aggregation {aggregate.name!r}")
            self._validate_state(aggregate.initial_state(), aggregate.name)

        states = [aggregate.initial_state() for aggregate in aggregates]
        iterator = iter(self._apply_plan(source, plan, action_kind="aggregate"))
        observer = current_action_observer()
        saw_rows = False
        merge_count = 0
        while True:
            block = tuple(itertools.islice(iterator, self._block_size))
            if not block:
                break
            saw_rows = True
            if observer is not None:
                observer.progress(rows=len(block), batches=1)
            for index, aggregate in enumerate(aggregates):
                partial = aggregate.aggregate_partial(block)
                self._validate_state(partial, aggregate.name)
                states[index] = aggregate.merge_states(states[index], partial)
                self._validate_state(states[index], aggregate.name)
            merge_count += 1

        if observer is not None:
            observer.set_phase("finalizing", merge_count=merge_count)
        if not saw_rows:
            return None
        result = {}
        for aggregate, state in zip(aggregates, states):
            value = aggregate.finalize_state(state)
            self._validate_bounded(value, aggregate.name, "result")
            result[aggregate.name] = value
        return result

    def _validate_serialized(self, value: Any, label: str) -> bytes:
        try:
            return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as exc:
            raise AggregateSerializationError(f"{label} is not serializable: {exc}") from exc

    def _validate_state(self, state: Any, name: str) -> None:
        self._validate_bounded(state, name, "state")

    def _validate_bounded(self, value: Any, name: str, kind: str) -> None:
        raw = self._validate_serialized(value, f"{kind} for aggregation {name!r}")
        if len(raw) > self._aggregate_state_max_bytes:
            raise AggregateStateLimitExceeded(
                f"{kind} for aggregation {name!r} exceeds "
                f"{self._aggregate_state_max_bytes} bytes"
            )

    def schema(self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool = True) -> Any:
        if not fetch_if_missing:
            return None
        rows = list(itertools.islice(self._apply_plan(source, plan, action_kind="schema"), 1))
        if not rows:
            return None
        try:
            import pyarrow as pa
            return pa.Table.from_pylist(rows).schema
        except ImportError:
            return {key: type(value).__name__ for key, value in rows[0].items()}

    def columns(
        self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool,
    ) -> list[str] | None:
        schema = self.schema(source, plan, fetch_if_missing=fetch_if_missing)
        if schema is None:
            return None
        names = getattr(schema, "names", None)
        return list(names if names is not None else schema)

    def size_bytes(self, source: SourcePlan, plan: LogicalPlan) -> int:
        total = 0
        iterator = iter(self._apply_plan(source, plan, action_kind="size_bytes"))
        while block := tuple(itertools.islice(iterator, self._block_size)):
            total += len(pickle.dumps(block, protocol=pickle.HIGHEST_PROTOCOL))
        return total

    def num_blocks(self, source: SourcePlan, plan: LogicalPlan) -> int:
        if not isinstance(source, MaterializedSource) or not plan.is_empty:
            raise TypeError("num_blocks is only available on MaterializedDataset")
        handle = source.handle
        if not isinstance(handle, _LocalMaterializedHandle):
            raise UnsupportedSourceError("materialized source belongs to another backend")
        return len(handle.blocks)

    def stats(self, source: SourcePlan, plan: LogicalPlan) -> str:
        if self._last_metadata is None:
            return f"Dataset({type(source).__name__}, operations={len(plan.operations)}, not executed)"
        return str(self._last_metadata)

    def execution_metadata(self, source: SourcePlan, plan: LogicalPlan) -> ExecutionMetadata | None:
        return self._last_metadata

    def write_datasink(
        self, source: SourcePlan, plan: LogicalPlan, datasink: Datasink,
        *, native_options=None,
    ) -> WriteResult:
        if native_options is not None:
            raise UnsupportedExecutionOptionError("Ray native options require a Ray target")
        physical=self.plan(source,plan,"write_datasink",terminal_category="sink")
        try:
            datasink.on_write_start(self.schema(source, plan, fetch_if_missing=False))
            rows=self._apply_plan(source,plan,action_kind="write_datasink",terminal_category="sink",physical_plan=physical)
            blocks=self._rows_as_batches(rows,batch_format="pyarrow")
            raw = datasink.write(blocks, WriteContext(task_index=0))
            if isinstance(raw, WriteResult):
                result = raw
            elif isinstance(raw, int):
                result = WriteResult(written_rows=raw)
            elif isinstance(raw, Mapping):
                result = WriteResult(**raw)
            else:
                raise TypeError("Datasink.write must return WriteResult, int, or mapping")
            datasink.on_write_complete(result)
            return result
        except Exception as exc:
            try:
                datasink.on_write_failed(exc)
            except Exception:
                pass
            raise

    def write_lance(self, source, plan, spec, *, native_options=None):
        if native_options is not None:
            raise UnsupportedExecutionOptionError("Ray native options require a Ray target")
        physical=self.plan(source,plan,"write_lance",terminal_category="sink",terminal_parallelism_cap=1)
        from ...lance.write import append_lance
        rows=self._apply_plan(source,plan,action_kind="write_lance",terminal_category="sink",physical_plan=physical)
        batches=self._rows_as_batches(rows,batch_size=self._block_size,batch_format="pyarrow")
        return append_lance(spec,batches)

    def write_file(
        self, source: SourcePlan, plan: LogicalPlan, format_name: str,
        path: str, options: dict[str, Any],
    ) -> WriteResult:
        target = Path(path)
        native=options.pop("_demiflow_native_options",None)
        unsupported = sorted(key for key in ("filesystem",) if options.get(key) is not None)
        if native is not None: unsupported.append("backend_options")
        if unsupported:
            raise UnsupportedExecutionOptionError(
                f"LocalDatasetExecutor write_{format_name} does not support: {unsupported}"
            )
        for key in ("filesystem",):
            options.pop(key, None)
        target.parent.mkdir(parents=True, exist_ok=True)
        physical=self.plan(source,plan,f"write_{format_name}",terminal_category="sink")
        rows = iter(self._apply_plan(source, plan, action_kind=f"write_{format_name}", terminal_category="sink",physical_plan=physical))
        written = 0
        blocks = 0
        if format_name == "json":
            lines = bool(options.pop("lines", True))
            if options:
                raise TypeError(f"unsupported local write_json options: {sorted(options)}")
            if not lines:
                raise UnsupportedExecutionOptionError(
                    "local write_json requires lines=True for bounded streaming"
                )
            with target.open("w", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
                    written += 1
        elif format_name == "csv":
            if options:
                raise TypeError(f"unsupported local write_csv options: {sorted(options)}")
            first = next(rows, None)
            fields = list(first) if first is not None else []
            with target.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fields)
                writer.writeheader()
                if first is not None:
                    writer.writerow(first)
                    written = 1
                for row in rows:
                    writer.writerow(row)
                    written += 1
        elif format_name == "parquet":
            import pyarrow as pa
            import pyarrow.parquet as pq
            writer = None
            try:
                while True:
                    chunk = list(itertools.islice(rows, self._block_size))
                    if not chunk:
                        break
                    table = pa.Table.from_pylist(chunk)
                    if writer is None:
                        writer = pq.ParquetWriter(target, table.schema, **options)
                    writer.write_table(table)
                    written += len(chunk)
                    blocks += 1
            finally:
                if writer is not None:
                    writer.close()
            if writer is None:
                pq.write_table(pa.Table.from_pylist([]), target, **options)
        else:
            raise UnsupportedSourceError(f"LocalDatasetExecutor does not support write_{format_name}")
        return WriteResult(
            written_rows=written, blocks_written=blocks, target=str(target),
            metadata={"format": format_name, "size_bytes": target.stat().st_size},
        )

    def close(self) -> None:
        if self._pool is not None:
            self._pool.shutdown()
            self._pool = None
        for path in list(self._spill_paths):
            try:
                os.unlink(path)
            except OSError:
                pass
            self._spill_paths.discard(path)

def _host_memory_bytes():
    try:
        page=os.sysconf("SC_PAGE_SIZE"); pages=os.sysconf("SC_PHYS_PAGES")
        return max(0,int(page)*int(pages))
    except (ValueError,OSError,AttributeError):
        return 0
