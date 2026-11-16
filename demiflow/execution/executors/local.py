"""Local host adapter for the unified v3 dataflow engine.

LocalDatasetExecutor no longer owns execution, compilation or binding authority:
every action is forwarded to the single ``DataflowExecution`` entry through
``make_action_request``/``run_dataset_action`` (E lane). Local only provides the
host and resource carrying (worker pool, spill scope, input handles, usage
accounting). The retired private execution route (``_apply_plan``/``_bind_plan``/
``_execute_plan``/``_execution_capabilities``/``_require_ray_backend``/
``_write_file_body``) has been removed; there is no second default executor.

A few private helpers below are retained solely because other lanes' baseline
modules still consume them (see the lane L delivery receipt for the per-symbol
retention justification); they are not reachable from the public action path.
"""

from __future__ import annotations

import csv
import itertools
import json
import os
import pickle
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional

from ...data.datasink import Datasink, WriteResult
from ...data.observability import DatasourceReadObserver, block_row_count
from ...errors import (
    UnsupportedExecutionOptionError,
    UnsupportedSourceError,
)
from ...data.plan import (
    LogicalPlan, MapBatchesOp, StandardCallable,
    reject_streaming_map_options,
)
from ...data.sources import DatasourceSource, FileSource, IterableSource, ItemsSource, LanceSource, MaterializedSource, RangeSource, SourcePlan, SqlSource
from ...data.constraints import SourceReadConstraints
from ...observability import current_action_observer
from ...data.stats import ExecutionMetadata
from .base import DatasetExecutor


@dataclass
class _LocalMaterializedHandle:
    blocks: Iterable[Any]
    row_count: int
    released: bool = False
    recovery_reference: Any = None

    def check_open(self):
        if self.released:
            raise RuntimeError('Materialized Dataset was released')


class _CheckpointBlocks:
    """Repeatable lazy block paths backed by immutable checkpoint pages."""
    def __init__(self, terminal):
        self.run, self.action_id = terminal.run, terminal.action.action_id
        self.sequence, self.reader, self.has_objects = terminal.sequence, terminal.row_sink, terminal.has_objects
        self.sink, self.coordinator = terminal.sink, terminal.coordinator

    def __iter__(self):
        for fact in self.run.journal.iter_execution_commits(self.action_id, through_sequence=self.sequence):
            # Historical rows also own object publications. Verify the complete
            # native receipt lazily before exposing this generation's blocks.
            receipt = self.sink.inspect(fact['prepared']['sinks']['output'],
                                        self.coordinator._context(fact['commit_id']))
            if receipt != fact['receipts']['output']:
                from ..recovery_contract import RecoveryContractError
                raise RecoveryContractError('Completed receipt differs from artifact evidence')
            value = receipt['data']
            if self.has_objects:
                value = value['rows']
            for block in self.reader.block_entries(value):
                yield _SpilledBlock(str(self.run.directory / block['path']), durable=True)

    def __len__(self):
        return sum(1 for _ in self)


@dataclass(frozen=True)
class _SpilledBlock:
    path: str
    durable: bool = False


@dataclass(frozen=True)
class _EncodedBlock:
    rows: tuple[bytes, ...]


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
        resource_root=None,
        usage_callback=None,
        planning_policy=None,
        candidate_execution=None,
        local_kernel=None,
        sort_workers: int | None = None,
        unified_chain: bool = False,
        unified_graph: bool = False,
        unified_queue_chain: bool = False,
    ) -> None:
        self._workers = max(1, int(workers))
        # Compatibility-only flags: they no longer select an execution engine.
        # Every public action runs through the unified v3 entry regardless.
        self._unified_chain = bool(unified_chain)
        self._unified_graph = bool(unified_graph)
        self._unified_queue_chain = bool(unified_queue_chain)
        if sort_workers is None:
            sort_workers = os.environ.get('DEMIFLOW_LOCAL_SORT_WORKERS',
                           os.environ.get('DEMIWFLOW_LOCAL_SORT_WORKERS', self._workers))
        if (isinstance(sort_workers, bool) or not isinstance(sort_workers, (int, str))
                or not str(sort_workers).isdigit() or int(sort_workers) < 1):
            raise ValueError('sort_workers must be a positive integer')
        self._sort_workers = int(sort_workers)
        from ...planning.policy import parse_platform_planning_policy
        self._planning_policy = planning_policy or parse_platform_planning_policy(None)
        self._candidate_execution = candidate_execution
        self._local_kernel = local_kernel
        self._local_kernel_closed = False
        self._local_kernel_stats = {}
        if local_kernel is not None and local_kernel.workers > self._planning_policy.max_parallelism:
            raise ValueError('local kernel workers exceed the platform max_parallelism')
        self._active_physical_plan = None
        self._block_size = max(1, int(block_size))
        self._materialize_memory_limit = max(1, int(materialize_memory_limit))
        self._aggregate_state_max_bytes = max(1, int(aggregate_state_max_bytes))
        self._pool: Optional[ThreadPoolExecutor] = None
        self._last_metadata: Optional[ExecutionMetadata] = None
        self._run_ref = None
        self._spill_paths: set[str] = set()
        self._spill_bytes = 0
        self.resource_root = Path(resource_root or '.').resolve()
        self._prompt_coordinators = {}
        self._usage_callback = usage_callback

    def _prompt_coordinator(self, operation):
        from ...operator_llm.runtime import InProcessOperatorLLMCoordinator
        # Workers of one node share accounting; different nodes never share limits.
        return self._prompt_coordinators.setdefault(
            operation.node_id,
            InProcessOperatorLLMCoordinator(operation.max_requests, on_change=self._usage_callback),
        )

    def prompt_actor(self, operation, *, service=None):
        from ...operator_llm.runtime import PromptActor
        return PromptActor(operation, operation.config,
                           self._prompt_coordinator(operation), operation.options, service=service)

    def operator_llm_usage(self) -> dict[str, int]:
        from ..dataflow_usage import executor_prompt_usage
        return executor_prompt_usage(self)

    def plan(
        self, source, plan, action_kind, *, terminal_category="action",
        terminal_native_options=None, source_parallelism_cap=None,
        terminal_parallelism_cap=None,
    ):
        reject_streaming_map_options(plan, 'local')
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
        return explain_dataset(source, plan, provider=self.NAME, options=options)

    @property
    def executor(self) -> ThreadPoolExecutor:
        if self._pool is None:
            self._pool = ThreadPoolExecutor(max_workers=self._workers)
        return self._pool

    def from_items(self, items: list[Any], *, override_num_blocks: int | None = None) -> Any:
        if not isinstance(items, list):
            raise TypeError("from_items requires a materialized list")
        from ..dataflow_inputs import build_input_handle
        return build_input_handle(items, format='items', override_num_blocks=override_num_blocks)

    def from_arrow(self, tables: Any) -> Any:
        from ..dataflow_inputs import build_input_handle
        return build_input_handle(tables, format='arrow')

    def from_numpy(self, arrays: Any) -> Any:
        from ..dataflow_inputs import build_input_handle
        return build_input_handle(arrays, format='numpy')

    def from_pandas(
        self, frames: Any, *, override_num_blocks: int | None = None,
    ) -> Any:
        from ..dataflow_inputs import build_input_handle
        return build_input_handle(frames, format='pandas', override_num_blocks=override_num_blocks)

    def _iter_block_rows(self, block: Any) -> Iterator[Any]:
        if block is None:
            return
        if isinstance(block, Mapping):
            yield dict(block)
            return
        if isinstance(block, _SpilledBlock):
            with open(block.path, "rb") as fh:
                while True:
                    try:
                        row = pickle.load(fh)
                    except EOFError:
                        return
                    yield row
            return
        if isinstance(block, _EncodedBlock):
            for payload in block.rows:
                yield pickle.loads(payload)
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
                handle.check_open()
                for block in handle.blocks:
                    yield from self._iter_block_rows(block)
                return
            if isinstance(source, ItemsSource):
                yield from source.items
                return
            if isinstance(source, IterableSource):
                # factory 每次动作产新迭代器；行不物化（流式消费契约）
                yield from iter(source.factory())
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
            if options and not (set(options) - {'read_options', 'parse_options', 'convert_options'}):
                from ..csv_source import arrow_csv_rows
                yield from arrow_csv_rows(source)
                return
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
        self, rows: Iterable[Any], operation: MapBatchesOp, width: int, *, max_bytes=None,
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
            from ...data.runtime_contract import check_size
            batch = self._rows_to_batch(list(batch_rows), operation.batch_format)
            result = runtime(batch)
            output, used = [], 0
            results = result if isinstance(result, Iterator) else (result,)
            for item in results:
                for row in self._batch_to_rows(item):
                    if max_bytes is not None:
                        used += check_size(row, max_bytes) + 64
                        if used > max_bytes:
                            from ..recovery_contract import RecoveryContractError
                            raise RecoveryContractError('Python batch output exceeds sealed byte budget')
                    output.append(row)
            return output

        def batches():
            from ...data.runtime_contract import check_size
            while True:
                values, used = [], 0
                for row in itertools.islice(iterator, size):
                    if max_bytes is not None:
                        used += check_size(row, max_bytes) + 64
                        if used > max_bytes:
                            from ..recovery_contract import RecoveryContractError
                            raise RecoveryContractError('Declared Python batch exceeds sealed byte budget')
                    values.append(row)
                if not values:
                    return
                yield values
        for output_rows in self._map_stage(batches(), apply, width):
            yield from output_rows

    def iter_rows(self, source: SourcePlan, plan: LogicalPlan) -> Iterable[dict[str, Any]]:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='iter_rows', arguments={})
        return run_dataset_action(request)

    def iter_batches(
        self, source: SourcePlan, plan: LogicalPlan, *, prefetch_batches: int = 1,
        batch_size: int | None = 256, batch_format: str | None = "default", drop_last: bool = False,
    ) -> Iterable[Any]:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='iter_batches', arguments={
            'prefetch_batches': prefetch_batches, 'batch_size': batch_size,
            'batch_format': batch_format, 'drop_last': drop_last,
        })
        return run_dataset_action(request)

    def _rows_as_batches(
        self, rows, *, batch_size: int | None = 256,
        batch_format: str | None = "default", drop_last: bool = False, schema=None, byte_limits=None,
    ):
        size = max(1, int(batch_size or self._block_size))
        iterator = iter(rows)
        def groups():
            if byte_limits is None:
                while batch := list(itertools.islice(iterator, size)):
                    yield batch
                return
            from ...data.runtime_contract import check_size
            from ..payload_limits import batch_target
            target = batch_target(byte_limits)
            batch, retained = [], 0
            for row in iterator:
                cost = check_size(row, byte_limits['max_row_bytes'])
                if batch and (len(batch) == size or retained + cost > target):
                    yield batch
                    batch, retained = [], 0
                batch.append(row)
                retained += cost
                if len(batch) == size or retained >= target:
                    yield batch
                    batch, retained = [], 0
            if batch:
                yield batch
        for rows in groups():
            if not rows or (drop_last and len(rows) < size):
                return
            if batch_format in ("pyarrow", "default"):
                try:
                    import pyarrow as pa
                    if schema is not None:
                        allowed = set(schema.names)
                        if any(set(row) - allowed for row in rows):
                            raise ValueError('Row has fields outside the declared sink schema')
                    yield pa.Table.from_pylist(rows, schema=schema)
                    continue
                except ImportError:
                    if batch_format == "pyarrow":
                        raise
            yield rows

    def materialize(self, source: SourcePlan, plan: LogicalPlan) -> Any:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='materialize', arguments={})
        result = run_dataset_action(request)
        self._last_metadata = ExecutionMetadata(source_name=type(source).__name__)
        return result

    def materialize_stream(self, dataset, *, source=None, plan=None) -> Any:
        from ..dataflow_api import make_action_request, run_dataset_action
        selected_source = dataset._source if source is None else source
        selected_plan = dataset._plan if plan is None else plan
        request = make_action_request(self, selected_source, selected_plan, kind='materialize', arguments={})
        result = run_dataset_action(request)
        self._last_metadata = ExecutionMetadata(source_name=type(selected_source).__name__)
        return result

    def materialize_graph(self, graph_spec):
        # graph_spec is translated to a standard v3 request by run_local_graph
        # (F015), which forwards to the same execute_action entry; there is no
        # independent graph driver here.
        from ..graph import run_local_graph
        from ..run_context import current_action
        if current_action() is None:
            raise RuntimeError("Graph materialization requires the native action context")
        return run_local_graph(graph_spec, _executor=self)

    def _materialize_blocks(self, source, execute) -> _LocalMaterializedHandle:
        """通用块缓存；失败时清理本次溢写，不返回部分结果或删除其他缓存。"""
        blocks = []
        rows_count = 0
        current = []
        current_bytes = 0
        memory_bytes = 0
        created_spills = []
        # Row count alone cannot bound wide Python rows. Serialize each row once,
        # cap encoded blocks by bytes, and restore spilled rows one at a time.
        block_bytes = max(1, min(self._materialize_memory_limit, 4 * 1024 * 1024))

        def finish_block(rows, size):
            nonlocal memory_bytes
            if memory_bytes + size <= self._materialize_memory_limit:
                memory_bytes += size
                return _EncodedBlock(tuple(rows))
            from ..checked import current as checked_current
            checked = checked_current()
            payload_bytes = sum(len(payload) for payload in rows)
            if checked is not None and self._spill_bytes + payload_bytes > checked.limits['max_scratch_bytes']:
                from ...data.runtime_contract import RuntimeContractError
                raise RuntimeContractError('Materialize spill reservation exceeds scratch budget')
            fd, path = tempfile.mkstemp(prefix="demiflow-block-", suffix=".pkl")
            os.close(fd)
            created_spills.append(path)
            with open(path, "wb") as fh:
                for payload in rows:
                    fh.write(payload)
            self._spill_paths.add(path)
            self._spill_bytes += payload_bytes
            return _SpilledBlock(path)

        def append(row):
            nonlocal current, current_bytes, rows_count
            from ..checked import current as checked_current
            checked = checked_current()
            if checked is not None:
                from ...data.runtime_contract import check_size
                checked.check_stop()
                check_size(row, checked.limits['max_row_bytes'])
                if rows_count >= checked.limits['max_output_rows']:
                    raise ValueError('Materialize row budget exceeded')
            payload = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
            size = len(payload) + 64  # bytes object + tuple slot allowance
            if current and current_bytes + size > block_bytes:
                blocks.append(finish_block(current, current_bytes))
                current = []
                current_bytes = 0
            current.append(payload)
            current_bytes += size
            rows_count += 1
            if len(current) >= self._block_size or current_bytes >= block_bytes:
                blocks.append(finish_block(current, current_bytes))
                current = []
                current_bytes = 0
            return row

        try:
            execute(append)
            if current:
                blocks.append(finish_block(current, current_bytes))
        except BaseException:
            for path in created_spills:
                try:
                    if path in self._spill_paths:
                        self._spill_bytes -= os.stat(path).st_size
                    os.unlink(path)
                except FileNotFoundError:
                    pass
                self._spill_paths.discard(path)
            raise
        self._last_metadata = ExecutionMetadata(
            source_name=type(source).__name__, rows_output=rows_count, blocks_output=len(blocks)
        )
        return _LocalMaterializedHandle(tuple(blocks), rows_count)

    def count(self, source: SourcePlan, plan: LogicalPlan) -> int:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='count', arguments={})
        result = run_dataset_action(request)
        self._last_metadata = ExecutionMetadata(source_name=type(source).__name__)
        return result

    def take(
        self, source: SourcePlan, plan: LogicalPlan, limit: int,
    ) -> list[dict[str, Any]]:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='take', arguments={'limit': limit})
        result = run_dataset_action(request)
        self._last_metadata = ExecutionMetadata(source_name=type(source).__name__)
        return result

    def take_batch(
        self, source: SourcePlan, plan: LogicalPlan, batch_size: int,
        *, batch_format: str,
    ) -> Any:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='take_batch', arguments={
            'batch_size': batch_size, 'batch_format': batch_format,
        })
        result = run_dataset_action(request)
        self._last_metadata = ExecutionMetadata(source_name=type(source).__name__)
        return result

    def aggregate(
        self, source: SourcePlan, plan: LogicalPlan,
        aggregates: tuple[Any, ...],
    ) -> Any:
        from ..dataflow_api import make_action_request, run_dataset_action
        from ..contracts.dataflow import ActionProgram
        # The retained low-level method historically accepts no aggregations:
        # empty input -> None, nonempty input -> {}. Count on the same native
        # terminal proves emptiness without restoring a second source loop.
        empty_aggregations = not aggregates
        if empty_aggregations:
            from ...data.aggregate import Count
            aggregates = (Count(),)
        programs = tuple(
            ActionProgram(name='aggregate-' + str(index), kind='aggregate', value=aggregate)
            for index, aggregate in enumerate(aggregates)
        )
        request = make_action_request(self, source, plan, kind='aggregate',
            arguments={'names': [aggregate.name for aggregate in aggregates]},
            programs=programs)
        result = run_dataset_action(request)
        self._last_metadata = ExecutionMetadata(source_name=type(source).__name__)
        if empty_aggregations:
            return {} if result['count()'] else None
        return result

    def schema(self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool = True) -> Any:
        if not fetch_if_missing:
            return None
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='schema',
            arguments={'fetch_if_missing': fetch_if_missing})
        return run_dataset_action(request)

    def columns(
        self, source: SourcePlan, plan: LogicalPlan, *, fetch_if_missing: bool,
    ) -> list[str] | None:
        if not fetch_if_missing:
            return None
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='columns',
            arguments={'fetch_if_missing': fetch_if_missing})
        return run_dataset_action(request)

    def size_bytes(self, source: SourcePlan, plan: LogicalPlan) -> int:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='size_bytes', arguments={})
        result = run_dataset_action(request)
        self._last_metadata = ExecutionMetadata(source_name=type(source).__name__)
        return result

    def num_blocks(self, source: SourcePlan, plan: LogicalPlan) -> int:
        if not isinstance(source, MaterializedSource) or not plan.is_empty:
            raise TypeError("num_blocks is only available on MaterializedDataset")
        handle = source.handle
        num_blocks = getattr(handle, 'num_blocks', None)
        if num_blocks is None:
            raise UnsupportedSourceError("materialized source belongs to another backend")
        return int(num_blocks())

    def release(self, source):
        handle = source.handle
        if isinstance(handle, _LocalMaterializedHandle):
            for block in handle.blocks:
                if isinstance(block, _SpilledBlock) and not block.durable:
                    if block.path in self._spill_paths and Path(block.path).exists():
                        self._spill_bytes -= Path(block.path).stat().st_size
                    Path(block.path).unlink(missing_ok=True)
                    self._spill_paths.discard(block.path)
            handle.blocks = ()
            handle.released = True
            return
        release = getattr(handle, 'release', None)
        if release is None:
            raise UnsupportedSourceError('materialized source belongs to another backend')
        release()

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
    ) -> WriteResult:
        if native_options is not None:
            raise UnsupportedExecutionOptionError("Ray native options require a Ray target")
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='write_datasink',
            arguments={}, sink=datasink)
        return run_dataset_action(request)

    def write_lance(self, source, plan, spec, *, native_options=None):
        if native_options is not None:
            raise UnsupportedExecutionOptionError("Ray native options require a Ray target")
        from ..dataflow_api import make_action_request, run_dataset_action
        from ..contracts.dataflow import ActionProgram
        request = make_action_request(self, source, plan, kind='write_lance', arguments={},
            programs=(ActionProgram(name='write_lance', kind='sink', value=spec),))
        return run_dataset_action(request)

    def write_file(
        self, source: SourcePlan, plan: LogicalPlan, format_name: str,
        path: str, options: dict[str, Any],
    ) -> WriteResult:
        from ..dataflow_api import make_action_request, run_dataset_action
        request = make_action_request(self, source, plan, kind='write_file', arguments={
            'format': format_name, 'path': path, 'options': dict(options),
        })
        return run_dataset_action(request)

    def close(self) -> None:
        # Release only this executor's hosted resource scope (worker pool and
        # spill files). Unified ActionHandles are owned by their callers and
        # other runs' pins are never touched here.
        if self._pool is not None:
            self._pool.shutdown()
            self._pool = None
        for path in list(self._spill_paths):
            try:
                os.unlink(path)
            except OSError:
                pass
            self._spill_paths.discard(path)
        self._spill_bytes = 0
