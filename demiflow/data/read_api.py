"""Public Dataset source functions; backend binding stays inside the platform.

Each function delegates to the existing reader implementation. A Pipeline driver
selects its executor; outside a driver the returned Dataset owns a local executor.
No prompt configuration, request budget or business state is registered here.
"""

from __future__ import annotations

from typing import Any, Callable, Iterator, Mapping, Sequence
from .flow_contract import flow_api as _flow_api
from .api import DataAPI, _select_executor
from .execution_contract import InputSpec, _current_executor
from .dataset import Dataset, MaterializedDataset
from demiflow.execution.executors import queue_flow as _queue_flow


@_flow_api('read_queue', execution='stream', effects=('queue',),
           normalize=_queue_flow.normalize_source, analyze=_queue_flow.analyze_untyped)
def read_queue(config, *, pool='default', settlement='sink', max_inflight=8,
               idle_timeout_s=300, lease_s=60, max_tasks=10000, stop=None) -> Dataset:
    """Claim unfinished tasks through the native queue datasource.

    Open-empty is not EOS. Historical completed rows are read explicitly with
    read_queue_records; workers never scan/replay the pool's completed history.
    """
    from demiflow.execution.native_queue.queue_source import QueueDatasource
    source=QueueDatasource(config,pool=pool,settlement=settlement,max_inflight=max_inflight,
        idle_timeout_s=idle_timeout_s,lease_s=lease_s,max_tasks=max_tasks,stop=stop)
    return DataAPI(_current_executor.get()).read_datasource(source)


@_flow_api('read_queue_records', effects=('queue_read', 'objects_read'),
           normalize=_queue_flow.normalize_history, analyze=_queue_flow.analyze_untyped)
def read_queue_records(config, *, pool, through, results=False, verify='artifacts', limits=None, after=None) -> Dataset:
    """Read a fixed publication prefix without leases, repair, GET or callbacks.

    Default inspection hashes referenced artifacts. Explicit metadata inspection
    adds verification.status=not_checked. Finite limits raise HistoryReadLimit
    carrying the first undelivered cursor bound to this exact QueueSnapshot.
    """
    from demiflow.execution.sources.queue_history import QueueHistoryDatasource
    source=QueueHistoryDatasource(config,pool=pool,through=through,results=results,verify=verify,limits=limits,after=after)
    return DataAPI(_current_executor.get()).read_datasource(source)



def read_legacy_queue_records(path, *, sha256, pool, through, results=False, limits=None,
                              max_journal_bytes=1024**3, after=None) -> Dataset:
    """Read a closed v1 SQLite backup by exact SHA256, without v2 certification.

    This archive-only adapter has no publication, claim, repair or ACK methods.
    It preserves the stored v1 values and marks them legacy_not_certified.
    """
    from demiflow.execution.sources.legacy_queue import LegacyQueueHistoryDatasource
    source=LegacyQueueHistoryDatasource(path,sha256=sha256,pool=pool,through=through,results=results,
        limits=limits,max_journal_bytes=max_journal_bytes,after=after)
    return DataAPI(_current_executor.get()).read_datasource(source)


def read_document_receipts(path: str, *, sha256: str, max_journal_bytes: int = 8*1024**3,
                           max_receipt_bytes: int = 8*1024**2, max_rows: int = 2_000_000) -> Dataset:
    """Read native fetch receipts from a closed SQLite backup pinned by SHA256.

    Emits receipt_id/receipt (DOCUMENT_RESULT), including failed fetches. Reads
    one JSON value at a time; bounds file/value/row growth and SQLite cache.
    It neither fetches nor edits the original journal. Materialize to a fixed
    Lance source before business joins. Limits are not a whole-process RSS cap.
    """
    from ..collect.receipt_source import document_receipt_factory
    factory = document_receipt_factory(path,sha256=sha256,max_journal_bytes=max_journal_bytes,
        max_receipt_bytes=max_receipt_bytes,max_rows=max_rows)
    return DataAPI(_current_executor.get()).from_iter(factory)


@_flow_api('from_items', execution='bridge', role='bridge')
def from_items(items: list[Any]) -> MaterializedDataset:
    """Create a materialized Dataset from a bounded Driver list.

    ``items`` must already be a Python ``list``; iterators and generators
    are not accepted. Use this for genuinely small Driver-created values,
    such as one aggregate-analysis request. Prefer lazy external sources for
    external data. Do not collect an external Dataset and pass its rows back
    to ``from_items`` solely to write the same detail rows."""
    from demiflow.execution.executors.base import prepare_public_items
    from .plan import LogicalPlan
    from .sources import MaterializedSource
    handle, executor = prepare_public_items(
        InputSpec('public_items', items),
        select_executor=lambda: _select_executor(_current_executor.get()),
    )
    return MaterializedDataset(MaterializedSource(handle, handle.row_count), LogicalPlan(), executor)


def from_iter(factory: Callable[[], Iterator[Any]]) -> Dataset:
    """Create a lazy Dataset from a zero-arg iterator factory.

    惰性流式源（2026-09-08 新增）：``factory`` 每次调用产出新迭代器，
    行在终结动作执行时才被拉取（run_stream 的 feed 分块拉取、级间
    有界队列背压），适合大文件逐条解析。队列项数不是进程内存上界；
    source 预读、行载荷、转换副本、在途结果和第三方分配仍须分别预算。
    迭代器/生成器不物化、不序列化，仅本地执行路径支持；跨节点分布请用
    可再分布的数据源。"""
    return DataAPI(_current_executor.get()).from_iter(factory)


def read_records(
    paths, *, format='jsonl', item_prefix=None, max_records=None, report_path=None, missing='error'
) -> Dataset:
    """Read local JSONL/gzip, JSON items, or text with path/row/error provenance.

    Rows contain value/path/row/error/raw. Malformed lines are retained;
    use a business map/flat_map to interpret values and route errors.
    JSON containers stream through optional ijson; max_records is per file."""
    return DataAPI(_current_executor.get()).read_records(
        paths,
        format=format,
        item_prefix=item_prefix,
        max_records=max_records,
        report_path=report_path,
        missing=missing,
    )


@_flow_api('range')
def range(n: int, *, backend_options=None) -> Dataset:
    """Create a lazy Dataset of ``{"id": value}`` rows in ``[0, n)``."""
    return DataAPI(_current_executor.get()).range(n, backend_options=backend_options)


def from_arrow(tables: Any) -> MaterializedDataset:
    """Create a current-run materialized Dataset from bounded Arrow table data."""
    return DataAPI(_current_executor.get()).from_arrow(tables)


def from_numpy(arrays: Any) -> MaterializedDataset:
    """Create a current-run materialized Dataset from bounded NumPy array data."""
    return DataAPI(_current_executor.get()).from_numpy(arrays)


def from_pandas(frames: Any) -> MaterializedDataset:
    """Create a current-run materialized Dataset from bounded pandas frame data."""
    return DataAPI(_current_executor.get()).from_pandas(frames)


def read_parquet(
    paths: str | Sequence[str],
    *,
    filesystem=None,
    columns=None,
    partition_filter=None,
    include_paths: bool = False,
    backend_options=None,
    **arrow_parquet_args: Any,
) -> Dataset:
    """Create a lazy Dataset from one or more Parquet locations using backend-supported options."""
    return DataAPI(_current_executor.get()).read_parquet(
        paths,
        filesystem=filesystem,
        columns=columns,
        partition_filter=partition_filter,
        include_paths=include_paths,
        backend_options=backend_options,
        **arrow_parquet_args,
    )


def read_json(
    paths: str | Sequence[str],
    *,
    filesystem=None,
    partition_filter=None,
    partitioning=None,
    include_paths: bool = False,
    backend_options=None,
    **arrow_json_args: Any,
) -> Dataset:
    """Create a lazy Dataset from JSON or JSON-Lines locations.

    ``paths`` may be one location or a sequence. Distributed Ray output may
    be a directory containing part files rather than one physical file.
    Reading starts only when a terminal action executes the plan; use
    ``take`` for bounded inspection and do not assume global row order."""
    return DataAPI(_current_executor.get()).read_json(
        paths,
        filesystem=filesystem,
        partition_filter=partition_filter,
        partitioning=partitioning,
        include_paths=include_paths,
        backend_options=backend_options,
        **arrow_json_args,
    )


def read_csv(
    paths: str | Sequence[str],
    *,
    filesystem=None,
    partition_filter=None,
    partitioning=None,
    include_paths: bool = False,
    backend_options=None,
    **arrow_csv_args: Any,
) -> Dataset:
    """Create a lazy Dataset from one or more CSV locations using backend-supported options."""
    return DataAPI(_current_executor.get()).read_csv(
        paths,
        filesystem=filesystem,
        partition_filter=partition_filter,
        partitioning=partitioning,
        include_paths=include_paths,
        backend_options=backend_options,
        **arrow_csv_args,
    )


def read_text(
    paths: str | Sequence[str],
    *,
    encoding: str = 'utf-8',
    drop_empty_lines: bool = True,
    filesystem=None,
    include_paths: bool = False,
    backend_options=None,
) -> Dataset:
    """Create a lazy Dataset from text files with explicit encoding and empty-line behavior."""
    return DataAPI(_current_executor.get()).read_text(
        paths,
        encoding=encoding,
        drop_empty_lines=drop_empty_lines,
        filesystem=filesystem,
        include_paths=include_paths,
        backend_options=backend_options,
    )


def read_binary_files(
    paths: str | Sequence[str], *, include_paths: bool = False, filesystem=None, backend_options=None
) -> Dataset:
    """Create a lazy Dataset of binary file records; optionally include source paths."""
    return DataAPI(_current_executor.get()).read_binary_files(
        paths, include_paths=include_paths, filesystem=filesystem, backend_options=backend_options
    )


def read_images(
    paths: str | Sequence[str],
    *,
    filesystem=None,
    size: tuple[int, int] | None = None,
    mode: str | None = None,
    include_paths: bool = False,
    backend_options=None,
) -> Dataset:
    """Create a lazy Dataset from image files with backend-supported decoding options."""
    return DataAPI(_current_executor.get()).read_images(
        paths,
        filesystem=filesystem,
        size=size,
        mode=mode,
        include_paths=include_paths,
        backend_options=backend_options,
    )


def read_sql(
    sql: str, connection_factory: Callable[[], Any], *, backend_options=None, **options: Any
) -> Dataset:
    """Create a lazy Dataset from a SQL query and serializable worker connection factory."""
    return DataAPI(_current_executor.get()).read_sql(
        sql, connection_factory, backend_options=backend_options, **options
    )


def read_datasource(datasource, *, backend_options=None, **read_args: Any) -> Dataset:
    """Create a lazy Dataset from a backend-neutral Datasource; IO begins only at an action."""
    return DataAPI(_current_executor.get()).read_datasource(
        datasource, backend_options=backend_options, **read_args
    )


@_flow_api("read_table", effects=("read_table",), version=2)
def read_lance(
    uri: str,
    *,
    version: int | None = None,
    columns: Sequence[str] | None = None,
    filter: str | None = None,
    limit: int | None = None,
    storage_options: Mapping[str, str] | None = None,
    batch_size: int | None = None,
    batch_readahead: int | None = None,
    fragment_readahead: int | None = None,
    projection: Mapping[str, str] | None = None,
    backend_options=None,
) -> Dataset:
    """Create a lazy Dataset from a Lance scan.

    ``version`` selects an exact positive Lance version; ``None`` resolves
    the head when the action starts. ``limit`` is global, so Ray executes a
    limited scan as one read task. ``storage_options`` contains only closed
    non-secret Lance connection values, while ``backend_options`` contains
    only Demiflow physical scheduling options. Formal Candidates should bind
    fixed inputs to an exact version obtained from authorized inspection.
    ``projection`` maps output names to Lance SQL expressions (for example
    ``{'item_n': 'array_length(items)'}``), mutually exclusive with ``columns``.
    Expressions execute in Lance; this does not promise storage-level pruning
    of every referenced nested field. Nulls and projected Arrow types survive.
    """
    return DataAPI(_current_executor.get()).read_lance(
        uri,
        version=version,
        columns=columns,
        filter=filter,
        limit=limit,
        storage_options=storage_options,
        batch_size=batch_size,
        batch_readahead=batch_readahead,
        fragment_readahead=fragment_readahead,
        projection=projection,
        backend_options=backend_options,
    )


def vector_search_lance(
    uri: str,
    vector: Sequence[float],
    *,
    vector_column: str,
    top_k: int,
    version: int | None = None,
    columns: Sequence[str] | None = None,
    filter: str | None = None,
    metric: str | None = None,
    storage_options: Mapping[str, str] | None = None,
    backend_options=None,
) -> Dataset:
    """Create a lazy Dataset from one bounded Lance nearest-vector query.

    Ray executes vector search as one read task so ``top_k`` remains a
    global bound. The vector must match a fixed-size floating Lance column;
    results include Lance's ``_distance`` field."""
    return DataAPI(_current_executor.get()).vector_search_lance(
        uri,
        vector,
        vector_column=vector_column,
        top_k=top_k,
        version=version,
        columns=columns,
        filter=filter,
        metric=metric,
        storage_options=storage_options,
        backend_options=backend_options,
    )
