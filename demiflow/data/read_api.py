"""Public Dataset source functions; backend binding stays inside the platform.

Each function delegates to the existing reader implementation. A Pipeline driver
selects its executor; outside a driver the returned Dataset owns a local executor.
No prompt configuration, request budget or business state is registered here.
"""

from __future__ import annotations

from typing import Any, Callable, Iterator, Mapping, Sequence
from .api import DataAPI, _current_executor
from .dataset import Dataset, MaterializedDataset


def read_queue(config, *, pool='default', stop=None, idle_timeout_s=1800) -> Dataset:
    """Consume embedded SQLiteQueue tasks; run_stream defaults to one-row delivery.

    Producer opens/seals its named channel explicitly. Stored completions replay
    in _queue_result; fresh payloads carry _queue_delivery until ack_queue.
    """
    from ..execution.sqlite_channel import QueueReader, channel_config
    if type(idle_timeout_s) not in (int,float) or not 0<idle_timeout_s<=86400:
        raise ValueError('Queue idle deadline must be in (0, 86400] seconds')
    actor=QueueReader(channel_config(**config),pool,stop,idle_timeout_s)
    ds=DataAPI(_current_executor.get()).from_iter(actor.rows)
    return Dataset(ds._source,ds._plan,ds._executor,(actor,))


def read_queue_records(config, *, pool, through, results=False) -> Dataset:
    """Scan immutable queue records at an explicit sequence, without claiming."""
    from ..execution.sqlite_channel import SQLiteChannel, channel_config
    settings=channel_config(**config)
    return DataAPI(_current_executor.get()).from_iter(
        lambda:SQLiteChannel.read_only(**settings).scan(pool,through=through,results=results))


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


def from_items(items: list[Any]) -> MaterializedDataset:
    """Create a materialized Dataset from a bounded Driver list.

    ``items`` must already be a Python ``list``; iterators and generators
    are not accepted. Use this for genuinely small Driver-created values,
    such as one aggregate-analysis request. Prefer lazy external sources for
    external data. Do not collect an external Dataset and pass its rows back
    to ``from_items`` solely to write the same detail rows."""
    return DataAPI(_current_executor.get()).from_items(items)


def from_iter(factory: Callable[[], Iterator[Any]]) -> Dataset:
    """Create a lazy Dataset from a zero-arg iterator factory.

    惰性流式源（2026-09-08 新增）：``factory`` 每次调用产出新迭代器，
    行在终结动作执行时才被拉取（run_stream 的 feed 分块拉取、级间
    有界队列背压），内存上界=队列深度×行载荷而非全量——大文件流式
    解析（如 Wikipedia dump 逐页喂入）的入口。迭代器/生成器不物化、
    不序列化，仅本地执行路径支持；跨节点分布请用可再分布的数据源。"""
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
