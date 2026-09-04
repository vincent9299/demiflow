"""Ray Data-compatible source construction API bound to a Dataset executor."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Callable, Mapping, Optional, Sequence

from .dataset import Dataset, MaterializedDataset
from .plan import LogicalPlan
from .native_options import parse_native_options
from .sources import (
    DatasourceSource,
    FileSource,
    ItemsSource,
    LanceSource,
    MaterializedSource,
    RangeSource,
    SqlSource,
    frozen_options,
)


class DataAPI:
    def __init__(self, executor: Any) -> None:
        if executor is None:
            raise ValueError("DataAPI requires a Dataset executor")
        self._executor = executor

    def from_items(
        self,
        items: list[Any],
    ) -> MaterializedDataset:
        """Create a materialized Dataset from a bounded Driver list.

        ``items`` must already be a Python ``list``; iterators and generators
        are not accepted. Use this for genuinely small Driver-created values,
        such as one aggregate-analysis request. Prefer lazy external sources for
        external data. Do not collect an external Dataset and pass its rows back
        to ``from_items`` solely to write the same detail rows.
        """
        if not isinstance(items, list) or isinstance(items, Iterator):
            raise TypeError("from_items requires a materialized list")
        bounded_items = list(items)
        handle = self._executor.from_items(bounded_items)
        return MaterializedDataset(
            MaterializedSource(handle, len(bounded_items)), LogicalPlan(), self._executor,
        )

    def range(
        self, n: int, *, backend_options=None,
    ) -> Dataset:
        """Create a lazy Dataset of ``{"id": value}`` rows in ``[0, n)``."""
        count = int(n)
        if count < 0:
            raise ValueError("range requires a non-negative row count")
        return Dataset(
            RangeSource(count, parse_native_options(backend_options, family="source")),
            LogicalPlan(), self._executor,
        )

    def from_arrow(self, tables: Any) -> MaterializedDataset:
        """Create a current-run materialized Dataset from bounded Arrow table data."""
        handle = self._executor.from_arrow(tables)
        return MaterializedDataset(
            MaterializedSource(handle), LogicalPlan(), self._executor,
        )

    def from_numpy(self, arrays: Any) -> MaterializedDataset:
        """Create a current-run materialized Dataset from bounded NumPy array data."""
        handle = self._executor.from_numpy(arrays)
        return MaterializedDataset(
            MaterializedSource(handle), LogicalPlan(), self._executor,
        )

    def from_pandas(self, frames: Any) -> MaterializedDataset:
        """Create a current-run materialized Dataset from bounded pandas frame data."""
        handle = self._executor.from_pandas(frames)
        return MaterializedDataset(
            MaterializedSource(handle), LogicalPlan(), self._executor,
        )

    def _read_file(
        self, format_name: str, paths: str | Sequence[str],
        options: Mapping[str, Any], backend_options=None,
    ) -> Dataset:
        normalized = (paths,) if isinstance(paths, str) else tuple(str(p) for p in paths)
        if not normalized:
            raise ValueError(f"read_{format_name} requires at least one path")
        return Dataset(
            FileSource(
                format_name, normalized, frozen_options(options),
                parse_native_options(backend_options, family="source"),
            ),
            LogicalPlan(), self._executor,
        )

    def read_parquet(
        self, paths: str | Sequence[str], *, filesystem=None, columns=None, partition_filter=None,
        include_paths: bool = False, backend_options=None, **arrow_parquet_args: Any,
    ) -> Dataset:
        """Create a lazy Dataset from one or more Parquet locations using backend-supported options."""
        options = {**arrow_parquet_args}
        _optional(options, "filesystem", filesystem)
        _optional(options, "columns", columns)
        _optional(options, "partition_filter", partition_filter)
        if include_paths: options["include_paths"] = True
        return self._read_file("parquet", paths, options, backend_options)

    def read_json(
        self, paths: str | Sequence[str], *, filesystem=None, partition_filter=None,
        partitioning=None, include_paths: bool = False, backend_options=None, **arrow_json_args: Any,
    ) -> Dataset:
        """Create a lazy Dataset from JSON or JSON-Lines locations.

        ``paths`` may be one location or a sequence. Distributed Ray output may
        be a directory containing part files rather than one physical file.
        Reading starts only when a terminal action executes the plan; use
        ``take`` for bounded inspection and do not assume global row order.
        """
        options = {**arrow_json_args}
        for name, value in (("filesystem", filesystem), ("partition_filter", partition_filter), ("partitioning", partitioning)): _optional(options, name, value)
        if include_paths: options["include_paths"] = True
        return self._read_file("json", paths, options, backend_options)

    def read_csv(
        self, paths: str | Sequence[str], *, filesystem=None, partition_filter=None,
        partitioning=None, include_paths: bool = False, backend_options=None, **arrow_csv_args: Any,
    ) -> Dataset:
        """Create a lazy Dataset from one or more CSV locations using backend-supported options."""
        options = {**arrow_csv_args}
        for name, value in (("filesystem", filesystem), ("partition_filter", partition_filter), ("partitioning", partitioning)): _optional(options, name, value)
        if include_paths: options["include_paths"] = True
        return self._read_file("csv", paths, options, backend_options)

    def read_text(
        self, paths: str | Sequence[str], *, encoding: str = "utf-8",
        drop_empty_lines: bool = True, filesystem=None,
        include_paths: bool = False, backend_options=None,
    ) -> Dataset:
        """Create a lazy Dataset from text files with explicit encoding and empty-line behavior."""
        options = {"encoding": encoding, "drop_empty_lines": drop_empty_lines}
        _optional(options, "filesystem", filesystem)
        if include_paths: options["include_paths"] = True
        return self._read_file("text", paths, options, backend_options)

    def read_binary_files(
        self, paths: str | Sequence[str], *, include_paths: bool = False,
        filesystem=None, backend_options=None,
    ) -> Dataset:
        """Create a lazy Dataset of binary file records; optionally include source paths."""
        options = {"include_paths": include_paths}
        _optional(options, "filesystem", filesystem)
        return self._read_file("binary_files", paths, options, backend_options)

    def read_images(
        self, paths: str | Sequence[str], *, filesystem=None,
        size: tuple[int, int] | None = None, mode: str | None = None,
        include_paths: bool = False, backend_options=None,
    ) -> Dataset:
        """Create a lazy Dataset from image files with backend-supported decoding options."""
        options = {}
        for name, value in (("filesystem", filesystem), ("size", size), ("mode", mode)): _optional(options, name, value)
        if include_paths: options["include_paths"] = True
        return self._read_file("images", paths, options, backend_options)

    def read_sql(
        self, sql: str, connection_factory: Callable[[], Any], *, backend_options=None, **options: Any,
    ) -> Dataset:
        """Create a lazy Dataset from a SQL query and serializable worker connection factory."""
        return Dataset(
            SqlSource(sql, connection_factory, frozen_options(options), parse_native_options(backend_options, family="source")),
            LogicalPlan(), self._executor,
        )

    def read_datasource(
        self, datasource, *, backend_options=None, **read_args: Any,
    ) -> Dataset:
        """Create a lazy Dataset from a backend-neutral Datasource; IO begins only at an action."""
        if read_args:
            raise TypeError(f"unsupported read_datasource arguments: {sorted(read_args)}")
        return Dataset(
            DatasourceSource(datasource, parse_native_options(backend_options, family="source")),
            LogicalPlan(), self._executor,
        )

    def read_lance(
        self, uri: str, *, version: int | None = None,
        columns: Sequence[str] | None = None, filter: str | None = None,
        limit: int | None = None, storage_options: Mapping[str, str] | None = None,
        backend_options=None,
    ) -> Dataset:
        """Create a lazy Dataset from a Lance scan.

        ``version`` selects an exact positive Lance version; ``None`` resolves
        the head when the action starts. ``limit`` is global, so Ray executes a
        limited scan as one read task. ``storage_options`` contains only closed
        non-secret Lance connection values, while ``backend_options`` contains
        only Demiflow physical scheduling options. Formal Candidates should bind
        fixed inputs to an exact version obtained from authorized inspection.
        """
        from ..lance.model import LanceScanSpec

        query = LanceScanSpec(
            uri=uri, version=version, columns=columns, filter=filter,
            limit=limit, storage_options=storage_options,
        )
        return Dataset(
            LanceSource(
                query,
                parse_native_options(backend_options, family="source"),
            ),
            LogicalPlan(), self._executor,
        )

    def vector_search_lance(
        self, uri: str, vector: Sequence[float], *, vector_column: str,
        top_k: int, version: int | None = None,
        columns: Sequence[str] | None = None, filter: str | None = None,
        metric: str | None = None,
        storage_options: Mapping[str, str] | None = None,
        backend_options=None,
    ) -> Dataset:
        """Create a lazy Dataset from one bounded Lance nearest-vector query.

        Ray executes vector search as one read task so ``top_k`` remains a
        global bound. The vector must match a fixed-size floating Lance column;
        results include Lance's ``_distance`` field.
        """
        from ..lance.model import LanceVectorSearchSpec

        query = LanceVectorSearchSpec(
            uri=uri, vector=vector, vector_column=vector_column, top_k=top_k,
            version=version, columns=columns, filter=filter, metric=metric,
            storage_options=storage_options,
        )
        return Dataset(
            LanceSource(
                query,
                parse_native_options(backend_options, family="source"),
            ),
            LogicalPlan(), self._executor,
        )


def _optional(options, name, value):
    if value is not None: options[name] = value
