"""Pure construction shared by public and checkpoint-bound Lance scans."""
from __future__ import annotations

from typing import Mapping, Sequence

from .native_options import parse_native_options
from .sources import LanceSource
from ..lance.model import LanceScanSpec


def make_lance_scan_source(
    uri: str, *, version: int | None = None,
    columns: Sequence[str] | None = None, filter: str | None = None,
    limit: int | None = None, storage_options: Mapping[str, str] | None = None,
    batch_size: int | None = None, batch_readahead: int | None = None,
    fragment_readahead: int | None = None,
    projection: Mapping[str, str] | None = None, backend_options=None,
) -> LanceSource:
    """Keep scan validation before native options, without opening a table."""
    query = LanceScanSpec(
        uri=uri, version=version, columns=columns, filter=filter,
        limit=limit, storage_options=storage_options,
        batch_size=batch_size, batch_readahead=batch_readahead,
        fragment_readahead=fragment_readahead, projection=projection,
    )
    return LanceSource(query, parse_native_options(backend_options, family="source"))
