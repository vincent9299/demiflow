"""Immutable lazy source plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Tuple

from .datasource import Datasource
from ..lance.model import LanceQuerySpec
from .native_options import NativeOptions


class SourcePlan:
    pass


@dataclass(frozen=True)
class ItemsSource(SourcePlan):
    items: Tuple[Any, ...]


@dataclass(frozen=True)
class RangeSource(SourcePlan):
    count: int
    native_options: NativeOptions | None = None


@dataclass(frozen=True)
class FileSource(SourcePlan):
    format: str
    paths: Tuple[str, ...]
    options: Mapping[str, Any] = field(default_factory=dict)
    native_options: NativeOptions | None = None


@dataclass(frozen=True)
class SqlSource(SourcePlan):
    sql: str
    connection_factory: Callable[[], Any]
    options: Mapping[str, Any] = field(default_factory=dict)
    native_options: NativeOptions | None = None


@dataclass(frozen=True)
class DatasourceSource(SourcePlan):
    datasource: Datasource
    native_options: NativeOptions | None = None


@dataclass(frozen=True)
class LanceSource(SourcePlan):
    query: LanceQuerySpec
    native_options: NativeOptions | None = None


@dataclass(frozen=True)
class MaterializedSource(SourcePlan):
    handle: Any
    row_count: int | None = None


def frozen_options(options: Mapping[str, Any]) -> Mapping[str, Any]:
    return dict(options)
