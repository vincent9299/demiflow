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


@dataclass(frozen=True)
class IterableSource(SourcePlan):
    """惰性迭代器源：factory 每次调用产出一个新迭代器（2026-09-08 新增）。

    factory 是零参可调用（如生成器函数），终结动作执行时才调用——源不持
    迭代器本体，同一 Dataset 的多次动作各自拿到全新消费（与惰性路径
    「动作触发即重算」语义一致）。仅本地执行路径支持（行留在进程内）；
    需要跨节点分布时换分布式数据源，不要序列化本源。
    """

    factory: Callable[[], Iterable[Any]]


def frozen_options(options: Mapping[str, Any]) -> Mapping[str, Any]:
    return dict(options)
