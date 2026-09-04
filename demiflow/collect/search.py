"""demiflow 检索引擎抽象（2026-09-04·八：接口归引擎、实现归消费方）。

分工铁律：引擎拥有「抽象」（协议/注册表/分派/通用件），消费方只拥有
「实现」（反爬解析/字段映射/档位知识）。第二个采集项目复用的是本模块
的注册与运行形态，不是各家的引擎实现。

结果 dict 约定（键全部可缺省）：
- tiers: list[str]    候选链接档位，大到小（与 fetch.fetch_tiers 对位）；
- landing/title: str  来源页与标题；
- width/height: int   声明尺寸（常失真，实测以下载解码为准）；
- mime/license/author
- native: dict        引擎原生元数据原样保留（溯源用）。
"""

from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable

import httpx


@runtime_checkable
class SearchEngine(Protocol):
    """检索引擎协议：结构化实现（无需显式继承）。"""
    name: str                        # 注册键（与限速策略表的源名对位）
    k_cap: int                       # 单次检索 K 封顶（不分页深翻）
    async def search(self, query: str, k: int, *, lang: str = "en",
                     client: Any = None) -> list: ...


_ENGINES: dict[str, SearchEngine] = {}


def register_engine(engine: SearchEngine) -> None:
    """注册引擎（消费方 import 期完成；重名后者覆盖前者）。"""
    _ENGINES[engine.name] = engine


def get_engine(name: str) -> SearchEngine:
    eng = _ENGINES.get(name)
    if eng is None:
        raise ValueError(
            f"未登记的检索引擎：{name}（先 register_engine，"
            f"已登记：{sorted(_ENGINES) or '无'}）")
    return eng


async def engine_search(name: str, query: str, k: int, *,
                        lang: str = "en", client: Any = None) -> list:
    """分派检索：K 按引擎 k_cap 封顶；异常语义由引擎实现与调用方约定。"""
    eng = get_engine(name)
    return await eng.search(query, min(k, eng.k_cap), lang=lang, client=client)


def is_connect_failure(exc: BaseException) -> bool:
    """异常链里是否存在连接建立失败（网关未启动/端口未监听的判据，
    供 fail-fast 诊断用——配置错误应终止而非认缺）。"""
    seen: set[int] = set()
    node = exc
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        if isinstance(node, httpx.ConnectError):
            return True
        node = node.__cause__ or node.__context__
    return False
