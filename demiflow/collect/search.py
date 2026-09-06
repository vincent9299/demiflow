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


# 引擎级遥测（2026-09-06：反爬/性能分析的的数据基础；进程级累计，
# flow drain 侧 dump_engine_telemetry() 落盘由消费方决定）
ENGINE_TELEMETRY: dict = {}


def _tel(name: str) -> dict:
    return ENGINE_TELEMETRY.setdefault(
        name, {"attempts": 0, "results": 0, "errors": {},
               "latency_ms_sum": 0.0, "latency_ms_max": 0.0})


def dump_engine_telemetry() -> dict:
    """快照引擎遥测（深拷贝；含均值派生）。"""
    import copy
    out = copy.deepcopy(ENGINE_TELEMETRY)
    for t in out.values():
        n = max(t["attempts"], 1)
        t["latency_ms_avg"] = round(t["latency_ms_sum"] / n, 1)
        t["error_rate"] = round(
            sum(t["errors"].values()) / max(t["attempts"], 1), 3)
    return out


async def engine_search(name: str, query: str, k: int, *,
                        lang: str = "en", client: Any = None) -> list:
    """分派检索：K 按引擎 k_cap 封顶；异常语义由引擎实现与调用方约定。

    遥测：attempts/errors（按异常类型）/results/延迟（累计与峰值）——
    反爬信号（403/429/Timeout 比率）与性能画像的数据源。
    """
    import time as _time
    eng = get_engine(name)
    tel = _tel(name)
    t0 = _time.perf_counter()
    tel["attempts"] += 1
    rows = None
    try:
        rows = await eng.search(query, min(k, eng.k_cap), lang=lang,
                                client=client)
    except BaseException as exc:
        key = type(exc).__name__
        tel["errors"][key] = tel["errors"].get(key, 0) + 1
        raise
    finally:
        dt = (_time.perf_counter() - t0) * 1000
        tel["latency_ms_sum"] += dt
        tel["latency_ms_max"] = max(tel["latency_ms_max"], dt)
        if rows:
            tel["results"] += len(rows)
    return rows


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
