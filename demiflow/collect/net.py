"""demiflow 采集 HTTP 底座（2026-09-04 自 collect_v2.infra 整体上移为引擎原语）。

契约：
- 按源限速，尽量快但避免被封：限速/并发/代理归属由消费方策略注册
  （SOURCE_LIMITS + PROXY_URL + register_limits），引擎零业务默认值；
- 分类重试：确定性失败（403/404/域名非法等）不重试直接抛出；
  瞬态失败（超时/连接重置/429/5xx）重试 MAX_RETRIES 次、固定间隔；
- 双池客户端：{直连, 代理} × {检索, 下载}，进程级惰性共享；
  检索池禁 keepalive 复用（半读连接复用死锁教训），下载池开启复用
  （CDN 场景握手是主瓶颈，配 stream 的 finally 关响应兜底）；
- stream 流式原语：建流/重试循环与读流彻底分离，读流阶段异常原样上抛。

对外原语：
- request(source, method, url, ...)   限速 + 分类重试的 HTTP 请求
- stream(source, method, url, ...)    流式版 request（字节封顶在调用方）
- SourceGate / RateLimiter            限流原语
- register_limits / SOURCE_LIMITS     消费方策略注册（源 → 限速/并发/代理）
- set_client / set_download_client    冒烟注入（MockTransport）
- BROWSER_UA                          通用浏览器 UA（API 身份 UA 由消费方自定）
"""

from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import httpx


@dataclass(frozen=True)
class SourceLimits:
    """单源限速策略：速率、并发、代理归属（策略数据，由消费方注册）。"""
    rate: float        # 每秒请求数
    concurrency: int   # 该源最大在途请求数
    proxy: bool = False  # 是否走代理出网


# ---------------------------------------------------------------------------
# 配置（机制常量；策略数据由消费方注册）
# ---------------------------------------------------------------------------

MAX_RETRIES = 3          # 瞬态失败重试次数（不含首次）
RETRY_INTERVAL = 1.0     # 重试固定间隔（秒），不做指数退避
DEFAULT_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=10.0)

# 下载专用池参数（下载打 CDN：连接复用换吞吐；回退 = keepalive 改回 0）
DOWNLOAD_LIMITS = httpx.Limits(max_connections=128, max_keepalive_connections=64)

# 消费方策略注册面（引擎零业务默认）：
# - PROXY_URL：代理源出网代理（None=无代理；消费方 startup 注册）
# - SOURCE_LIMITS：源 → 限速/并发/代理归属（gate_for 未登记源直接报错）
PROXY_URL: Optional[str] = None
SOURCE_LIMITS: dict[str, SourceLimits] = {}


def register_limits(limits: dict) -> None:
    """消费方批量注册限速策略（update 语义，可增量；支持「检索键与下载键分开声明代理归属」的双闸式注册）。"""
    SOURCE_LIMITS.update(limits)


# 通用浏览器 UA（自报机器人身份会被反爬拦截的场合）；API 身份 UA 含
# 项目归属与联系方式，属消费方身份，由其策略模块自带。
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


# ---------------------------------------------------------------------------
# 异常与失败分类
# ---------------------------------------------------------------------------

class InfraError(Exception):
    """基础设施层异常基类。"""


class DeterministicError(InfraError):
    """确定性失败（403/404/域名非法等）：不重试，调用方认缺。"""


class TransientExhaustedError(InfraError):
    """瞬态失败且有界重试已用尽。"""


def classify_status(status: int) -> str:
    """HTTP 状态码分类：ok / transient / deterministic。"""
    if status < 400:
        return "ok"
    if status == 429 or status >= 500:
        return "transient"
    return "deterministic"


def _in_chain(exc: BaseException, target: type) -> bool:
    cur: Optional[BaseException] = exc
    while cur is not None:
        if isinstance(cur, target):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def classify_network_error(exc: Exception) -> Optional[str]:
    """网络异常分类：deterministic / transient；不认识返回 None（原样抛出）。"""
    if isinstance(exc, httpx.TimeoutException):
        return "transient"
    if isinstance(exc, httpx.ConnectError):
        # 域名解析失败 = 域名非法，确定性失败
        if _in_chain(exc, socket.gaierror):
            return "deterministic"
        return "transient"
    if isinstance(exc, httpx.NetworkError):
        return "transient"
    return None


# ---------------------------------------------------------------------------
# 限速原语
# ---------------------------------------------------------------------------

class RateLimiter:
    """最小间隔限速器：同源请求按 1/rate 秒最小间隔串行放行。"""

    def __init__(self, rate: float):
        self._interval = 1.0 / rate
        self._lock = asyncio.Lock()
        self._next_at = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            wait = self._next_at - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = loop.time()
            self._next_at = now + self._interval


class SourceGate:
    """每源闸门：并发信号量 + 限速器。slot() 内发请求。"""

    def __init__(self, limits: SourceLimits):
        self.limits = limits
        self._sem = asyncio.Semaphore(limits.concurrency)
        self._rl = RateLimiter(limits.rate)

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        async with self._sem:
            await self._rl.acquire()
            yield


_gates: dict[str, SourceGate] = {}


def gate_for(source: str) -> SourceGate:
    """按源名取闸门（惰性创建）。未登记源直接报错，不给默认限速。"""
    gate = _gates.get(source)
    if gate is None:
        limits = SOURCE_LIMITS.get(source)
        if limits is None:
            raise ValueError(f"源 {source!r} 未在限速表 SOURCE_LIMITS 登记")
        gate = _gates[source] = SourceGate(limits)
    return gate


# ---------------------------------------------------------------------------
# 带限速与分类重试的 HTTP 请求
# ---------------------------------------------------------------------------

_client_direct: Optional[httpx.AsyncClient] = None
_client_proxy: Optional[httpx.AsyncClient] = None
_dl_client_direct: Optional[httpx.AsyncClient] = None
_dl_client_proxy: Optional[httpx.AsyncClient] = None

# 下载专用池参数（2026-08-22 拍板恢复连接复用，显式推翻 2026-08-21 keepalive=0 定案，
# 沿革见 get_download_client 文档串）；回退 = max_keepalive_connections 改回 0。
DOWNLOAD_LIMITS = httpx.Limits(max_connections=128, max_keepalive_connections=64)


def _limits_for(source: str) -> Optional[SourceLimits]:
    """取源的限速配置：先精确键（含 dl: 前缀下载键），再回退源级键。

    精确键优先：让「检索端点直连 + 下载走代理」的非对称源
    （本机网关检索 + 海外 CDN 资源）能各自声明 proxy 归属，
    不必把整源的检索与下载绑在同一代理开关上。
    """
    return SOURCE_LIMITS.get(source) or SOURCE_LIMITS.get(
        source.removeprefix("dl:"))


def get_client(source: str = "") -> httpx.AsyncClient:
    """按源取进程级共享 HTTP 客户端（双池：直连 / 代理，惰性创建）。
    检索侧专用：下载侧（dl: 流量）另走 get_download_client。

    两池均 max_keepalive_connections=0（2026-08-21 定案）：三次夜跑卡死
    同一签名——半读状态的连接（CLOSE-WAIT 且接收缓冲有未读字节）被池
    保留，下次复用读流永久阻塞，全链路静默停摆。任务取消是半读连接
    的主要来源，无法从池层根治，故直接禁用复用：每次请求新连接，
    代价（每请求一次 TCP+TLS 握手）远小于停摆风险。

    冒烟可先 set_client 注入（无 proxy 需求的源注 direct 池，
    proxy 源注 proxy 池）。
    """
    global _client_direct, _client_proxy
    no_keepalive = httpx.Limits(max_keepalive_connections=0)
    lim = _limits_for(source)
    need_proxy = bool(lim and lim.proxy)
    if need_proxy:
        if _client_proxy is None:
            _client_proxy = httpx.AsyncClient(
                timeout=DEFAULT_TIMEOUT, follow_redirects=True,
                proxy=PROXY_URL, limits=no_keepalive)
        return _client_proxy
    if _client_direct is None:
        _client_direct = httpx.AsyncClient(
            timeout=DEFAULT_TIMEOUT, follow_redirects=True,
            limits=no_keepalive)
    return _client_direct


def get_download_client(source: str) -> httpx.AsyncClient:
    """下载专用客户端（双池：直连 / 代理，惰性创建）：开启连接复用。

    沿革：2026-08-21 三次夜跑停摆后全链路禁复用（keepalive=0）；2026-08-22
    拍板仅下载侧恢复复用——病根（stream yield-in-retry 制造半读连接）已修，
    全链零任务取消源，停摆有 supervise 12 分钟自愈兜底，且下载打的是 CDN、
    每图一次全新 TCP+TLS 握手已成实测主瓶颈（py-spy 实锤）。
    配套三层防线：① 仅本池开复用、检索侧维持禁用；② 调用方（op_download）
    每请求硬超时 90s，超时取消任务经 stream 的 finally 关响应——没读完的
    响应关闭即销毁连接不入池；③ read=30s 读超时与看门狗不动。
    回退条件：任何停摆/风控迹象 → 上面 DOWNLOAD_LIMITS 的
    max_keepalive_connections 改回 0，重启即恢复旧行为。
    """
    global _dl_client_direct, _dl_client_proxy
    lim = _limits_for(source)
    need_proxy = bool(lim and lim.proxy)
    if need_proxy:
        if _dl_client_proxy is None:
            _dl_client_proxy = httpx.AsyncClient(
                timeout=DEFAULT_TIMEOUT, follow_redirects=True,
                proxy=PROXY_URL, limits=DOWNLOAD_LIMITS)
        return _dl_client_proxy
    if _dl_client_direct is None:
        _dl_client_direct = httpx.AsyncClient(
            timeout=DEFAULT_TIMEOUT, follow_redirects=True,
            limits=DOWNLOAD_LIMITS)
    return _dl_client_direct


def set_client(client: httpx.AsyncClient, *, proxy: bool = False) -> None:
    global _client_direct, _client_proxy
    if proxy:
        _client_proxy = client
    else:
        _client_direct = client


def set_download_client(client: httpx.AsyncClient, *, proxy: bool = False) -> None:
    """冒烟注入下载池客户端（与 set_client 对称；2026-09-04 flow 冒烟需要）。"""
    global _dl_client_direct, _dl_client_proxy
    if proxy:
        _dl_client_proxy = client
    else:
        _dl_client_direct = client


async def close_client() -> None:
    global _client_direct, _client_proxy, _dl_client_direct, _dl_client_proxy
    for _c in (_client_direct, _client_proxy, _dl_client_direct, _dl_client_proxy):
        if _c is not None:
            await _c.aclose()
    _client_direct = _client_proxy = None
    _dl_client_direct = _dl_client_proxy = None


async def request(
    source: str,
    method: str,
    url: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
    **kwargs,
) -> httpx.Response:
    """对 source 发一次受限速、带分类重试的请求，成功返回响应。

    - 确定性失败：抛 DeterministicError，不重试；
    - 瞬态失败：固定间隔重试 MAX_RETRIES 次后用尽抛 TransientExhaustedError；
    - 未识别异常：原样抛出，不归类。
    """
    gate = gate_for(source)
    http = client or get_client(source)
    last_exc: Optional[Exception] = None
    last_status: Optional[int] = None

    for attempt in range(MAX_RETRIES + 1):
        async with gate.slot():
            try:
                resp = await http.request(method, url, **kwargs)
            except Exception as exc:  # noqa: BLE001 - 需要分类后决定重试与否
                verdict = classify_network_error(exc)
                if verdict is None:
                    raise
                if verdict == "deterministic":
                    raise DeterministicError(f"{source} {url}: 域名/连接确定性失败") from exc
                last_exc, last_status = exc, None
            else:
                verdict = classify_status(resp.status_code)
                if verdict == "ok":
                    return resp
                if verdict == "deterministic":
                    raise DeterministicError(f"{source} {url}: HTTP {resp.status_code}")
                last_exc, last_status = None, resp.status_code
        # 瞬态失败：固定间隔后重试
        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_INTERVAL)

    detail = f"HTTP {last_status}" if last_status else repr(last_exc)
    raise TransientExhaustedError(
        f"{source} {url}: 瞬态失败重试用尽（{MAX_RETRIES} 次）最后状态 {detail}"
    ) from last_exc


@asynccontextmanager
async def stream(
    source: str,
    method: str,
    url: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
    **kwargs,
) -> AsyncIterator[httpx.Response]:
    """流式版 request：分类/重试只作用于建流与首包状态码。

    建流成功后把响应交给调用方流式读取（调用方负责字节封顶），
    读出阶段的网络异常原样上抛，不重试（下载重头再来代价大，认缺即可）。

    坑修（2026-08-21 夜跑实测）：yield 曾写在重试 for 循环内，调用方在读流
    阶段抛的异常会被 throw 进 yield 处，随后无守卫地进入下一轮重试，
    把读流错误转成 TransientExhaustedError 重抛——违反「读出阶段异常
    原样上抛」契约；且读流期 gate slot 早已释放，重试也无限速保护。
    现改为建流/重试循环与 yield 彻底分离。
    """
    gate = gate_for(source)
    # dl: 流量（下载专用，op_download 是唯一消费者）走复用池客户端；
    # 其余（仅冒烟可能）维持禁用复用的共享客户端。
    if client is None:
        client = (get_download_client(source) if source.startswith("dl:")
                  else get_client(source))
    http = client
    last_exc: Optional[Exception] = None
    last_status: Optional[int] = None
    resp: Optional[httpx.Response] = None

    for attempt in range(MAX_RETRIES + 1):
        async with gate.slot():
            try:
                req = http.build_request(method, url, **kwargs)
                resp = await http.send(req, stream=True)
            except Exception as exc:  # noqa: BLE001 - 需要分类后决定重试与否
                verdict = classify_network_error(exc)
                if verdict is None:
                    raise
                if verdict == "deterministic":
                    raise DeterministicError(f"{source} {url}: 域名/连接确定性失败") from exc
                last_exc, last_status = exc, None
                continue
            verdict = classify_status(resp.status_code)
            if verdict == "ok":
                break                       # 建流成功，跳出循环后交给调用方
            status = resp.status_code
            await resp.aclose()
            resp = None
            if verdict == "deterministic":
                raise DeterministicError(f"{source} {url}: HTTP {status}")
            last_exc, last_status = None, status
        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_INTERVAL)
    else:
        detail = f"HTTP {last_status}" if last_status else repr(last_exc)
        raise TransientExhaustedError(
            f"{source} {url}: 瞬态失败重试用尽（{MAX_RETRIES} 次）最后状态 {detail}"
        ) from last_exc

    # 建流成功：读流阶段异常原样上抛不重试（调用方认缺），finally 只关响应
    try:
        yield resp
    finally:
        await resp.aclose()
