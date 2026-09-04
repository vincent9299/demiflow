"""demiflow 采集档位拉取原语：多候选链接轮转 + 字节封顶 + 硬超时 + verify 钩子。

自 collect_v2.op_download 骨架沉淀（2026-09-04，机制归引擎）：
- 档位轮转：候选 URL 按序（大到小）依次试，首个成功即停，获胜链接回传；
- 确定性失败（404/403）换下一档；候选全败上抛 net.DeterministicError（调用方认缺）；
- 硬超时：单请求总时长封顶（防慢渗连接绕过单次读超时），超时换下一档；
- 字节封顶：流式读取超限拒收（认缺不轮转——同一逻辑内容各档一致，轮转无意义）；
- verify 钩子：verify(data) -> dict | None。None=非目标内容拒收（不轮转）；
  dict=业务元数据（如解码提取的宽高/格式），原样放进 Fetched.extra。
  verify 是同步函数（内容解码属 CPU 短任务，阻塞可接受；重活调用方自行 to_thread）。
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Callable, Mapping, Optional, Sequence

from . import net

DEFAULT_MAX_BYTES = 20 * 1024 * 1024    # 单资源字节上限
DEFAULT_HARD_TIMEOUT = 90.0             # 单请求总时长封顶（秒）
CHUNK = 64 * 1024                       # 流式读取块


@dataclass(frozen=True)
class Fetched:
    """档位拉取结果：字节 + 获胜链接 + 内容哈希 + verify 提取的业务元数据。"""
    data: bytes
    url: str                      # 获胜候选（清单只写它）
    sha256: str
    size_bytes: int
    extra: Optional[dict] = None  # verify 的返回值（业务元数据，原样透传）


async def _fetch_one(source: str, url: str, *, client, max_bytes: int,
                     headers: Optional[Mapping]) -> Optional[bytes]:
    """拉单个 URL 的字节；超限返回 None，网络异常按 net 分类上抛。

    闸门走独立的 dl:<源> 键（下载与检索分桶限流的通用约定）。"""
    buf = bytearray()
    capped = False
    async with net.stream(
        f"dl:{source}", "GET", url,
        client=client, headers=dict(headers or {}),
    ) as resp:
        async for chunk in resp.aiter_bytes(CHUNK):
            buf.extend(chunk)
            if len(buf) > max_bytes:
                capped = True
                break
    return None if capped else bytes(buf)


async def fetch_tiers(
    urls: Sequence[str],
    *,
    source: str,
    client=None,
    max_bytes: int = DEFAULT_MAX_BYTES,
    hard_timeout: float = DEFAULT_HARD_TIMEOUT,
    headers: Optional[Mapping] = None,
    verify: Optional[Callable[[bytes], Optional[dict]]] = None,
) -> Optional[Fetched]:
    """按档位序拉候选 URL，返回 Fetched；返回 None = 拒收（超限/verify 不过）。

    档位全败上抛 net.DeterministicError（调用方认缺）。
    """
    for url in urls:
        try:
            got = await asyncio.wait_for(
                _fetch_one(source, url, client=client,
                           max_bytes=max_bytes, headers=headers),
                timeout=hard_timeout)
        except net.DeterministicError:
            continue        # 档位确定性不可得（404/403）：试下一档
        except asyncio.TimeoutError:
            continue        # 硬超时：该档不可得，换下一档
        if got is None:
            return None     # 超字节上限：认缺，不轮转不重试
        extra = verify(got) if verify is not None else {}
        if extra is None:
            return None     # 内容校验失败（非目标类型）：拒收，不轮转
        return Fetched(
            data=got, url=url,
            sha256=hashlib.sha256(got).hexdigest(),
            size_bytes=len(got), extra=extra)
    raise net.DeterministicError(
        f"{source}: 全部 {len(urls)} 个候选链接确定性失败")
