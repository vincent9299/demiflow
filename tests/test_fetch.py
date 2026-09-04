"""demiflow.collect.fetch 测试：档位轮转/拒收/超时/verify 语义。"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from demiflow.collect import net
from demiflow.collect.fetch import fetch_tiers


@pytest.fixture()
def clean_policy():
    saved = (dict(net.SOURCE_LIMITS), dict(net._gates))
    net._gates.clear()
    net.SOURCE_LIMITS.clear()
    net.SOURCE_LIMITS.update({
        "_f": net.SourceLimits(rate=100.0, concurrency=8),
        "dl:_f": net.SourceLimits(rate=100.0, concurrency=8),
    })
    yield
    net.SOURCE_LIMITS.clear()
    net.SOURCE_LIMITS.update(saved[0])
    net._gates.clear()
    net._gates.update(saved[1])
    net._dl_client_direct = net._dl_client_proxy = None


def _png_like(n: int) -> bytes:
    return b"\x89PNG-fake-" + bytes([n]) * 64


async def test_tier_rotation_first_wins(clean_policy):
    def h(req):
        if "bad" in str(req.url):
            return httpx.Response(404)
        return httpx.Response(200, content=_png_like(1))
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(h)))

    got = await fetch_tiers(
        ["https://x/bad1.png", "https://x/bad2.png", "https://x/ok.png"],
        source="_f", verify=lambda d: {"size": len(d)})
    assert got is not None
    assert got.url == "https://x/ok.png"
    assert got.extra == {"size": 64 + len(b"\x89PNG-fake-")}
    assert got.sha256 == __import__("hashlib").sha256(got.data).hexdigest()
    print("[PASS] 档位轮转：前档 404 换下一档，首个成功即停")


async def test_byte_cap_reject_no_rotation(clean_policy):
    def h(req):
        return httpx.Response(200, content=b"z" * 4096)
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(h)))

    got = await fetch_tiers(["https://x/a", "https://x/b"], source="_f",
                            max_bytes=1024)
    assert got is None
    print("[PASS] 超字节上限拒收不轮转")


async def test_verify_reject_no_rotation(clean_policy):
    def h(req):
        return httpx.Response(200, content=b"not-target")
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(h)))

    got = await fetch_tiers(["https://x/a", "https://x/b"], source="_f",
                            verify=lambda d: None)
    assert got is None
    print("[PASS] verify 不过拒收不轮转")


async def test_hard_timeout_moves_to_next_tier(clean_policy):
    # MockTransport 的 handler 是同步的（会阻塞事件循环使超时无法触发），
    # 用 AsyncBaseTransport 才能模拟真正的慢渗连接
    class SelectiveSlow(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            if "slow" in str(request.url):
                await asyncio.sleep(2.0)
            return httpx.Response(200, content=_png_like(8))
    net.set_download_client(httpx.AsyncClient(transport=SelectiveSlow()))

    got = await fetch_tiers(["https://x/slow", "https://x/fast"], source="_f",
                            hard_timeout=0.05,
                            verify=lambda d: {"ok": True})
    assert got is not None and got.url == "https://x/fast"
    print("[PASS] 硬超时换下一档")


async def test_all_tiers_fail_raises(clean_policy):
    def h(req):
        return httpx.Response(403)
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(h)))

    with pytest.raises(net.DeterministicError):
        await fetch_tiers(["https://x/a", "https://x/b"], source="_f")
    print("[PASS] 档位全败上抛 DeterministicError")


async def test_no_verify_passes_empty_extra(clean_policy):
    def h(req):
        return httpx.Response(200, content=b"payload")
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(h)))

    got = await fetch_tiers(["https://x/a"], source="_f")
    assert got is not None and got.extra == {}
    print("[PASS] 无 verify 时 extra 为空 dict")
