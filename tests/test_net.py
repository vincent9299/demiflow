"""demiflow.collect.net 测试：分类重试、限速、注册式闸门、代理归属解析。"""

from __future__ import annotations

import socket
import time

import httpx
import pytest

from demiflow.collect import net


@pytest.fixture()
def clean_policy():
    """每个用例独立的策略注册面（避免跨用例污染模块状态）。"""
    saved_limits = dict(net.SOURCE_LIMITS)
    saved_gates = dict(net._gates)
    saved_proxy = net.PROXY_URL
    net._gates.clear()
    net.SOURCE_LIMITS.clear()
    net.SOURCE_LIMITS.update({
        "_t_direct": net.SourceLimits(rate=100.0, concurrency=8),
        "_t_proxy": net.SourceLimits(rate=100.0, concurrency=8, proxy=True),
        "dl:_t_asym": net.SourceLimits(rate=100.0, concurrency=8, proxy=True),
    })
    yield
    net.SOURCE_LIMITS.clear()
    net.SOURCE_LIMITS.update(saved_limits)
    net._gates.clear()
    net._gates.update(saved_gates)
    net.PROXY_URL = saved_proxy
    net._client_direct = net._client_proxy = None
    net._dl_client_direct = net._dl_client_proxy = None
    net.reset_injected_clients()


async def test_deterministic_404_zero_retry(clean_policy):
    calls = {"n": 0}

    def h(req):
        calls["n"] += 1
        return httpx.Response(404)

    net.set_client(httpx.AsyncClient(transport=httpx.MockTransport(h)), proxy=True)
    with pytest.raises(net.DeterministicError):
        await net.request("_t_proxy", "GET", "http://mock/a")
    assert calls["n"] == 1
    print("[PASS] 404 确定性失败零重试")


async def test_transient_retry_then_success(clean_policy):
    net.RETRY_INTERVAL = 0.05
    seq = iter([500, 500, 200])
    calls = {"n": 0}

    def h(req):
        calls["n"] += 1
        return httpx.Response(next(seq))

    net.set_client(httpx.AsyncClient(transport=httpx.MockTransport(h)), proxy=True)
    resp = await net.request("_t_proxy", "GET", "http://mock/b")
    assert resp.status_code == 200 and calls["n"] == 3
    print("[PASS] 瞬态重试后成功")


async def test_transient_exhausted(clean_policy):
    net.RETRY_INTERVAL = 0.05
    calls = {"n": 0}

    def h(req):
        calls["n"] += 1
        return httpx.Response(500)

    net.set_client(httpx.AsyncClient(transport=httpx.MockTransport(h)), proxy=True)
    with pytest.raises(net.TransientExhaustedError):
        await net.request("_t_proxy", "GET", "http://mock/c")
    assert calls["n"] == net.MAX_RETRIES + 1
    print("[PASS] 瞬态重试用尽")


async def test_dns_failure_deterministic(clean_policy):
    def h(req):
        try:
            raise socket.gaierror(-2, "Name or service not known")
        except socket.gaierror as e:
            raise httpx.ConnectError("dns fail", request=req) from e

    net.set_client(httpx.AsyncClient(transport=httpx.MockTransport(h)), proxy=True)
    with pytest.raises(net.DeterministicError):
        await net.request("_t_proxy", "GET", "http://mock/d")
    print("[PASS] 域名非法确定性失败")


async def test_rate_limiter_min_interval(clean_policy):
    rl = net.RateLimiter(rate=20.0)
    t0 = time.monotonic()
    for _ in range(5):
        await rl.acquire()
    assert time.monotonic() - t0 >= 0.2 * 0.9
    print("[PASS] 限速器最小间隔")


async def test_unregistered_source_rejected(clean_policy):
    with pytest.raises(ValueError):
        net.gate_for("no_such_source")
    print("[PASS] 未登记源拒绝")


async def test_proxy_resolution_exact_key_first(clean_policy):
    # _t_direct 有源级键（直连）；dl:_t_asym 精确键声明代理（双闸式注册）
    assert net._limits_for("_t_direct").proxy is False
    assert net._limits_for("dl:_t_asym").proxy is True
    assert net._limits_for("_t_proxy").proxy is True
    print("[PASS] 代理归属精确键优先")


async def test_register_limits_update_semantics(clean_policy):
    net.register_limits({"_t_new": net.SourceLimits(rate=1.0, concurrency=1)})
    assert "_t_new" in net.SOURCE_LIMITS
    gate = net.gate_for("_t_new")
    assert gate.limits.rate == 1.0
    print("[PASS] register_limits 注册语义")
