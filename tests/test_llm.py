"""demiflow.collect.llm 测试：chat 载荷构造/异常语义、图片前处理。"""

from __future__ import annotations

import base64
import json
import io

import httpx
import pytest

from demiflow.collect.llm import AsyncLLMClient, encode_image_b64


def _client(handler):
    return AsyncLLMClient(base_url="http://mock/v1", model="m",
                          http=httpx.AsyncClient(
                              transport=httpx.MockTransport(handler)))


async def test_chat_payload_and_content():
    seen = {}

    def h(req):
        seen["payload"] = json.loads(req.content)
        return httpx.Response(200, json={
            "choices": [{"message": {"content": "答案"}}]})

    c = _client(h)
    out = await c.chat(
        [{"role": "user", "content": "问"}],
        max_tokens=512, temperature=0.0, json_mode=True, thinking=False)
    assert out == "答案"
    p = seen["payload"]
    assert p["model"] == "m" and p["max_tokens"] == 512
    assert p["temperature"] == 0.0
    assert p["response_format"] == {"type": "json_object"}
    assert p["chat_template_kwargs"] == {"enable_thinking": False}
    assert p["messages"] == [{"role": "user", "content": "问"}]


async def test_chat_failure_raises_no_internal_retry():
    calls = {"n": 0}

    def h(req):
        calls["n"] += 1
        return httpx.Response(503)

    c = _client(h)
    with pytest.raises(httpx.HTTPStatusError):
        await c.chat([{"role": "user", "content": "x"}], max_tokens=8)
    assert calls["n"] == 1          # 单次尝试，重试归调用方


def _img(w, h):
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (w, h), (9, 9, 9)).save(buf, "PNG")
    return buf.getvalue()


def test_encode_image_b64_resize_long_edge():
    b64 = encode_image_b64(_img(2000, 1000), max_edge=768)
    assert b64 is not None
    from PIL import Image
    im = Image.open(io.BytesIO(base64.b64decode(b64)))
    assert im.format == "JPEG" and max(im.size) == 768
    assert (im.width, im.height) == (768, 384)


def test_encode_image_b64_small_untouched():
    b64 = encode_image_b64(_img(100, 50), max_edge=768)
    from PIL import Image
    im = Image.open(io.BytesIO(base64.b64decode(b64)))
    assert (im.width, im.height) == (100, 50)


def test_encode_image_b64_rejects_garbage():
    assert encode_image_b64(b"not image") is None
    assert encode_image_b64(_img(64, 64)[:16]) is None


# ---------------------------------------------------------------------------
# 端点资源注册表
# ---------------------------------------------------------------------------

async def test_endpoint_registry_env_override(monkeypatch):
    from demiflow.collect import llm
    llm.register_endpoint("_t", base_url="http://default/v1", model="m1",
                          base_url_env="T_BASE", model_env="T_MODEL")
    c1 = llm.get_llm_client("_t")
    assert c1.base_url == "http://default/v1" and c1.model == "m1"
    monkeypatch.setenv("T_BASE", "http://overridden/v1")
    monkeypatch.setenv("T_MODEL", "m2")
    llm.reconfigure_endpoint("_t", max_connections=8)   # 作废重建
    c2 = llm.get_llm_client("_t")
    assert c2.base_url == "http://overridden/v1" and c2.model == "m2"
    await llm.close_all_llm()
    assert llm.get_llm_client("_t").base_url == "http://overridden/v1"  # 重建仍走 env


def test_endpoint_unknown_rejected():
    from demiflow.collect import llm
    import pytest
    with pytest.raises(KeyError, match="未声明"):
        llm.get_llm_client("_nope")


def test_run_stages_orchestration():
    from demiflow.collect import llm
    from demiflow.data.plan import StreamStage
    from demiflow.standalone import local_data, run_stages

    class Double(StreamStage):
        label = "double"
        concurrency = 2

        async def __call__(self, row):
            return {**row, "v": row["i"] * 2}

    class Keep(StreamStage):
        label = "keep"

        def __call__(self, row):
            return row if row["v"] > 4 else None

    stats = run_stages(local_data(), [{"i": i} for i in range(5)],
                       [Double(), Keep()],
                       concurrency={"double": (3, 8)})
    assert stats.emitted == 2
    assert stats.stage("keep")["in"] == 5


async def test_injection_survives_reconfigure():
    """冒烟注入优先且不被 reconfigure 清除（编排启动期常规 reconfigure 池上限）。"""
    from demiflow.collect import llm
    llm.register_endpoint("_t2", base_url="http://default/v1", model="m")
    c = AsyncLLMClient(base_url="http://mock/v1", model="mock",
                       http=httpx.AsyncClient(transport=httpx.MockTransport(
                           lambda req: httpx.Response(200, json={
                               "choices": [{"message": {"content": "x"}}]}))))
    llm.inject_endpoint_client("_t2", c)
    llm.reconfigure_endpoint("_t2", max_connections=4)
    assert llm.get_llm_client("_t2") is c
    await llm.close_all_llm()
