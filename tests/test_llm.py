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
