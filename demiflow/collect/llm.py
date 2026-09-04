"""demiflow 流式 LLM 机制层（2026-09-04·八：接口归引擎、口径归消费方）。

分工边界：
- 引擎拥有「机制」：OpenAI 兼容端点的进程级共享 async 客户端（连接池
  显式给足——httpx 裸默认 keepalive 仅 20，长超时会连带「等连接」放大，
  高并发下 CLOSE-WAIT 占坑的实测教训随迁）、单次 chat 请求构造
  （json_mode/thinking 关闭等 vLLM 兼容参数）、图片前处理
  （解码→最长边缩放→JPEG base64）；
- 消费方拥有「口径」：prompt 文本、messages 结构、max_tokens、重试
  策略与解析/钳制——这些是数据可比性契约，不进引擎。

chat() 的重试语义：**单次尝试、失败上抛**——重试循环归调用方，
因为「网络失败与解析失败统一重试」是消费方口径（如打标的三段式
重试），引擎不越界。
"""

from __future__ import annotations

import base64
import io
import os
from typing import Optional

import httpx


class AsyncLLMClient:
    """OpenAI 兼容端点的进程级共享客户端（一个端点一个实例）。

    http 注入参数供冒烟测试使用（MockTransport）；生产路径按
    max_connections 自建连接池。
    """

    def __init__(self, *, base_url: str, model: str,
                 max_connections: int = 56, timeout: float = 600.0,
                 http: Optional[httpx.AsyncClient] = None):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._timeout = timeout
        self._own = http is None
        self._http = http or httpx.AsyncClient(limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_connections))

    async def chat(self, messages, *, max_tokens: int,
                   temperature: float = 0.0, json_mode: bool = False,
                   thinking: Optional[bool] = None,
                   timeout: Optional[float] = None) -> str:
        """单次 chat 请求；成功返回 content 字符串，失败上抛（重试归调用方）。

        json_mode → response_format=json_object；thinking=False →
        chat_template_kwargs.enable_thinking=False（Qwen3 系兼容：默认
        开 thinking 会把 token 预算耗在推理链上）。
        """
        payload: dict = {
            "model": self.model,
            "max_tokens": int(max_tokens),
            "temperature": float(temperature),
            "messages": list(messages),
        }
        if json_mode:
            payload["response_format"] = {"type": "json_object"}
        if thinking is not None:
            payload["chat_template_kwargs"] = {"enable_thinking": bool(thinking)}
        r = await self._http.post(f"{self.base_url}/chat/completions",
                                  json=payload,
                                  timeout=timeout or self._timeout)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    async def aclose(self) -> None:
        if self._own:
            await self._http.aclose()


def encode_image_b64(data: bytes, *, max_edge: int = 768,
                     fmt: str = "JPEG") -> Optional[str]:
    """图片前处理：Pillow 解码 → 最长边缩放到 max_edge 内 → base64。

    解码失败/截断/非图返回 None（调用方按拒收处理）。同步函数
    （CPU 短任务，调用方自行 to_thread）。
    """
    from PIL import Image
    try:
        with Image.open(io.BytesIO(data)) as im:
            im.load()
            if max(im.size) > max_edge:
                ratio = max_edge / max(im.size)
                size = (max(1, round(im.width * ratio)),
                        max(1, round(im.height * ratio)))
                im = im.resize(size)
            if fmt == "JPEG" and im.mode not in ("RGB", "L"):
                im = im.convert("RGB")
            buf = io.BytesIO()
            im.save(buf, fmt)
            return base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:  # noqa: BLE001 - 解码失败一律按拒收
        return None


# ---------------------------------------------------------------------------
# 端点资源注册表（平台资源：配置驱动、惰性建客户端、统一生命周期）
# 消费方（算子集）只声明配置——与 map_prompt 的 PromptPack 同款形态，
# 不维护自己的客户端包；env 覆盖支持跨机器部署零代码改动。
# ---------------------------------------------------------------------------

_ENDPOINT_CFG: dict[str, dict] = {}
_ENDPOINT_CLIENTS: dict[str, AsyncLLMClient] = {}


def register_endpoint(name: str, *, base_url: str, model: str,
                      max_connections: int = 56, timeout: float = 600.0,
                      base_url_env: str = "", model_env: str = "") -> None:
    """声明一个 LLM 端点资源（重复注册后者覆盖；改配置用 reconfigure）。"""
    _ENDPOINT_CFG[name] = dict(base_url=base_url, model=model,
                                max_connections=max_connections,
                                timeout=timeout, base_url_env=base_url_env,
                                model_env=model_env)
    _ENDPOINT_CLIENTS.pop(name, None)


def reconfigure_endpoint(name: str, **overrides) -> None:
    """更新端点配置（如按并发上限调池），已建客户端作废重建。"""
    if name not in _ENDPOINT_CFG:
        raise KeyError(f"未声明的端点资源：{name}")
    _ENDPOINT_CFG[name].update(overrides)
    _ENDPOINT_CLIENTS.pop(name, None)


def get_llm_client(name: str) -> AsyncLLMClient:
    """取端点的共享客户端（惰性建；env 覆盖 base_url/model）。"""
    if name not in _ENDPOINT_CLIENTS:
        cfg = _ENDPOINT_CFG.get(name)
        if cfg is None:
            raise KeyError(f"未声明的端点资源：{name}"
                           f"（已声明：{sorted(_ENDPOINT_CFG) or '无'}）")
        base_url = (os.environ.get(cfg["base_url_env"], cfg["base_url"])
                    if cfg["base_url_env"] else cfg["base_url"])
        model = (os.environ.get(cfg["model_env"], cfg["model"])
                 if cfg["model_env"] else cfg["model"])
        _ENDPOINT_CLIENTS[name] = AsyncLLMClient(
            base_url=base_url, model=model,
            max_connections=cfg["max_connections"], timeout=cfg["timeout"])
    return _ENDPOINT_CLIENTS[name]


def inject_endpoint_client(name: str, client: AsyncLLMClient) -> None:
    """冒烟注入（MockTransport 版客户端顶替惰性构造）。"""
    _ENDPOINT_CLIENTS[name] = client


async def close_all_llm() -> None:
    """平台收尾：关全部端点客户端（run_stages 退出期统一调用）。"""
    for client in _ENDPOINT_CLIENTS.values():
        await client.aclose()
    _ENDPOINT_CLIENTS.clear()
