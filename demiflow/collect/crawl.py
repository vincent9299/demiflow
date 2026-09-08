"""demiflow 页面抽取原语：URL → 正文 Markdown（Crawl4AI 进程内浏览器封装）。

自 collect_v2.op_crawl 沉淀（2026-09-04，机制归引擎）：
- 浏览器生命周期：async with 单实例，多页并发共享（arun）；并发钳制由
  调用方负责（Semaphore）；
- 版本兼容 shim：CrawlerRunConfig（新版）/ CrawlerConfig（旧版）命名漂移、
  markdown 结果 str / MarkdownGenerationContainer 容器漂移均在此吸收，
  调用方只见稳定协议；
- 惰性依赖：crawl4ai 未安装时模块可 import（extras [crawl]），调用期才报错；
- 代理显式传参（BrowserConfig.proxy）：不依赖环境变量代理——消费方往往
  在启动期清理 env 代理残留，env 代理不可依赖；
- 失败语义认缺：网络/渲染/超时/解析失败返回 None 不抛异常（调用方循环
  不断）；页面 4xx/5xx 同样 None（res.success=False）；
- 输出 {url, title, markdown}：markdown 取 fit（正文裁剪版）优先，
  退化 raw，再退化字符串本体；落盘布局由消费方决定。
"""

from __future__ import annotations

import re
from typing import Optional


def _extract_markdown(md) -> str:
    """Crawl4AI 结果的 markdown 兼容抽取（新旧版本返回形态不同）。

    新版返回 MarkdownGenerationContainer（fit_markdown/raw_markdown 字段），
    旧版直接是 str；fit 优先（正文裁剪版），退化 raw，再退化字符串本体。
    """
    if isinstance(md, str):
        return md
    fit = getattr(md, "fit_markdown", None)
    if isinstance(fit, str) and fit.strip():
        return fit
    raw = getattr(md, "raw_markdown", None)
    if isinstance(raw, str) and raw.strip():
        return raw
    return ""


class PageCrawler:
    """Crawl4AI 浏览器封装：async with 生命周期，fetch 并发共享一个实例。"""

    # 带项目标识的浏览器 UA（2026-09-07 合规改造：浏览器兼容性保底的
    # 同时声明采集身份——比裸 HeadlessChrome 诚实，比纯标识 UA 存活）
    DEFAULT_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 "
                  "demiwtg-collector/0.1 (+https://github.com/vincent9299/demiwtg-data)")

    def __init__(self, *, proxy: Optional[str] = None,
                 page_timeout: float = 40.0, headless: bool = True,
                 user_agent: Optional[str] = None,
                 recycle_every: Optional[int] = 50):
        self._proxy = proxy
        self._page_timeout = page_timeout
        self._headless = headless
        self._ua = user_agent or self.DEFAULT_UA
        self._crawler = None
        # 浏览器定量回收（2026-09-08 宕机复盘）：crawl4ai 超时路径的
        # context/tab 碎片在单例长生命周期进程内只进不出（本机 docs 线
        # 7h 累积→内存耗尽→平台硬重启实证）。每 recycle_every 次抓取
        # 重建浏览器进程，碎片封顶；None=关闭（行为同旧版）。
        self._recycle_every = recycle_every
        self._fetch_count = 0
        self._recycle_lock = None

    def _build(self):
        from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode
        try:
            from crawl4ai import CrawlerRunConfig as RunConfig   # 新版命名
        except ImportError:                                      # noqa: F401
            from crawl4ai import CrawlerConfig as RunConfig      # 旧版命名
        browser_cfg = BrowserConfig(headless=self._headless,
                                    proxy=self._proxy,
                                    user_agent=self._ua)
        run_cfg = RunConfig(cache_mode=CacheMode.BYPASS,
                            page_timeout=int(self._page_timeout * 1000))
        return AsyncWebCrawler(config=browser_cfg), run_cfg

    async def __aenter__(self) -> "PageCrawler":
        self._crawler, self._run_cfg = self._build()
        self._fetch_count = 0
        await self._crawler.__aenter__()
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self._crawler is not None:
            await self._crawler.__aexit__(*exc_info)
            self._crawler = None

    async def fetch(self, url: str) -> Optional[dict]:
        """抓单页；成功返回 {url, title, markdown, images}，失败 None（认缺）。

        images = [{src, alt}]：页面内嵌图（图文绑定原料）——Crawl4AI media
        透传为主、markdown 内联 ![alt](src) 解析兜底，按出现序去重。
        """
        if self._crawler is None:
            raise RuntimeError("PageCrawler 须 async with 使用（浏览器未启动）")
        await self._maybe_recycle()
        try:
            res = await self._crawler.arun(url=url, config=self._run_cfg)
        except Exception:  # noqa: BLE001 - 网络/渲染/超时一律认缺
            return None
        finally:
            self._fetch_count += 1
        if not getattr(res, "success", False):
            return None
        markdown = _extract_markdown(getattr(res, "markdown", None))
        if not markdown.strip():
            return None
        meta = getattr(res, "metadata", None) or {}
        title = meta.get("title") if isinstance(meta, dict) else None
        return {"url": url, "title": (str(title).strip() if title else None),
                "markdown": markdown,
                "images": _extract_images(markdown, getattr(res, "media", None))}

    async def _maybe_recycle(self) -> None:
        """浏览器进程定量重建（防碎片累积）。

        到量即换新实例；旧实例延迟 page_timeout+10s 关闭——在途 arun
        要么已自然结束，要么随旧浏览器消亡走认缺（失败语义不变）。
        锁内双检防并发重复回收。
        """
        if not self._recycle_every or self._fetch_count < self._recycle_every:
            return
        if self._recycle_lock is None:
            import asyncio
            self._recycle_lock = asyncio.Lock()
        async with self._recycle_lock:
            if self._fetch_count < self._recycle_every:
                return                          # 并发路径已回收过
            import asyncio
            old = self._crawler
            self._crawler, self._run_cfg = self._build()
            self._fetch_count = 0
            await self._crawler.__aenter__()

            async def _retire():
                await asyncio.sleep(self._page_timeout + 10)
                try:
                    await old.__aexit__(None, None, None)
                except Exception:               # noqa: BLE001 - 退役尽力而为
                    pass
            asyncio.create_task(_retire())


_IMG_MD_RE = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)[^)]*\)")


def _extract_images(markdown: str, media) -> list:
    """内嵌图清单（图文绑定原料）：Crawl4AI media 优先，markdown 内联兜底。"""
    out, seen = [], set()

    def _add(src, alt):
        src = (src or "").strip()
        if (not src or src.startswith(("data:", "javascript:"))
                or src in seen):
            return
        seen.add(src)
        out.append({"src": src, "alt": (alt or "").strip()})

    if isinstance(media, dict):
        for m in media.get("images") or []:
            if isinstance(m, dict):
                _add(m.get("src"), m.get("alt"))
    for alt, src in _IMG_MD_RE.findall(markdown or ""):
        _add(src, alt)
    return out
