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

    def __init__(self, *, proxy: Optional[str] = None,
                 page_timeout: float = 40.0, headless: bool = True):
        self._proxy = proxy
        self._page_timeout = page_timeout
        self._headless = headless
        self._crawler = None

    def _build(self):
        from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode
        try:
            from crawl4ai import CrawlerRunConfig as RunConfig   # 新版命名
        except ImportError:                                      # noqa: F401
            from crawl4ai import CrawlerConfig as RunConfig      # 旧版命名
        browser_cfg = BrowserConfig(headless=self._headless,
                                    proxy=self._proxy)
        run_cfg = RunConfig(cache_mode=CacheMode.BYPASS,
                            page_timeout=int(self._page_timeout * 1000))
        return AsyncWebCrawler(config=browser_cfg), run_cfg

    async def __aenter__(self) -> "PageCrawler":
        self._crawler, self._run_cfg = self._build()
        await self._crawler.__aenter__()
        return self

    async def __aexit__(self, *exc_info) -> None:
        if self._crawler is not None:
            await self._crawler.__aexit__(*exc_info)
            self._crawler = None

    async def fetch(self, url: str) -> Optional[dict]:
        """抓单页正文；成功返回 {url, title, markdown}，失败返回 None（认缺）。"""
        if self._crawler is None:
            raise RuntimeError("PageCrawler 须 async with 使用（浏览器未启动）")
        try:
            res = await self._crawler.arun(url=url, config=self._run_cfg)
        except Exception:  # noqa: BLE001 - 网络/渲染/超时一律认缺
            return None
        if not getattr(res, "success", False):
            return None
        markdown = _extract_markdown(getattr(res, "markdown", None))
        if not markdown.strip():
            return None
        meta = getattr(res, "metadata", None) or {}
        title = meta.get("title") if isinstance(meta, dict) else None
        return {"url": url, "title": (str(title).strip() if title else None),
                "markdown": markdown}
