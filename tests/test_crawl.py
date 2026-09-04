"""demiflow.collect.crawl 测试：markdown 兼容抽取与惰性依赖。"""

from __future__ import annotations

from demiflow.collect.crawl import PageCrawler, _extract_markdown


def test_extract_markdown_plain_str():
    assert _extract_markdown("# hello") == "# hello"


def test_extract_markdown_container_fit_preferred():
    class C:
        raw_markdown = "# raw"
        fit_markdown = "# fit"
    assert _extract_markdown(C()) == "# fit"


def test_extract_markdown_container_falls_back_to_raw():
    class C:
        raw_markdown = "# raw"
        fit_markdown = "   "        # 空白 fit → 退化 raw
    assert _extract_markdown(C()) == "# raw"


def test_extract_markdown_container_all_empty():
    class C:
        raw_markdown = ""
        fit_markdown = None
    assert _extract_markdown(C()) == ""


def test_lazy_import_module_loads_without_crawl4ai_error():
    # 模块级不 import crawl4ai（惰性）；PageCrawler 仅在 _build 时才需要
    assert PageCrawler is not None
