# SPDX-License-Identifier: AGPL-3.0-or-later
"""demiwtg 源：360 百科站内搜索（baike.so.com/search）。

结果页结构（2026-09-07 实测）：
  li.res-list > h3.res-title > a[href=/doc/xxx.html]（标题）
              > p.res-desc（摘要，含 cite 尾巴需剥）
内容页 robots 全站禁（User-agent: * Disallow: /）——本引擎只做检索层
（候选+snippet），正文由 docs 线合规兜底链（Wayback/snippet）承接。
"""

import re
from urllib.parse import quote, unquote

about = {
    "website": "https://baike.so.com/",
    "wikidata_id": None,
    "official_api_documentation": None,
    "use_official_api": False,
    "require_api_key": False,
    "results": "HTML",
}

categories = ["general"]
paging = False

base_url = "https://baike.so.com/search/"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_BLOCK_RE = re.compile(
    r'<li class="res-list">\s*<h3 class="res-title">\s*'
    r'<a[^>]+href="(https?://baike\.so\.com/doc/\d+-\d+\.html)"[^>]*>(.*?)</a>'
    r'\s*</h3>\s*<p class="res-desc">(.*?)</p>', re.S)
_TAG_RE = re.compile(r"<[^>]+>")


def request(query, params):
    params["url"] = base_url + "?q=" + quote(query)
    params["method"] = "GET"
    params["headers"] = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }


def response(resp):
    html = resp.text or ""
    results = []
    for url, title, desc in _BLOCK_RE.findall(html):
        desc = _TAG_RE.sub("", desc)
        # 剥 cite 尾巴（原文 URL + 日期回显）
        desc = re.sub(r"https?://\S+\s*\d{4}-\d{2}-\d{2}.*$", "", desc).strip()
        title = _TAG_RE.sub("", title).strip() or unquote(url)
        if not desc:
            continue
        results.append({
            "url": url,
            "title": title,
            "content": desc[:300],
        })
    return results
