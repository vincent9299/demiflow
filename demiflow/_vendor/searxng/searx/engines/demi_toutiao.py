# SPDX-License-Identifier: AGPL-3.0-or-later
"""demiwtg 中文源：今日头条搜索全文本图链抽取（自 ToutiaoEngine 移植）。

- so.toutiao.com 搜索页含内联 JSON，比仅扫 <img> 更全；
- toutiaoimg.com 为签名图床普遍 403 防盗链，出口剔除；
  仅保留 byteimg/douyinpic CDN（host 命中排前）。
"""

import re
from urllib.parse import urlencode

from searx.exceptions import SearxEngineResponseException

about = {
    "website": "https://so.toutiao.com/",
    "wikidata_id": None,
    "official_api_documentation": None,
    "use_official_api": False,
    "require_api_key": False,
    "results": "HTML",
}

categories = ["images"]
paging = False

base_url = "https://so.toutiao.com/search"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_URL_RE = re.compile(r"https?://[^\s\"'<>]+\.(?:jpg|jpeg|png|webp)", re.I)
_EXT_RE = re.compile(r"\.(?:jpg|jpeg|png|webp)$", re.I)
_HOST_HINT = ("byteimg.com", "douyinpic.com")
_EXCLUDE = ("toutiaoimg.com",)


def request(query, params):
    args = {"keyword": query, "source": "input", "traffic_source": "web_search_tab"}
    params["url"] = f"{base_url}?{urlencode(args)}"
    params["method"] = "GET"
    params["headers"] = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }


def response(resp):
    urls = _URL_RE.findall(resp.text or "")
    out, seen = [], set()
    ordered = sorted(
        urls, key=lambda u: 0 if any(h in u.lower() for h in _HOST_HINT) else 1)
    for u in ordered:
        if not _EXT_RE.search(u):
            continue
        if u.lower().startswith("http://"):
            u = "https://" + u[u.find("://") + 3:]
        low = u.lower()
        if any(k in low for k in _EXCLUDE):
            continue
        if any(k in low for k in ("logo", "icon", "avatar", "sprite")):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append({
            "template": "images.html",
            "url": u,
            "img_src": u,
            "title": "",
            "content": "",
            "source": "toutiao.com",
        })
    if not out and not (resp.text or "").strip():
        raise SearxEngineResponseException("toutiao 空应答（疑似反爬）")
    return out
