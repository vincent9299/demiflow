# SPDX-License-Identifier: AGPL-3.0-or-later
"""demiwtg 中文源：花瓣 api.huaban.com/search JSON 接口（自 HuabanApiEngine 移植）。

HTML 页 JS 渲染拦截不迁；pins[].file.key 拼 hbimg.huaban.com 直链（即原图，
file 内宽高即原图尺寸，单档）。
"""

from urllib.parse import urlencode

from searx.exceptions import SearxEngineResponseException

about = {
    "website": "https://huaban.com/",
    "wikidata_id": None,
    "official_api_documentation": None,
    "use_official_api": False,
    "require_api_key": False,
    "results": "JSON",
}

categories = ["images"]
paging = False

base_url = "https://api.huaban.com/search"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def request(query, params):
    args = {"q": query, "limit": "20"}
    params["url"] = f"{base_url}?{urlencode(args)}"
    params["method"] = "GET"
    params["headers"] = {
        "User-Agent": _UA,
        "Accept": "application/json",
        "Referer": "https://huaban.com/",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }


def response(resp):
    try:
        data = resp.json()
    except ValueError as exc:
        raise SearxEngineResponseException("huaban 应答非 JSON（疑似反爬页）") from exc
    results = []
    for p in data.get("pins") or data.get("data") or []:
        if not isinstance(p, dict):
            continue
        f = p.get("file") or {}
        key = f.get("key")
        if not key:
            continue
        w, h = f.get("width"), f.get("height")
        results.append({
            "template": "images.html",
            "url": f"https://huaban.com/pins/{p.get('pin_id')}/" if p.get("pin_id") else "",
            "img_src": "https://hbimg.huaban.com/" + key,
            "title": (p.get("board") or {}).get("title") or "",
            "content": "",
            "source": "huaban.com",
            "resolution": f"{w}x{h}" if w and h else "",
        })
    return results
