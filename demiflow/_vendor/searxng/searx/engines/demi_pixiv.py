# SPDX-License-Identifier: AGPL-3.0-or-later
"""demiwtg 源：Pixiv 搜索 ajax 接口（自 PixivEngine 移植，无需登录，必须带站内 Referer）。

档位契约（下载端按序试错，tiers 额外字段透传 JSON API）：
[original.jpg, original.png, master1200]——原图扩展名接口不告知，jpg/png 依次试；
ugoira（illustType=2）无静态原图，只给 master1200 首帧；custom-thumb（AI 作）
无 img-original 对应档，custom1200 即最大档。xRestrict>0（R18）出口剔除。
"""

import re
from urllib.parse import quote

from searx.exceptions import SearxEngineResponseException

about = {
    "website": "https://www.pixiv.net/",
    "wikidata_id": "Q306757",
    "official_api_documentation": None,
    "use_official_api": False,
    "require_api_key": False,
    "results": "JSON",
}

categories = ["images"]
paging = False

base_url = "https://www.pixiv.net/ajax/search/artworks/"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def _regular_url(thumb):
    """250 方图 → master（去裁剪前缀 + _square1200 换 _master1200，_p0 有无保留）。"""
    url = thumb.replace("/c/250x250_80_a2/", "/")
    return re.sub(r"_square1200(\.\w+)$", r"_master1200\1", url)


def _candidate_urls(thumb, illust_type):
    master = _regular_url(thumb)
    if (illust_type if isinstance(illust_type, int) else 0) == 2 or "/custom-thumb/" in master:
        return [master]
    orig = re.sub(r"/img-master/", "/img-original/", master)
    orig = re.sub(r"_master1200\.\w+$", "", orig)
    return [orig + ".jpg", orig + ".png", master]


def request(query, params):
    params["url"] = base_url + quote(query) + "?lang=en"
    params["method"] = "GET"
    params["headers"] = {
        "User-Agent": _UA,
        "Referer": "https://www.pixiv.net/",
        "Accept": "application/json",
    }


def response(resp):
    try:
        data = resp.json()
    except ValueError as exc:
        raise SearxEngineResponseException("pixiv 应答非 JSON") from exc
    if data.get("error"):
        raise SearxEngineResponseException(f"pixiv error=true: {data.get('message')!r}")

    results = []
    for a in ((data.get("body") or {}).get("illustManga") or {}).get("data") or []:
        if a.get("xRestrict", 0) > 0:
            continue
        url = a.get("url")
        if not url:
            continue
        tiers = _candidate_urls(url, a.get("illustType"))
        results.append({
            "template": "images.html",
            "url": f"https://www.pixiv.net/artworks/{a.get('id')}",
            "img_src": tiers[0],
            "thumbnail_src": url,
            "title": a.get("title") or "",
            "content": "",
            "source": "pixiv.net",
            "author": a.get("userName"),
            "resolution": f"{a.get('width')}x{a.get('height')}",
            "tiers": tiers,
        })
    return results
