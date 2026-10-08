# SPDX-License-Identifier: AGPL-3.0-or-later
"""demiwtg 中文源：百度图片 acjson 接口（自 operators/search.py BaiduEngine 移植）。

- 无会话 cookie 直接调 acjson 会被 antiFlag 拦截：setup() 启动期同步预热
  拿 BAIDUID（长期 cookie，进程生命周期内复用）；
- objURL 混淆编码不用；middleURL（明文较大）优先，回退 thumbURL/hoverURL；
- 宽高从 CDN URL 查询串 ?w=&h= 提取（acjson 声明尺寸与实际服务尺寸常不符）；
- antiFlag 应答抛 SearxEngineResponseException（引擎级熔断，不影响其他引擎）。
"""

import re
import urllib.request
from urllib.parse import urlencode

from searx.exceptions import SearxEngineResponseException

about = {
    "website": "https://image.baidu.com/",
    "wikidata_id": "Q14772",
    "official_api_documentation": None,
    "use_official_api": False,
    "require_api_key": False,
    "results": "JSON",
}

categories = ["images"]
paging = False

base_url = "https://image.baidu.com/search/acjson"
home_url = "https://www.baidu.com/"
cookies = {}
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def setup(engine_settings=None):  # pylint: disable=unused-argument
    """启动期预热拿 BAIDUID；失败不阻断（留给正式请求自行暴露）。"""
    global cookies
    req = urllib.request.Request(home_url, headers={
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9"})
    try:
        import http.cookiejar
        jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(jar))
        with opener.open(req, timeout=10):
            pass
        cookies = {c.name: c.value for c in jar}
    except Exception:  # pylint: disable=broad-except
        pass
    return True


def request(query, params):
    args = {
        "tn": "resultjson_com", "ipn": "rj", "ct": "201326592",
        "fp": "result", "word": query, "queryWord": query,
        "rn": "60", "pn": "0", "ie": "utf-8",
    }
    params["url"] = f"{base_url}?{urlencode(args)}"
    params["method"] = "GET"
    params["headers"] = {
        "User-Agent": _UA,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://image.baidu.com/",
        "X-Requested-With": "XMLHttpRequest",
    }
    if cookies:
        params["cookies"] = dict(cookies)


def _pick_url(it):
    for key in ("middleURL", "thumbURL", "hoverURL"):
        u = (it.get(key) or "").strip()
        if u and u.lower().startswith("http"):
            return u
    return None


def _dims_from_url(url):
    mw = re.search(r"[?&]w=(\d+)", url)
    mh = re.search(r"[?&]h=(\d+)", url)
    if mw and mh:
        return int(mw.group(1)), int(mh.group(1))
    return None, None


def response(resp):
    try:
        data = resp.json()
    except ValueError as exc:
        raise SearxEngineResponseException("baidu 应答非 JSON（疑似反爬页）") from exc
    if data.get("antiFlag"):
        raise SearxEngineResponseException(f"baidu 反爬拦截: {data.get('message')!r}")

    results = []
    for it in data.get("data") or []:
        if not isinstance(it, dict):
            continue
        content_url = _pick_url(it)
        if not content_url:
            continue
        w, h = _dims_from_url(content_url)
        results.append({
            "template": "images.html",
            "url": it.get("fromURL") or it.get("hoverURL") or content_url,
            "img_src": content_url,
            "title": it.get("fromPageTitleEnc") or "",
            "content": "",
            "source": "baidu.com",
            "resolution": f"{w}x{h}" if w and h else "",
        })
    return results
