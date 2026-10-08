# SPDX-License-Identifier: AGPL-3.0-or-later
"""demiwtg 源：Wikipedia 关键词检索（自 operators/text_engines.py WikiEntityEngine 移植）。

上游 searxng 自带的 wikipedia 引擎是单实体摘要 API（title 精确查询），
非关键词搜索——docs 线的 wiki 权威召回（search/page 模糊检索取候选页）
由本引擎承接。zh/en 按 searxng_locale 语言对位。"""

from urllib.parse import quote

from searx.exceptions import SearxEngineResponseException

about = {
    "website": "https://www.wikipedia.org/",
    "wikidata_id": "Q52",
    "official_api_documentation": "https://en.wikipedia.org/api/rest_v1/#/Search",
    "use_official_api": True,
    "require_api_key": False,
    "results": "JSON",
}

categories = ["general"]
paging = False

base_url = "https://{wiki_netloc}/w/rest.php/v1/search/page"
_NETLOC = {"zh": "zh.wikipedia.org", "en": "en.wikipedia.org"}
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def request(query, params):
    locale = params.get("searxng_locale", "en") or "en"
    netloc = _NETLOC.get(str(locale).split("-")[0], _NETLOC["en"])
    params["url"] = f"https://{netloc}/w/rest.php/v1/search/page?q={quote(query)}&limit=8"
    params["method"] = "GET"
    params["headers"] = {"User-Agent": _UA, "Accept": "application/json"}


def response(resp):
    try:
        data = resp.json()
    except ValueError as exc:
        raise SearxEngineResponseException("wiki_search 应答非 JSON") from exc
    results = []
    for p in (data.get("pages") or [])[:8]:
        key = p.get("key")
        if not key:
            continue
        results.append({
            "url": resp.url.split("/w/rest.php")[0] + "/wiki/" + key,
            "title": p.get("title") or key,
            "content": (p.get("excerpt") or "")[:300],
        })
    return results
