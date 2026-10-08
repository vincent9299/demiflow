# SPDX-License-Identifier: AGPL-3.0-or-later
"""demiwtg 源：iNaturalist 观测照片（官方开放 API，免 key）。

设计定位：结构化自然类源（K_STRUCTURED 槽位，2026-09-04 预留至今启用）——
- research 级观测（社区质检）；
- 照片自带 license_code/attribution → JSON 透传，落清单 license/author 字段
  （wikicommons 之后第二个真实授权数据源）；
- URL 档位推导：square.jpg → medium.jpg → original.jpg（官方静态 CDN 路径
  规律，img_src 给 original、缩图档给 medium）；
- original_dimensions → resolution；
- 限速 60 req/min（官方口径），远高于我们的查询频率。
"""

from urllib.parse import quote

about = {
    "website": "https://www.inaturalist.org/",
    "wikidata_id": "Q182113",
    "official_api_documentation": "https://api.inaturalist.org/v1/docs/",
    "use_official_api": True,
    "require_api_key": False,
    "results": "JSON",
}

categories = ["images"]
paging = False

base_url = "https://api.inaturalist.org/v1/observations"
_UA = ("demiwtg-collector/0.1 "
       "(+https://github.com/vincent9299/demiwtg-data)")


def request(query, params):
    params["url"] = (base_url + "?taxon_name=" + quote(query)
                     + "&photos=true&per_page=30&quality_grade=research")
    params["method"] = "GET"
    params["headers"] = {"User-Agent": _UA, "Accept": "application/json"}


def response(resp):
    try:
        data = resp.json()
    except ValueError:
        return []
    results = []
    for obs in data.get("results") or []:
        photos = obs.get("photos") or []
        if not photos:
            continue
        ph = photos[0]
        url = ph.get("url") or ""
        if "/square.jpg" not in url:
            continue
        stem = url.rsplit("/", 1)[0]
        dims = ph.get("original_dimensions") or {}
        license_ = ph.get("license_code") or obs.get("license_code")
        results.append({
            "template": "images.html",
            "url": f"https://www.inaturalist.org/observations/{obs.get('id')}",
            "img_src": stem + "/original.jpg",
            "thumbnail_src": stem + "/medium.jpg",
            "title": obs.get("species_guess") or "",
            "content": "",
            "source": "inaturalist.org",
            "resolution": f"{dims.get('width')}x{dims.get('height')}"
                          if dims.get("width") else "",
            "license": license_,
            "author": (ph.get("attribution") or "")[:80] or None,
        })
    return results
