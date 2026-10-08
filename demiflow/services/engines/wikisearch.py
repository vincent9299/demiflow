# SPDX-License-Identifier: AGPL-3.0-or-later
"""Wikipedia keyword engine shipped with the demiflow SearXNG service profile.

Migrated from the existing AGPL-3.0-or-later engine; source license preserved.
The native wikipedia engine returns entity summaries, not keyword recall.
"""

from urllib.parse import quote
from searx.enginelib.traits import EngineTraitsMap

_WIKI_TRAITS = EngineTraitsMap.from_data()["wikipedia"]

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
language_support = True

base_url = "https://{wiki_netloc}/w/rest.php/v1/search/page"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def request(query, params):
    locale = params.get("searxng_locale")
    if not locale or locale == "all":
        raise SearxEngineResponseException("wikisearch requires an explicit language")
    tag = _WIKI_TRAITS.get_region(locale, _WIKI_TRAITS.get_language(locale))
    netloc = _WIKI_TRAITS.custom["wiki_netloc"].get(tag)
    if not netloc:
        raise SearxEngineResponseException("Wikipedia does not declare the requested language")
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
