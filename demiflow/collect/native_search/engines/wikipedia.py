"""Wikipedia article summary (REST ``page/summary``) in the edition of the request language.

``display_type`` holds ``infobox`` (summary box with extract and image)
and/or ``list`` (one result line). Missing articles are an empty result.
"""
from urllib.parse import quote

from demiflow.collect.native_search.adapter import html_to_text
from demiflow.collect.native_search.errors import check_status
from demiflow.collect.native_search.locales import wikipedia_host

about = {'website': 'https://www.wikipedia.org/', 'wikidata_id': 'Q52',
         'official_api_documentation': 'https://en.wikipedia.org/api/rest_v1/#/Page%20content',
         'use_official_api': True, 'require_api_key': False, 'results': 'JSON'}
categories = ['general']
paging = False
language_support = True
display_type = ['infobox']
summary_url = 'https://{host}/api/rest_v1/page/summary/{title}'
INVALID_TITLE = 'https://mediawiki.org/wiki/HyperSwitch/errors/bad_request'


def request(query, params):
    if query.islower():
        query = query.title()
    params['url'] = summary_url.format(host=wikipedia_host(params['locale']), title=quote(query))
    params['raise_for_httperror'] = False
    params['soft_max_redirects'] = 2


def response(resp):
    if resp.status_code == 404:
        return []
    if resp.status_code == 400:
        try:
            error = resp.json()
        except Exception:
            error = None
        if isinstance(error, dict) and error.get('type') == INVALID_TITLE \
                and error.get('detail') == 'title-invalid-characters':
            return []
    check_status(resp)
    summary = resp.json()
    title = html_to_text((summary.get('titles') or {}).get('display') or summary.get('title'))
    link = summary['content_urls']['desktop']['page']
    found = []
    if 'list' in display_type or summary.get('type') != 'standard':
        found.append({'url': link, 'title': title, 'content': summary.get('description', '')})
    if 'infobox' in display_type and summary.get('type') == 'standard':
        found.append({'infobox': title, 'id': link, 'content': summary.get('extract', ''),
                      'img_src': (summary.get('thumbnail') or {}).get('source'),
                      'urls': [{'title': 'Wikipedia', 'url': link}]})
    return found
