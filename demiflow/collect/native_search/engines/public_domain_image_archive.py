"""Public Domain Image Archive (pdimagearchive.org) through its search proxy (20 per page).

The proxy URL is read from the site's search script once per source; a 403
answer forgets it so the next search looks it up again.
"""
from json import dumps
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from demiflow.collect.native_search.adapter import SourceAccessDenied, SourceError, get

about = {'website': 'https://pdimagearchive.org', 'use_official_api': False, 'require_api_key': False,
         'results': 'JSON'}
pdia_base_url = 'https://pdimagearchive.org'
categories = ['images']
page_size = 20
paging = True
THUMBNAIL_SUFFIX = '?fit=max&h=360&w=360'
CONFIG_START, CONFIG_END = '/_astro/InfiniteSearch.', '.js'
PROXY_URL = re.compile(r'"(https://.*?/search-proxy)"')
_proxy = {'url': None}


def clean_url(url):
    parts = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k not in ('ixid', 's')]
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, urlencode(query), parts.fragment))


def proxy_url():
    if _proxy['url']:
        return _proxy['url']
    page = get(f'{pdia_base_url}/search/?q=', timeout=3)
    if page.status_code != 200:
        raise LookupError('Search page unavailable')
    try:
        start = page.text.index(CONFIG_START) + len(CONFIG_START)
        part = page.text[start:page.text.index(CONFIG_END, start)]
    except ValueError:
        part = ''
    script = get(pdia_base_url + CONFIG_START + part + CONFIG_END)
    if script.status_code != 200:
        raise LookupError('Search script unavailable')
    match = PROXY_URL.search(script.text)
    if match is None:
        raise LookupError('Search proxy URL not found')
    _proxy['url'] = match.group(1)
    return _proxy['url']


def request(query, params):
    params['url'] = proxy_url()
    params['method'] = 'POST'
    params['headers'] = {'Content-Type': 'application/json'}
    params['data'] = dumps({'page': params['pageno'] - 1, 'query': query, 'hitsPerPage': page_size,
                            'indexName': 'prod_all-images'})
    params['raise_for_httperror'] = False


def response(resp):
    data = resp.json()
    if resp.status_code == 403:
        _proxy['url'] = None
        raise SourceAccessDenied('Search proxy refused')
    if resp.status_code != 200:
        raise SourceError('Search proxy error')
    if 'results' not in data:
        return []
    found = []
    for hit in data['results'][0]['hits']:
        content = []
        if hit.get('themes'):
            content.append('Themes: ' + hit['themes'])
        if hit.get('encompassingWork'):
            content.append('Encompassing work: ' + hit['encompassingWork'])
        image = hit['thumbnail'].split('?')[0]
        found.append({'template': 'images.html', 'url': clean_url(f"{pdia_base_url}/images/{hit['objectID']}"),
                      'img_src': clean_url(image), 'thumbnail_src': clean_url(image + THUMBNAIL_SUFFIX),
                      'title': f"{hit['title'].strip()} by {hit['artist']} {hit.get('displayYear', '')}",
                      'content': '\n'.join(content)})
    return found
