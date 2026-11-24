"""DuckDuckGo image search (``i.js`` JSON, results from Bing).

Each query needs the ``vqd`` token of DuckDuckGo's search page; it is fetched
with the same headers and kept for an hour per query and User-Agent in this
worker (at most 256 tokens of at most 256 characters; a longer value is not a
token). Queries of 500 characters or more are declined.
"""
import time
from urllib.parse import quote_plus, urlencode

from demiflow.collect.native_search.adapter import get, image_result
from demiflow.collect.native_search.locales import duckduckgo_locale

about = {'website': 'https://duckduckgo.com/', 'wikidata_id': 'Q12805', 'use_official_api': False,
         'require_api_key': False, 'results': 'JSON (site requests)'}
language_support = True
categories = []
ddg_category = 'images'
paging = True
safesearch = True
send_accept_language_header = False
SAFE_SEARCH = {0: '-2', 1: '', 2: '1'}
BROWSER = 'Mozilla/5.0 (X11; Linux x86_64; rv:139.0) Gecko/20100101 Firefox/139.0'
TOKEN_SECONDS = 3600
TOKEN_ENTRIES = 256
TOKEN_MAX_CHARS = 256
_tokens = {}


def setup(settings):
    return ddg_category == 'images'


def vqd(query, params):
    """The ``vqd`` token for this query, from the worker cache or DuckDuckGo's search page ('' if absent)."""
    key = (query, params['headers'].get('User-Agent'))
    value, until = _tokens.get(key, ('', 0.0))
    if value and until > time.monotonic():
        return value
    page = get(f'https://duckduckgo.com/?q={quote_plus(query)}&iar=images&t=h_', headers=params['headers'],
               timeout=2)
    value = ''
    if page.status_code == 200:
        text = page.text
        start = text.find('vqd="')
        end = text.find('"', start + 5) if start >= 0 else -1
        value = text[start + 5:end] if start >= 0 and end >= 0 else ''
    if len(value) > TOKEN_MAX_CHARS:
        value = ''
    if value:
        if len(_tokens) >= TOKEN_ENTRIES:
            _tokens.pop(next(iter(_tokens)))
        _tokens[key] = (value, time.monotonic() + TOKEN_SECONDS)
    return value


def request(query, params):
    if len(query) >= 500:
        params['url'] = None
        return
    headers = params['headers']
    headers['User-Agent'] = BROWSER
    params['impersonate'] = 'none'
    token = vqd(query, params)
    headers.update({'Accept': '*/*', 'Referer': 'https://duckduckgo.com/', 'Host': 'duckduckgo.com'})
    region, language = duckduckgo_locale(params['locale'])
    args = {'o': 'json', 'q': query, 'u': 'bing', 'l': region, 'bpia': '1', 'vqd': token, 'a': 'h_'}
    params['cookies'].update(ad=language, ah=region, l=region)
    args['ct'] = 'EN' if params['locale'] == 'all' else params['locale'].split('-')[0].upper()
    if params['pageno'] > 1:
        args['s'] = (params['pageno'] - 1) * 100
    safe = SAFE_SEARCH.get(params['safesearch'])
    if safe:
        params['cookies']['p'] = safe
        args['p'] = safe
    params['url'] = f'https://duckduckgo.com/i.js?{urlencode(args)}'


def response(resp):
    return [image_result(item['url'], item['image'], title=item['title'], content='',
                         thumbnail_src=item['thumbnail'], resolution='%s x %s' % (item['width'], item['height']),
                         source=item['source'])
            for item in resp.json()['results']]
