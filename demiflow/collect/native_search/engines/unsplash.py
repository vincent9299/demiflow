"""Unsplash photo search (site JSON endpoint, 20 per page); tracking ``ixid`` parameters are dropped."""
import json
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from demiflow.collect.native_search.adapter import user_agent

about = {'website': 'https://unsplash.com', 'wikidata_id': 'Q28233552',
         'official_api_documentation': 'https://unsplash.com/developers', 'use_official_api': False,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 20
base_url = 'https://unsplash.com/'
search_url = base_url + 'napi/search/photos?'


def clean_url(url):
    parts = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k != 'ixid']
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, urlencode(query), parts.fragment))


def request(query, params):
    params['url'] = search_url + urlencode({'query': query, 'page': params['pageno'], 'per_page': page_size})
    params['headers']['User-Agent'] = user_agent()


def response(resp):
    data = json.loads(resp.text)
    return [{'template': 'images.html', 'url': clean_url(item['links']['html']),
             'thumbnail_src': clean_url(item['urls']['thumb']), 'img_src': clean_url(item['urls']['regular']),
             'title': item.get('alt_description') or 'unknown', 'content': item.get('description') or ''}
            for item in data.get('results', [])] if 'results' in data else []
