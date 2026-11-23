"""Mwmbl, a non-profit open web search index (JSON API, no paging or locale options)."""
from urllib.parse import urlencode

about = {'website': 'https://github.com/mwmbl/mwmbl', 'use_official_api': True, 'require_api_key': False,
         'results': 'JSON'}
categories = ['general']
paging = False
api_url = 'https://api.mwmbl.org/api/v1'


def request(query, params):
    params['url'] = f"{api_url}/search/?{urlencode({'s': query})}"


def _join(parts):
    return ''.join(part['value'] for part in parts or [])


def response(resp):
    return [{'url': item['url'], 'title': _join(item['title']), 'content': _join(item.get('extract'))}
            for item in resp.json()]
