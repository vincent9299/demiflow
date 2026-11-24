"""Qwant image search (``api.qwant.com`` v3, 50 images per page).

The API answers with a status document: error code 24 means rate limited, a
redirect URL in the answer means a challenge, 403 means access denied. The
anti-bot ``datadome`` cookie Qwant sets is kept for this source in this worker.
"""
import random
from urllib.parse import urlencode

from demiflow.collect.native_search.adapter import (SourceAccessDenied, SourceCaptcha, SourceError, SourceRateLimited,
                                                   legacy_record)
from demiflow.collect.native_search.errors import check_status
from demiflow.collect.native_search.locales import qwant_locale

about = {'website': 'https://www.qwant.com/', 'wikidata_id': 'Q14657870', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON'}
categories = []
paging = True
max_page = 5
qwant_categ = 'images'
safesearch = True
language_support = True
test_group_value = random.randint(1, 3)
base_url = 'https://www.qwant.com'
api_url = 'https://api.qwant.com/v3/search/'
_datadome = {'value': None}


def setup(settings):
    return qwant_categ == 'images'


def request(query, params):
    if not query:
        return
    per_page = 50
    args = {'q': query, 'count': per_page, 'locale': qwant_locale(params['locale']),
            'offset': (params['pageno'] - 1) * per_page, 'tgp': test_group_value, 'device': 'desktop',
            'safesearch': params['safesearch'], 'displayed': 'true', 'llm': 'true'}
    params['raise_for_httperror'] = False
    params['url'] = f'{api_url}{qwant_categ}?{urlencode(args)}'
    params['cookies']['datadome'] = _datadome['value']
    params['headers'].update({'Accept': 'application/json', 'Referer': f'{base_url}/', 'Origin': base_url})


def response(resp):
    cookie = resp.cookies.get('datadome')
    if cookie:
        _datadome['value'] = cookie
    try:
        answer = resp.json()
    except ValueError:
        answer = {}
    data = answer.get('data', {})
    if answer.get('status') != 'success':
        if data.get('error_code') == 24:
            raise SourceRateLimited('Qwant error 24')
        if answer.get('url') is not None:
            raise SourceCaptcha('Qwant challenge')
        if resp.status_code == 403:
            raise SourceAccessDenied('HTTP error 403')
        raise SourceError(','.join(data.get('message', ['unknown'])) + f" ({data.get('error_code')})")
    check_status(resp)
    found = []
    for item in data.get('result', {}).get('items', []) or []:
        found.append(legacy_record(title=item.get('title', ''), url=item.get('url', ''), template='images.html',
                                   thumbnail_src=item['thumbnail'] or '', img_src=item['media'] or '',
                                   resolution=f"{item['width']} x {item['height']}", img_format=item.get('thumb_type')))
    return found
