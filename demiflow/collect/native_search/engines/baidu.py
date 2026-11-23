"""Baidu image search (``acjson`` endpoint), 10 images per page.

The image endpoint expects the cookies Baidu sets on its image home page; they
are fetched once per source and kept for an hour in this worker. A redirect to
Baidu's CAPTCHA page or ``antiFlag`` in the answer are reported as refusals.
"""
from datetime import datetime
import json
import time
from urllib.parse import urlencode

from demiflow.collect.native_search.adapter import SourceAccessDenied, SourceCaptcha, get, html_to_text

about = {'website': 'https://www.baidu.com', 'wikidata_id': 'Q14772', 'official_api_documentation': '',
         'use_official_api': False, 'require_api_key': False, 'results': 'JSON'}
language = 'zh'
paging = True
categories = []
results_per_page = 10
baidu_category = 'images'
time_range_support = True
image_base_url = 'https://image.baidu.com/'
COOKIE_SECONDS = 3600
_cookies = {'value': None, 'until': 0.0}


def setup(settings):
    return baidu_category == 'images'


def image_cookies(headers):
    if _cookies['value'] is not None and _cookies['until'] > time.monotonic():
        return dict(_cookies['value'])
    warmup = get(image_base_url, headers=headers, timeout=10)
    _cookies['value'], _cookies['until'] = dict(warmup.cookies.items()), time.monotonic() + COOKIE_SECONDS
    return dict(_cookies['value'])


def request(query, params):
    args = {'word': query, 'rn': results_per_page, 'pn': (params['pageno'] - 1) * results_per_page,
            'tn': 'resultjson_com'}
    params['cookies'] = image_cookies(params['headers'])
    params['url'] = f'https://image.baidu.com/search/acjson?{urlencode(args)}'
    params['allow_redirects'] = False


def response(resp):
    if 'wappass.baidu.com/static/captcha' in resp.headers.get('Location', ''):
        raise SourceCaptcha('Baidu CAPTCHA')
    data = json.loads(resp.text.replace(r'\/', '/').replace(r"\'", "'"), strict=False)
    if data.get('antiFlag') == 1:
        raise SourceAccessDenied(data.get('message', 'Forbid spider access'))
    found = []
    for item in data.get('data') or []:
        if not item:
            continue
        link = item.get('replaceUrl', [{}])[0]
        published = item.get('bdImgnewsDate')
        found.append({'template': 'images.html', 'url': link.get('FromURL'), 'thumbnail_src': item.get('thumbURL'),
                      'img_src': link.get('ObjURL'), 'title': html_to_text(item.get('fromPageTitle')),
                      'source': item.get('fromURLHost'), 'resolution': f"{item.get('width')} x {item.get('height')}",
                      'img_format': item.get('type'), 'filesize': item.get('filesize'),
                      'publishedDate': datetime.fromisoformat(published) if published else None})
    return found
