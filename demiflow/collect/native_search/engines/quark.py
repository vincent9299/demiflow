"""Quark (Alibaba) image search (``vt.sm.cn`` picture list API, 10 per page).

An Alibaba X5SEC challenge document in the answer is reported as CAPTCHA.
"""
from datetime import datetime
import json
import re
from urllib.parse import urlencode

from demiflow.collect.native_search.adapter import SourceCaptcha

about = {'website': 'https://quark.sm.cn/', 'wikidata_id': 'Q48816502', 'use_official_api': False,
         'require_api_key': False, 'results': 'HTML'}
language = 'zh'
categories = []
paging = True
results_per_page = 10
quark_category = 'images'
time_range_support = True
CHALLENGE = re.compile(r'\{[^{]*?"action"\s*:\s*"captcha"\s*,\s*"url"\s*:\s*"([^"]+)"[^{]*?\}')


def setup(settings):
    return quark_category == 'images'


def request(query, params):
    args = {'query': query, 'limit': results_per_page, 'start': (params['pageno'] - 1) * results_per_page}
    params['url'] = f'https://vt.sm.cn/api/pic/list?{urlencode(args)}'


def published(value):
    try:
        return datetime.fromtimestamp(int(value))
    except (ValueError, TypeError):
        return None


def response(resp):
    text = resp.text
    if CHALLENGE.search(text):
        raise SourceCaptcha('Alibaba CAPTCHA')
    items = json.loads(text).get('data', {}).get('hit', {}).get('imgInfo', {}).get('item', [])
    return [{'template': 'images.html', 'url': item.get('imgUrl'), 'thumbnail_src': item.get('img'),
             'img_src': item.get('bigPicUrl'), 'title': item.get('title'), 'source': item.get('site'),
             'resolution': f"{item['width']} x {item['height']}", 'publishedDate': published(item.get('publish_time'))}
            for item in items]
