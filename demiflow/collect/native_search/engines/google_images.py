"""Google image search through the WML endpoint (same transport as the ``google`` source).

Locale filters (``lr``/``cr``) are not sent; safe search uses Google's image
filter values. Results are the ``/imgres`` links: original image, page and
declared size; thumbnails come from Google's thumbnail host.
"""
import random
from urllib.parse import parse_qs, unquote, urlencode, urlparse

from demiflow.collect.native_search.adapter import image_result
from demiflow.collect.native_search.engines.google import FEATURE_PHONES, TIME_RANGES, page
from demiflow.collect.native_search.locales import google_parameters

about = {'website': 'https://images.google.com', 'wikidata_id': 'Q521550',
         'official_api_documentation': 'https://developers.google.com/custom-search', 'use_official_api': False,
         'require_api_key': False, 'results': 'XML'}
categories = ['images', 'web']
paging = True
max_page = 50
time_range_support = True
language_support = True
safesearch = True
enable_http3 = True
SAFE_SEARCH = {0: 'images', 1: 'active', 2: 'active'}


def request(query, params):
    locale = google_parameters(params['locale'])
    args = {'q': query, 'sca_esv': '1', **{k: v for k, v in locale.items() if k not in ('lr', 'cr')}, 'tbm': 'isch'}
    start = (params['pageno'] - 1) * 10
    if start:
        args['start'] = start
    if params['time_range'] in TIME_RANGES:
        args['tbs'] = 'qdr:' + TIME_RANGES[params['time_range']]
    if params['safesearch']:
        args['safe'] = SAFE_SEARCH[params['safesearch']]
    params['url'] = 'https://www.google.com/wml/search?' + urlencode(args)
    params['headers']['User-Agent'] = random.choice(FEATURE_PHONES)
    params['impersonate'] = 'chrome99_android'


def response(resp):
    found = []
    for link in page(resp).xpath('//a[contains(@href, "/imgres?")]'):
        query = parse_qs(urlparse(link.get('href', '')).query)
        image, source = query.get('imgurl', [''])[0], query.get('imgrefurl', [''])[0]
        if not image or not source:
            continue
        width, height, thumbnail = query.get('w', [''])[0], query.get('h', [''])[0], query.get('tbnid', [''])[0]
        found.append(image_result(
            source, image, title=unquote(urlparse(image).path.rsplit('/', 1)[-1]) or urlparse(source).netloc,
            thumbnail_src=f'https://encrypted-tbn0.gstatic.com/images?q=tbn:{thumbnail}',
            resolution=f'{width} x {height}' if width and height else ''))
    return found
