"""Bing image search (asynchronous result fragment), 35 images per page."""
import json
from urllib.parse import urlencode

from lxml import html

from demiflow.collect.native_search.engines.bing import localize

about = {'website': 'https://www.bing.com/images', 'wikidata_id': 'Q182496',
         'official_api_documentation': 'https://github.com/MicrosoftDocs/bing-docs', 'use_official_api': False,
         'require_api_key': False, 'results': 'HTML'}
categories = ['images', 'web']
paging = True
enable_http3 = True
safesearch = True
language_support = True
time_range_support = True
base_url = 'https://www.bing.com'
page_size = 35
time_map = {'day': 60 * 24, 'week': 60 * 24 * 7, 'month': 60 * 24 * 31, 'year': 60 * 24 * 365}  # minutes


def request(query, params):
    market = localize(params)
    args = {'q': query, 'async': '1', 'first': (int(params.get('pageno', 1)) - 1) * page_size + 1,
            'count': page_size}
    if market:
        args['mkt'] = market
    if params['time_range']:
        args['qft'] = 'filterui:age-lt%s' % time_map[params['time_range']]
    params['url'] = base_url + '/images/async?' + urlencode(args)


def response(resp):
    found = []
    for item in html.fromstring(resp.text).xpath('//ul[contains(@class, "dgControl_list")]/li'):
        metadata = item.xpath('.//a[@class="iusc"]/@m')
        if not metadata:
            continue
        metadata = json.loads(metadata[0])
        details = ' '.join(item.xpath('.//div[@class="imgpt"]/div/span/text()')).strip().split(' · ')
        found.append({'template': 'images.html', 'url': metadata['purl'], 'thumbnail_src': metadata['turl'],
                      'img_src': metadata['murl'], 'content': metadata.get('desc'),
                      'title': ' '.join(item.xpath('.//div[@class="infnmpt"]//a/text()')).strip(),
                      'source': ' '.join(item.xpath('.//div[@class="imgpt"]//div[@class="lnkw"]//a/text()')).strip(),
                      'resolution': details[0], 'img_format': details[1] if len(details) >= 2 else None})
    return found
