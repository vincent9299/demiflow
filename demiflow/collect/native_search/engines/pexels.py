"""Pexels photo search through the site's JSON API.

The API wants the ``secret-key`` the Pexels web app embeds in one of its
scripts; it is read from the home page scripts once per source and kept in this
worker. ``fallback_secret_key`` may supply a key when that lookup fails.
"""
import re
from urllib.parse import urlencode

from lxml import html

from demiflow.collect.native_search.adapter import SourceAccessDenied, SourceError, get, legacy_record

about = {'website': 'https://www.pexels.com', 'wikidata_id': 'Q101240504', 'use_official_api': False,
         'require_api_key': False, 'results': 'JSON'}
base_url = 'https://www.pexels.com'
categories = ['images']
results_per_page = 20
paging = True
time_range_support = True
enable_http2 = False
fallback_secret_key = ''
DATE_FROM = {'day': 'last_24_hours', 'week': 'last_week', 'month': 'last_month', 'year': 'last_year'}
SECRET_KEY = re.compile('"secret-key":\b*"(.*?)"')
BROWSER = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
           'Chrome/136.0.0.0 Safari/537.36')
_secret = {'value': None}


def secret_key():
    home = get(base_url, headers={'User-Agent': BROWSER, 'Referer': base_url, 'Sec-GPC': '1',
                                  'Connection': 'keep-alive'})
    if home.status_code != 200:
        raise SourceError('Pexels home page unavailable')
    for source in html.fromstring(home.text).xpath('//script/@src'):
        script = get(source)
        if script.status_code != 200:
            raise SourceError('Pexels script unavailable')
        match = SECRET_KEY.search(script.text)
        if match:
            return match.group(1)
    raise SourceError('Pexels secret key not found')


def request(query, params):
    args = {'query': query, 'page': params['pageno'], 'per_page': results_per_page}
    if params['time_range']:
        args['date_from'] = DATE_FROM[params['time_range']]
    params['url'] = f'{base_url}/en-us/api/v3/search/photos?{urlencode(args)}'
    if not _secret['value']:
        try:
            _secret['value'] = secret_key()
        except (SourceError, SourceAccessDenied):
            if not fallback_secret_key:
                raise
            _secret['value'] = fallback_secret_key
    params['headers']['secret-key'] = _secret['value']


def response(resp):
    found = []
    for item in resp.json().get('data', []):
        photo = item['attributes']
        found.append(legacy_record(**{'template': 'images.html', 'url': f"{base_url}/photo/{photo['slug']}-{photo['id']}/",
                      'title': photo['title'], 'content': photo['description'],
                      'thumbnail_src': photo['image']['small'], 'img_src': photo['image']['download_link'],
                      'resolution': f"{photo['width']}x{photo['height']}", 'author': f"{photo['user']['username']}"}))
    return found
