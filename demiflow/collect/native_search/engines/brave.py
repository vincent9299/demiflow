"""Brave image search; results come from the data embedded in the results page.

The embedded data is read by ``native_search.brave_data`` without executing
page scripts. Country, interface language and safe search travel as cookies.
"""
from urllib.parse import urlencode

from demiflow.collect.native_search.adapter import legacy_record
from demiflow.collect.native_search.brave_data import extract_data
from demiflow.collect.native_search.locales import brave_cookies

about = {'website': 'https://search.brave.com/', 'wikidata_id': 'Q22906900', 'use_official_api': False,
         'require_api_key': False, 'results': 'HTML'}
base_url = 'https://search.brave.com/'
categories = []
enable_http3 = True
brave_category = 'images'
brave_spellcheck = False
paging = False
max_page = 10
safesearch = True
language_support = True
time_range_support = False
SAFE_SEARCH = {2: 'strict', 1: 'moderate', 0: 'off'}


def setup(settings):
    return brave_category == 'images'


def request(query, params):
    args = {'q': query, 'source': 'web'}
    if brave_spellcheck:
        args['spellcheck'] = '1'
    params['headers']['Accept-Encoding'] = 'gzip, deflate'
    params['url'] = f'{base_url}{brave_category}?{urlencode(args)}'
    country, ui_lang = brave_cookies(params['locale'])
    params['cookies'].update(safesearch=SAFE_SEARCH.get(params['safesearch'], 'off'), useLocation='0',
                             summarizer='0', country=country, ui_lang=ui_lang)


def response(resp):
    page = extract_data(resp.text)['data'][1]['data']['body']['response']
    return [legacy_record(template='images.html', url=item['url'], title=item['title'], source=item['source'],
                          img_src=item['properties']['url'], thumbnail_src=item['thumbnail']['src'])
            for item in page['results']]
