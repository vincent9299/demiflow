"""Google web search through the lightweight WML endpoint.

Requests ask for the mobile WML layout (feature-phone User-Agent, Android TLS
fingerprint) whose markup is small and stable; ``hl``/``lr``/``cr`` follow the
request language (see ``locales.google_parameters``). Google's "sorry" pages
are reported as CAPTCHA.
"""
import random
from urllib.parse import unquote, urlencode, urlsplit

from lxml import html

from demiflow.collect.native_search.adapter import SourceCaptcha, extract_text, legacy_record, main_result
from demiflow.collect.native_search.locales import google_parameters

about = {'website': 'https://www.google.com', 'wikidata_id': 'Q9366', 'use_official_api': False,
         'require_api_key': False, 'results': 'HTML'}
categories = ['general', 'web']
paging = True
max_page = 50
time_range_support = True
language_support = True
safesearch = True
TIME_RANGES = {'day': 'd', 'week': 'w', 'month': 'm', 'year': 'y'}
SAFE_SEARCH = {0: 'off', 1: 'medium', 2: 'high'}
FEATURE_PHONES = (
    'Nokia7610/2.0 (5.0509.0) SymbianOS/7.0s Series60/2.1 Profile/MIDP-2.0 Configuration/CLDC-1.0',
    'Nokia7610/2.0 (7.0642.0) SymbianOS/7.0s Series60/2.1 Profile/MIDP-2.0 Configuration/CLDC-1.0',
    'Nokia6230/2.0 (05.50) Profile/MIDP-2.0 Configuration/CLDC-1.1',
    'Nokia6230i/2.0 (03.80) Profile/MIDP-2.0 Configuration/CLDC-1.1',
    'Nokia6280/2.0 (03.60) Profile/MIDP-2.0 Configuration/CLDC-1.1',
    'NokiaN72/2.0617.1.0.3 Series60/2.8 Profile/MIDP-2.0 Configuration/CLDC-1.1',
)
RESULT = '//div[contains(@class, "zMzFAb")]'
TITLE = './/a[contains(@class, "fuLhoc")]//span[contains(@class, "CVA68e")]'
LINK = './/a[contains(@class, "fuLhoc")]/@href'
SNIPPET = './/div[contains(@class, "taTFJ")]//span[contains(@class, "FrIlee")]'
THUMBNAIL = './/img[contains(@src, "encrypted-tbn")]/@src'
SUGGESTION = '//table[contains(@class, "HExoMb")]//a[contains(@class, "ZWRArf")]'


def request(query, params):
    args = {'q': query, 'sca_esv': '1', **google_parameters(params['locale'])}
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


def blocked(resp):
    """True for Google's bot protection: the sorry host/path, a redirect, or a short stub naming it."""
    where = urlsplit(str(resp.url))
    return (where.hostname == 'sorry.google.com' or where.path.startswith('/sorry') or resp.status_code == 302
            or (len(resp.text) < 2000 and '/sorry/' in resp.text))


def page(resp):
    if blocked(resp):
        raise SourceCaptcha('Google sorry page')
    text = resp.text
    if text.lstrip().startswith('<?xml'):
        text = text.split('?>', 1)[-1]
    return html.fromstring(text)


def unwrap(link):
    return unquote(link[7:].split('&sa=U')[0]) if link.startswith('/url?q=') else link


def response(resp):
    dom = page(resp)
    found = []
    for item in dom.xpath(RESULT):
        titles, links = item.xpath(TITLE), item.xpath(LINK)
        if not titles or not links:
            continue
        thumbnails = item.xpath(THUMBNAIL)
        found.append(main_result(unwrap(links[0]), title=extract_text(titles[0]) or '',
                                 content=extract_text(item.xpath(SNIPPET)) or '',
                                 thumbnail=thumbnails[0] if thumbnails else ''))
    found.extend(legacy_record(suggestion=extract_text(node)) for node in dom.xpath(SUGGESTION))
    return found
