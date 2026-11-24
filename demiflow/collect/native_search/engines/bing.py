"""Bing web search (HTML results page) in the market of the request language."""
import base64
from urllib.parse import parse_qs, urlencode, urlparse

from lxml import html

from demiflow.collect.native_search.adapter import extract_text
from demiflow.collect.native_search.locales import bing_market

about = {'website': 'https://www.bing.com', 'wikidata_id': 'Q182496',
         'official_api_documentation': 'https://github.com/MicrosoftDocs/bing-docs', 'use_official_api': False,
         'require_api_key': False, 'results': 'HTML'}
categories = ['general', 'web']
safesearch = True
language_support = True
enable_http3 = True
base_url = 'https://www.bing.com'
SAFE_SEARCH = {0: 'off', 1: 'moderate', 2: 'strict'}
REDIRECT = 'https://www.bing.com/ck/a?'


def localize(params):
    """Market of the request language; Bing follows Accept-Language closely, so it names the market too."""
    market = bing_market(params['locale'])
    if market:
        params['headers']['Accept-Language'] = f"{market},{market.split('-')[0]};q=0.9"
    return market


def request(query, params):
    market = localize(params)
    args = {'q': query, 'adlt': SAFE_SEARCH.get(params.get('safesearch', 0), 'off')}
    if market:
        args['mkt'] = market
    params['url'] = f'{base_url}/search?{urlencode(args)}'


def target(href):
    """Destination of a Bing click-tracking link (``u=a1`` + base64 of the URL)."""
    if href.startswith(REDIRECT):
        value = (parse_qs(urlparse(href).query).get('u') or [''])[0]
        if value.startswith('a1'):
            encoded = value[2:] + '=' * (-len(value[2:]) % 4)
            return base64.urlsafe_b64decode(encoded).decode('utf-8', errors='replace')
    return href


def response(resp):
    found = []
    for item in html.fromstring(resp.text).xpath('//ol[@id="b_results"]/li[contains(@class, "b_algo")]'):
        links = item.xpath('.//h2/a')
        if not links:
            continue
        href, title = links[0].attrib.get('href', ''), extract_text(links[0])
        if not href or not title:
            continue
        paragraphs = item.xpath('.//p')
        for paragraph in paragraphs:
            for icon in paragraph.xpath('.//span[@class="algoSlug_icon"]'):
                icon.getparent().remove(icon)
        found.append({'url': target(href), 'title': title, 'content': extract_text(paragraphs)})
    return found
