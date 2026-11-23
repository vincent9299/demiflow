"""Yandex web search (site-search frame) and image search.

``search_type`` selects ``web`` or ``images``. Image results come from the
JSON state embedded in the results page; the largest rendition offered is
used. A ``x-yandex-captcha`` response header is reported as CAPTCHA.
"""
from html import unescape
import json
from urllib.parse import urlencode

from lxml import html

from demiflow.collect.native_search.adapter import SourceCaptcha, extract_text, size_text

about = {'website': 'https://yandex.com/', 'wikidata_id': 'Q5281', 'official_api_documentation': '?',
         'use_official_api': False, 'require_api_key': False, 'results': 'HTML'}
categories = []
paging = True
enable_http3 = True
search_type = ''
base_url_web = 'https://yandex.com/search/site/'
base_url_images = 'https://yandex.com/images/search'
WEB_LANGUAGES = ('ru', 'en', 'be', 'fr', 'de', 'id', 'kk', 'tt', 'tr', 'uk')
DISPLAY_COOKIE = 'yp=1716337604.sp.family%3A0#1685406411.szm.1:1920x1080:1920x999'
STATE_START = '{"location":"/images/search/'
STATE_ENDS = ('advRsyaSearchColumn":null}}', 'false}}}')


def request(query, params):
    web = {'tmpl_version': 'releases', 'text': query, 'web': '1', 'frame': '1', 'searchid': '3131712'}
    language = params['language'].split('-')[0]
    if language in WEB_LANGUAGES:
        web['lang'] = language
    images = {'text': query, 'uinfo': 'sw-1920-sh-1080-ww-1125-wh-999'}
    if params['pageno'] > 1:
        web['p'] = images['p'] = params['pageno'] - 1
    params['cookies'] = {'cookie': DISPLAY_COOKIE}
    if search_type == 'web':
        params['url'] = f'{base_url_web}?{urlencode(web)}'
    elif search_type == 'images':
        params['url'] = f'{base_url_images}?{urlencode(images)}'


def between(text, start, end):
    begin = text.index(start) + len(start)
    return text[begin:text.index(end, begin)]


def image_state(resp):
    page = unescape(html.tostring(html.fromstring(resp.text), encoding='unicode'))
    for end in STATE_ENDS:
        try:
            return json.loads(STATE_START + between(page, STATE_START, end) + end)
        except ValueError:
            if end == STATE_ENDS[-1]:
                raise
    raise ValueError('Yandex image state not found')


def response(resp):
    if search_type not in ('web', 'images'):
        return []
    if resp.headers.get('x-yandex-captcha') == 'captcha':
        raise SourceCaptcha('Yandex CAPTCHA')
    if search_type == 'web':
        return [{'url': extract_text(item.xpath('.//a[@class="b-serp-item__title-link"]/@href')),
                 'title': extract_text(item.xpath('.//h3[@class="b-serp-item__title"]'
                                                  '/a[@class="b-serp-item__title-link"]/span')),
                 'content': extract_text(item.xpath('.//div[@class="b-serp-item__content"]'
                                                    '//div[@class="b-serp-item__text"]'))}
                for item in html.fromstring(resp.text).xpath('//li[contains(@class, "serp-item")]')]
    found = []
    for item in image_state(resp)['initialState']['serpList']['items']['entities'].values():
        viewer = item['viewerData']
        best = viewer['thumb']
        for rendition in viewer['dups'] + viewer['preview']:
            if rendition['h'] > best['h']:
                best = rendition
        found.append({'title': item['snippet']['title'], 'url': item['snippet']['url'], 'img_src': best['url'],
                      'filesize': size_text(best['fileSizeInBytes']) if best.get('fileSizeInBytes') else None,
                      'thumbnail_src': item['image'], 'template': 'images.html',
                      'resolution': f"{best['w']} x {best['h']}"})
    return found
