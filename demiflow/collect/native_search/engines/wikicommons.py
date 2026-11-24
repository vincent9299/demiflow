"""Wikimedia Commons image search (MediaWiki query API, 10 files per page)."""
from urllib.parse import urlencode

from demiflow.collect.native_search.adapter import html_to_text, legacy_record, size_text

about = {'website': 'https://commons.wikimedia.org/', 'wikidata_id': 'Q565',
         'official_api_documentation': 'https://commons.wikimedia.org/w/api.php', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON'}
categories = []
paging = True
page_size = 10
wc_api_url = 'https://commons.wikimedia.org/w/api.php'
wc_search_type = 'image'
FILE_TYPES = {'image': 'bitmap|drawing'}


def setup(settings):
    return settings.get('wc_search_type') in FILE_TYPES


def request(query, params):
    language = 'en' if params['locale'] == 'all' else params['locale'].split('-')[0]
    args = {'format': 'json', 'uselang': language, 'action': 'query', 'prop': 'info|imageinfo',
            'generator': 'search', 'gsrnamespace': '6', 'gsrprop': 'snippet', 'gsrlimit': page_size,
            'gsroffset': page_size * (params['pageno'] - 1),
            'gsrsearch': f'filetype:{FILE_TYPES[wc_search_type]} {query}',
            'iiprop': 'url|size|mime|extmetadata', 'iiurlheight': '180'}
    params['url'] = f"{wc_api_url}?{urlencode(args, safe=':|')}"


def response(resp):
    found = []
    for page in resp.json().get('query', {}).get('pages', {}).values():
        if not page.get('imageinfo', []):
            continue
        info = page['imageinfo'][0]
        meta = info.get('extmetadata') or {}
        artist = (meta.get('Artist') or {}).get('value') or ''
        found.append(legacy_record(**{'template': 'images.html', 'title': page['title'].replace('File:', '').rsplit('.', 1)[0],
                      'url': info['descriptionurl'], 'content': html_to_text(page['snippet']), 'img_src': info['url'],
                      'thumbnail_src': info['thumburl'], 'resolution': f"{info['width']} x {info['height']}",
                      'img_format': info['mime'], 'filesize': size_text(info['size']) if info.get('size') else info.get('size'),
                      'license': (meta.get('LicenseShortName') or {}).get('value'),
                      'author': html_to_text(artist) if artist else None}))
    return found
