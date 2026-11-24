"""Sogou image search; results come from the page's initial state JSON (48 per page)."""
import json
import re
from urllib.parse import urlencode

about = {'website': 'https://pic.sogou.com/', 'wikidata_id': 'Q7554565', 'use_official_api': False,
         'require_api_key': False, 'results': 'HTML'}
categories = ['images']
paging = True
base_url = 'https://pic.sogou.com'
INITIAL_STATE = re.compile(r'window\.__INITIAL_STATE__\s*=\s*({.*?});', re.S)


def request(query, params):
    params['url'] = f"{base_url}/pics?{urlencode({'query': query, 'start': (params['pageno'] - 1) * 48})}"


def response(resp):
    match = INITIAL_STATE.search(resp.text)
    if not match:
        return []
    state = json.loads(match.group(1))
    listing = state.get('searchList') or {}
    return [{'template': 'images.html', 'url': item.get('url', ''), 'thumbnail_src': item.get('picUrl', ''),
             'img_src': item.get('picUrl', ''), 'content': item.get('content_major', ''),
             'title': item.get('title', ''), 'source': item.get('ch_site_name', '')}
            for item in (listing.get('searchList') or [] if 'searchList' in listing else [])]
