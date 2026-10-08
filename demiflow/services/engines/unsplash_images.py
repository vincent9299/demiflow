"""Unsplash public search; use its explicit full-size URL and matching metadata."""
from urllib.parse import urlencode
from searx.utils import searxng_useragent
from demiflow.services.engines.image_source_utils import result_list

about = {'website': 'https://unsplash.com/', 'use_official_api': False,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 20


def request(query, params):
    params['url'] = 'https://unsplash.com/napi/search/photos?' + urlencode(
        {'query': query, 'page': params['pageno'], 'per_page': page_size})
    params['headers']['User-Agent'] = searxng_useragent()
    return params


def response(resp):
    results = []
    for photo in result_list(resp.json(), 'results')[:page_size]:
        image = (photo.get('urls') or {}).get('full')
        page = (photo.get('links') or {}).get('html')
        if not image or not page:continue
        row = {'template': 'images.html', 'url': page, 'img_src': image,
               'thumbnail_src': photo['urls'].get('thumb'),
               'title': photo.get('alt_description') or '', 'content': photo.get('description') or '',
               'author': (photo.get('user') or {}).get('name', '')}
        if photo.get('width') and photo.get('height'):
            row['resolution'] = f"{photo['width']} x {photo['height']}"
        results.append(row)
    return results
