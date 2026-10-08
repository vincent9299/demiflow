"""Public-domain AIC images using the documented larger IIIF rendition."""
from urllib.parse import urlencode, quote
from searx.network import get
from demiflow.services.engines.image_source_utils import bounded, image_row, http_url, positive, result_list

about = {'website': 'https://www.artic.edu/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 4


def request(query, params):
    n = bounded(page_size, 'page_size', 10)
    params['url'] = 'https://api.artic.edu/api/v1/artworks/search?' + urlencode({
        'q': query, 'query[term][is_public_domain]': 'true', 'page': params['pageno'],
        'limit': n, 'fields': 'id,title,image_id,is_public_domain'})
    return params


def response(resp):
    body = resp.json()
    items = result_list(body, 'data')
    base = http_url((body.get('config') or {}).get('iiif_url'))
    if not base:
        raise ValueError('AIC response is missing its IIIF service')
    rows = []
    for item in items[:bounded(page_size, 'page_size', 10)]:
        if not item.get('image_id') or not item.get('is_public_domain'):
            continue
        service = base.rstrip('/') + '/' + quote(item['image_id'], safe='')
        info = get('https://api.artic.edu/api/v1/images/' + quote(item['image_id'], safe='')
                   + '?fields=id,width,height').json().get('data') or {}
        w, h = positive(info.get('width')), positive(info.get('height'))
        if not w or not h:
            raise ValueError('AIC image metadata is missing native dimensions')
        # AIC documents width 1686 for public-domain works. Never upscale a
        # smaller original. Leave resized height unknown until the file header.
        width = min(1686, w)
        row = image_row('https://www.artic.edu/artworks/' + str(item['id']),
            service + f'/full/{width},/0/default.jpg', item.get('title'),
            width=w if w and width == w else None, height=h if w and width == w else None,
            license='public domain')
        if row:
            rows.append(row)
    return rows
