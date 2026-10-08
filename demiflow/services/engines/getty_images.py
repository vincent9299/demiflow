"""Getty Museum public collection frontend and IIIF (not Getty Images stock)."""
from urllib.parse import urlencode, urlsplit
from searx.network import get
from demiflow.services.engines.image_source_utils import bounded, image_row, result_list, iiif3_variant

about = {'website': 'https://www.getty.edu/art/collection/', 'use_official_api': False,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 2
min_short_side = 0
max_pixels = 0
target_edge = 2400
open_content_only = False


def request(query, params):
    n = bounded(page_size, 'page_size', 4)
    if type(open_content_only) is not bool:
        raise ValueError('open_content_only must be boolean')
    params['url'] = 'https://www.getty.edu/art/collection/api/search?' + urlencode(
        {'q': query.replace('+', ' '), 'size': n, 'from': (params['pageno'] - 1) * n,
         **({'open_content': 'true'} if open_content_only else {})})
    return params


def response(resp):
    rows = []
    for item in result_list(resp.json(), 'data')[:bounded(page_size, 'page_size', 4)]:
        manifest_url = (item.get('manifest') or {}).get('url', '')
        if urlsplit(manifest_url).hostname != 'media.getty.edu':
            continue
        manifest = get(manifest_url).json()
        # The first painting image only; other views can be requested in a later
        # bounded expansion rather than an unbounded manifest walk.
        for canvas in result_list(manifest, 'items')[:1]:
            for page in (canvas.get('items') or [])[:1]:
                for annotation in (page.get('items') or [])[:1]:
                    body = annotation.get('body') or {}
                    if not isinstance(body, dict) or body.get('type') != 'Image':
                        continue
                    service = next((x for x in (body.get('service') or [])[:4]
                                    if x.get('type') == 'ImageService3'), None)
                    if not service or urlsplit(service.get('id', '')).hostname != 'media.getty.edu':
                        continue
                    info = get(service['id'].rstrip('/') + '/info.json').json()
                    variant = iiif3_variant(info, min_short_side=min_short_side, max_pixels=max_pixels, target_edge=target_edge)
                    slug = item.get('slug_with_path', '')
                    if variant and isinstance(slug, str) and slug.startswith('/object/'):
                        rows.append(image_row('https://www.getty.edu/art/collection' + slug,
                            variant['url'], item.get('primary_name'), width=variant['width'],
                            height=variant['height'], mime_type=variant['mimetype'], license=canvas.get('rights')))
    return [row for row in rows if row]
