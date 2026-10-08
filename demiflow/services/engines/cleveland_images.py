"""Cleveland Open Access search with explicit, size-matched image variants."""
from urllib.parse import urlencode
from demiflow.services.engines.image_source_utils import bounded, image_row, raster_variant, result_list

about = {'website': 'https://www.clevelandart.org/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 20
max_images_per_record = 3
min_short_side = 0
max_pixels = 0
max_image_bytes = 0


def request(query, params):
    n = bounded(page_size, 'page_size', 50)
    bounded(max_images_per_record, 'max_images_per_record', 5)
    params['url'] = 'https://openaccess-api.clevelandart.org/api/artworks/?' + urlencode({
        'q': query, 'has_image': 1, 'cc0': 1, 'limit': n, 'skip': (params['pageno'] - 1) * n,
        'fields': 'id,title,url,images,alternate_images,share_license_status,technique'})
    return params


def response(resp):
    rows = []
    for item in result_list(resp.json(), 'data')[:bounded(page_size, 'page_size', 50)]:
        groups = [item.get('images') or {}, *(item.get('alternate_images') or [])[:bounded(max_images_per_record, 'max_images_per_record', 5) - 1]]
        for group in groups:
            variant = raster_variant([group.get(k) for k in ('print', 'web', 'full')],
                min_short_side=min_short_side, max_pixels=max_pixels, max_bytes=max_image_bytes)
            if variant is None:
                continue
            row = image_row(item.get('url'), variant['url'], item.get('title'),
                width=variant.get('width'), height=variant.get('height'),
                declared_file_bytes=variant.get('filesize'), mime_type=variant.get('mimetype'), license=item.get('share_license_status'),
                content=item.get('technique', ''))
            if row:
                rows.append(row)
    return rows
