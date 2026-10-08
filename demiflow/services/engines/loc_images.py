"""Library of Congress photographs: bounded item details and explicit JPEGs."""
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
from searx.network import get
from demiflow.services.engines.image_source_utils import bounded, image_row, http_url, raster_variant, result_list

about = {'website': 'https://www.loc.gov/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 4
min_short_side = 0
max_pixels = 0
max_image_bytes = 0


def request(query, params):
    params['url'] = 'https://www.loc.gov/photos/?' + urlencode(
        {'q': query, 'fo': 'json', 'c': bounded(page_size, 'page_size', 10), 'sp': params['pageno']})
    return params


def response(resp):
    rows = []
    for item in result_list(resp.json(), 'results')[:bounded(page_size, 'page_size', 10)]:
        page = http_url(item.get('id'))
        if not page or not (urlsplit(page).hostname or '').endswith('.loc.gov'):
            continue
        parsed = urlsplit(page)
        detail = get(urlunsplit(parsed._replace(query=urlencode({**dict(parse_qsl(parsed.query)), 'fo': 'json'})))).json()
        for resource in (detail.get('resources') or [])[:2]:
            if resource.get('download_restricted'):
                continue
            for files in (resource.get('files') or [])[:2]:
                variants = [{**f, 'filesize': f.get('size')} for f in files[:20]
                            if f.get('mimetype') in {'image/jpeg', 'image/png'}]
                selected = raster_variant(variants, min_short_side=min_short_side,
                    max_pixels=max_pixels, max_bytes=max_image_bytes)
                if not selected:
                    continue
                row = image_row(page, selected['url'], item.get('title'),
                    width=selected.get('width'), height=selected.get('height'),
                    declared_file_bytes=selected.get('size'), mime_type=selected.get('mimetype'))
                if row:
                    rows.append(row)
    return rows
