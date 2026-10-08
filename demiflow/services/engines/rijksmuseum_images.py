"""Rijksmuseum Linked Art search; at most four metadata requests per result."""
from urllib.parse import urlencode, urlsplit
from searx.network import get
from demiflow.services.engines.image_source_utils import bounded, image_row, result_list, iiif3_variant

about = {'website': 'https://data.rijksmuseum.nl/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
# The service uses opaque cursors, not page numbers. Do not repeat page one.
paging = False
page_size = 2
min_short_side = 0
max_pixels = 0
target_edge = 2400


def request(query, params):
    bounded(page_size, 'page_size', 4)
    params['url'] = 'https://data.rijksmuseum.nl/search/collection?' + urlencode(
        {'title': query.replace('+', ' '), 'imageAvailable': 'true'})
    return params


def linked(identifier):
    p = urlsplit(identifier or '')
    if p.scheme != 'https' or p.hostname != 'id.rijksmuseum.nl' or not p.path[1:].isdigit():
        raise ValueError('Invalid Rijksmuseum Linked Art identifier')
    return get('https://data.rijksmuseum.nl' + p.path + '?_profile=la-framed').json()


def response(resp):
    rows = []
    for item in result_list(resp.json(), 'orderedItems')[:bounded(page_size, 'page_size', 4)]:
        obj = linked(item.get('id'))
        visual = next((x for x in (obj.get('shows') or [])[:8] if x.get('type') == 'VisualItem'), None)
        if not visual:
            continue
        visual = linked(visual.get('id'))
        digital = next((x for x in (visual.get('digitally_shown_by') or [])[:8]
                        if x.get('type') == 'DigitalObject'), None)
        if not digital:
            continue
        digital = linked(digital.get('id'))
        original = next((x.get('id') for x in (digital.get('access_point') or [])[:8]
                         if isinstance(x.get('id'), str) and x['id'].endswith('/full/max/0/default.jpg')), None)
        if not original or urlsplit(original).hostname != 'iiif.micr.io':
            continue
        info = get(original.rsplit('/full/', 1)[0] + '/info.json').json()
        variant = iiif3_variant(info, min_short_side=min_short_side, max_pixels=max_pixels, target_edge=target_edge)
        if not variant:
            continue
        title = next((x.get('content') for x in (obj.get('identified_by') or [])[:32]
                      if x.get('type') == 'Name'), '')
        rows.append(image_row(item['id'], variant['url'], title, width=variant['width'],
                              height=variant['height'], mime_type=variant['mimetype']))
    return [row for row in rows if row]
