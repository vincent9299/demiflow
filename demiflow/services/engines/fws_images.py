"""FWS public site's image-search endpoint and explicit media download links."""
import json
import re
from urllib.parse import urlencode, urljoin, urlsplit
from lxml import html
from searx.network import get
from demiflow.services.engines.image_source_utils import bounded, image_row, raster_variant

about = {'website': 'https://www.fws.gov/library', 'use_official_api': False,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 4
min_short_side = 0
max_pixels = 0
max_image_bytes = 0


def request(query, params):
    n = bounded(page_size, 'page_size', 10)
    # Parameters and type label used by the public site's current search UI.
    params['url'] = 'https://www.fws.gov/fws_search/global_search?' + urlencode({
        '$keywords': query, '$top': n, '$skip': (params['pageno'] - 1) * n,
        'type': json.dumps(['Image'])})
    return params


def media_image(text, page, title):
    doc = html.fromstring(text)
    variants = []
    original_size = None
    for a in doc.xpath('//a[@href]'):
        label = a.text_content().strip()
        if not re.match(r'^(Original|Large|Medium)\s*\(', label):
            continue
        dims = re.search(r'\((\d+)\s*[x×]\s*(\d+)\)', label)
        if dims:
            variants.append({'url': urljoin(page, a.get('href')),
                             'width': int(dims[1]), 'height': int(dims[2])})
            if label.startswith('Original'):
                original_size = int(dims[1]), int(dims[2])
    # Some FWS "Large" renditions are larger than the declared original.
    # Exclude those upscaled versions even when they would pass a size minimum.
    if original_size:
        variants = [v for v in variants if v['width'] <= original_size[0] and v['height'] <= original_size[1]]
    selected = raster_variant(variants, min_short_side=min_short_side,
        max_pixels=max_pixels, max_bytes=max_image_bytes)
    return image_row(page, selected['url'], title, width=selected['width'], height=selected['height']) if selected else None


def response(resp):
    body = resp.json()
    if body.get('_meta', {}).get('total') == 0:
        return []
    if not isinstance(body.get('list'), list):
        raise ValueError('FWS search response is missing its result list')
    rows = []
    seen = set()
    for snippet in body['list'][:bounded(page_size, 'page_size', 10)]:
        doc = html.fromstring(snippet)
        links = doc.xpath('//*[contains(concat(" ",normalize-space(@class)," ")," teaser-title ")]//a[@href]')
        if not links:
            continue
        a = links[0]
        page = urljoin('https://www.fws.gov/', a.get('href'))
        parsed = urlsplit(page)
        if parsed.hostname != 'www.fws.gov' or not parsed.path.startswith('/media/') or page in seen:
            continue
        seen.add(page)
        row = media_image(get(page).text, page, a.text_content().strip())
        if row:
            rows.append(row)
    return rows
