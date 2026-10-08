"""Wellcome image catalogue and bounded IIIF v2 original JPEG metadata."""
from urllib.parse import urlencode
from searx.network import get
from demiflow.services.engines.image_source_utils import bounded, image_row, http_url, result_list

about = {'website': 'https://wellcomecollection.org/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 4


def request(query, params):
    params['url'] = 'https://api.wellcomecollection.org/catalogue/v2/images?' + urlencode(
        {'query': query, 'page': params['pageno'], 'pageSize': bounded(page_size, 'page_size', 10)})
    return params


def response(resp):
    rows = []
    for item in result_list(resp.json(), 'results')[:bounded(page_size, 'page_size', 10)]:
        location = next((x for x in (item.get('locations') or [])[:10]
            if x.get('locationType', {}).get('id') == 'iiif-image' and http_url(x.get('url'))), None)
        if not location:
            continue
        if any(x.get('status', {}).get('id') in {'closed', 'restricted', 'permission-required'}
               for x in (location.get('accessConditions') or [])):
            continue
        info = get(location['url']).json()
        service = http_url(info.get('@id'))
        if not service or info.get('protocol') != 'http://iiif.io/api/image':
            continue
        source = item.get('source') or {}
        row = image_row('https://wellcomecollection.org/works/' + str(source.get('id', '')),
            service.rstrip('/') + '/full/full/0/default.jpg', source.get('title'),
            width=info.get('width'), height=info.get('height'),
            license=(location.get('license') or {}).get('url'), author=location.get('credit'))
        if row:
            rows.append(row)
    return rows
