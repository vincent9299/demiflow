"""GBIF occurrence media: explicit publisher originals, not the image cache."""
from urllib.parse import urlencode
from demiflow.services.engines.image_source_utils import bounded, image_row, result_list

about = {'website': 'https://www.gbif.org/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON'}
categories = ['images']
paging = True
page_size = 20
max_images_per_record = 3


def request(query, params):
    n = bounded(page_size, 'page_size', 50)
    bounded(max_images_per_record, 'max_images_per_record', 5)
    params['url'] = 'https://api.gbif.org/v1/occurrence/search?' + urlencode(
        {'q': query, 'mediaType': 'StillImage', 'limit': n, 'offset': (params['pageno'] - 1) * n})
    return params


def response(resp):
    rows = []
    for item in result_list(resp.json(), 'results')[:bounded(page_size, 'page_size', 50)]:
        for media in (item.get('media') or [])[:bounded(max_images_per_record, 'max_images_per_record', 5)]:
            if media.get('type') != 'StillImage':
                continue
            row = image_row('https://www.gbif.org/occurrence/' + str(item['key']),
                media.get('identifier'), item.get('scientificName'), author=media.get('creator'),
                license=media.get('license'), content=item.get('datasetName', ''),
                source=media.get('publisher'))
            if row:
                rows.append(row)
    return rows
