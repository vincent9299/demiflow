"""Openverse image metadata, preserving the selected URL's declared dimensions."""
from urllib.parse import urlencode
from demiflow.services.engines.image_source_utils import result_list

about = {'website': 'https://openverse.org/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON',
         'official_api_documentation': 'https://api.openverse.org/v1/'}
categories = ['images']
paging = True
page_size = 20


def request(query, params):
    params['url'] = 'https://api.openverse.org/v1/images/?' + urlencode(
        {'q': query, 'page': params['pageno'], 'page_size': page_size, 'format': 'json'})
    return params


def response(resp):
    results = []
    for photo in result_list(resp.json(), 'results')[:page_size]:
        if not photo.get('url') or not photo.get('foreign_landing_url'):continue
        row = {'template': 'images.html', 'url': photo['foreign_landing_url'],
               'img_src': photo['url'], 'thumbnail_src': photo.get('thumbnail'),
               'title': photo.get('title', ''), 'license': photo.get('license'),
               'author': photo.get('creator'), 'source': photo.get('source')}
        if photo.get('width') and photo.get('height'):
            row['resolution'] = f"{photo['width']} x {photo['height']}"
        results.append(row)
    return results
