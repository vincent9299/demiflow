"""NASA image search with explicit canonical links or a bounded asset manifest."""
from urllib.parse import urlencode, quote, urlsplit
from searx.network import get
from demiflow.services.engines.image_source_utils import result_list

about = {'website': 'https://images.nasa.gov/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON',
         'official_api_documentation': 'https://images.nasa.gov/docs/images.nasa.gov_api_docs.pdf'}
categories = ['images']
paging = True
page_size = 20
max_manifests = 3


def request(query, params):
    params['url'] = 'https://images-api.nasa.gov/search?' + urlencode(
        {'q': query, 'media_type': 'image', 'page': params['pageno'], 'page_size': page_size})
    return params


def response(resp):
    results = []; manifests = 0
    for item in result_list(resp.json().get('collection'), 'items')[:page_size]:
        data = (item.get('data') or [{}])[0]
        nid = data.get('nasa_id')
        if data.get('media_type') != 'image' or not isinstance(nid, str):
            continue
        links = [x for x in item.get('links', []) if x.get('render') == 'image' and x.get('href')]
        canonical = next((x for x in links if x.get('rel') == 'canonical'), None)
        if canonical is None and manifests < max_manifests:
            manifests += 1
            assets = get('https://images-api.nasa.gov/asset/' + quote(nid, safe='')).json()
            original = next((x.get('href') for x in result_list(assets.get('collection'), 'items')
                if '~orig.' in x.get('href', '') and urlsplit(x['href']).path.lower().endswith(('.jpg', '.jpeg', '.png'))), None)
            if original:
                canonical = {'href': original}
        selected = canonical or max(links, key=lambda x: (x.get('width') or 0) * (x.get('height') or 0), default=None)
        if selected is None:
            continue
        result = {'template': 'images.html', 'url': 'https://images.nasa.gov/details/' + quote(nid, safe=''),
                  'img_src': selected['href'], 'title': data.get('title', ''),
                  'content': data.get('description', ''), 'author': data.get('photographer') or data.get('secondary_creator', '')}
        thumb = next((x['href'] for x in links if x.get('rel') == 'preview'), None)
        if thumb:result['thumbnail_src'] = thumb
        if selected.get('width') and selected.get('height'):
            result['resolution'] = f"{selected['width']} x {selected['height']}"
        results.append(result)
    return results
