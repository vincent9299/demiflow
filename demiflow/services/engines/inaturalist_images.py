"""iNaturalist research observations; image URLs come from photo metadata.

The current API supplies original dimensions but often only a square URL. A
bounded observation JSON read obtains original_url or large_url without guessing
CDN paths. Original dimensions are never attributed to a preview rendition.
"""
from urllib.parse import urlencode, urlsplit
import re
from searx.network import get
from demiflow.services.engines.image_source_utils import result_list

about = {'website': 'https://www.inaturalist.org/', 'use_official_api': True,
         'require_api_key': False, 'results': 'JSON',
         'official_api_documentation': 'https://api.inaturalist.org/v1/docs/'}
categories = ['images']
paging = True
max_observations = 4
min_short_side = 0


def request(query, params):
    params['url'] = 'https://api.inaturalist.org/v1/observations?' + urlencode({
        'taxon_name': query, 'photos': 'true', 'per_page': max_observations,
        'page': params['pageno'], 'quality_grade': 'research'})
    return params


def response(resp):
    results = []
    for obs in result_list(resp.json(), 'results')[:max_observations]:
        eligible = {}
        for photo in (obs.get('photos') or [])[:20]:
            dims = photo.get('original_dimensions') or {}
            if photo.get('hidden'):
                continue
            if dims.get('width') and dims.get('height') and min(dims['width'], dims['height']) < min_short_side:
                continue
            eligible[photo['id']] = photo
        if not eligible:continue
        oid = obs.get('id')
        if type(oid) is not int or oid <= 0:continue
        # The official Open Data contract declares photos/ID/original.EXT.
        # Limit construction to that exact public bucket, ID and extension;
        # never rewrite arbitrary CDN URLs or infer access to private photos.
        # https://github.com/inaturalist/inaturalist-open-data#readme
        for pid, meta in list(eligible.items()):
            parsed = urlsplit(meta.get('url') or '')
            match = re.fullmatch(r'/photos/(\d+)/square\.(jpg|jpeg|png)', parsed.path)
            if parsed.hostname != 'inaturalist-open-data.s3.amazonaws.com' or not match or str(pid) != match[1]:
                continue
            dims = meta.get('original_dimensions') or {}
            row = {'template': 'images.html', 'url': f'https://www.inaturalist.org/observations/{oid}',
                   'img_src': f'https://inaturalist-open-data.s3.amazonaws.com/photos/{pid}/original.{match[2]}',
                   'thumbnail_src': meta['url'], 'title': obs.get('species_guess') or (obs.get('taxon') or {}).get('name', ''),
                   'content': 'Research-grade observation; official iNaturalist Open Data original',
                   'license': meta.get('license_code'), 'author': meta.get('attribution', '')}
            if dims.get('width') and dims.get('height'):
                row['resolution'] = f"{dims['width']} x {dims['height']}"
            results.append(row);del eligible[pid]
        if not eligible:continue
        full = get(f'https://www.inaturalist.org/observations/{oid}.json').json()
        for op in (full.get('observation_photos') or [])[:20]:
            photo = op.get('photo') or {}; meta = eligible.get(photo.get('id'))
            image_url = photo.get('original_url') or photo.get('large_url')
            if meta is None or not image_url:continue
            dims = meta.get('original_dimensions') or {}
            result = {'template': 'images.html', 'url': f'https://www.inaturalist.org/observations/{oid}',
                'img_src': image_url, 'thumbnail_src': photo.get('medium_url'),
                'title': obs.get('species_guess') or (obs.get('taxon') or {}).get('name', ''),
                'content': 'Research-grade observation', 'license': meta.get('license_code'),
                'author': meta.get('attribution', '')}
            if photo.get('original_url') and dims.get('width') and dims.get('height'):
                result['resolution'] = f"{dims['width']} x {dims['height']}"
            results.append(result)
    return results
