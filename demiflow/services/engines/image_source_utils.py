"""Small helpers for bounded public image adapters; no network or business state."""
from urllib.parse import urlsplit


def bounded(value, name, maximum=100):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f'{name} must be between 1 and {maximum}')
    return value


def positive(value):
    if isinstance(value, str) and value.isascii() and value.isdigit() and len(value) <= 10:
        value = int(value)
    return value if type(value) is int and 0 < value < 2**31 else None


def result_list(payload, key):
    """A missing/invalid result collection is not a successful empty search."""
    if not isinstance(payload, dict) or not isinstance(payload.get(key), list):
        raise ValueError(f'Image source response is missing a valid {key} array')
    return payload[key]


def http_url(value):
    if not isinstance(value, str):
        return None
    if value.startswith('//'):
        value = 'https:' + value
    p = urlsplit(value)
    return value if p.scheme in {'http', 'https'} and p.hostname and not p.username else None


def image_row(page, image, title='', *, width=None, height=None, **metadata):
    page, image = http_url(page), http_url(image)
    if not page or not image:
        return None
    row = {'template': 'images.html', 'url': page, 'img_src': image,
           'title': title or '', **metadata}
    w, h = positive(width), positive(height)
    if w and h:
        row['resolution'] = f'{w} x {h}'
    return row


def raster_variant(variants, *, min_short_side=0, max_pixels=0, max_bytes=0):
    """Prefer the largest eligible explicit JPEG/PNG, keeping a fallback for audit.

    Caller bounds the variant list. Width/height belong to this exact rendition,
    never to the physical object, its thumbnail, or another image version.
    """
    values = []
    for v in variants:
        if not isinstance(v, dict) or not http_url(v.get('url')):
            continue
        mime = v.get('mimetype')
        if mime:
            # LoC may expose an extensionless download endpoint. An explicit
            # nonimage MIME takes precedence over a misleading URL suffix.
            if not isinstance(mime, str) or mime.split(';', 1)[0].strip().lower() not in {'image/jpeg', 'image/png'}:
                continue
        elif not urlsplit(v['url']).path.lower().endswith(('.jpg', '.jpeg', '.png')):
            continue
        w, h, size = positive(v.get('width')), positive(v.get('height')), positive(v.get('filesize'))
        area = w * h if w and h else 0
        eligible = (not area or (min(w, h) >= min_short_side and (not max_pixels or area <= max_pixels)))
        eligible = eligible and (not size or not max_bytes or size <= max_bytes)
        values.append((eligible, area, v))
    return max(values, key=lambda x: (x[0], x[1]), default=(False, 0, None))[2]


def iiif3_variant(info, *, min_short_side=0, max_pixels=0, target_edge=2400):
    """Select an advertised size or a supported confined request, never upscale.

    Computed requests deliberately omit exact dimensions: server rounding is
    checked on the downloaded file. Listed sizes retain their own dimensions.
    """
    from math import ceil
    bounded(target_edge, 'target_edge', 16384)
    for name, value, maximum in [('min_short_side', min_short_side, 16384), ('max_pixels', max_pixels, 100000000)]:
        if type(value) is not int or not 0 <= value <= maximum:
            raise ValueError('Invalid IIIF selection limit: ' + name)
    service = http_url(info.get('id'))
    w, h = positive(info.get('width')), positive(info.get('height'))
    if not service or not w or not h or info.get('type') != 'ImageService3':
        return None
    if info.get('protocol') != 'http://iiif.io/api/image':
        return None
    limit = min(max_pixels or w * h, positive(info.get('maxArea')) or w * h)
    max_w = min(w, positive(info.get('maxWidth')) or w)
    max_h = min(h, positive(info.get('maxHeight')) or h)
    def rendition(size, width=None, height=None):
        return {'url': service.rstrip('/') + '/full/' + size + '/0/default.jpg',
                'width': width, 'height': height, 'mimetype': 'image/jpeg'}
    sizes = info.get('sizes') or []
    if not isinstance(sizes, list):
        raise ValueError('IIIF sizes must be an array')
    eligible = []
    for size in sizes[:64]:
        sw, sh = positive(size.get('width')), positive(size.get('height'))
        if sw and sh and sw <= max_w and sh <= max_h and min(sw, sh) >= min_short_side and sw * sh <= limit:
            eligible.append((sw, sh))
    if eligible:
        preferred = [s for s in eligible if max(s) <= target_edge]
        sw, sh = (max(preferred, key=lambda s: s[0] * s[1]) if preferred else
                  min(eligible, key=lambda s: s[0] * s[1]))
        return rendition(f'{sw},{sh}', sw, sh)
    # An undersized original is retained with exact dimensions for the caller's
    # auditable pre-download rejection. It must not become a larger rendition.
    if min(w, h) < min_short_side:
        return rendition('max', w, h)
    if 'sizeByConfinedWh' in (info.get('extraFeatures') or []):
        minimum_edge = max(1, ceil(min_short_side * max(w, h) / min(w, h)))
        edge = min(max(w, h), max(target_edge, minimum_edge))
        for candidate in dict.fromkeys((edge, minimum_edge)):
            cw, ch = ceil(w * candidate / max(w, h)), ceil(h * candidate / max(w, h))
            if cw <= max_w and ch <= max_h and cw * ch <= limit:
                return rendition(f'!{candidate},{candidate}')
    # Exact original dimensions also expose infeasible pixel budgets upstream.
    return rendition('max', w, h)
