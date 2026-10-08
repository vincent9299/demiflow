"""ARS gallery's official Search.gov form, then bounded caption-page originals."""
from urllib.parse import urlencode, urljoin, urlsplit
from lxml import html
from searx.network import get
from searx.exceptions import SearxEngineAccessDeniedException
from demiflow.services.engines.image_source_utils import bounded, image_row

about = {'website': 'https://www.ars.usda.gov/oc/images/image-gallery/',
         'use_official_api': False, 'require_api_key': False, 'results': 'HTML'}
categories = ['images']
paging = True
max_details = 4


def request(query, params):
    bounded(max_details, 'max_details', 10)
    params['url'] = 'https://search.usa.gov/search/docs?' + urlencode({
        'affiliate': 'agriculturalresearchservicears', 'dc': '1218',
        'query': query, 'page': params['pageno']})
    return params


def caption_image(text, page):
    doc = html.fromstring(text)
    links = doc.xpath('//a[@href][.//img[contains(translate(@alt,"HIGHRESOLUTION","highresolution"),"high-resolution")]]/@href')
    if not links:
        links = doc.xpath('//a[contains(@href,"/300dpi/")]/@href')
    if not links:
        return None
    title = ' '.join(doc.xpath('//title/text()')).strip()
    return image_row(page, urljoin(page, links[0]), title)


def response(resp):
    if resp.status_code != 200 or not resp.text.strip():
        raise SearxEngineAccessDeniedException()
    doc = html.fromstring(resp.text)
    pages = []
    for href in doc.xpath('//a[@href]/@href'):
        page = urljoin(str(resp.url), href)
        parsed = urlsplit(page)
        if parsed.hostname not in {'www.ars.usda.gov', 'ars.usda.gov'} or not parsed.path.startswith('/oc/images/photos/'):
            continue
        if page not in pages:
            pages.append(page)
        if len(pages) >= bounded(max_details, 'max_details', 10):
            break
    if not pages and not any(t in doc.text_content().lower() for t in ('no results', 'no documents', 'did not match')):
        raise ValueError('ARS search result structure not recognized')
    rows = []
    for page in pages:
        row = caption_image(get(page).text, page)
        if row:
            rows.append(row)
    return rows
