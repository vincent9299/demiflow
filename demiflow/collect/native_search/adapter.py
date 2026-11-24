"""What a search source adapter may import.

An adapter is a Python module with ``request(query, params)``, which fills
``params['url']`` (and optionally method, headers, data, cookies), and
``response(resp)``, which returns a list of result records. Module attributes
such as ``categories``, ``paging``, ``page_size`` and adapter options can be
overridden per source in ``SearchConfig.engines``.

Inside ``request``/``response`` use the helpers below for any additional HTTP
request: they share the source's admission, budgets and receipts.

An offline adapter's ``search`` may return a list or yield its results one at a
time. The worker counts them as they come and stops at ``max_results`` or at
``output_limit()`` encoded bytes (``ResponseTooLarge``); an adapter that reads
from a store should hand over no more than that, and raise ``ResponseTooLarge``
before it reads a result that cannot fit. However the search ends, the worker
closes what ``search`` returned: its ``close()`` when it has one, and that of the
iterator made from it.
"""
import re
from html.parser import HTMLParser

from .errors import SourceAccessDenied, SourceCaptcha, SourceError, SourceRateLimited
from .worker_network import Request, ResponseTooLarge, get, head, multi_requests, output_limit, post, request

__all__ = ['Request', 'ResponseTooLarge', 'SourceAccessDenied', 'SourceCaptcha', 'SourceError', 'SourceRateLimited',
           'browser_user_agent', 'ecma_unescape', 'extract_text', 'get', 'head', 'html_to_text',
           'image_result', 'key_value', 'legacy_record', 'main_result', 'multi_requests', 'output_limit', 'post',
           'request', 'size_text', 'user_agent']

_UNICODE_ESCAPE = re.compile(r'%u([0-9a-fA-F]{4})')
_BYTE_ESCAPE = re.compile(r'%([0-9a-fA-F]{2})')
_SKIPPED = ('script', 'style')


def user_agent():
    """User-Agent naming DemiFlow, for sources that ask clients to identify themselves."""
    try:
        from importlib.metadata import version
        return 'DemiFlow/' + version('demiflow')
    except Exception:
        return 'DemiFlow'


BROWSER_PLATFORMS = ('Windows NT 10.0; Win64; x64', 'X11; Linux x86_64')
BROWSER_VERSIONS = ('154.0', '153.0')


def browser_user_agent():
    """User-Agent of a current desktop Firefox; platform and version are picked at random."""
    import random
    version = random.choice(BROWSER_VERSIONS)
    return f'Mozilla/5.0 ({random.choice(BROWSER_PLATFORMS)}; rv:{version}) Gecko/20100101 Firefox/{version}'



def ecma_unescape(text):
    """JavaScript ``unescape``: ``%uXXXX`` first, then ``%XX``."""
    text = _UNICODE_ESCAPE.sub(lambda m: chr(int(m.group(1), 16)), text)
    return _BYTE_ESCAPE.sub(lambda m: chr(int(m.group(1), 16)), text)


class _Text(HTMLParser):
    """Collect visible text; ``<br>`` is a space and stray end tags stay literal."""

    def __init__(self):
        super().__init__()
        self.parts, self.open = [], []

    def handle_starttag(self, tag, attrs):
        self.open.append(tag)
        if tag == 'br':
            self.parts.append(' ')

    def handle_endtag(self, tag):
        if self.open:
            if self.open[-1] == tag:
                self.open.pop()
            else:
                self.parts.append(f'</{tag}>')

    def handle_data(self, data):
        if not self.open or self.open[-1] not in _SKIPPED:
            self.parts.append(data)


def html_to_text(html):
    """Plain text of an HTML fragment with whitespace collapsed."""
    if not html:
        return ''
    parser = _Text()
    parser.feed(' '.join(html.replace('\n', ' ').replace('\r', ' ').split()))
    parser.close()
    return ''.join(parser.parts).strip()


def extract_text(nodes, allow_none=False):
    """Text of lxml nodes: elements without tail, whitespace collapsed; a node list is concatenated."""
    if isinstance(nodes, list):
        return ''.join(extract_text(node) or '' for node in nodes).strip()
    if nodes is None:
        if allow_none:
            return None
        raise ValueError('No text node to extract')
    if isinstance(nodes, (str, int, float, bool)):
        return str(nodes)
    from lxml import html as lxml_html
    text = lxml_html.tostring(nodes, encoding='unicode', method='text', with_tail=False)
    return ' '.join(text.strip().replace('\n', ' ').split())


MAIN_RESULT_FIELDS = {'template': 'default.html', 'title': '', 'content': '', 'img_src': '', 'iframe_src': '',
                      'audio_src': '', 'thumbnail': '', 'publishedDate': None, 'pubdate': '', 'length': None,
                      'views': '', 'author': '', 'metadata': '', 'priority': '', 'open_group': False,
                      'close_group': False, 'positions': [], 'score': 0, 'category': ''}


IMAGE_RESULT_FIELDS = {**MAIN_RESULT_FIELDS, 'template': 'images.html', 'thumbnail_src': '', 'resolution': '',
                       'img_format': '', 'source': '', 'filesize': '', 'formats': []}


KEY_VALUE_FIELDS = {**MAIN_RESULT_FIELDS, 'template': 'keyvalue.html', 'caption': '', 'key_title': '',
                    'value_title': ''}


def _typed(kind, defaults, url, fields, **required):
    unknown = set(fields) - set(defaults)
    if unknown:
        raise TypeError(f'Unknown {kind} fields: ' + ', '.join(sorted(unknown)))
    from .results import TYPED
    return {'url': url, **{k: (list(v) if isinstance(v, list) else v) for k, v in defaults.items()},
            **required, **fields, TYPED: kind}


def main_result(url, **fields):
    """A typed main result: every standard field present; duplicates match ignoring the URL scheme."""
    return _typed('MainResult', MAIN_RESULT_FIELDS, url, fields)


def key_value(kvmap, url=None, **fields):
    """A typed key/value table (``keyvalue.html``) of ``kvmap``; such results are never merged with others."""
    return _typed('KeyValue', KEY_VALUE_FIELDS, url, fields, kvmap=kvmap)


IMAGE_FORMATS = {'png': 'PNG', 'svg+xml': 'SVG', 'jpeg': 'JPG', 'bmp': 'BMP', 'x-icon': 'ICO', 'tiff': 'TIF'}


def image_result(url, img_src, **fields):
    """A typed image result (``images.html``); ``img_format`` defaults to the type guessed from ``img_src``."""
    if not fields.get('img_format'):
        import mimetypes
        guessed = mimetypes.guess_type(img_src)[0]
        if guessed:
            subtype = guessed.split('/')[-1]
            fields['img_format'] = IMAGE_FORMATS.get(subtype, subtype.upper())
    return _typed('Image', IMAGE_RESULT_FIELDS, url, {'img_src': img_src, **fields})


def legacy_record(**fields):
    """A plain result record with the standard fields filled the way merged records are."""
    record = dict(fields)
    for name, default in (('url', None), ('template', 'default.html'), ('engine', ''), ('parsed_url', None),
                          ('title', ''), ('content', ''), ('img_src', ''), ('thumbnail', ''), ('priority', ''),
                          ('engines', set()), ('positions', ''), ('score', 0), ('category', ''),
                          ('publishedDate', None), ('iframe_src', None)):
        record.setdefault(name, default)
    if 'infobox' in record:
        record.setdefault('urls', [])
        record.setdefault('attributes', [])
    if 'answer' in record:
        record['template'] = 'answer/legacy.html'
    return record


def size_text(size, precision=2):
    """File size on a 1024 base, e.g. ``1.50 MB`` (bytes keep a trailing space: ``512.00 B ``)."""
    units = ['B ', 'KB', 'MB', 'GB', 'TB']
    unit = 0
    while size > 1024 and unit < len(units):
        unit += 1
        size = size / 1024.0
    return '%.*f %s' % (precision, size, units[unit])
