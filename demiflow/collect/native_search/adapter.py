"""What a search source adapter may import.

An adapter is a Python module with ``request(query, params)``, which fills
``params['url']`` (and optionally method, headers, data, cookies), and
``response(resp)``, which returns a list of result records. Module attributes
such as ``categories``, ``paging``, ``page_size`` and adapter options can be
overridden per source in ``SearchConfig.engines``.

Inside ``request``/``response`` use the helpers below for any additional HTTP
request: they share the source's admission, budgets and receipts.
"""
import re
from html.parser import HTMLParser

from .errors import SourceAccessDenied, SourceCaptcha, SourceError, SourceRateLimited
from .worker_network import Request, get, head, multi_requests, post, request

__all__ = ['Request', 'SourceAccessDenied', 'SourceCaptcha', 'SourceError', 'SourceRateLimited',
           'ecma_unescape', 'extract_text', 'get', 'head', 'html_to_text', 'main_result', 'multi_requests',
           'post', 'request', 'user_agent']

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


def main_result(url, **fields):
    """A main result record with every standard field present (stored responses list them all)."""
    unknown = set(fields) - set(MAIN_RESULT_FIELDS)
    if unknown:
        raise TypeError('Unknown main result fields: ' + ', '.join(sorted(unknown)))
    return {'url': url, **{k: (list(v) if isinstance(v, list) else v) for k, v in MAIN_RESULT_FIELDS.items()},
            **fields}
