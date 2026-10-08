"""Verified document spans for embeddings, with an action-owned bounded cache.

Spans use Unicode character offsets into normalized document blocks. No HTTP,
tokenization, truncation or business segmentation is performed by this reader.
"""
from collections import OrderedDict
import hashlib
import sys
import threading

from demiflow.collect.documents import read_document


class DocumentInputReader:
    """Cache only verified block text, bounded by measured Python payload bytes.

    A miss additionally holds one bounded encoded/decoded document per prepare
    worker. JSON decoder allocations are not an RSS hard bound. Cache admission
    happens after decoding, but before retaining it beyond that worker's call.
    """
    def __init__(self, options):
        self.options = options
        self.cache = OrderedDict()
        self.bytes = 0
        self.lock = threading.Lock()

    def clear(self):
        with self.lock:
            self.cache.clear()
            self.bytes = 0

    def _blocks(self, ref):
        if not isinstance(ref, dict) or not ref.get('uri') or not ref.get('sha256'):
            raise ValueError('document_ref requires uri and sha256')
        key = (ref['uri'], ref['sha256'])
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key][0]
        document = read_document(ref, max_bytes=self.options['max_document_bytes'])
        if len(document['blocks']) > self.options['max_document_blocks']:
            raise ValueError('Document exceeds max_document_blocks')
        blocks = {block['block_id']: block['text'] for block in document['blocks']}
        size = sys.getsizeof(blocks) + sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in blocks.items())
        size += sys.getsizeof(key) + sum(sys.getsizeof(v) for v in key) + 256
        limit = self.options['document_cache_bytes']
        with self.lock:
            if key in self.cache:
                return self.cache[key][0]
            if size <= limit:
                while self.cache and (self.bytes + size > limit or
                                      len(self.cache) >= self.options['document_cache_entries']):
                    _, (_, old_size) = self.cache.popitem(last=False)
                    self.bytes -= old_size
                self.cache[key] = (blocks, size)
                self.bytes += size
        return blocks

    def __call__(self, value):
        if not isinstance(value, dict) or set(value) - {'document_ref', 'spans', 'prefix', 'text_sha256', 'model_prefix'}:
            raise ValueError('Document input requires document_ref, spans, prefix, text_sha256')
        spans, prefix = value.get('spans'), value.get('prefix', '')
        if not isinstance(spans, list) or len(spans) > self.options['max_document_spans']:
            raise ValueError('Invalid spans or max_document_spans exceeded')
        if not isinstance(prefix, str) or len(prefix) > self.options['max_document_text_bytes']:
            raise ValueError('Invalid prefix or max_document_text_bytes exceeded')
        size = len(prefix.encode('utf-8'))
        limit = self.options['max_document_text_bytes']
        if size > limit:
            raise ValueError('Document prefix exceeds max_document_text_bytes')
        blocks = self._blocks(value.get('document_ref'))
        pieces = [prefix]
        for i, span in enumerate(spans):
            if not isinstance(span, dict) or set(span) != {'block_id', 'start', 'end'}:
                raise ValueError('Span requires block_id, start, end')
            text = blocks.get(span['block_id'])
            start, end = span['start'], span['end']
            if (text is None or type(start) is not int or type(end) is not int or
                    not 0 <= start < end <= len(text)):
                raise ValueError('Span lies outside its verified document block')
            # At most four UTF-8 bytes per character; bound the slice itself
            # before allocating it, then admit its actual encoding before join.
            if end - start > limit:
                raise ValueError('Document span exceeds max_document_text_bytes')
            part = text[start:end]
            size += len(part.encode('utf-8')) + (2 if i else 0)
            if size > limit:
                raise ValueError('Document input exceeds max_document_text_bytes')
            if i:
                pieces.append('\n\n')
            pieces.append(part)
        result = ''.join(pieces)
        if not result.strip():
            raise ValueError('Document input is empty')
        digest = hashlib.sha256(result.encode('utf-8')).hexdigest()
        if digest != value.get('text_sha256'):
            raise ValueError('Document embedding text differs from declared SHA256')
        model_prefix = value.get('model_prefix', '')
        if not isinstance(model_prefix, str) or len(model_prefix) > limit:
            raise ValueError('Invalid or oversized model_prefix')
        if len(model_prefix.encode()) + size > limit:
            raise ValueError('Model prefix and document exceed max_document_text_bytes')
        result = model_prefix + result
        digest = hashlib.sha256(result.encode()).hexdigest()
        return result, {'document_sha256': value['document_ref']['sha256'],
                        'input_text_sha256': digest, 'span_count': len(spans)}
