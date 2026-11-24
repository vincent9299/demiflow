"""Independent body parsing and normalized document publication.

Raw acquisition is Dataset.fetch. Parsing never queries a document library.
"""
import asyncio
import math

from .documents import DocumentError, PARSER_VERSION, parse_document, read_document
from .pdf_text import pdf_policy, pdf_identity
from ..objects import LocalObjectStore, ObjectRef
from demiflow.execution.hosts.isolation import run_isolated


def publish_document(store, document, raw_ref, *, max_document_bytes):
    """Persist an already parsed, generic document without copying its raw body."""
    from ..kv import encode
    document = {**document, 'raw_ref': raw_ref}
    try:
        payload = encode(document, max_document_bytes)
    except ValueError as exc:
        raise DocumentError('invalid_normalized_document: ' + str(exc)) from exc
    ref = store.put(payload).to_dict()
    read_document(ref, max_bytes=max_document_bytes)
    return ref


def _parse(request, store, max_raw_bytes, max_document_bytes, pdf_parser):
    try:
        raw = ObjectRef(**request['raw_ref']).read(max_bytes=max_raw_bytes)
    except ValueError as exc:
        raise DocumentError('invalid_raw_snapshot: ' + str(exc)) from exc
    source = {key: request[key] for key in ('url', 'final_url', 'content_type', 'retrieved_at')}
    doc = parse_document(raw, **source, pdf_parser=pdf_parser)
    ref = publish_document(store, doc, request['raw_ref'], max_document_bytes=max_document_bytes)
    return {**request, 'document_ref': ref, 'status': 'ok', 'reason': '',
            'parser_version': doc['parser']['version']}


class ParseDocument:
    label = 'parse_document'
    concurrency = 1
    queue_depth = 1
    catch = ()

    def __init__(self, *, store, max_raw_bytes=2*1024**2,
                 max_document_bytes=8*1024**2, timeout_s=30., pdf_parser=None, request_gate=None):
        if not isinstance(store, LocalObjectStore):
            raise TypeError('ParseDocument requires an explicit LocalObjectStore')
        for value in (max_raw_bytes, max_document_bytes):
            if type(value) is not int or not 1 <= value <= 64*1024**2:
                raise ValueError('Invalid document parse byte limit')
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 3600:
            raise ValueError('Invalid document parse timeout')
        self.store, self.max_raw_bytes = store, max_raw_bytes
        self.max_document_bytes, self.timeout_s = max_document_bytes, timeout_s
        self.pdf_parser = pdf_policy(pdf_parser)
        self.request_gate = request_gate
        if request_gate is not None and not callable(getattr(request_gate,'enter',None)):
            raise TypeError('ParseDocument request_gate requires enter() admission')
        self.cache_identity = {'store': str(store.directory), 'version': PARSER_VERSION,
            'max_raw_bytes': max_raw_bytes, 'max_document_bytes': max_document_bytes,
            'timeout_s': timeout_s, 'pdf': pdf_identity(self.pdf_parser) if self.pdf_parser is not None else None}

    async def __call__(self, request):
        required = {'raw_ref', 'url', 'final_url', 'content_type', 'retrieved_at'}
        if not isinstance(request, dict) or set(request) != required:
            raise ValueError('ParseDocument request requires raw_ref and source context')
        if self.request_gate is not None:
            async with self.request_gate.enter():
                return await self._execute(request)
        return await self._execute(request)

    async def _execute(self, request):
        task = asyncio.create_task(asyncio.to_thread(run_isolated, _parse, request, self.store,
            self.max_raw_bytes, self.max_document_bytes, self.pdf_parser, timeout_s=self.timeout_s))
        try:
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                await asyncio.gather(task, return_exceptions=True)
                raise
        except (DocumentError, TimeoutError) as exc:
            return {**request, 'document_ref': None, 'status': 'parse_error', 'reason': str(exc),
                    'parser_version': PARSER_VERSION}
