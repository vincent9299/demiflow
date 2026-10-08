"""Bounded image acquisition behind Dataset.fetch_images.

The HTTP implementation is the same bounded transport used by documents.
Original bytes are preserved. Decode runs in an owned, timed subprocess with
an address-space limit; only scalar metadata returns to the Dataset row.
"""
import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import math
from pathlib import Path

from demiflow.objects import LocalObjectStore
from .image_library import ImageLibrary, local_path, verify_file
from .image_filters import ImageFilters, ImageHeaderProbe, declared_dimensions
from .web import normalized_url

IMAGE_DECODER_VERSION = 'bounded-image-decode-1'


@dataclass(frozen=True)
class ImageFetchPolicy:
    max_bytes: int = 20 * 1024 * 1024
    max_pixels: int = 40_000_000
    max_frames: int = 1
    decode_memory_bytes: int = 1024 * 1024 * 1024
    decode_timeout_s: float = 30
    concurrency: int = 2
    reuse_completed_concurrency: tuple[int, ...] | list[int] = ()
    decode_concurrency: int = 2
    filters: ImageFilters | dict | None = None
    allowed_mime_types: tuple[str, ...] | list[str] | None = None

    def __post_init__(self):
        previous = self.reuse_completed_concurrency
        if (not isinstance(previous, (tuple, list)) or len(previous) > 16 or
                any(type(n) is not int or not 1 <= n <= 256 for n in previous)):
            raise ValueError('reuse_completed_concurrency requires at most 16 previous limits in 1..256')
        object.__setattr__(self, 'reuse_completed_concurrency', tuple(sorted(set(previous))))
        filters = ImageFilters(**self.filters) if isinstance(self.filters, dict) else self.filters
        if filters is not None and not isinstance(filters, ImageFilters):
            raise TypeError('Image filters must be ImageFilters, a mapping, or None')
        object.__setattr__(self, 'filters', filters if filters and filters.enabled else None)
        if self.allowed_mime_types is not None:
            import re
            mimes = self.allowed_mime_types
            if (not isinstance(mimes, (tuple, list)) or not 1 <= len(mimes) <= 64 or
                    any(not isinstance(m, str) or len(m) > 255 or
                        not re.fullmatch(r'[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+', m) for m in mimes)):
                raise ValueError('allowed_mime_types must contain 1..64 lowercase MIME types')
            object.__setattr__(self, 'allowed_mime_types', tuple(sorted(set(mimes))))
        for key in ('max_bytes', 'max_pixels', 'max_frames', 'decode_memory_bytes', 'concurrency', 'decode_concurrency'):
            if type(getattr(self, key)) is not int or getattr(self, key) < 1:
                raise ValueError('Invalid image limit: ' + key)
        if self.max_bytes > 512 * 1024 * 1024 or self.max_frames > 256 or self.concurrency > 256 or self.decode_concurrency > 256:
            raise ValueError('Image policy exceeds supported acquisition limits')
        if self.decode_memory_bytes < 256 * 1024 * 1024:
            raise ValueError('Decoder address-space budget must be at least 256 MiB')
        if not isinstance(self.decode_timeout_s, (int, float)) or not math.isfinite(self.decode_timeout_s) or not 0 < self.decode_timeout_s <= 300:
            raise ValueError('Invalid image decode timeout')

    def declared_rejection(self, request):
        """Pure preflight shared by candidate selection and HTTP acquisition.

        Unknown metadata defers to transfer/decode. No consumer thresholds or
        image-content decisions belong here; all conditions come from policy.
        """
        dims = declared_dimensions(request)
        size = request.get('declared_file_bytes')
        mime = request.get('mime_type')
        if size is not None and (type(size) is not int or not 0 < size < 2**63):
            raise ValueError('declared_file_bytes must be a positive int64')
        if mime is not None and (not isinstance(mime, str) or len(mime) > 255):
            raise ValueError('mime_type must be a bounded string')
        if size and size > self.max_bytes:
            return f'max_bytes: declared={size}, allowed={self.max_bytes}'
        mime = mime.split(';', 1)[0].strip().lower() if mime else None
        if self.allowed_mime_types and mime and mime != 'application/octet-stream' and mime not in self.allowed_mime_types:
            return f'mime_type: declared={mime}, allowed={",".join(self.allowed_mime_types)}'
        if dims:
            reason = self.filters.rejection(*dims) if self.filters else None
            if reason:
                return reason
            if dims[0] * dims[1] > self.max_pixels:
                return f'max_pixels: declared={dims[0] * dims[1]}, allowed={self.max_pixels}'
        return None


def decode_file(uri, sha256, policy):
    """Owned subprocess; limits apply before importing Pillow or decoding."""
    from .image_decode import decode_image
    return decode_image(uri, sha256, policy)


async def drain_thread(function, *args, **kwargs):
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


class ImageClient:
    def __init__(self, web, library, policy):
        self.web, self.library, self.policy = web, library, policy
        self.gate = asyncio.Semaphore(policy.concurrency)
        self.decode_gate = asyncio.Semaphore(policy.decode_concurrency)
        self.metrics = {'local_reused': 0, 'downloaded': 0, 'decode_failed': 0}

    async def _validate(self, ref):
        async with self.decode_gate:
            return await drain_thread(decode_file, ref['uri'], ref['sha256'], asdict(self.policy))

    async def fetch(self, request):
        declared = declared_dimensions(request)
        declared_reason = self.policy.declared_rejection(request)
        url = request.get('url')
        expected = request.get('sha256')
        local_uri = request.get('image_uri')
        if url is not None and (not isinstance(url, str) or len(url.encode()) > 16384 or not normalized_url(url)):
            raise ValueError('Image request requires a valid bounded HTTP(S) URL')
        url = normalized_url(url) if url else None
        if expected is not None:
            self.library.reference(expected)  # validates the exact SHA syntax
        if local_uri is not None and (not expected or len(local_uri.encode()) > 16384):
            raise ValueError('Local image reuse requires bounded URI and expected SHA256')
        if not (url or expected):
            raise ValueError('Image request requires URL or expected SHA256')
        blank = {'url': url, 'final_url': None, 'image_ref': None, 'attempts': [], 'origin': None}
        excluded = self.web.exclusion_reason(url) if url else None
        if excluded:
            return {**blank, 'status': 'excluded', 'reason': excluded}

        async def acquire():
            async with self.gate, self.library.claim(url or expected):
                receipt = None
                ref = None
                if expected:
                    candidate = self.library.reference(expected).to_dict()
                    if local_path(candidate['uri']).exists():
                        ref = candidate
                    elif local_uri and local_path(local_uri).exists():
                        ref = {'uri': local_uri, 'sha256': expected}
                if ref is None and url:
                    receipt = await drain_thread(self.library.lookup, url)
                    if receipt is not None:
                        candidate = receipt['image_ref']
                        if expected and candidate['sha256'] != expected:
                            receipt = None
                        elif local_path(candidate['uri']).exists():
                            ref = candidate
                attempts = []
                retrieved_at = receipt.get('retrieved_at') if receipt else None
                final_url = receipt.get('final_url') if receipt else url
                origin = 'local'
                if ref is None:
                    if not url:
                        return {**blank, 'status': 'missing_local_image', 'reason': 'No object exists for expected SHA256'}
                    if declared_reason:
                        return {**blank, 'status': 'filtered', 'reason': declared_reason, 'filter_stage': 'declared',
                                **({'width': declared[0], 'height': declared[1]} if declared else {})}
                    from functools import partial
                    response = await self.web._get(url, **({'body_filter': partial(ImageHeaderProbe, self.policy.filters)}
                        if self.policy.filters else {}))
                    if response['status'] != 'ok':
                        return {**blank, **response}
                    body = response['body']
                    actual = hashlib.sha256(body).hexdigest()
                    if expected and actual != expected:
                        return {**blank, 'status': 'integrity_error', 'reason': 'image_sha256_mismatch', 'attempts': response['attempts']}
                    ref = (await drain_thread(LocalObjectStore(self.library.object_directory).put,
                                              body, sha256=actual)).to_dict()
                    del body
                    attempts = response['attempts']
                    final_url = response['final_url']
                    retrieved_at = datetime.now(timezone.utc).isoformat()
                    del response
                    origin = 'download'
                try:
                    info = await self._validate(ref)
                except (ValueError, OSError, RuntimeError, TimeoutError, MemoryError) as exc:
                    self.metrics['decode_failed'] += 1
                    return {**blank, 'status': 'image_error', 'reason': type(exc).__name__ + ': ' + str(exc)[:512],
                            'attempts': attempts, 'origin': origin}
                if self.policy.filters:
                    reason = self.policy.filters.rejection(info['width'], info['height'])
                    if reason:
                        return {**blank, **info, 'status': 'filtered', 'reason': reason, 'filter_stage': 'decoded',
                                'attempts': attempts, 'origin': origin, 'final_url': final_url}
                if self.policy.allowed_mime_types and info['content_type'] not in self.policy.allowed_mime_types:
                    return {**blank, **info, 'status': 'filtered', 'reason': 'mime_type: decoded=' + info['content_type'],
                            'filter_stage': 'decoded', 'attempts': attempts, 'origin': origin, 'final_url': final_url}
                if local_path(ref['uri']).resolve() != local_path(self.library.reference(ref['sha256']).uri).resolve():
                    # Explicit local sources become shared objects after byte
                    # and decode verification. No URL is needed for SHA reuse.
                    def import_file():
                        with local_path(ref['uri']).open('rb') as stream:
                            class BoundedReader:
                                consumed = 0
                                def read(reader, size):
                                    value = stream.read(min(size, self.policy.max_bytes-reader.consumed+1))
                                    reader.consumed += len(value)
                                    if reader.consumed > self.policy.max_bytes:
                                        raise ValueError('image_bytes_exceeded_during_import')
                                    return value
                            return LocalObjectStore(self.library.object_directory).put_stream(BoundedReader(), sha256=ref['sha256']).to_dict()
                    ref = await drain_thread(import_file)
                result = {**blank, **info, 'image_ref': ref, 'status': 'ok', 'reason': '',
                          'origin': origin, 'final_url': final_url, 'retrieved_at': retrieved_at, 'attempts': attempts}
                await drain_thread(self.library.publish, result)
                self.metrics['downloaded' if origin == 'download' else 'local_reused'] += 1
                return result

        identity_policy = asdict(self.policy)
        metadata = {k: request[k] for k in ('declared_width', 'declared_height', 'declared_file_bytes', 'mime_type')
                    if request.get(k) is not None}
        if self.policy.allowed_mime_types is None:
            identity_policy.pop('allowed_mime_types')
        if metadata or self.policy.allowed_mime_types:
            identity_policy['metadata_filter_version'] = 'image-metadata-1'
            identity_policy['declared_metadata'] = metadata
        identity_policy.pop('decode_concurrency')  # Scheduling alone preserves existing receipts.
        identity_policy.pop('concurrency')
        identity_policy.pop('reuse_completed_concurrency')
        if not self.policy.filters:
            identity_policy.pop('filters')  # Opt-out preserves pre-filter receipts.
        else:
            identity_policy['filter_version'] = 'image-filters-1'
            identity_policy['declared_dimensions'] = declared
        identity = [IMAGE_DECODER_VERSION, url, expected, local_uri, self.library.identity, identity_policy,
                    self.web.max_bytes, self.web.redirects, self.web.retries, self.web.timeout_s,
                    self.web.proxy_identity(url) if url else None]
        # Historical image keys included network concurrency. Explicit migration
        # reads those receipts without granting another request; new keys omit it.
        previous = []
        for limit in dict.fromkeys((self.policy.concurrency, *self.policy.reuse_completed_concurrency)):
            before = list(identity)
            before[5] = {**identity_policy, 'concurrency': limit}
            previous.append(before)
        result = (await asyncio.to_thread(self.web._completed_fetch_from_previous_proxy,url,identity,
            operation='fetch_image',transport_index=10,previous_identities=previous)) if url else None
        if result is None:
            result = await self.web._once('fetch_image', identity, acquire)
        else:
            self.web.metrics['reused']+=1
            self.web.metrics['reused_previous_proxy']=self.web.metrics.get('reused_previous_proxy',0)+1
        if result['status'] == 'ok':
            # A successful old journal entry must not hide a subsequently
            # missing/corrupt file. Failure does not silently renew HTTP quota.
            try:
                await drain_thread(verify_file, result['image_ref']['uri'], result['image_ref']['sha256'], self.policy.max_bytes)
            except (OSError, ValueError) as exc:
                return {**blank, 'status': 'integrity_error', 'reason': str(exc)[:512]}
        return result


from .connection_manager import WebOperatorLifecycle


class FetchImages(WebOperatorLifecycle):
    concurrency = 1

    def __init__(self, requests, output, session, when, request_concurrency, max_requests, max_request_bytes):
        self.requests, self.output, self.session, self.when = requests, output, session, when
        self.request_concurrency = request_concurrency
        self.max_requests, self.max_request_bytes = max_requests, max_request_bytes
        self.resources = (session,)

    async def __call__(self, row):
        if self.when is not None and not self.when(row):
            return row
        import json
        from .session import bounded
        requests = row[self.requests]
        if not isinstance(requests, list) or len(requests) > self.max_requests:
            raise ValueError('Image request list exceeds declared bound')
        size = 0
        ids = set()
        for r in requests:
            # Reject individual oversized fields before serializing the bounded
            # metadata request. Bodies/embedded image bytes are not accepted.
            if not isinstance(r, dict) or set(r) - {'request_id', 'url', 'sha256', 'image_uri', 'bindings', 'declared_width', 'declared_height', 'declared_file_bytes', 'mime_type'}:
                raise ValueError('Invalid image request fields')
            declared_dimensions(r)
            if not isinstance(r.get('request_id'), str) or not r['request_id'] or r['request_id'] in ids:
                raise ValueError('Image request IDs must be unique within the row')
            for key in ('request_id', 'url', 'sha256', 'image_uri'):
                if r.get(key) is not None and (not isinstance(r[key], str) or len(r[key]) > 16384):
                    raise ValueError('Image request field exceeds declared bound')
            bindings = r.get('bindings', [])
            if not isinstance(bindings, list) or len(bindings) > 128 or any(not isinstance(v, str) or len(v) > 1024 for v in bindings):
                raise ValueError('Image request bindings exceed declared bound')
            ids.add(r['request_id'])
            size += len(json.dumps(r, ensure_ascii=False).encode())
            if size > self.max_request_bytes:
                raise ValueError('Image requests exceed metadata byte budget')

        async def one(r):
            return {**r, 'result': await self.session.fetch_image(r)}
        return {**row, self.output: await bounded(requests, one, self.request_concurrency)}
