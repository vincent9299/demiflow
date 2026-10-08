"""Bounded blocking preparation/validation; called only in the node I/O pool."""
import base64
import hashlib
import io
import json
import math
import re
import struct
import time
import zlib

from demiflow.objects import ObjectRef, open_object
from demiflow.operator_llm.journal import RequestRecord
from .model import canonical


class EmbeddingInputError(ValueError):
    """A particular input cannot be encoded; optionally retained as a failed row."""


class EmbeddingProtocolError(ValueError):
    """Provider vectors violate the declared contract; abort the whole action."""


def _add(timings, key, value):
    if timings is not None:
        timings[key] = timings.get(key, 0) + value


class _LimitedBuffer(io.BytesIO):
    """Reject a PNG write before growing beyond the configured encoded budget."""
    def __init__(self, limit):
        super().__init__()
        self.limit = limit

    def write(self, value):
        if self.tell() + len(value) > self.limit:
            raise ValueError('Prepared PNG exceeds max_image_bytes')
        return super().write(value)


def image_content(value, options, *, transport='png', timings=None, image_metadata=None):
    """One ObjectRef, or alternate ObjectRefs for the same SHA (not a collage)."""
    from PIL import Image, ImageOps, PngImagePlugin
    refs = value if isinstance(value, (list, tuple)) else [value]
    refs = [ref.to_dict() if isinstance(ref, ObjectRef) else ref for ref in refs]
    if (not refs or any(not isinstance(ref, dict) for ref in refs)
            or any(not isinstance(ref.get('sha256'), str)
                   or not re.fullmatch('[0-9a-f]{64}', ref['sha256']) for ref in refs)
            or len({ref['sha256'] for ref in refs}) != 1):
        raise EmbeddingInputError('Image requires ObjectRefs with one identical SHA256')
    errors = []
    for ref in refs:
        try:
            started = time.perf_counter()
            with open_object(ref.get('uri')) as source:
                raw = source.read(options['max_image_bytes'] + 1)
            if len(raw) > options['max_image_bytes']:
                raise ValueError('Image exceeds max_image_bytes')
            if hashlib.sha256(raw).hexdigest() != ref['sha256']:
                raise ValueError('Image content differs from declared SHA256')
            _add(timings, 'image_read_sha_s', time.perf_counter() - started)
            _add(timings, 'image_input_bytes', len(raw))
            started = time.perf_counter()
            with Image.open(io.BytesIO(raw)) as original:
                pixels = original.width * original.height
                if pixels > options['max_decode_pixels']:
                    raise ValueError('Image exceeds max_decode_pixels')
                original.seek(0)
                original.load()
                # Full decode and SHA validation still happen locally. Only bypass
                # conversion when the endpoint receives exactly the normalized RGB
                # first frame: no animation, orientation, transparency or color work.
                mime = {'JPEG': 'image/jpeg', 'PNG': 'image/png', 'WEBP': 'image/webp'}
                invalid_exif = False
                try:
                    # JPEG opening may swallow a malformed EXIF error while
                    # inferring DPI, leaving getexif() cached as an empty result.
                    # A new decoder (including the backend) can still raise on
                    # those bytes. Validate ancillary EXIF independently first.
                    exif_bytes = original.info.get('exif')
                    if exif_bytes is None and 'Raw profile type exif' in original.info:
                        exif_bytes = bytes.fromhex(''.join(
                            original.info['Raw profile type exif'].split('\n')[3:]))
                    if exif_bytes is not None:
                        Image.Exif().load(exif_bytes)
                    orientation = original.getexif().get(274, 1)
                except (SyntaxError, ValueError, TypeError, struct.error, EOFError):
                    orientation, invalid_exif = 1, True
                passthrough = (transport == 'original_if_compatible'
                    and original.format in mime and original.mode == 'RGB'
                    and not getattr(original, 'is_animated', False)
                    and not invalid_exif and orientation == 1
                    and 'transparency' not in original.info)
                # An RGB ICC profile alone does not require pixel conversion:
                # the normalized PNG path preserves that same profile too.
                _add(timings, 'image_decode_s', time.perf_counter() - started)
                started = time.perf_counter()
                if passthrough:
                    media_type, payload = mime[original.format], raw
                    _add(timings, 'original_images', 1)
                else:
                    try:
                        if invalid_exif:
                            raise SyntaxError('Invalid EXIF metadata')
                        oriented = ImageOps.exif_transpose(original)
                    except (SyntaxError, ValueError, TypeError, struct.error, EOFError):
                        # Preserve a readable orientation, otherwise keep stored
                        # pixels. Invalid ancillary EXIF must not reach the PNG
                        # reader again, nor abort unrelated images in the batch.
                        method = {2: Image.Transpose.FLIP_LEFT_RIGHT, 3: Image.Transpose.ROTATE_180,
                            4: Image.Transpose.FLIP_TOP_BOTTOM, 5: Image.Transpose.TRANSPOSE,
                            6: Image.Transpose.ROTATE_270, 7: Image.Transpose.TRANSVERSE,
                            8: Image.Transpose.ROTATE_90}.get(orientation)
                        oriented = original.transpose(method) if method is not None else original.copy()
                        oriented.info.pop('exif', None)
                        oriented.info.pop('Raw profile type exif', None)
                        _add(timings, 'invalid_exif_omitted', 1)
                    try:
                        with oriented.convert('RGB') as rgb:
                            # Some monochrome PNGs retain a scalar tRNS value
                            # after RGB conversion. It is invalid for an RGB
                            # PNG writer; the declared RGB pixels are opaque.
                            if ('transparency' in rgb.info
                                    and not isinstance(rgb.info['transparency'], (tuple, list))):
                                rgb.info.pop('transparency')
                            with _LimitedBuffer(options['max_image_bytes']) as encoded:
                                # JPEG/TIFF can carry ICC metadata larger than
                                # Pillow's PNG decompression limit. The model
                                # consumes the already normalized RGB pixels;
                                # omit only this oversized ancillary profile.
                                save_options = {}
                                if len(rgb.info.get('icc_profile') or b'') > PngImagePlugin.MAX_TEXT_CHUNK:
                                    save_options['icc_profile'] = None
                                    _add(timings, 'oversized_png_profiles_omitted', 1)
                                rgb.save(encoded, format='PNG', compress_level=options.get('png_compress_level', 6),
                                         **save_options)
                                payload = encoded.getvalue()
                        media_type = 'image/png'
                        _add(timings, 'png_images', 1)
                    finally:
                        if oriented is not original:
                            oriented.close()
                _add(timings, 'image_normalize_encode_s', time.perf_counter() - started)
            # Bound the base64 expansion before allocating it, in addition to
            # prepare()'s aggregate batch check. JSON envelope is checked there.
            if 4 * ((len(payload) + 2) // 3) + len(media_type) + 13 > options['max_request_bytes']:
                raise ValueError('Image data URI exceeds max_request_bytes')
            started = time.perf_counter()
            url = 'data:' + media_type + ';base64,' + base64.b64encode(payload).decode()
            _add(timings, 'image_base64_s', time.perf_counter() - started)
            _add(timings, 'image_transport_bytes', len(payload))
            if image_metadata is not None:
                image_metadata['image_pixels'] = pixels
            return {'type': 'image_url', 'image_url': {'url': url}}, ref['uri']
        except (OSError, ValueError, KeyError, TypeError, SyntaxError, struct.error,
                EOFError, zlib.error, Image.DecompressionBombError) as error:
            errors.append(f'{ref.get("uri")}: {type(error).__name__}: {error}')
    raise EmbeddingInputError('; '.join(errors))


def prepare(rows, *, model, inputs, options, error_output, timings=None, document_reader=None):
    """Prepare one exact request (legacy strict batch admission)."""
    return next(prepare_batches(rows, model=model, inputs=inputs, options=options,
                               error_output=error_output, timings=timings, split=False,
                               document_reader=document_reader))


def prepare_batches(rows, *, model, inputs, options, error_output, timings=None, split=True,
                    document_reader=None):
    """Lazy byte-bounded requests; at most one group's payload plus one carry item.

    The caller advances this iterator in its bounded preparation pool and drains
    each request before advancing. No decoding is repeated when a group is split.
    batch_request_bytes is a packing target; a larger single item is allowed only
    within max_request_bytes (including its JSON envelope).
    batch_decode_pixels additionally limits aggregate decoded pixels per group;
    larger single images still obey max_decode_pixels without being resized.
    """
    valid, invalid, values, metadata, encoded_values = [], [], [], [], []
    modality, column = next(iter(inputs.items()))
    input_key = 'input' if model.input_format == 'text' else 'messages'
    envelope = len(canonical({'model': model.name, 'encoding_format': 'float',
                             **model.request_options, input_key: []}).encode())
    hard_limit = options['max_request_bytes']
    target = options.get('batch_request_bytes') or hard_limit
    pixel_target = options.get('batch_decode_pixels')
    prepared_bytes = envelope
    prepared_pixels = 0
    for row in rows:
        try:
            value = row.get(column)
            pixels = 0
            if modality == 'image':
                meta = {}
                content, uri = image_content(value, options, transport=model.image_transport, timings=timings,
                                             image_metadata=meta)
                meta['image_uri'] = uri
                pixels = meta['image_pixels']
            else:
                meta = {}
                if modality == 'document':
                    from .documents import DocumentInputReader
                    if document_reader is None:
                        document_reader = DocumentInputReader(options)
                    started = time.perf_counter()
                    try:
                        value, meta = document_reader(value)
                    except (OSError, ValueError, KeyError, TypeError) as error:
                        raise EmbeddingInputError(str(error)) from error
                    _add(timings, 'document_read_prepare_s', time.perf_counter() - started)
                if not isinstance(value, str) or not value.strip():
                    raise EmbeddingInputError('Text input must be a nonempty string')
                if len(value) > hard_limit:
                    raise EmbeddingInputError('Text exceeds max_request_bytes')
                content = {'type': 'text', 'text': value}
            prepared_value = value if model.input_format == 'text' else [{'role': 'user', 'content': [content]}]
            started = time.perf_counter()
            if modality == 'image' and model.input_format == 'chat':
                # image_content constructs an ASCII data URI from a fixed MIME
                # and base64 alphabet: none of its characters needs JSON escaping.
                # Preserve canonical()'s exact bytes without scanning multi-MiB
                # strings under the JSON encoder's GIL a second time.
                url = content['image_url']['url']
                prefix = b'[{"content":[{"image_url":{"url":"'
                suffix = b'"},"type":"image_url"}],"role":"user"}]'
                if envelope + len(prefix) + len(url) + len(suffix) > hard_limit:
                    raise EmbeddingInputError('Single input with JSON envelope exceeds max_request_bytes')
                encoded_value = b''.join((prefix, url.encode('ascii'), suffix))
            else:
                encoded_value = canonical(prepared_value).encode()
            _add(timings, 'serialize_s', time.perf_counter() - started)
            if envelope + len(encoded_value) > hard_limit:
                raise EmbeddingInputError('Single input with JSON envelope exceeds max_request_bytes')
        except EmbeddingInputError as error:
            if error_output is None:
                raise
            invalid.append((row, f'{type(error).__name__}: {error}'))
            continue
        if valid and split and (prepared_bytes + 1 + len(encoded_value) > target or
                                (pixel_target is not None and prepared_pixels + pixels > pixel_target)):
            yield _pack(valid, invalid, values, metadata, encoded_values, input_key, model, options, timings)
            valid, invalid, values, metadata, encoded_values = [], [], [], [], []
            prepared_bytes = envelope
            prepared_pixels = 0
        added = len(encoded_value) + bool(valid)
        if prepared_bytes + added > hard_limit:
            raise ValueError('Batch exceeds max_request_bytes; enable byte packing or lower batch_size')
        valid.append(row)
        metadata.append(meta)
        values.append(prepared_value)
        encoded_values.append(encoded_value)
        prepared_bytes += added
        prepared_pixels += pixels
    if valid or invalid:
        yield _pack(valid, invalid, values, metadata, encoded_values, input_key, model, options, timings)


def _pack(valid, invalid, values, metadata, encoded_values, input_key, model, options, timings):
    if not valid:
        return valid, invalid, metadata, None, None
    body = {'model': model.name, 'encoding_format': 'float', **model.request_options,
            input_key: values}
    started = time.perf_counter()
    # Each input was serialized once for byte admission. Reuse those fragments
    # instead of serializing the full base64 body twice more for HTTP and hashing.
    parts = [b'{']
    for index, key in enumerate(sorted(body)):
        if index:
            parts.append(b',')
        parts.extend((canonical(key).encode(), b':'))
        if key == input_key:
            parts.append(b'[')
            for i, value in enumerate(encoded_values):
                if i:
                    parts.append(b',')
                parts.append(value)
            parts.append(b']')
        else:
            parts.append(canonical(body[key]).encode())
    parts.append(b'}')
    if sum(map(len, parts)) > options['max_request_bytes']:
        raise EmbeddingInputError('Batch exceeds max_request_bytes; lower batch_size or image limits')
    encoded = b''.join(parts)
    _add(timings, 'serialize_s', time.perf_counter() - started)
    _add(timings, 'request_bytes', len(encoded))
    if len(encoded) > options['max_request_bytes']:
        raise EmbeddingInputError('Batch exceeds max_request_bytes; lower batch_size or image limits')
    # Hashing large inline images must also stay off the shared event loop.
    started = time.perf_counter()
    request = RequestRecord._from_canonical_body(protocol='embeddings-v1',
        model_contract=model.contract(), body=body, encoded_body=encoded)
    _add(timings, 'request_hash_s', time.perf_counter() - started)
    return valid, invalid, metadata, encoded, request


def response_record(status, raw):
    """Use the native HTTP journal shape, retaining malformed complete bodies too."""
    try:
        body = json.loads(raw)
        canonical(body)  # Reject non-finite JSON before writing a SQLite JSON column.
    except ValueError:
        return {'status_code': status, 'body': None, 'raw_response': raw}
    return {'status_code': status, 'body': body}


def vectors(response, count, model):
    if not isinstance(response, dict) or not isinstance(response.get('data'), list):
        raise EmbeddingProtocolError('Embedding response requires data[]')
    if response.get('model') not in (None, model.name):
        raise EmbeddingProtocolError('Embedding response model differs from declaration')
    if len(response['data']) != count:
        raise EmbeddingProtocolError('Embedding response cardinality differs from input batch')
    result = [None] * count
    for item in response['data']:
        index = item.get('index') if isinstance(item, dict) else None
        if type(index) is not int or not 0 <= index < count or result[index] is not None:
            raise EmbeddingProtocolError('Embedding response indices must be unique and cover the batch')
        value = item.get('embedding')
        if (not isinstance(value, list) or len(value) != model.dimensions
                or any(type(v) not in (int, float) for v in value)):
            raise EmbeddingProtocolError('Embedding vector has an invalid dimension or numeric type')
        try:
            value = [float(v) for v in value]
            if not all(math.isfinite(v) for v in value):
                raise ValueError('non-finite value')
            norm = math.hypot(*value)
            if not math.isfinite(norm) or norm == 0:
                raise ValueError('zero or non-finite norm')
            if model.normalize:
                value = [v / norm for v in value]
            value = list(struct.unpack(f'{len(value)}f', struct.pack(f'{len(value)}f', *value)))
            if not all(math.isfinite(v) for v in value) or not math.hypot(*value):
                raise ValueError('invalid float32 vector')
        except (ValueError, OverflowError, struct.error) as error:
            raise EmbeddingProtocolError(f'Embedding contains an invalid vector: {error}') from error
        result[index] = value
    return result
