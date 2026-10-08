"""Bounded single-image edit call for a local JSON /v1/images/edits service.

This is the local service contract (not OpenAI's multipart endpoint). The
request contains only an image and an instruction plus inference settings.
Dataset owns scheduling/caching; this function never scans a table or retries.
"""
import base64
import asyncio
import hashlib
import io
import json
from time import perf_counter

import httpx
from PIL import Image

from .objects import ObjectRef
from .execution.artifacts import digest


def checked_image(raw, *, max_bytes, max_pixels=24_000_000):
    if len(raw) > max_bytes:
        raise ValueError('Image exceeds byte budget')
    with Image.open(io.BytesIO(raw)) as image:
        if image.format not in {'PNG', 'JPEG', 'WEBP'} or image.width * image.height > max_pixels:
            raise ValueError('Unsupported image format or pixel budget exceeded')
        image.load()
        return Image.MIME[image.format], image.width, image.height


async def edit_image(*, source, instruction, model, revision, base_url, size, steps,
                     seed, object_store, timeout_s=600, max_image_bytes=32 * 1024**2):
    """Return a durable ObjectRef or an explicit technical failure; no pixels in logs."""
    metadata = {'model': model, 'prompt': instruction, 'size': size, 'n': 1,
                'response_format': 'b64_json', 'num_inference_steps': steps, 'seed': seed,
                'source_sha256': source['sha256']}
    identity = digest({'revision': revision, 'base_url': base_url, 'request': metadata})
    call = {'transport': 'local_image_edit_json', 'base_url': base_url,
            'endpoint': '/images/edits', 'revision': revision, 'request': metadata,
            'generation_id': identity}
    result = {'generation_id': identity, 'model': model, 'revision': revision,
              'seed': seed, 'image_sha256': None, 'object_ref': None, 'width': 0, 'height': 0}
    started = perf_counter()
    try:
        if not isinstance(instruction, str) or not instruction.strip() or len(instruction) > 16000:
            raise ValueError('Image edit instruction must contain 1..16000 characters')
        raw = ObjectRef(**source).read(max_bytes=max_image_bytes)
        mime, _, _ = checked_image(raw, max_bytes=max_image_bytes)
        request = {key: value for key, value in metadata.items() if key != 'source_sha256'}
        request['image'] = f'data:{mime};base64,' + base64.b64encode(raw).decode('ascii')
        async with asyncio.timeout(timeout_s), httpx.AsyncClient(timeout=timeout_s, trust_env=False) as client:
            async with client.stream('POST', base_url.rstrip('/') + '/images/edits', json=request) as response:
                limit = (max_image_bytes + 2) // 3 * 4 + 128 * 1024
                chunks, length = [], 0
                async for chunk in response.aiter_bytes():
                    length += len(chunk)
                    if length > limit:
                        raise ValueError('Image response exceeds byte budget')
                    chunks.append(chunk)
                call['http_status'] = response.status_code
                if response.status_code != 200:
                    raise ValueError('Image endpoint HTTP ' + str(response.status_code))
                body = json.loads(b''.join(chunks))
        if body.get('model') != model:
            raise ValueError('Returned model differs from requested model')
        usage = body.get('usage') or {}
        if usage.get('source_sha256') != source['sha256']:
            raise ValueError('Service did not acknowledge the actual source image')
        if usage.get('steps') != steps or usage.get('seed') != seed:
            raise ValueError('Returned generation parameters differ from request')
        images = body.get('data')
        if not isinstance(images, list) or len(images) != 1:
            raise ValueError('Exactly one generated image is required')
        encoded = images[0]['b64_json']
        if not isinstance(encoded, str) or len(encoded) > (max_image_bytes + 2) // 3 * 4:
            raise ValueError('Encoded image exceeds byte budget')
        raw = base64.b64decode(encoded, validate=True)
        _, width, height = checked_image(raw, max_bytes=max_image_bytes)
        if f'{width}x{height}' != size:
            raise ValueError('Returned image dimensions differ from request')
    except (httpx.HTTPError, TimeoutError, ValueError, OSError, KeyError, TypeError, AttributeError, Image.DecompressionBombError) as exc:
        result.update(status='failed', reason=f'{type(exc).__name__}: {exc}')
    else:
        # A storage failure aborts the action; it is not a model failure/low score.
        ref = object_store.put(raw).to_dict()
        result.update(status='generated', reason='', object_ref=ref,
                      image_sha256=hashlib.sha256(raw).hexdigest(), width=width, height=height)
    result['latency_s'] = round(perf_counter() - started, 3)
    call['latency_s'] = result['latency_s']
    result['call_json'] = json.dumps(call, ensure_ascii=False)
    return result
