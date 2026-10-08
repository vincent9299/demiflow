"""Scalar image validation in a small, resource-limited Pillow process.

This file is also the worker entry point. Keep its imports stdlib-only: invoking
``python -I <file>`` must not import Demiflow, Arrow or BLAS before applying limits.
The caller owns concurrency; each invocation uses <=64 KiB temporary metadata.
"""
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.parse import unquote, urlsplit


def local_path(uri):
    p = urlsplit(uri)
    if p.scheme != 'file' or p.netloc not in ('', 'localhost') or p.query or p.fragment:
        raise ValueError('Image reuse requires an explicit local file URI')
    path = Path(unquote(p.path))
    if not path.is_absolute():
        raise ValueError('Image reuse requires an absolute local file URI')
    return path


def verify_file(uri, expected_sha256, max_bytes):
    path = local_path(uri)
    if path.stat().st_size > max_bytes:
        raise ValueError('image_bytes_exceeded')
    digest = hashlib.sha256()
    size = 0
    with path.open('rb') as stream:
        while chunk := stream.read(min(1024 * 1024, max_bytes - size + 1)):
            size += len(chunk)
            if size > max_bytes:
                raise ValueError('image_bytes_exceeded')
            digest.update(chunk)
    if digest.hexdigest() != expected_sha256:
        raise ValueError('image_sha256_mismatch')
    return size


def decode_image(uri, sha256, policy):
    """Validate encoded bytes without importing the framework in the child."""
    request = {'uri': uri, 'sha256': sha256, 'policy': {key: policy[key] for key in (
        'max_bytes', 'max_pixels', 'max_frames', 'decode_memory_bytes', 'decode_timeout_s')}}
    encoded = json.dumps(request).encode()
    if len(encoded) > 32 * 1024:
        raise ValueError('Image decoder request exceeds metadata budget')
    with tempfile.TemporaryDirectory(prefix='demiflow-image-decode-') as name:
        directory = Path(name)
        (directory / 'request.json').write_bytes(encoded)
        try:
            result = subprocess.run([sys.executable, '-I', str(Path(__file__).resolve()), name],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=policy['decode_timeout_s'], start_new_session=True, check=False)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError('Image decode exceeded wall-time budget') from exc
        response = directory / 'result.json'
        if result.returncode != 0 or not response.exists():
            raise RuntimeError(f'Isolated image decoder exited {result.returncode}')
        with response.open('rb') as stream:
            raw = stream.read(16 * 1024 + 1)
        if len(raw) > 16 * 1024:
            raise RuntimeError('Image decoder response exceeds metadata budget')
        value = json.loads(raw)
        if value['ok']:
            return value['metadata']
        kind, reason = value['error_type'], value['reason']
        error = {'MemoryError': MemoryError, 'OSError': OSError,
                 'RuntimeError': RuntimeError, 'TimeoutError': TimeoutError}.get(kind, ValueError)
        raise error(reason)


def _decode(uri, sha256, policy):
    size = verify_file(uri, sha256, policy['max_bytes'])
    from PIL import Image, ImageFile
    import warnings
    ImageFile.LOAD_TRUNCATED_IMAGES = False
    Image.MAX_IMAGE_PIXELS = policy['max_pixels']
    with warnings.catch_warnings():
        warnings.simplefilter('error', Image.DecompressionBombWarning)
        with Image.open(local_path(uri)) as im:
            width, height = im.size
            frames = getattr(im, 'n_frames', 1)
            if width * height > policy['max_pixels'] or frames > policy['max_frames']:
                raise ValueError('image_pixel_or_frame_limit')
            fmt = im.format
            if fmt not in {'JPEG', 'PNG', 'WEBP', 'GIF', 'BMP', 'TIFF'}:
                raise ValueError('unsupported_raster_format')
            im.verify()
        with Image.open(local_path(uri)) as im:
            for frame in range(frames):
                im.seek(frame)
                if im.width * im.height > policy['max_pixels']:
                    raise ValueError('image_pixel_limit')
                im.load()
        return {'width': width, 'height': height, 'format': fmt,
                'content_type': Image.MIME.get(fmt, 'application/octet-stream'), 'size_bytes': size}


def _worker(directory):
    import resource
    with (directory / 'request.json').open('rb') as stream:
        raw = stream.read(32 * 1024 + 1)
    if len(raw) > 32 * 1024:
        raise ValueError('Image decoder request exceeds metadata budget')
    request = json.loads(raw)
    policy = request['policy']
    resource.setrlimit(resource.RLIMIT_AS, (policy['decode_memory_bytes'],) * 2)
    resource.setrlimit(resource.RLIMIT_CPU, (math.ceil(policy['decode_timeout_s']) + 1,) * 2)
    resource.setrlimit(resource.RLIMIT_FSIZE, (16 * 1024,) * 2)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    try:
        value = {'ok': True, 'metadata': _decode(request['uri'], request['sha256'], policy)}
    except Exception as error:
        value = {'ok': False, 'error_type': type(error).__name__, 'reason': str(error)[:1024]}
    (directory / 'result.json').write_text(json.dumps(value))


if __name__ == '__main__':
    _worker(Path(sys.argv[1]))
