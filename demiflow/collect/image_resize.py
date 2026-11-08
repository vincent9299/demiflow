"""Bounded raster resizing in an owned, memory-limited Pillow subprocess.

The caller supplies verified encoded bytes. Originals are never rewritten.
Only two decoders per calling process may run concurrently; no model calls.
"""
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time

_SLOTS = threading.BoundedSemaphore(2)


def resize_image(raw, *, max_side=1536, max_source_pixels=160_000_000,
                 max_bytes=32 * 1024**2, memory_bytes=1536 * 1024**2,
                 timeout_s=45, quality=90, profile='thumbnail_lanczos_v1'):
    """Return a white-background, EXIF-oriented JPEG within max_side.

    Input <=32 MiB, source <=160M pixels, output <=8 MiB. Address space and CPU
    limits apply in the decoder; semaphore waiting shares the wall-time budget.
    Temporary disk <=48 MiB per invocation, plus a small policy file.
    """
    values = {'max_side': (max_side, 1, 4096),
              'max_source_pixels': (max_source_pixels, 1, 160_000_000),
              'max_bytes': (max_bytes, 1, 32 * 1024**2),
              'memory_bytes': (memory_bytes, 256 * 1024**2, 2 * 1024**3),
              'quality': (quality, 1, 95)}
    for key, (value, low, high) in values.items():
        if type(value) is not int or not low <= value <= high:
            raise ValueError('Invalid resize limit: ' + key)
    if profile not in {'thumbnail_lanczos_v1', 'oriented_white_bicubic_v1'}:
        raise ValueError('Unknown image encoding profile')
    if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120:
        raise ValueError('Invalid resize timeout_s')
    if not isinstance(raw, bytes) or not raw or len(raw) > max_bytes:
        raise ValueError('Resize input exceeds byte budget or is empty')
    deadline = time.monotonic() + timeout_s
    if not _SLOTS.acquire(timeout=timeout_s):
        raise ValueError('Image resize timed out waiting for a decoder')
    try:
        with tempfile.TemporaryDirectory(prefix='demiflow-image-resize-') as name:
            directory = Path(name)
            (directory / 'input').write_bytes(raw)
            policy = {key: value[0] for key, value in values.items()}
            policy.update(timeout_s=timeout_s, max_output_bytes=8 * 1024**2, profile=profile, parent_pid=os.getpid())
            (directory / 'policy.json').write_text(json.dumps(policy))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError('Image resize timed out before decoding')
            # Direct script execution avoids importing Arrow/BLAS in the decoder.
            with (directory / 'stderr').open('wb') as stderr:
                try:
                    result = subprocess.run([sys.executable, '-I', str(Path(__file__).resolve()), name],
                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=stderr,
                        timeout=remaining, start_new_session=True, check=False)
                except subprocess.TimeoutExpired as exc:
                    raise ValueError('Image resize exceeded wall-time budget') from exc
            output = directory / 'output.jpg'
            if result.returncode != 0 or not output.exists():
                with (directory / 'stderr').open('rb') as stream:
                    detail = stream.read(4096).decode('utf-8', errors='replace')
                raise ValueError('Isolated image resize failed: ' + (detail or f'exit {result.returncode}'))
            if output.stat().st_size > policy['max_output_bytes']:
                raise ValueError('Resized image exceeds output byte budget')
            return output.read_bytes()
    finally:
        _SLOTS.release()


def _worker(directory):
    import resource
    policy = json.loads((directory / 'policy.json').read_text())
    if sys.platform == 'linux':
        import ctypes
        import signal
        if ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL) != 0:
            raise OSError('Cannot bind decoder lifetime to parent process')
        if os.getppid() != policy['parent_pid']:
            raise RuntimeError('Image decoder parent exited before lifetime binding')
    resource.setrlimit(resource.RLIMIT_AS, (policy['memory_bytes'],) * 2)
    resource.setrlimit(resource.RLIMIT_CPU, (math.ceil(policy['timeout_s']),) * 2)
    resource.setrlimit(resource.RLIMIT_FSIZE, (policy['max_output_bytes'],) * 2)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    from PIL import Image, ImageOps, ImageFile
    ImageFile.LOAD_TRUNCATED_IMAGES = False
    Image.MAX_IMAGE_PIXELS = policy['max_source_pixels']
    source = directory / 'input'
    if source.stat().st_size > policy['max_bytes']:
        raise ValueError('Image input byte limit')
    with Image.open(source) as picture:
        if picture.width * picture.height > policy['max_source_pixels']:
            raise ValueError(f'Image exceeds source pixel limit ({policy["max_source_pixels"]})')
        if picture.format not in {'JPEG', 'PNG', 'WEBP', 'GIF', 'BMP', 'TIFF'}:
            raise ValueError('Unsupported raster format')
        picture.load()  # Pixel expansion is confined to this limited process.
        if policy['profile'] == 'oriented_white_bicubic_v1':
            import struct
            try:
                oriented = ImageOps.exif_transpose(picture)
            except (SyntaxError, ValueError, struct.error):
                oriented = picture.copy()
            # Each stage owns an independent Pillow core. Release the previous
            # full-resolution image before allocating the next stage's buffers.
            picture.close()
            rgba = oriented.convert('RGBA')
            oriented.close()
            background = Image.new('RGBA', rgba.size, 'white')
            background.alpha_composite(rgba)
            rgba.close()
            rgb = background.convert('RGB')
            background.close()
            rgb.thumbnail((policy['max_side'], policy['max_side']), Image.Resampling.BICUBIC)
            rgb.save(directory / 'output.jpg', format='JPEG', quality=policy['quality'])
            rgb.close()
            return
        if picture.mode == 'P':
            picture = picture.convert('RGBA')
        picture.thumbnail((policy['max_side'], policy['max_side']), Image.Resampling.LANCZOS)
        picture = ImageOps.exif_transpose(picture).convert('RGBA')
        background = Image.new('RGB', picture.size, 'white')
        background.paste(picture, mask=picture.getchannel('A'))
        background.save(directory / 'output.jpg', format='JPEG', quality=policy['quality'])


class EncodeImage:
    def __init__(self, **arguments):
        from .flow_contract import normalize_image
        self.arguments = normalize_image(arguments)

    def snapshot_state(self):
        # Profile/limits are bound by collect_facts. Each call owns and drains
        # its decoder process/temp directory; neither is cross-row state.
        return {}

    def restore_state(self, state):
        if type(state) is not dict or state:
            raise ValueError('Image encoder has no mutable semantic state')

    def __call__(self, row):
        import base64
        import hashlib
        from .flow_contract import row_result
        a = self.arguments
        raw = row[a['column']]
        if raw is None:
            return row_result(row, a, None)
        try:
            encoded = resize_image(raw, **{k: a[k] for k in ('max_side', 'max_source_pixels', 'max_bytes',
                'memory_bytes', 'timeout_s', 'quality', 'profile')})
            result = {'data_url': 'data:image/jpeg;base64,' + base64.b64encode(encoded).decode('ascii'),
                      'sha256': hashlib.sha256(encoded).hexdigest()}
        except (OSError, ValueError, TypeError) as error:
            return row_result(row, a, None, error)
        return row_result(row, a, result)


if __name__ == '__main__':
    try:
        _worker(Path(sys.argv[1]))
    except Exception as error:
        print(type(error).__name__ + ': ' + str(error)[:1024], file=sys.stderr)
        sys.exit(1)
