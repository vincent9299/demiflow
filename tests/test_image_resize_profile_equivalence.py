"""Byte-level profile checks for sources affected by early image-core release."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from PIL import Image, ImageOps

from demiflow.collect import image_resize


def source_bytes(fmt):
    first = Image.new('RGBA', (43, 29), (31, 91, 151, 73))
    first.paste((211, 53, 17, 191), (3, 2, 27, 19))
    second = Image.new('RGBA', first.size, (5, 233, 47, 255))
    output = io.BytesIO()
    if fmt == 'GIF':
        first = first.convert('P', palette=Image.Palette.ADAPTIVE, colors=16)
        second = second.convert('P', palette=Image.Palette.ADAPTIVE, colors=16)
    options = {'save_all': True, 'append_images': [second]} if fmt in {'GIF', 'TIFF'} else {}
    if fmt == 'PNG':
        first = first.convert('RGB').quantize(colors=16)
        options['transparency'] = bytes(range(0, 256, 16))
    first.save(output, format=fmt, **options)
    return output.getvalue()


def profile_reference(raw, profile, *, force_orientation_fallback=False):
    """The established pixel contract, retaining every original stage.

    This oracle deliberately does not use the worker or release its intermediates.
    In particular, it preserves the different resize/composite order of the two
    existing profiles and the first-frame semantics of multi-frame files.
    """
    with Image.open(io.BytesIO(raw)) as picture:
        picture.load()
        if profile == 'oriented_white_bicubic_v1':
            oriented = picture.copy() if force_orientation_fallback else ImageOps.exif_transpose(picture)
            rgba = oriented.convert('RGBA')
            background = Image.new('RGBA', rgba.size, 'white')
            background.alpha_composite(rgba)
            result = background.convert('RGB')
            result.thumbnail((17, 17), Image.Resampling.BICUBIC)
        else:
            if picture.mode == 'P':
                picture = picture.convert('RGBA')
            picture.thumbnail((17, 17), Image.Resampling.LANCZOS)
            rgba = ImageOps.exif_transpose(picture).convert('RGBA')
            result = Image.new('RGB', rgba.size, 'white')
            result.paste(rgba, mask=rgba.getchannel('A'))
        output = io.BytesIO()
        result.save(output, format='JPEG', quality=88)
        return output.getvalue()


@pytest.mark.parametrize('fmt', ['PNG', 'GIF', 'TIFF'])
@pytest.mark.parametrize('profile', ['thumbnail_lanczos_v1', 'oriented_white_bicubic_v1'])
def test_palette_and_multiframe_sources_keep_exact_profile_bytes(fmt, profile):
    raw = source_bytes(fmt)
    actual = image_resize.resize_image(raw, max_side=17, quality=88, profile=profile)
    assert actual == profile_reference(raw, profile)


def test_orientation_failure_still_uses_independent_copy(tmp_path):
    raw = source_bytes('PNG')
    expected = profile_reference(raw, 'oriented_white_bicubic_v1', force_orientation_fallback=True)
    (tmp_path / 'input').write_bytes(raw)
    policy = dict(max_source_pixels=160_000_000, max_bytes=32 * 1024**2,
                  max_side=17, quality=88, memory_bytes=256 * 1024**2,
                  timeout_s=5, max_output_bytes=8 * 1024**2,
                  profile='oriented_white_bicubic_v1', parent_pid=os.getpid())
    (tmp_path / 'policy.json').write_text(json.dumps(policy))
    # The real worker is a separate limited process. Inject only an EXIF parse
    # failure there to exercise the existing fallback, including its copy owner.
    code = '''
import runpy, sys
from PIL import ImageOps
def invalid_orientation(image):
    raise ValueError('controlled EXIF parsing failure')
ImageOps.exif_transpose = invalid_orientation
sys.argv = [sys.argv[1], sys.argv[2]]
runpy.run_path(sys.argv[0], run_name='__main__')
'''
    subprocess.run([sys.executable, '-I', '-c', code, str(Path(image_resize.__file__).resolve()),
                    str(tmp_path)], check=True, capture_output=True, timeout=10)
    assert (tmp_path / 'output.jpg').read_bytes() == expected


def test_encoded_byte_failure_cleans_up_and_preserves_input(tmp_path, monkeypatch):
    monkeypatch.setattr(image_resize.tempfile, 'tempdir', str(tmp_path))
    raw = source_bytes('PNG')
    original = bytes(raw)
    with pytest.raises(ValueError, match='byte budget'):
        image_resize.resize_image(raw, max_bytes=len(raw) - 1)
    with pytest.raises(ValueError, match='Isolated image resize failed'):
        image_resize.resize_image(b'corrupt image bytes')
    assert raw == original
    assert not list(tmp_path.iterdir())
