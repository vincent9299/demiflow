import io

import pytest
from PIL import Image

from demiflow.collect.image_resize import resize_image


def encoded(size=(32, 20), fmt='PNG'):
    output = io.BytesIO()
    image = Image.new('RGBA', size, (255, 0, 0, 0))
    image.paste((255, 0, 0, 255), (0, 0, size[0] // 2, size[1]))
    exif = Image.Exif()
    exif[274] = 6
    image.save(output, format=fmt, exif=exif)
    return output.getvalue()


def test_large_png_resizes_in_worker_with_orientation_and_white_transparency():
    raw = encoded((6000, 4200))  # 25.2M pixels, above the caller's 24M direct decode limit.
    result = resize_image(raw)
    with Image.open(io.BytesIO(result)) as image:
        assert image.size == (1075, 1536)
        assert image.mode == 'RGB'
        assert image.getpixel((500, 200))[0] > 245
        assert image.getpixel((500, 200))[1] < 10
        assert min(image.getpixel((500, 1300))) > 245


def test_source_pixel_limit_and_corrupt_image_are_explicit_failures():
    with pytest.raises(ValueError, match='source pixel limit'):
        resize_image(encoded((32, 20)), max_source_pixels=500)
    with pytest.raises(ValueError, match='Isolated image resize failed'):
        resize_image(b'not an image')


def test_timeout_releases_worker_slot_and_preserves_caller():
    with pytest.raises(ValueError, match='time|budget'):
        resize_image(encoded(), timeout_s=.001)
    with Image.open(io.BytesIO(resize_image(encoded(), max_side=16))) as image:
        assert image.size == (10, 16)


@pytest.mark.parametrize('options', [{'max_side':0}, {'memory_bytes':128}, {'timeout_s':float('inf')},
                                     {'max_source_pixels':160_000_001}])
def test_limits_are_checked_before_decoding(options):
    with pytest.raises(ValueError, match='Invalid resize'):
        resize_image(encoded(), **options)
