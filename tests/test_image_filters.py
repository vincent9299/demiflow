"""Generic content quality checks run after raw acquisition."""
import io
import pytest
from PIL import Image
from demiflow.collect.image_filters import ImageFilters,ImageHeaderProbe,header_dimensions,declared_dimensions,BodyRejected

def test_filter_config_validation_and_boundaries():
    assert not ImageFilters().enabled
    for bad in (0, -1, True, '1024', 1.5, 2**63):
        with pytest.raises(ValueError):
            ImageFilters(min_long_side=bad)
    with pytest.raises(TypeError):
        ImageFilters(unknown_condition=100)
    policy = ImageFilters(min_width=6, min_height=6, min_long_side=8, min_short_side=6, min_pixels=48)
    assert policy.rejection(8, 6) is None
    assert policy.rejection(6, 8) is None
    assert policy.rejection(8, 5).startswith('min_height:')
    assert ImageFilters(min_long_side=8).rejection(7, 7).startswith('min_long_side:')
    assert ImageFilters(min_short_side=6).rejection(8, 5).startswith('min_short_side:')
    assert ImageFilters(min_pixels=49).rejection(8, 6).startswith('min_pixels:')


@pytest.mark.parametrize('fmt,options', [
    ('PNG', {}), ('JPEG', {}), ('JPEG', {'progressive': True}), ('GIF', {}),
    ('BMP', {}), ('WEBP', {}), ('WEBP', {'lossless': True}),
])
def test_headers_agree_with_decoded_sizes(fmt, options):
    stream = io.BytesIO()
    Image.new('RGB', (257, 113), 'red').save(stream, format=fmt, **options)
    raw = stream.getvalue()
    assert header_dimensions(raw[:128*1024]) == (257, 113)
    # Every short prefix must remain safe; ambiguous input defers to decode.
    for length in range(min(len(raw), 100)):
        assert header_dimensions(raw[:length]) in (None, (257, 113))


def test_header_probe_memory_is_bounded_and_unknown_defers():
    probe = ImageHeaderProbe(ImageFilters(min_long_side=1024))
    for _ in range(32):
        probe.feed(b'not an image' * 1024)
        assert len(probe.prefix) <= probe.max_bytes
    assert probe.done and not probe.prefix


def test_content_rejection_and_declared_dimensions_are_independent_of_transport():
    out=io.BytesIO();Image.new('RGB',(8,6),'red').save(out,format='PNG')
    with pytest.raises(BodyRejected) as rejected:ImageHeaderProbe(ImageFilters(min_width=16)).feed(out.getvalue())
    assert rejected.value.metadata=={'width':8,'height':6,'filter_stage':'header'}
    assert declared_dimensions({'declared_width':8,'declared_height':6})==(8,6)
    for request in ({'declared_width':8},{'declared_width':True,'declared_height':6}):
        with pytest.raises(ValueError):declared_dimensions(request)
