"""Image admission rejects network/oversized payloads before processor work."""
import base64
import io
import threading
from types import SimpleNamespace

import pytest
from PIL import Image

from demiflow.operator_llm.image_tokens import HuggingFaceImageTokenCounter


def fixture_counter():
    counter = HuggingFaceImageTokenCounter.__new__(HuggingFaceImageTokenCounter)
    counter.max_images = 1
    counter.max_image_bytes = 1024
    counter.max_image_pixels = 4096
    counter.template_kwargs = {}
    counter._lock = threading.Lock()
    return counter


def picture(width=32, height=32):
    buf = io.BytesIO()
    Image.new('RGB', (width, height)).save(buf, format='PNG')
    return 'data:image/png;base64,' + base64.b64encode(buf.getvalue()).decode()


def message(url):
    return [{'role': 'user', 'content': [{'type': 'text', 'text': 'test'},
                                      {'type': 'image_url', 'image_url': {'url': url}}]}]


def test_processor_expanded_count_and_unchanged_request():
    counter = fixture_counter()
    seen = []
    def process(messages, **kwargs):
        seen.append(messages[0]['content'][1]['image'].size)
        return {'input_ids': [[0]*321], 'pixel_values': []}
    counter.processor = SimpleNamespace(apply_chat_template=process)
    messages = message(picture())
    assert counter.messages(messages) == 321
    assert seen == [(32, 32)]
    assert messages[0]['content'][1]['type'] == 'image_url'


@pytest.mark.parametrize('url, error', [
    ('https://fixture.invalid/image.png', 'inline'),
    ('data:image/png;base64,' + 'a'*1600, 'byte'),
    (picture(100, 100), 'pixel'),
])
def test_limits_precede_processor(url, error):
    counter = fixture_counter()
    # No processor installed: any accidental preprocessing would fail this test.
    with pytest.raises(ValueError, match=error):
        counter.messages(message(url))


def test_count_limit_precedes_second_decode():
    counter = fixture_counter()
    messages = message(picture())
    messages[0]['content'].append({'type': 'image_url', 'image_url': {'url': 'invalid'}})
    with pytest.raises(ValueError, match='count'):
        counter.messages(messages)
