"""Numbered multimodal inputs preserve image identity through HTTP rendering."""
import pytest
from demiflow.operator_llm import compile_template, render_template, OperatorLLMRequest
from demiflow.operator_llm.client import _request_content
from demiflow.operator_llm.errors import PromptArgumentTypeError


def test_numbered_images_are_labeled_at_their_actual_position():
    urls = ['data:image/png;base64,AAAA', 'data:image/jpeg;base64,BBBB']
    template = compile_template('Read {{ pictures | numbered_image }} then answer.')
    parts = render_template(template, {'pictures': urls})
    content = _request_content(OperatorLLMRequest('review', 'v1', 'mock', parts))
    assert content == [
        {'type': 'text', 'text': 'Read '},
        {'type': 'text', 'text': '\nImage 1:\n'},
        {'type': 'image_url', 'image_url': {'url': urls[0]}},
        {'type': 'text', 'text': '\nImage 2:\n'},
        {'type': 'image_url', 'image_url': {'url': urls[1]}},
        {'type': 'text', 'text': ' then answer.'},
    ]
    plain = render_template(compile_template('{{ pictures | image }}'), {'pictures': urls})
    assert _request_content(OperatorLLMRequest('review', 'v1', 'mock', plain)) == [content[2], content[4]]


def test_numbered_image_empty_single_and_invalid_input():
    template = compile_template('Read {{ pictures | numbered_image }}done')
    assert _request_content(OperatorLLMRequest('r', '1', 'm', render_template(template, {'pictures': []}))) == 'Read done'
    parts = render_template(template, {'pictures': 'https://example.invalid/image.png'})
    content = _request_content(OperatorLLMRequest('r', '1', 'm', parts))
    assert content[1]['text'] == '\nImage 1:\n'
    assert content[2]['image_url']['url'].endswith('/image.png')
    with pytest.raises(PromptArgumentTypeError):
        render_template(template, {'pictures': ['https://example.invalid/ok.png', None]})
