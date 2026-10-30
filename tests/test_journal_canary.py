"""The canary must not count a non-2xx or malformed body as a model answer."""
import importlib.util
from pathlib import Path


def _canary():
    path = Path(__file__).resolve().parents[1] / 'benchmarks' / 'journal_canary.py'
    spec = importlib.util.spec_from_file_location('journal_canary', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_valid_completion_requires_the_declared_contract():
    valid_completion = _canary().valid_completion
    assert valid_completion({'choices': [{'message': {'content': 'ok'}}]}) is None
    assert valid_completion({'choices': [{'message': {'content': ''}}]}) is None
    assert valid_completion({'error': {'message': 'HTTP 400'}}) == 'missing choices'
    assert valid_completion({'choices': []}) == 'missing choices'
    assert valid_completion({'choices': 'no'}) == 'missing choices'
    assert valid_completion({'choices': [{'message': {'content': None}}]}) == 'missing message content'
    assert valid_completion({'choices': [{'message': 'no'}]}) == 'missing message content'
    assert valid_completion([]) == 'missing choices'
