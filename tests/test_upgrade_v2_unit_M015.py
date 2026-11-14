"""M015: validate_http_options rejects recorded credentials unconditionally."""
import pytest
from demiflow.operator_llm.http_options import validate_http_options, gateway_credentials


def test_recorded_api_key_rejected_without_credentials_env():
    with pytest.raises(ValueError, match='Gateway credentials must not be placed in recorded request_options'):
        validate_http_options({'request_options': {'api_key': 'sk-secret-value'}})


def test_recorded_extra_headers_rejected_without_credentials_env():
    with pytest.raises(ValueError, match='Gateway credentials must not be placed in recorded request_options'):
        validate_http_options({'request_options': {'extra_headers': {'Authorization': 'Bearer secret'}}})


@pytest.mark.parametrize('secret', ['api_key', 'extra_headers'])
def test_recorded_credentials_rejected_even_with_credentials_env(secret):
    options = {'gateway': 'litellm', 'gateway_credentials_env': 'TEST_PROVIDER_CREDENTIALS',
               'request_options': {secret: 'x' if secret == 'api_key' else {'a': 'b'}}}
    with pytest.raises(ValueError, match='Gateway credentials must not be placed in recorded request_options'):
        validate_http_options(options)


def test_rejection_does_not_echo_secret_value():
    with pytest.raises(ValueError) as caught:
        validate_http_options({'request_options': {'api_key': 'sk-do-not-leak-12345'}})
    assert 'sk-do-not-leak-12345' not in str(caught.value)


def test_legal_non_secret_request_options_preserved():
    options = validate_http_options({'request_options': {'temperature': 0.5, 'n': 1}})
    assert options['request_options']['temperature'] == 0.5


def test_transport_only_gateway_credentials_still_load(monkeypatch):
    import json
    monkeypatch.setenv('TEST_PROVIDER_CREDENTIALS', json.dumps(
        {'api_key': 'provider-secret', 'extra_headers': {'x-actor': 'extension'}}))
    value = gateway_credentials({'gateway_credentials_env': 'TEST_PROVIDER_CREDENTIALS'})
    assert value['api_key'] == 'provider-secret'
    options = validate_http_options({'gateway': 'litellm', 'gateway_credentials_env': 'TEST_PROVIDER_CREDENTIALS'})
    assert 'api_key' not in options.get('request_options', {})