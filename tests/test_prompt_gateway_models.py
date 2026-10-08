"""Gateway discovery accepts membership; direct endpoints remain exact-match."""
import asyncio
import json
import httpx
import pytest

from demiflow.operator_llm.client import AsyncOperatorLLMClient
from demiflow.operator_llm.model import PromptModel, OperatorLLMRequest, TextPart


@pytest.mark.parametrize('verification,names,accepted', [
    ('listed', ['other', 'selected'], True),
    ('listed', ['other'], False),
    (True, ['other', 'selected'], False),
    (True, ['selected'], True),
])
def test_model_discovery_precedes_generation(monkeypatch, verification, names, accepted):
    monkeypatch.setenv('TEST_GATEWAY_KEY', 'fixture')
    calls = []
    def handler(request):
        calls.append(request.method)
        if request.method == 'GET':
            return httpx.Response(200, json={'data': [{'id': n} for n in names]})
        return httpx.Response(200, json={'choices': [{'message': {'content': '{}'}, 'finish_reason': 'stop'}]})
    async def exercise():
        client = AsyncOperatorLLMClient(PromptModel('selected', 'openai_compatible',
            base_url='http://gateway.invalid/v1', api_key_env='TEST_GATEWAY_KEY'),
            {'verify_model': verification})
        await client.client.aclose()
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            request = OperatorLLMRequest('test', 'v1', 'selected', (TextPart('fixture'),))
            if accepted:
                await client.execute(request)
            else:
                with pytest.raises(ValueError, match='Endpoint model differs'):
                    await client.execute(request)
        finally:
            await client.aclose()
    asyncio.run(exercise())
    assert calls == (['GET', 'POST'] if accepted else ['GET'])


def test_gateway_credentials_are_wire_only_and_rotation_reuses_journal(monkeypatch, tmp_path):
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    monkeypatch.setenv('TEST_GATEWAY_KEY', 'local-client-placeholder')
    monkeypatch.setenv('TEST_PROVIDER_CREDENTIALS', json.dumps({
        'api_key': 'provider-secret-one',
        'extra_headers': {'x-openai-actor-authorization': 'local-image-extension'}}))
    sent = []
    def handler(request):
        sent.append(json.loads(request.content))
        return httpx.Response(200, json={'choices': [{'message': {'content': '{}'}, 'finish_reason': 'stop'}]})
    journal_path = tmp_path / 'calls.sqlite'
    async def exercise():
        model = PromptModel('selected', 'openai_compatible', base_url='http://gateway.invalid/v1',
                            api_key_env='TEST_GATEWAY_KEY')
        request = OperatorLLMRequest('test', 'v1', 'selected', (TextPart('fixture'),))
        options = {'gateway': 'litellm', 'gateway_credentials_env': 'TEST_PROVIDER_CREDENTIALS',
                   'sqlite_journal': {'path': str(journal_path), 'max_requests': 1}}
        records = []
        for secret in ('provider-secret-one', 'provider-secret-two'):
            credentials = json.loads(__import__('os').environ['TEST_PROVIDER_CREDENTIALS'])
            credentials['api_key'] = secret
            monkeypatch.setenv('TEST_PROVIDER_CREDENTIALS', json.dumps(credentials))
            client = AsyncOperatorLLMClient(model, options)
            client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                records.append(client.record(request))
                await client.execute(request)
            finally:
                await client.aclose()
        assert records[0] == records[1]
        assert 'api_key' not in records[0]['payload']
    asyncio.run(exercise())
    assert len(sent) == 1
    assert sent[0]['api_key'] == 'provider-secret-one'
    assert sent[0]['extra_headers']['x-openai-actor-authorization'] == 'local-image-extension'
    journal = SQLitePromptJournal(journal_path, read_only=True)
    try:
        saved = json.dumps(journal.call_page())
        assert 'provider-secret' not in saved and 'extra_headers' not in saved
    finally:
        journal.close()


@pytest.mark.parametrize('value', ['not-json', '{}', '{"api_key":"x","model":"other"}',
                                    '{"api_key":"x","extra_headers":{"bad":"a\\nb"}}', 'x' * 16385])
def test_invalid_gateway_credentials_fail_before_reserving(monkeypatch, tmp_path, value):
    monkeypatch.setenv('TEST_PROVIDER_CREDENTIALS', value)
    with pytest.raises(ValueError, match='TEST_PROVIDER_CREDENTIALS'):
        AsyncOperatorLLMClient(PromptModel('selected', 'openai_compatible',
            base_url='http://gateway.invalid/v1', api_key_env='TEST_GATEWAY_KEY'),
            {'gateway':'litellm', 'gateway_credentials_env':'TEST_PROVIDER_CREDENTIALS',
             'sqlite_journal':{'path':str(tmp_path/'calls.sqlite')}})
    assert not (tmp_path/'calls.sqlite').exists()
