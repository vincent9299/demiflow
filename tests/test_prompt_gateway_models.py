"""Gateway discovery accepts membership; direct endpoints remain exact-match."""
import asyncio
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
