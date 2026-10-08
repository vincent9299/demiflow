"""Compressed JSON responses decode once and retain durable HTTP evidence."""
import asyncio
import gzip
import json
import zlib

import httpx
import pytest

from demiflow.operator_llm.call_ref import read_call
from demiflow.operator_llm.client import AsyncOperatorLLMClient
from demiflow.operator_llm.errors import PromptResponseContractError
from demiflow.operator_llm.model import OperatorLLMRequest, PromptModel, TextPart


@pytest.mark.parametrize('encoding', ['gzip', 'deflate'])
@pytest.mark.parametrize('status', [200, 503])
@pytest.mark.parametrize('journaled', [False, True])
def test_compressed_json_success_and_http_error(monkeypatch, tmp_path, encoding, status, journaled):
    monkeypatch.setenv('COMPRESSION_TEST_KEY', 'fixture')
    body = ({'choices': [{'message': {'content': '{"result":"中文"}'}}],
             'usage': {'prompt_tokens': 2, 'completion_tokens': 3}}
            if status == 200 else {'error': {'message': 'upstream unavailable'}})
    raw = json.dumps(body, ensure_ascii=False).encode()
    compressed = (gzip.compress if encoding == 'gzip' else zlib.compress)(raw)
    requests = []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            for offset in range(0, len(compressed), 11):
                yield compressed[offset:offset + 11]

    def handler(request):
        requests.append(request)
        return httpx.Response(status, stream=Body(), headers={
            'content-type': 'application/json', 'content-encoding': encoding,
            'content-length': str(len(compressed))})

    async def exercise():
        model = PromptModel('mock', 'openai_compatible', base_url='http://fixture/v1', api_key_env='COMPRESSION_TEST_KEY')
        options = {'sqlite_journal': {'path': str(tmp_path/'calls.sqlite'), 'max_requests': 1}} if journaled else {}
        client = AsyncOperatorLLMClient(model, options)
        client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        request = OperatorLLMRequest('test', 'v1', model.name, (TextPart('input'),))
        try:
            if status == 200:
                result = await client.execute(request)
                assert result.content == '{"result":"中文"}'
                if journaled:
                    assert read_call(result.metadata['response_ref'])['body'] == body
                    assert client.lookup(request).metadata['reused']
            else:
                expected = PromptResponseContractError if journaled else httpx.HTTPStatusError
                with pytest.raises(expected) as failure:
                    await client.execute(request)
                assert failure.value.http_status == 503
                if journaled:
                    assert read_call(failure.value.call['response_ref'])['body'] == body
                else:
                    assert failure.value.response.json() == body
        finally:
            await client.aclose()
        assert len(requests) == 1

    asyncio.run(exercise())
