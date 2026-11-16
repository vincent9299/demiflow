"""Fixed embedding failures cross workers without remote imports or secrets."""
import json

import httpx
import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_failures import failure_payload, restore_failure
from demiflow.embeddings.payload import EmbeddingProtocolError
from demiflow.operator_llm.journal import UncertainPromptCall


def test_http_status_keeps_type_status_method_and_only_public_url():
    request = httpx.Request('POST', 'https://user:password@example.test/v1/embeddings?api_key=secret#private',
                            headers={'Authorization': 'Bearer header-secret'})
    original = httpx.HTTPStatusError('raw-body-secret', request=request,
                                    response=httpx.Response(503, request=request))
    try:
        raise original
    except httpx.HTTPStatusError as error:
        payload = failure_payload(error)
    raw = json.dumps(payload)
    for secret in ('password', 'api_key', 'header-secret', 'raw-body-secret', '#private', 'user:'):
        assert secret not in raw
    restored = restore_failure({**payload, 'message': 'injected-secret', 'traceback': 'injected-secret'})
    assert type(restored) is httpx.HTTPStatusError
    assert restored.request.method == 'POST' and restored.response.status_code == 503
    assert str(restored.request.url) == 'https://example.test/v1/embeddings'
    assert 'injected-secret' not in str(restored) and not getattr(restored, '__notes__', ())
    try:
        raise ExceptionGroup('raw-body-secret', [original])
    except ExceptionGroup as group:
        grouped = failure_payload(group)
    assert 'raw-body-secret' not in json.dumps(grouped)
    assert 'password' not in json.dumps(grouped)


@pytest.mark.parametrize('change', [
    {'status': True}, {'status': 999}, {'method': 'POST\r\nsecret'},
    {'url': 'https://user:secret@example.test/path'}, {'url': 'https://example.test/?key=secret'},
    {'url': 'file:///tmp/private'}, {'url': 'https://example.test/#secret'}, {'extra': 'field'},
])
def test_malformed_http_payload_never_rebuilds_a_request(change):
    fields = {'status': 503, 'method': 'POST', 'url': 'https://example.test/path', **change}
    with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
        restore_failure({'error_type': 'httpx.HTTPStatusError', 'http': fields, 'message': 'x'})


def test_uncertain_and_protocol_types_preserve_semantics_without_dynamic_import():
    for original in (UncertainPromptCall('pending intent'), EmbeddingProtocolError('wrong vector dimension')):
        payload = failure_payload(original)
        restored = restore_failure(payload)
        assert type(restored) is type(original) and str(restored) == str(original)
        if isinstance(original, UncertainPromptCall):
            assert payload['code'] == 'UNKNOWN_EFFECT'
            with pytest.raises(DataflowError, match='UNKNOWN_EFFECT'):
                restore_failure({**payload, 'code': 'CAPACITY_PENDING'})
    unknown = restore_failure({'error_type': 'arbitrary_business.module.Class', 'message': 'unknown',
                               'code': 'UNKNOWN_EFFECT'})
    assert type(unknown) is DataflowError and unknown.code == 'UNKNOWN_EFFECT'
