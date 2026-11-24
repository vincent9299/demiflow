"""Provider and response failures of a prompt node reach the caller with their own type."""
import json

import pytest

from demiflow.data.api import DataAPI
from demiflow.execution.contracts.dataflow_failures import failure_payload, restore_failure
from demiflow.operator_llm.errors import (
    PromptProviderCapabilityError, PromptProviderUnavailableError, PromptResponseContractError,
    PromptResponseParseError, PromptStreamError)
from demiflow.operator_llm.parser import parse_prompt_pack
from test_map_prompt_async import PACK, server


@pytest.mark.parametrize('kind', [
    PromptProviderUnavailableError, PromptProviderCapabilityError, PromptResponseParseError,
    PromptResponseContractError, PromptStreamError])
def test_prompt_failure_type_and_message_survive_the_worker_boundary(kind):
    try:
        raise kind('provider detail')
    except kind as error:
        payload = json.loads(json.dumps(failure_payload(error)))
    restored = restore_failure(payload)
    assert type(restored) is kind and str(restored) == 'provider detail'


def prompt_node(api, method):
    kwargs = {'concurrency': 1} if method == 'map_prompt_async' else {}
    return getattr(api.from_items([{'item': 1}]), method)(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer', **kwargs)


@pytest.mark.parametrize('method', ['map_prompt', 'map_prompt_async'])
def test_provider_http_error_reaches_the_caller_as_a_prompt_error_without_retry(server, method):
    server['status'] = 503
    api = DataAPI()
    with pytest.raises(PromptResponseContractError, match='HTTP 503; full response saved'):
        prompt_node(api, method).take_all()
    assert len(server['requests']) == 1
    assert api.prompt_usage()['provider_requests_failed'] == 1


@pytest.mark.parametrize('method', ['map_prompt', 'map_prompt_async'])
def test_missing_provider_environment_reaches_the_caller_as_provider_unavailable(server, monkeypatch, method):
    monkeypatch.delenv('TEST_PROMPT_URL')
    with pytest.raises(PromptProviderUnavailableError, match='TEST_PROMPT_URL'):
        prompt_node(DataAPI(), method).take_all()
    assert not server['requests']
