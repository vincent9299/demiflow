"""IL009: public policy survives sealing, shared runtime resources do not."""
import inspect

import cloudpickle
import pytest

from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.data.flow_contract import field, scalar
from demiflow.services.adaptive_requests import AdaptiveRequestGate
from demiflow.execution.plan_facts import operation_facts
from demiflow.services.request_limits import RequestGate
from demiflow.operator_llm.flow_contract import analyze_prompt, normalize_prompt, prompt_facts
from demiflow.operator_llm.parser import parse_prompt_pack
from test_map_prompt_async import PACK
from test_prompt_http_stream import sse_server

POLICY = dict(min_concurrency=1, initial_concurrency=2, max_concurrency=2)
OPTIONS = dict(trust_env=False, timeout_s=5, max_request_bytes=1048576, max_response_bytes=1048576)
TEXT = PACK.replace('base_url_env: TEST_PROMPT_URL', 'base_url: http://127.0.0.1:1/v1')


def build(**kwargs):
    return data.from_items([{'item':'one'}]).map_prompt_async('enrich',
        config=parse_prompt_pack(TEXT), inputs={'payload':'item'}, output='answer',
        concurrency=4, queue_depth=8, max_requests=64, options=OPTIONS, **kwargs)


def test_policy_facts_match_static_contract_and_survive_serialization():
    ds = build(request_policy=POLICY)
    actor = ds._stages[-1]
    actual = operation_facts(ds._plan.operations[-1])['native']
    args = inspect.signature(Dataset.map_prompt_async).bind(None, 'enrich',
        config='prompt.yaml', inputs={'payload':'item'}, output='answer',
        concurrency=4, queue_depth=8, max_requests=64, options=OPTIONS, request_policy=POLICY)
    args.apply_defaults()
    declared = normalize_prompt({k:v for k,v in args.arguments.items() if k != 'self'})
    expected = analyze_prompt(declared, inputs=[{'item':field(scalar('str'))}],
        callables=[], resources={'config':{'text':TEXT}})['native']
    assert actual == expected
    assert actual['request_policy']['max_concurrency'] == 2
    declaration, owned, runtime_gate = cloudpickle.loads(cloudpickle.dumps(
        (actor._request_policy, actor._owned_request_gate, actor._runtime.request_gate)))
    assert dict(declaration) == actual['request_policy']
    assert owned is runtime_gate
    actor._runtime.request_gate.fresh_responses = 20
    actor._runtime.request_gate.capacity = 1
    assert prompt_facts(actor) == actual  # runtime adaptation is not plan identity
    assert 'request_policy' not in prompt_facts(build()._stages[-1])
    assert prompt_facts(build(request_policy={**POLICY,'initial_concurrency':1})._stages[-1]) != actual


@pytest.mark.parametrize('gate', [RequestGate(2), AdaptiveRequestGate(POLICY)])
def test_explicit_shared_gate_remains_rejected(gate):
    with pytest.raises(ValueError, match='configuration copies differ'):
        operation_facts(build(request_gate=gate)._plan.operations[-1])


@pytest.mark.parametrize('mutation', ['policy', 'replacement', 'path', 'failures', 'environment',
                                     'service', 'clients', 'owner', 'owners'])
def test_tampered_or_unsealed_runtime_resources_remain_rejected(mutation, tmp_path):
    actor = build(request_policy=POLICY)._stages[-1]
    runtime = actor._runtime
    if mutation == 'policy': runtime.request_gate.policy['initial_concurrency'] = 1
    elif mutation == 'replacement': runtime.request_gate = AdaptiveRequestGate(POLICY)
    elif mutation == 'path': runtime.request_gate.state_path = tmp_path/'unsealed.json'
    elif mutation == 'failures': runtime.request_gate.failures = 999
    elif mutation == 'environment': actor.environment = object()
    elif mutation == 'service': runtime._service = object()
    elif mutation == 'clients': runtime._clients = {'x':object()}
    elif mutation == 'owner': runtime._service_owner = object()
    elif mutation == 'owners': runtime._service_owners = [object()]
    with pytest.raises(ValueError, match='configuration copies differ'):
        prompt_facts(actor)


def test_caller_mutation_does_not_change_sealed_policy():
    policy = dict(POLICY)
    actor = build(request_policy=policy)._stages[-1]
    before = prompt_facts(actor)
    policy['initial_concurrency'] = 1
    assert prompt_facts(actor) == before


def test_policy_preserves_real_stream_concurrency_and_usage(sse_server, tmp_path):
    import json
    from test_map_prompt_async import prompt
    from demiflow.execution.pipeline.run_context import RunContext
    from test_prompt_http_stream import journal_totals
    ds = prompt(data.from_items([{'item':i} for i in range(6)]), concurrency=3,
        request_policy=POLICY, options={'stream':True,'gateway':'litellm','trust_env':False},
        call_output='call')
    gate = ds._stages[-1]._runtime.request_gate
    with RunContext.start(tmp_path/'run'):
        rows = ds.materialize().take_all()
    assert sorted(json.loads(r['answer']) for r in rows) == list(range(6))
    assert 1 < sse_server['peak'] <= 2 and gate.peak == 2
    assert gate.fresh_responses == 6
    assert len(set(sse_server['ports'])) <= 2
    assert all(r['call']['stream_complete'] and not r['call']['reused'] for r in rows)
    assert journal_totals(tmp_path/'run'/'run.sqlite')['input_tokens'] == 66
