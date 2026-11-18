import hashlib
import json
from pathlib import Path

from demiflow import data
from demiflow.execution.plan_capture import capture_pipeline
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.workbench.graph.plan_details import build_plan_details, material, safe_json
from test_map_prompt_async import PACK


def meaningful_transform(row):
    """This is the exact implementation, not an inferred flow call."""
    raise AssertionError('A material read must not execute user rows')


def test_function_material_is_the_compiled_function_and_file_digest():
    root = Path(__file__).parent
    digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    compiled = capture_pipeline(lambda: data.from_items([{'item': 'x'}]).map(meaningful_transform).take_all(), {}).compiled
    details = build_plan_details(compiled, sources={Path(__file__).name: digest}, root=root)
    node = next(n for n in compiled.graph.nodes if n.kernel.kind == 'MapOp')
    value = details[node.occurrence_id]['implementation']
    assert value['availability'] == 'available' and value['sha256'] == digest
    assert value['text'].startswith('def meaningful_transform(row):')
    assert 'exact implementation' in value['text'] and 'def test_' not in value['text']
    assert value['metadata']['definition']['signature'] == 'meaningful_transform(row)'
    assert value['metadata']['definition']['returns'] is None
    stale = build_plan_details(compiled, sources={Path(__file__).name: '0' * 64}, root=root)
    assert stale[node.occurrence_id]['implementation']['availability'] == 'unavailable'
    assert details[node.occurrence_id]['prompt']['availability'] == 'unavailable'


def test_prompt_material_is_selected_compiled_template_not_reopened_path():
    pack = parse_prompt_pack(PACK)
    compiled = capture_pipeline(lambda: data.from_items([{'item': 'x'}]).map_prompt_async(
        'enrich', config=pack, inputs={'payload': 'item'}, output='answer', max_requests=1).take_all(), {}).compiled
    details = build_plan_details(compiled, sources={}, root=Path('/nonexistent'))
    node = next(n for n in compiled.graph.nodes if n.kernel.kind == 'prompt')
    value = details[node.occurrence_id]['prompt']
    definition = pack.prompt_definitions['enrich']
    assert value['text'] == definition.template.source
    assert value['sha256'] == hashlib.sha256(definition.template.source.encode()).hexdigest()
    assert value['metadata']['name'] == 'enrich' and value['metadata']['version'] == definition.version
    assert value['metadata']['inputs'] == {'payload': 'item'}
    assert value['metadata']['max_requests'] == 1
    assert value['source_basis'] == 'compiled_snapshot'
    contract = value['metadata']['definition']
    assert contract['input_variables'] == [{'name': 'payload', 'type': 'json', 'required': True}]
    assert contract['output_schema'] == dict(definition.response_schema)
    assert contract['template'] == definition.template.source
    assert 'inputs' not in contract  # Dataset field bindings belong to the call.


def prompt_condition(row) -> bool:
    raise AssertionError('Preview must not call a predicate')


def test_prompt_node_retains_its_explicit_business_predicate_definition():
    pack = parse_prompt_pack(PACK)
    compiled = capture_pipeline(lambda: data.from_items([{'item': 'x'}]).map_prompt_async(
        'enrich', config=pack, inputs={'payload': 'item'}, output='answer', when=prompt_condition,
        max_requests=1).take_all(), {}).compiled
    path = Path(__file__)
    details = build_plan_details(compiled, sources={path.name:hashlib.sha256(path.read_bytes()).hexdigest()}, root=path.parent)
    node = next(n for n in compiled.graph.nodes if n.kernel.kind == 'prompt')
    code = details[node.occurrence_id]['implementation']
    assert code['availability'] == 'available'
    assert code['metadata']['definition']['returns'] == 'bool'
    assert 'def prompt_condition(row)' in code['text']
    assert details[node.occurrence_id]['prompt']['metadata']['definition']['symbol'] == 'enrich'


def test_material_bounds_and_credential_fields():
    value = material('汉' * 30000, language='text')
    assert value['truncated'] and len(value['text'].encode()) <= 65536
    result = safe_json({'model': {'api_key_env': 'KEY_NAME', 'api_key': 'never-visible',
        'base_url': 'https://user:password@example.org/v1?token=secret'}, 'headers': {'Authorization': 'secret'}})
    assert result['model']['api_key_env'] == 'KEY_NAME'
    assert 'never-visible' not in str(result) and 'password' not in str(result) and 'secret' not in str(result)
    assert result['model']['base_url'] == 'https://example.org/v1'
    source = safe_json({'uri': 'https://user:hidden@example.org/table?access=hidden',
                       'storage_options': {'aws_access_key_id': 'hidden'}, 'secret_key': 'hidden'})
    assert 'hidden' not in str(source)
    assert source['uri'] == 'https://example.org/table'
    assert safe_json({'paths': ['https://user:hidden@example.org/data?sig=hidden#hidden']}) == {
        'paths': ['https://example.org/data']}


def test_legal_deep_prompt_schema_degrades_material_without_discarding_graph():
    schema = {'type': 'string'}
    for _ in range(8):
        schema = {'type': 'object', 'properties': {'item': schema}, 'required': ['item'], 'additionalProperties': False}
    pack = parse_prompt_pack(json.dumps({'schema_version': 'demiflow_prompt_pack_v2', 'prompts': {'p': {
        'version': '1', 'model': {'name': 'fixture', 'transport': 'openai_compatible',
            'base_url': 'http://127.0.0.1:1/v1', 'api_key_env': 'UNUSED_FIXTURE_KEY'},
        'template': '{{ value }}', 'response_schema': {'type': 'object', 'properties': {'result': schema},
            'required': ['result'], 'additionalProperties': False}}}}))
    compiled = capture_pipeline(lambda: data.from_items([{'value': 'x'}]).map_prompt_async(
        'p', config=pack, inputs=['value'], output='answer', max_requests=1).take_all(), {}).compiled
    details = build_plan_details(compiled, sources={}, root=Path('/nonexistent'))
    assert len(details) == len(compiled.graph.nodes) == 3
    prompt = next(n for n in compiled.graph.nodes if n.kernel.kind == 'prompt')
    assert details[prompt.occurrence_id]['prompt']['availability'] == 'unavailable'
    assert '预算' in details[prompt.occurrence_id]['prompt']['reason']
    assert details[prompt.occurrence_id]['prompt']['referenced']


class PassiveActor:
    def __init__(self):
        raise AssertionError('material reads must not construct an actor')

    async def __call__(self, row: dict) -> dict:
        raise AssertionError('material reads must not execute a row')


def test_actor_material_comes_from_bound_class_without_construction():
    path = Path(__file__)
    compiled = capture_pipeline(lambda: data.from_items([{'item': 'x'}]).map_async(PassiveActor).take_all(), {}).compiled
    details = build_plan_details(compiled, sources={path.name: hashlib.sha256(path.read_bytes()).hexdigest()}, root=path.parent)
    item = next(v['implementation'] for v in details.values() if v['implementation'].get('referenced'))
    assert item['availability'] == 'available', item
    assert item['text'].startswith('class PassiveActor:')
    assert item['metadata']['definition']['signature'] == 'PassiveActor(row: dict) -> dict'
    assert item['metadata']['definition']['parameters'][0]['name'] == 'row'
