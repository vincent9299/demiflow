import hashlib
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


def test_material_bounds_and_credential_fields():
    value = material('汉' * 30000, language='text')
    assert value['truncated'] and len(value['text'].encode()) <= 65536
    result = safe_json({'model': {'api_key_env': 'KEY_NAME', 'api_key': 'never-visible',
        'base_url': 'https://user:password@example.org/v1?token=secret'}, 'headers': {'Authorization': 'secret'}})
    assert result['model']['api_key_env'] == 'KEY_NAME'
    assert 'never-visible' not in str(result) and 'password' not in str(result) and 'secret' not in str(result)
    assert result['model']['base_url'] == 'https://example.org/v1'
