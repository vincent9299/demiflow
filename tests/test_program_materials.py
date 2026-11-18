"""The engine reads selected Prompt values without starting model programs."""
from dataclasses import replace
from pathlib import Path
import json

import pytest

from demiflow import data
from demiflow.data.plan import CallableSpec
from demiflow.execution.dataflow_model_factory import CacheFactory, PromptFactory, RoutedPromptFactory
from demiflow.execution.plan_capture import capture_pipeline
from demiflow.execution.program_materials import program_prompt_materials
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.workbench.graph.plan_details import build_plan_details
from test_map_prompt_async import PACK
from test_plan_capture import no_execution, forbidden


def predicate(row):
    raise AssertionError('predicate must remain passive')


def prompt_capture(**kwargs):
    return capture_pipeline(lambda: data.from_items([{'item': 'x', 'which': 'first'}]).map_prompt_async(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer',
        max_requests=2, when=predicate, **kwargs).take_all(), {})


@pytest.mark.parametrize('routed', [False, True])
def test_exact_selected_definition_bindings_and_predicate_without_factories(no_execution, monkeypatch, routed):
    captured = prompt_capture(**({'routes': {'first': {}, 'second': {}}, 'route': 'which'} if routed else {}))
    declaration = next(d for d in captured.compiled.declarations if d.callable_spec is not None)
    for factory in (PromptFactory, CacheFactory, RoutedPromptFactory):
        monkeypatch.setattr(factory, '__new__', forbidden)
    monkeypatch.setattr(CallableSpec, 'instantiate', forbidden)
    result = program_prompt_materials(declaration)
    assert result.state == 'available' and result.occurrence_id == declaration.occurrence_id
    assert [p.name for p in result.prompts] == (['first/enrich', 'second/enrich'] if routed else ['enrich'])
    for item in result.prompts:
        assert item.inputs == {'payload': 'item'} and item.output == 'answer' and item.outputs is None
        assert item.max_requests == 2 and item.predicate is predicate
        assert item.definition.name == 'enrich'
    details = build_plan_details(captured.compiled, root=Path('/nonexistent'), sources={})
    material = details[declaration.occurrence_id]['prompt']
    assert material['availability'] == 'available' and material['referenced']
    entries = material['metadata']['routes'] if routed else [material['metadata']]
    assert [p['name'] for p in entries] == [p.name for p in result.prompts]
    assert all(p['inputs'] == {'payload': 'item'} and p['max_requests'] == 2 for p in entries)


def test_cache_wraps_the_same_prompt_and_keeps_original_binding_shape(no_execution):
    captured = prompt_capture()
    declaration = next(d for d in captured.compiled.declarations if d.callable_spec is not None)
    plain = program_prompt_materials(declaration).prompts[0]
    operation, settings = declaration.callable_spec.constructor_args
    # Material access preserves declaration values; it is not another SDK input
    # normalizer. Ordinary SDK sequences have already become maps before compile.
    unusual = replace(operation, inputs=(('payload', 'item'),), outputs=(('answer', 'result'),))
    inner = replace(declaration.callable_spec, constructor_args=(unusual, settings))
    wrapper = CallableSpec.create(CacheFactory, fn_constructor_args=(inner, '/unused', 'v1', {}, False))
    result = program_prompt_materials(replace(declaration, callable_spec=wrapper, code_identity={})).prompts[0]
    assert result.definition is plain.definition and result.predicate is plain.predicate
    assert result.inputs is unusual.inputs and result.outputs is unusual.outputs


def test_real_cached_prompt_declaration_remains_passive(tmp_path, no_execution, monkeypatch):
    source = data.from_items([{'item': 'x'}])
    prompt = source.map_prompt_async('enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'},
        output='answer', when=predicate, max_requests=2)
    cached = source.map_cached(prompt._plan.operations[-1].callable.target,
        cache_dir=str(tmp_path / 'unused-cache'), version='fixture')
    captured = capture_pipeline(cached.take_all, {})
    declaration = next(d for d in captured.compiled.declarations if d.callable_spec is not None)
    assert declaration.callable_spec.target is CacheFactory
    monkeypatch.setattr(CallableSpec, 'instantiate', forbidden)
    value = program_prompt_materials(declaration)
    assert value.state == 'available' and len(value.prompts) == 1
    assert value.prompts[0].name == 'enrich' and value.prompts[0].predicate is predicate
    assert not (tmp_path / 'unused-cache').exists()


def test_material_limits_are_honest_and_do_not_expand_all_wrappers(no_execution):
    captured = prompt_capture()
    declaration = next(d for d in captured.compiled.declarations if d.callable_spec is not None)
    original = declaration.callable_spec
    deep = original
    for _ in range(6):
        deep = CallableSpec.create(CacheFactory, fn_constructor_args=(deep,))
    many = CallableSpec.create(RoutedPromptFactory, fn_constructor_args=({str(i): original for i in range(33)},))
    # Empty native route trees still have a finite visit budget, independent of
    # the number of material leaves they happen to produce.
    tree = CallableSpec.create(RoutedPromptFactory, fn_constructor_args=({},))
    for _ in range(3):
        tree = CallableSpec.create(RoutedPromptFactory, fn_constructor_args=({str(i): tree for i in range(32)},))
    for spec in (deep, many, tree):
        changed = replace(declaration, callable_spec=spec, code_identity={})
        result = program_prompt_materials(changed)
        assert result.state == 'unavailable' and not result.prompts and '预算' in result.reason
        compiled = replace(captured.compiled, declarations=tuple(
            changed if d is declaration else d for d in captured.compiled.declarations))
        details = build_plan_details(compiled, root=Path('/nonexistent'), sources={})
        value = details[declaration.occurrence_id]['prompt']
        assert value['availability'] == 'unavailable' and value['referenced']


def test_sdk_sequence_inputs_and_multiple_outputs_keep_ui_metadata(no_execution):
    raw = json.loads(PACK) if PACK.lstrip().startswith('{') else __import__('yaml').safe_load(PACK)
    prompt = raw['prompts']['enrich']
    prompt['template'] = '{{ payload | json }}'
    prompt['response_schema'] = {'type': 'object', 'properties': {'a': {'type': 'string'}, 'b': {'type': 'string'}},
        'required': ['a', 'b'], 'additionalProperties': False}
    captured = capture_pipeline(lambda: data.from_items([{'payload': 'x'}]).map_prompt(
        'enrich', config=parse_prompt_pack(json.dumps(raw)), inputs=['payload'],
        outputs={'a': 'left', 'b': 'right'}).take_all(), {})
    declaration = next(d for d in captured.compiled.declarations if d.callable_spec is not None)
    item = program_prompt_materials(declaration).prompts[0]
    assert item.inputs == {'payload': 'payload'} and item.outputs == {'a': 'left', 'b': 'right'}
    details = build_plan_details(captured.compiled, root=Path('/nonexistent'), sources={})
    metadata = details[declaration.occurrence_id]['prompt']['metadata']
    assert metadata['outputs'] == {'a': 'left', 'b': 'right'} and metadata['output'] is None
    assert metadata['definition']['output_schema'] == prompt['response_schema']
