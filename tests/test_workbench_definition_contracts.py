"""Definitions are passive source projections, distinct from flow bindings."""
import ast

from demiflow.workbench.definition_contracts import code_definition, prompt_definition
from demiflow.workbench.graph.api_definition import from_source
from demiflow.workbench.graph.source_view import _public_prompt


def test_definition_does_not_evaluate_annotations_or_dynamic_defaults():
    node = ast.parse('def clean(row: dangerous(), limit=explode(), *, mode="safe") -> dict: pass').body[0]
    value = code_definition(node, 'exact source')
    assert value['parameters'][0]['annotation'] == 'dangerous()'
    assert value['parameters'][1]['default_known'] is False
    assert value['parameters'][1]['default_expression'] == 'explode()'
    assert value['parameters'][2]['default'] == 'safe'
    assert value['returns'] == 'dict' and value['source'] == 'exact source'


def test_api_definition_uses_given_version_and_omits_receiver():
    raw = b'class Dataset:\n def map(self, fn, *, batch_size=7) -> Dataset:\n  """Bound version."""\n  pass\n'
    value = from_source('Dataset.map', raw)
    assert value['signature'] == 'Dataset.map(fn, *, batch_size=7) -> Dataset'
    assert value['parameters'][1]['default'] == 7
    assert 'Bound version.' == value['description']
    assert from_source('other.module:Dataset.map', raw) is None
    assert from_source('Dataset.filter', raw) is None


def test_historical_prompt_preserves_explicit_contract_without_credentials():
    raw = b'''schema_version: demiflow_prompt_pack_v2
prompts:
  review:
    version: v2
    input_mode: messages
    schema_retries: 2
    response_format: json
    message_limits: {max_messages: 8}
    model: {name: model-a, api_key: never-visible}
    response_schema: {type: object, properties: {answer: {type: string}}}
'''
    item = _public_prompt(raw)['prompts']['review']
    value = prompt_definition('review', item)
    assert value['input_variables'] == [{'name': 'messages', 'type': 'json', 'required': True}]
    assert value['defaults'] == {'schema_retries': 2, 'input_mode': 'messages', 'message_limits': {'max_messages': 8}}
    assert value['output_schema']['properties']['answer'] == {'type': 'string'}
    assert 'never-visible' not in str(value)
