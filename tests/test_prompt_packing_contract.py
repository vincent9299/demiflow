import copy
from pathlib import Path

import pytest
import yaml

from demiflow import data
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.checked import CheckedExecution
from demiflow.execution.local_kernel import local_execution
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules, LIMITS
from demiflow.workbench.pipeline_policy import DEFAULT_LIMITS
from demiflow.workbench.projects.workspace import resolve_flow_resources
from demiflow.operator_llm.packing import PackPromptInputs
from demiflow.operator_llm.parser import parse_prompt_pack
from test_operator_foundation_models import load
from test_prompt_systemone_questions import DEFINITION


def definition():
    value = copy.deepcopy(DEFINITION)
    model = value['prompts']['match']['model']
    model.pop('base_url_env')
    model['base_url'] = 'http://127.0.0.1:1/v1'
    return yaml.safe_dump(value)


def arguments(**changes):
    return {**dict(prompt='match', config='flows/prompt.yaml', inputs={'candidates': 'items', 'image': 'picture'},
        item_argument='candidates', output='ranges', max_items=2, max_input_items=10000,
        max_input_chars=1000, max_request_bytes=65536, max_expansion_bytes=131072,
        max_measure_bytes=16 * 1024**2, concurrency=1, queue_depth=1, label='pack_prompt_inputs'), **changes}


def row():
    return {'picture': 'data:image/png;base64,aW1hZ2U=',
        'items': [{'id': str(i), 'concept': 'item-' + str(i)} for i in range(5)]}


def test_packing_preserves_order_coverage_and_single_oversized_item():
    pack = parse_prompt_pack(definition())
    actor = PackPromptInputs(pack, **arguments())
    value = row()
    before = copy.deepcopy(value)
    result = actor(value)['ranges']
    assert result == [{'start': 0, 'stop': 2, 'error': None}, {'start': 2, 'stop': 4, 'error': None}, {'start': 4, 'stop': 5, 'error': None}]
    assert value == before
    value['items'][2]['concept'] = 'x' * 2000
    result = actor(value)['ranges']
    assert result == [{'start': 0, 'stop': 2, 'error': None}, {'start': 2, 'stop': 3, 'error': 'input_budget_exceeded'}, {'start': 3, 'stop': 5, 'error': None}]
    assert actor({**value, 'items': []})['ranges'] == []
    assert actor({'picture': None, 'items': []})['ranges'] == []
    with pytest.raises(ValueError, match='image bytes or data URLs'):
        actor({**row(), 'picture': None})
    with pytest.raises(ValueError, match='work budget'):
        PackPromptInputs(pack, **arguments(max_measure_bytes=1))(row())
    with pytest.raises(ValueError, match='item count'):
        PackPromptInputs(pack, **arguments(max_input_items=2))(row())


@pytest.mark.parametrize('mutation', [False, True])
def test_packing_is_checked_against_actual_native_actor(tmp_path, monkeypatch, mutation):
    (tmp_path / 'flows').mkdir()
    (tmp_path / 'operators').mkdir()
    (tmp_path / 'flows/prompt.yaml').write_text(definition())
    a = arguments()
    prompt = a.pop('prompt')
    source = 'from demiflow import data\ndef run():\n    return data.from_items(' + repr([row()]) + ').pack_prompt_inputs(' + repr(prompt) + ', '
    source += ', '.join(k + '=' + repr(v) for k, v in a.items()) + ').materialize()\n'
    (tmp_path / 'flows/pack.py').write_text(source)
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    report = FlowChecker(tmp_path, catalog, {}, [], resource_resolver=lambda tree, refs, bindings:
        resolve_flow_resources(tmp_path, tree, refs, bindings)).inspect('flows/pack.py:run', {})
    assert report['status'] == 'checked', report['diagnostics']
    flow = load(monkeypatch, 'flows.pack', tmp_path / 'flows/pack.py')
    if mutation:
        original = data.Dataset.pack_prompt_inputs
        def changed(self, *args, **kwargs):
            value = original(self, *args, **kwargs)
            value._stages[-1].arguments['max_items'] = 1
            return value
        monkeypatch.setattr(data.Dataset, 'pack_prompt_inputs', changed)
    context = CheckedExecution(nodes=report['nodes'], edges=report['edges'], actions=report['actions'],
        operators=report['operators'], source_root=tmp_path, limits=DEFAULT_LIMITS,
        record=lambda *a: None, check_stop=lambda: None)
    with local_execution(workers=1, worker_mode='thread', partitions=1, memory_bytes=128 * 1024**2) as session:
        session._executor.resource_root = tmp_path
        if mutation:
            with pytest.raises(RuntimeContractError, match='differs'):
                with context.bind_context():
                    flow.run()
        else:
            with context.bind_context():
                result = flow.run()
            assert len(result.take_all()[0]['ranges']) == 3
