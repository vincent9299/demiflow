"""Generic SystemOne contracts use the same native renderer/client and facts."""
import copy
import os
from pathlib import Path

import lance
import pytest
import yaml

from demiflow import data
from demiflow.execution.checked import CheckedExecution
from demiflow.execution.local_kernel import local_execution
from demiflow.execution.run_context import RunContext
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules, LIMITS
from demiflow.workbench.runs.limits import DEFAULT_LIMITS
from demiflow.workbench.runs.limits import DEPLOYMENT_LIMITS
from demiflow.workbench.runs.limits import limits_with
from demiflow.workbench.projects.workspace import resolve_flow_resources
from demiflow.operator_llm.messages import admit_template
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.tokens import budget_declaration, input_budget
from test_operator_foundation_models import load
from test_prompt_systemone_questions import DEFINITION, ITEMS, server


def contract(tmp_path, definition, *, options=None):
    (tmp_path / 'flows').mkdir()
    (tmp_path / 'operators').mkdir()
    definition = copy.deepcopy(definition)
    model = definition['prompts']['match']['model']
    model.pop('base_url_env')
    model['base_url'] = os.environ['TEST_QUESTIONS_URL']
    (tmp_path / 'flows/prompt.yaml').write_text(yaml.safe_dump(definition, sort_keys=False))
    settings = {'trust_env': False, 'collect_journal_totals': False, 'timeout_s': 2,
                'max_request_bytes': 1048576, 'max_response_bytes': 1048576, 'model_revision': 'fixture-v1',
                'input_limits': {'max_images': 1, 'max_image_bytes': 1024, 'max_total_image_bytes': 1024}}
    settings.update(options or {})
    body = "from demiflow import data\ndef run(*, target):\n    rows = data.from_items(" + repr([
        {'candidates': ITEMS, 'image': 'data:image/png;base64,aW1hZ2U='}]) + ")\n"
    body += "    result = rows.map_prompt_async('match', config='flows/prompt.yaml', inputs={'candidates': 'candidates', 'image': 'image'}, output='answer', call_output='call', error_output='error', max_requests=2, token_budget={'unit': 'chars', 'max_input': 10000}, options=" + repr(settings) + ").materialize()\n"
    body += "    return result.select_columns(['answer']).write_lance(target, mode='create', return_receipt=True)\n"
    (tmp_path / 'flows/model.py').write_text(body)
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    target = str(tmp_path / 'out.lance')
    report = FlowChecker(tmp_path, catalog, {}, [target], resource_resolver=lambda tree, refs, bindings:
        resolve_flow_resources(tmp_path, tree, refs, bindings)).inspect('flows/model.py:run', {'target': target})
    return report, target


def test_managed_questions_preserve_partial_answers_and_exact_replay(server, tmp_path, monkeypatch):
    report, target = contract(tmp_path, DEFINITION)
    assert report['status'] == 'checked', report['diagnostics']
    node = next(n for n in report['nodes'] if n['kind'] == 'prompt')
    answer = node['fields']['answer']['type']['fields']
    assert answer['answers']['type']['value']['fields']['noul']['type']['kind'] == 'union'
    assert answer['errors']['type']['value']['kind'] == 'str'
    server['answers'] = {'a': {'type': 'noul', 'noul': 0.9}}
    flow = load(monkeypatch, 'flows.model', tmp_path / 'flows/model.py')
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'work').mkdir()
    def execute():
        context = CheckedExecution(nodes=report['nodes'], edges=report['edges'], actions=report['actions'],
            operators=report['operators'], source_root=tmp_path, limits=DEFAULT_LIMITS,
            record=lambda *args: None, check_stop=lambda: None)
        with local_execution(workers=1, worker_mode='thread', partitions=1, memory_bytes=128 * 1024**2,
                             temp_directory=str(tmp_path / 'work')) as session:
            session._executor.resource_root = tmp_path
            with context.bind_context():
                flow.run(target=target)
    class Interrupted(Exception):
        pass
    # Replay the original native run. A second business SQLite journal and
    # deleting an already published output would test a different contract.
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run:
            reference = run.ref
            execute()
            raise Interrupted('after confirmed native actions')
    first = lance.dataset(target)
    expected = {'answers': {'a': {'type': 'noul', 'noul': 0.9}}, 'errors': {'b': 'missing_answer'}}
    assert first.to_table().to_pylist()[0]['answer'] == expected
    with RunContext.resume(reference) as resumed:
        execute()
        assert resumed.attempt == 2
        assert resumed.journal.budget_consumed('requests') == 1
    final = lance.dataset(target)
    assert final.version == first.version
    assert final.to_table().to_pylist()[0]['answer'] == expected
    assert not (tmp_path/'evidence/calls.sqlite').exists()
    assert len(server['requests']) == 1


@pytest.mark.parametrize('change', ['no_revision', 'no_limits', 'wrong_image_type'])
def test_managed_image_effects_require_explicit_binding(server, tmp_path, change):
    options = {'model_revision': None} if change == 'no_revision' else {'input_limits': None} if change == 'no_limits' else {'input_limits': {'max_images': True, 'max_image_bytes': 1, 'max_total_image_bytes': 1}}
    report, _ = contract(tmp_path, DEFINITION, options=options)
    assert report['status'] != 'checked'
    assert not server['requests']


def test_pre_render_image_and_text_budget_units_are_independent():
    prompt = parse_prompt_pack(yaml.safe_dump(DEFINITION)).prompt_definitions['match']
    values = {'candidates': ITEMS, 'image': 'data:image/png;base64,aW1hZ2U='}
    options = {'max_request_bytes': 65536, 'input_limits': {'max_images': 1, 'max_image_bytes': 10, 'max_total_image_bytes': 10}}
    admit_template(prompt, values, options)
    for images in ([values['image']] * 2, 'data:image/png;base64,' + 'A' * 100, 'http://example.invalid/image.png'):
        with pytest.raises(TypeError):
            admit_template(prompt, {**values, 'image': images}, options)
    declared = {'unit': 'chars', 'max_input': 7}
    assert budget_declaration(input_budget(declared)) == declared
    with pytest.raises(ValueError):
        budget_declaration({'unit': 'tokens', 'max_input': 1, 'max_output': 1, 'profile': {}})


def test_workspace_capacity_is_explicit_and_runs_can_only_tighten_it():
    assert limits_with()['max_input_rows'] == 100000
    with pytest.raises(ValueError):
        limits_with({'max_input_rows': 1000000})
    assert limits_with({'max_input_rows': 500000}, policy={'max_input_rows': 1000000})['max_input_rows'] == 500000
    with pytest.raises(ValueError):
        limits_with(policy={'max_input_rows': DEPLOYMENT_LIMITS['max_input_rows'] + 1})
    with pytest.raises(ValueError):
        limits_with(policy={'memory_bytes': DEFAULT_LIMITS['max_rss_bytes']})


def test_large_ledger_is_an_explicit_ceiling_not_a_preallocation(tmp_path):
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal, MAX_DISK_BYTES
    path = tmp_path / 'large.sqlite'
    journal = SQLitePromptJournal(path, max_requests=1000000, max_disk_bytes=1024**3)
    journal.reserve({'request': 1})
    journal.response({'request': 1}, {'result': 'complete'})
    journal.close()
    assert path.stat().st_size < 1024**2
    with pytest.raises(ValueError):
        SQLitePromptJournal(path, max_disk_bytes=MAX_DISK_BYTES + 1)
