"""Project-owned definitions and independent flow entrypoints, without execution."""
import copy
import json
from pathlib import Path
import sys

import pytest

from demiflow.forge.development import cli, tasks
from demiflow.forge.development.checks import structural
from demiflow.forge.development.project import ForgeError, Project, TOOL_ROOT, VERSION


@pytest.fixture
def workspace(tmp_path):
    def write(path, text):
        p = tmp_path / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    write('AGENTS.md', 'DemiForge candidate.10: independent flows, shared definitions.')
    write('orders/README.md', 'DemiForge: select and evaluate share quality and review.')
    write('orders/tests/fixture.py', '# No network/model calls.')
    write('orders/operators/quality.py', 'def keep(value, threshold=5): return value >= threshold\n')
    write('orders/prompts/review.yaml', 'schema_version: demiflow_prompt_pack_v2\nprompts: {}\n')
    write('orders/flows/select.py', 'from ..operators.quality import keep\ndef run(value, threshold=5): return keep(value, threshold)\n')
    write('orders/flows/evaluate.py', 'from ..operators.quality import keep\ndef run(value): return keep(value, 8)\n')
    config = dict(schema='demiforge-project-v3', standard=VERSION, tool_root=str(TOOL_ROOT),
        sources={'project-rules': {'path': 'AGENTS.md'}}, watch=['orders'], readonly=[],
        projects={'orders': dict(path='orders', flows={
            'select': dict(path='orders/flows/select.py', symbol='run'),
            'evaluate': dict(path='orders/flows/evaluate.py', symbol='run')},
            operators=['orders/operators'], prompts=['orders/prompts'], notebooks=[], verify=[dict(
                id='both-flows', argv=[sys.executable, '-B', '-c',
                    'from orders.flows.select import run as select; from orders.flows.evaluate import run as evaluate; '
                    'assert select(6) is True; assert evaluate(6) is False'],
                env={'PYTHONPATH': '{project}'}, timeout=10)])})
    def save():
        write('.demiforge/project.json', json.dumps(config))
        return Project(tmp_path)
    return tmp_path, config, write, save


def test_two_flows_share_owned_definitions_without_default_entry(workspace):
    root, config, write, save = workspace
    p = save()
    assert structural(p, 'orders') == []
    assert set(p.entries('orders')) == {'select', 'evaluate'}
    with pytest.raises(ForgeError, match='no default execution entry'):
        p.entry_path('orders')
    observation = p.observe('orders')
    assert set(observation.sources) == {'standard', 'project-rules', 'project-readme',
                                       'flow-entry-select', 'flow-entry-evaluate'}
    assert 'orders/prompts/review.yaml' in observation.tree
    assert observation.coverage['policy'] == 'candidate.10-multi-flow-source-v1'
    # The checker must never import either entry, even for parameter discovery.
    write('orders/flows/evaluate.py', 'raise RuntimeError("do not execute")\ndef run(value): return value\n')
    assert structural(save(), 'orders') == []


@pytest.mark.parametrize('path,code', [
    ('orders/flows/select.py', 'from .evaluate import run as other\ndef run(value): return other(value)\n'),
    ('orders/operators/quality.py', 'from ..flows.select import run\ndef keep(value, threshold=5): return value\n'),
])
def test_cross_flow_and_reverse_operator_dependencies_remain_rejected(workspace, path, code):
    _, _, write, save = workspace
    write(path, code)
    problems = ' '.join(structural(save(), 'orders'))
    assert 'cross-entry' in problems or 'operator imports entry' in problems


def test_indirect_entry_dependency_is_not_a_shared_operator(workspace):
    _, _, write, save = workspace
    write('orders/operators/quality.py', 'from .hidden import run\ndef keep(value, threshold=5): return value\n')
    write('orders/operators/hidden.py', 'from ..flows.evaluate import run\n')
    assert 'operator imports entry' in ' '.join(structural(save(), 'orders'))


@pytest.mark.parametrize('change', ['overlap', 'outside', 'duplicate', 'old-single-entry', 'not-watched', 'old-version'])
def test_invalid_registration_cannot_silently_collapse_to_one_flow(workspace, change):
    _, config, _, save = workspace
    project = config['projects']['orders']
    if change == 'overlap': project['prompts'] = ['orders/operators']
    elif change == 'outside': project['prompts'] = ['outside/prompts']
    elif change == 'duplicate': project['flows']['evaluate'] = copy.deepcopy(project['flows']['select'])
    elif change == 'old-single-entry': project['entry'] = project['flows'].pop('select')
    elif change == 'not-watched': config['watch'] = ['orders/flows/select.py']
    elif change == 'old-version': config['standard'] = '1.0.0-candidate.9'
    with pytest.raises(ForgeError): save()


def test_missing_second_flow_and_prompt_are_reported(workspace):
    root, _, _, save = workspace
    (root/'orders/flows/evaluate.py').unlink()
    (root/'orders/prompts/review.yaml').unlink()
    (root/'orders/prompts').rmdir()
    problems = ' '.join(structural(save(), 'orders'))
    assert 'orders/flows/evaluate.py' in problems and 'orders/prompts' in problems


def test_notebook_can_explicitly_call_both_flows_without_becoming_an_operator(workspace):
    _, config, write, save = workspace
    config['projects']['orders']['notebooks'] = ['orders/review.ipynb']
    write('orders/review.ipynb', json.dumps({'cells': [dict(cell_type='code', source=
        'from orders.flows.select import run as select\nfrom orders.flows.evaluate import run as evaluate\n')]}))
    assert structural(save(), 'orders') == []


def test_declared_prompts_inside_pruning_candidates_remain_fully_bound(workspace):
    root, config, write, save = workspace
    config['projects']['orders']['prompts'] = ['orders/runs/prompts']
    write('orders/runs/prompts/review.yaml', 'version: first\n')
    p = save(); before = p.observe('orders')
    assert before.coverage['complete']
    write('orders/runs/prompts/review.yaml', 'version: second\n')
    assert before.tree != p.observe('orders').tree


def test_cli_lifecycle_checks_both_flows_and_shared_changes_invalidate_evidence(workspace, capsys):
    root, _, write, save = workspace
    project = save()
    assert cli.main(['--project', str(root), 'start', '--id', 'two-flows', '--project-name', 'orders',
        '--goal', 'Independent flows with shared operators', '--write', 'orders',
        '--non-goal', 'No model/network/production execution']) == 0
    capsys.readouterr()
    with tasks.locked(project, 'two-flows') as (directory, state):
        for key, source in state['sources'].items():
            for page in range(1, source['pages'] + 1): tasks.context(project, state, key, page)
        assert tasks.check(project, state)['passed']
        assert tasks.verify(project, state, directory, True)['passed']
        before = tasks.current(project, state)[3]
        write('orders/prompts/review.yaml', 'schema_version: demiflow_prompt_pack_v2\nprompts: {}\n# changed\n')
        after = tasks.current(project, state)[3]
        assert before != after
        assert tasks.evidence_status(state["verification"], after) == "stale"
        assert not tasks.handoff(project, state, directory, False)['ready']


def test_flow_count_budget_is_checked_before_source_inspection(workspace):
    _, config, _, save = workspace
    config['projects']['orders']['flows'] = {f'f{i}': dict(path=f'orders/flows/f{i}.py',symbol='run') for i in range(65)}
    with pytest.raises(ForgeError, match='1..64'): save()


@pytest.mark.parametrize('version', ['1.0.0-candidate.5', '1.0.0-candidate.9'])
def test_upgrade_one_owner_preserves_other_owners_original_protocol(workspace, version):
    from demiflow.workbench.projects.discovery import discover
    root, config, write, save = workspace
    write('legacy/README.md', 'DemiForge historical owner, independently versioned.')
    write('legacy/operators/clean.py', 'def clean(row): return row\n')
    write('legacy/legacy_pipeline.py', 'def config(): return {}\ndef run_pipeline(configuration): return configuration\n')
    command = copy.deepcopy(config['projects']['orders']['verify'])
    record = dict(path='legacy', standard=version, entry='legacy_pipeline.py', verify=command)
    if version.endswith('.9'):
        record.update(entry=dict(path='legacy/legacy_pipeline.py', symbol='run_pipeline'),
                      operators=['legacy/operators'], notebooks=[])
    config['projects']['legacy'] = record
    config['watch'].append('legacy')
    original = copy.deepcopy(record)
    project = save()
    assert project.standard('legacy') == version
    assert project.standard('orders') == VERSION
    assert project.pipeline('legacy') == original
    assert project.entries('legacy') == {'main': dict(path='legacy/legacy_pipeline.py', symbol='run_pipeline')}
    assert 'pipeline-entry' in project.source_defs('legacy')
    assert 'flow-entry-select' in project.source_defs('orders')
    if version.endswith('.9'):
        assert project.observe('legacy').coverage['policy'] == 'candidate.9-local-source-v1'
    old = discover(root/'legacy', {'entries': {}})
    assert old['entries'][0]['standard'] == version
    assert old['entries'][0]['protocol'] == ('native_flow' if version.endswith('.9') else 'config_run_pipeline')
    new = discover(root/'orders', {'entries': {}})
    assert {e['flow_id'] for e in new['entries']} == {'select', 'evaluate'}
    assert new['prompts'][0]['path'] == 'prompts'
    # Writing v3 at the repository level must not reinterpret an old record as
    # a multi-flow owner, nor accept new fields without its explicit upgrade.
    config['projects']['legacy']['flows'] = copy.deepcopy(config['projects']['orders']['flows'])
    with pytest.raises(ForgeError, match='Invalid pipeline registration'):
        save()
