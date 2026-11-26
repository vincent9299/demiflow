"""Uniform rules and native execution must not depend on directory spelling."""
import json
import shutil

import pytest
from click.testing import CliRunner

from demiflow.cli import main
from demiflow.forge.runs import manager as runs

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.schema import ForgeError
from test_forge_workspace import demo, isolated
from test_forge_runs import execute, rows


@pytest.fixture
def project(isolated):
    root = isolated
    (root / 'recipe.py').write_text((root / 'flows/questions.py').read_text().replace('operators.question_rules', 'processing'))
    (root / 'processing.py').write_text((root / 'operators/question_rules.py').read_text())
    (root / 'settings').mkdir()
    (root / 'settings/small.json').write_bytes((root / 'flows/select_a.json').read_bytes())
    value = ws.read_control(root / 'workspace.json')
    value['entries'] = {'recipe.py:select': ['settings/small.json']}
    (root / 'workspace.json').write_text(json.dumps(value))
    shutil.rmtree(root / 'flows'); shutil.rmtree(root / 'operators')
    return root


def test_original_module_paths_check_execute_and_stale_gate(project):
    checked = project_ops.check_project(project)
    assert checked['status'] == 'invalid', checked
    assert checked['native_flow']['status'] == 'passed' and checked['entries'][0]['status'] == 'checked'
    assert checked['project_layout']['status'] == 'failed'
    assert 'rules' not in checked and 'static_status' not in checked
    assert checked['coverage']['declared'] == checked['coverage']['inspected'] == 1
    assert set(checked['sources']) == {'recipe.py', 'processing.py'}
    (project / 'README.md').write_text('# Documentation only\n\n[Missing reference](notes.json)\n')
    assert project_ops.project_check_status(project)['status'] == 'invalid'
    prepared = runs.prepare(project, 'recipe.py:select', params='settings/small.json')
    done = execute(project, prepared)
    assert done['status'] == 'succeeded', done
    assert {r['case_id'] for r in rows(project, done)} == {'c01', 'c02'}
    pending = runs.prepare(project, 'recipe.py:select', params='settings/small.json')
    source = project / 'processing.py'; source.write_text(source.read_text() + '\n# changed\n')
    assert project_ops.project_check_status(project)['status'] == 'stale'
    with pytest.raises(ForgeError, match='stale'):
        runs.start(project, pending['run_id'], expected=pending['digest'])



def test_previous_project_review_does_not_count_as_current_static_check(project):
    record = project / '.forge/project-check.json'
    saved = b'{"format":"demiflow_workbench_project_static_check_v2","status":"checked"}'
    record.write_bytes(saved)
    assert project_ops.project_check_status(project)['status'] == 'missing'
    assert record.read_bytes() == saved
    assert project_ops.check_project(project)['status'] == 'invalid'
    assert project_ops.project_check_status(project)['status'] == 'invalid'


def test_forbidden_effects_are_reported_and_never_fixed(project):
    source = project / 'processing.py'
    source.write_text('import subprocess\n' + source.read_text())
    before = source.read_bytes()
    result = project_ops.check_project(project)
    assert result['status'] == 'invalid', result
    assert any(d['rule'] == 'OP-05' for d in result['diagnostics'])
    assert source.read_bytes() == before
    with pytest.raises(ForgeError):
        runs.prepare(project, 'recipe.py:select', params='settings/small.json')
    cli = CliRunner().invoke(main, ['workbench','check',str(project)])
    assert cli.exit_code == 1
    assert json.loads(cli.output)['status'] == 'invalid'


def test_generic_source_read_and_observation(project):
    first = project_ops.workspace_changes(project)
    assert ws.source_text(project, 'processing.py')['text']
    source = project / 'recipe.py'; source.write_text(source.read_text() + '\n# changed\n')
    assert first['sources'] != project_ops.workspace_changes(project)['sources']
    for name in ('../outside.py', '.forge/internal.py'):
        with pytest.raises(ForgeError):
            ws.source_text(project, name)


def test_empty_local_directory_cannot_admit_an_external_import(project):
    (project/'subprocess').mkdir()
    source=project/'processing.py';source.write_text('import subprocess\n'+source.read_text())
    result=project_ops.inspect_flow(project,'recipe.py:select',params='settings/small.json')
    assert result['status']=='invalid', result
    assert result['diagnostics'][0]['rule']=='OP-05'
