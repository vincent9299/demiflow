"""Project construction and registration exercise native checks against actual candidate files."""
import json
import shutil

import pytest
import yaml

from demiflow.forge import pipeline_check, project
from demiflow.forge.schema import ForgeError
from test_map_prompt_async import PACK


@pytest.fixture(scope='module')
def initialized(tmp_path_factory):
    root = tmp_path_factory.mktemp('project')
    result = project.initialize_project(root)
    assert result['project_id'] == project.initialize_project(root)['project_id']
    return root


@pytest.fixture
def candidate(initialized, tmp_path):
    root = tmp_path / 'candidate'
    shutil.copytree(initialized, root)
    project.create_role_file(root, 'flow', 'counts')
    (root / 'flows/counts.py').write_text(
        'from demiflow import data\n'
        'def run():\n    return data.range(10).count()\n')
    return root


def test_interrupted_registration_rechecks_and_completes(candidate, monkeypatch):
    write = project.write_control
    def before_manifest(path, *args, **kwargs):
        if path.name == 'workspace.json':
            raise OSError('interrupted')
        return write(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(project, 'write_control', before_manifest)
        with pytest.raises(OSError, match='interrupted'):
            project.register_flow(candidate, 'flows/counts.py:run')
    default = candidate / 'configs/counts.json'
    assert json.loads(default.read_bytes()) == {'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {}}
    assert project.load_project(candidate)[1]['entries'] == {}
    assert project.register_flow(candidate, 'flows/counts.py:run')['status'] == 'registered'
    assert project.register_flow(candidate, 'flows/counts.py:run')['status'] == 'unchanged'
    with pytest.raises(ForgeError, match='existing parameter binding'):
        project.register_flow(candidate, 'flows/counts.py:run', params='configs/different.json')


def test_registration_rejects_changes_to_checked_source(candidate, monkeypatch):
    check = pipeline_check.check_project
    def concurrent_edit(*args, **kwargs):
        result = check(*args, **kwargs)
        assert result['status'] == 'passed'
        (candidate / 'flows/counts.py').write_text('def run():\n    pass\n')
        return result
    monkeypatch.setattr(pipeline_check, 'check_project', concurrent_edit)
    with pytest.raises(ForgeError) as error:
        project.register_flow(candidate, 'flows/counts.py:run')
    assert error.value.code == 'inputs_changed'
    assert project.load_project(candidate)[1]['entries'] == {}
    assert not (candidate / 'parameters').exists()


def test_add_never_overwrites_or_follows_role_link(candidate, tmp_path):
    path = candidate / 'flows/counts.py'
    original = path.read_bytes()
    with pytest.raises(ForgeError, match='already exists'):
        project.create_role_file(candidate,'flow','counts')
    assert path.read_bytes() == original
    (candidate / 'operators').symlink_to(tmp_path)
    with pytest.raises(ForgeError):
        project.create_role_file(candidate,'operator','helper')
    assert not (tmp_path / 'helper.py').exists()


def test_static_agent_document_validates_without_loading_candidate(candidate, monkeypatch):
    monkeypatch.syspath_prepend(str(candidate))
    name = 'operators.would_raise'
    (candidate / 'operators').mkdir()
    (candidate / 'operators/__init__.py').touch()
    (candidate / 'operators/would_raise.py').write_text("raise AssertionError('candidate was imported')\n")
    task = yaml.safe_load(PACK)['prompts']['enrich']
    model = task.pop('model')
    document = dict(schema_version='demiflow_agent_v2', runtime='demiflow', model=model,
                    tasks={'enrich': task}, budgets={'max_requests': 2},
                    operators={'material': dict(fn=name+':run', description='Get one item', replay='verify',
                               arguments={'type':'object','properties':{},'additionalProperties':False})})
    result = project.create_role_file(candidate, 'prompt', 'evaluate', content=yaml.safe_dump(document))
    assert result['format'] == 'demiflow_agent_v2' and result['definitions'] == ['enrich']
    document['operators']['material']['fn'] = 'unknown.would_raise:call'
    with pytest.raises(ForgeError, match='operators/'):
        project.create_role_file(candidate, 'prompt', 'wrong', content=yaml.safe_dump(document))
    assert not (candidate / 'prompts/wrong.yaml').exists()


def test_inline_prompt_accepts_bounded_anchors_and_rejects_recursive_input(candidate):
    prompt = yaml.safe_load(PACK)
    prompt['prompts']['second'] = prompt['prompts']['enrich']
    result = project.create_role_file(candidate, 'prompt', 'aliases', content=yaml.safe_dump(prompt))
    assert sorted(result['definitions']) == ['enrich', 'second']
    with pytest.raises(ForgeError, match='alias'):
        project.create_role_file(candidate,'prompt','loop', content='x: &a [*a]')
    assert not (candidate / 'prompts/loop.yaml').exists()


def test_project_and_product_check_same_native_budget_fields(candidate):
    from demiflow.forge.pipeline_policy import limits_with
    manifest = candidate / 'workspace.json'
    control = project.load_project(candidate)[1]
    control['budgets'] = {'typo': 1}
    manifest.write_text(json.dumps(control))
    with pytest.raises(ForgeError, match='Unknown Workspace budget'):
        project.load_project(candidate)
    with pytest.raises(ForgeError, match='Unknown Workspace budget'):
        limits_with(policy=control['budgets'])


def test_renew_authorization_before_first_publication(candidate):
    def revoked():
        raise ForgeError('Not authorized')
    with pytest.raises(ForgeError, match='Not authorized'):
        project.register_flow(candidate, 'flows/counts.py:run', authorize=revoked)
    assert not (candidate / 'parameters').exists()
    assert project.load_project(candidate)[1]['entries'] == {}


def test_init_never_creates_an_unverifiable_project(tmp_path, monkeypatch):
    project_root = tmp_path / 'new'
    monkeypatch.setattr(project, 'build_snapshot', lambda *a, **kw: {
        'catalog': {'inspection_complete': False}, 'environment': {'diagnostics': []}})
    with pytest.raises(ForgeError) as error:
        project.initialize_project(project_root)
    assert error.value.code == 'inspection_incomplete'
    assert not project_root.exists()


@pytest.mark.parametrize('name', ['false', 'true', 'none'])
def test_actor_skeleton_uses_a_valid_class_name(candidate, name):
    import ast
    result = project.create_role_file(candidate, 'operator', name, actor=True)
    tree = ast.parse((candidate / result['path']).read_text())
    assert isinstance(tree.body[0], ast.ClassDef)
    assert tree.body[0].name == result['symbol'] == name.title() + 'Actor'
    from demiflow.forge.definition_exports import OPERATOR_EXPORT, exports
    assert exports(tree, OPERATOR_EXPORT) == {result['symbol']: 'operator'}
