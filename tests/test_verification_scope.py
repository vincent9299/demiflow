"""The scope a change must verify (repository rules 5.1) and the test-module map behind it."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

import verification_scope as scope

TESTS = Path(__file__).resolve().parent
TREE = TESTS.parent


def _run(code, log, *flags):
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join([str(TREE), str(TESTS / 'import_recorder')]),
           'DEMIFLOW_IMPORT_LOG': str(log)}
    subprocess.run([sys.executable, '-B', *flags, '-c', code], env=env, check=True, cwd=str(TREE))


def test_the_recorder_logs_demiflow_modules_of_a_process_and_of_its_children(tmp_path):
    log = tmp_path / 'imports.log'
    _run('import demiflow.resources.contracts, subprocess, sys\n'
         'subprocess.run([sys.executable, "-B", "-c", "import demiflow.resources.evidence"], check=True)\n', log)
    recorded = log.read_text().split()
    assert {'demiflow', 'demiflow.resources', 'demiflow.resources.contracts', 'demiflow.resources.evidence'} <= set(recorded)
    assert all(name == 'demiflow' or name.startswith('demiflow.') for name in recorded)
    # Once per process: the parent and its child both load contracts, only the child loads evidence.
    assert recorded.count('demiflow.resources.contracts') == 2
    assert recorded.count('demiflow.resources.evidence') == 1


def test_the_recorder_is_inert_without_a_log_and_blind_to_isolated_processes(tmp_path):
    log = tmp_path / 'imports.log'
    _run('import sys; sys.path.insert(0, %r); import demiflow.resources.contracts' % str(TREE), log, '-I')
    assert not log.exists()
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join([str(TREE), str(TESTS / 'import_recorder')])}
    env.pop('DEMIFLOW_IMPORT_LOG', None)
    subprocess.run([sys.executable, '-B', '-c', 'import demiflow.resources.contracts'], env=env, check=True)
    assert not log.exists()


def test_module_names_and_static_imports():
    assert scope.module_name('demiflow/resources/capacity.py') == 'demiflow.resources.capacity'
    assert scope.module_name('demiflow/resources/__init__.py') == 'demiflow.resources'
    assert scope.module_name('tests/test_x.py') is None and scope.module_name('demiflow/x.json') is None
    source = ('import os\nimport demiflow.data as data\nfrom demiflow.resources import capacity, local\n'
              'def test():\n    from demiflow.execution.hosts.capacity import local_capacity\n    from . import helper\n')
    assert scope.static_imports(source) == {
        'demiflow.data', 'demiflow.resources', 'demiflow.resources.capacity', 'demiflow.resources.local',
        'demiflow.execution.hosts.capacity', 'demiflow.execution.hosts.capacity.local_capacity'}


@pytest.mark.parametrize('paths,expected', [
    (['docs/todo.md', 'README.md'], 'docs'),
    (['demiflow/workbench/api.py', 'tests/test_workbench_run_module.py', 'docs/x.md'], 'workbench'),
    (['tests/test_forge_audit_browser.py'], 'workbench'),
    (['demiflow/resources/capacity.py', 'tests/test_u2_resources_local.py', 'docs/x.md'], 'engine'),
    (['demiflow/errors.py'], 'engine'),
    (['demiflow/resources/capacity.py', 'demiflow/workbench/api.py'], 'cross'),
    (['tests/conftest.py'], 'cross'), (['tests/runtime_v3_support.py'], 'cross'),
    (['pyproject.toml'], 'cross'), (['demiflow/cli.py'], 'cross'), (['smoke_standalone.py'], 'cross'),
])
def test_scope_rows(paths, expected):
    assert scope.scope_of(paths) == expected


@pytest.fixture
def tree(tmp_path):
    for name in ('demiflow/a/__init__.py', 'demiflow/a/one.py', 'demiflow/a/two.py', 'demiflow/a/fresh.py',
                 'demiflow/b/__init__.py', 'demiflow/b/core.py', 'demiflow/b/unseen.py',
                 'demiflow/execution/seal/plan.py', 'demiflow/resources/capacity.py',
                 'tests/test_one.py', 'tests/test_two.py', 'tests/test_core.py', 'tests/test_brand_new.py',
                 'tests/test_workbench_page.py', 'tests/test_seal.py'):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('')
    return tmp_path


def _map():
    modules = ['demiflow.a', 'demiflow.a.one', 'demiflow.a.two', 'demiflow.b', 'demiflow.b.core',
               'demiflow.execution.seal.plan', 'demiflow.resources.capacity']
    index = {name: number for number, name in enumerate(modules)}
    loads = {'test_one.py': ['demiflow.a', 'demiflow.a.one', 'demiflow.b.core'],
             'test_two.py': ['demiflow.a', 'demiflow.a.two', 'demiflow.b.core'],
             'test_core.py': ['demiflow.b', 'demiflow.b.core', 'demiflow.resources.capacity'],
             'test_seal.py': ['demiflow.execution.seal.plan', 'demiflow.b.core'],
             'test_workbench_page.py': ['demiflow.a.one']}
    return {'schema': scope.SCHEMA, 'candidate': 'c' * 40, 'modules': modules,
            'tests': {'standard': {name: [index[m] for m in found] for name, found in loads.items()},
                      'historical': {'test_old.py': [index['demiflow.b.core']]}}}


def test_an_engine_change_runs_the_tests_that_load_the_changed_modules(tree):
    chosen = scope.select(_map(), ['demiflow/a/one.py'], tree)
    assert chosen['scope'] == 'engine' and chosen['lanes'] == ['standard']
    # test_workbench_page loads the module too, but it is not an engine test; the unrecorded file always runs.
    assert chosen['tests'] == ['test_brand_new.py', 'test_one.py']
    everyone = scope.select(_map(), ['demiflow/b/core.py'], tree)
    assert everyone['tests'] == ['test_brand_new.py', 'test_core.py', 'test_one.py', 'test_seal.py', 'test_two.py']


def test_changed_and_unrecorded_test_files_always_run(tree):
    chosen = scope.select(_map(), ['tests/test_two.py', 'docs/todo.md'], tree)
    assert chosen['scope'] == 'engine' and chosen['changed_tests'] == ['test_two.py']
    assert chosen['tests'] == ['test_brand_new.py', 'test_two.py']
    deleted = scope.select(_map(), ['tests/test_gone.py'], tree)
    assert deleted['changed_tests'] == [] and deleted['tests'] == ['test_brand_new.py']


def test_a_new_module_is_covered_by_its_package_and_an_unseen_one_by_everything(tree):
    fresh = scope.select(_map(), ['demiflow/a/fresh.py'], tree)
    assert fresh['tests'] == ['test_brand_new.py', 'test_one.py', 'test_two.py']
    assert fresh['reasons'] == ['demiflow.a.fresh: not in the map, covered by the tests of demiflow.a']
    # A deleted module nobody recorded cannot be placed: everything runs.
    removed = scope.select(_map(), ['demiflow/a/removed.py'], tree)
    assert removed['tests'] == ['test_brand_new.py', 'test_core.py', 'test_one.py', 'test_seal.py', 'test_two.py']
    assert removed['reasons'] == ['demiflow.a.removed: no test recorded loading it, all engine tests']


def test_lanes_follow_what_the_change_touches(tree):
    assert scope.select(_map(), ['demiflow/a/one.py'], tree)['lanes'] == ['standard']
    assert scope.select(_map(), ['demiflow/execution/seal/plan.py'], tree)['lanes'] == ['standard', 'historical']
    assert scope.select(_map(), ['demiflow/resources/capacity.py'], tree)['lanes'] == ['standard', 'ray']
    both = scope.select(_map(), ['demiflow/execution/seal/plan.py', 'demiflow/resources/capacity.py'], tree)
    assert both['lanes'] == ['standard', 'historical', 'ray', 'historical_ray']
    assert both['tests'] == ['test_brand_new.py', 'test_core.py', 'test_seal.py']


def test_other_scopes(tree):
    assert scope.select(_map(), ['docs/todo.md'], tree)['lanes'] == []
    workbench = scope.select(_map(), ['demiflow/workbench/api.py'], tree)
    assert workbench['lanes'] == ['standard'] and workbench['tests'] == ['test_workbench_page.py']
    cross = scope.select(_map(), ['tests/conftest.py'], tree)
    assert cross['lanes'] == list(scope.LANES) and cross['tests'] is None
    with pytest.raises(ValueError):
        scope.select({'schema': 'something-else'}, ['docs/todo.md'], tree)


def test_build_joins_recorded_and_static_imports_and_keeps_only_modules_of_the_tree(tmp_path):
    root = tmp_path / 'lane'
    for number, (name, done, logged) in enumerate([
            ('test_verification_scope.py', True, ['demiflow', 'demiflow.resources.capacity', 'demiflow.not_a_module']),
            ('test_unit_child_processes.py', True, None), ('test_merge_gate.py', False, ['demiflow.data'])], 1):
        directory = root / 'f' / ('%03d' % number)
        directory.mkdir(parents=True)
        (directory / 'meta.json').write_text(json.dumps({'file': name, **({'returncode': 0} if done else {})}))
        if logged is not None:
            (directory / 'imports.log').write_text('\n'.join(logged) + '\n')
    document = scope.build('h' * 40, {'standard': root})
    assert document['schema'] == scope.SCHEMA and document['candidate'] == 'h' * 40
    files = document['tests']['standard']
    assert set(files) == {'test_verification_scope.py', 'test_unit_child_processes.py'}   # not-executed is left out
    loaded = {document['modules'][number] for number in files['test_verification_scope.py']}
    assert loaded == {'demiflow', 'demiflow.resources.capacity'}
    assert files['test_unit_child_processes.py'] == []
    assert scope.select(document, ['demiflow/resources/capacity.py'])['scope'] == 'engine'
    with pytest.raises(ValueError):
        scope.build('h' * 40, {'nightly': root})
