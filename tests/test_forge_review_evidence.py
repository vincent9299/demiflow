import json
import subprocess

import pytest

from demiflow.forge.review_evidence import FILE_BYTES, capture_evidence, verify_evidence
from demiflow.forge.schema import ForgeError


def write(root, path, text):
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding='utf8')


def git(root, *args):
    subprocess.run(['git', '-C', str(root), *args], check=True, capture_output=True)


def git_project(root):
    git(root, 'init', '-q')
    git(root, 'config', 'user.name', 'Fixture')
    git(root, 'config', 'user.email', 'fixture@example.org')


def test_forge_review_git_scope_nested_project_and_staleness(tmp_path):
    git_project(tmp_path)
    root = tmp_path / 'projects/alpha'
    write(root, 'operators/shared.py', 'def use(): return 1\n')
    write(root, 'flows/replaced.py', 'def old(): return 3\n')
    write(root, 'prompts/idle.yaml', 'prompts: {}\n')
    write(tmp_path, 'other/outside.py', 'other project\n')
    git(tmp_path, 'add', '.')
    git(tmp_path, 'commit', '-qm', 'base')
    write(root, 'operators/shared.py', 'def use(): return 2\n')
    git(tmp_path, 'add', 'projects/alpha/operators/shared.py')
    (root / 'flows/replaced.py').unlink()
    write(root, 'flows/new.py', 'from operators.shared import use\ndef run(): return use()\n')
    write(root, '.forge/runs/copy.py', 'private\n')
    write(root, 'runs/result.json', 'private\n')
    write(tmp_path, 'other/outside.py', 'changed elsewhere\n')
    capture = capture_evidence(root)
    assert set(capture['scope']['changed']) == {'operators/shared.py', 'flows/replaced.py', 'flows/new.py'}
    assert capture['files']['flows/replaced.py'] == dict(role='target', before='def old(): return 3\n', after=None)
    assert capture['files']['operators/shared.py']['before'] == 'def use(): return 1\n'
    assert set(capture['files']) == set(capture['scope']['changed'])
    assert verify_evidence(root, capture)
    write(root, 'tests/new_check.py', 'assert 1\n')
    assert not verify_evidence(root, capture)


def test_forge_review_staged_and_unstaged_content_combined(tmp_path):
    git_project(tmp_path)
    write(tmp_path, 'op.py', 'start\n')
    git(tmp_path, 'add', 'op.py')
    git(tmp_path, 'commit', '-qm', 'start')
    write(tmp_path, 'op.py', 'staged\n')
    git(tmp_path, 'add', 'op.py')
    write(tmp_path, 'op.py', 'unstaged\n')
    capture = capture_evidence(tmp_path, paths=('op.py',), base='HEAD')
    assert capture['files']['op.py'] == dict(role='target', before='start\n', after='unstaged\n')


def test_entry_resolves_indirection_prompts_and_evidence_without_import(tmp_path):
    root = tmp_path / 'mygroup' / 'business'
    write(root, 'flows/site.py',
          'from mygroup.business.operators import wrapper\ndef run(): return wrapper.use("screen.yaml")\n')
    write(root, 'operators/wrapper.py', 'from .implementation import use\n')
    write(root, 'operators/implementation.py', 'raise RuntimeError("must not import")\n')
    write(root, 'prompts/screen.yaml', 'value: 2\n')
    write(root, 'tests/test_site.py', 'def test_scope(): pass\n')
    write(root, 'parameters/sample.json', '{}')
    write(root, 'tests/small-report.txt', 'Two fixture cases passed.')
    info = capture_evidence(root, entry='flows/site.py:run', params='parameters/sample.json',
                            evidence=['tests/small-report.txt'])
    assert set(info['files']) == {'flows/site.py', 'operators/wrapper.py', 'operators/implementation.py',
                                  'prompts/screen.yaml', 'parameters/sample.json', 'tests/small-report.txt', 'tests/test_site.py'}
    assert info['files']['tests/small-report.txt']['role'] == 'evidence'
    assert info['files']['parameters/sample.json']['role'] == 'target'
    assert verify_evidence(root, info)
    write(root, 'operators/implementation.py', 'def use(name): return name\n')
    assert not verify_evidence(root, info)


def test_changes_affect_registered_flow_through_intermediate(tmp_path):
    write(tmp_path, 'flows/site.py', 'from operators.wrapper import use\ndef run(): return use()\n')
    write(tmp_path, 'operators/wrapper.py', 'from .implementation import use\n')
    write(tmp_path, 'operators/implementation.py', 'def use(): return 1\n')
    write(tmp_path, 'parameters/sample.json', '{}')
    write(tmp_path, 'workspace.json', json.dumps(dict(format='demiflow_forge_workspace_v2',
        id='w' + 'b' * 32, name='Review', entries={'flows/site.py:run': ['parameters/sample.json']},
        locations={}, platform={'path': '.forge/platform/current.json', 'digest':'0'*64})))
    info = capture_evidence(tmp_path, paths=['operators/implementation.py'])
    assert info['scope']['entries'] == ['flows/site.py:run']
    assert info['files']['parameters/sample.json']['role'] == 'dependency'
    assert set(info['files']) >= {'flows/site.py', 'operators/wrapper.py', 'parameters/sample.json'}


def test_sensitive_symlink_and_budget_refused(tmp_path):
    write(tmp_path, 'credentials.json', 'sensitive')
    write(tmp_path, 'operators/large.py', ' ' * (FILE_BYTES + 1))
    (tmp_path / 'link.json').symlink_to(tmp_path / 'credentials.json')
    for path in ('credentials.json', '.forge/secrets.md', 'link.json', '../escape.py', 'operators/large.py'):
        with pytest.raises(ForgeError):
            capture_evidence(tmp_path, paths=[path])
    with pytest.raises(ForgeError):
        capture_evidence(tmp_path, paths=['operators/large.py'], exclude=['operators'])


def test_review_nested_data_module_and_package_initializer(tmp_path):
    write(tmp_path, 'flows/run.py',
          'from operators.data.normalize import normalize\ndef run(): return normalize()\n')
    write(tmp_path, 'operators/__init__.py', 'from .initialize import settings\n')
    write(tmp_path, 'operators/initialize.py', 'settings = {}\n')
    write(tmp_path, 'operators/data/normalize.py', 'def normalize(): return 1\n')
    original = capture_evidence(tmp_path, entry='flows/run.py:run')
    assert set(original['files']) == {
        'flows/run.py', 'operators/__init__.py', 'operators/initialize.py', 'operators/data/normalize.py'}
    assert not original['gaps']
    write(tmp_path, 'operators/initialize.py', 'settings = {"case": 2}\n')
    assert not verify_evidence(tmp_path, original)


def test_review_reports_missing_and_protected_local_imports_only(tmp_path):
    write(tmp_path, 'flows/run.py',
          'import sys\nfrom datasets import Sequence\nfrom operators.missing import apply\n'
          'from operators import restricted\ndef run(): return apply(restricted)\n')
    write(tmp_path, 'operators/restricted.py', 'private = "do not show"\n')
    result = capture_evidence(tmp_path, entry='flows/run.py:run', exclude=['operators/restricted.py'])
    assert len(result['gaps']) == 2
    assert 'operators/missing' in result['gaps'][0]
    assert 'operators/restricted' in result['gaps'][1]
    assert set(result['files']) == {'flows/run.py'}
    # A previously missing dependency appearing affects the result even in a path-only review.
    write(tmp_path, 'operators/missing.py', 'def apply(x): return x\n')
    assert not verify_evidence(tmp_path, result, exclude=['operators/restricted.py'])


def test_review_observed_modification_addition_and_deletion_without_git(tmp_path):
    from hashlib import sha256

    write(tmp_path, 'operators/now.py', 'changed\n')
    write(tmp_path, 'operators/created.py', 'added\n')
    basis = {'operators/now.py': {'before':'earlier\n', 'after_sha256':sha256(b'changed\n').hexdigest()},
             'operators/gone.py': {'before':'removed\n','after_sha256':None},
             'operators/created.py': {'before':None,'after_sha256':sha256(b'added\n').hexdigest()}}
    result = capture_evidence(tmp_path, paths=list(basis), prior_sources=basis)
    assert result['scope']['mode'] == 'observed_changes'
    assert result['scope']['base'] is None
    assert set(result['scope']['changed']) == set(basis)
    assert result['files']['operators/now.py'] == {'role':'target', 'before':'earlier\n','after':'changed\n'}
    assert result['files']['operators/gone.py'] == {'role':'target','before':'removed\n','after':None}
    assert result['files']['operators/created.py']['before'] is None
    assert verify_evidence(tmp_path, result, prior_sources=basis)
    write(tmp_path, 'operators/now.py', 'later change\n')
    assert not verify_evidence(tmp_path, result, prior_sources=basis)
    with pytest.raises(ForgeError) as err:
        capture_evidence(tmp_path, paths=list(basis), prior_sources=basis)
    assert err.value.code == 'inputs_changed'


def test_git_subset_is_literal_and_rechecks_only_selected_paths(tmp_path):
    git_project(tmp_path)
    target = 'operators/tokenizer[1].py'
    write(tmp_path, target, 'old\n')
    write(tmp_path, 'operators/tokenizer1.py', 'other\n')
    git(tmp_path, 'add', '.')
    git(tmp_path, 'commit', '-qm', 'initial')
    git(tmp_path, 'rm', '--', target)
    deleted = capture_evidence(tmp_path, paths=[target], base='HEAD')
    assert deleted['files'][target] == {'role':'target', 'before':'old\n', 'after':None}
    write(tmp_path, 'operators/unrelated.py', 'new file outside path review\n')
    assert verify_evidence(tmp_path, deleted)
    write(tmp_path, target, 'restored\n')
    assert not verify_evidence(tmp_path, deleted)
    restored = capture_evidence(tmp_path, paths=[target], base='HEAD')
    assert restored['files'][target] == {'role':'target','before':'old\n','after':'restored\n'}


def test_git_untracked_link_is_never_opened_as_review_source(tmp_path):
    git_project(tmp_path)
    write(tmp_path, 'README.md', 'fixture')
    git(tmp_path, 'add', '.')
    git(tmp_path, 'commit', '-qm', 'initial')
    write(tmp_path, 'operators/actual.py', 'def run(): return True\n')
    (tmp_path / 'operators/alias.py').symlink_to(tmp_path / 'README.md')
    result = capture_evidence(tmp_path)
    assert set(result['files']) == {'operators/actual.py'}


def test_vendored_dependencies_can_be_added_one_at_a_time_and_reject_reverse_imports(tmp_path):
    import sys
    from demiflow.forge.dependencies import LOCK, binding, update
    from demiflow.forge.schema import ForgeError

    root = tmp_path / 'project_fixture'
    (root / '.forge').mkdir(parents=True)
    (root / 'pyproject.toml').write_text('[tool.demiflow.packages]\n'
        'first_dep_fixture = {source = "vendored", path = "packages/first_dep_fixture"}\n'
        'second_dep_fixture = {source = "vendored", path = "packages/second_dep_fixture"}\n')
    first, second = tmp_path / 'first', tmp_path / 'second'
    for source in (first, second):
        source.mkdir()
        (source / '__init__.py').write_text('')
    staged = update(root, sys.executable, name='first_dep_fixture', vendor=first)
    assert staged == {'status': 'staged', 'package': 'first_dep_fixture', 'missing': ['second_dep_fixture']}
    assert not (root / LOCK).exists()
    (second / '__init__.py').write_text('from project_fixture import flows\n')
    with pytest.raises(ForgeError, match='imports a project role'):
        update(root, sys.executable, name='second_dep_fixture', vendor=second)
    assert not (root / 'packages/second_dep_fixture').exists()
    (second / '__init__.py').write_text('')
    final = update(root, sys.executable, name='second_dep_fixture', vendor=second)
    assert final['digest'] == binding(root)['digest']
