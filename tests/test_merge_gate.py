"""The merge gate fails for each thing it exists to catch, and names it.

A count of passed tests says nothing about a guard file that was deleted, a frozen file that
changed, or tests that ran against another checkout. Each case below is one of those.
"""
import hashlib
import sys
from pathlib import Path

import merge_gate

ROOT = Path(__file__).resolve().parents[1]


def tree(tmp_path, *, tests, frozen=b'frozen\n', package=True):
    root = tmp_path / 'tree'
    (root / 'tests').mkdir(parents=True)
    if package:
        (root / 'demiflow').mkdir()
        (root / 'demiflow' / '__init__.py').write_text('')
    (root / 'frozen.py').write_bytes(frozen)
    for name, body in tests.items():
        (root / 'tests' / name).write_text(body)
    return root


RECORDED = {'frozen.py': hashlib.sha256(b'frozen\n').hexdigest()}
PASSING = 'def test_holds():\n    assert True\n'
FAILING = 'def test_holds():\n    assert True\n\n\ndef test_breaks():\n    assert 1 == 2\n'


def run(root, patterns, tmp_path):
    scratch = tmp_path / 'scratch'
    scratch.mkdir(exist_ok=True)
    return merge_gate.gate(root, sys.executable, patterns, RECORDED, scratch_parent=str(scratch))


def test_passes_when_every_guard_passes_and_nothing_frozen_changed(tmp_path):
    root = tree(tmp_path, tests={'test_a.py': PASSING, 'test_b.py': PASSING})
    result = run(root, ('tests/test_a.py', 'tests/test_b.py'), tmp_path)
    assert result['ok'], merge_gate.describe(result)
    assert result['tests']['passed'] == 2 and result['tests']['failed'] == []


def test_a_failing_guard_fails_the_gate_and_is_named(tmp_path):
    root = tree(tmp_path, tests={'test_a.py': FAILING})
    result = run(root, ('tests/test_a.py',), tmp_path)
    assert not result['ok']
    assert [nodeid.split('::')[-1] for nodeid in result['tests']['failed']] == ['test_breaks']
    assert 'FAILED:' in merge_gate.describe(result) and 'test_breaks' in merge_gate.describe(result)


def test_a_deleted_guard_file_is_a_failure_not_a_skip(tmp_path):
    root = tree(tmp_path, tests={'test_a.py': PASSING})
    result = run(root, ('tests/test_a.py', 'tests/test_gone.py', 'tests/test_p9_*.py'), tmp_path)
    assert not result['ok']
    assert result['missing_guards'] == ['tests/test_gone.py', 'tests/test_p9_*.py']
    assert result['tests']['passed'] == 1  # the guards that exist still ran


def test_a_changed_or_missing_frozen_file_fails_the_gate(tmp_path):
    root = tree(tmp_path, tests={'test_a.py': PASSING}, frozen=b'edited\n')
    result = run(root, ('tests/test_a.py',), tmp_path)
    assert not result['ok']
    assert result['frozen_differences'] == [['frozen.py', RECORDED['frozen.py'], hashlib.sha256(b'edited\n').hexdigest()]]
    (root / 'frozen.py').unlink()
    assert merge_gate.frozen_differences(root, RECORDED) == [('frozen.py', RECORDED['frozen.py'], None)]


def test_tests_are_not_run_when_demiflow_would_come_from_another_checkout(tmp_path):
    root = tree(tmp_path, tests={'test_a.py': PASSING}, package=False)
    result = run(root, ('tests/test_a.py',), tmp_path)
    assert not result['ok'] and not result['tree_bound']
    assert result['tests'] is None
    assert 'NOT THIS TREE' in merge_gate.describe(result)


def test_a_glob_contributes_every_matching_file_once(tmp_path):
    root = tree(tmp_path, tests={'test_p1_a.py': PASSING, 'test_p1_b.py': PASSING, 'test_other.py': PASSING})
    files, missing = merge_gate.guard_files(root, ('tests/test_p1_*.py', 'tests/test_p1_a.py'))
    assert files == ['tests/test_p1_a.py', 'tests/test_p1_b.py'] and missing == []


def test_the_frozen_files_of_this_tree_are_the_recorded_ones():
    assert merge_gate.frozen_differences(ROOT) == []
    assert len(merge_gate.FROZEN) == 7
