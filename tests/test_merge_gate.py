"""The merge gate fails for each thing it exists to catch, and names it.

A count of passed tests says nothing about a guard file that was deleted, a frozen file that
changed, or tests that ran against another checkout. Each case below is one of those.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

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


def test_a_skipped_guard_fails_the_gate_even_when_pytest_passes(tmp_path):
    skipping = 'import pytest\n\ndef test_skips():\n    pytest.skip("missing dependency")\n'
    root = tree(tmp_path, tests={'test_a.py': PASSING, 'test_b.py': skipping})
    result = run(root, ('tests/test_a.py', 'tests/test_b.py'), tmp_path)
    assert result['tests']['returncode'] == 0
    assert result['tests']['passed'] == 1
    assert result['tests']['skipped'] == 1
    assert result['tests']['failed'] == []
    assert not result['ok']
    assert 'Skipped mandatory guards' in merge_gate.describe(result)


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


def test_the_real_guard_roster_includes_the_entire_cold_import_file():
    files, missing = merge_gate.guard_files(ROOT)
    assert missing == []
    assert 'tests/test_package_cold_imports.py' in files
    assert 'tests/test_workbench_project_boundary.py' in files
    assert 'tests/test_dataflow_configuration.py' in files
    assert 'tests/test_configuration_schema_evidence.py' not in files


def diagnostic_reply(command):
    """Transport fixture only; scientific output is tested with real Git separately."""
    output = Path(command[-1])
    output.mkdir()
    files = []
    for name in ('scc-membership.json', 'declaration-upward-imports.json'):
        path = output/name
        raw = b'{"fixture":"diagnostic transport"}\n'
        path.write_bytes(raw)
        files.append({'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
    value = {'status': 'complete', 'inputs': {
        label: {'requested_sha': command[command.index('--'+label)+1]}
        for label in ('base', 'candidate')}, 'files': files}
    (output/'receipt.json').write_text(json.dumps(value))
    return subprocess.CompletedProcess(command, 0, json.dumps(value), '')


def test_diagnostics_not_requested_and_partial_configuration_are_explicit():
    assert merge_gate.run_diagnostics(ROOT, sys.executable) == {'status': 'not_requested'}
    result = merge_gate.run_diagnostics(ROOT, sys.executable, base='a'*40)
    assert result['status'] == 'failed' and result['error']['code'] == 'invalid_configuration'


def test_oversized_diagnostic_arguments_have_bounded_failure_evidence(tmp_path):
    def forbidden(command):
        raise AssertionError('Oversized arguments must be rejected before child launch')
    result = merge_gate.run_diagnostics(ROOT, sys.executable, 'x'*70000, 'y'*70000,
        str(tmp_path/'reports'), runner=forbidden)
    assert result['status'] == 'failed' and result['error']['code'] == 'invalid_configuration'
    assert result['requested']['base'] == {'prefix': 'x'*64, 'characters': 70000}
    assert len(json.dumps(result).encode()) < 65536


@pytest.mark.parametrize('fault', ['timeout', 'nonzero', 'corrupt', 'write', 'missing', 'digest', 'input'])
def test_diagnostic_failures_are_visible_not_empty_success(tmp_path, fault):
    def runner(command):
        if fault == 'timeout': raise subprocess.TimeoutExpired(command, 90)
        if fault == 'write': raise PermissionError('controlled output write failure')
        if fault == 'nonzero': return subprocess.CompletedProcess(command, 1, '{"status":"failed"}', '')
        if fault == 'corrupt': return subprocess.CompletedProcess(command, 0, '{broken', '')
        child = diagnostic_reply(command)
        value = json.loads(child.stdout)
        if fault == 'missing': Path(value['files'][0]['path']).unlink()
        if fault == 'digest': Path(value['files'][0]['path']).write_bytes(b'changed')
        if fault == 'input': value['inputs']['base']['requested_sha'] = 'c'*40
        child.stdout = json.dumps(value)
        return child
    result = merge_gate.run_diagnostics(ROOT, sys.executable, 'a'*40, 'b'*40, str(tmp_path/'reports'), runner=runner)
    assert result['status'] == 'failed' and result['error']


@pytest.mark.parametrize('guard_passes,diagnostic_completes', [(True, True), (False, True), (True, False), (False, False)])
def test_cli_exit_and_guard_ok_are_independent_of_diagnostics(tmp_path, monkeypatch, guard_passes, diagnostic_completes):
    root = tree(tmp_path, tests={'test_a.py': PASSING if guard_passes else FAILING})
    real_result = run(root, ('tests/test_a.py',), tmp_path)
    assert real_result['ok'] == guard_passes
    monkeypatch.setattr(merge_gate, 'gate', lambda: real_result)
    def runner(command):
        if diagnostic_completes:
            return diagnostic_reply(command)
        return subprocess.CompletedProcess(command, 1, '{"status":"failed","error":{"code":"fixture"}}', '')
    monkeypatch.setattr(merge_gate, 'diagnostics_process', runner)
    report = tmp_path/'gate.json'
    code = merge_gate.main(['--json', str(report), '--diagnostics-base', 'a'*40,
        '--diagnostics-candidate', 'b'*40, '--diagnostics-dir', str(tmp_path/'reports')])
    value = json.loads(report.read_text())
    assert code == (0 if guard_passes else 1)
    assert value['ok'] == guard_passes
    assert value['diagnostics']['status'] == ('complete' if diagnostic_completes else 'failed')
    if not diagnostic_completes:
        assert 'DIAGNOSTIC FAILED (report only)' in merge_gate.describe(value)


def test_diagnostic_timeout_terminates_only_the_owned_process_group(monkeypatch):
    calls = []
    class Child:
        pid = 123456789
        def __init__(self): self.returncode = None
        def communicate(self, timeout=None):
            calls.append(('communicate', timeout))
            if timeout == 90:
                raise subprocess.TimeoutExpired(['fixture'], timeout)
            self.returncode = -15
            return '', ''
        def poll(self): return self.returncode
    monkeypatch.setattr(merge_gate.subprocess, 'Popen', lambda *args, **kwargs: Child())
    monkeypatch.setattr(merge_gate.os, 'killpg', lambda pid, sig: calls.append(('killpg', pid, sig)))
    with pytest.raises(subprocess.TimeoutExpired):
        merge_gate.diagnostics_process(['fixture'])
    assert ('killpg', 123456789, merge_gate.signal.SIGTERM) in calls
    assert ('communicate', 2) in calls
