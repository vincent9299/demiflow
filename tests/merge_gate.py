"""The fixed checks a merge into the integration line has to pass. About a minute.

usage: python tests/merge_gate.py [--list] [--json <out.json>]

Run it in the tree that holds the merge result, with the interpreter the tests use. It

  1. confirms in a child process that ``demiflow`` is imported from this tree,
  2. compares the frozen files with their recorded digests,
  3. runs the guard tests in one pytest process with private temporary directories.

Exit code 0 only when all three hold. A guard file that is missing is a failure, not a skip:
a guard that was deleted no longer guards. Skipped mandatory guards do not count as passing.

To change a frozen file or the set of guards on purpose, change FROZEN or GUARDS in the same
merge and say why in the commit message.
"""
import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ElementTree
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# What may cross a package or module boundary, and the inventories that record it.
GUARDS = (
    'tests/test_package_dependency_ratchet.py',
    'tests/test_package_cold_imports.py',
    'tests/test_workbench_engine_boundary.py',
    'tests/test_workbench_project_boundary.py',
    'tests/test_p1_*.py',
    'tests/test_p4_managed_cleanup.py',
    'tests/test_compile_contract_inventory.py',
    'tests/test_dataflow_compile_inputs.py',
    'tests/test_dataflow_configuration.py',
    'tests/test_pre_v3_retention.py',
    'tests/test_engine_known_state.py',
    'tests/test_merge_gate.py',
)

# The seven files no change may touch without an explicit decision.
FROZEN = {
    'demiflow/execution/raw_fetch.py': '55bbc1e592fbe285b54d174d79f9cd0faa3d2f4beb6d41dc43d588daba596493',
    'demiflow/execution/queue_execution.py': '2a9851ce7df32da80906a859ef61080ea7dc5535d543a68953ab6cf71666a7c2',
    'demiflow/execution/queue_sinks.py': 'b44601ef8742301c4f0ec11e1f11f14a2dafb5cbd5a96e1475d2d0e237828074',
    'demiflow/execution/queue_source.py': '1c3717b3e5db84c92d0d249870ddf720fe79481b1ede4e6b885a6431f1bbd307',
    'demiflow/execution/contracts/exchange.py': 'dac2f80fce4b4d4b3bd8b7f2cd14e31b8189590baeeeba6182310464f18cb6b2',
    'demiflow/planning/dag.py': '294d2de6a417d611470b7e4b00cf9c4b887377988c63a6f80b0a2d7e41d4c48b',
    'demiflow/resources/contracts.py': '61ff1db526a0d7a669fb63f6fb72a8fe6243dadaa9be14671527203f146225ff',
}

PRIVATE_DIRECTORIES = {'DEMIFLOW_RESOURCE_ROOT': 'resources', 'DEMIFLOW_RESOURCE_HOME': 'home',
                       'DEMIFLOW_FORGE_DOMAIN': 'forge', 'DEMIFLOW_LOCATION_CACHE': 'locations'}


def guard_files(root, patterns=GUARDS):
    """The guard test files in this tree, and the patterns that match nothing."""
    files, missing = [], []
    for pattern in patterns:
        found = sorted(str(path.relative_to(root)) for path in root.glob(pattern))
        if not found:
            missing.append(pattern)
        files.extend(name for name in found if name not in files)
    return files, missing


def frozen_differences(root, frozen=FROZEN):
    """Frozen files whose content is not the recorded one: (path, recorded, found or None)."""
    differences = []
    for name, recorded in frozen.items():
        path = root / name
        found = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
        if found != recorded:
            differences.append((name, recorded, found))
    return differences


def environment(root, scratch):
    env = dict(os.environ)
    env.pop('PYTEST_ADDOPTS', None)
    temporary = scratch / 'scratch'
    temporary.mkdir(parents=True, exist_ok=True)
    env.update(PYTHONPATH=os.pathsep.join((str(root), str(root / 'tests'))), PYTHONDONTWRITEBYTECODE='1',
               TMPDIR=str(temporary), TEMP=str(temporary), TMP=str(temporary),
               OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1')
    for key, name in PRIVATE_DIRECTORIES.items():
        (scratch / name).mkdir(parents=True, exist_ok=True)
        env[key] = str(scratch / name)
    return env


def imported_from(root, python, env):
    """Where a child process imports ``demiflow`` from, or the reason it cannot."""
    child = subprocess.run([python, '-P', '-B', '-c', 'import demiflow; print(demiflow.__file__)'],
                           cwd=root, env=env, capture_output=True, text=True)
    if child.returncode:
        return None, (child.stderr.strip().splitlines() or ['import failed'])[-1]
    return Path(child.stdout.strip()).resolve(), ''


def read_junit(path):
    counts = {'passed': 0, 'skipped': 0}
    failed = []
    if not path.is_file():
        return counts, failed
    for case in ElementTree.parse(path).getroot().iter('testcase'):
        kinds = {child.tag for child in case}
        nodeid = f"{case.get('classname', '')}::{case.get('name', '')}"
        if kinds & {'failure', 'error'}:
            failed.append(nodeid)
        elif 'skipped' in kinds:
            counts['skipped'] += 1
        else:
            counts['passed'] += 1
    return counts, failed


def run_guards(root, python, files, env, scratch):
    """One pytest process over the guard files: counts, failed test ids, return code."""
    report = scratch / 'guards.xml'
    child = subprocess.run([python, '-P', '-B', '-m', 'pytest', '-q', '-p', 'no:cacheprovider',
                            '--basetemp', str(scratch / 'pytest'), '--junitxml', str(report), *files],
                           cwd=root, env=env, capture_output=True, text=True)
    counts, failed = read_junit(report)
    tail = child.stdout.strip().splitlines()[-1:] if child.stdout.strip() else []
    return {'returncode': child.returncode, **counts, 'failed': failed, 'summary': tail[0] if tail else ''}


def gate(root=ROOT, python=sys.executable, patterns=GUARDS, frozen=FROZEN, scratch_parent='/tmp'):
    scratch = Path(tempfile.mkdtemp(prefix='demiflow-gate-', dir=scratch_parent))
    try:
        env = environment(root, scratch)
        files, missing = guard_files(root, patterns)
        origin, reason = imported_from(root, python, env)
        bound = origin is not None and origin.is_relative_to(root.resolve())
        result = {'tree': str(root), 'imported_from': str(origin) if origin else reason, 'tree_bound': bound,
                  'frozen_differences': [list(item) for item in frozen_differences(root, frozen)],
                  'missing_guards': missing, 'guard_files': files}
        result['tests'] = run_guards(root, python, files, env, scratch) if files and bound else None
        tests = result['tests']
        result['ok'] = bool(bound and not result['frozen_differences'] and not missing
                            and tests and tests['returncode'] == 0 and not tests['failed']
                            and tests['skipped'] == 0)
        result['diagnostics'] = {'status': 'not_requested'}
        return result
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def diagnostics_process(command):
    """Supervise only the independent diagnostic process and its Git child."""
    child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             text=True, start_new_session=True)
    try:
        stdout, stderr = child.communicate(timeout=90)
        return subprocess.CompletedProcess(command, child.returncode, stdout, stderr)
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.communicate()


def run_diagnostics(root, python, base=None, candidate=None, output=None, *, runner=None):
    """Optional report evidence; never reads or assigns the guard result's ok field."""
    values = {'base': base, 'candidate': candidate, 'output_dir': output}
    requested = {}
    for name, value in values.items():
        if value is None:
            requested[name] = None
        else:
            text = str(value)
            maximum = 4096 if name == 'output_dir' else 64
            requested[name] = text if len(text) <= maximum else {'prefix': text[:64], 'characters': len(text)}
    if all(value is None for value in values.values()):
        return {'status': 'not_requested'}
    failed = {'status': 'failed', 'requested': requested}
    if any(value is None for value in values.values()):
        return dict(failed, error={'code': 'invalid_configuration', 'message': 'All three diagnostic options are required'})
    if any(not isinstance(v, str) or len(v) > 64 for v in (base, candidate)) or len(str(output)) > 4096:
        return dict(failed, error={'code': 'invalid_configuration', 'message': 'Diagnostic SHA/path argument exceeds input bounds'})
    try:
        command = [python, '-I', '-B', str(root / 'tools/architecture_diagnostics.py'),
                   '--repository', str(root), '--base', base, '--candidate', candidate,
                   '--output-dir', str(output)]
        child = (runner or diagnostics_process)(command)
        if len(child.stdout.encode()) > 65536 or len(child.stderr.encode()) > 65536:
            raise ValueError('Diagnostic process output exceeds 64 KiB')
        value = json.loads(child.stdout)
        if child.returncode or value.get('status') != 'complete':
            return dict(failed, error=value.get('error') or {'code': 'process_failed', 'message': f'Diagnostic exited {child.returncode}'})
        for label, expected in (('base', base), ('candidate', candidate)):
            if value['inputs'][label]['requested_sha'] != expected:
                raise ValueError('Diagnostic input binding mismatch')
        expected_names = {'scc-membership.json', 'declaration-upward-imports.json'}
        if len(value['files']) != 2 or {Path(r['path']).name for r in value['files']} != expected_names:
            raise ValueError('Missing diagnostic reports')
        for record in value['files']:
            path = Path(record['path'])
            if path.resolve() != (Path(output)/path.name).resolve() or path.is_symlink():
                raise ValueError('Diagnostic report path mismatch')
            if not 0 < path.stat().st_size <= 8*1024**2 or path.stat().st_size != record['bytes']:
                raise ValueError('Diagnostic report size mismatch')
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(65536), b''):
                    digest.update(chunk)
            if digest.hexdigest() != record['sha256']:
                raise ValueError('Diagnostic report digest mismatch')
        receipt = Path(output)/'receipt.json'
        if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 65536:
            raise ValueError('Missing or oversized diagnostic receipt')
        with receipt.open('rb') as stream:
            raw = stream.read(65537)
        if len(raw) > 65536 or json.loads(raw) != value:
            raise ValueError('Diagnostic receipt mismatch')
        return dict(value, receipt_path=str(receipt.absolute()), receipt_sha256=hashlib.sha256(raw).hexdigest())
    except Exception as exc:
        return dict(failed, error={'code': 'timeout' if isinstance(exc, subprocess.TimeoutExpired) else 'diagnostic_error',
                                  'message': str(exc).encode()[:4096].decode(errors='replace')})


def describe(result):
    lines = [f"tree: {result['tree']}"]
    lines.append('demiflow is imported from this tree' if result['tree_bound']
                 else f"NOT THIS TREE: demiflow comes from {result['imported_from']}")
    for name, recorded, found in result['frozen_differences']:
        lines.append(f"FROZEN FILE CHANGED: {name} is {found or 'missing'}, recorded {recorded}")
    if not result['frozen_differences']:
        lines.append('frozen files: unchanged')
    for pattern in result['missing_guards']:
        lines.append(f'GUARD MISSING: {pattern}')
    tests = result['tests']
    if tests is None:
        lines.append('guard tests: not run')
    else:
        lines.append(f"guard tests: {tests['passed']} passed, {len(tests['failed'])} failed, "
                     f"{tests['skipped']} skipped in {len(result['guard_files'])} files")
        lines.extend(f'FAILED: {nodeid}' for nodeid in tests['failed'])
        if tests['skipped']:
            lines.append('Skipped mandatory guards do not count as passing')
        if tests['returncode'] and not tests['failed']:
            lines.append(f"pytest exited {tests['returncode']}: {tests['summary']}")
    diagnostics = result.get('diagnostics', {'status': 'not_requested'})
    lines.append('architecture diagnostics: ' + diagnostics['status']
                 + (' (not generated)' if diagnostics['status'] == 'not_requested' else ''))
    if diagnostics['status'] == 'failed':
        lines.append('DIAGNOSTIC FAILED (report only): ' + str(diagnostics.get('error', {})))
    lines.append('MERGE GATE: ' + ('passed' if result['ok'] else 'FAILED'))
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--list', action='store_true', help='print the guard files and frozen files, run nothing')
    parser.add_argument('--json', metavar='OUT', help='also write the result as JSON')
    parser.add_argument('--diagnostics-base', help='full released-main commit SHA; report only')
    parser.add_argument('--diagnostics-candidate', help='full candidate commit or staged tree SHA; report only')
    parser.add_argument('--diagnostics-dir', help='new external report directory; never overwrite')
    options = parser.parse_args(argv)
    if options.list:
        files, missing = guard_files(ROOT)
        print('\n'.join(files + [f'(missing) {pattern}' for pattern in missing] + sorted(FROZEN)))
        return 0
    result = gate()
    result['diagnostics'] = run_diagnostics(ROOT, sys.executable, options.diagnostics_base,
                                            options.diagnostics_candidate, options.diagnostics_dir)
    print(describe(result))
    if options.json:
        Path(options.json).write_text(json.dumps(result, indent=1) + '\n')
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    sys.exit(main())
