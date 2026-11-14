"""Run an explicit acceptance gate and reject incomplete/skip-only evidence.

This verifies test execution, not semantic adequacy or overall product completion.
No business tests are added by this script. Missing future tests remain blocking.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from scope_guard import scope_mismatches

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT/'docs/upgrade-contracts-v2-20261113'
MAIN = Path('/yzp/zhaozy/yangzepeng/0905/demiflow').resolve()


def matches(selector, nodeid):
    return selector == nodeid or nodeid.startswith(selector+'[')


def evaluate(gate, report, exit_code):
    reasons = []
    required_not_passed = []
    if exit_code or report.get('pytest_exit_code', 99):
        reasons.append('pytest did not finish successfully')
    collected = report.get('collected', [])
    if not collected or report.get('collection_errors'):
        reasons.append('empty or incomplete collection')
    for selector in gate.get('selectors', []):
        selected = (any(n.startswith(selector+'::') for n in collected) if '::' not in selector
                    else any(matches(selector,n) for n in collected))
        if not selected:
            reasons.append('selector collected no tests: '+selector)
    extra_deselect = set(report.get('deselected', []))-set(gate['deselect'])
    if extra_deselect:
        reasons.append('undeclared deselection')
    passed = set()
    for nodeid in collected:
        phases = report.get('tests', {}).get(nodeid, [])
        by_phase = {p['phase']: p for p in phases}
        if (set(by_phase) == {'setup', 'call', 'teardown'}
                and all(p['outcome'] == 'passed' and not p['wasxfail'] for p in phases)):
            passed.add(nodeid)
        else:
            reasons.append('not fully passed: '+nodeid)
    for required in gate['required_nodeids']:
        actual = [n for n in collected if matches(required, n)]
        if not actual or not set(actual) <= passed:
            reasons.append('required test missing/not passed: '+required)
            required_not_passed.append(required)
    if len(passed) < gate['policy']['min_passed']:
        reasons.append('too few passed tests')
    failed_nodes = {node for node, phases in report.get('tests', {}).items()
                    if any(p['outcome']=='failed' for p in phases)}
    new_failures = sorted(node for node in failed_nodes
                          if not any(matches(required,node) for required in gate['required_nodeids']))
    return {'status': 'passed' if not reasons else 'not_accepted',
            'passed_count': len(passed), 'reasons': reasons,
            'required_not_passed': required_not_passed,
            'failing_nodes_outside_required_set': new_failures}


def frozen_mismatches(root, guard):
    return [name for name, digest in guard['files'].items()
            if not (root/name).is_file()
            or hashlib.sha256((root/name).read_bytes()).hexdigest() != digest]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('gate')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--timeout', type=int, default=1200)
    args = parser.parse_args()
    if ROOT.resolve() == MAIN or Path.cwd().resolve() != ROOT.resolve():
        parser.error('run from the isolated integration tree root; main checkout forbidden')
    dest = args.output.resolve()
    if dest.parent != Path('/tmp') or not dest.name.startswith('demiflow-'):
        parser.error('output must be a new /tmp/demiflow-* directory')
    dest.mkdir(exist_ok=False)
    gate_bytes = (OUT/'gates.json').read_bytes()
    gates = json.loads(gate_bytes)
    gate = gates[args.gate]
    guard = json.loads((OUT/'review-20261114/frozen-files.json').read_text())
    evidence = {'gate': args.gate, 'revision': subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'git_status': subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True),
        'python': sys.executable, 'depends_on': gate['depends_on'],
        'gate_config_sha256': hashlib.sha256(gate_bytes).hexdigest(),
        'scope': 'test gate only; dependent gates and product evidence separately required',
        'frozen_mismatches': frozen_mismatches(ROOT, guard),
        'symbol_scope_mismatches': scope_mismatches(ROOT, guard), 'rounds': []}
    # Dirty or untracked integration inputs invalidate same-revision comparisons.
    dirty = evidence['git_status']
    if dirty or evidence['frozen_mismatches'] or evidence['symbol_scope_mismatches']:
        evidence['status'] = 'blocked'
        evidence['reason'] = 'commit integration inputs and preserve frozen source before acceptance'
    else:
        for number in range(gate['repeat']):
            report_path = dest/f'pytest-{number+1}.json'
            command = [sys.executable, '-m', 'pytest', '-q', '-p', 'no:cacheprovider',
                       '-p', 'gate_plugin', '--basetemp='+str(dest/f'tmp-{number+1}'),
                       *gate['selectors']]
            for node in gate['deselect']:
                command += ['--deselect', node]
            env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1',
                       PYTHONPATH=str(ROOT)+os.pathsep+str(OUT/'tools'),
                       DEMIFLOW_GATE_ROOT=str(ROOT), DEMIFLOW_GATE_REPORT=str(report_path))
            # Inherited selection/addopts must not silently narrow this acceptance run.
            env.pop('PYTEST_ADDOPTS', None)
            with (dest/f'pytest-{number+1}.log').open('w') as log:
                try:
                    proc = subprocess.run(command, cwd=ROOT, env=env, stdout=log,
                                          stderr=subprocess.STDOUT, timeout=args.timeout)
                    code = proc.returncode
                except subprocess.TimeoutExpired:
                    code = 124
            report = json.loads(report_path.read_text()) if report_path.exists() else {}
            result = evaluate(gate, report, code)
            evidence['rounds'].append(dict(command=command, **result))
            if result['status'] != 'passed':
                break
        evidence['status'] = ('passed' if len(evidence['rounds']) == gate['repeat']
                              and all(r['status'] == 'passed' for r in evidence['rounds'])
                              else 'not_accepted')
        after = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)
        if after != dirty or frozen_mismatches(ROOT, guard) or scope_mismatches(ROOT,guard):
            evidence.update(status='not_accepted', reason='source changed during test run')
    (dest/'result.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({'gate': args.gate, 'status': evidence['status'], 'evidence': str(dest)}))
    return 0 if evidence['status'] == 'passed' else 1


if __name__ == '__main__':
    sys.exit(main())
