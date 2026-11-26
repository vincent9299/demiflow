"""V035: exact inventory, explicit reviewed batches, real phase evidence.

Unknown fixtures are never auto-approved. Environment classification is a
scheduling declaration, not proof that the tests ran. This module owns test
subprocesses only; it creates no execution/runtime/resource authority.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

from runtime_v3_regression_inventory import ROOT, snapshot

META_NODE = 'tests/test_runtime_v3_acceptance.py::test_all_regressions'
ENVIRONMENTS = frozenset({'local', 'ray', 'browser', 'postgres', 'gpu', 'two_hosts'})
SCHEMA = 'demiflow.regression_execution.v1'


def read_json(path):
    path = Path(path)
    if not path.is_file() or path.stat().st_size > 64 * 2**20:
        raise ValueError('regression evidence must be a bounded regular JSON file')
    return json.loads(path.read_text())


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _private(path):
    path = Path(path).resolve()
    if not path.is_relative_to('/tmp') or len(path.parts) < 3 or not path.parts[2].startswith('demiflow-'):
        raise ValueError('regression subprocess writes require an owned /tmp/demiflow-* directory')
    return path


def _bound(receipt, inputs):
    return (receipt.get('source_unchanged') is True
            and receipt.get('source_before', {}).get('inputs') == inputs
            and receipt.get('source_after', {}).get('inputs') == inputs)


def inventory_union(receipts, *, inputs):
    """Merge explicit environment collection; unresolved imports remain visible."""
    nodes, problems = {}, []
    if not any({'tests', 'tests/'}.intersection(r.get('args', ())) for r in receipts):
        raise ValueError('inventory must include complete repository collection')
    for receipt in receipts:
        if receipt.get('kind') != 'collection_only' or not _bound(receipt, inputs):
            raise ValueError('collection is not bound to the current candidate inputs')
        if receipt.get('returncode') != 0 or receipt.get('deselected'):
            raise ValueError('collection failed or silently deselected tests')
        if '--collect-only' not in receipt.get('args', ()):
            raise ValueError('inventory receipt lacks a real collection invocation')
        seen = set()
        for item in receipt['items']:
            node = item['nodeid']
            if node in seen:
                raise ValueError('duplicate collected nodeid in one interpreter')
            seen.add(node)
            if node in nodes and nodes[node]['file'] != item['file']:
                raise ValueError('conflicting environment collection')
            nodes[node] = item
        problems.extend(receipt.get('collection_problems', ()))
    if not nodes:
        raise ValueError('empty regression inventory')
    # A module import skip is closed only when another interpreter really
    # collects its test cases. Arbitrary collection errors are never erased.
    unresolved = [p for p in problems if p['outcome'] != 'skipped'
                  or not any(n.split('::')[0] == p['nodeid'] for n in nodes)]
    accounted_files = {item['file'] for item in nodes.values()} | {p['nodeid'] for p in unresolved}
    expected_files = {str(path.relative_to(ROOT)) for path in ROOT.joinpath('tests').rglob('test*.py')}
    baseline = read_json(ROOT / 'tests/runtime_v3_package/evidence/regression-files.json')
    expected_files.update(item['file'] for item in baseline['files'])
    for name in sorted(expected_files):
        if name not in accounted_files:
            unresolved.append({'nodeid': name, 'outcome': 'uncollected',
                               'detail': 'repository test file absent from all environment collections'})
    return nodes, unresolved


def disposition_map(universe):
    """All 44 old failures have exact executable assertions, including params."""
    path = ROOT / 'tests/runtime_v3_package/evidence/historical-dispositions.json'
    replacements = {
        'test_every_explicit_option_rejects_lazy_actions_before_reads':
            'test_every_explicit_option_supports_lazy_actions_without_eager_reads',
        'test_checkpoint_bridges_map_only_and_replays_without_calls':
            'test_retired_checkpoint_is_rejected_and_native_run_replays_without_calls',
        'test_stream_sink_checkpoint_accepts_thread_map_policy_change':
            'test_stream_native_recovery_preserves_sealed_policy_and_new_run_can_append',
        'test_nested_relations_and_lazy_workers_reject_before_source_access':
            'test_nested_relations_support_stream_options_without_eager_source_access',
        'test_lance_checkpoint_writer_failure_joins_stream_and_closes_workers':
            'test_native_lance_writer_failure_joins_stream_and_closes_workers',
    }
    results = []
    for case in read_json(path)['cases']:
        node = case['nodeid']
        new = node
        for old, target in replacements.items():
            new = new.replace('::' + old, '::' + target)
        if new not in universe:
            raise ValueError('historical replacement is not collected: ' + new)
        results.append({**case, 'replacement_nodeids': [new],
            'rationale': ('Original defect assertion retained.' if new == node else
                'Retired split execution/checkpoint contract is replaced by explicit rejection and real unified behavior; parameter suffix retained.')})
    if len(results) != 44 or len({r['nodeid'] for r in results}) != 44:
        raise ValueError('historical failure inventory differs from the frozen 44 cases')
    return results


def make_plan(collections, *, reviewed_files=None):
    """Create a reviewable full manifest. Unreviewed is deliberately the default."""
    inputs = snapshot()['inputs']
    nodes, problems = inventory_union(collections, inputs=inputs)
    reviewed_files = reviewed_files or {}
    entries = []
    for node, item in sorted(nodes.items()):
        review = reviewed_files.get(item['file'])
        if node == META_NODE:
            entry = {'state': 'meta', 'environment': None,
                     'reason': 'This outer aggregation gate must not recursively launch itself.'}
        elif review:
            if review.get('environment') not in ENVIRONMENTS or not review.get('reason'):
                raise ValueError('reviewed file needs explicit environment and isolation rationale')
            entry = {'state': 'reviewed', **review}
        else:
            entry = {'state': 'unreviewed', 'environment': None,
                     'reason': 'Fixture ownership and required environment have not been audited.'}
        entries.append({'nodeid': node, 'file': item['file'], **entry})
    return {'schema': SCHEMA, 'inputs': inputs, 'entries': entries,
            'collection_problems': problems, 'dispositions': disposition_map(nodes),
            'batches': []}


def validate_plan(plan, collections, *, inputs):
    if plan.get('schema') != SCHEMA or plan.get('inputs') != inputs:
        raise ValueError('regression plan is stale or has an unsupported schema')
    universe, problems = inventory_union(collections, inputs=inputs)
    entries = plan.get('entries', [])
    bynode = {e['nodeid']: e for e in entries}
    if len(bynode) != len(entries) or set(bynode) != set(universe):
        raise ValueError('every collected nodeid needs exactly one explicit disposition')
    for node, entry in bynode.items():
        if entry.get('file') != universe[node]['file'] or not entry.get('reason'):
            raise ValueError('regression entry lacks its actual file or review rationale')
        if entry.get('state') not in {'meta', 'reviewed', 'unreviewed', 'environment_missing'}:
            raise ValueError('unknown regression disposition')
        if (entry['state'] == 'meta') != (node == META_NODE):
            raise ValueError('only the exact outer regression gate may avoid recursive execution')
        if entry['state'] == 'reviewed' and entry.get('environment') not in ENVIRONMENTS:
            raise ValueError('reviewed node has no known environment')
    if META_NODE not in universe:
        raise ValueError('full collection omitted the outer regression gate')
    claimed = set()
    for batch in plan.get('batches', []):
        nodes, environment = batch.get('nodeids', []), batch.get('environment')
        if (type(nodes) is not list or not nodes or environment not in ENVIRONMENTS
                or len(set(nodes)) != len(nodes) or claimed.intersection(nodes)
                or any(n not in bynode or bynode[n]['state'] != 'reviewed'
                       or bynode[n]['environment'] != environment for n in nodes)):
            raise ValueError('planned batch is unreviewed, recursive, duplicated or has wrong environment')
        claimed.update(nodes)
    if plan.get('dispositions') != disposition_map(universe):
        raise ValueError('historical mapping must retain exact replacement assertions')
    return universe, bynode, problems


def complete_phases(phases):
    """One parent execution and every explicitly identified native subtest pass.

    Old ambiguous repeated call reports stay invalid. Subtests cannot replace
    the parent's call, suppress failures, or make retry/duplicate parents green.
    """
    main, subtests = [], []
    for phase in phases:
        subtest = phase.get('subtest')
        if subtest is None:
            main.append(phase)
            continue
        if (type(subtest) is not dict or set(subtest) != {'schema', 'index', 'message', 'parameters'}
                or subtest['schema'] != 'pytest.subtest.v1' or phase['when'] != 'call'
                or type(subtest['index']) is not int or subtest['index'] != len(subtests) + 1
                or subtest['index'] > 100000
                or subtest['message'] is not None and (type(subtest['message']) is not str or len(subtest['message']) > 4096)
                or type(subtest['parameters']) is not dict or len(subtest['parameters']) > 64
                or any(type(k) is not str or len(k) > 256 or type(v) is not str or len(v) > 4096
                       for k, v in subtest['parameters'].items())):
            return False
        subtests.append(phase)
    return (len(main) == 3 and Counter(p['when'] for p in main)
            == Counter({'setup': 1, 'call': 1, 'teardown': 1})
            and all(p['outcome'] == 'passed' and p.get('wasxfail') is None
                    and not p.get('rerun') for p in phases))


def evaluate(plan, collections, receipts, *, inputs):
    """A complete verdict requires one setup/call/teardown success per leaf.

    Whole-run prior failures, retries, duplicate reports and xfail are never
    overwritten by a later success. Independent failed development receipts do
    not belong to the final candidate set.
    """
    universe, entries, problems = validate_plan(plan, collections, inputs=inputs)
    executed, failed, missing = {}, {}, {}
    for receipt in receipts:
        if receipt.get('kind') != 'execution' or not _bound(receipt, inputs):
            raise ValueError('execution receipt is not bound to the current candidate inputs')
        if receipt.get('deselected') or receipt.get('collection_problems'):
            raise ValueError('execution collection was skipped, failed or deselected')
        collected = [item['nodeid'] for item in receipt['items']]
        if len(collected) != len(set(collected)) or not set(collected) <= set(universe):
            raise ValueError('duplicate or unclassified executed nodeid')
        if any(entries[n]['state'] != 'reviewed' for n in collected):
            raise ValueError('receipt executes a fixture not approved by this plan')
        if META_NODE in collected:
            raise ValueError('regression gate cannot recursively execute itself')
        reports = defaultdict(list)
        for phase in receipt['reports']:
            if phase['nodeid'] not in collected:
                raise ValueError('phase report has no matching collected item')
            reports[phase['nodeid']].append(phase)
        for node in collected:
            if node in executed or node in failed:
                raise ValueError('repeated execution receipt cannot replace an earlier result')
            phases = reports[node]
            good = complete_phases(phases)
            (executed if good else failed)[node] = phases
        if receipt.get('returncode') != 0 and not any(n in failed for n in collected):
            raise ValueError('nonzero pytest exit without an accounted failed node')
    for node, entry in entries.items():
        if node == META_NODE:
            continue
        if node not in executed and node not in failed:
            missing[node] = {'state': entry['state'], 'environment': entry['environment'],
                             'reason': entry['reason']}
    replacements = {node for d in plan['dispositions'] for node in d['replacement_nodeids']}
    return {'complete': not missing and not failed and not problems,
        'collection': {'exit_code': 0, 'nodeid_count': len(universe),
            'file_count': len({i['file'] for i in universe.values()}),
            'baseline_file_count': read_json(ROOT / 'tests/runtime_v3_package/evidence/regression-files.json')['count']},
        'dispositions': [{**d, 'passed': all(n in executed for n in d['replacement_nodeids'])}
                         for d in plan['dispositions']],
        'executed': executed, 'failed': failed, 'not_run': missing,
        'collection_problems': problems, 'meta_nodeids': [META_NODE],
        'historical_replacements_passed': replacements <= executed.keys()}


def execute_batch(batch, *, entries, destination, timeout=1800):
    """Launch only a reviewed exact batch, sequentially, in four owned domains."""
    destination = _private(destination)
    nodes = batch.get('nodeids', [])
    environment = batch.get('environment')
    if environment not in ENVIRONMENTS or type(nodes) is not list or not nodes:
        raise ValueError('batch requires an explicit known environment and exact selectors')
    if len(set(nodes)) != len(nodes) or any(n == META_NODE or n not in entries for n in nodes):
        raise ValueError('batch has duplicate, unknown or recursive selectors')
    if any(entries[n]['state'] != 'reviewed' or entries[n]['environment'] != environment for n in nodes):
        raise ValueError('batch would execute an unreviewed fixture or mismatched environment')
    # Hardware/remote/browser execution is separately scheduled by the owner.
    # Only an explicit opt-in on this invocation permits these batches.
    enabled = set(os.environ.get('DEMIFLOW_REGRESSION_ENVIRONMENTS', 'local').split(','))
    if environment not in enabled:
        return {'not_run': 'environment not enabled for this invocation', 'environment': environment}
    if environment == 'postgres' and not os.environ.get('DEMIFLOW_TEST_POSTGRES_DSN'):
        return {'not_run': 'PostgreSQL test DSN not provided', 'environment': environment}
    python = Path(batch.get('python', sys.executable)).absolute()
    if not python.is_file() or python.name not in {'python', 'python3', 'python3.11', 'python3.12'}:
        raise ValueError('batch requires an explicit Python interpreter')
    if type(timeout) not in (int, float) or not 0 < timeout <= 7200:
        raise ValueError('regression subprocess deadline must be explicit and bounded')
    destination.mkdir(parents=True, exist_ok=False)
    request = destination / 'request.json'
    request.write_text(json.dumps({'destination': str(destination), 'selectors': nodes}) + '\n')
    env = dict(os.environ)
    env.pop('PYTEST_ADDOPTS', None)
    scratch = destination / 'scratch'
    scratch.mkdir()
    env.update(PYTHONPATH=str(ROOT) + os.pathsep + str(ROOT / 'tests'),
               PYTHONDONTWRITEBYTECODE='1', TMPDIR=str(scratch), TEMP=str(scratch), TMP=str(scratch))
    for key, suffix in [('DEMIFLOW_RESOURCE_ROOT', 'resources'), ('DEMIFLOW_RESOURCE_HOME', 'home'),
                        ('DEMIFLOW_FORGE_DOMAIN', 'forge'), ('DEMIFLOW_LOCATION_CACHE', 'locations')]:
        env[key] = str(destination / suffix)
    for key, suffix in [('FORGE_COMBINED_SCREENSHOTS', 'screenshots'), ('FORGE_WORKBENCH_EVIDENCE', 'workbench'),
                        ('FORGE_ACCEPTANCE_DIR', 'acceptance'), ('FORGE_OUTPUT_EVIDENCE', 'output')]:
        env[key] = str(destination / suffix)
    argv = [str(python), '-P', '-B', str(ROOT / 'tests/runtime_v3_regression_inventory.py'), str(request)]
    with (destination / 'stdout.txt').open('w') as stdout, (destination / 'stderr.txt').open('w') as stderr:
        process = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise TimeoutError('owned regression batch exceeded its explicit deadline')
    result = read_json(destination / 'receipt.json')
    if result['returncode'] != code or {n['nodeid'] for n in result['items']} != set(nodes):
        raise ValueError('subprocess executed a different batch or returned an inconsistent receipt')
    return {'receipt': str(destination / 'receipt.json'), 'sha256': sha256(destination / 'receipt.json'),
            'argv': argv, 'environment': environment}


def all_regressions(fx, fault):
    from runtime_v3_support import NotRunError
    path = os.environ.get('DEMIFLOW_REGRESSION_EXECUTION_PLAN')
    if not path:
        detail = {'missing_semantic_harness': 'reviewed candidate-bound exact regression execution plan required'}
        fx._record_not_run('all_regressions', detail)
        raise NotRunError('all_regressions', detail)
    plan = read_json(path)
    collections = [read_json(item['path']) for item in plan['collection_receipts']
                   if sha256(item['path']) == item['sha256']]
    if len(collections) != len(plan['collection_receipts']):
        raise ValueError('collection receipt digest changed')
    inputs = snapshot()['inputs']
    _, entries, _ = validate_plan(plan, collections, inputs=inputs)
    receipts = []
    for item in plan.get('execution_receipts', []):
        if sha256(item['path']) != item['sha256']:
            raise ValueError('execution receipt digest changed')
        receipts.append(read_json(item['path']))
    # Validate retained reports and all overlaps before starting any side effect.
    evaluate(plan, collections, receipts, inputs=inputs)
    reused = {item['nodeid'] for receipt in receipts for item in receipt['items']}
    if reused.intersection(n for batch in plan['batches'] for n in batch['nodeids']):
        raise ValueError('planned execution duplicates an existing candidate receipt')
    batches = []
    for index, batch in enumerate(plan['batches']):
        result = execute_batch(batch, entries=entries, destination=fx.root / ('regressions-' + str(index)))
        batches.append(result)
        if 'receipt' in result:
            receipts.append(read_json(result['receipt']))
    result = evaluate(plan, collections, receipts, inputs=inputs)
    result['batches'] = batches
    (fx.root / 'regression-verdict.json').write_text(json.dumps(result, indent=2) + '\n')
    if result['failed']:
        raise AssertionError('regression nodes failed: ' + ', '.join(result['failed']))
    if not result['complete']:
        detail = {'missing': 'classified regression execution incomplete',
            'not_run_count': len(result['not_run']), 'collection_problems': result['collection_problems'],
            'verdict': str(fx.root / 'regression-verdict.json')}
        fx._record_not_run('all_regressions', detail)
        raise NotRunError('all_regressions', detail)
    return result
