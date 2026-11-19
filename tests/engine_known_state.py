"""Known state of the engine tests at one candidate: what does not pass, where, and why.

The evidence is the receipts written by ``runtime_v3_regression_inventory.py`` (complete
collection, setup/call/teardown per nodeid, source digests before and after). This module
only reads receipts. It does two things:

    build    receipts -> a known-state document: per lane (one Python environment and one
             way of running) every collected nodeid, and for every nodeid that is not a
             plain pass its phase, failure type and a stable reason
    compare  a known-state document against the receipts of another run of one lane,
             of the same candidate or of a later one -> the exact differences by nodeid

A known-state entry keeps two separate facts apart:

    cause    which change made the test stop passing (found by bisecting); a location only
    ruling   who is right: the test, the product, or neither yet (decided by the owner)

A located cause is never a ruling. Nothing here edits tests, skips them or marks them
expected to fail; an entry that passes again, or whose nodeid no longer exists, is a
difference the comparison reports, and it moves to ``history`` with its mapping.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from pathlib import Path

SCHEMA = 'demiflow_engine_known_state_v1'
PHASES = ('setup', 'call', 'teardown')
VERDICTS = ('undetermined', 'test_mismatch', 'product_regression', 'scenario_must_be_rebuilt',
            'retired_contract_needs_mapping', 'environment_not_run')
DIFFERENCES = ('files_not_collected', 'collected_only_now', 'no_longer_collected', 'new_non_pass', 'now_passing',
               'changed', 'not_reported')
PRIVATE = ('DEMIFLOW_RESOURCE_ROOT', 'DEMIFLOW_RESOURCE_HOME', 'DEMIFLOW_FORGE_DOMAIN', 'DEMIFLOW_LOCATION_CACHE',
           'TMPDIR')


def digest_inputs(inputs):
    """One digest for the whole source snapshot of a receipt."""
    body = json.dumps(sorted(inputs.items()), separators=(',', ':')).encode()
    return hashlib.sha256(body).hexdigest()


def file_digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


_NOISE = (
    # a mkdtemp name (demiflow-<what>-<8 random characters>) and a Ray session directory
    (re.compile(r'(?<=/)(demiflow-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*?-)[A-Za-z0-9_]{8}(?=/|\b)'), r'\1*'),
    (re.compile(r'session_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_\d+_\d+'), 'session_*'),
    (re.compile(r'0x[0-9a-f]{6,}'), '0x*'),
    (re.compile(r'\b\d{4,}\.\d{3,}\b'), '<float>'),
    (re.compile(r'(pid|PID)[ =:]+\d+'), r'\1 *'),
)


def run_paths(receipt):
    """The run's own directories, as the receipt recorded them: (literal, placeholder) pairs.

    The destination is the common parent of the private directories the run was given;
    the tree is the first PYTHONPATH entry. Both differ between two runs of the same code.
    """
    environment = receipt.get('environment') or {}
    pairs = []
    private = [environment[name] for name in PRIVATE if environment.get(name)]
    if private:
        destination = os.path.commonpath(private)
        if destination.count('/') >= 2:
            pairs.append((destination, '<destination>'))
    tree = (environment.get('PYTHONPATH') or '').split(os.pathsep)[0]
    if tree:
        pairs.append((tree, '<tree>'))
    return sorted(pairs, key=lambda pair: -len(pair[0]))


def stable_reason(detail, paths=()):
    """The exception line of a pytest long representation, without run-specific noise."""
    lines = [line[1:].strip() for line in (detail or '').splitlines() if line.startswith('E ')]
    text = lines[0] if lines else ((detail or '').strip().splitlines() or [''])[-1]
    for literal, placeholder in paths:
        text = text.replace(literal, placeholder)
    for pattern, replacement in _NOISE:
        text = pattern.sub(replacement, text)
    return text[:240]


def failure_type(reason):
    head = reason.split(':', 1)[0].strip()
    return head.rsplit('.', 1)[-1] if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.]*', head) else ''


def node_state(reports, paths=()):
    """Collapse the phase reports of one nodeid.

    Returns (outcome, phases, failure). outcome is passed / failed (the call failed) /
    error (setup or teardown failed) / skipped / incomplete (a phase is missing, e.g. the
    process died). A subtest report is folded into its phase: any failed subtest fails it,
    and the failure record binds how many reports failed and which.
    """
    phases = {}
    failure = None
    failed = []
    for report in reports:
        when, outcome = report['when'], report['outcome']
        if phases.get(when) != 'failed':
            phases[when] = outcome
        if outcome == 'failed':
            reason = stable_reason(report.get('detail'), paths)
            subtest = report.get('subtest') or {}
            which = json.dumps([subtest.get('message'), subtest.get('parameters')], sort_keys=True) if subtest else ''
            failed.append([when, which, reason])
            if failure is None:
                failure = {'when': when, 'type': failure_type(reason), 'reason': reason}
    if len(failed) > 1:
        # Several failed reports under one nodeid (subtests, or a call and its teardown): the
        # first one is shown, all of them are bound, so one more failing subtest is a change.
        failure['failed_reports'] = len(failed)
        failure['failed_reports_sha256'] = hashlib.sha256(
            json.dumps(sorted(failed), separators=(',', ':')).encode()).hexdigest()
    if 'setup' not in phases or (phases.get('setup') == 'passed' and ('call' not in phases or 'teardown' not in phases)):
        return 'incomplete', phases, failure
    if phases.get('setup') == 'failed' or phases.get('teardown') == 'failed':
        return 'error', phases, failure
    if phases.get('call') == 'failed':
        return 'failed', phases, failure
    if 'skipped' in phases.values():
        return 'skipped', phases, failure
    return 'passed', phases, failure


def read_collection(path):
    receipt = json.loads(Path(path).read_text())
    if receipt.get('kind') != 'collection_only' or not receipt.get('source_unchanged'):
        raise ValueError('not an unchanged-source collection receipt: ' + str(path))
    paths = run_paths(receipt)
    files = {}
    for item in receipt['items']:
        files.setdefault(item['file'], []).append(item['nodeid'].split('::', 1)[1])
    return {'head': receipt['source_before']['head'], 'inputs': digest_inputs(receipt['source_before']['inputs']),
            'files': files, 'problems': [{'nodeid': p['nodeid'], 'outcome': p['outcome'],
                                          'reason': stable_reason(p.get('detail'), paths)}
                                         for p in receipt['collection_problems']],
            'sha256': file_digest(path), 'python': receipt['python']}


def junit_skips(path):
    """{(classname, name): message} for skipped cases. The receipt records that a phase
    was skipped but not why; the junit file the same run wrote next to it does."""
    import xml.etree.ElementTree as ET
    out = {}
    path = Path(path)
    if not path.is_file():
        return out
    for case in ET.parse(path).getroot().iter('testcase'):
        for child in case:
            if child.tag == 'skipped':
                out[(case.get('classname', ''), case.get('name', ''))] = (child.get('message') or '')[:240]
    return out


def _junit_key(nodeid):
    parts = nodeid.split('::')
    module = parts[0][:-3].replace('/', '.')
    return ('.'.join([module] + parts[1:-1]), parts[-1])


def read_execution(path, *, head, inputs):
    """Per-nodeid states of one execution receipt, refused unless it is bound to the given source."""
    path = Path(path)
    receipt = json.loads(path.read_text())
    if receipt.get('kind') != 'execution':
        raise ValueError('not an execution receipt: ' + str(path))
    if not receipt.get('source_unchanged'):
        raise ValueError('source changed while executing: ' + str(path))
    if receipt['source_before']['head'] != head or digest_inputs(receipt['source_before']['inputs']) != inputs:
        raise ValueError('receipt is bound to another source snapshot: ' + str(path))
    paths = run_paths(receipt)
    reports = {}
    for report in receipt['reports']:
        reports.setdefault(report['nodeid'], []).append(report)
    skips = junit_skips(path.with_name('result.xml'))
    states = {}
    for item in receipt['items']:
        outcome, phases, failure = node_state(reports.get(item['nodeid'], []), paths)
        states[item['nodeid']] = {'outcome': outcome, 'phases': phases, 'failure': failure,
                                  'skip': skips.get(_junit_key(item['nodeid']), '') if outcome == 'skipped' else ''}
    return states, receipt


def read_run(collection_path, execution_roots):
    """One run of one lane: its collection receipt and every ``<root>/*/receipt.json``.

    A nodeid reported by two receipts is refused: the run would not say which one counts.
    """
    collection = read_collection(collection_path)
    states, receipts = {}, []
    for root in execution_roots:
        for path in sorted(Path(root).glob('*/receipt.json')):
            part, receipt = read_execution(path, head=collection['head'], inputs=collection['inputs'])
            overlap = set(part) & set(states)
            if overlap:
                raise ValueError('nodeid executed twice: ' + sorted(overlap)[0])
            states.update(part)
            receipts.append({'path': os.path.realpath(path), 'sha256': file_digest(path),
                             'files': sorted({nodeid.split('::', 1)[0] for nodeid in part})})
    return collection, states, receipts


def signature(state):
    """What must stay the same for a known entry to count as unchanged."""
    failure = state.get('failure') or {}
    return (state['outcome'], failure.get('when', ''), failure.get('type', ''), failure.get('reason', ''),
            failure.get('failed_reports', 1 if failure else 0), failure.get('failed_reports_sha256', ''))


def annotate(rules, nodeid, state):
    """(cause, ruling) of the first rule that matches; a rule matches on file, function,
    outcome and a fragment of the reason, whichever of them it names."""
    file, _, rest = nodeid.partition('::')
    function = rest.split('[')[0]
    reason = ((state.get('failure') or {}).get('reason') or '') + ' ' + (state.get('skip') or '')
    for rule in rules:
        match = rule['match']
        if match.get('lane') and match['lane'] != state.get('lane'):
            continue
        if match.get('file') and match['file'] != file:
            continue
        if match.get('function') and match['function'] != function:
            continue
        if match.get('outcome') and match['outcome'] != state['outcome']:
            continue
        if match.get('reason_contains') and match['reason_contains'] not in reason:
            continue
        return rule.get('cause'), rule.get('ruling')
    return None, None


def build(spec):
    """A known-state document from a build specification.

    spec = {'evidence_root': directory the receipt paths are recorded relative to (optional),
            'lanes': {name: {'description': str, 'collection': receipt path,
                             'executions': [directory, ...], 'confirm': [directory, ...]}},
            'annotations': {'rules': [...], 'history': [...], 'not_run': {...}, 'supplementary': [...]}}
    ``not_run`` names what was deliberately not executed; ``supplementary`` lists results
    obtained without a receipt (and says how), for nodeids the receipt tool cannot execute.
    Every lane must be bound to the same source snapshot. ``confirm`` is a second run of
    some nodeids; an entry is ``stable`` when both runs agree on outcome, phase, type and reason.
    """
    annotations = spec.get('annotations') or {}
    rules = annotations.get('rules', [])
    evidence_root = os.path.realpath(spec['evidence_root']) + os.sep if spec.get('evidence_root') else None
    lanes, entries, candidate = {}, [], None
    for name, lane in spec['lanes'].items():
        collection, states, receipts = read_run(lane['collection'], lane['executions'])
        source = {'head': collection['head'], 'inputs': collection['inputs']}
        if candidate is None:
            candidate = source
        elif source != candidate:
            raise ValueError('lane %s is bound to another source snapshot' % name)
        confirmed = read_run(lane['collection'], lane['confirm'])[1] if lane.get('confirm') else {}
        if evidence_root:
            for receipt in receipts:
                if receipt['path'].startswith(evidence_root):
                    receipt['path'] = receipt['path'][len(evidence_root):]
        counts = {}
        for state in states.values():
            counts[state['outcome']] = counts.get(state['outcome'], 0) + 1
        executed = sorted({nodeid.split('::', 1)[0] for nodeid in states})
        lanes[name] = {'description': lane.get('description', ''), 'python': collection['python'],
                       'collection': {'receipt_sha256': collection['sha256'], 'problems': collection['problems'],
                                      'files': collection['files']},
                       'collected': sum(len(names) for names in collection['files'].values()),
                       'executed_files': executed,
                       'collected_not_executed': sorted(set(collection['files']) - set(executed)),
                       'executed': len(states), 'outcomes': dict(sorted(counts.items())),
                       'execution_receipts': receipts}
        for nodeid in sorted(states):
            state = states[nodeid]
            if state['outcome'] == 'passed':
                continue
            entry = {'lane': name, 'nodeid': nodeid, 'outcome': state['outcome'], 'phases': state['phases'],
                     'failure': state['failure'], 'skip': state['skip']}
            if lane.get('confirm'):
                again = confirmed.get(nodeid)
                entry['stability'] = ('not_rerun' if again is None else
                                      'stable' if signature(again) == signature(state) else 'unstable')
                if entry['stability'] == 'unstable':
                    entry['second_run'] = list(signature(again))
            cause, ruling = annotate(rules, nodeid, dict(state, lane=name))
            entry['cause'] = cause
            entry['ruling'] = ruling or {'verdict': 'undetermined', 'by': None, 'required': None}
            if entry['ruling']['verdict'] not in VERDICTS:
                raise ValueError('unknown verdict %r for %s' % (entry['ruling']['verdict'], nodeid))
            entries.append(entry)
    return {'schema': SCHEMA, 'candidate': candidate, 'lanes': lanes, 'entries': entries,
            'not_run': annotations.get('not_run', {}), 'supplementary': annotations.get('supplementary', []),
            'history': annotations.get('history', [])}


def compare(known, lane, collection, states, *, partial=False):
    """Differences between one lane of a known-state document and another run of that lane.

    known      the document
    collection {file: [names]} from the new collection receipt
    states     {nodeid: state} from the new execution receipts
    partial    the new run deliberately covers only some files; files it did not collect
               are then not a difference
    Every list in the result is a difference that needs an explanation; all empty means
    the new run shows exactly the known state. A file that is no longer collected at all
    is reported once in ``files_not_collected`` rather than once per nodeid.
    """
    result = {key: [] for key in DIFFERENCES}
    before_files = known['lanes'][lane]['collection']['files']
    if not partial:
        result['files_not_collected'] = sorted(set(before_files) - set(collection))
    before = {file + '::' + name for file, names in before_files.items() if file in collection for name in names}
    after = {file + '::' + name for file, names in collection.items() for name in names}
    result['collected_only_now'] = sorted(after - before)
    result['no_longer_collected'] = sorted(before - after)
    entries = {entry['nodeid']: entry for entry in known['entries'] if entry['lane'] == lane}
    for nodeid in sorted(states):
        state = states[nodeid]
        entry = entries.get(nodeid)
        if entry is None:
            if state['outcome'] != 'passed':
                result['new_non_pass'].append([nodeid, list(signature(state))])
        elif state['outcome'] == 'passed':
            result['now_passing'].append(nodeid)
        elif signature(state) != signature(entry):
            result['changed'].append([nodeid, list(signature(entry)), list(signature(state))])
    executed_files = {nodeid.split('::', 1)[0] for nodeid in states}
    for nodeid, entry in sorted(entries.items()):
        if nodeid.split('::', 1)[0] in executed_files and nodeid not in states and nodeid in after:
            result['not_reported'].append(nodeid)
    return result


def is_clean(result):
    return not any(result.values())


def main(argv):
    """build   <spec.json> <out.json>
    compare <known-state.json> <lane> <collection receipt> <execution directory>... [--partial]

    build writes the document. compare prints the two candidates and the differences as
    JSON and exits 1 when there is any difference.
    """
    if len(argv) == 3 and argv[0] == 'build':
        spec = json.loads(Path(argv[1]).read_text())
        if isinstance(spec.get('annotations'), str):
            spec['annotations'] = json.loads(Path(spec['annotations']).read_text())
        document = build(spec)
        Path(argv[2]).write_text(json.dumps(document, ensure_ascii=False, indent=1) + '\n')
        for name, lane in document['lanes'].items():
            print(name, 'collected', lane['collected'], 'executed', lane['executed'], lane['outcomes'])
        print('entries', len(document['entries']))
        return 0
    partial = '--partial' in argv
    argv = [value for value in argv if value != '--partial']
    if len(argv) < 5 or argv[0] != 'compare':
        print(main.__doc__)
        return 2
    known = json.loads(Path(argv[1]).read_text())
    collection, states, _ = read_run(argv[3], argv[4:])
    run = {'head': collection['head'], 'inputs': collection['inputs']}
    result = compare(known, argv[2], collection['files'], states, partial=partial)
    print(json.dumps({'known_candidate': known['candidate']['head'], 'run_candidate': run['head'],
                      'same_source': run == known['candidate'], 'lane': argv[2], 'executed': len(states),
                      'differences': result}, indent=1, ensure_ascii=False))
    return 0 if is_clean(result) else 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
