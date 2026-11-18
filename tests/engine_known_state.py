"""Known state of the engine tests at one candidate: what does not pass, where, and why.

The evidence is the receipts written by ``runtime_v3_regression_inventory.py`` (complete
collection, setup/call/teardown per nodeid, source digests before and after). This module
only reads receipts. It does two things:

    build    receipts -> a known-state document (every collected nodeid, every nodeid
             that is not a plain pass, with the phase, the failure type and a stable reason)
    compare  a known-state document against new receipts -> the exact differences

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
import re
import sys
from pathlib import Path

SCHEMA = 'demiflow_engine_known_state_v1'
PHASES = ('setup', 'call', 'teardown')
VERDICTS = ('undetermined', 'test_mismatch', 'product_regression', 'scenario_must_be_rebuilt',
            'retired_contract_needs_mapping', 'environment_not_run')


def digest_inputs(inputs):
    """One digest for the whole source snapshot of a receipt."""
    body = json.dumps(sorted(inputs.items()), separators=(',', ':')).encode()
    return hashlib.sha256(body).hexdigest()


def file_digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


_NOISE = (
    (re.compile(r'/tmp/demiflow-claude-[a-z0-9]+/[a-z0-9]+/f/\d+'), '<destination>'),
    (re.compile(r'/tmp/demiflow-claude-[a-z0-9]+/tree'), '<tree>'),
    (re.compile(r'(/tmp/demiflow-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*?-)[A-Za-z0-9_]{8}\b'), r'\1*'),
    (re.compile(r'0x[0-9a-f]{6,}'), '0x*'),
    (re.compile(r'\b\d{4,}\.\d{3,}\b'), '<float>'),
    (re.compile(r'(pid|PID)[ =:]+\d+'), r'\1 *'),
)


def stable_reason(detail):
    """The exception line of a pytest long representation, without run-specific noise."""
    lines = [line[1:].strip() for line in (detail or '').splitlines() if line.startswith('E ')]
    text = lines[0] if lines else ((detail or '').strip().splitlines() or [''])[-1]
    for pattern, replacement in _NOISE:
        text = pattern.sub(replacement, text)
    return text[:240]


def failure_type(reason):
    head = reason.split(':', 1)[0].strip()
    return head.rsplit('.', 1)[-1] if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.]*', head) else ''


def node_state(reports):
    """Collapse the phase reports of one nodeid.

    Returns (outcome, phases, failure). outcome is passed / failed (the call failed) /
    error (setup or teardown failed) / skipped / incomplete (a phase is missing, e.g. the
    process died). A subtest report is folded into its phase: any failed subtest fails it.
    """
    phases = {}
    failure = None
    skip = None
    for report in reports:
        when, outcome = report['when'], report['outcome']
        if phases.get(when) != 'failed':
            phases[when] = outcome
        if outcome == 'failed' and failure is None:
            reason = stable_reason(report.get('detail'))
            failure = {'when': when, 'type': failure_type(reason), 'reason': reason}
        if outcome == 'skipped' and skip is None:
            skip = report.get('wasxfail') or ''
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
    files = {}
    for item in receipt['items']:
        files.setdefault(item['file'], []).append(item['nodeid'].split('::', 1)[1])
    return {'head': receipt['source_before']['head'], 'inputs': digest_inputs(receipt['source_before']['inputs']),
            'files': files, 'problems': [{'nodeid': p['nodeid'], 'outcome': p['outcome'],
                                          'reason': stable_reason(p.get('detail'))} for p in receipt['collection_problems']],
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
    """Per-nodeid states of one execution receipt, refused unless it is bound to the same source."""
    path = Path(path)
    receipt = json.loads(path.read_text())
    if receipt.get('kind') != 'execution':
        raise ValueError('not an execution receipt: ' + str(path))
    if not receipt.get('source_unchanged'):
        raise ValueError('source changed while executing: ' + str(path))
    if receipt['source_before']['head'] != head or digest_inputs(receipt['source_before']['inputs']) != inputs:
        raise ValueError('receipt is bound to another source snapshot: ' + str(path))
    reports = {}
    for report in receipt['reports']:
        reports.setdefault(report['nodeid'], []).append(report)
    skips = junit_skips(path.with_name('result.xml'))
    states = {}
    for item in receipt['items']:
        outcome, phases, failure = node_state(reports.get(item['nodeid'], []))
        states[item['nodeid']] = {'outcome': outcome, 'phases': phases, 'failure': failure,
                                  'skip': skips.get(_junit_key(item['nodeid']), '') if outcome == 'skipped' else ''}
    return states, receipt


def signature(state):
    """What must stay the same for a known entry to count as unchanged."""
    failure = state.get('failure') or {}
    return (state['outcome'], failure.get('when', ''), failure.get('type', ''), failure.get('reason', ''))


def compare(known, collection, states):
    """Differences between a known-state document and new receipts for the same file set.

    known      the document (``collection.files`` and ``entries``)
    collection {file: [names]} from the new collection receipt
    states     {nodeid: state} from the new execution receipts (only executed files)
    Every list in the result is a difference that needs an explanation; all empty means
    the new run shows exactly the known state.
    """
    result = {key: [] for key in ('collected_only_now', 'no_longer_collected', 'new_non_pass', 'now_passing',
                                  'changed', 'not_reported')}
    before = {file + '::' + name for file, names in known['collection']['files'].items() for name in names}
    after = {file + '::' + name for file, names in collection.items() for name in names}
    result['collected_only_now'] = sorted(after - before)
    result['no_longer_collected'] = sorted(before - after)
    entries = {entry['nodeid']: entry for entry in known['entries']}
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


def _load_run(collection_path, execution_roots):
    collection = read_collection(collection_path)
    states, receipts = {}, []
    for root in execution_roots:
        for path in sorted(Path(root).glob('*/receipt.json')):
            part, _ = read_execution(path, head=collection['head'], inputs=collection['inputs'])
            overlap = set(part) & set(states)
            if overlap:
                raise ValueError('nodeid executed twice: ' + sorted(overlap)[0])
            states.update(part)
            receipts.append({'path': str(path), 'sha256': file_digest(path)})
    return collection, states, receipts


def main(argv):
    """compare <known-state.json> <collection receipt> <execution directory>...

    Prints the differences as JSON and exits 1 when there is any.
    """
    if len(argv) < 4 or argv[0] != 'compare':
        print(main.__doc__)
        return 2
    known = json.loads(Path(argv[1]).read_text())
    collection, states, _ = _load_run(argv[2], argv[3:])
    if collection['head'] != known['candidate']['head'] or collection['inputs'] != known['candidate']['inputs']:
        print(json.dumps({'different_source': [known['candidate']['head'], collection['head']]}, indent=1))
        return 1
    result = compare(known, collection['files'], states)
    print(json.dumps(result, indent=1, ensure_ascii=False))
    return 0 if is_clean(result) else 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
