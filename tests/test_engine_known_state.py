"""The known-state comparison reports every difference by nodeid, phase and reason.

These are the cases a count-based comparison misses: one test removed and one added in the
same file; the same test failing in another phase or for another reason; a file that no
longer collects; an entry that passes again.
"""
import json

import engine_known_state as ks


def report(nodeid, when, outcome, detail=None):
    return {'nodeid': nodeid, 'when': when, 'outcome': outcome, 'detail': detail, 'wasxfail': None}


def passed(nodeid):
    return [report(nodeid, when, 'passed') for when in ks.PHASES]


def call_failed(nodeid, line):
    return [report(nodeid, 'setup', 'passed'), report(nodeid, 'call', 'failed', 'x.py:1: in t\n    assert a\nE   ' + line),
            report(nodeid, 'teardown', 'passed')]


def state(reports):
    outcome, phases, failure = ks.node_state(reports)
    return {'outcome': outcome, 'phases': phases, 'failure': failure, 'skip': ''}


def known(files, entries, lane='standard'):
    return {'lanes': {lane: {'collection': {'files': files}}},
            'entries': [dict(lane=lane, nodeid=n, **state(r)) for n, r in entries.items()]}


def compare(base, files, states, **options):
    return ks.compare(base, 'standard', files, states, **options)


A, B, C = 'tests/t.py::a', 'tests/t.py::b', 'tests/t.py::c'


def test_phases_collapse_to_the_pytest_outcome():
    assert ks.node_state(passed(A))[0] == 'passed'
    assert ks.node_state(call_failed(A, 'AssertionError: no'))[0] == 'failed'
    assert ks.node_state([report(A, 'setup', 'failed', 'E   PermissionError: denied')])[0] == 'error'
    assert ks.node_state([report(A, 'setup', 'skipped'), report(A, 'teardown', 'passed')])[0] == 'skipped'
    assert ks.node_state([report(A, 'setup', 'passed')])[0] == 'incomplete'      # the process died in the call
    assert ks.node_state([])[0] == 'incomplete'
    teardown = passed(A)[:2] + [report(A, 'teardown', 'failed', 'E   OSError: leak')]
    assert ks.node_state(teardown)[0] == 'error' and ks.node_state(teardown)[2]['when'] == 'teardown'


def test_reason_is_the_exception_line_without_run_noise():
    receipt = {'environment': {'TMPDIR': '/tmp/demiflow-run/f/017/scratch', 'DEMIFLOW_RESOURCE_ROOT': '/tmp/demiflow-run/f/017/resources',
                               'PYTHONPATH': '/tmp/demiflow-run/tree:/tmp/demiflow-run/tree/tests'}}
    paths = ks.run_paths(receipt)
    assert paths == [('/tmp/demiflow-run/f/017', '<destination>'), ('/tmp/demiflow-run/tree', '<tree>')]
    detail = ('tests/x.py:5: in test\n    f()\nE   demiflow.execution.contracts.dataflow_errors.DataflowError: BUDGET_EXCEEDED: '
              'at /tmp/demiflow-run/f/017/tmp/t0 from /tmp/demiflow-run/tree/demiflow/x.py object <X at 0x7f20611d6330>\nE   second line')
    reason = ks.stable_reason(detail, paths)
    assert reason == ('demiflow.execution.contracts.dataflow_errors.DataflowError: BUDGET_EXCEEDED: '
                      'at <destination>/tmp/t0 from <tree>/demiflow/x.py object <X at 0x*>')
    assert ks.failure_type(reason) == 'DataflowError'
    assert ks.failure_type('assert 7 == 2') == ''
    socket = ('E   OSError: AF_UNIX path length cannot exceed 107 bytes: /tmp/demiflow-run/f/017/scratch/demiflow-ray-blocks-za7bg_hl/'
              'session_2026-11-19_00-28-28_163730_40676/sockets/plasma_store')
    assert ks.stable_reason(socket, paths) == ('OSError: AF_UNIX path length cannot exceed 107 bytes: '
                                               '<destination>/scratch/demiflow-ray-blocks-*/session_*/sockets/plasma_store')
    assert ks.stable_reason('E   RuntimeError: left /tmp/demiflow-w-js and /tmp/demiflow-scope-ab12cd_3/x') == (
        'RuntimeError: left /tmp/demiflow-w-js and /tmp/demiflow-scope-*/x')


def test_same_count_but_a_test_replaced_is_reported():
    base = known({'tests/t.py': ['a', 'b']}, {})
    result = compare(base, {'tests/t.py': ['a', 'c']}, {A: state(passed(A)), C: state(passed(C))})
    assert result['no_longer_collected'] == [B] and result['collected_only_now'] == [C]
    assert not ks.is_clean(result)


def test_same_nodeid_failing_differently_is_reported():
    base = known({'tests/t.py': ['a']}, {A: call_failed(A, 'AssertionError: assert 7 == 2')})
    same = compare(base, {'tests/t.py': ['a']}, {A: state(call_failed(A, 'AssertionError: assert 7 == 2'))})
    assert ks.is_clean(same)
    other_reason = compare(base, {'tests/t.py': ['a']}, {A: state(call_failed(A, 'TypeError: bad call'))})
    assert [item[0] for item in other_reason['changed']] == [A]
    setup_crash = compare(base, {'tests/t.py': ['a']},
                             {A: state([report(A, 'setup', 'failed', 'E   AssertionError: assert 7 == 2')])})
    assert [item[0] for item in setup_crash['changed']] == [A]      # same message, other phase


def test_one_more_failing_subtest_is_a_change():
    def subtests(entries):
        reports = [report(A, 'setup', 'passed')]
        for entry in entries:
            reports.append(dict(report(A, 'call', 'failed', 'x.py:1: in t\nE   AssertionError: 1 != 0'), subtest={'schema': 'pytest.subtest.v1', 'index': 1, 'message': None, 'parameters': {'entry': entry}}))
        return reports + [report(A, 'call', 'passed'), report(A, 'teardown', 'passed')]
    two = state(subtests(['a', 'b']))
    assert two['outcome'] == 'failed' and two['failure']['failed_reports'] == 2
    base = known({'tests/t.py': ['a']}, {A: subtests(['a', 'b'])})
    assert ks.is_clean(compare(base, {'tests/t.py': ['a']}, {A: two}))
    three = compare(base, {'tests/t.py': ['a']}, {A: state(subtests(['a', 'b', 'c']))})
    other = compare(base, {'tests/t.py': ['a']}, {A: state(subtests(['a', 'c']))})
    assert [item[0] for item in three['changed']] == [A] and [item[0] for item in other['changed']] == [A]


def test_new_failure_fixed_entry_and_missing_report():
    base = known({'tests/t.py': ['a', 'b', 'c']}, {A: call_failed(A, 'AssertionError: no')})
    result = compare(base, {'tests/t.py': ['a', 'b', 'c']},
                        {A: state(passed(A)), B: state(call_failed(B, 'ValueError: new')), C: state(passed(C))})
    assert result['now_passing'] == [A] and [item[0] for item in result['new_non_pass']] == [B]
    missing = compare(base, {'tests/t.py': ['a', 'b', 'c']}, {B: state(passed(B)), C: state(passed(C))})
    assert missing['not_reported'] == [A]


def test_lanes_are_kept_apart_and_a_dropped_file_is_one_difference():
    """The same nodeid fails without Ray and passes with it: neither lane may hide the other."""
    base = known({'tests/t.py': ['a'], 'tests/u.py': ['x']}, {A: call_failed(A, "ModuleNotFoundError: No module named 'ray'")})
    base['lanes']['ray'] = {'collection': {'files': {'tests/t.py': ['a']}}}
    assert ks.is_clean(ks.compare(base, 'ray', {'tests/t.py': ['a']}, {A: state(passed(A))}))
    standard = compare(base, {'tests/t.py': ['a']}, {A: state(passed(A))})
    assert standard['now_passing'] == [A] and standard['files_not_collected'] == ['tests/u.py']
    assert standard['no_longer_collected'] == []                      # the file is reported once, not per test
    only_t = compare(base, {'tests/t.py': ['a']}, {A: state(call_failed(A, "ModuleNotFoundError: No module named 'ray'"))},
                     partial=True)
    assert ks.is_clean(only_t)


def write_receipts(root, head, items, reports, python='py'):
    source = {'head': head, 'inputs': {'a': '1'}}
    environment = {'TMPDIR': str(root / 'f/000/scratch'), 'PYTHONPATH': str(root / 'tree')}
    common = {'python': python, 'environment': environment, 'source_before': source, 'source_after': source,
              'source_unchanged': True, 'collection_problems': []}
    listed = [{'file': nodeid.split('::')[0], 'nodeid': nodeid} for nodeid in items]
    (root / 'c').mkdir(parents=True)
    (root / 'f/000').mkdir(parents=True)
    (root / 'c/receipt.json').write_text(json.dumps(dict(common, kind='collection_only', items=listed)))
    (root / 'f/000/receipt.json').write_text(json.dumps(dict(common, kind='execution', items=listed, reports=reports)))
    return str(root / 'c/receipt.json'), str(root / 'f')


def test_build_then_compare_a_later_candidate(tmp_path, capsys):
    collection, executions = write_receipts(tmp_path / 'one', 'old', [A, B],
                                            passed(A) + call_failed(B, 'AssertionError: assert 7 == 2'))
    rules = [{'match': {'file': 'tests/t.py', 'function': 'b'}, 'cause': {'flip_commit': 'abc'},
              'ruling': {'verdict': 'test_mismatch', 'by': 'owner', 'required': 'fix the fixture'}}]
    document = ks.build({'lanes': {'standard': {'description': 'd', 'collection': collection, 'executions': [executions],
                                                'confirm': [executions]}},
                         'annotations': {'rules': rules}})
    assert document['candidate']['head'] == 'old' and document['lanes']['standard']['outcomes'] == {'failed': 1, 'passed': 1}
    (entry,) = document['entries']
    assert (entry['nodeid'], entry['stability'], entry['ruling']['verdict'], entry['cause']) == (
        B, 'stable', 'test_mismatch', {'flip_commit': 'abc'})
    path = tmp_path / 'known.json'
    path.write_text(json.dumps(document))
    assert ks.main(['compare', str(path), 'standard', collection, executions]) == 0
    capsys.readouterr()
    later = write_receipts(tmp_path / 'two', 'new', [A, B], passed(A) + passed(B))
    assert ks.main(['compare', str(path), 'standard', *later]) == 1
    printed = json.loads(capsys.readouterr().out)
    assert printed['same_source'] is False and printed['run_candidate'] == 'new'
    assert printed['differences']['now_passing'] == [B]


def test_a_receipt_bound_to_another_source_is_refused(tmp_path):
    receipt = {'kind': 'execution', 'source_unchanged': True, 'source_before': {'head': 'x', 'inputs': {'a': '1'}},
               'items': [], 'reports': []}
    path = tmp_path / 'receipt.json'
    path.write_text(json.dumps(receipt))
    states, _ = ks.read_execution(path, head='x', inputs=ks.digest_inputs({'a': '1'}))
    assert states == {}
    for head, inputs in (('y', {'a': '1'}), ('x', {'a': '2'})):
        try:
            ks.read_execution(path, head=head, inputs=ks.digest_inputs(inputs))
        except ValueError as error:
            assert 'another source' in str(error)
        else:
            raise AssertionError('accepted a receipt from another source snapshot')
    receipt['source_unchanged'] = False
    path.write_text(json.dumps(receipt))
    try:
        ks.read_execution(path, head='x', inputs=ks.digest_inputs({'a': '1'}))
    except ValueError as error:
        assert 'source changed' in str(error)
    else:
        raise AssertionError('accepted a receipt whose source changed during the run')
