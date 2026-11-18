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


def known(files, entries):
    return {'collection': {'files': files}, 'entries': [dict(nodeid=n, **state(r)) for n, r in entries.items()]}


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
    detail = ('tests/x.py:5: in test\n    f()\nE   demiflow.execution.contracts.dataflow_errors.DataflowError: BUDGET_EXCEEDED: '
              'at /tmp/demiflow-claude-e1/s1/f/017/tmp/t0 object <X at 0x7f20611d6330>\nE   second line')
    reason = ks.stable_reason(detail)
    assert reason == ('demiflow.execution.contracts.dataflow_errors.DataflowError: BUDGET_EXCEEDED: '
                      'at <destination>/tmp/t0 object <X at 0x*>')
    assert ks.failure_type(reason) == 'DataflowError'
    assert ks.failure_type('assert 7 == 2') == ''


def test_same_count_but_a_test_replaced_is_reported():
    base = known({'tests/t.py': ['a', 'b']}, {})
    result = ks.compare(base, {'tests/t.py': ['a', 'c']}, {A: state(passed(A)), C: state(passed(C))})
    assert result['no_longer_collected'] == [B] and result['collected_only_now'] == [C]
    assert not ks.is_clean(result)


def test_same_nodeid_failing_differently_is_reported():
    base = known({'tests/t.py': ['a']}, {A: call_failed(A, 'AssertionError: assert 7 == 2')})
    same = ks.compare(base, {'tests/t.py': ['a']}, {A: state(call_failed(A, 'AssertionError: assert 7 == 2'))})
    assert ks.is_clean(same)
    other_reason = ks.compare(base, {'tests/t.py': ['a']}, {A: state(call_failed(A, 'TypeError: bad call'))})
    assert [item[0] for item in other_reason['changed']] == [A]
    setup_crash = ks.compare(base, {'tests/t.py': ['a']},
                             {A: state([report(A, 'setup', 'failed', 'E   AssertionError: assert 7 == 2')])})
    assert [item[0] for item in setup_crash['changed']] == [A]      # same message, other phase


def test_new_failure_fixed_entry_and_missing_report():
    base = known({'tests/t.py': ['a', 'b', 'c']}, {A: call_failed(A, 'AssertionError: no')})
    result = ks.compare(base, {'tests/t.py': ['a', 'b', 'c']},
                        {A: state(passed(A)), B: state(call_failed(B, 'ValueError: new')), C: state(passed(C))})
    assert result['now_passing'] == [A] and [item[0] for item in result['new_non_pass']] == [B]
    missing = ks.compare(base, {'tests/t.py': ['a', 'b', 'c']}, {B: state(passed(B)), C: state(passed(C))})
    assert missing['not_reported'] == [A]


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
