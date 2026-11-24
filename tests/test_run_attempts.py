"""Native journal attempts retain facts and fence obsolete workers."""
import concurrent.futures

import pytest

from demiflow.execution.journal.run_journal import RunJournal


def interrupted(tmp_path):
    control = RunJournal(tmp_path)
    control.initialize({'immutable': 'binding'}, attempts=True)
    worker = RunJournal(tmp_path, attempt=1)
    worker.transition({None}, 'prepared')
    worker.transition({'prepared'}, 'running')
    worker.reserve_nodes([('a', 'n', 0)])
    worker.action_event('a', 'value', {'value': 1})
    worker.observe_nodes([{'action_id': 'a', 'node_id': 'n', 'occurrence': 0,
                          'document': {'sequence': 1, 'state': 'succeeded', 'calls': 1}}])
    worker.finish({'outcome': 'interrupted'})
    worker.transition({'running'}, 'interrupted', usage={'requests': 3, 'output_bytes': 100})
    return control, worker


def test_new_attempt_preserves_previous_facts_usage_and_completion(tmp_path):
    control, previous = interrupted(tmp_path)
    before = previous.execution_view()
    state = control.begin_attempt(expected_attempt=1, expected_revision=before['state']['revision'])
    assert state['attempt'] == 2 and state['usage'] == before['state']['usage']
    current = RunJournal(tmp_path, attempt=2)
    assert current.execution_view()['events'] == []
    assert current.execution_view()['completion'] is None
    current.reserve_nodes([('a', 'n', 0)])
    current.action_event('a', 'value', {'value': 2})
    current.observe_nodes([{'action_id': 'a', 'node_id': 'n', 'occurrence': 0,
                           'document': {'sequence': 1, 'state': 'running', 'calls': 0}}])
    assert previous.execution_view() == before
    assert current.execution_view()['events'][0]['value'] == {'value': 2}
    assert len(control.list_attempts()) == 2
    history = control.action_history(['a'], before_attempt=2, kinds=['value'])
    assert len(history) == 1 and history[0]['attempt'] == 1 and history[0]['value'] == {'value': 1}


@pytest.mark.parametrize('method', ['action', 'observation', 'reserve', 'transition', 'finish', 'activity', 'poll', 'usage'])
def test_obsolete_worker_cannot_report_into_new_attempt(tmp_path, method):
    control, worker = interrupted(tmp_path)
    control.begin_attempt(expected_attempt=1, expected_revision=worker.execution_status()['revision'])
    operations = {
        'action': lambda: worker.action_event('a', 'value', {'value': 99}),
        'observation': lambda: worker.observe_nodes([]),
        'reserve': lambda: worker.reserve_nodes([]),
        'transition': lambda: worker.transition({'prepared'}, 'running'),
        'finish': lambda: worker.finish({'outcome': 'succeeded'}),
        'activity': lambda: worker.activity('model', 'old'),
        'poll': worker.execution_status,
        'usage': lambda: worker.record_usage(elapsed_ms=1, output_bytes=100),
    }
    with pytest.raises(ValueError, match='Stale'):
        operations[method]()
    assert control.execution_view()['state']['status'] == 'prepared'
    assert control.execution_view()['completion'] is None


def test_concurrent_attempt_cas_has_one_winner(tmp_path):
    control, worker = interrupted(tmp_path)
    revision = worker.execution_status()['revision']
    def attempt(_):
        try:
            return RunJournal(tmp_path).begin_attempt(expected_attempt=1, expected_revision=revision)['attempt']
        except ValueError:
            return None
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(attempt, range(2)), key=lambda v: v or 0) == [None, 2]
    assert len(control.list_attempts()) == 2


def test_historical_journal_is_not_migrated_during_resume(tmp_path):
    legacy = RunJournal(tmp_path)
    legacy.initialize({'legacy': True})
    legacy.transition({None}, 'interrupted')
    legacy.action_event('a', 'value', {'value': 1})
    before = legacy.execution_view()
    with pytest.raises(ValueError, match='Historical'):
        legacy.begin_attempt(expected_attempt=1, expected_revision=1)
    with pytest.raises(ValueError, match='new empty'):
        legacy.initialize({'legacy': True}, attempts=True)
    assert legacy.execution_view() == before and legacy.list_attempts() == []


def test_usage_is_cumulative_and_closing_is_explicit(tmp_path):
    control, worker = interrupted(tmp_path)
    worker.record_usage(elapsed_ms=1000, output_bytes=120)
    assert control.execution_status()['usage_complete'] is False
    with pytest.raises(ValueError, match='cannot decrease'):
        worker.record_usage(elapsed_ms=999, output_bytes=120, complete=True)
    worker.record_usage(elapsed_ms=1100, output_bytes=120, complete=True)
    state = control.execution_status()
    next_state = control.begin_attempt(expected_attempt=1, expected_revision=state['revision'])
    assert next_state['usage'] == {'elapsed_ms': 1100, 'output_bytes': 120}
    assert next_state['usage_complete'] is True
    RunJournal(tmp_path, attempt=2).record_usage(elapsed_ms=1100, output_bytes=120)
    assert control.execution_status()['usage_complete'] is False
    assert worker.execution_view()['state']['usage_complete'] is True
