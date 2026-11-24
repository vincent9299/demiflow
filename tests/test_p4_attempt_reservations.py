"""An attempt and its reliable-fact capacity are a single durable decision."""
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3

import pytest

from demiflow.errors import ExecutionConflict
from demiflow.execution.journal.run_journal import RunJournal


def managed(directory, *, actions=()):
    journal = RunJournal(directory)
    journal.initialize(dict(runtime_contract='demiflow_forge_runtime_v1', run_id='r',
        root=str(directory), entry='flow', digest='d', report_digest='r',
        parameter_path='p', outputs={}, limits={}, created_at=1), attempts=True)
    journal.transition({None}, 'prepared')
    journal.reserve_nodes([('a', 'original', 0)])
    journal.reserve_actions(list(actions))
    journal.transition({'prepared'}, 'interrupted', usage={'elapsed_ms': 31, 'output_bytes': 9},
                       usage_complete=True, preparation_ready=True)
    return journal


def rows(journal):
    with sqlite3.connect(journal.path) as db:
        return {name: db.execute('SELECT * FROM ' + name + ' ORDER BY rowid').fetchall()
                for name in ('attempts', 'execution_state', 'node_observations',
                             'forge_action_reservations', 'control_events')}


def resume(journal, *, nodes=None, actions=None):
    state = journal.execution_status()
    return journal.begin_attempt(expected_attempt=state['attempt'], expected_revision=state['revision'],
                                 nodes=[] if nodes is None else nodes, actions=[] if actions is None else actions)


def test_atomic_attempt_reserves_new_scope_even_with_bound_old_instance(tmp_path):
    control = managed(tmp_path, actions=['a'])
    previous = RunJournal(tmp_path, attempt=1)
    before = previous.execution_view()
    state = resume(previous, nodes=[('a', 'new', 1)], actions=['a', 'a'])
    assert set(state) == {'attempt', 'status', 'revision', 'updated_at', 'usage', 'usage_complete', 'preparation_ready'}
    assert state['attempt'] == 2 and state['status'] == 'prepared'
    assert state['usage'] == before['state']['usage'] and state['usage_complete'] is True
    assert previous.execution_view() == before
    current = RunJournal(tmp_path, attempt=2).execution_view()
    assert [(n['node_id'], n['state']) for n in current['observations']] == [('new', 'pending')]
    with sqlite3.connect(control.path) as db:
        assert db.execute('SELECT attempt,action_id FROM forge_action_reservations ORDER BY attempt').fetchall() == [(1, 'a'), (2, 'a')]


@pytest.mark.parametrize('table,operation', [
    ('attempts', 'AFTER INSERT'), ('node_observations', 'BEFORE INSERT'),
    ('forge_action_reservations', 'BEFORE INSERT'),
])
def test_faults_rollback_state_attempt_and_all_reservations(tmp_path, table, operation):
    journal = managed(tmp_path, actions=['a'])
    before = rows(journal)
    with sqlite3.connect(journal.path) as db:
        db.execute(f"CREATE TRIGGER fail_attempt {operation} ON {table} WHEN NEW.attempt=2 BEGIN SELECT RAISE(ABORT, 'injected'); END")
    with pytest.raises(sqlite3.IntegrityError, match='injected'):
        resume(journal, nodes=[('a', 'new', 0)], actions=['b'])
    assert rows(journal) == before


@pytest.mark.parametrize('arguments', [{}, {'nodes': []}, {'actions': []}])
def test_managed_missing_reservation_set_cannot_create_attempt(tmp_path, arguments):
    journal = managed(tmp_path)
    before = rows(journal)
    with pytest.raises(ValueError, match='require'):
        journal.begin_attempt(expected_attempt=1, expected_revision=journal.execution_status()['revision'], **arguments)
    assert rows(journal) == before


def test_cumulative_action_limit_is_eight_and_ninth_rolls_back(tmp_path):
    journal = managed(tmp_path, actions=['a', 'b', 'c', 'd'])
    resume(journal, actions=['a', 'b', 'c', 'd', 'd'])
    journal.transition({'prepared'}, 'interrupted')
    before = rows(journal)
    with pytest.raises(ValueError, match='reservation exceeds budget'):
        resume(journal, nodes=[('a', 'new', 0)], actions=['ninth'])
    assert rows(journal) == before


def test_five_action_run_cannot_get_a_second_reservation_set(tmp_path):
    journal = managed(tmp_path, actions=['a', 'b', 'c', 'd', 'e'])
    before = rows(journal)
    with pytest.raises(ValueError, match='reservation exceeds budget'):
        resume(journal, actions=['a', 'b', 'c', 'd', 'e'])
    assert rows(journal) == before


def test_duplicate_nodes_rejected_and_nonmanaged_old_call_remains_valid(tmp_path):
    journal = managed(tmp_path / 'managed')
    before = rows(journal)
    with pytest.raises(ValueError, match='Duplicate'):
        resume(journal, nodes=[('a', 'n', 0)] * 2)
    assert rows(journal) == before
    sdk = RunJournal(tmp_path / 'sdk')
    sdk.initialize({'sdk': True}, attempts=True)
    sdk.transition({None}, 'interrupted')
    assert sdk.begin_attempt(expected_attempt=1, expected_revision=1)['attempt'] == 2


def test_concurrent_attempt_has_one_complete_winner(tmp_path):
    journal = managed(tmp_path, actions=['a'])
    revision = journal.execution_status()['revision']
    def create(_):
        try:
            return RunJournal(tmp_path).begin_attempt(expected_attempt=1, expected_revision=revision,
                nodes=[('a', 'new', 0)], actions=['a'])['attempt']
        except ExecutionConflict:
            return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(create, range(2)), key=lambda value: value or 0) == [None, 2]
    facts = rows(journal)
    assert len(facts['attempts']) == 2 and len(facts['node_observations']) == 2
    assert len(facts['forge_action_reservations']) == 2
