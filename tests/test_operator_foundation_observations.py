"""Durable observation identity, reservation, ordering and native counters."""
import sqlite3

import pytest

from demiflow.execution.run_journal import RunJournal, OBSERVATION_COUNT


def update(node='node', sequence=1, state='running', **details):
    return dict(action_id='action', node_id=node, occurrence=0,
                document=dict(sequence=sequence, state=state, **details))


def test_reservation_precedes_execution_and_terminal_wins(tmp_path):
    journal = RunJournal(tmp_path)
    journal.initialize({'format': 'test'})
    journal.reserve_nodes([('action', 'node', 0)])
    journal.observe_nodes([update(rows_output=3)])
    journal.observe_nodes([update(sequence=3, state='succeeded', rows_output=5)])
    journal.observe_nodes([update(sequence=2, rows_output=4), update(sequence=10, rows_output=99)])
    row, = journal.execution_view()['observations']
    assert (row['state'], row['rows_output'], row['sequence']) == ('succeeded', 5, 3)


def test_invalid_identity_rolls_back_entire_batch(tmp_path):
    journal = RunJournal(tmp_path)
    journal.initialize({})
    journal.reserve_nodes([('action', 'node', 0)])
    with pytest.raises(ValueError, match='not reserved'):
        journal.observe_nodes([update(rows_output=3), update('unknown')])
    assert journal.execution_view()['observations'][0]['state'] == 'pending'
    for details in ({'rows_output': -1}, {'elapsed_s': float('nan')}, {'detail': 'x' * 1025}, {'raw_row': {}}):
        with pytest.raises(ValueError):
            journal.observe_nodes([update(**details)])


def test_reservation_uses_count_and_total_byte_intersection(tmp_path):
    journal = RunJournal(tmp_path)
    journal.initialize({})
    journal.reserve_nodes([('action', str(i), 0) for i in range(OBSERVATION_COUNT)])
    with pytest.raises(ValueError, match='byte budget'):
        journal.reserve_nodes([('action', 'extra', 0)])
    assert len(journal.execution_view()['observations']) == OBSERVATION_COUNT


def test_historical_read_does_not_create_observation_table(tmp_path):
    journal = RunJournal(tmp_path)
    journal.initialize({})
    with sqlite3.connect(journal.path) as db:
        db.execute('DROP TABLE node_observations')
    before = journal.path.read_bytes()
    assert journal.execution_view()['observations'] == []
    assert journal.path.read_bytes() == before


def test_local_task_fused_counters_match_real_expansion_and_filter(tmp_path):
    import cloudpickle
    from demiflow.data.plan import CallableSpec, MapOp, FilterOp, FlatMapOp
    from demiflow.execution.local_tasks import _map_task, _rows
    operations = (MapOp(CallableSpec(lambda row: {'x': row['x'] + 1})),
                  FilterOp(CallableSpec(lambda row: row['x'] % 2 == 0)),
                  FlatMapOp(CallableSpec(lambda row: [{'x': row['x']}, {'x': row['x'] + 10}])))
    identities = tuple(('action', str(i), 0) for i in range(3))
    result = _map_task([{'x': i} for i in range(4)], cloudpickle.dumps(operations), tmp_path / 'part', identities)
    assert list(_rows(result.value)) == [{'x': 2}, {'x': 12}, {'x': 4}, {'x': 14}]
    assert [counts for _, counts in result.observations] == [
        {'rows_input': 4, 'rows_output': 4, 'calls': 4}, {'rows_input': 4, 'rows_output': 2, 'calls': 4},
        {'rows_input': 2, 'rows_output': 4, 'calls': 2}]
