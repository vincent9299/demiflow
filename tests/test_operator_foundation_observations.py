"""Durable observation identity, reservation, ordering and native counters."""
import sqlite3

import pytest

from demiflow.execution.journal.run_journal import RunJournal, OBSERVATION_COUNT


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


def test_worker_metrics_match_real_expansion_and_filter(tmp_path, monkeypatch):
    from demiflow import data
    from demiflow.execution import RunContext
    from demiflow.execution.action_driver.dataflow import DataflowExecution

    finished = []
    accept = DataflowExecution._handle_event

    def observe(execution, event):
        result = accept(execution, event)
        if event.kind == 'progress':
            occurrence = event.payload['occurrence_id']
            metrics = event.payload['metrics']
            node = next(node for node in execution._graph.nodes if node.occurrence_id == occurrence)
            finished.append((node.kernel.kind, metrics['rows_input'], metrics['rows_output']))
        return result

    monkeypatch.setattr(DataflowExecution, '_handle_event', observe)
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=1, partitions=1):
        result = (data.range(4).map(lambda row: {'x': row['id'] + 1})
                  .filter(lambda row: row['x'] % 2 == 0)
                  .flat_map(lambda row: [row, {'x': row['x'] + 10}]).take_all())
    assert result == [{'x': 2}, {'x': 12}, {'x': 4}, {'x': 14}]
    assert ('MapOp', 4, 4) in finished
    assert ('FilterOp', 4, 2) in finished
    assert ('FlatMapOp', 2, 4) in finished
