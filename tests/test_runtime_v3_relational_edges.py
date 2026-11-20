"""Public relational combinations share one real bounded graph and commit."""
from collections import Counter
import json

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
def test_empty_relational_scope_has_real_zero_row_completion(tmp_path, how):
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        assert data.from_items([]).join(data.from_items([]), on='absent', how=how).take_all() == []
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


def test_five_joins_share_the_run_spill_budget_and_preserve_multiplicity(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    plans = []
    start = DataflowExecution.start
    def observe(execution):
        plans.append(execution._physical)
        return start(execution)
    monkeypatch.setattr(DataflowExecution, 'start', observe)
    rows = [{'k': 2, 'v': 'a'}, {'k': 1, 'v': 'b'}, {'k': 2, 'v': 'c'}, {'k': None, 'v': None}]
    expected = rows
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        dataset = data.from_items(rows)
        for index in range(5):
            right = [{'k': 2, 'r' + str(index): {'flag': value}} for value in (None, True)]
            dataset = dataset.join(data.from_items(right), on='k', how='left')
            expected = [dict(left, **{key: value for key, value in child.items() if key != 'k'})
                for left in expected for child in (right if left['k'] == 2 else [{}])]
        result = dataset.take_all()
        assert Counter(json.dumps(row, sort_keys=True) for row in result) == Counter(
            json.dumps(row, sort_keys=True) for row in expected)
        assert len(result) == 66
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert len(plans) == 1
    assert sum(edge.storage_bytes for edge in plans[0].exchanges) <= plans[0].bounds.max_spill_bytes


def test_join_write_uses_declared_order_and_missing_nullable_field(tmp_path):
    schema = pa.schema([('k', pa.int64()), ('v', pa.struct([('flag', pa.bool_())])),
                        ('r', pa.list_(pa.struct([('n', pa.int64())])))])
    target = str(tmp_path / 'result.lance')
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        left = data.from_items([{'k': 2, 'v': None}, {'k': 1, 'v': {'flag': True}}])
        right = data.from_items([{'k': 1, 'r': [None, {'n': 3}]}])
        left.join(right, on='k', how='left').write_lance(target, mode='create', schema=schema)
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    table = lance.dataset(target).to_table()
    assert table.schema.equals(schema)
    assert sorted(table.to_pylist(), key=lambda row: row['k']) == [
        {'k': 1, 'v': {'flag': True}, 'r': [None, {'n': 3}]}, {'k': 2, 'v': None, 'r': None}]
