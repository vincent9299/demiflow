"""Business bytes, numeric order and generated seeds cross the actual runtime."""
import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError


@pytest.mark.parametrize('copies,limits', [(160000, {}), (750000, {'max_batch_bytes': 64 * 1024**2})])
def test_wide_binary_join_reaches_declared_native_sink(tmp_path, copies, limits):
    payload = b'full-original\x00' * copies
    target = str(tmp_path / 'wide.lance')
    schema = pa.schema([('k', pa.int64()), ('payload', pa.large_binary()), ('tag', pa.string())])
    with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution():
        left = data.from_items([{'k': 1, 'payload': payload}, {'k': 2, 'payload': None}])
        right = data.from_items([{'k': 1, 'tag': 'a'}, {'k': 1, 'tag': 'b'}])
        left.join(right, on='k', how='left').write_lance(target, schema=schema, mode='create')
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    rows = lance.dataset(target).to_table().to_pylist()
    assert sorted((row['k'], row['tag'] or '') for row in rows) == [(1, 'a'), (1, 'b'), (2, '')]
    assert all(row['payload'] == payload for row in rows if row['k'] == 1)
    assert [row for row in rows if row['k'] == 2] == [{'k': 2, 'payload': None, 'tag': None}]


def test_noncommutative_fold_preserves_numeric_source_order(tmp_path):
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        result = (data.range(120).map(lambda row: {'k': row['id'] % 13, 'v': row['id']})
                  .filter(lambda row: row['v'] % 2 == 0)
                  .reduce_by_key('k', lambda acc, row: {'k': row['k'], 'values':
                      (acc or {}).get('values', []) + [row['v']]}).take_all())
    assert {row['k']: row['values'] for row in result} == {
        key: [i for i in range(120) if i % 13 == key and i % 2 == 0] for key in range(13)}


def test_generated_shuffle_seed_is_consumed_from_sealed_constants(tmp_path, monkeypatch):
    from demiflow.execution.seal import plan as dataflow_seal
    calls = []
    generate = dataflow_seal._generate_constants
    def observe(graph):
        constants = generate(graph)
        calls.append(constants)
        return constants
    monkeypatch.setattr(dataflow_seal, '_generate_constants', observe)
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        rows = data.range(30).random_shuffle().take_all()
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert sorted(row['id'] for row in rows) == list(range(30))
    assert len(calls) == 1 and any(isinstance(value.get('seed'), int) for value in calls[0].values())


def test_declared_row_budget_still_rejects_oversize_before_commit(tmp_path):
    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED|row.*exceed|exceed.*row'):
        with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 4096}) as run, data.local_execution():
            data.from_items([{'payload': b'x' * 10000}]).take_all()
    assert not run.journal.execution_commits('action-1')


def test_finite_join_to_parked_consumer_announces_new_idle_frontier(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_batch_bytes': 64 * 1024**2,
            'timeout_s': 30}) as run, data.local_execution():
        rows = data.from_items([{'k': 1}, {'k': 2}]).join(
            data.from_items([{'k': 1, 'r': 'a'}, {'k': 1, 'r': 'b'}]), on='k', how='left').take_all()
        assert sorted((r['k'], r.get('r', '')) for r in rows) == [(1, 'a'), (1, 'b'), (2, '')]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
