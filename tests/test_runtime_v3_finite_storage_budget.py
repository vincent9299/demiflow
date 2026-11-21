"""Finite input storage is bounded by disk, independently of transport credit."""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import ExecutionBudgetExceeded
from demiflow.execution.contracts.dataflow_errors import DataflowError


def _rows():
    for i in range(16384):
        yield {'k': 0 if i % 10 else 1 + (i // 10) % 127,
               'v': i, 'payload': 'x' * 4096}


def _reduce(acc, row):
    return {'k': row['k'], 'count': (acc or {}).get('count', 0) + 1,
            'sum': (acc or {}).get('sum', 0) + row['v']}


@pytest.mark.parametrize('disk_bytes', [2 * 1024**3, 8 * 1024**2])
def test_wide_hot_key_uses_original_disk_budget(tmp_path, monkeypatch, disk_bytes):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    prepare, plans, executions = DataflowExecution.prepare, [], []
    def capture(execution):
        result = prepare(execution)
        plans.append(execution._physical)
        executions.append(execution)
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', capture)
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path/'capacity'))
    limits = dict(memory_bytes=64*1024**2, max_row_bytes=65536,
        max_batch_bytes=262144, max_scratch_bytes=disk_bytes,
        max_state_bytes=2*1024**2, max_output_rows=1000000,
        checkpoint_rows=512, checkpoint_interval_ms=1000, timeout_s=120)
    if disk_bytes < 64*1024**2:
        with pytest.raises((DataflowError, ExecutionBudgetExceeded)) as error:
            with RunContext.start(tmp_path/'run', limits=limits):
                data.from_iter(_rows).reduce_by_key('k', _reduce).take_all()
        if isinstance(error.value, DataflowError):
            assert error.value.code == 'BUDGET_EXCEEDED'
        else:
            # Staging and other journal-owned files share the original scratch ceiling.
            assert (error.value.budget, str(error.value)) in {
                ('artifact_staging_bytes', 'Execution budget exhausted: artifact_staging_bytes'),
                ('max_scratch_bytes', 'Runtime storage exceeds scratch admission budget'),
            }
        assert not any(e._restore_image is not None and e._restore_image.cut.final
                       for e in executions)
    else:
        with RunContext.start(tmp_path/'run', limits=limits) as run:
            actual = data.from_iter(_rows).reduce_by_key('k', _reduce).take_all()
            expected = {}
            for row in _rows():
                expected[row['k']] = _reduce(expected.get(row['k']), row)
            assert sorted(actual, key=lambda r: r['k']) == sorted(expected.values(), key=lambda r: r['k'])
            assert len(actual) == 128 and sum(r['count'] for r in actual) == 16384
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    plan = plans[0]
    finite = [e for e in plan.exchanges if e.mode == 'finite_sealed']
    assert finite and sum(e.storage_bytes for e in plan.exchanges) <= disk_bytes
    assert all(e.credit_bytes <= 4*limits['max_batch_bytes'] for e in finite)
    assert max(e.storage_bytes for e in finite) > 8*limits['max_batch_bytes']

