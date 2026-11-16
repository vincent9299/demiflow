"""Actual sealed native caller keeps its take limit while producing Arrow."""
from contextlib import nullcontext
from types import SimpleNamespace

import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.native_plan import bind_native_execution, execute_native_plan
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.run_context import RunContext


@pytest.mark.parametrize('workers', [2, 4])
@pytest.mark.parametrize('kind', ['take', 'take_batch'])
def test_sealed_stream_arrow_keeps_limit_and_the_borrowed_action(tmp_path, kind, workers):
    marker = tmp_path / 'source-read'
    def source():
        for value in range(1000):
            with marker.open('a') as stream:
                stream.write(str(value) + '\n')
            yield {'id': value}
    limits = {'batch_rows': 2, 'checkpoint_rows': 2}
    # This attempt-only source cannot park: source/map/schema/terminal are
    # four real resident stages. Smaller budgets must reject before reading.
    expected = pytest.raises(DataflowError, match='cannot safely park') if workers == 2 else nullcontext()
    with expected:
        with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(workers=workers, worker_mode='thread'):
            ds = data.from_iter(source).map(lambda row: row)
            args = {'limit': 3} if kind == 'take' else {'batch_size': 3}
            with run.action(plan_identity(kind, ds._source, ds._plan, arguments=args)) as action:
                layout, placement = bind_native_execution(action, ds._source, ds._plan,
                    decision=SimpleNamespace(action_kind=kind, backend='local_stream',
                        route='stream_delivery', recovery='delivery_only'),
                    profile={'requests': {'cpu_millis': workers * 1000, 'memory_bytes': 192 * 2**20}})
                try:
                    batches = list(execute_native_plan(ds._executor, ds._source, ds._plan,
                        layout=layout, execution_id=placement.execution_id, arrow=True,
                        sink_schema=pa.schema([('id', pa.int32())])))
                finally:
                    if workers == 2:
                        assert not marker.exists()
                        assert not run.journal.execution_commits('action-1')
                assert batches and all(isinstance(batch, pa.RecordBatch) and batch.num_rows <= 2
                                       for batch in batches)
                assert [row['id'] for batch in batches for row in batch.to_pylist()] == [0, 1, 2]
                assert all(batch.schema == pa.schema([('id', pa.int32())]) for batch in batches)
                assert 0 < len(marker.read_text().splitlines()) < 1000
                assert action.record['result'] is None
                action.complete({'kind': kind, 'delivery': 'completed'})
            assert run.position == 1
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
