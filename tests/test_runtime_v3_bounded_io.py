"""Bounded source/spool execution through real Dataset and committed cursors."""
import sqlite3
import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.action_driver.dataflow import DataflowExecution


def test_sql_recovery_reuses_sealed_spool_after_database_changes(tmp_path, monkeypatch):
    path = str(tmp_path / 'input.sqlite')
    with sqlite3.connect(path) as db:
        db.execute('create table records (n integer)')
        db.executemany('insert into records values (?)', [(n,) for n in range(6)])
    connects = tmp_path / 'connections'
    def connection():
        with connects.open('a') as file:
            file.write('connect\n')
        return sqlite3.connect(path)
    def pipeline():
        return data.read_sql('select n from records order by n', connection,
                             snapshot_id='captured-v1')
    class Interrupted(Exception):
        pass
    original = DataflowExecution.pump
    def interrupt(execution, **kwargs):
        result = original(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            raise Interrupted()
        return result
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 2}) as run, data.local_execution():
            reference = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                pipeline().take_all()
    with sqlite3.connect(path) as db:
        db.execute('delete from records')
        db.execute('insert into records values (99)')
    with RunContext.resume(reference), data.local_execution():
        assert pipeline().take_all() == [{'n': n} for n in range(6)]
    assert connects.read_text().splitlines() == ['connect']


def test_batch_generator_streams_first_result_before_producing_rest(tmp_path):
    consumed = tmp_path / 'consumed'
    def expand(batch):
        import time
        yield {'n': [0]}
        deadline = time.monotonic() + 5
        while not consumed.exists():
            if time.monotonic() > deadline:
                raise AssertionError('batch generator was eagerly collected')
            time.sleep(.01)
        yield {'n': [1]}
    def downstream(row):
        consumed.write_text('delivered')
        return row
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        rows = data.from_items([{'n': 1}]).map_batches(expand, batch_size=1).map(downstream).take_all()
        assert rows == [{'n': 0}, {'n': 1}]
