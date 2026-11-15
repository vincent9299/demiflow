"""Actual worker concurrency, ordered delivery and per-owner checkpoint state."""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError


def test_async_calls_really_overlap_and_preserve_output_order(tmp_path):
    class Actor:
        async def astart(self):
            import asyncio
            self.barrier = asyncio.Barrier(3)
        async def __call__(self, row):
            import asyncio
            await asyncio.wait_for(self.barrier.wait(), timeout=3)
            await asyncio.sleep((2 - row['n'] % 3) * .01)
            return {'n': row['n']}
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        actual = data.from_items([{'n': i} for i in range(6)]).map_async(Actor, concurrency=3).take_all()
        assert actual == [{'n': i} for i in range(6)]


def test_shared_auto_json_actor_rejected_before_any_business_call(tmp_path):
    called = tmp_path / 'called'
    class Counter:
        def __init__(self):
            self.count = 0
        def __call__(self, row):
            called.write_text('incorrectly called')
            return row
    with pytest.raises(DataflowError, match='auto_json state requires concurrency 1'):
        with RunContext.start(tmp_path / 'run'), data.local_execution():
            data.from_items([{'n': 1}]).map(Counter, concurrency=3,
                execution='thread', callable_scope='stage').take_all()
    assert not called.exists()


def test_thread_calls_really_overlap(tmp_path):
    class Actor:
        def __init__(self):
            import threading
            self.barrier = threading.Barrier(3)
        def __call__(self, row):
            self.barrier.wait(timeout=3)
            return row
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        assert data.from_items([{'n': i} for i in range(6)]).map(Actor,
            concurrency=3, execution='thread').take_all() == [{'n': i} for i in range(6)]


def test_worker_callable_slots_and_rotation_resume(tmp_path, monkeypatch):
    from demiflow.execution.dataflow import DataflowExecution
    class Counter:
        def __init__(self):
            self.count = 0
        def __call__(self, row):
            self.count += 1
            return {'n': row['n'], 'count': self.count}
    def pipeline():
        return data.from_items([{'n': i} for i in range(9)]).map(Counter,
            concurrency=3, execution='thread', callable_scope='worker')
    class Interrupted(Exception):
        pass
    pump = DataflowExecution.pump
    def interrupt(execution, **kwargs):
        result = pump(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            raise Interrupted()
        return result
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 4}) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                pipeline().take_all()
    with RunContext.resume(ref), data.local_execution():
        assert pipeline().take_all() == [{'n': i, 'count': i // 3 + 1} for i in range(9)]
