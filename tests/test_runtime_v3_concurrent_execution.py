"""Actual worker concurrency, timely delivery and per-owner checkpoint state."""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError


def test_async_calls_really_overlap_without_losing_outputs(tmp_path):
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
        assert sorted(actual, key=lambda r: r['n']) == [{'n': i} for i in range(6)]


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
        rows = data.from_items([{'n': i} for i in range(6)]).map(Actor,
            concurrency=3, execution='thread').take_all()
        assert sorted(rows, key=lambda r: r['n']) == [{'n': i} for i in range(6)]


def test_worker_callable_slots_and_rotation_resume(tmp_path, monkeypatch):
    _factory_slots_and_rotation_resume(tmp_path, monkeypatch, scope='worker', concurrency=3)


def test_stage_factory_restores_its_owned_state(tmp_path, monkeypatch):
    _factory_slots_and_rotation_resume(tmp_path, monkeypatch, scope='stage', concurrency=1)


def _factory_slots_and_rotation_resume(tmp_path, monkeypatch, *, scope, concurrency):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    class Counter:
        def __init__(self):
            import uuid
            self.owner = uuid.uuid4().hex
            self.count = 0
        def __call__(self, row):
            self.count += 1
            return {'n': row['n'], 'owner': self.owner, 'count': self.count}
    def pipeline():
        return data.from_items([{'n': i} for i in range(9)]).map(Counter,
            concurrency=concurrency, execution='thread', callable_scope=scope)
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
        rows = pipeline().take_all()
        assert sorted(r['n'] for r in rows) == list(range(9))
        owners = {r['owner'] for r in rows}
        assert len(owners) == concurrency
        for owner in owners:
            counts = [r['count'] for r in rows if r['owner'] == owner]
            assert counts == list(range(1, len(counts) + 1))


def test_fast_result_reaches_consumer_while_predecessor_is_blocked(tmp_path):
    import time
    consumed = tmp_path / 'fast-consumed'
    def produce(row):
        if row['n'] == 0:
            end = time.monotonic() + 5
            while not consumed.exists():
                if time.monotonic() >= end:
                    raise AssertionError('Fast result was blocked by its predecessor')
                time.sleep(.01)
        return row
    def consume(row):
        if row['n'] == 1:
            consumed.write_text('consumed')
        return row
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        rows = data.from_items([{'n': 0}, {'n': 1}]).map(produce, concurrency=2,
            execution='thread').map(consume).take_all()
        assert rows == [{'n': 1}, {'n': 0}]
