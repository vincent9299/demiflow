"""An interrupted feed must never return success to the materialization caller."""
import asyncio
import pytest
from demiflow import data
from demiflow.execution.stream import _arun, _materialize, StreamStats


def test_external_cancellation_cannot_return_partial_success():
    async def scenario():
        entered = asyncio.Event()
        async def blocked(row):
            entered.set()
            await asyncio.Event().wait()
        dataset = data.from_items([{'i': i} for i in range(1000)]).map_async(blocked, concurrency=1, queue_depth=1)
        stages = _materialize(dataset._plan)
        task = asyncio.create_task(_arun(iter([{'i': i} for i in range(1000)]), stages, StreamStats(),
                                        on_progress=None, on_drain=None, log_every=0, cancellation=None))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    asyncio.run(scenario())


def test_operator_stop_file_drains_admitted_work(tmp_path, monkeypatch):
    from demiflow.execution.request_limits import ServiceStopped
    import demiflow.execution.stream as runtime
    monkeypatch.setattr(runtime, '_WATCHDOG_INTERVAL', .01)
    stop = tmp_path / 'STOP'
    admitted, committed, drained = [], [], []

    class Pending:
        cancel_drain_timeout_s = 2

        async def __call__(self, row):
            admitted.append(row['i'])
            if len(admitted) == 2:
                stop.write_text('operator review')
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError as exc:
                assert exc.args and isinstance(exc.args[0], ServiceStopped)
                # Model actors use this same reason to finish their exchange
                # and persist the response before relinquishing the worker.
                await asyncio.sleep(.03)
                committed.append(row['i'])
                raise

    ds = data.from_items([{'i': i} for i in range(100)]).map_async(Pending(), concurrency=2, queue_depth=2)
    with pytest.raises(ServiceStopped, match='operator_stop_file'):
        ds.run_stream(stop_file=stop, on_drain=lambda stats: drained.append(True))
    assert len(admitted) == 2 and sorted(committed) == sorted(admitted)
    assert drained == [True]


def test_existing_operator_stop_file_prevents_dispatch(tmp_path):
    from demiflow.execution.request_limits import ServiceStopped
    stop = tmp_path / 'STOP'
    stop.touch()
    admitted = []

    async def work(row):
        admitted.append(row)
        return row

    with pytest.raises(ServiceStopped, match='operator_stop_file'):
        data.from_items([{'i': 1}]).map_async(work).run_stream(stop_file=stop)
    assert admitted == []
