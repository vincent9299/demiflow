"""An interrupted feed must never return success to the materialization caller."""
import asyncio
import pytest
from demiflow import data
from _worker_event_fixtures import emit, events


def test_operator_stop_file_drains_admitted_work(tmp_path, monkeypatch):
    from demiflow.services.request_limits import ServiceStopped
    stop = tmp_path / 'STOP'
    observed = str(tmp_path / "actual-cancellation.jsonl")
    drained = []

    class Pending:
        cancel_drain_timeout_s = 2

        async def __call__(self, row):
            emit(observed, 'admitted', value=row['i'])
            if len([e for e in events(observed) if e['event'] == 'admitted']) == 2:
                stop.write_text('operator review')
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError as exc:
                assert exc.args and isinstance(exc.args[0], ServiceStopped)
                # Model actors use this same reason to finish their exchange
                # and persist the response before relinquishing the worker.
                await asyncio.sleep(.03)
                emit(observed, 'committed', value=row['i'])
                raise

    ds = data.from_items([{'i': i} for i in range(100)]).map_async(Pending(), concurrency=2, queue_depth=2)
    with pytest.raises(ServiceStopped, match='operator_stop_file'):
        ds.run_stream(stop_file=stop, on_drain=lambda stats: drained.append(True))
    actual = events(observed)
    admitted = [e['value'] for e in actual if e['event'] == 'admitted']
    committed = [e['value'] for e in actual if e['event'] == 'committed']
    assert len(admitted) == 2 and sorted(committed) == sorted(admitted)
    assert drained == [True]


def test_existing_operator_stop_file_prevents_dispatch(tmp_path):
    from demiflow.services.request_limits import ServiceStopped
    stop = tmp_path / 'STOP'
    stop.touch()
    admitted = str(tmp_path / "forbidden-admission.jsonl")

    async def work(row):
        emit(admitted, "admitted", row=row)
        return row

    with pytest.raises(ServiceStopped, match='operator_stop_file'):
        data.from_items([{'i': 1}]).map_async(work).run_stream(stop_file=stop)
    assert events(admitted) == []
