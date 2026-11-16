"""Bounded real-worker observations for original relational history semantics."""
import asyncio
from pathlib import Path
from _worker_event_fixtures import emit


async def cached_transform(row, *, log, failure, key, output, square=False):
    value = row[key]
    emit(log, 'call', value=value)
    await asyncio.sleep(0)
    if value == 2 and not Path(failure).exists():
        Path(failure).write_text('original one-shot business failure')
        raise RuntimeError('interrupted' if key == 'i' else 'failure to preserve')
    return {**row, output: value * value if square else value * 2}


async def double(row):
    await asyncio.sleep(0)
    return {**row, 'doubled': row['n'] * 2}


async def echo(row):
    await asyncio.sleep(0)
    return row


def source_with_context(log):
    from demiflow.execution.run_context import current_run, current_action
    view = current_run()
    assert view is not None and current_action() is None
    assert not hasattr(view, 'journal') and not hasattr(view, 'reserve')
    try:
        for number in range(7):
            assert current_run() is view and current_action() is None
            emit(log, 'pull', value=number, run_id=view.ref.run_id,
                 directory=view.ref.directory, attempt=view.attempt)
            yield {'n': number}
    finally:
        emit(log, 'source_close', run_id=view.ref.run_id)
