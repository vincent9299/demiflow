"""Cut control must not restart a timed batch behind a sealed consumer."""
from dataclasses import replace
import time

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import thaw_record_value
from demiflow.execution.recovery_contract import identity
import test_runtime_v3_b as harness
from test_runtime_v3_real_seams import prepared


async def _batch_rows(rows):
    return rows


@pytest.mark.parametrize('control', ['cut', 'receipt'])
def test_idle_cut_control_keeps_due_batch_until_resume(tmp_path, monkeypatch, control):
    dataset = data.from_items([{'x': 1}, {'x': 2}]).batch_map(
        _batch_rows, max_batch=3, concurrency=1, flush_interval=10)
    # Reuse the real lease/journal/worker fixture with a real batch program.
    # Only the external clock is controlled; compiler, kernels, transport,
    # snapshots, peer bytes and per-record ACKs are all original implementations.
    monkeypatch.setattr(harness, 'prepared', lambda path, unused: prepared(path, dataset))
    with harness.worker_env(tmp_path) as env:
        source, batch, target = env.make(), env.make('transform'), env.make('terminal')
        for item in (source, batch, target):
            harness._start_with_source_credit(item.worker)
        runner = batch.worker._runner
        kernel = next(iter(batch.worker._kernels.values()))
        now = [100000]
        kernel._services = replace(kernel._services, clock_ms=lambda: now[0],
            constants=thaw_record_value(kernel._services.constants))
        source.worker.step(max_records=1, deadline_ms=1000)
        delivery = next(e for e in harness._events(source) if e.kind == 'data')
        batch.worker.receive(dict(thaw_record_value(delivery.payload), kind='data'))
        batch.worker.step(max_records=1, deadline_ms=1000)
        ack = next(e for e in harness._events(batch) if e.kind == 'ack')
        source.worker.receive(dict(thaw_record_value(ack.payload), kind='ack'))
        assert not source.worker._runner._credits

        for item in (batch, target):
            item.worker.receive({'kind': 'pause'})
            item.worker.step(max_records=1, deadline_ms=50)
            assert any(e.kind == 'idle' for e in harness._events(item))
        assert len(kernel._batch) == 1 and runner._idle_sent
        target_cut = harness._cut(target)
        target.worker.request_cut(target_cut)
        target.worker.step(max_records=1, deadline_ms=50)
        assert any(e.kind == 'cut' for e in harness._events(target))
        assert target.worker._runner._sealed_cut == target_cut.cut_id

        batch_cut = harness._cut(batch)
        if control == 'receipt':
            batch.worker.request_cut(batch_cut)
            batch.worker.step(max_records=1, deadline_ms=50)
            assert any(e.kind == 'cut' for e in harness._events(batch))
            batch.worker.receive({'kind': 'resume', 'publishing': True})
            batch.worker.receive({'kind': 'pause'})
            batch.worker.step(max_records=1, deadline_ms=50)
            assert any(e.kind == 'idle' for e in harness._events(batch))
        now[0] += 10001  # Deadline passes only after both original idle replies.
        if control == 'cut':
            batch.worker.request_cut(batch_cut)
        else:
            batch.worker.receive({'kind': 'receipt', 'cut_id': batch_cut.cut_id,
                                  'commit_id': identity(['original-receipt', batch_cut.cut_id])})
        batch.worker.step(max_records=1, deadline_ms=50)
        events = harness._events(batch)
        # The old runner flushes this batch on the control step. It creates a
        # new peer delivery to the already sealed consumer, then cannot seal
        # its own contribution until an ACK that requires global resume.
        assert len(kernel._batch) == 1
        assert not runner._credits and not any(e.kind == 'data' for e in events)
        assert any(e.kind == 'idle' for e in events)  # Fresh notification retained.
        if control == 'receipt':
            assert batch_cut.cut_id in batch.worker._receipts
            batch_cut = harness._cut(batch, sequence=1)
            batch.worker.request_cut(batch_cut)
            batch.worker.step(max_records=1, deadline_ms=50)
            events = harness._events(batch)
        assert any(e.kind == 'cut' for e in events)
        assert batch_cut.cut_id in batch.worker._snapshot_contributions

        # The work is deferred, not discarded. Original receipts and resume
        # reopen both workers, then the exact pending row crosses real peer
        # transport and releases its original credit through the consumer ACK.
        for item, cut in ((batch, batch_cut), (target, target_cut)):
            item.worker.receive({'kind': 'receipt', 'cut_id': cut.cut_id,
                                 'commit_id': identity(['final-test-receipt', cut.cut_id])})
            item.worker.step(max_records=1, deadline_ms=50)
            harness._events(item)
            item.worker.receive({'kind': 'resume'})
        deadline = time.monotonic() + 3
        outgoing = []
        while not outgoing and time.monotonic() < deadline:
            batch.worker.step(max_records=1, deadline_ms=50)
            outgoing += [e for e in harness._events(batch) if e.kind == 'data']
            if not outgoing:
                time.sleep(.001)
        assert len(outgoing) == 1 and not kernel._batch
        target.worker.receive(dict(thaw_record_value(outgoing[0].payload), kind='data'))
        target.worker.step(max_records=1, deadline_ms=50)
        ack = next(e for e in harness._events(target) if e.kind == 'ack')
        batch.worker.receive(dict(thaw_record_value(ack.payload), kind='ack'))
        assert target.worker._runner._terminal_rows == 1
        assert not runner._credits
