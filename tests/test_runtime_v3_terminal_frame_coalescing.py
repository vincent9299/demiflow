"""Single-row write coalescing keeps old bytes and the original owner grant."""
from collections import deque
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import threading
from types import SimpleNamespace as NS

import pytest

import demiflow
from demiflow.execution import dataflow_record_frames as frames
from demiflow.execution.contracts.dataflow import BlockPartition, Record
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.exchange.dataflow_bridge import _encode_payload, _encode_record_document
from demiflow.execution.recovery_contract import document, identity
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
from demiflow.execution.worker.terminal_blocks import TerminalBlocks
from test_runtime_v3_artifact_accounting import store_fixture
from test_runtime_v3_c import bounds


@pytest.fixture(autouse=True)
def pinned_tree():
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


def record(value, *, tag=None):
    row = {'value': value}
    raw = _encode_payload(row, max_bytes=65536)
    return Record(hashlib.sha256(raw).hexdigest(), {'offset': 1}, None, row,
                  len(raw), 0, block_partition=tag)


def old_line(rec, limits, *, target=None, newline=True):
    raw = _encode_record_document(rec, max_bytes=limits.max_row_bytes + limits.max_control_bytes)
    fields = {'document': raw.decode(), 'record_id': rec.record_id, 'sequence': rec.sequence}
    if target is not None:
        fields['target'] = target
    return document(fields, frames.line_limit(limits)) + (b'\n' if newline else b'')


def observe_pwrite(monkeypatch):
    writes = []
    original = os.pwrite
    def write(fd, raw, offset):
        result = original(fd, raw, offset)
        writes.append((len(raw), result, offset))
        return result
    monkeypatch.setattr(os, 'pwrite', write)
    return writes


def read_tail(blocks):
    tail = blocks._tail
    tail.seek(0)
    chunks = []
    while raw := tail.read(min(tail._io, blocks.bytes - sum(map(len, chunks)))):
        chunks.append(raw)
    return b''.join(chunks)


@pytest.mark.parametrize('value', ['plain', 'x'*11000, '\\"'*1400,
                                  b'\x00\xff'*3000, '测试🙂'*600])
@pytest.mark.parametrize('target', [None, 'consumer-3'])
def test_real_scratch_writes_one_identical_row_instead_of_original_fragments(
        tmp_path, monkeypatch, record_property, value, target):
    limits = bounds(max_row_bytes=16384, max_control_bytes=8192, max_resident_bytes=256*1024)
    rec = record(value)
    expected = old_line(rec, limits, target=target)
    assert len(expected) <= 16384
    writes = observe_pwrite(monkeypatch)
    with store_fixture(tmp_path) as (_, store, _):
        services = NS(store=store, bounds=limits)
        counts = []
        for original_path in (True, False):
            blocks = TerminalBlocks(services, 'worker', partitioned=target is None)
            size, parts = frames.frame_parts(rec, limits, target=target)
            # A generic iterator must keep the original per-fragment validation.
            chunks = (part for part in parts) if original_path else parts
            before = len(writes)
            try:
                blocks.append_parts(rec, chunks, size=size)
                counts.append(len(writes)-before)
                assert read_tail(blocks) == expected
                assert blocks.rows == 1 and blocks.bytes == len(expected)
            finally:
                blocks.close()
                parts.close()
            # Original close retains the zero-byte owner evidence for driver reconciliation.
            assert not list((store.root/store.store_id/'staging').glob('.pending-*'))
        assert counts[0] >= 3 and counts[1] == 1
        record_property('actual_pwrite_counts_original_coalesced', counts)


@pytest.mark.parametrize('case', ['cap', 'scratch_io', 'resident'])
def test_large_row_or_small_budget_keeps_original_fragments(tmp_path, monkeypatch, case):
    settings = dict(max_row_bytes=32768, max_control_bytes=8192, max_resident_bytes=256*1024)
    value = 'x'*17000
    if case == 'scratch_io':
        settings.update(max_row_bytes=4096, max_control_bytes=512)
        value = '\\"'*900
    elif case == 'resident':
        settings.update(max_resident_bytes=20000)
        value = 'x'*4000
    limits = bounds(**settings)
    rec = record(value)
    expected = old_line(rec, limits)
    if case == 'cap':
        assert len(expected) > 16384
    if case == 'scratch_io':
        assert 16384 >= len(expected) > limits.max_row_bytes + limits.max_control_bytes
    writes = observe_pwrite(monkeypatch)
    with store_fixture(tmp_path) as (_, store, _):
        blocks = TerminalBlocks(NS(store=store, bounds=limits), 'worker')
        size, chunks = frames.frame_parts(rec, limits)
        try:
            blocks.append_parts(rec, chunks, size=size)
            assert len(writes) >= 3
            assert read_tail(blocks) == expected
        finally:
            blocks.close()


def owner(store, limits, rec):
    runner = WorkerGraphRunner.__new__(WorkerGraphRunner)
    runner.bounds = limits
    runner.services = NS(store=store, bounds=limits, check_stop=lambda: None)
    native = frames._typed_shape(rec.inline, limits.max_row_bytes)[1]
    source = NS(_pending_record=rec, pending_inline_bytes=native, retained_bytes=native)
    runner.e = NS(_kernels={}, _readers={'source': source})
    runner._peer = NS(_bytes=0)
    runner._finite_input = NS(retained_bytes=0)
    runner._condition, runner._wake_generation = threading.Condition(), 0
    runner._queue = deque()
    runner._terminal_blocks = TerminalBlocks(runner.services, 'worker', frame_call=runner._frame_call)
    runner._condition.wait = lambda *args: pytest.fail('optional fast path waited for credit')
    return runner


@pytest.mark.parametrize('other_owner', ['same_source', 'peer', 'kernel', 'reader', 'another_source'])
def test_real_frame_owner_accounts_current_owners_and_deduplicates_same_source(
        tmp_path, monkeypatch, other_owner):
    limits = bounds(max_row_bytes=8192, max_control_bytes=8192, max_resident_bytes=65536)
    rec = record('x'*4000)
    writes = observe_pwrite(monkeypatch)
    with store_fixture(tmp_path) as (_, store, _):
        runner = owner(store, limits, rec)
        blocks = runner._terminal_blocks
        blocks._block(None)
        size, chunks = frames.frame_parts(rec, limits)
        measured = []
        assert chunks.coalesce(rec, limits, size=size, max_write_bytes=16384,
            admit=lambda peak, current: measured.append((peak, current)) or False) is None
        peak, current = measured[0]
        assert current is rec
        free = limits.max_resident_bytes - peak - blocks._retained_metadata()
        assert free > 0
        if other_owner == 'same_source':
            # Exactly enough room for one copy of source pending row. The
            # original _frame_call sees the same Record and de-duplicates it.
            runner._peer._bytes = free - 1
        elif other_owner == 'peer':
            runner._peer._bytes = free + 1
        elif other_owner == 'kernel':
            runner.e._kernels['kernel'] = NS(retained_resident_bytes=free+1)
        elif other_owner == 'reader':
            runner._finite_input.retained_bytes = free+1
        else:
            runner.e._readers['other'] = NS(retained_bytes=free+1)
        try:
            blocks.append_parts(rec, chunks, size=size)
            assert (len(writes) == 1) == (other_owner == 'same_source')
            assert read_tail(blocks) == old_line(rec, limits)
        finally:
            blocks.close()


@pytest.mark.parametrize('mismatch', ['record', 'bounds', 'size', 'started'])
def test_private_capability_cannot_coalesce_different_or_already_started_input(mismatch):
    limits = bounds()
    rec = record('x'*900)
    size, chunks = frames.frame_parts(rec, limits)
    wanted = old_line(rec, limits)
    used_rec, used_bounds, used_size = rec, limits, size
    prefix = b''
    if mismatch == 'record': used_rec = replace(rec)
    if mismatch == 'bounds': used_bounds = replace(limits)
    if mismatch == 'size': used_size += 1
    if mismatch == 'started': prefix = next(chunks)
    def forbidden(*args): pytest.fail('unbound coalescing reached current owner')
    assert chunks.coalesce(used_rec, used_bounds, size=used_size, max_write_bytes=16384,
                           admit=forbidden) is None
    assert prefix + b''.join(chunks) == wanted
    assert chunks._record is chunks._raw is chunks._suffix is chunks._chunks is None


def test_optional_admission_runs_after_block_switch_flush(tmp_path, monkeypatch):
    limits = bounds(max_row_bytes=8192, max_control_bytes=8192, max_resident_bytes=256*1024)
    layout = identity('two-current-blocks')
    with store_fixture(tmp_path) as (_, store, _):
        blocks = TerminalBlocks(NS(store=store, bounds=limits), 'worker')
        original, checks = blocks._coalescing_fits, []
        def fits(peak, rec):
            index = rec.block_partition.index
            if index:
                assert blocks.blocks[(layout, 0)][1].roots
                assert blocks._tail_key == (layout, 1)
                assert blocks._tail_rows == blocks._tail_bytes == 0
            checks.append(index)
            return original(peak, rec)
        monkeypatch.setattr(blocks, '_coalescing_fits', fits)
        try:
            for index in range(2):
                rec = record('x'*100, tag=BlockPartition(layout, index, 2))
                size, chunks = frames.frame_parts(rec, limits)
                blocks.append_parts(rec, chunks, size=size)
            assert checks == [0, 1]
        finally:
            blocks.close()


@pytest.mark.parametrize('failure', ['io', 'cancel', 'corrupt'])
def test_exception_closes_parts_and_keeps_no_new_copy_in_coalescing_frames(tmp_path, monkeypatch, failure):
    limits = bounds()
    rec = record('x'*2000)
    with store_fixture(tmp_path) as (_, store, _):
        runner = owner(store, limits, rec)
        blocks = runner._terminal_blocks
        size, chunks = frames.frame_parts(rec, limits)
        if failure == 'io':
            def fail(*args): raise OSError('owned writer failure')
            monkeypatch.setattr(os, 'pwrite', fail)
            kind, message = OSError, 'owned writer'
        elif failure == 'cancel':
            def fail(): raise DataflowError('CANCELLED', 'owned control cancelled')
            runner.services.check_stop = fail
            kind, message = DataflowError, 'owned control'
        else:
            # Corrupt this private one-use capability to exercise the defensive
            # length check; no arbitrary external chunks get this capability.
            chunks._suffix += b'corrupt'
            kind, message = DataflowError, 'measured size'
        try:
            with pytest.raises(kind, match=message) as caught:
                blocks.append_parts(rec, chunks, size=size)
            assert chunks._record is chunks._raw is chunks._suffix is chunks._chunks is None
            trace = caught.value.__traceback__
            checked = []
            while trace:
                name = trace.tb_frame.f_code.co_name
                if name == 'coalesce':
                    assert 'escaped' not in trace.tb_frame.f_locals and 'raw' not in trace.tb_frame.f_locals
                    checked.append(name)
                elif name == '_append_parts':
                    assert 'joined' not in trace.tb_frame.f_locals
                    checked.append(name)
                trace = trace.tb_next
            assert '_append_parts' in checked
            if failure == 'corrupt': assert 'coalesce' in checked
            assert blocks.rows == blocks.bytes == blocks._pending_resident == 0
        finally:
            blocks.close()
        assert not list((store.root/store.store_id/'staging').glob('.pending-*'))


def test_default_newline_false_and_generic_chunks_keep_original_validation(tmp_path):
    limits = bounds()
    rec = record('x'*1500)
    size, chunks = frames.frame_parts(rec, limits, target='consumer', newline=False)
    parts = tuple(chunks)
    assert len(parts) >= 3 and b''.join(parts) == old_line(rec, limits, target='consumer', newline=False)
    with store_fixture(tmp_path) as (_, store, _):
        blocks = TerminalBlocks(NS(store=store, bounds=limits), 'worker')
        try:
            with pytest.raises(DataflowError, match='exceeds measured size'):
                blocks.append_parts(rec, iter((b'long',)), size=1)
            assert blocks.rows == blocks.bytes == 0
        finally:
            blocks.close()


def test_additional_finite_spool_dynamically_disables_both_optional_fast_paths(tmp_path, monkeypatch):
    limits = bounds(max_row_bytes=8192, max_control_bytes=8192, max_resident_bytes=65536)
    rec = record('x'*1000)
    writes = observe_pwrite(monkeypatch)
    with store_fixture(tmp_path) as (_, store, _):
        runner = owner(store, limits, rec)
        runner.partition, runner._finite = 'worker', {}
        first = runner._finite_buffer(NS(channel_id='first'))
        def append(blocks):
            size, chunks = frames.frame_parts(rec, limits, target='consumer')
            before = len(writes)
            blocks.append_parts(rec, chunks, size=size)
            return len(writes)-before
        try:
            assert append(first) == 1
            second = runner._finite_buffer(NS(channel_id='second'))
            assert append(first) >= 3
            assert append(second) >= 3
            expected = old_line(rec, limits, target='consumer')
            assert read_tail(first) == expected*2
            assert read_tail(second) == expected
            assert (first.rows, second.rows) == (2, 1)
        finally:
            for blocks in runner._finite.values(): blocks.close()
            runner._terminal_blocks.close()


def test_legacy_optional_metadata_measurement_failure_preserves_original_fragments(tmp_path, monkeypatch):
    limits = bounds()
    rec = record('x'*1000)
    writes = observe_pwrite(monkeypatch)
    with store_fixture(tmp_path) as (_, store, _):
        blocks = TerminalBlocks(NS(store=store, bounds=limits), 'worker')
        def unavailable(*args):
            raise DataflowError('BUDGET_EXCEEDED', 'optional metadata shape bound')
        monkeypatch.setattr(blocks, '_retained_metadata', unavailable)
        size, chunks = frames.frame_parts(rec, limits)
        try:
            blocks.append_parts(rec, chunks, size=size)
            assert len(writes) >= 3 and read_tail(blocks) == old_line(rec, limits)
        finally:
            blocks.close()
