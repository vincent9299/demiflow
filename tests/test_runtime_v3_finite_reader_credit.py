"""Finite readers share the real worker codec grant with live state."""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
from dataclasses import replace
import sys
import threading
from types import SimpleNamespace as NS

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import encode_document, resident_limit
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
from demiflow.execution.worker.finite_input import FiniteInputDrain
from test_runtime_v3_artifact_accounting import store_fixture
from test_runtime_v3_finite_borrow import fixture, make_seal, record


class HeldState:
    def __init__(self):
        self.value = None

    def retain(self, size):
        self.value = bytearray(size - sys.getsizeof(bytearray()))
        assert sys.getsizeof(self.value) <= size + 1

    @property
    def retained_resident_bytes(self):
        return 0 if self.value is None else sys.getsizeof(self.value)


def owner(services, spec):
    runner = WorkerGraphRunner.__new__(WorkerGraphRunner)
    state = HeldState()
    edge = NS(edge_id=spec.edge_id, consumer='terminal', input_port='input')
    runner.e = NS(_graph=NS(edges=(edge,)), _node_specs={'terminal': None},
                  _kernels={'held': state})
    runner.services = services
    runner.bounds = services.bounds
    runner.partition = 'c0'
    runner._exchange = {spec.edge_id: spec}
    runner._finished_inputs = set()
    runner._finished = set()
    runner._closed_inputs = set()
    runner._paused = runner._park_requested = runner._park_mode = False
    runner._park_finish_pending = False
    runner._cut = None
    runner._condition = threading.Condition()
    runner._wake_generation = 0
    runner._peer = NS(_bytes=0)
    runner._edge_eoi = lambda edge, producer: runner._finished_inputs.add((edge, producer))
    output = []
    runner._push = lambda occurrence, port, item: output.append(dict(item.inline))
    runner._finite_input = FiniteInputDrain(runner)
    return runner, state, output


def limited_fixture(store):
    services, spec = fixture(store)
    services.bounds = replace(services.bounds, max_resident_bytes=65536)
    return services, spec


def test_suspended_finite_reader_is_charged_to_other_worker_codec_calls(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = limited_fixture(store)
        seal, _, _ = make_seal(services, spec)
        runner, held, _ = owner(services, spec)
        drain = runner._finite_input
        try:
            drain.adopt(spec.edge_id, seal)
            assert drain.step(max_records=1, deadline_ms=1000) == 1
            assert drain._stream.buffered_bytes > 0
            probe = record({'payload': 'x' * 1024})
            # Observe the original codec's actual workspace requirement; this
            # test does not duplicate its sizing formula.
            tight = NS(**{**vars(services), 'bounds': replace(services.bounds, max_resident_bytes=4096)})
            with pytest.raises(DataflowError) as measured:
                encode_document(probe, tight.bounds, resident_bytes=stage_resident_bytes(getattr(tight, 'assignment', None)))
            required = measured.value.context['codec_workspace_bytes']
            persistent = getattr(drain, 'retained_bytes', drain._stream.buffered_bytes) - drain._stream.buffered_bytes
            held.retain(resident_limit(runner.bounds, stage_resident_bytes(getattr(services, 'assignment', None))) - required - persistent - 1)
            with pytest.raises(DataflowError, match='original resident budget'):
                runner._frame_call(encode_document, probe, runner.bounds)
            drain.close()
            assert runner._frame_call(encode_document, probe, runner.bounds)
        finally:
            drain.close()


def test_initial_finite_authentication_cannot_borrow_live_kernel_window(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = limited_fixture(store)
        seal, _, _ = make_seal(services, spec)
        runner, held, output = owner(services, spec)
        held.retain(resident_limit(runner.bounds, stage_resident_bytes(getattr(services, 'assignment', None))) - 1024)
        try:
            with pytest.raises(DataflowError, match='original resident budget'):
                runner._finite_input.adopt(spec.edge_id, seal)
            assert not output and runner._finite_input.snapshot() == {}
        finally:
            runner._finite_input.close()


def test_reader_growth_rejects_before_fetching_a_new_chunk(tmp_path, monkeypatch):
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = limited_fixture(store)
        seal, refs, _ = make_seal(services, spec)
        runner, held, output = owner(services, spec)
        runner._finite_input.adopt(spec.edge_id, seal)
        original, reads = store.read, []
        def read(ref, **kwargs):
            for chunk in original(ref, **kwargs):
                if ref in refs:
                    reads.append(len(chunk))
                yield chunk
        monkeypatch.setattr(store, 'read', read)
        held.retain(resident_limit(runner.bounds, stage_resident_bytes(getattr(services, 'assignment', None))) - 1024)
        try:
            with pytest.raises(DataflowError, match='original resident budget'):
                runner._finite_input.step(max_records=1, deadline_ms=1000)
            assert reads == [] and output == []
        finally:
            runner._finite_input.close()


def test_existing_buffer_is_not_charged_twice_during_growth(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = limited_fixture(store)
        seal, _, _ = make_seal(services, spec)
        runner, held, _ = owner(services, spec)
        drain = runner._finite_input
        try:
            drain.adopt(spec.edge_id, seal)
            drain.step(max_records=1, deadline_ms=1000)
            stream = drain._stream
            assert b'\n' in stream._buffer
            original = stream._check
            checks = []
            def exact_room(peak):
                # Use the real reader's proposed peak, including its old and
                # replacement buffers. Only the unrelated metadata/state is
                # added; charging the old buffer again rejects this legal read.
                other = drain.retained_bytes - drain.buffered_bytes
                held.retain(resident_limit(runner.bounds, stage_resident_bytes(getattr(services, 'assignment', None))) - peak - other - 1)
                original(peak)
                checks.append(peak)
            stream._check = exact_room
            assert stream.readline(65536).endswith(b'\n')
            assert checks
        finally:
            drain.close()


def test_segment_index_authentication_shares_the_same_live_grant(tmp_path, monkeypatch):
    from dataclasses import asdict
    from demiflow.execution.contracts.dataflow import ProducerCompletion
    from demiflow.execution.storage.segments import SEGMENTS_MEDIA, SCHEMA
    from demiflow.execution.exchange.dataflow_channels import seal_scope
    from demiflow.execution.recovery_contract import document
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = limited_fixture(store)
        _, leaves, _ = make_seal(services, spec)
        members = []
        for part, leaf in zip(spec.producer_partitions, leaves):
            child = {'ref': asdict(leaf), 'rows': 4, 'bytes': leaf.size_bytes, 'height': 0}
            raw = document({'schema': SCHEMA, 'height': 1, 'rows': 64,
                            'bytes': leaf.size_bytes * 16, 'children': [child] * 16})
            index = store.put(iter([raw]), max_bytes=len(raw), media_type=SEGMENTS_MEDIA)
            members.append(ProducerCompletion(part, 0, 'succeeded', 63, index))
        seal = seal_scope(spec.scope_id, tuple(members), store=store)
        runner, held, output = owner(services, spec)
        original, reads = store.read, []
        def read(ref, **kwargs):
            for chunk in original(ref, **kwargs):
                if ref in leaves:
                    reads.append(len(chunk))
                yield chunk
        monkeypatch.setattr(store, 'read', read)
        held.retain(32768)
        try:
            with pytest.raises(DataflowError, match='original resident budget'):
                runner._finite_input.adopt(spec.edge_id, seal)
            assert not reads and not output and runner._finite_input.snapshot() == {}
            held.value = None
            runner._finite_input.adopt(spec.edge_id, seal)
            assert reads and not output
        finally:
            runner._finite_input.close()


def test_parked_reader_uses_original_output_credit_and_keeps_exact_position(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = limited_fixture(store)
        seal, _, expected = make_seal(services, spec)
        runner, _, output = owner(services, spec)
        drain = runner._finite_input
        credit = [1]
        runner._park_can_output = lambda occurrence: credit[0] > 0
        original = runner._push
        def push(occurrence, port, item):
            original(occurrence, port, item)
            credit[0] -= 1
        runner._push = push
        try:
            drain.adopt(spec.edge_id, seal)
            runner._paused = runner._park_requested = runner._park_mode = True
            assert drain.step(max_records=20, deadline_ms=1000) == 1
            assert output == expected[:1] and not runner._park_finish_pending
            assert drain.step(max_records=20, deadline_ms=1000) == 0
            credit[0] = 1
            assert drain.step(max_records=20, deadline_ms=1000) == 1
            assert output == expected[:2]
            assert drain.snapshot()[spec.edge_id]['positions']['p0'] == 2
            runner._park_requested = False
            assert drain.step(max_records=20, deadline_ms=1000) == 0
        finally:
            drain.close()


@pytest.mark.parametrize('maximum', [260, (1 << 30) + 31, (1 << 60) + 31])
def test_cursor_metadata_ceiling_covers_alias_splitting_and_integer_growth(tmp_path, maximum):
    # A pure metadata boundary: no huge producer or fabricated seal is run.
    # Shared initial integers later become independently allocated cursors.
    from demiflow.execution.worker.finite_input import _metadata_ceiling
    from demiflow.execution.contracts.record_codec import metadata_bytes, admit_workspace
    positions = dict.fromkeys(('p' + str(i) for i in range(32)), 0)
    states = {'edge': {'positions': positions}}
    entries = {'edge': tuple({'producer': p, 'total_rows': maximum} for p in positions)}
    ceiling = _metadata_ceiling(states, entries)
    for index, producer in enumerate(positions):
        positions[producer] = int(str(maximum - index))
        assert metadata_bytes((states, entries)) <= ceiling
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = limited_fixture(store)
        runner, _, _ = owner(services, spec)
        runner._finite_input._states, runner._finite_input._entries = states, entries
        runner._finite_input._metadata_bytes = ceiling
        remaining = resident_limit(runner.bounds, stage_resident_bytes(getattr(services, 'assignment', None))) - ceiling
        assert remaining > 0
        runner._frame_call(admit_workspace, remaining, runner.bounds)
        with pytest.raises(DataflowError, match='original resident budget'):
            runner._frame_call(admit_workspace, remaining + 1, runner.bounds)
        runner._finite_input.close()
        assert runner._finite_input.retained_bytes == ceiling


def test_cached_multi_producer_reader_keeps_order_and_original_recovery(tmp_path, monkeypatch):
    from demiflow.execution.worker import finite_input as module
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = fixture(store)
        spec = replace(spec, producer_partitions=tuple('p' + str(i) for i in range(8)))
        services.assignment.inputs = (spec,)
        seal, _, expected = make_seal(services, spec)
        runner, _, prefix = owner(services, spec)
        drain, resumed = runner._finite_input, None
        original, measurements = module.metadata_bytes, []
        def measure(value):
            measurements.append(None)
            return original(value)
        monkeypatch.setattr(module, 'metadata_bytes', measure)
        try:
            drain.adopt(spec.edge_id, seal)
            measurements.clear()
            for _ in range(6):
                assert drain.step(max_records=1, deadline_ms=1000) == 1
            saved, completed = drain.snapshot(), set(runner._finished_inputs)
            assert prefix == expected[:6] and completed == {(spec.edge_id, 'p0')}
            assert measurements == []  # No whole metadata walk per row/codec.
            assert set(saved[spec.edge_id]) == {'seal_ref', 'positions'}
            drain.close()
            next_runner, _, suffix = owner(services, spec)
            next_runner._finished_inputs = completed
            resumed = next_runner._finite_input
            resumed.restore(saved)
            resumed.adopt(spec.edge_id, seal)
            assert resumed.snapshot() == saved
            measurements.clear()
            for _ in range(100):
                if not resumed.step(max_records=1, deadline_ms=1000):
                    break
            assert prefix + suffix == expected
            assert next_runner._finished_inputs == {(spec.edge_id, p) for p in spec.producer_partitions}
            assert measurements == [] and resumed._stream is None
        finally:
            drain.close()
            if resumed is not None:
                resumed.close()


def test_index_growth_is_readmitted_before_extending_the_record_buffer(tmp_path):
    # Independent reproduced boundary: with 7999 bytes free, old-index growth
    # and new-index chunk allocation each fit, but complete growth with the
    # newly retained real index does not. Reject before bytearray.extend.
    from dataclasses import asdict
    from demiflow.execution.storage.segments import block_chunks, SEGMENTS_MEDIA, FINITE_RECORDS_MEDIA, SCHEMA
    from demiflow.execution.storage.streams import frame_stream
    from demiflow.execution.contracts.record_codec import read_chunk_bytes, admit_workspace
    from demiflow.execution.contracts.record_codec import _encode_record_document
    from demiflow.execution.recovery_contract import document
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = limited_fixture(store)
        rows = (record({'v': 1}, 0), record({'v': 'x' * 8192}, 1))
        payload = b''.join(document({'record_id': row.record_id, 'sequence': row.sequence,
            'target': 'c0', 'document': _encode_record_document(row, max_bytes=65536).decode()})
            + b'\n' for row in rows)
        leaf = store.put(iter([payload]), max_bytes=len(payload), media_type=FINITE_RECORDS_MEDIA)
        raw = document({'schema': SCHEMA, 'height': 1, 'rows': 2, 'bytes': len(payload),
            'children': [{'ref': asdict(leaf), 'rows': 2, 'bytes': len(payload), 'height': 0}]})
        root = store.put(iter([raw]), max_bytes=len(raw), media_type=SEGMENTS_MEDIA)
        runner, held, _ = owner(services, spec)
        drain = runner._finite_input
        original, checks = runner._frame_call, []
        def call(function, *args, **kwargs):
            check = {'growth': kwargs.get('_frame_reader_growth', False),
                     'index': drain._index_bytes, 'passed': False}
            checks.append(check)
            result = original(function, *args, **kwargs)
            check['passed'] = True
            return result
        runner._frame_call = call
        def stream():
            return frame_stream(block_chunks(store, root, services.bounds,
                chunk_bytes=read_chunk_bytes(services.bounds, stage_resident_bytes(getattr(services, 'assignment', None))), frame_call=call,
                on_retained=drain._retain_index), services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)),
                check_workspace=lambda size: call(admit_workspace, size, services.bounds,
                                                   _frame_reader_growth=True))
        try:
            drain._stream = stream()
            assert drain._stream.readline(65536).endswith(b'\n')
            drain.close()
            checks.clear()
            held.retain(services.bounds.max_resident_bytes - 8000)
            drain._stream = stream()
            with pytest.raises(DataflowError, match='original resident budget'):
                drain._stream.readline(65536)
            assert checks[0] == {'growth': True, 'index': 0, 'passed': True}
            assert any(c['index'] > 0 and not c['growth'] and c['passed'] for c in checks)
            assert checks[-1]['growth'] and checks[-1]['index'] > 0 and not checks[-1]['passed']
            assert drain._stream._buffer == bytearray()
        finally:
            drain.close()
