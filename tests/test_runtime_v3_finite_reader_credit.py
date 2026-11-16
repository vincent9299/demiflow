"""Finite readers share the real worker codec grant with live state."""
from dataclasses import replace
import sys
import threading
from types import SimpleNamespace as NS

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_record_frames import encode_document, resident_limit
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
                encode_document(probe, tight.bounds, services=tight)
            required = measured.value.context['codec_workspace_bytes']
            persistent = getattr(drain, 'retained_bytes', drain._stream.buffered_bytes) - drain._stream.buffered_bytes
            held.retain(resident_limit(runner.bounds, services) - required - persistent - 1)
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
        held.retain(resident_limit(runner.bounds, services) - 1024)
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
        held.retain(resident_limit(runner.bounds, services) - 1024)
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
                held.retain(resident_limit(runner.bounds, services) - peak - other - 1)
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
    from demiflow.execution.dataflow_segments import SEGMENTS_MEDIA, SCHEMA
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
