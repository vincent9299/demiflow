"""Real finite artifacts advance cooperatively through the original frontier."""
from copy import deepcopy
from types import SimpleNamespace as NS
import threading

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.worker.finite_input import FiniteInputDrain
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
from test_runtime_v3_artifact_accounting import store_fixture
from test_runtime_v3_finite_borrow import fixture, make_seal


def reader(services, spec):
    rows = []
    edge = NS(edge_id=spec.edge_id, consumer='terminal', input_port='input')
    runner = NS(services=services, bounds=services.bounds, partition='c0',
        _exchange={spec.edge_id: spec}, e=NS(_graph=NS(edges=(edge,)), _node_specs={'terminal': None}, _kernels={}),
        _finished_inputs=set(), _finished=set(), _closed_inputs=set(), _paused=False, _cut=None,
        _park_requested=False, _park_mode=False, _park_finish_pending=False,
        _condition=threading.Condition(), _wake_generation=0, _peer=NS(_bytes=0))
    runner._edge_eoi = lambda edge, producer: runner._finished_inputs.add((edge, producer))
    runner._frame_call = WorkerGraphRunner._frame_call.__get__(runner)
    runner._push = lambda occurrence, port, record: rows.append(dict(record.inline))
    runner._finite_input = FiniteInputDrain(runner)
    return runner._finite_input, runner, rows


@pytest.mark.parametrize('routing', ['forward', 'rebalance', 'broadcast', 'hash', 'target'])
def test_finite_reader_pause_restore_duplicate_handoff_and_exact_typed_suffix(tmp_path, routing):
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = fixture(store, routing=routing)
        seal, _, expected = make_seal(services, spec)
        drain, runner, prefix = reader(services, spec)
        resumed = None
        try:
            drain.adopt(spec.edge_id, seal)
            for _ in range(3):
                assert drain.step(max_records=1, deadline_ms=1000) == 1
            saved = drain.snapshot()
            assert sum(saved[spec.edge_id]['positions'].values()) == 3
            assert not runner._finished_inputs
            runner._paused = True
            assert drain.step(max_records=20, deadline_ms=1000) == 0
            assert drain.snapshot() == saved
            drain.close()
            resumed, owner, suffix = reader(services, spec)
            resumed.restore(saved)
            resumed.adopt(spec.edge_id, seal)  # Driver redelivers after activate.
            assert resumed.snapshot() == saved
            for _ in range(40):
                advanced = resumed.step(max_records=1, deadline_ms=1000)
                assert advanced in (0, 1)
                if advanced == 0:
                    break
            assert owner._finished_inputs == {(spec.edge_id, p) for p in spec.producer_partitions}
            assert prefix + suffix == expected
            assert resumed._stream is None
        finally:
            drain.close()
            if resumed is not None:
                resumed.close()


@pytest.mark.parametrize('corruption', ['negative', 'beyond', 'foreign_edge', 'foreign_producer', 'bool'])
def test_finite_reader_rejects_invalid_cursor_before_business(tmp_path, corruption):
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = fixture(store)
        seal, _, _ = make_seal(services, spec)
        drain, _, _ = reader(services, spec)
        drain.adopt(spec.edge_id, seal)
        saved = deepcopy(drain.snapshot())
        state = saved[spec.edge_id]['positions']
        if corruption == 'foreign_edge':
            saved['foreign'] = saved.pop(spec.edge_id)
        elif corruption == 'foreign_producer':
            state['foreign'] = state.pop('p0')
        else:
            state['p0'] = {'negative': -1, 'beyond': 5, 'bool': True}[corruption]
        restored, _, rows = reader(services, spec)
        try:
            with pytest.raises(DataflowError):
                restored.restore(saved)
            assert rows == [] and restored._stream is None
        finally:
            drain.close()
            restored.close()


def test_old_completed_frontier_survives_two_resume_generations(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        services, spec = fixture(store)
        seal, _, expected = make_seal(services, spec)
        first, owner, rows = reader(services, spec)
        second = None
        try:
            owner._finished_inputs.add((spec.edge_id, 'p0'))
            first.restore({})  # Historical frontier did not contain this key.
            first.adopt(spec.edge_id, seal)
            saved = first.snapshot()
            assert saved[spec.edge_id]['positions'] == {'p0': 4, 'p1': 0}
            second, restored, suffix = reader(services, spec)
            restored._finished_inputs = set(owner._finished_inputs)
            second.restore(saved)
            while second.step(max_records=1, deadline_ms=1000):
                pass
            assert rows == [] and suffix == expected[4:]
        finally:
            first.close()
            if second is not None:
                second.close()
