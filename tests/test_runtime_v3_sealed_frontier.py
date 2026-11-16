"""Late EOI cannot mutate an original contribution while its cut is sealed."""
import json
from dataclasses import replace

import pytest

from demiflow.execution.recovery_contract import identity
from test_runtime_v3_b import worker_env, _cut, _events


@pytest.mark.parametrize('arrival', ['after_snapshot', 'during_snapshot'])
def test_late_eoi_waits_for_resume_but_receipt_and_recut_stay_live(tmp_path, monkeypatch, arrival):
    with worker_env(tmp_path, rows=[]) as env:
        item = env.make('transform')
        worker = item.worker
        worker.start()
        runner = worker._runner
        # Fill the entire original business allowance. Control must not depend
        # on a spare business slot or consume the late EOI to make room.
        runner.bounds = replace(runner.bounds, max_pending_events=1)
        spec, = item.assignment.inputs
        late = {'kind': 'edge_eoi', 'edge_id': spec.edge_id,
                'producer_partition': spec.producer_partitions[0]}
        original = runner.snapshot
        def snapshot(request):
            if arrival == 'during_snapshot' and request.sequence == 0:
                worker.receive(dict(late))
            return original(request)
        monkeypatch.setattr(runner, 'snapshot', snapshot)
        worker.receive({'kind': 'pause'})
        first = _cut(item, sequence=0)
        worker.request_cut(first)
        worker.step(max_records=1, deadline_ms=50)
        assert sum(e.kind == 'cut' for e in _events(item)) == 1
        contribution = worker._snapshot_contributions[first.cut_id]
        key = identity(['worker-frontier', runner.partition])
        def state(value):
            return json.loads(b''.join(item.services.store.read(value.operator_states[key],
                max_bytes=runner.bounds.max_state_bytes)))
        before = state(contribution)
        assert not before['inputs'] and not before['nodes'] and not before['emitted_eoi']
        if arrival == 'after_snapshot':
            worker.receive(dict(late))
        assert len(runner._queue) == runner.bounds.max_pending_events
        commit_id = identity(['test-real-worker-receipt', first.cut_id])
        worker.receive({'kind': 'receipt', 'cut_id': first.cut_id, 'commit_id': commit_id})
        worker.receive({'kind': 'pause'})
        worker.step(max_records=1, deadline_ms=50)
        events = _events(item)
        assert worker._receipts[first.cut_id] == commit_id
        assert first.cut_id not in worker._snapshot_contributions
        assert any(e.kind == 'idle' for e in events)
        assert not any(e.kind == 'eoi' for e in events)
        assert not runner._finished_inputs and not runner._finished
        assert len(runner._queue) == 1
        # The driver's actual receipt -> pause -> idle -> new cut protocol can
        # seal this unchanged frontier even with next-generation input queued.
        second = _cut(item, sequence=1)
        worker.request_cut(second)
        worker.step(max_records=1, deadline_ms=50)
        assert sum(e.kind == 'cut' for e in _events(item)) == 1
        after = state(worker._snapshot_contributions[second.cut_id])
        for field in ('inputs', 'nodes', 'emitted_eoi', 'finished_ports'):
            assert after[field] == before[field]
        assert len(runner._queue) == 1
        worker.receive({'kind': 'receipt', 'cut_id': second.cut_id,
                        'commit_id': identity(['test-real-worker-receipt', second.cut_id])})
        worker.step(max_records=1, deadline_ms=50)
        assert not runner._finished_inputs
        _events(item)
        worker.receive({'kind': 'resume'})
        worker.step(max_records=1, deadline_ms=50)
        events = _events(item)
        assert runner._finished_inputs == {(spec.edge_id, spec.producer_partitions[0])}
        assert runner._finished and not runner._queue
        edge_eoi = [e for e in events if e.kind == 'eoi' and e.payload['scope'] == 'edge']
        assert len(edge_eoi) == 1
        worker.step(max_records=1, deadline_ms=50)
        assert not any(e.kind == 'eoi' for e in _events(item))
