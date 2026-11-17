"""One real terminal document feeds the original spool and live transport."""
from collections import Counter
import math
from types import SimpleNamespace as NS

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_record_frames import _typed_shape, encode_document
from demiflow.execution.exchange import dataflow_bridge as bridge
from demiflow.execution.run_context import RunContext
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
from test_runtime_v3_c import bounds
from test_runtime_v3_record_frame_budget import _record


def project(row):
    return {'id': row['id'], 'value': ('测试🙂"\\', b'\x00\xff', -0.0), 'out': row['id'] * 2}


def test_public_rows_and_final_replay_keep_original_document_and_ack(tmp_path, monkeypatch):
    encode = bridge._encode_record_document
    publish = WorkerGraphRunner._publish
    encodings, documents = Counter(), []

    def counted(record, **kwargs):
        encodings[(record.record_id, record.sequence)] += 1
        return encode(record, **kwargs)

    def observed(runner, record, **kwargs):
        result = publish(runner, record, **kwargs)
        if kwargs.get('spec') is None:
            # Real peer publication must have consumed the exact same bytes
            # already admitted for this record's durable terminal spool.
            assert kwargs.get('validated_raw') == encode(record,
                max_bytes=runner.bounds.max_row_bytes + runner.bounds.max_control_bytes,
                payload_max_bytes=runner.bounds.max_row_bytes)
            assert encodings[(record.record_id, record.sequence)] == 1
            documents.append(record.record_id)
        return result

    monkeypatch.setattr(bridge, '_encode_record_document', counted)
    monkeypatch.setattr(WorkerGraphRunner, '_publish', observed)
    rows = [{'id': i} for i in range(24)]
    limits = {'memory_bytes': 1024 * 1024, 'max_row_bytes': 4096,
              'max_batch_bytes': 8192, 'max_state_bytes': 32768,
              'checkpoint_rows': 4, 'checkpoint_interval_ms': 60000}
    def flow():
        return data.from_items(rows).map(project)
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted, match='after final action ACK'):
        with RunContext.start(tmp_path / 'run', limits=limits) as run:
            reference = run.ref
            with data.local_execution(workers=2, worker_mode='thread'):
                actual = list(flow().iter_rows())
            assert actual == [project(row) for row in rows]
            assert all(type(row['value']) is tuple and type(row['value'][1]) is bytes
                       and math.copysign(1, row['value'][2]) == -1 for row in actual)
            commits = run.journal.execution_commits('action-1')
            assert commits and all(cut['status'] == 'acked' for cut in commits)
            raise Interrupted('after final action ACK, before run completion')
    assert len(documents) == len(rows)
    before = dict(encodings)
    with RunContext.resume(reference) as run, data.local_execution(workers=2, worker_mode='thread'):
        assert list(flow().iter_rows()) == actual
        assert run.journal.execution_commits('action-1') == commits
    assert dict(encodings) == before and len(documents) == len(rows)


@pytest.mark.parametrize('position', [{}, {'group_key': 'k' * 8192}])
def test_reused_document_still_checks_current_workspace_before_peer_admission(position):
    record = _record({'value': 'x' * 100}, position)
    limits = bounds(max_resident_bytes=65536, max_control_bytes=32768)
    raw = encode_document(record, limits)
    native = _typed_shape(record.inline, limits.max_row_bytes)[1]
    from demiflow.execution.dataflow_record_frames import admit_encoded_record
    calls = []
    def frame(function, *args, **kwargs):
        calls.append(function)
        # The earlier encoding fit, but another live owner now fills the grant.
        return function(*args, retained=limits.max_resident_bytes - native - len(raw) - 4096, **kwargs)
    runner = NS(bounds=limits, _frame_call=frame)
    with pytest.raises(DataflowError, match='workspace'):
        WorkerGraphRunner._publish(runner, record, validated_raw=raw)
    assert calls == [admit_encoded_record]
