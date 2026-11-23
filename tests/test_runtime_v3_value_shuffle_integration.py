"""Typed reducer values traverse shuffle, materialization and the original ACK."""
from collections import Counter
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.contracts.record_codec import _encode_payload
from demiflow.execution.pipeline.run_context import RunContext
from test_runtime_v3_reducer_values import LIMITS, provider, value_executor


VALUES = [[-0.0, b'\x00'], 0, False, b'\xff', (1, None), {'v': 1},
          [-0.0, b'\x00'], [], {}]


def _value(state, row, *, calls):
    with Path(calls).open('a') as target:
        target.write(str(row['key']) + '\n')
    return row['value']


def _box(value):
    return {'value': value}


def _flow(calls):
    return (data.from_items([{'key': index, 'value': value}
                            for index, value in enumerate(VALUES)])
            .reduce_by_key('key', _value, fn_kwargs={'calls': str(calls)})
            .random_shuffle(seed=7))


def _encoded(values):
    return [_encode_payload(value, max_bytes=LIMITS['max_row_bytes']) for value in values]


class _AfterAck(Exception):
    pass


def test_values_shuffle_materialize_and_final_ack_share_one_runtime(provider, tmp_path, monkeypatch):
    calls = tmp_path / 'calls'
    with pytest.raises(_AfterAck):
        with RunContext.start(tmp_path / 'first', limits=LIMITS) as run:
            reference = run.ref
            first = _flow(calls).take_all()
            commits = run.journal.execution_commits('action-1')
            assert commits and all(item['status'] == 'acked' for item in commits)
            raise _AfterAck('interrupt the run only after the actual action ACK')
    assert Counter(_encoded(first)) == Counter(_encoded(VALUES))

    with RunContext.start(tmp_path / 'second', limits=LIMITS):
        second = _flow(calls).take_all()
    assert _encoded(second) == _encoded(first)

    with RunContext.start(tmp_path / 'materialize', limits=LIMITS):
        saved = _flow(calls).materialize()
    try:
        assert saved.columns(fetch_if_missing=False) is None
        assert saved.schema(fetch_if_missing=False) is None
        with RunContext.start(tmp_path / 'consume', limits=LIMITS):
            boxed = saved.map(_box).take_all()
        assert Counter(_encoded(row['value'] for row in boxed)) == Counter(_encoded(VALUES))
    finally:
        saved.release()

    before = calls.read_bytes()
    def forbidden(*args, **kwargs):
        raise AssertionError('final ACK replay must not launch workers')
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', forbidden)
    with RunContext.resume(reference) as run:
        assert _encoded(_flow(calls).take_all()) == _encoded(first)
        assert run.journal.execution_commits('action-1') == commits
    assert calls.read_bytes() == before
